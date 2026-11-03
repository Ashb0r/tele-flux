import os
import re
import asyncio
from contextlib import asynccontextmanager
from urllib.parse import quote
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from config import PUBLIC_URL, SERVER_HOST, SERVER_PORT
from database import (
    init_db,
    get_catalog_items,
    get_meta_by_id,
    get_streams_by_id,
    get_video_by_location,
    get_library_stats,
)
from settings_manager import load_settings, save_settings, is_configured
from telegram_client import (
    start_telegram_service,
    stop_telegram_service,
    discover_bot_chats,
    lookup_single_chat,
    stream_telegram_file,
    scan_all_channels,
    clear_and_rescan,
)
import httpx

TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("=" * 60)
    print("  🚀 Telegram Stremio Addon Server")
    print(f"  📡 Public URL: {PUBLIC_URL}")
    print(f"  ⚙️ Configuration Web UI: {PUBLIC_URL}/configure")
    print("=" * 60)
    await init_db()
    try:
        from downloader.database import init_downloader_db
        await init_downloader_db()
    except Exception as dbe:
        print(f"[Warning] Downloader DB init notice: {dbe}")

    # If already configured from previous run, connect Telegram service
    if is_configured():
        print("[Telegram] Connecting to Telegram...")
        await start_telegram_service()
    else:
        print("[Notice] Setup required! Open the Web UI in your browser to configure:")
        print(f"         👉 {PUBLIC_URL}/settings")

    yield

    print("[Shutdown] Stopping Telegram services...")
    await stop_telegram_service()

app = FastAPI(title="Telegram Stremio Addon", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------
# Web Configuration UI Endpoints
# ---------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index_page(request: Request):
    # If request is from Stremio or asking for JSON, return manifest
    accept = request.headers.get("accept", "")
    if "application/json" in accept and "text/html" not in accept:
        return await get_manifest()
    
    # Otherwise render configuration UI
    return await settings_page()

@app.get("/settings", response_class=HTMLResponse)
async def settings_page():
    html_file = os.path.join(TEMPLATES_DIR, "settings.html")
    if os.path.exists(html_file):
        with open(html_file, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>Settings template missing</h1>", status_code=500)

@app.get("/configure", response_class=HTMLResponse)
async def configure_page():
    # Backwards compatibility: render settings page
    return await settings_page()

@app.get("/stremio", response_class=HTMLResponse)
async def stremio_page():
    html_file = os.path.join(TEMPLATES_DIR, "stremio.html")
    if os.path.exists(html_file):
        with open(html_file, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>Stremio template missing</h1>", status_code=500)

@app.get("/api/config")
async def get_config_api():
    s = load_settings()
    return {
        "configured": s.get("configured", False),
        "api_id": s.get("api_id"),
        "api_hash": s.get("api_hash", ""),
        "bot_token": s.get("bot_token", ""),
        "login_mode": s.get("login_mode", "bot"),
        "session_string": s.get("session_string", ""),
        "tmdb_api_key": s.get("tmdb_api_key", ""),
        "channel_ids": s.get("channel_ids", []),
        "download_channel_ids": s.get("download_channel_ids", []),
        "downloader_enabled": s.get("downloader_enabled", False),
        "media_dir": s.get("media_dir", "/media"),
        "min_free_disk_gb": s.get("min_free_disk_gb", 4.5),
        "public_url": PUBLIC_URL
    }

class SaveSettingsRequest(BaseModel):
    api_id: int
    api_hash: str
    bot_token: str | None = ""
    login_mode: str | None = "bot"
    session_string: str | None = ""
    tmdb_api_key: str | None = ""

@app.post("/api/settings")
async def save_settings_api(payload: SaveSettingsRequest):
    if not payload.api_id or not payload.api_hash:
        raise HTTPException(status_code=400, detail="Telegram API ID and API Hash are required.")
    if payload.login_mode == "user" and not (payload.session_string or "").strip():
        raise HTTPException(status_code=400, detail="Session string is required for user login mode.")
    if payload.login_mode != "user" and not (payload.bot_token or "").strip():
        raise HTTPException(status_code=400, detail="Bot token is required for bot login mode.")

    save_data = {
        "api_id": payload.api_id,
        "api_hash": payload.api_hash.strip(),
        "bot_token": (payload.bot_token or "").strip(),
        "login_mode": payload.login_mode or "bot",
        "session_string": (payload.session_string or "").strip(),
        "tmdb_api_key": (payload.tmdb_api_key or "").strip()
    }
    save_settings(save_data)
    success = await start_telegram_service()
    return {
        "success": True,
        "connected": success,
        "message": "API credentials saved and verified successfully!"
    }

class VerifyTelegramRequest(BaseModel):
    bot_token: str

@app.post("/api/verify-telegram")
async def verify_telegram_api(payload: VerifyTelegramRequest):
    token = payload.bot_token.strip()
    if not token:
        return {"valid": False, "message": "Bot token cannot be empty."}
    url = f"https://api.telegram.org/bot{token}/getMe"
    try:
        async with httpx.AsyncClient(timeout=8.0) as http:
            resp = await http.get(url)
            data = resp.json()
            if data.get("ok"):
                bot = data.get("result", {})
                return {
                    "valid": True,
                    "bot_name": bot.get("first_name", "Bot"),
                    "bot_username": bot.get("username", ""),
                    "bot_id": bot.get("id"),
                    "message": f"Connected to Telegram as @{bot.get('username')}"
                }
            else:
                return {"valid": False, "message": data.get("description", "Invalid Bot Token")}
    except Exception as e:
        return {"valid": False, "message": f"Network error contacting Telegram: {str(e)}"}

class StremioChannelsRequest(BaseModel):
    channel_ids: list[int]

@app.post("/api/stremio/channels")
async def save_stremio_channels_api(payload: StremioChannelsRequest):
    current = load_settings()
    dl_set = set(current.get("download_channel_ids", []))
    conflicts = [cid for cid in payload.channel_ids if cid in dl_set]
    if conflicts:
        raise HTTPException(status_code=400, detail="Cannot select channels already configured for local downloading.")

    save_settings({"channel_ids": payload.channel_ids})
    await start_telegram_service()
    return {
        "success": True,
        "channel_ids": payload.channel_ids,
        "manifest_url": f"{PUBLIC_URL}/manifest.json"
    }

@app.post("/api/stremio/scan")
async def scan_stremio_channel_api():
    from telegram_client import trigger_stremio_scan
    res = await trigger_stremio_scan()
    return res

@app.post("/api/stremio/clear")
async def clear_stremio_catalog_api():
    from database import clear_library_database
    await clear_library_database()
    return {"success": True, "message": "Stremio indexed catalog cleared successfully."}

@app.get("/api/stremio/stats")
async def stremio_stats_api():
    from database import get_library_stats
    from telegram_client import is_scanning_channels
    stats = await get_library_stats()
    return {
        "stats": stats,
        "is_scanning": is_scanning_channels(),
        "manifest_url": f"{PUBLIC_URL}/manifest.json"
    }

@app.post("/api/system/reset")
async def system_reset_api():
    from database import reset_all_data
    from settings_manager import reset_settings
    from telegram_client import stop_telegram_service
    await stop_telegram_service()
    await reset_all_data()
    reset_settings()
    return {"success": True, "message": "System has been completely reset to fresh state."}

class FetchChatsRequest(BaseModel):
    bot_token: str | None = None

@app.post("/api/fetch-chats")
async def fetch_chats_api(payload: FetchChatsRequest | None = None):
    token = (payload.bot_token.strip() if payload and payload.bot_token else "") or load_settings().get("bot_token", "")
    if not token:
        return {
            "error": "Telegram Bot Token is not configured. Please configure your bot token in Settings first.",
            "chats": []
        }
    res = await discover_bot_chats(token)
    return res

class LookupChatRequest(BaseModel):
    chat_identifier: str
    bot_token: str | None = None

@app.post("/api/lookup-chat")
async def lookup_chat_api(payload: LookupChatRequest):
    token = (payload.bot_token.strip() if payload.bot_token else "") or load_settings().get("bot_token", "")
    if not token:
        return {"error": "Telegram Bot Token is not configured. Please configure your bot token in Settings first."}
    res = await lookup_single_chat(token, payload.chat_identifier)
    return res

class VerifyTmdbRequest(BaseModel):
    api_key: str

@app.post("/api/verify-tmdb")
async def verify_tmdb_api(payload: VerifyTmdbRequest):
    key = payload.api_key.strip()
    if not key:
        return {"valid": False, "message": "API Key cannot be empty."}
    
    url = f"https://api.themoviedb.org/3/configuration?api_key={key}"
    try:
        async with httpx.AsyncClient(timeout=8.0) as http:
            resp = await http.get(url)
            if resp.status_code == 200:
                return {"valid": True, "message": "TMDB API Key verified successfully!"}
            else:
                return {"valid": False, "message": "Invalid TMDB API key. Check key from themoviedb.org and try again."}
    except Exception as e:
        return {"valid": False, "message": f"Network error contacting TMDB: {str(e)}"}

class SaveConfigRequest(BaseModel):
    api_id: int
    api_hash: str
    bot_token: str
    tmdb_api_key: str | None = ""
    channel_ids: list[int] | None = None
    download_channel_ids: list[int] | None = None

@app.post("/api/save-config")
async def save_config_api(payload: SaveConfigRequest):
    if not payload.api_id or not payload.api_hash or not payload.bot_token:
        raise HTTPException(status_code=400, detail="Missing required credentials.")

    # Save to persistent settings.json
    save_data = {
        "api_id": payload.api_id,
        "api_hash": payload.api_hash.strip(),
        "bot_token": payload.bot_token.strip(),
        "tmdb_api_key": (payload.tmdb_api_key or "").strip()
    }
    if payload.channel_ids is not None:
        save_data["channel_ids"] = payload.channel_ids
    if payload.download_channel_ids is not None:
        save_data["download_channel_ids"] = payload.download_channel_ids

    save_settings(save_data)

    # Start or restart the Telegram client with new settings
    success = await start_telegram_service()
    if not success:
        return {"error": "Failed to connect to Telegram with provided credentials."}

    manifest_url = f"{PUBLIC_URL}/manifest.json"
    return {
        "success": True,
        "manifest_url": manifest_url
    }

# ---------------------------------------------------------------------
# Stremio Addon Protocol Endpoints
# ---------------------------------------------------------------------

@app.get("/manifest.json")
async def get_manifest():
    conf = is_configured()
    return {
        "id": "org.homelab.telegram.streams",
        "version": "1.2.0",
        "name": "Telegram Library TV",
        "description": "Stream high-definition videos directly from your Telegram channels without buffering",
        "resources": ["catalog", "meta", "stream"] if conf else [],
        "types": ["movie", "series"],
        "catalogs": [
            {
                "type": "movie",
                "id": "tg_movies",
                "name": "Telegram Movies",
                "extra": [{"name": "skip", "isRequired": False}]
            },
            {
                "type": "series",
                "id": "tg_series",
                "name": "Telegram Series",
                "extra": [{"name": "skip", "isRequired": False}]
            }
        ] if conf else [],
        "idPrefixes": ["tt", "tmdb:"],
        "behaviorHints": {
            "configurable": True,
            "configurationRequired": not conf
        }
    }

# ---------------------------------------------------------------------
# Dynamic SVG Poster Generator (Ensures no card is EVER blank in Stremio)
# ---------------------------------------------------------------------

@app.get("/poster/{media_type}/{title}.svg")
async def get_poster_image(media_type: str, title: str):
    from tmdb import generate_poster_svg
    svg_data = generate_poster_svg(title, media_type)
    return Response(
        content=svg_data,
        media_type="image/svg+xml",
        headers={"Cache-Control": "public, max-age=86400"}
    )

def resolve_poster(poster: str | None, media_type: str, title: str) -> str:
    if poster and poster.strip() and poster.startswith("http"):
        return poster
    return f"{PUBLIC_URL}/poster/{media_type}/{quote(title)}.svg"

@app.get("/catalog/{media_type}/{catalog_id}.json")
@app.get("/catalog/{media_type}/{catalog_id}/{extra}.json")
async def get_catalog(media_type: str, catalog_id: str, extra: str | None = None):
    skip = 0
    if extra and "skip=" in extra:
        try:
            skip = int(extra.split("skip=")[-1].split("&")[0])
        except ValueError:
            skip = 0

    target_type = "series" if media_type == "series" else "movie"
    items = await get_catalog_items(media_type=target_type, limit=100, skip=skip)

    metas = []
    for item in items:
        safe_poster = resolve_poster(item.get("poster"), target_type, item["title"])
        metas.append({
            "id": item["stremio_id"],
            "type": target_type,
            "name": item["title"],
            "poster": safe_poster,
            "releaseInfo": str(item["year"]) if item["year"] else None,
            "description": item["overview"]
        })

    return {"metas": metas}

@app.get("/meta/{media_type}/{item_id}.json")
async def get_meta(media_type: str, item_id: str):
    target_type = "series" if media_type == "series" else "movie"
    item = await get_meta_by_id(item_id, target_type)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    safe_poster = resolve_poster(item.get("poster"), target_type, item["title"])

    meta_dict = {
        "id": item_id,
        "type": target_type,
        "name": item["title"],
        "year": item["year"],
        "poster": safe_poster,
        "background": item["backdrop"],
        "description": item["overview"],
        "genres": [g.strip() for g in (item["genres"] or "").split(",") if g.strip()],
        "imdbRating": str(item["rating"]) if item["rating"] else None
    }

    if target_type == "series" and "videos" in item:
        meta_dict["videos"] = item["videos"]

    return {"meta": meta_dict}

@app.get("/stream/{media_type}/{item_id}.json")
async def get_stream(media_type: str, item_id: str):
    target_type = "series" if media_type == "series" else "movie"
    videos = await get_streams_by_id(item_id, target_type)

    if not videos:
        return {"streams": []}

    streams = []
    for v in videos:
        safe_name = quote(v["file_name"])
        play_url = f"{PUBLIC_URL}/play/{v['chat_id']}/{v['message_id']}/{safe_name}"

        badge = v.get("stream_badge") or f"[Telegram ⚡] {v.get('resolution') or 'HD'}"
        desc = v.get("stream_desc")
        if not desc:
            size_gb = round(v["file_size"] / (1024 ** 3), 2)
            desc = f"📁 {v['file_name']}\n📦 {size_gb} GB"

        streams.append({
            "name": badge,
            "title": desc,
            "url": play_url
        })

    return {"streams": streams}

# HEAD probe endpoint for players checking file size & range support
@app.head("/play/{chat_id}/{message_id}/{filename}")
async def probe_stream(chat_id: int, message_id: int, filename: str):
    video = await get_video_by_location(chat_id, message_id)
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    headers = {
        "Content-Type": video["mime_type"],
        "Content-Length": str(video["file_size"]),
        "Accept-Ranges": "bytes",
    }
    return Response(status_code=200, headers=headers)

# HTTP 206 Partial Content Streamer for ExoPlayer / MPV / VLC
@app.get("/play/{chat_id}/{message_id}/{filename}")
async def play_stream(chat_id: int, message_id: int, filename: str, request: Request):
    video = await get_video_by_location(chat_id, message_id)
    if not video:
        raise HTTPException(status_code=404, detail="Video not found in library")

    file_size = video["file_size"]
    range_header = request.headers.get("range")

    if not range_header:
        headers = {
            "Content-Type": video["mime_type"],
            "Content-Length": str(file_size),
            "Accept-Ranges": "bytes",
        }
        return StreamingResponse(
            stream_telegram_file(chat_id, message_id, 0, file_size),
            status_code=200,
            headers=headers
        )

    match = re.match(r"bytes=(\d+)-(\d+)?", range_header)
    if not match:
        raise HTTPException(status_code=416, detail="Invalid Range Header")

    start = int(match.group(1))
    end = int(match.group(2)) if match.group(2) else file_size - 1

    if start >= file_size:
        raise HTTPException(
            status_code=416,
            detail="Requested range outside bounds",
            headers={"Content-Range": f"bytes */{file_size}"}
        )

    end = min(end, file_size - 1)
    content_length = (end - start) + 1

    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(content_length),
        "Content-Type": video["mime_type"],
    }

    return StreamingResponse(
        stream_telegram_file(chat_id, message_id, offset=start, length=content_length),
        status_code=206,
        headers=headers
    )

@app.get("/stats")
async def show_stats():
    stats = await get_library_stats()
    return stats

@app.post("/rescan")
async def trigger_rescan():
    asyncio.create_task(clear_and_rescan())
    return {"status": "Library wipe and fresh channel rescan initiated in background"}

# ---------------------------------------------------------------------
# Downloader & Local Archive Endpoints
# ---------------------------------------------------------------------

class DownloaderConfigRequest(BaseModel):
    downloader_enabled: bool | None = None
    download_channel_ids: list[int] | None = None
    media_dir: str | None = None
    min_free_disk_gb: float | None = None
    max_concurrent_downloads: int | None = None
    auto_tmdb_identify: bool | None = None
    auto_organize: bool | None = None
    max_retries: int | None = None
    startup_recovery: bool | None = None
    history_reconciliation: bool | None = None
    reconciliation_interval_min: int | None = None
    notifications_enabled: bool | None = None
    notification_mode: str | None = None
    notification_chat_id: int | None = None

class MetadataOverrideRequest(BaseModel):
    media_type: str | None = None
    title: str | None = None
    year: int | None = None
    season: int | None = None
    episode: int | None = None
    episode_title: str | None = None
    tmdb_id: int | None = None

@app.get("/downloads", response_class=HTMLResponse)
async def downloads_dashboard_page():
    html_file = os.path.join(TEMPLATES_DIR, "downloader.html")
    if not os.path.exists(html_file):
        html_file = os.path.join(TEMPLATES_DIR, "dashboard.html")
    if os.path.exists(html_file):
        with open(html_file, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>Downloader template missing</h1>", status_code=500)

@app.get("/api/downloader/config")
async def get_downloader_config():
    s = load_settings()
    return {
        "downloader_enabled": s.get("downloader_enabled", True),
        "download_channel_ids": s.get("download_channel_ids", []),
        "media_dir": s.get("media_dir"),
        "min_free_disk_gb": s.get("min_free_disk_gb", 5.0),
        "max_concurrent_downloads": s.get("max_concurrent_downloads", 1),
        "auto_tmdb_identify": s.get("auto_tmdb_identify", True),
        "auto_organize": s.get("auto_organize", True),
        "max_retries": s.get("max_retries", 5),
        "startup_recovery": s.get("startup_recovery", True),
        "history_reconciliation": s.get("history_reconciliation", True),
        "reconciliation_interval_min": s.get("reconciliation_interval_min", 30),
        "notifications_enabled": s.get("notifications_enabled", True),
        "notification_mode": s.get("notification_mode", "reply"),
        "notification_chat_id": s.get("notification_chat_id")
    }

@app.post("/api/downloader/config")
async def save_downloader_config(payload: DownloaderConfigRequest):
    update_data = {}
    for k, v in payload.model_dump().items():
        if v is not None:
            update_data[k] = v

    saved = save_settings(update_data)

    from telegram_client import get_client, start_telegram_service
    from downloader.service import (
        start_downloader_service,
        stop_downloader_service,
        get_downloader_worker,
        update_monitored_download_channels,
        reconcile_download_channels
    )

    client = get_client()
    dl_enabled = saved.get("downloader_enabled", False)
    dl_channels = saved.get("download_channel_ids", [])

    if dl_enabled and dl_channels:
        worker = get_downloader_worker()
        if not worker or not worker.is_running:
            if not client or not client.is_connected():
                await start_telegram_service()
                client = get_client()
            if client and client.is_connected():
                await start_downloader_service(client)
        else:
            update_monitored_download_channels(dl_channels)
            asyncio.create_task(reconcile_download_channels())
            worker.wake()
    else:
        await stop_downloader_service()

    return {"success": True, "settings": saved}

@app.get("/api/downloader/stats")
async def get_downloader_stats_api():
    from downloader.database import get_downloader_stats
    from downloader.service import get_downloader_worker
    import shutil

    s = load_settings()
    media_dir = s.get("media_dir") or os.path.join(s.get("data_dir", "data"), "media")
    os.makedirs(media_dir, exist_ok=True)

    counts = await get_downloader_stats()

    # Disk usage
    disk_info = None
    try:
        usage = shutil.disk_usage(media_dir)
        total_gb = round(usage.total / (1024 ** 3), 2)
        free_gb = round(usage.free / (1024 ** 3), 2)
        used_gb = round((usage.total - usage.free) / (1024 ** 3), 2)
        pct = round((usage.total - usage.free) / usage.total * 100, 1) if usage.total > 0 else 0
        disk_info = {
            "total_gb": total_gb,
            "free_gb": free_gb,
            "used_gb": used_gb,
            "used_percent": pct
        }
    except Exception as e:
        print(f"[Stats Warning] Disk check: {e}")

    # Active job / transfer
    worker = get_downloader_worker()
    active_transfer = worker.get_active_transfer() if worker else None

    return {
        "counts": counts,
        "disk": disk_info,
        "active_transfer": active_transfer,
        "active_job": active_transfer
    }

@app.get("/api/downloader/queue")
async def get_downloader_queue_api():
    from downloader.database import get_all_jobs
    from downloader.models import DownloadStatus
    from downloader.service import get_downloader_worker

    active_statuses = (
        DownloadStatus.DOWNLOADING.value,
        DownloadStatus.VALIDATING.value,
        DownloadStatus.QUEUED.value,
        DownloadStatus.PAUSED_NETWORK.value,
        DownloadStatus.PAUSED_DISK.value,
        DownloadStatus.VERIFYING.value,
        DownloadStatus.IDENTIFYING.value,
        DownloadStatus.ORGANIZING.value,
        DownloadStatus.RETRY_PENDING.value,
    )
    all_jobs = await get_all_jobs(limit=200)
    worker = get_downloader_worker()
    active_transfer = worker.get_active_transfer() if worker else None

    queue_jobs = []
    for j in all_jobs:
        if j.status in active_statuses:
            d = j.to_dict()
            if active_transfer and (active_transfer.get("job_id") == j.id or active_transfer.get("id") == j.id):
                d["bytes_downloaded"] = active_transfer.get("bytes_downloaded", d.get("bytes_downloaded", 0))
                d["downloaded_bytes"] = d["bytes_downloaded"]
                d["download_speed"] = active_transfer.get("download_speed", 0.0)
                d["speed_bytes_sec"] = d["download_speed"]
                d["progress_pct"] = active_transfer.get("progress_pct", 0.0)
                d["eta_seconds"] = active_transfer.get("eta_seconds", 0)
                d["status"] = active_transfer.get("status", d.get("status"))
            else:
                dl_bytes = d.get("bytes_downloaded", 0)
                fsize = d.get("file_size", 1) or 1
                pct = round((dl_bytes / fsize) * 100, 1)
                d["downloaded_bytes"] = dl_bytes
                d["speed_bytes_sec"] = d.get("download_speed", 0.0)
                d["progress_pct"] = pct
                d["eta_seconds"] = 0
            queue_jobs.append(d)

    return {"jobs": queue_jobs}

@app.get("/api/downloader/history")
async def get_downloader_history_api():
    from downloader.database import get_all_jobs
    from downloader.models import DownloadStatus

    history_statuses = (
        DownloadStatus.COMPLETED.value,
        DownloadStatus.FAILED.value,
        DownloadStatus.SOURCE_DELETED.value,
        DownloadStatus.SKIPPED.value,
    )
    all_jobs = await get_all_jobs(limit=200)
    history_jobs = [j.to_dict() for j in all_jobs if j.status in history_statuses]
    return {"jobs": history_jobs, "history": history_jobs}

@app.post("/api/downloader/job/{job_id}/pause")
async def pause_download_job_api(job_id: int):
    from downloader.database import pause_job
    from downloader.service import get_downloader_worker
    worker = get_downloader_worker()
    if worker and worker.engine:
        if worker.get_active_job() and worker.get_active_job().id == job_id:
            worker.engine.cancel_active_download()
    await pause_job(job_id)
    return {"success": True}

@app.post("/api/downloader/job/{job_id}/resume")
async def resume_download_job_api(job_id: int):
    from downloader.database import resume_job
    await resume_job(job_id)
    return {"success": True}

@app.post("/api/downloader/job/{job_id}/retry")
async def retry_download_job_api(job_id: int):
    from downloader.database import retry_job
    await retry_job(job_id)
    return {"success": True}

@app.post("/api/downloader/job/{job_id}/skip")
async def skip_download_job_api(job_id: int):
    from downloader.database import skip_job
    from downloader.service import get_downloader_worker
    worker = get_downloader_worker()
    if worker and worker.engine:
        if worker.get_active_job() and worker.get_active_job().id == job_id:
            worker.engine.cancel_active_download()
    await skip_job(job_id)
    return {"success": True}

@app.delete("/api/downloader/job/{job_id}")
async def delete_download_job_api(job_id: int):
    from downloader.database import delete_job
    from downloader.service import get_downloader_worker
    worker = get_downloader_worker()
    if worker and worker.engine:
        if worker.get_active_job() and worker.get_active_job().id == job_id:
            worker.engine.cancel_active_download()
    await delete_job(job_id)
    return {"success": True}

@app.post("/api/downloader/job/{job_id}/metadata")
async def override_job_metadata_api(job_id: int, payload: MetadataOverrideRequest):
    import json
    from downloader.database import get_job_by_id, update_job_metadata
    job = await get_job_by_id(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    existing_meta = {}
    if job.metadata_json:
        try:
            existing_meta = json.loads(job.metadata_json)
        except Exception:
            pass

    for k, v in payload.model_dump().items():
        if v is not None:
            existing_meta[k] = v

    await update_job_metadata(
        job_id=job_id,
        media_type=existing_meta.get("media_type") or job.media_type,
        identified_title=existing_meta.get("title") or job.identified_title,
        identified_year=existing_meta.get("year") or job.identified_year,
        identified_season=existing_meta.get("season") or job.identified_season,
        identified_episode=existing_meta.get("episode") or job.identified_episode,
        identified_episode_title=existing_meta.get("episode_title") or job.identified_episode_title,
        tmdb_id=existing_meta.get("tmdb_id") or job.tmdb_id,
        imdb_id=existing_meta.get("imdb_id") or job.imdb_id,
        metadata_json=json.dumps(existing_meta)
    )
    return {"success": True}

@app.post("/api/downloader/job/{job_id}/reidentify")
async def reidentify_job_metadata_api(job_id: int):
    from downloader.database import get_job_by_id
    from downloader.organizer import MediaOrganizer
    job = await get_job_by_id(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    s = load_settings()
    media_dir = s.get("media_dir") or os.path.join(s.get("data_dir", "data"), "media")
    organizer = MediaOrganizer(media_dir)
    meta = await organizer.identify_metadata(job, auto_tmdb=True)
    return {"success": True, "metadata": meta}

@app.post("/api/downloader/reconcile")
async def trigger_downloader_reconcile_api():
    s = load_settings()
    if not s.get("bot_token"):
        return {
            "success": False,
            "discovered": 0,
            "status": "Telegram Bot Token is not configured. Please set your credentials in Settings first."
        }
    if not s.get("download_channel_ids"):
        return {
            "success": False,
            "discovered": 0,
            "status": "No download channels configured. Please select at least one channel in Downloader Settings."
        }

    from telegram_client import get_client, start_telegram_service
    from downloader.service import (
        start_downloader_service,
        get_downloader_worker,
        reconcile_download_channels
    )
    client = get_client()
    if not client or not client.is_connected():
        await start_telegram_service()
        client = get_client()
    worker = get_downloader_worker()
    if not worker or not worker.is_running:
        if client and client.is_connected():
            await start_downloader_service(client)

    discovered = await reconcile_download_channels()
    return {"success": True, "discovered": discovered, "status": f"Discovered and enqueued {discovered} media post(s)."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=SERVER_HOST, port=SERVER_PORT, reload=False)
