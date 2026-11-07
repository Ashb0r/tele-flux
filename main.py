import os
import re
import asyncio
from contextlib import asynccontextmanager
from urllib.parse import quote
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from config import PUBLIC_URL, SERVER_HOST, SERVER_PORT, ADULT_ONLY
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
            content = f.read().replace("__ADULT_ONLY__", "true" if ADULT_ONLY else "false")
            return HTMLResponse(content=content)
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
            content = f.read().replace("__ADULT_ONLY__", "true" if ADULT_ONLY else "false")
            return HTMLResponse(content=content)
    return HTMLResponse("<h1>Stremio template missing</h1>", status_code=500)

@app.get("/api/config")
async def get_config_api():
    s = load_settings()
    return {
        "configured": s.get("configured", False),
        "adult_only": ADULT_ONLY,
        "bot_token": s.get("bot_token", ""),
        "login_mode": s.get("login_mode", "bot"),
        "session_string": s.get("session_string", ""),
        "tmdb_api_key": s.get("tmdb_api_key", ""),
        "stashdb_api_key": s.get("stashdb_api_key", ""),
        "channel_ids": s.get("channel_ids", []),
        "public_url": PUBLIC_URL
    }

class SaveSettingsRequest(BaseModel):
    api_id: int | None = None
    api_hash: str | None = None
    bot_token: str | None = ""
    login_mode: str | None = "bot"
    session_string: str | None = ""
    tmdb_api_key: str | None = ""
    stashdb_api_key: str | None = ""

@app.post("/api/settings")
async def save_settings_api(payload: SaveSettingsRequest):
    current = load_settings()
    api_id = payload.api_id or current.get("api_id")
    api_hash = (payload.api_hash or "").strip() or current.get("api_hash", "")

    if not api_id or not api_hash:
        raise HTTPException(
            status_code=400,
            detail="Telegram API ID and API Hash must be set in your .env file."
        )

    if payload.login_mode == "user" and not (payload.session_string or "").strip():
        raise HTTPException(status_code=400, detail="Session string is required for user login mode.")
    if payload.login_mode != "user" and not ((payload.bot_token or "").strip() or current.get("bot_token", "")):
        raise HTTPException(status_code=400, detail="Bot token is required for bot login mode.")

    bot_token = (payload.bot_token or "").strip() or current.get("bot_token", "")

    save_data = {
        "api_id": int(api_id),
        "api_hash": api_hash,
        "bot_token": bot_token,
        "login_mode": payload.login_mode or "bot",
        "session_string": (payload.session_string or "").strip(),
        "tmdb_api_key": (payload.tmdb_api_key or "").strip(),
        "stashdb_api_key": (payload.stashdb_api_key or "").strip()
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

class VerifyStashdbRequest(BaseModel):
    api_key: str

@app.post("/api/verify-stashdb")
async def verify_stashdb_api(payload: VerifyStashdbRequest):
    key = payload.api_key.strip()
    if not key:
        return {"valid": False, "message": "API Key cannot be empty."}
    from stashdb import verify_stashdb_key
    return await verify_stashdb_key(key)

class SaveConfigRequest(BaseModel):
    api_id: int | None = None
    api_hash: str | None = None
    bot_token: str
    tmdb_api_key: str | None = ""
    stashdb_api_key: str | None = ""
    channel_ids: list[int] | None = None

@app.post("/api/save-config")
async def save_config_api(payload: SaveConfigRequest):
    current = load_settings()
    api_id = payload.api_id or current.get("api_id")
    api_hash = (payload.api_hash or "").strip() or current.get("api_hash", "")
    if not api_id or not api_hash or not payload.bot_token:
        raise HTTPException(
            status_code=400,
            detail="Missing required credentials (Telegram API ID and Hash must be set in .env)."
        )

    # Save to persistent settings.json
    save_data = {
        "api_id": int(api_id),
        "api_hash": api_hash,
        "bot_token": payload.bot_token.strip(),
        "tmdb_api_key": (payload.tmdb_api_key or "").strip(),
        "stashdb_api_key": (payload.stashdb_api_key or "").strip()
    }
    if payload.channel_ids is not None:
        save_data["channel_ids"] = payload.channel_ids

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

    types = []
    id_prefixes = []
    catalogs = []

    if ADULT_ONLY:
        types = ["porn"]
        id_prefixes = ["stash_"]
        catalogs = [
            {
                "type": "porn",
                "id": "tg_adult",
                "name": "Telegram Adult Vault",
                "extra": [{"name": "skip", "isRequired": False}]
            }
        ]
        addon_name = "Telegram Adult Vault"
    else:
        types = ["movie", "series"]
        id_prefixes = ["tt", "tmdb:"]
        catalogs = [
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
        ]
        addon_name = "Telegram Library TV"

    resources = []
    if conf:
        if catalogs:
            resources.append("catalog")
        resources.append({"name": "meta", "types": types, "idPrefixes": id_prefixes})
        resources.append({"name": "stream", "types": types, "idPrefixes": id_prefixes})

    return {
        "id": "org.homelab.telegram.streams",
        "version": "1.3.0",
        "name": addon_name,
        "description": "Stream high-definition videos directly from your Telegram channels without buffering",
        "resources": resources,
        "types": types if conf else [],
        "catalogs": catalogs if conf else [],
        "idPrefixes": id_prefixes,
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
    if ADULT_ONLY or media_type == "porn":
        from stashdb import generate_adult_poster_svg
        svg_data = generate_adult_poster_svg(title)
    else:
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

    if ADULT_ONLY or media_type == "porn" or catalog_id == "tg_adult":
        target_type = "porn"
    elif media_type == "series" or catalog_id == "tg_series":
        target_type = "series"
    else:
        target_type = "movie"

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
    if ADULT_ONLY or media_type == "porn" or item_id.startswith("stash_") or item_id.startswith("stash:"):
        target_type = "porn"
    elif media_type == "series":
        target_type = "series"
    else:
        target_type = "movie"

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
async def get_stream(media_type: str, item_id: str, request: Request):
    if ADULT_ONLY or media_type == "porn" or item_id.startswith("stash_") or item_id.startswith("stash:"):
        target_type = "porn"
    elif media_type == "series":
        target_type = "series"
    else:
        target_type = "movie"

    videos = await get_streams_by_id(item_id, target_type)

    if not videos:
        return {"streams": []}

    # Dynamically determine the base URL so mobile devices on Wi-Fi/LAN can reach it!
    host = request.headers.get("x-forwarded-host") or request.headers.get("host")
    if host and "127.0.0.1" not in host and "localhost" not in host:
        proto = request.headers.get("x-forwarded-proto", request.url.scheme)
        base_url = f"{proto}://{host}".rstrip("/")
    elif PUBLIC_URL and "127.0.0.1" not in PUBLIC_URL and "localhost" not in PUBLIC_URL:
        base_url = PUBLIC_URL.rstrip("/")
    else:
        base_url = f"http://192.168.1.16:{SERVER_PORT}"

    streams = []
    for v in videos:
        safe_name = quote(v["file_name"])
        play_url = f"{base_url}/play/{v['chat_id']}/{v['message_id']}/{safe_name}"

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

# OPTIONS pre-flight handler for mobile players (ExoPlayer / VLC / Stremio Mobile)
@app.options("/play/{chat_id}/{message_id}/{filename}")
async def options_stream(chat_id: int, message_id: int, filename: str):
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
        "Access-Control-Max-Age": "86400",
    }
    return Response(status_code=200, headers=headers)

# HEAD probe endpoint for players checking file size & range support
@app.head("/play/{chat_id}/{message_id}/{filename}")
async def probe_stream(chat_id: int, message_id: int, filename: str):
    video = await get_video_by_location(chat_id, message_id)
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    headers = {
        "Content-Type": video["mime_type"] or "video/mp4",
        "Content-Length": str(video["file_size"]),
        "Accept-Ranges": "bytes",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
    }
    return Response(status_code=200, headers=headers)

# HTTP 206 Partial Content Streamer for ExoPlayer / MPV / VLC / Stremio
@app.get("/play/{chat_id}/{message_id}/{filename}")
async def play_stream(chat_id: int, message_id: int, filename: str, request: Request):
    video = await get_video_by_location(chat_id, message_id)
    if not video:
        raise HTTPException(status_code=404, detail="Video not found in library")

    from streaming_engine import streaming_engine
    from config import STREAM_MAX_ACTIVE_STREAMS
    if streaming_engine.active_stream_count() >= STREAM_MAX_ACTIVE_STREAMS:
        raise HTTPException(
            status_code=503,
            detail=f"Server busy: maximum active playback sessions reached ({STREAM_MAX_ACTIVE_STREAMS})",
            headers={"Retry-After": "5"}
        )

    file_size = video["file_size"]
    range_header = request.headers.get("range")

    if not range_header:
        headers = {
            "Content-Type": video["mime_type"] or "video/mp4",
            "Content-Length": str(file_size),
            "Accept-Ranges": "bytes",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
        }
        return StreamingResponse(
            stream_telegram_file(chat_id, message_id, 0, file_size),
            status_code=200,
            headers=headers
        )

    # Standard RFC 7233 HTTP Range parsing (supports standard, open-ended, and suffix ranges)
    range_match = re.match(r"^bytes=(\d*)-(\d*)$", range_header.strip())
    if not range_match:
        raise HTTPException(
            status_code=416,
            detail="Invalid Range Header",
            headers={"Content-Range": f"bytes */{file_size}"}
        )

    start_str, end_str = range_match.groups()
    if not start_str and not end_str:
        raise HTTPException(
            status_code=416,
            detail="Invalid Range Header",
            headers={"Content-Range": f"bytes */{file_size}"}
        )

    if not start_str:
        # Suffix byte range: e.g. bytes=-1048576 (last 1MB of file for trailer atom probes)
        suffix_length = int(end_str)
        if suffix_length <= 0:
            raise HTTPException(
                status_code=416,
                detail="Invalid suffix range length",
                headers={"Content-Range": f"bytes */{file_size}"}
            )
        start = max(0, file_size - suffix_length)
        end = file_size - 1
    elif not end_str:
        # Open-ended range: e.g. bytes=1048576- (to end of file)
        start = int(start_str)
        end = file_size - 1
    else:
        # Explicit range: e.g. bytes=0-1048575
        start = int(start_str)
        end = int(end_str)

    # Validate range boundaries
    if start >= file_size or start > end or start < 0:
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
        "Content-Type": video["mime_type"] or "video/mp4",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
        "Connection": "keep-alive"
    }

    return StreamingResponse(
        stream_telegram_file(chat_id, message_id, offset=start, length=content_length),
        status_code=206,
        headers=headers
    )

@app.get("/stats")
async def show_stats():
    stats = await get_library_stats()
    from streaming_engine import get_streaming_stats
    stats["streaming"] = get_streaming_stats()
    return stats

@app.get("/api/streaming/stats")
async def get_streaming_stats_api():
    from streaming_engine import get_streaming_stats
    return get_streaming_stats()

@app.post("/rescan")
async def trigger_rescan():
    asyncio.create_task(clear_and_rescan())
    return {"status": "Library wipe and fresh channel rescan initiated in background"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=SERVER_HOST, port=SERVER_PORT, reload=False)
