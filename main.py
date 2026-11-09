import os
import re
import asyncio
import secrets
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
    is_uuid_str,
)
from settings_manager import load_settings, save_settings, is_configured, regenerate_security_token
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
    brand = "Teleflux Red" if ADULT_ONLY else "Teleflux"
    print("=" * 60)
    print(f"  🚀 {brand} Addon Server")
    print(f"  📡 Base Public URL: {PUBLIC_URL}")
    print(f"  ⚙️ Configuration Web UI: {PUBLIC_URL}/stremio")
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

app = FastAPI(title="Teleflux Red Addon" if ADULT_ONLY else "Teleflux Addon", lifespan=lifespan)

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
    # Render main Stremio configuration dashboard
    return await stremio_page()

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
    # Backwards compatibility: render stremio dashboard
    return await stremio_page()

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
    token = s.get("security_token", "")
    manifest_url = f"{PUBLIC_URL}/{token}/manifest.json" if token else f"{PUBLIC_URL}/manifest.json"
    return {
        "configured": s.get("configured", False),
        "adult_only": ADULT_ONLY,
        "addon_name": "Teleflux Red" if ADULT_ONLY else "Teleflux",
        "bot_token": s.get("bot_token", ""),
        "login_mode": s.get("login_mode", "bot"),
        "session_string": s.get("session_string", ""),
        "tmdb_api_key": s.get("tmdb_api_key", ""),
        "stashdb_api_key": s.get("stashdb_api_key", ""),
        "channel_ids": s.get("channel_ids", []),
        "public_url": PUBLIC_URL,
        "security_token": token,
        "manifest_url": manifest_url
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
    s = load_settings()
    token = s.get("security_token", "")
    manifest_url = f"{PUBLIC_URL}/{token}/manifest.json" if token else f"{PUBLIC_URL}/manifest.json"
    return {
        "success": True,
        "channel_ids": payload.channel_ids,
        "manifest_url": manifest_url
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

class ManualCorrectionRequest(BaseModel):
    chat_id: int
    message_id: int
    title_hint: str

@app.post("/api/manual-correction")
async def manual_correction_api(payload: ManualCorrectionRequest):
    from telegram_client import apply_manual_title_correction
    res = await apply_manual_title_correction(
        chat_id=payload.chat_id,
        message_id=payload.message_id,
        title_hint=payload.title_hint
    )
    return res

@app.get("/api/title-corrections")
async def get_title_corrections_api():
    from database import get_all_corrections
    corrections = await get_all_corrections()
    return {"corrections": corrections}

@app.get("/api/stremio/stats")
async def stremio_stats_api():
    from database import get_library_stats
    from telegram_client import is_scanning_channels
    stats = await get_library_stats()
    s = load_settings()
    token = s.get("security_token", "")
    manifest_url = f"{PUBLIC_URL}/{token}/manifest.json" if token else f"{PUBLIC_URL}/manifest.json"
    return {
        "stats": stats,
        "is_scanning": is_scanning_channels(),
        "manifest_url": manifest_url,
        "security_token": token
    }

@app.post("/api/security-token/regenerate")
async def regenerate_token_api():
    new_token = regenerate_security_token()
    manifest_url = f"{PUBLIC_URL}/{new_token}/manifest.json"
    return {
        "success": True,
        "security_token": new_token,
        "manifest_url": manifest_url,
        "message": "New security token generated successfully! Please update your Stremio installation with the new URL."
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

    s = load_settings()
    token = s.get("security_token", "")
    manifest_url = f"{PUBLIC_URL}/{token}/manifest.json" if token else f"{PUBLIC_URL}/manifest.json"
    return {
        "success": True,
        "manifest_url": manifest_url
    }

# ---------------------------------------------------------------------
# Stremio Addon Protocol & Security Token Endpoints
# ---------------------------------------------------------------------

def verify_security_token(token: str):
    s = load_settings()
    expected = s.get("security_token")
    if not expected:
        return
    if not token or not secrets.compare_digest(str(token), str(expected)):
        raise HTTPException(
            status_code=403,
            detail="Forbidden: Invalid or missing security token."
        )

def build_manifest():
    conf = is_configured()

    types = []
    id_prefixes = []
    catalogs = []

    if ADULT_ONLY:
        types = ["porn", "movie"]
        id_prefixes = ["stash_", "stash:", "stashdb_", "stashdb:"]
        catalogs = [
            {
                "type": "porn",
                "id": "tg_adult",
                "name": "Teleflux Red",
                "extra": [{"name": "skip", "isRequired": False}]
            }
        ]
        addon_name = "Teleflux Red"
        addon_id = "org.teleflux.red"
        addon_desc = "Private high-definition adult streaming vault powered by Teleflux"
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
        addon_name = "Teleflux"
        addon_id = "org.teleflux.streams"
        addon_desc = "High-speed continuous Telegram media streaming bridge for Stremio"

    resources = []
    if conf:
        if catalogs:
            resources.append("catalog")
        resources.append({"name": "meta", "types": types, "idPrefixes": id_prefixes})
        resources.append({"name": "stream", "types": types, "idPrefixes": id_prefixes})

    return {
        "id": addon_id,
        "version": "2.0.0",
        "name": addon_name,
        "description": addon_desc,
        "resources": resources,
        "types": types if conf else [],
        "catalogs": catalogs if conf else [],
        "idPrefixes": id_prefixes,
        "behaviorHints": {
            "configurable": True,
            "configurationRequired": not conf
        }
    }

@app.get("/{token}/manifest.json")
async def get_secure_manifest(token: str):
    verify_security_token(token)
    return build_manifest()

@app.get("/manifest.json")
async def get_manifest_unauthorized():
    raise HTTPException(
        status_code=403,
        detail="Forbidden: Security token required. Please install using your private URL from the configuration dashboard."
    )

# ---------------------------------------------------------------------
# Dynamic SVG Poster Generator (Ensures no card is EVER blank in Stremio)
# ---------------------------------------------------------------------

@app.get("/{token}/poster/{media_type}/{title}.svg")
@app.get("/poster/{media_type}/{title}.svg")
async def get_poster_image(media_type: str, title: str, token: str | None = None):
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

def resolve_poster(poster: str | None, media_type: str, title: str, token: str = "") -> str:
    if poster and poster.strip() and poster.startswith("http"):
        return poster
    prefix = f"/{token}" if token else ""
    return f"{PUBLIC_URL}{prefix}/poster/{media_type}/{quote(title)}.svg"

# ---------------------------------------------------------------------
# Catalog Handlers
# ---------------------------------------------------------------------

async def handle_catalog(media_type: str, catalog_id: str, extra: str | None = None, token: str = ""):
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
        safe_poster = resolve_poster(item.get("poster"), target_type, item["title"], token=token)
        metas.append({
            "id": item["stremio_id"],
            "type": target_type,
            "name": item["title"],
            "poster": safe_poster,
            "releaseInfo": str(item["year"]) if item["year"] else None,
            "description": item["overview"]
        })

    return {"metas": metas}

@app.get("/{token}/catalog/{media_type}/{catalog_id}.json")
@app.get("/{token}/catalog/{media_type}/{catalog_id}/{extra}.json")
async def get_secure_catalog(token: str, media_type: str, catalog_id: str, extra: str | None = None):
    verify_security_token(token)
    return await handle_catalog(media_type, catalog_id, extra, token=token)

@app.get("/catalog/{media_type}/{catalog_id}.json")
@app.get("/catalog/{media_type}/{catalog_id}/{extra}.json")
async def get_catalog_unauthorized(media_type: str, catalog_id: str, extra: str | None = None):
    raise HTTPException(status_code=403, detail="Forbidden: Security token required.")

# ---------------------------------------------------------------------
# Metadata Handlers
# ---------------------------------------------------------------------

async def handle_meta(media_type: str, item_id: str, token: str = ""):
    if ADULT_ONLY or media_type == "porn" or item_id.startswith("stash_") or item_id.startswith("stash:") or item_id.startswith("stashdb_") or item_id.startswith("stashdb:") or is_uuid_str(item_id):
        target_type = "porn"
    elif media_type == "series":
        target_type = "series"
    else:
        target_type = "movie"

    item = await get_meta_by_id(item_id, target_type)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    safe_poster = resolve_poster(item.get("poster"), target_type, item["title"], token=token)

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

@app.get("/{token}/meta/{media_type}/{item_id}.json")
async def get_secure_meta(token: str, media_type: str, item_id: str):
    verify_security_token(token)
    return await handle_meta(media_type, item_id, token=token)

@app.get("/meta/{media_type}/{item_id}.json")
async def get_meta_unauthorized(media_type: str, item_id: str):
    raise HTTPException(status_code=403, detail="Forbidden: Security token required.")

# ---------------------------------------------------------------------
# Stream Handlers
# ---------------------------------------------------------------------

async def handle_stream(token: str, media_type: str, item_id: str, request: Request):
    if ADULT_ONLY or media_type == "porn" or item_id.startswith("stash_") or item_id.startswith("stash:") or item_id.startswith("stashdb_") or item_id.startswith("stashdb:") or is_uuid_str(item_id):
        target_type = "porn"
    elif media_type == "series":
        target_type = "series"
    else:
        target_type = "movie"

    videos = await get_streams_by_id(item_id, target_type)
    if not videos:
        return {"streams": []}

    host = request.headers.get("x-forwarded-host") or request.headers.get("host")
    if host and "127.0.0.1" not in host and "localhost" not in host:
        proto = request.headers.get("x-forwarded-proto", request.url.scheme)
        base_url = f"{proto}://{host}".rstrip("/")
    elif PUBLIC_URL and "127.0.0.1" not in PUBLIC_URL and "localhost" not in PUBLIC_URL:
        base_url = PUBLIC_URL.rstrip("/")
    else:
        base_url = f"http://192.168.1.16:{SERVER_PORT}"

    streams = []
    default_badge = "[Teleflux Red ⚡] " if ADULT_ONLY else "[Teleflux ⚡] "
    for v in videos:
        safe_name = quote(v["file_name"])
        play_url = f"{base_url}/{token}/play/{v['chat_id']}/{v['message_id']}/{safe_name}"

        badge = v.get("stream_badge") or f"{default_badge}{v.get('resolution') or 'HD'}"
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

@app.get("/{token}/stream/{media_type}/{item_id}.json")
async def get_secure_stream(token: str, media_type: str, item_id: str, request: Request):
    verify_security_token(token)
    return await handle_stream(token, media_type, item_id, request)

@app.get("/stream/{media_type}/{item_id}.json")
async def get_stream_unauthorized(media_type: str, item_id: str):
    raise HTTPException(status_code=403, detail="Forbidden: Security token required.")

# ---------------------------------------------------------------------
# Protected Video Streaming Endpoints (HTTP 206 Partial Content)
# ---------------------------------------------------------------------

async def handle_options_stream():
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
        "Access-Control-Max-Age": "86400",
    }
    return Response(status_code=200, headers=headers)

async def handle_probe_stream(chat_id: int, message_id: int, filename: str):
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

async def handle_play_stream(chat_id: int, message_id: int, filename: str, request: Request):
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

# Protected playback routes
@app.options("/{token}/play/{chat_id}/{message_id}/{filename}")
async def options_secure_stream(token: str, chat_id: int, message_id: int, filename: str):
    verify_security_token(token)
    return await handle_options_stream()

@app.head("/{token}/play/{chat_id}/{message_id}/{filename}")
async def probe_secure_stream(token: str, chat_id: int, message_id: int, filename: str):
    verify_security_token(token)
    return await handle_probe_stream(chat_id, message_id, filename)

@app.get("/{token}/play/{chat_id}/{message_id}/{filename}")
async def play_secure_stream(token: str, chat_id: int, message_id: int, filename: str, request: Request):
    verify_security_token(token)
    return await handle_play_stream(chat_id, message_id, filename, request)

# Block unauthorized direct playback without token
@app.options("/play/{chat_id}/{message_id}/{filename}")
@app.head("/play/{chat_id}/{message_id}/{filename}")
@app.get("/play/{chat_id}/{message_id}/{filename}")
async def play_unauthorized():
    raise HTTPException(
        status_code=403,
        detail="Forbidden: Security token required for video playback. Install your personalized addon manifest from the dashboard."
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
