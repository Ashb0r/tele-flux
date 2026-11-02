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

    # If already configured from previous run, start Telegram service right away
    if is_configured():
        print("[Telegram] Found existing configuration. Connecting to Telegram...")
        await start_telegram_service()
    else:
        print("[Notice] Setup required! Open the Web UI in your browser to configure:")
        print(f"         👉 {PUBLIC_URL}/configure")

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
    return await configure_page()

@app.get("/configure", response_class=HTMLResponse)
async def configure_page():
    html_file = os.path.join(TEMPLATES_DIR, "configure.html")
    if os.path.exists(html_file):
        with open(html_file, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>Configuration template missing</h1>", status_code=500)

@app.get("/api/config")
async def get_config_api():
    s = load_settings()
    return {
        "configured": s.get("configured", False),
        "api_id": s.get("api_id"),
        "api_hash": s.get("api_hash", ""),
        "bot_token": s.get("bot_token", ""),
        "tmdb_api_key": s.get("tmdb_api_key", ""),
        "channel_ids": s.get("channel_ids", []),
        "public_url": PUBLIC_URL
    }

class FetchChatsRequest(BaseModel):
    bot_token: str

@app.post("/api/fetch-chats")
async def fetch_chats_api(payload: FetchChatsRequest):
    res = await discover_bot_chats(payload.bot_token)
    return res

class LookupChatRequest(BaseModel):
    bot_token: str
    chat_identifier: str

@app.post("/api/lookup-chat")
async def lookup_chat_api(payload: LookupChatRequest):
    res = await lookup_single_chat(payload.bot_token, payload.chat_identifier)
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
    channel_ids: list[int]

@app.post("/api/save-config")
async def save_config_api(payload: SaveConfigRequest):
    if not payload.api_id or not payload.api_hash or not payload.bot_token:
        raise HTTPException(status_code=400, detail="Missing required credentials.")
    if not payload.channel_ids:
        raise HTTPException(status_code=400, detail="Must select at least one channel.")

    # Save to persistent settings.json
    save_settings({
        "api_id": payload.api_id,
        "api_hash": payload.api_hash.strip(),
        "bot_token": payload.bot_token.strip(),
        "tmdb_api_key": (payload.tmdb_api_key or "").strip(),
        "channel_ids": payload.channel_ids
    })

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
        metas.append({
            "id": item["stremio_id"],
            "type": target_type,
            "name": item["title"],
            "poster": item["poster"],
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

    meta_dict = {
        "id": item_id,
        "type": target_type,
        "name": item["title"],
        "year": item["year"],
        "poster": item["poster"],
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
        size_gb = round(v["file_size"] / (1024 ** 3), 2)
        safe_name = quote(v["file_name"])
        play_url = f"{PUBLIC_URL}/play/{v['chat_id']}/{v['message_id']}/{safe_name}"

        streams.append({
            "name": "[Telegram]",
            "title": f"{v['file_name']}\n💾 {size_gb} GB",
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=SERVER_HOST, port=SERVER_PORT, reload=False)
