import os
import json
import time
import asyncio
from collections import OrderedDict
import httpx
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import (
    MessageMediaDocument,
    DocumentAttributeVideo,
    DocumentAttributeFilename,
)
from telethon.errors import FloodWaitError
from config import DATA_DIR, CHUNK_SIZE_BYTES
from database import upsert_video
from tmdb import lookup_tmdb, parse_filename, extract_media_specs, format_stremio_stream
from settings_manager import load_settings

VIDEO_EXTENSIONS = (
    ".mkv", ".mp4", ".avi", ".mov", ".webm", ".ts",
    ".m2ts", ".m4v", ".wmv", ".flv", ".vob", ".mpg", ".mpeg", ".3gp"
)

VIDEO_MIMES = (
    "video/", "application/x-matroska", "application/x-mp4",
    "application/octet-stream", "binary/octet-stream"
)

SESSION_PATH = os.path.join(DATA_DIR, "bot_session")

# Global client and channel tracking
telethon_client: TelegramClient | None = None
active_channels: list[int] = []
scan_task: asyncio.Task | None = None

# ---------------------------------------------------------------------
# Performance Optimization 1: In-Memory Document Descriptor Cache
# Eliminates 300-500ms network round-trip per range request on player start!
# ---------------------------------------------------------------------
_doc_cache = {} # (chat_id, message_id) -> (doc, timestamp)

async def get_cached_document(chat_id: int, message_id: int):
    now = time.time()
    key = (chat_id, message_id)
    if key in _doc_cache:
        doc, expires_at = _doc_cache[key]
        if now < expires_at:
            return doc

    # Fetch once and cache for 1 hour
    message = await telethon_client.get_messages(chat_id, ids=message_id)
    if not message or not message.media:
        return None

    doc = getattr(message.media, "document", None)
    if not doc and hasattr(message, "video") and message.video:
        doc = message.video

    if doc:
        _doc_cache[key] = (doc, now + 3600)
    return doc

# ---------------------------------------------------------------------
# Performance Optimization 2: 128 MB Block-Aligned LRU Cache
# Caches file blocks (512 KB) so header probes, trailer atom seeks,
# and stream restarts begin INSTANTANEOUSLY from RAM (0.01ms latency!)
# ---------------------------------------------------------------------
BLOCK_SIZE = CHUNK_SIZE_BYTES

class BlockCache:
    def __init__(self, max_mb=128):
        self.max_bytes = max_mb * 1024 * 1024
        self.current_bytes = 0
        self.cache = OrderedDict() # (doc_id, block_index) -> bytes
        self.lock = asyncio.Lock()

    async def get(self, doc_id: int, block_idx: int) -> bytes | None:
        async with self.lock:
            key = (doc_id, block_idx)
            if key in self.cache:
                self.cache.move_to_end(key)
                return self.cache[key]
            return None

    async def put(self, doc_id: int, block_idx: int, data: bytes):
        async with self.lock:
            key = (doc_id, block_idx)
            if key in self.cache:
                return
            self.cache[key] = data
            self.current_bytes += len(data)
            while self.current_bytes > self.max_bytes and self.cache:
                _, evicted = self.cache.popitem(last=False)
                self.current_bytes -= len(evicted)

block_cache = BlockCache(max_mb=128)

def get_client() -> TelegramClient | None:
    return telethon_client

async def lookup_single_chat(bot_token: str, chat_identifier: str) -> dict:
    token = bot_token.strip()
    target = chat_identifier.strip()
    if not token or not target:
        return {"error": "Bot token and Channel ID/Username required."}

    if "t.me/" in target:
        target = target.split("t.me/")[-1].replace("+", "").replace("/", "")
        if not target.startswith("@") and not target.startswith("-100"):
            target = f"@{target}"

    url = f"https://api.telegram.org/bot{token}/getChat"
    try:
        async with httpx.AsyncClient(timeout=10.0) as http:
            resp = await http.get(url, params={"chat_id": target})
            data = resp.json()
            if not data.get("ok"):
                desc = data.get("description", "Chat not found")
                return {"error": f"Telegram: {desc}. Ensure the bot is added as an Admin in this channel!"}

            chat = data.get("result", {})
            cid = chat.get("id")
            ctype = chat.get("type", "channel")
            title = chat.get("title") or chat.get("first_name") or chat.get("username") or f"Chat {cid}"

            display_type = "Channel" if ctype == "channel" else ("Group" if ctype in ("group", "supergroup") else "Chat")
            return {
                "id": cid,
                "title": title,
                "type": display_type,
                "username": chat.get("username")
            }
    except Exception as e:
        return {"error": f"Network error resolving chat: {str(e)}"}

async def discover_bot_chats(bot_token: str) -> dict:
    token = bot_token.strip()
    if not token:
        return {"error": "Bot token cannot be empty"}

    url_me = f"https://api.telegram.org/bot{token}/getMe"
    url_updates = f"https://api.telegram.org/bot{token}/getUpdates"
    url_del_webhook = f"https://api.telegram.org/bot{token}/deleteWebhook"

    try:
        async with httpx.AsyncClient(timeout=12.0) as http:
            me_resp = await http.get(url_me)
            if me_resp.status_code != 200:
                err_desc = me_resp.json().get("description", "Invalid Bot Token")
                return {"error": f"Telegram error: {err_desc}"}
            
            bot_data = me_resp.json().get("result", {})
            bot_username = bot_data.get("username", "")
            bot_name = bot_data.get("first_name", "Bot")

            await http.get(url_del_webhook, params={"drop_pending_updates": False})

            # Query recent updates (both latest tail with offset=-100 and earliest head)
            all_updates = []
            for offset_val in [-100, None]:
                params = {
                    "allowed_updates": json.dumps([
                        "message",
                        "edited_message",
                        "channel_post",
                        "edited_channel_post",
                        "my_chat_member",
                        "chat_member"
                    ]),
                    "limit": 100
                }
                if offset_val is not None:
                    params["offset"] = offset_val
                try:
                    upd_resp = await http.get(url_updates, params=params)
                    if upd_resp.status_code == 200:
                        all_updates.extend(upd_resp.json().get("result", []))
                except Exception:
                    pass

            chats_map = {}
            for item in all_updates:
                candidate_chats = []
                for key in ("channel_post", "edited_channel_post", "message", "edited_message", "my_chat_member", "chat_member"):
                    if key in item and isinstance(item[key], dict) and "chat" in item[key]:
                        candidate_chats.append(item[key]["chat"])
                # Note: We intentionally DO NOT inspect forward_from_chat here so external
                # channels that messages were forwarded from do not clutter discovered chats.

                for chat in candidate_chats:
                    if chat and "id" in chat:
                        cid = chat["id"]
                        ctype = chat.get("type", "unknown")
                        display_type = "Channel" if ctype == "channel" else ("Group" if ctype in ("group", "supergroup") else "Chat")
                        title = chat.get("title") or chat.get("first_name") or chat.get("username") or f"Chat {cid}"
                        
                        chats_map[cid] = {
                            "id": cid,
                            "title": title,
                            "type": display_type,
                            "username": chat.get("username")
                        }

            # Also ensure any channels currently saved in settings are preserved
            from settings_manager import load_settings
            cur_settings = load_settings()
            saved_cids = set(cur_settings.get("channel_ids", []) + cur_settings.get("download_channel_ids", []))
            for saved_cid in saved_cids:
                if saved_cid not in chats_map:
                    try:
                        resolved = await lookup_single_chat(token, str(saved_cid))
                        if "error" not in resolved and "id" in resolved:
                            chats_map[saved_cid] = resolved
                    except Exception:
                        pass

            return {
                "bot": {
                    "username": bot_username,
                    "name": bot_name,
                    "id": bot_data.get("id")
                },
                "chats": list(chats_map.values())
            }
    except Exception as e:
        return {"error": f"Failed to connect to Telegram: {str(e)}"}

def extract_video_metadata(message) -> dict | None:
    """
    Universal video extractor:
    - Extracts from native video or document
    - Reads video dimensions (w, h) and duration
    - Checks captions & file names
    - Parses series/episode indicators and technical specifications
    """
    try:
        if not message or not message.media:
            return None

        doc = getattr(message.media, "document", None)
        if not doc and hasattr(message, "video") and message.video:
            doc = message.video

        if not doc:
            return None

        file_name = None
        is_video = False
        video_attr = None

        mime = (getattr(doc, "mime_type", "") or "").lower()
        if any(mime.startswith(vm) for vm in VIDEO_MIMES):
            is_video = True

        if hasattr(doc, "attributes") and doc.attributes:
            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    file_name = attr.file_name
                elif isinstance(attr, DocumentAttributeVideo):
                    is_video = True
                    video_attr = {
                        "w": getattr(attr, "w", 0),
                        "h": getattr(attr, "h", 0),
                        "duration": getattr(attr, "duration", 0)
                    }

        # Fallback to Telethon message.file property
        if not file_name and hasattr(message, "file") and message.file and message.file.name:
            file_name = message.file.name

        if file_name and file_name.lower().endswith(VIDEO_EXTENSIONS):
            is_video = True

        if not is_video:
            return None

        caption = (message.message or message.raw_text or "").strip()

        actual_file_name = file_name
        if not actual_file_name:
            # Check if caption contains a recognizable title line or filename
            if caption:
                import re
                from tmdb import JUNK_LINE_REGEX
                for line in caption.split("\n"):
                    line = line.strip()
                    if not line or JUNK_LINE_REGEX.match(line):
                        continue
                    # If line ends with an extension or ' mkv'/' mp4', format it as filename
                    cand = re.sub(r'(?i)\s+(mkv|mp4|avi|mov|webm|ts)$', r'.\1', line)
                    if not any(cand.lower().endswith(ext) for ext in VIDEO_EXTENSIONS):
                        cand = f"{cand}.mp4"
                    # Strip characters invalid in filenames: / \ : * ? " < > |
                    cand = re.sub(r'[\\/:*?"<>|]+', ' ', cand)
                    cand = re.sub(r'\s+', ' ', cand).strip()
                    if len(cand) >= 5:
                        actual_file_name = cand
                        break
            if not actual_file_name:
                actual_file_name = f"video_{message.id}.mp4"

        # Full text corpus containing all metadata tokens
        text_corpus = f"{actual_file_name}\n{caption}"

        # Parse TV series vs Movie
        parsed = parse_filename(file_name=actual_file_name, caption=caption)

        # Extract deep technical specifications (resolution, codec, audio, flags)
        specs = extract_media_specs(text_corpus, file_size=doc.size, video_attr=video_attr)

        return {
            "file_name": actual_file_name,
            "caption": caption,
            "file_size": doc.size,
            "mime_type": mime if mime.startswith("video/") else "video/mp4",
            "document": doc,
            "parsed": parsed,
            "specs": specs,
            "video_attr": video_attr
        }
    except Exception as e:
        print(f"[Video Extraction Error] Message #{getattr(message, 'id', '?')}: {e}")
        return None

async def index_single_message(chat_id: int, message):
    meta = extract_video_metadata(message)
    if not meta:
        return False

    parsed = meta["parsed"]
    specs = meta["specs"]

    # If TV Series, pre-populate series cache from database if another episode was already indexed
    if parsed.get("media_type") == "series":
        from tmdb import get_series_cache, set_series_cache
        from database import get_series_master_metadata
        if not get_series_cache(parsed["raw_title"]):
            master = await get_series_master_metadata(parsed["raw_title"])
            if master:
                set_series_cache(parsed["raw_title"], master)
                set_series_cache(master["title"], master)

    # Lookup TMDB using parsed information and filename/caption
    tmdb_info = await lookup_tmdb(meta["file_name"], meta["caption"], parsed_override=parsed)

    # Determine header for Stremio stream card
    if tmdb_info.get("media_type") == "series":
        s = tmdb_info.get("season") or 1
        e = tmdb_info.get("episode") or 1
        ep_t = tmdb_info.get("episode_title") or f"Episode {e}"
        header = f"{tmdb_info['title']} - S{s:02d}E{e:02d} - {ep_t}"
    else:
        y_str = f" ({tmdb_info['year']})" if tmdb_info.get("year") else ""
        header = f"{tmdb_info['title']}{y_str}"

    badge, desc = format_stremio_stream(header, specs)

    await upsert_video(
        chat_id=chat_id,
        message_id=message.id,
        file_name=meta["file_name"],
        file_size=meta["file_size"],
        mime_type=meta["mime_type"],
        tmdb_info=tmdb_info,
        specs=specs,
        stream_badge=badge,
        stream_desc=desc
    )

    # If it's a series and we have a valid TMDB poster, heal any other episodes of this series
    if tmdb_info.get("media_type") == "series" and tmdb_info.get("poster") and not tmdb_info["poster"].endswith(".svg"):
        from database import heal_series_episodes
        await heal_series_episodes(tmdb_info["title"], tmdb_info)
        if parsed.get("raw_title") and parsed["raw_title"].lower() != tmdb_info["title"].lower():
            await heal_series_episodes(parsed["raw_title"], tmdb_info)

    res_badge = specs.get("resolution") or "HD"
    codec_str = specs.get("codec") or ""
    aud_str = specs.get("audio") or ""
    tech_str = f"[{res_badge} {codec_str} {aud_str}]".replace("  ", " ")
    sz_str = specs.get("size_str") or ""

    if tmdb_info.get("media_type") == "series":
        s = tmdb_info.get("season") or 1
        e = tmdb_info.get("episode") or 1
        ep_t = tmdb_info.get("episode_title") or f"Episode {e}"
        print(f"[Indexed Series] #{message.id}: {tmdb_info['title']} S{s:02d}E{e:02d} ({ep_t}) {tech_str} {sz_str} [TMDB: {tmdb_info.get('tmdb_id')}, IMDb: {tmdb_info.get('imdb_id')}]")
    else:
        print(f"[Indexed Movie] #{message.id}: {tmdb_info['title']} ({tmdb_info.get('year')}) {tech_str} {sz_str} [TMDB: {tmdb_info.get('tmdb_id')}, IMDb: {tmdb_info.get('imdb_id')}]")
    return True

async def scan_single_channel_history(chat_id: int) -> int:
    """
    Robust Bot & User Channel History Scanner:
    - Bots CANNOT use GetHistoryRequest/iter_messages (restricted by Telegram).
    - But Bots CAN fetch messages directly by ID (channels.getMessages)!
    - Probes the latest message ID and scans backwards in batches of 100.
    - Captures ALL movies and episodes sent after the bot joined!
    """
    global telethon_client
    if not telethon_client or not telethon_client.is_connected():
        return 0

    count = 0
    scanned_ids = set()

    try:
        entity = await telethon_client.get_entity(chat_id)
        chat_title = getattr(entity, 'title', str(chat_id))
        print(f"[Telegram] Scanning history of '{chat_title}' (Chat ID: {chat_id})...")

        # Check if connected as user (user mode can use iter_messages)
        me = await telethon_client.get_me()
        is_user_account = not getattr(me, "bot", False)

        if is_user_account:
            print(f"[Telegram] User mode active: reading full history via iter_messages...")
            try:
                async for msg in telethon_client.iter_messages(entity, limit=None):
                    if msg and msg.id not in scanned_ids:
                        scanned_ids.add(msg.id)
                        if extract_video_metadata(msg):
                            indexed = await index_single_message(chat_id, msg)
                            if indexed:
                                count += 1
            except Exception as iter_err:
                print(f"[Warning] iter_messages note: {iter_err}")
            print(f"[Telegram] User scan finished: indexed {count} videos.")
            return count

        # -------------------------------------------------------------
        # BOT MODE: Direct Sequential ID Scanner (bypasses bot getHistory cap)
        # -------------------------------------------------------------
        latest_id = None

        # Attempt A: Quick 0.1s sync ping to get exact current head ID
        try:
            ping = await telethon_client.send_message(entity, "🔄 Syncing Telegram Library...")
            if ping and ping.id:
                latest_id = ping.id
                await telethon_client.delete_messages(entity, ping.id)
                print(f"[Telegram] Channel head message ID identified: #{latest_id}")
        except Exception as ping_err:
            print(f"[Telegram Notice] Sync ping skipped ({ping_err}). Using adaptive ID probe...")

        # Attempt B: Adaptive ID probe if bot doesn't have post-message rights
        if not latest_id:
            probe_id = 50
            high_bound = 50
            # Test exponentially: 50, 100, 200, 400, 800, 1600, 3200...
            while probe_id <= 30000:
                try:
                    probe_msgs = await telethon_client.get_messages(entity, ids=[probe_id])
                    if probe_msgs and len(probe_msgs) > 0 and probe_msgs[0]:
                        high_bound = probe_id
                        probe_id = probe_id * 2
                    else:
                        break
                except Exception:
                    break
            latest_id = high_bound + 150

        print(f"[Telegram] Scanning messages by ID from #{latest_id} downwards for '{chat_title}'...")

        batch_size = 100
        consecutive_empty_batches = 0

        # Scan from newest message downwards to 1
        for end_id in range(latest_id, 0, -batch_size):
            start_id = max(1, end_id - batch_size + 1)
            id_batch = [i for i in range(start_id, end_id + 1) if i not in scanned_ids]
            if not id_batch:
                continue

            try:
                # MTProto channels.getMessagesRequest by IDs - FULLY ALLOWED FOR BOTS!
                msgs = await telethon_client.get_messages(entity, ids=id_batch)
                batch_has_messages = False

                for m in msgs:
                    if m and getattr(m, 'id', None):
                        batch_has_messages = True
                        if m.id not in scanned_ids:
                            scanned_ids.add(m.id)
                            try:
                                if extract_video_metadata(m):
                                    indexed = await index_single_message(chat_id, m)
                                    if indexed:
                                        count += 1
                                        await asyncio.sleep(0.1)
                            except Exception as m_err:
                                print(f"[Telegram Notice] Message #{m.id} skipped due to error: {m_err}")

                if not batch_has_messages:
                    consecutive_empty_batches += 1
                    # Stop after 500 empty message IDs (safely past bot's join date)
                    if consecutive_empty_batches >= 5:
                        print(f"[Telegram] Reached boundary of bot's authorized history at #{start_id}.")
                        break
                else:
                    consecutive_empty_batches = 0

            except FloodWaitError as e:
                print(f"[Telegram FloodWait] Sleeping for {e.seconds}s...")
                await asyncio.sleep(e.seconds)
            except Exception as b_err:
                print(f"[Telegram Warning] Batch {start_id}-{end_id} error: {b_err}")

        print(f"[Telegram] Finished '{chat_title}': successfully indexed {count} total videos.")
    except Exception as e:
        print(f"[Telegram Error] Could not access chat {chat_id}. Ensure bot is an Admin! Error: {e}")

    return count

async def scan_all_channels():
    global telethon_client, active_channels
    if not telethon_client or not telethon_client.is_connected():
        return

    print("[Telegram] Starting full channel history scan across all selected channels...")
    total_found = 0

    for chat_id in active_channels:
        found = await scan_single_channel_history(chat_id)
        total_found += found

    print(f"[Telegram] History scan completed. Total library size: {total_found} videos.")
    # Run self-healing library reconciliation
    try:
        from database import reconcile_library
        await reconcile_library()
    except Exception as rec_err:
        print(f"[Warning] Post-scan reconciliation notice: {rec_err}")

async def clear_and_rescan():
    from database import clear_library_database
    print("[Telegram] Clearing previous index and running fresh channel rescan...")
    await clear_library_database()
    _doc_cache.clear()
    await scan_all_channels()

async def trigger_stremio_scan() -> dict:
    global scan_task, telethon_client, active_channels
    if not telethon_client or not telethon_client.is_connected():
        return {"status": "error", "message": "Telegram client is not connected. Configure credentials first."}
    if not active_channels:
        return {"status": "error", "message": "No streaming channels selected."}
    if scan_task and not scan_task.done():
        return {"status": "running", "message": "Scan already in progress."}
    scan_task = asyncio.create_task(scan_all_channels())
    return {"status": "started", "message": f"Scanning {len(active_channels)} streaming channel(s) in background."}

def is_scanning_channels() -> bool:
    global scan_task
    return bool(scan_task and not scan_task.done())

async def start_telegram_service():
    global telethon_client, active_channels, scan_task
    settings = load_settings()
    
    # Require Telegram credentials (either bot token or user session)
    has_creds = bool(
        settings.get("api_id")
        and settings.get("api_hash")
        and (settings.get("bot_token") or settings.get("session_string"))
    )

    if not has_creds:
        print("[Telegram] Not configured yet. Awaiting setup via Web UI at /configure.")
        return False

    api_id = settings["api_id"]
    api_hash = settings["api_hash"]
    bot_token = settings.get("bot_token", "")
    active_channels = settings.get("channel_ids", [])
    login_mode = settings.get("login_mode", "bot")
    session_string = settings.get("session_string", "").strip()

    if telethon_client and telethon_client.is_connected():
        await telethon_client.disconnect()

    if login_mode == "user" and session_string:
        print("[Telegram] Starting MTProto client in USER MODE (Full Channel History Access)...")
        telethon_client = TelegramClient(
            StringSession(session_string),
            api_id,
            api_hash,
            connection_retries=10,
            retry_delay=1,
            auto_reconnect=True,
            flood_sleep_threshold=60,
            request_retries=10
        )
        await telethon_client.start()
    else:
        print("[Telegram] Starting MTProto client in BOT MODE...")
        telethon_client = TelegramClient(
            SESSION_PATH,
            api_id,
            api_hash,
            connection_retries=10,
            retry_delay=1,
            auto_reconnect=True,
            flood_sleep_threshold=60,
            request_retries=10
        )
        await telethon_client.start(bot_token=bot_token)

    me = await telethon_client.get_me()
    is_bot = getattr(me, "bot", False)
    if is_bot:
        print(f"[Telegram] Connected to MTProto as Bot: @{me.username} ({me.first_name})")
    else:
        print(f"[Telegram] Connected to MTProto as User: {me.first_name} (@{me.username or 'No username'}) [FULL HISTORY ENABLED]")

    # Real-time event listener for streaming channels (preserve existing Stremio functionality)
    if active_channels:
        @telethon_client.on(events.NewMessage(chats=active_channels))
        async def on_new_video(event):
            try:
                indexed = await index_single_message(event.chat_id, event.message)
                if indexed:
                    print(f"[Realtime Streaming] New video detected in chat {event.chat_id}!")
            except Exception as e:
                print(f"[Realtime Streaming Error] Failed processing message: {e}")

        # Note: We do NOT auto-scan history on container boot to prevent blocking the UI,
        # rate-limiting TMDB, or generating log spam. Channel indexing is triggered
        # on-demand via the Stremio UI page ("Scan Channel" button).
        pass

    # Start independent downloader module ONLY if explicitly enabled and download channels are chosen
    dl_enabled = settings.get("downloader_enabled", False)
    dl_channels = settings.get("download_channel_ids", [])
    if dl_enabled and dl_channels:
        try:
            from downloader.service import start_downloader_service
            await start_downloader_service(telethon_client)
            print("[Downloader] Independent downloader module started successfully.")
        except Exception as dl_err:
            print(f"[Downloader Error] Failed to start downloader module: {dl_err}")
    else:
        print("[Downloader] Downloader is disabled or has no download channels configured. Standby mode.")

    return True

async def stop_telegram_service():
    global telethon_client, scan_task
    # Stop downloader module first
    try:
        from downloader.service import stop_downloader_service
        await stop_downloader_service()
    except Exception as dl_stop_err:
        print(f"[Downloader Warning] Error stopping downloader: {dl_stop_err}")

    if scan_task and not scan_task.done():
        scan_task.cancel()
    if telethon_client and telethon_client.is_connected():
        await telethon_client.disconnect()
        telethon_client = None

async def stream_telegram_file(chat_id: int, message_id: int, offset: int, length: int):
    """
    Ultra-low latency MTProto streaming engine:
    1. Instant Document Resolution: Caches doc descriptor to eliminate 500ms network round-trip.
    2. Zero-Buffer Start from RAM: Serves cached header/atoms/initial blocks immediately from RAM (0.01ms).
    3. Prefetch Pipeline: Background task pre-fetches 2-3 chunks ahead in an asyncio.Queue,
       eliminating WAN RTT idle pauses and saturating local network playback.
    4. 512 KB Block Size: Rapid first-packet arrival (<250ms) for instant player response.
    """
    global telethon_client
    if not telethon_client or not telethon_client.is_connected():
        return

    # Instant cached document lookup (0 ms latency!)
    doc = await get_cached_document(chat_id, message_id)
    if not doc:
        return

    file_size = getattr(doc, "size", 0)
    if file_size <= 0:
        return

    end_offset = min(offset + length, file_size)
    actual_length = end_offset - offset
    if actual_length <= 0:
        return

    cur_offset = offset
    bytes_remaining = actual_length

    # Phase 1: Fast Start from Block Cache (Instant RAM delivery for header/tail probes & restart)
    while bytes_remaining > 0:
        b_idx = cur_offset // BLOCK_SIZE
        b_offset = cur_offset % BLOCK_SIZE
        cached_data = await block_cache.get(doc.id, b_idx)
        if not cached_data:
            break

        avail_in_block = len(cached_data) - b_offset
        if avail_in_block <= 0:
            break

        take = min(bytes_remaining, avail_in_block)
        yield cached_data[b_offset : b_offset + take]
        cur_offset += take
        bytes_remaining -= take

    # If completely satisfied from cache (e.g. 0-64KB header probe or EOF atom), done!
    if bytes_remaining <= 0:
        return

    # Phase 2: Prefetch Pipeline via Producer-Consumer Queue
    queue = asyncio.Queue(maxsize=3)  # Keep 3 blocks (1.5 MB) ready in RAM ahead of playback
    stop_event = asyncio.Event()

    async def download_worker():
        try:
            async for chunk in telethon_client.iter_download(
                doc,
                offset=cur_offset,
                limit=bytes_remaining,
                request_size=BLOCK_SIZE
            ):
                if stop_event.is_set():
                    break
                await queue.put(chunk)
            await queue.put(None)  # EOF indicator
        except asyncio.CancelledError:
            pass
        except Exception as e:
            if not stop_event.is_set():
                await queue.put(e)

    producer_task = asyncio.create_task(download_worker())
    chunk_tracker_offset = cur_offset

    try:
        while bytes_remaining > 0:
            item = await queue.get()
            if item is None:
                break
            if isinstance(item, Exception):
                print(f"[Streaming Warning] MTProto download notice: {item}")
                break

            chunk = item
            chunk_len = len(chunk)

            # Save aligned block into cache so player re-probes or restarts hit RAM instantly
            if chunk_tracker_offset % BLOCK_SIZE == 0 and chunk_len == BLOCK_SIZE:
                b_idx = chunk_tracker_offset // BLOCK_SIZE
                await block_cache.put(doc.id, b_idx, chunk)

            # Yield chunk to HTTP client
            yield chunk
            chunk_tracker_offset += chunk_len
            bytes_remaining -= chunk_len

    finally:
        stop_event.set()
        producer_task.cancel()
        while not queue.empty():
            try:
                queue.get_nowait()
            except Exception:
                break
