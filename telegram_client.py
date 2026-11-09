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
from config import DATA_DIR, CHUNK_SIZE_BYTES, ADULT_ONLY
from database import upsert_video
from tmdb import lookup_tmdb, parse_filename, extract_media_specs, format_stremio_stream, resolve_tmdb_by_hint, set_series_cache
from stashdb import search_stashdb_scene, format_stashdb_stream, clean_stash_filename, extract_filename_date, extract_scene_title_and_date
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
# Performance Optimization: High-Performance Shared Streaming Engine Integration
# ---------------------------------------------------------------------
from streaming_engine import (
    streaming_engine,
    doc_manager,
    block_cache,
    LRUBlockCache,
    get_streaming_stats,
    is_streaming_active,
)
BlockCache = LRUBlockCache
BLOCK_SIZE = CHUNK_SIZE_BYTES
_doc_cache = doc_manager._doc_cache

async def get_cached_document(chat_id: int, message_id: int):
    global telethon_client
    return await doc_manager.get_document(telethon_client, chat_id, message_id)

def get_client() -> TelegramClient | None:
    return telethon_client

KNOWN_CHANNELS_FILE = os.path.join(DATA_DIR, "known_channels.json")

def load_known_channels() -> dict:
    if os.path.exists(KNOWN_CHANNELS_FILE):
        try:
            with open(KNOWN_CHANNELS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}

def save_known_channels(channels: dict):
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        # Convert keys to strings for valid JSON
        clean_map = {}
        for k, v in channels.items():
            if isinstance(v, dict) and "id" in v:
                clean_map[str(v["id"])] = v
        with open(KNOWN_CHANNELS_FILE, "w", encoding="utf-8") as f:
            json.dump(clean_map, f, indent=2)
    except Exception as e:
        print(f"[Warning] Failed to save known channels: {e}")

def get_session_channels() -> list[dict]:
    session_file = SESSION_PATH + ".session" if not SESSION_PATH.endswith(".session") else SESSION_PATH
    if not os.path.exists(session_file):
        return []
    try:
        import sqlite3
        con = sqlite3.connect(session_file)
        cur = con.cursor()
        rows = cur.execute("SELECT id, name, username FROM entities").fetchall()
        con.close()
        results = []
        for r in rows:
            raw_id, name, username = r[0], r[1], r[2]
            if not raw_id:
                continue
            # Telethon stores channel and group peer IDs with negative sign (< 0)
            if raw_id < 0:
                results.append({
                    "id": raw_id,
                    "title": name or username or f"Channel {raw_id}",
                    "type": "Channel",
                    "username": username
                })
        return results
    except Exception as e:
        print(f"[Session Read Notice] {e}")
        return []

def get_session_channel_ids() -> list[int]:
    return [c["id"] for c in get_session_channels()]

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
            chat_info = {
                "id": cid,
                "title": title,
                "type": display_type,
                "username": chat.get("username")
            }
            # Cache permanently into known channels so it is never lost on refresh
            known = load_known_channels()
            known[str(cid)] = chat_info
            save_known_channels(known)
            return chat_info
    except Exception as e:
        return {"error": f"Network error resolving chat: {str(e)}"}

async def sync_mtproto_channels(client: TelegramClient) -> dict:
    """
    Queries Telegram MTProto's updates difference to discover all channels
    and supergroups the bot belongs to or is an administrator of.
    Bypasses the 24-hour limit of the HTTP Bot API getUpdates method.
    """
    if not client or not client.is_connected():
        return {}

    discovered = {}
    try:
        from telethon.tl.functions.updates import GetStateRequest, GetDifferenceRequest
        import telethon.utils as telethon_utils
        import datetime

        state = await client(GetStateRequest())
        diff = await client(GetDifferenceRequest(
            pts=1,
            date=state.date - datetime.timedelta(days=90),
            qts=0
        ))
        for c in getattr(diff, "chats", []):
            peer_id = telethon_utils.get_peer_id(c)
            title = getattr(c, "title", f"Channel {peer_id}")
            ctype = "Channel" if getattr(c, "broadcast", True) else "Group"
            discovered[str(peer_id)] = {
                "id": peer_id,
                "title": title,
                "type": ctype,
                "username": getattr(c, "username", None)
            }

        if discovered:
            known = load_known_channels()
            known.update(discovered)
            save_known_channels(known)
            print(f"[Telegram] MTProto synced {len(discovered)} channel(s): {', '.join([c['title'] for c in discovered.values()])}")
    except Exception as e:
        print(f"[Telegram Notice] Channel sync via MTProto: {e}")

    return discovered

async def discover_bot_chats(bot_token: str) -> dict:
    global telethon_client
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

            # Query recent updates — always add timeout=1 so Telegram never long-polls
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
                    "limit": 100,
                    "timeout": 1,  # Prevent Telegram long-poll; return immediately
                }
                if offset_val is not None:
                    params["offset"] = offset_val
                try:
                    upd_resp = await http.get(url_updates, params=params, timeout=8.0)
                    if upd_resp.status_code == 200:
                        all_updates.extend(upd_resp.json().get("result", []))
                except Exception:
                    pass

            chats_map = {}

            # Source 1: Persistent known channels cache (from previous discoveries and live syncs)
            known = load_known_channels()
            for k, v in known.items():
                if isinstance(v, dict) and "id" in v:
                    chats_map[v["id"]] = v

            # Source 2: Telegram MTProto Updates Difference
            # Bypasses the 24-hour expiration of HTTP getUpdates and retrieves ALL channels the bot is in
            if telethon_client and telethon_client.is_connected():
                mt_chats = await sync_mtproto_channels(telethon_client)
                for cid, cinfo in mt_chats.items():
                    chats_map[cinfo["id"]] = cinfo
            else:
                from settings_manager import load_settings
                s = load_settings()
                if s.get("api_id") and s.get("api_hash") and (token or s.get("bot_token")):
                    try:
                        temp_client = TelegramClient(SESSION_PATH, s["api_id"], s["api_hash"])
                        await temp_client.start(bot_token=(token or s.get("bot_token")))
                        mt_chats = await sync_mtproto_channels(temp_client)
                        for cid, cinfo in mt_chats.items():
                            chats_map[cinfo["id"]] = cinfo
                        telethon_client = temp_client
                    except Exception as mt_err:
                        print(f"[Telegram Notice] Auto-connect MTProto sync in discover_bot_chats: {mt_err}")

            # Source 3: Channels cached in Telethon MTProto SQLite session (instant & offline)
            session_chats = get_session_channels()
            for sc in session_chats:
                cid = sc["id"]
                if cid not in chats_map:
                    chats_map[cid] = sc

            # Source 4: Live updates from getUpdates
            for item in all_updates:
                candidate_chats = []
                for key in ("channel_post", "edited_channel_post", "message", "edited_message", "my_chat_member", "chat_member"):
                    if key in item and isinstance(item[key], dict) and "chat" in item[key]:
                        candidate_chats.append(item[key]["chat"])

                # If user forwarded a message from a channel into bot DM, recognize the forwarded channel
                msg = item.get("message") or item.get("edited_message")
                if msg and isinstance(msg, dict):
                    fwd_chat = msg.get("forward_from_chat")
                    if fwd_chat and isinstance(fwd_chat, dict) and fwd_chat.get("type") in ("channel", "supergroup"):
                        candidate_chats.append(fwd_chat)

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

                # If bot was removed/kicked from channel, update state
                if "my_chat_member" in item:
                    mcm = item["my_chat_member"]
                    ncm = mcm.get("new_chat_member", {}) if isinstance(mcm, dict) else {}
                    if ncm.get("status") in ("left", "kicked"):
                        rm_cid = mcm.get("chat", {}).get("id")
                        if rm_cid in chats_map:
                            del chats_map[rm_cid]

            # Source 4: Preserved channels from settings.json
            from settings_manager import load_settings
            cur_settings = load_settings()
            saved_cids = set(cur_settings.get("channel_ids", []) + cur_settings.get("download_channel_ids", []))
            for saved_cid in saved_cids:
                if saved_cid not in chats_map:
                    chats_map[saved_cid] = {
                        "id": saved_cid,
                        "title": f"Channel {saved_cid}",
                        "type": "Channel",
                        "username": None
                    }
                    try:
                        resolved = await lookup_single_chat(token, str(saved_cid))
                        if "error" not in resolved and "id" in resolved:
                            chats_map[saved_cid] = resolved
                    except Exception:
                        pass

            # Source 5: Live Telethon client dialogs (if active in user mode)
            if telethon_client and telethon_client.is_connected():
                try:
                    if not getattr(telethon_client, "_is_bot", False):
                        async for dialog in telethon_client.iter_dialogs(limit=50):
                            if getattr(dialog, "is_channel", False) or getattr(dialog, "is_group", False):
                                chats_map[dialog.id] = {
                                    "id": dialog.id,
                                    "title": dialog.name or f"Chat {dialog.id}",
                                    "type": "Channel" if getattr(dialog, "is_channel", False) else "Group",
                                    "username": getattr(dialog.entity, "username", None)
                                }
                except Exception:
                    pass

            # Persist the merged list into known_channels.json
            save_known_channels(chats_map)

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

    settings = load_settings()

    # Route 1: Adult / StashDB identification (only active when ADULT_ONLY=True)
    if ADULT_ONLY:
        stash_key = (settings.get("stashdb_api_key") or "").strip()
        stash_scene = None
        if stash_key:
            stash_scene = await search_stashdb_scene(meta["file_name"], meta["caption"], api_key=stash_key)

        if not stash_scene:
            c_title, date_hint = extract_scene_title_and_date(meta["file_name"])
            year_hint = int(date_hint[:4]) if date_hint and len(date_hint) >= 4 and date_hint[:4].isdigit() else None
            stash_scene = {
                "stash_id": f"stash_local_{message.id}",
                "media_type": "porn",
                "title": c_title,
                "year": year_hint,
                "date": date_hint,
                "poster": None,
                "backdrop": None,
                "overview": f"Adult Scene: {c_title}" + (f"\nRelease Date: {date_hint}" if date_hint else ""),
                "genres": ["Adult"],
                "rating": 0.0
            }

        header = stash_scene["title"]
        if stash_scene.get("date"):
            header += f" ({stash_scene['date']})"
        elif stash_scene.get("year"):
            header += f" ({stash_scene['year']})"

        badge, desc = format_stashdb_stream(header, specs)

        await upsert_video(
            chat_id=chat_id,
            message_id=message.id,
            file_name=meta["file_name"],
            file_size=meta["file_size"],
            mime_type=meta["mime_type"],
            stash_info=stash_scene,
            specs=specs,
            stream_badge=badge,
            stream_desc=desc
        )

        res_badge = specs.get("resolution") or "HD"
        codec_str = specs.get("codec") or ""
        aud_str = specs.get("audio") or ""
        tech_str = f"[{res_badge} {codec_str} {aud_str}]".replace("  ", " ")
        sz_str = specs.get("size_str") or ""
        studio_str = f" [{stash_scene.get('studio')}]" if stash_scene.get("studio") else ""

        print(f"[Indexed Adult/StashDB] #{message.id}: {stash_scene['title']}{studio_str} {tech_str} {sz_str} [ID: {stash_scene.get('stash_id')}]")
        return True

    # Route 2: TMDB Movie / TV Series identification (when ADULT_ONLY=False)
    # Priority 1: Check persistent manual corrections before automatic parsing / search
    from database import get_manual_correction, get_series_manual_correction
    manual_corr = await get_manual_correction(chat_id=chat_id, message_id=message.id, file_name=meta["file_name"])
    if not manual_corr and parsed.get("raw_title"):
        manual_corr = await get_series_manual_correction(parsed["raw_title"])

    if manual_corr:
        tmdb_info = dict(manual_corr)
        if tmdb_info.get("media_type") == "series":
            s = parsed.get("season") or tmdb_info.get("season") or 1
            e = parsed.get("episode") or tmdb_info.get("episode") or 1
            ep_t = parsed.get("episode_title") or f"Episode {e}"
            tmdb_info["season"] = s
            tmdb_info["episode"] = e
            tmdb_info["episode_title"] = ep_t
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

        if tmdb_info.get("media_type") == "series":
            from tmdb import set_series_cache
            set_series_cache(tmdb_info["title"], tmdb_info)
            if parsed.get("raw_title"):
                set_series_cache(parsed["raw_title"], tmdb_info)

        print(f"[Indexed via Manual Correction] #{message.id}: {header} [TMDB: {tmdb_info.get('tmdb_id')}]")
        return True

    tmdb_key = (settings.get("tmdb_api_key") or "").strip()
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

async def apply_manual_title_correction(
    chat_id: int,
    message_id: int,
    title_hint: str,
    target_message=None
) -> dict:
    """
    Applies manual title correction for a Telegram video message:
    1. Validates and resolves title hint via TMDB (spelling variations, transliterations, approximate matches).
    2. Movies: updates the movie's TMDB metadata and persists the correction.
    3. TV Shows: updates canonical series title and metadata across all existing episodes (preserving season/ep),
       updates in-memory cache, and persists series correction for future episodes and rescans.
    """
    from database import (
        get_video_by_location,
        save_movie_correction,
        save_series_correction,
        apply_movie_correction_to_db,
        apply_series_correction_to_db
    )

    video = await get_video_by_location(chat_id, message_id)
    if not video:
        if target_message:
            indexed = await index_single_message(chat_id, target_message)
            if indexed:
                video = await get_video_by_location(chat_id, message_id)
        elif telethon_client and telethon_client.is_connected():
            try:
                entity = await telethon_client.get_entity(chat_id)
                msgs = await telethon_client.get_messages(entity, ids=[message_id])
                if msgs and msgs[0]:
                    indexed = await index_single_message(chat_id, msgs[0])
                    if indexed:
                        video = await get_video_by_location(chat_id, message_id)
            except Exception as e:
                print(f"[Correction Notice] Error retrieving message #{message_id}: {e}")

    if not video:
        return {"success": False, "error": f"Video message #{message_id} not found."}

    current_media_type = video.get("media_type") or "movie"
    parsed = parse_filename(video["file_name"], video.get("caption", ""))
    is_series = (current_media_type == "series" or parsed.get("media_type") == "series")
    media_type_hint = "series" if is_series else "movie"
    year_hint = video.get("year") or parsed.get("year")

    tmdb_info = await resolve_tmdb_by_hint(title_hint, year_hint=year_hint, media_type_hint=media_type_hint)
    if not tmdb_info:
        return {"success": False, "error": f"No verified TMDB title match found for '{title_hint}'."}

    resolved_type = tmdb_info.get("media_type", "movie")

    if resolved_type == "series" or is_series:
        tmdb_info["media_type"] = "series"
        old_title = video.get("title") or video.get("file_name")
        raw_title = parsed.get("raw_title") or old_title

        updated_count = await apply_series_correction_to_db(
            series_key=raw_title,
            old_title=old_title,
            tmdb_info=tmdb_info,
            chat_id=chat_id,
            message_id=message_id
        )
        await save_series_correction(
            source_title=old_title,
            tmdb_info=tmdb_info,
            user_query=title_hint,
            raw_title=raw_title,
            chat_id=chat_id,
            message_id=message_id
        )
        set_series_cache(old_title, tmdb_info)
        set_series_cache(raw_title, tmdb_info)
        set_series_cache(tmdb_info["title"], tmdb_info)

        print(f"[Manual Correction Success] Series '{old_title}' -> '{tmdb_info['title']}' ({tmdb_info.get('year')}), updated {updated_count} episode(s)")
        return {
            "success": True,
            "media_type": "series",
            "title": tmdb_info["title"],
            "year": tmdb_info.get("year"),
            "tmdb_id": tmdb_info["tmdb_id"],
            "imdb_id": tmdb_info.get("imdb_id"),
            "updated_episodes": updated_count
        }
    else:
        tmdb_info["media_type"] = "movie"
        await apply_movie_correction_to_db(chat_id, message_id, tmdb_info)
        await save_movie_correction(
            chat_id=chat_id,
            message_id=message_id,
            file_name=video["file_name"],
            tmdb_info=tmdb_info,
            user_query=title_hint
        )
        print(f"[Manual Correction Success] Movie #{message_id} -> '{tmdb_info['title']}' ({tmdb_info.get('year')})")
        return {
            "success": True,
            "media_type": "movie",
            "title": tmdb_info["title"],
            "year": tmdb_info.get("year"),
            "tmdb_id": tmdb_info["tmdb_id"],
            "imdb_id": tmdb_info.get("imdb_id")
        }

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

    # Discover and sync all channels the bot is an admin/member of
    await sync_mtproto_channels(telethon_client)

    import telethon.utils as telethon_utils

    # Realtime channel discovery: when bot is added to a new channel or receives any post
    @telethon_client.on(events.ChatAction)
    async def on_bot_chat_action(event):
        try:
            chat = await event.get_chat()
            if chat:
                peer_id = telethon_utils.get_peer_id(chat)
                known = load_known_channels()
                known[str(peer_id)] = {
                    "id": peer_id,
                    "title": getattr(chat, "title", f"Channel {peer_id}"),
                    "type": "Channel" if getattr(chat, "broadcast", True) else "Group",
                    "username": getattr(chat, "username", None)
                }
                save_known_channels(known)
        except Exception:
            pass

    @telethon_client.on(events.NewMessage)
    async def on_new_incoming_message(event):
        try:
            # 1. Realtime channel tracking
            if event.is_channel:
                chat = await event.get_chat()
                if chat:
                    peer_id = telethon_utils.get_peer_id(chat)
                    known = load_known_channels()
                    if str(peer_id) not in known:
                        known[str(peer_id)] = {
                            "id": peer_id,
                            "title": getattr(chat, "title", f"Channel {peer_id}"),
                            "type": "Channel" if getattr(chat, "broadcast", True) else "Group",
                            "username": getattr(chat, "username", None)
                        }
                        save_known_channels(known)

            # 2. Check for manual title correction via reply to video message
            if event.message and event.message.is_reply:
                reply_text = (event.message.message or event.message.raw_text or "").strip()
                if reply_text:
                    replied_msg = await event.get_reply_message()
                    if not replied_msg and event.message.reply_to_msg_id:
                        try:
                            replied_msg = await telethon_client.get_messages(event.chat_id, ids=event.message.reply_to_msg_id)
                        except Exception:
                            pass

                    if replied_msg:
                        target_chat_id = event.chat_id
                        target_msg_id = replied_msg.id

                        # Handle forwarded channel posts in discussion groups
                        if getattr(replied_msg, "fwd_from", None):
                            fwd = replied_msg.fwd_from
                            fwd_cid = getattr(fwd, "channel_id", None)
                            if not fwd_cid and getattr(fwd, "from_id", None):
                                fwd_cid = telethon_utils.get_peer_id(fwd.from_id)
                            fwd_mid = getattr(fwd, "channel_post", None) or getattr(fwd, "saved_from_msg_id", None)
                            if fwd_cid and fwd_mid:
                                from database import get_video_by_location
                                if await get_video_by_location(fwd_cid, fwd_mid):
                                    target_chat_id = fwd_cid
                                    target_msg_id = fwd_mid

                        # Check if target is an indexed video or contains video media
                        from database import get_video_by_location
                        is_target_video = bool(await get_video_by_location(target_chat_id, target_msg_id)) or bool(extract_video_metadata(replied_msg))

                        if is_target_video:
                            res = await apply_manual_title_correction(
                                chat_id=target_chat_id,
                                message_id=target_msg_id,
                                title_hint=reply_text,
                                target_message=replied_msg
                            )
                            try:
                                if res.get("success"):
                                    if res.get("media_type") == "series":
                                        confirm_text = f"✅ Corrected series to **{res['title']}** ({res.get('year')}) [TMDB: {res.get('tmdb_id')}]. Updated {res.get('updated_episodes', 1)} episode(s)."
                                    else:
                                        confirm_text = f"✅ Corrected to **{res['title']}** ({res.get('year')}) [TMDB: {res.get('tmdb_id')}]."
                                    await event.reply(confirm_text)
                                else:
                                    await event.reply(f"⚠️ Title correction error: {res.get('error', 'Unknown error')}")
                            except Exception as reply_err:
                                print(f"[Telegram Reply Notice] Could not send confirmation reply: {reply_err}")
                            return

            # 3. Real-time video listener for active streaming channels (preserve Stremio functionality)
            if active_channels and event.chat_id in active_channels:
                meta = extract_video_metadata(event.message)
                if meta:
                    indexed = await index_single_message(event.chat_id, event.message)
                    if indexed:
                        print(f"[Realtime Streaming] New video detected in chat {event.chat_id}!")
        except Exception as e:
            print(f"[Realtime Event Error] Failed processing message: {e}")

    return True

async def stop_telegram_service():
    global telethon_client, scan_task
    if scan_task and not scan_task.done():
        scan_task.cancel()
    if telethon_client and telethon_client.is_connected():
        await telethon_client.disconnect()
        telethon_client = None

async def stream_telegram_file(chat_id: int, message_id: int, offset: int, length: int):
    """
    High-Performance MTProto streaming engine:
    1. Aligned Block-Based File Retrieval: Slices exact requested bytes on aligned block boundaries.
    2. Shared 128 MB LRU Block Cache: Delivers cached blocks instantly from RAM (0.01 ms).
    3. Parallel Block Fetcher: Concurrent MTProto block requests bounded by global & per-stream limits.
    4. Shared In-Flight Request Deduplication: Prevents duplicate fetches for the same block.
    5. Priority Scheduler & Seek-Aware Cancellation: Prioritizes immediate playback and purges old prefetch.
    6. Telegram File Reference Refresh: Automatically recovers from expired file references.
    """
    global telethon_client
    if not telethon_client or not telethon_client.is_connected():
        return

    async for chunk in streaming_engine.stream_file(
        client=telethon_client,
        chat_id=chat_id,
        message_id=message_id,
        offset=offset,
        length=length
    ):
        yield chunk
