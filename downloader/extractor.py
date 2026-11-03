import re
import time
from typing import Optional, Dict, Any
from telethon.tl.types import (
    MessageMediaDocument,
    DocumentAttributeVideo,
    DocumentAttributeFilename,
)
from tmdb import JUNK_LINE_REGEX

VIDEO_EXTENSIONS = (
    ".mkv", ".mp4", ".avi", ".mov", ".webm", ".ts",
    ".m2ts", ".m4v", ".wmv", ".flv", ".vob", ".mpg", ".mpeg", ".3gp"
)

VIDEO_MIMES = (
    "video/", "application/x-matroska", "application/x-mp4",
    "application/octet-stream", "binary/octet-stream"
)

def extract_message_video(message) -> Optional[Dict[str, Any]]:
    """
    Extracts video attachment details from a Telegram message:
    Supports native video, document video, forwarded media, captions, and media groups.
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
        mime = (getattr(doc, "mime_type", "") or "").lower()

        if any(mime.startswith(vm) for vm in VIDEO_MIMES):
            is_video = True

        if hasattr(doc, "attributes") and doc.attributes:
            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    file_name = attr.file_name
                elif isinstance(attr, DocumentAttributeVideo):
                    is_video = True

        if not file_name and hasattr(message, "file") and message.file and message.file.name:
            file_name = message.file.name

        if file_name and file_name.lower().endswith(VIDEO_EXTENSIONS):
            is_video = True

        if not is_video:
            return None

        caption = (message.message or message.raw_text or "").strip()
        actual_file_name = file_name
        if not actual_file_name:
            if caption:
                for line in caption.split("\n"):
                    line = line.strip()
                    if not line or JUNK_LINE_REGEX.match(line):
                        continue
                    cand = re.sub(r'(?i)\s+(mkv|mp4|avi|mov|webm|ts)$', r'.\1', line)
                    if not any(cand.lower().endswith(ext) for ext in VIDEO_EXTENSIONS):
                        cand = f"{cand}.mp4"
                    cand = re.sub(r'[\\/:*?"<>|]+', ' ', cand)
                    cand = re.sub(r'\s+', ' ', cand).strip()
                    if len(cand) >= 5:
                        actual_file_name = cand
                        break
            if not actual_file_name:
                actual_file_name = f"video_{message.id}.mp4"

        # Sender info
        sender_id = message.sender_id
        sender_name = None
        if hasattr(message, "sender") and message.sender:
            sender_name = getattr(message.sender, "title", None) or getattr(message.sender, "username", None) or getattr(message.sender, "first_name", None)

        # Message date
        msg_date = int(message.date.timestamp()) if hasattr(message, "date") and message.date else int(time.time())

        # File reference
        file_ref_hex = None
        if hasattr(doc, "file_reference") and doc.file_reference:
            file_ref_hex = doc.file_reference.hex()

        return {
            "channel_id": message.chat_id,
            "message_id": message.id,
            "grouped_id": getattr(message, "grouped_id", None),
            "file_id": str(getattr(doc, "id", "")),
            "file_name": actual_file_name,
            "file_size": getattr(doc, "size", 0),
            "mime_type": mime if mime.startswith("video/") else "video/mp4",
            "message_date": msg_date,
            "file_reference": file_ref_hex,
            "sender_id": sender_id,
            "sender_name": sender_name
        }
    except Exception as e:
        print(f"[Downloader Extraction Error] Message #{getattr(message, 'id', '?')}: {e}")
        return None
