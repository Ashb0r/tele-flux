import os
import time
import asyncio
from typing import Optional, List, Dict, Any
from telethon import TelegramClient, events
from settings_manager import load_settings
from downloader.database import (
    init_downloader_db,
    enqueue_message,
    get_all_jobs,
    get_job_by_id,
    get_downloader_stats,
    pause_job,
    resume_job,
    retry_job,
    skip_job,
    delete_job,
    recalculate_queue_positions
)
from downloader.extractor import extract_message_video
from downloader.worker import DownloaderWorker

# Global service state
_worker: Optional[DownloaderWorker] = None
_client: Optional[TelegramClient] = None
_event_handler = None
_active_download_channels: List[int] = []

def channel_matches(c1: int, c2: int) -> bool:
    """Robustly checks if two channel IDs match regardless of -100 prefix differences."""
    if c1 == c2 or abs(c1) == abs(c2):
        return True
    s1 = str(abs(c1))
    s2 = str(abs(c2))
    if s1.startswith("100") and len(s1) > 10:
        s1 = s1[3:]
    if s2.startswith("100") and len(s2) > 10:
        s2 = s2[3:]
    return s1 == s2

def get_downloader_worker() -> Optional[DownloaderWorker]:
    return _worker

async def handle_detected_message(message) -> Optional[Any]:
    """Inspects and persistently records a detected message into the download queue."""
    if getattr(message, "out", False):
        return None

    extracted = extract_message_video(message)
    if not extracted:
        return None

    job, is_new = await enqueue_message(
        channel_id=extracted["channel_id"],
        message_id=extracted["message_id"],
        file_name=extracted["file_name"],
        file_size=extracted["file_size"],
        mime_type=extracted["mime_type"],
        message_date=extracted["message_date"],
        file_id=extracted["file_id"],
        grouped_id=extracted["grouped_id"],
        file_reference=extracted["file_reference"],
        sender_id=extracted["sender_id"],
        sender_name=extracted["sender_name"]
    )
    if is_new:
        print(f"[Downloader Service] Detected new video: #{job.message_id} '{job.file_name}' ({job.file_size} bytes) in channel {job.channel_id} (Queue pos: #{job.queue_position})")
        if _worker:
            _worker.wake()
    return job

async def reconcile_download_channels() -> int:
    """Trigger channel history reconciliation across all download channels."""
    global _worker
    if _worker:
        return await _worker.reconcile_channels()
    return 0

async def start_downloader_service(client: TelegramClient) -> bool:
    global _worker, _client, _event_handler, _active_download_channels
    _client = client

    settings = load_settings()
    if not settings.get("downloader_enabled", False):
        print("[Downloader Service] Downloader is disabled in settings. Standby.")
        return False

    _active_download_channels = settings.get("download_channel_ids", [])
    if not _active_download_channels:
        print("[Downloader Service] No download channels configured. Standby.")
        return False

    # 1. Initialize downloader database
    await init_downloader_db()

    # 2. Start Downloader Worker
    if _worker:
        await _worker.stop()
    _worker = DownloaderWorker(client)
    await _worker.start()

    # 3. Attach real-time event listener strictly for download channels
    if _event_handler:
        try:
            client.remove_event_handler(_event_handler)
        except Exception:
            pass

    @client.on(events.NewMessage())
    async def on_download_channel_message(event):
        try:
            chat_id = event.chat_id
            is_monitored = any(channel_matches(chat_id, ac) for ac in _active_download_channels)
            if is_monitored:
                job = await handle_detected_message(event.message)
                if job and _worker:
                    _worker.wake()
        except Exception as e:
            print(f"[Downloader Realtime Error] Chat {event.chat_id}: {e}")

    _event_handler = on_download_channel_message
    print(f"[Downloader Service] Active and monitoring {len(_active_download_channels)} download channel(s): {_active_download_channels}")

    # 4. Trigger initial channel history reconciliation in background
    if settings.get("history_reconciliation", True):
        asyncio.create_task(_worker.reconcile_channels())

    return True

async def stop_downloader_service():
    global _worker, _client, _event_handler
    if _event_handler and _client:
        try:
            _client.remove_event_handler(_event_handler)
        except Exception:
            pass
        _event_handler = None
    if _worker:
        await _worker.stop()
        _worker = None
    print("[Downloader Service] Downloader service stopped.")

def update_monitored_download_channels(channel_ids: List[int]):
    global _active_download_channels, _worker
    _active_download_channels = [int(x) for x in channel_ids]
    print(f"[Downloader Service] Updated monitored download channels: {_active_download_channels}")
    if _worker:
        _worker.wake()
