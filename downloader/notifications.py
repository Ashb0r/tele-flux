import os
import asyncio
from typing import Optional
from telethon import TelegramClient
from telethon.errors import (
    MessageIdInvalidError,
    ChannelPrivateError,
    ChatAdminRequiredError,
    FloodWaitError
)
from downloader.models import DownloadJob, NotificationStatus
from downloader.database import update_notification_status

def format_file_size(size_bytes: int) -> str:
    if size_bytes >= 1024 ** 3:
        return f"{round(size_bytes / (1024 ** 3), 2)} GB"
    elif size_bytes >= 1024 ** 2:
        return f"{round(size_bytes / (1024 ** 2), 2)} MB"
    elif size_bytes >= 1024:
        return f"{round(size_bytes / 1024, 2)} KB"
    return f"{size_bytes} B"

def build_notification_text(job: DownloadJob, media_dir: str) -> str:
    # Compute relative location for neat display
    rel_location = job.final_file_path or ""
    if rel_location and media_dir:
        try:
            rel_location = os.path.relpath(os.path.dirname(rel_location), media_dir)
            rel_location = rel_location.replace("\\", "/") + "/"
        except Exception:
            pass

    size_str = format_file_size(job.file_size)

    if job.media_type == "series":
        s = job.identified_season or 1
        e = job.identified_episode or 1
        ep_name = job.identified_episode_title or f"Episode {e}"
        show_title = job.identified_title or job.file_name

        text = (
            "DOWNLOAD COMPLETED\n\n"
            f"TV Show: {show_title}\n"
            f"Season: {s:02d}\n"
            f"Episode: {e:02d}\n\n"
            f"Episode Name: {ep_name}\n\n"
            f"File Size: {size_str}\n\n"
            f"Saved Location:\n{rel_location}\n\n"
            "Status: Successfully archived on Home Server."
        )
    elif job.media_type == "movie":
        m_title = job.identified_title or job.file_name
        year_str = str(job.identified_year) if job.identified_year else "N/A"

        text = (
            "DOWNLOAD COMPLETED\n\n"
            f"Movie: {m_title}\n"
            f"Year: {year_str}\n"
            "Type: Movie\n\n"
            f"File Size: {size_str}\n\n"
            f"Saved Location:\n{rel_location}\n\n"
            "Status: Successfully archived on Home Server."
        )
    else:
        # Unsorted / generic
        text = (
            "DOWNLOAD COMPLETED\n\n"
            f"File: {job.file_name}\n"
            "Type: Media File\n\n"
            f"File Size: {size_str}\n\n"
            f"Saved Location:\n{rel_location}\n\n"
            "Status: Successfully archived on Home Server."
        )

    return text

class TelegramNotifier:
    def __init__(self, client: TelegramClient, media_dir: str):
        self.client = client
        self.media_dir = media_dir

    async def send_completion_notification(
        self,
        job: DownloadJob,
        fallback_chat_id: Optional[int] = None
    ) -> bool:
        """
        Sends Telegram completion notification.
        Attempts to reply directly to the source media post.
        Falls back to channel post or custom chat if direct reply fails.
        """
        if not self.client or not self.client.is_connected():
            await update_notification_status(job.id, NotificationStatus.FAILED.value, "Telegram client not connected")
            return False

        # Guard against duplicate notifications
        if job.notification_status == NotificationStatus.SENT.value:
            return True

        text = build_notification_text(job, self.media_dir)
        target_chat = job.channel_id

        # Attempt A: Direct reply to original message
        try:
            print(f"[Telegram Notifier] Sending completion reply for #{job.id} to message #{job.message_id} in {target_chat}...")
            await self.client.send_message(
                target_chat,
                text,
                reply_to=job.message_id
            )
            await update_notification_status(job.id, NotificationStatus.SENT.value)
            print(f"[Telegram Notifier] Reply notification sent for #{job.id}.")
            return True

        except (MessageIdInvalidError, Exception) as reply_err:
            print(f"[Telegram Notifier] Direct reply to #{job.message_id} failed ({reply_err}). Attempting channel / fallback delivery...")

        # Attempt B: Direct post to channel without reply_to
        try:
            await self.client.send_message(target_chat, text)
            await update_notification_status(job.id, NotificationStatus.SENT.value)
            print(f"[Telegram Notifier] Fallback channel notification sent for #{job.id}.")
            return True
        except Exception as chan_err:
            print(f"[Telegram Notifier] Channel notification failed ({chan_err}).")

        # Attempt C: Configured fallback notification chat
        if fallback_chat_id and fallback_chat_id != target_chat:
            try:
                await self.client.send_message(fallback_chat_id, text)
                await update_notification_status(job.id, NotificationStatus.SENT.value)
                print(f"[Telegram Notifier] Sent notification to fallback chat {fallback_chat_id} for #{job.id}.")
                return True
            except Exception as fb_err:
                print(f"[Telegram Notifier] Fallback chat notification failed: {fb_err}")

        # Record failure for future retry without re-downloading media
        await update_notification_status(
            job.id,
            NotificationStatus.FAILED.value,
            "Failed delivery to channel and fallback chat."
        )
        return False
