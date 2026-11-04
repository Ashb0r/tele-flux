import os
import time
import math
import hashlib
import asyncio
import shutil
import random
from typing import Optional, Callable, Awaitable
from telethon import TelegramClient
from telethon.errors import (
    FileReferenceExpiredError,
    FloodWaitError,
    MessageIdInvalidError,
    ChannelPrivateError,
    ChatAdminRequiredError
)
from telethon.tl.types import MessageMediaDocument, DocumentAttributeFilename
from telethon import utils
from config import DOWNLOAD_CHUNK_SIZE_BYTES, DOWNLOAD_PARALLEL_CONNECTIONS
from downloader.models import DownloadStatus, DownloadJob
from downloader.fast_download import ParallelTransferrer
from downloader.archive_check import find_existing_media_file
from downloader.database import (
    update_job_status,
    update_download_progress,
    update_temp_file_path,
    record_checksum,
    get_job_by_id
)

class DownloadError(Exception):
    """Base exception for downloader engine failures."""
    pass

class DiskSpaceExhaustedError(DownloadError):
    pass

class SourceDeletedError(DownloadError):
    pass

class FileCorruptedError(DownloadError):
    pass

def sanitize_filename(filename: str) -> str:
    """Sanitize filename to be safe on all filesystems (Windows/Linux/macOS)."""
    clean = filename.strip()
    for ch in r'<>:"/\|?*':
        clean = clean.replace(ch, "_")
    # Collapse multiple underscores/spaces
    while "__" in clean:
        clean = clean.replace("__", "_")
    return clean.strip(" .") or "unnamed_video.mp4"

def get_temp_download_dir(media_dir: str) -> str:
    temp_dir = os.path.join(media_dir, ".downloads")
    os.makedirs(temp_dir, exist_ok=True)
    return temp_dir

def get_temp_part_path(media_dir: str, job_id: int, original_filename: str) -> str:
    safe_name = sanitize_filename(original_filename)
    temp_dir = get_temp_download_dir(media_dir)
    return os.path.join(temp_dir, f"{job_id}_{safe_name}.part")

def check_disk_space(path: str, required_bytes: int, min_margin_gb: float = 5.0) -> bool:
    try:
        usage = shutil.disk_usage(path)
        min_margin_bytes = int(min_margin_gb * 1024 * 1024 * 1024)
        available_bytes = usage.free
        return available_bytes >= (required_bytes + min_margin_bytes)
    except Exception as e:
        print(f"[Downloader Disk Check] Warning checking disk usage: {e}")
        return True

def calculate_sha256(filepath: str, chunk_size: int = 1024 * 1024) -> str:
    sha = hashlib.sha256()
    with open(filepath, "rb") as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            sha.update(data)
    return sha.hexdigest()

class TelethonDownloadEngine:
    def __init__(
        self,
        client: TelegramClient,
        chunk_size: int = DOWNLOAD_CHUNK_SIZE_BYTES
    ):
        self.client = client
        self.chunk_size = chunk_size
        self._current_cancel_event: Optional[asyncio.Event] = None
        self._active_job_id: Optional[int] = None
        self.active_progress: dict = {}

    def cancel_active_download(self):
        """Signals active download to stop cleanly and preserve partial progress."""
        self.active_progress = {}
        if self._current_cancel_event and not self._current_cancel_event.is_set():
            self._current_cancel_event.set()

    async def _fetch_fresh_document(self, channel_id: int, message_id: int):
        """
        Refreshes expired file references by retrieving the original Telegram message.
        """
        try:
            message = await self.client.get_messages(channel_id, ids=message_id)
            if not message or not message.media:
                return None
            doc = getattr(message.media, "document", None)
            if not doc and hasattr(message, "video") and message.video:
                doc = message.video
            return doc
        except (MessageIdInvalidError, ChannelPrivateError):
            return None
        except Exception as e:
            print(f"[Downloader Engine] Document refresh warning for #{message_id}: {e}")
            return None

    async def _iter_chunks(self, doc, offset: int, expected_size: int):
        """
        Yields chunks using ParallelTransferrer (multi-socket MTProto transfer)
        for maximum download speed saturating the network pipe.
        Falls back seamlessly to Telethon's sequential iter_download if parallel
        transfer encounters an issue.
        """
        dc_id = getattr(doc, 'dc_id', None) or getattr(self.client.session, 'dc_id', None)
        current_offset = offset
        if DOWNLOAD_PARALLEL_CONNECTIONS > 1 and dc_id:
            try:
                print(f"[Downloader Engine] Using maximum-speed parallel MTProto transfer ({DOWNLOAD_PARALLEL_CONNECTIONS} connections, 512KB chunks) for DC {dc_id} from offset {offset}...")
                transferrer = ParallelTransferrer(self.client, dc_id=dc_id)
                async for chunk in transferrer.download(
                    file=doc,
                    file_size=expected_size,
                    start_offset=current_offset,
                    part_size=self.chunk_size,
                    connections=DOWNLOAD_PARALLEL_CONNECTIONS
                ):
                    current_offset += len(chunk)
                    yield chunk
                return
            except (FileReferenceExpiredError, FloodWaitError):
                raise
            except Exception as e:
                print(f"[Downloader Engine] Parallel transfer encountered ({e}). Gracefully continuing with sequential iter_download from offset {current_offset}...")

        # Sequential fallback (from current offset)
        bytes_remaining = expected_size - current_offset
        if bytes_remaining > 0:
            async for chunk in self.client.iter_download(
                doc,
                offset=current_offset,
                limit=bytes_remaining,
                request_size=self.chunk_size
            ):
                yield chunk

    async def download_job(
        self,
        job: DownloadJob,
        media_dir: str,
        min_free_disk_gb: float = 5.0,
        progress_callback: Optional[Callable[[int, float], Awaitable[None]]] = None
    ) -> str:
        """
        Downloads a media file with full fault-tolerance:
        - Chunked MTProto transfer
        - Resumes from verified chunk boundary
        - Automatic Telegram file reference refresh
        - Flushes and fsyncs data
        - Validates SHA-256 and expected file size
        Returns the path to the completed temporary .part file (ready for atomic rename).
        """
        self._active_job_id = job.id
        cancel_event = asyncio.Event()
        self._current_cancel_event = cancel_event

        part_path = get_temp_part_path(media_dir, job.id, job.file_name)
        await update_temp_file_path(job.id, part_path)

        expected_size = job.file_size
        if expected_size <= 0:
            raise DownloadError(f"Job #{job.id} has invalid file size: {expected_size}")

        # 0. Check if file is already completely archived on disk
        existing = find_existing_media_file(media_dir, job.file_name, job.file_size)
        if not existing:
            from downloader.archive_check import check_db_for_existing_archive
            existing = await check_db_for_existing_archive(job.file_name, job.file_size)
        if existing:
            print(f"[Downloader Engine] File '{job.file_name}' already exists in library at '{existing}'. Skipping download.")
            return existing

        # 1. Inspect existing partial file and compute verified offset
        verified_offset = 0
        if os.path.exists(part_path):
            actual_disk_size = os.path.getsize(part_path)
            if actual_disk_size > expected_size:
                print(f"[Downloader Engine] Partial file exceeds expected size ({actual_disk_size} > {expected_size}). Resetting...")
                with open(part_path, "wb") as f:
                    f.truncate(0)
                verified_offset = 0
            else:
                # Truncate partial chunk to last verified chunk boundary
                aligned_size = (actual_disk_size // self.chunk_size) * self.chunk_size
                if aligned_size != actual_disk_size:
                    print(f"[Downloader Engine] Truncating partial chunk from {actual_disk_size} to {aligned_size} bytes...")
                    with open(part_path, "a+b") as f:
                        f.truncate(aligned_size)
                verified_offset = aligned_size
                print(f"[Downloader Engine] Resuming #{job.id} '{job.file_name}' from verified offset {verified_offset}/{expected_size} bytes ({round(verified_offset/expected_size*100, 1)}%)")

        # 2. Check disk space before proceeding
        bytes_needed = expected_size - verified_offset
        if not check_disk_space(media_dir, bytes_needed, min_free_disk_gb):
            raise DiskSpaceExhaustedError(
                f"Insufficient disk space in {media_dir}. Need {bytes_needed} bytes + {min_free_disk_gb} GB margin."
            )

        # 3. If already completely downloaded, verify immediately
        if verified_offset >= expected_size:
            print(f"[Downloader Engine] File already fully downloaded on disk. Verifying integrity...")
            return await self._verify_completed_part(job, part_path, expected_size)

        # 4. Resolve Telegram document descriptor and check source availability
        doc = await self._fetch_fresh_document(job.channel_id, job.message_id)
        if not doc:
            raise SourceDeletedError(f"Original Telegram message #{job.message_id} in channel {job.channel_id} was deleted or inaccessible.")

        # 5. Start / Resume chunked download loop
        await update_job_status(job.id, DownloadStatus.DOWNLOADING.value)

        # Initialize live in-memory progress tracker
        init_pct = round((verified_offset / expected_size) * 100, 1) if expected_size else 0.0
        self.active_progress = {
            "id": job.id,
            "job_id": job.id,
            "file_name": job.file_name,
            "file_size": expected_size,
            "bytes_downloaded": verified_offset,
            "downloaded_bytes": verified_offset,
            "download_speed": 0.0,
            "speed_bytes_sec": 0.0,
            "progress_pct": init_pct,
            "eta_seconds": 0,
            "status": DownloadStatus.DOWNLOADING.value
        }

        last_db_progress_time = time.time()
        last_db_progress_bytes = verified_offset
        speed_window_start = time.time()
        speed_bytes = 0
        current_speed = 0.0

        try:
            # Open file in append/read binary mode
            mode = "r+b" if os.path.exists(part_path) and verified_offset > 0 else "wb"
            with open(part_path, mode) as part_file:
                if mode == "r+b":
                    part_file.seek(verified_offset)

                max_ref_retries = 3
                ref_retry_count = 0

                while verified_offset < expected_size:
                    if cancel_event.is_set():
                        part_file.flush()
                        try:
                            os.fsync(part_file.fileno())
                        except Exception:
                            pass
                        raise DownloadError(f"Download of job #{job.id} was paused or cancelled.")

                    try:
                        async for chunk in self._iter_chunks(doc, verified_offset, expected_size):
                            if cancel_event.is_set():
                                part_file.flush()
                                try:
                                    os.fsync(part_file.fileno())
                                except Exception:
                                    pass
                                raise DownloadError(f"Download of job #{job.id} was paused or cancelled.")

                            part_file.write(chunk)
                            chunk_len = len(chunk)
                            verified_offset += chunk_len
                            speed_bytes += chunk_len

                            now = time.time()

                            # Calculate running speed over responsive 0.5 - 1.0 second window
                            speed_elapsed = now - speed_window_start
                            if speed_elapsed >= 0.5:
                                current_speed = speed_bytes / speed_elapsed
                                speed_bytes = 0
                                speed_window_start = now

                            # Compute live percentage & ETA
                            pct = round((verified_offset / expected_size) * 100, 1) if expected_size else 0.0
                            bytes_left = max(0, expected_size - verified_offset)
                            eta = int(bytes_left / current_speed) if current_speed > 1000 else 0

                            # Update live in-memory progress on every chunk!
                            self.active_progress["bytes_downloaded"] = verified_offset
                            self.active_progress["downloaded_bytes"] = verified_offset
                            self.active_progress["download_speed"] = current_speed
                            self.active_progress["speed_bytes_sec"] = current_speed
                            self.active_progress["progress_pct"] = pct
                            self.active_progress["eta_seconds"] = eta

                            if progress_callback:
                                try:
                                    cb_res = progress_callback(verified_offset, current_speed, pct, eta)
                                    if asyncio.iscoroutine(cb_res):
                                        await cb_res
                                except Exception:
                                    pass

                            # Flush buffer and update database periodically (every 1.5s) without blocking fsync
                            if (now - last_db_progress_time >= 1.5) or (verified_offset >= expected_size):
                                part_file.flush()
                                await update_download_progress(job.id, verified_offset, current_speed)
                                last_db_progress_time = now
                                last_db_progress_bytes = verified_offset

                    except FileReferenceExpiredError:
                        part_file.flush()
                        ref_retry_count += 1
                        if ref_retry_count > max_ref_retries:
                            raise DownloadError(f"Exceeded maximum file reference retries ({max_ref_retries}) for #{job.id}")
                        print(f"[Downloader Engine] File reference expired at offset {verified_offset}. Refreshing from Telegram...")
                        doc = await self._fetch_fresh_document(job.channel_id, job.message_id)
                        if not doc:
                            raise SourceDeletedError(f"Cannot refresh expired file reference: Telegram message #{job.message_id} was deleted.")
                        await asyncio.sleep(1.0)
                        continue

                    except FloodWaitError as fwe:
                        part_file.flush()
                        sleep_time = fwe.seconds + random.uniform(1.0, 3.0)
                        print(f"[Downloader Engine] FloodWait encountered. Backing off for {sleep_time:.1f}s...")
                        await asyncio.sleep(sleep_time)
                        continue

                    except (ConnectionError, asyncio.TimeoutError) as net_err:
                        part_file.flush()
                        print(f"[Downloader Engine] Network glitch during chunk transfer: {net_err}")
                        raise

                # Flush and commit final data to physical disk
                part_file.flush()
                try:
                    os.fsync(part_file.fileno())
                except Exception:
                    pass

            # 6. Verification
            self.active_progress["status"] = DownloadStatus.VERIFYING.value
            return await self._verify_completed_part(job, part_path, expected_size)
        finally:
            self._active_job_id = None
            self.active_progress = {}

    async def _verify_completed_part(self, job: DownloadJob, part_path: str, expected_size: int) -> str:
        await update_job_status(job.id, DownloadStatus.VERIFYING.value)
        actual_size = os.path.getsize(part_path)
        if actual_size != expected_size:
            raise FileCorruptedError(
                f"File size verification failed: expected {expected_size} bytes, got {actual_size} bytes on disk."
            )

        print(f"[Downloader Engine] Calculating SHA-256 integrity checksum for #{job.id}...")
        sha_hash = calculate_sha256(part_path)
        await record_checksum(job.id, sha_hash)
        print(f"[Downloader Engine] Checksum verified for #{job.id}: SHA-256 = {sha_hash}")
        return part_path
