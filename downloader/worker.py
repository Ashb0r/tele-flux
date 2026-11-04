import os
import time
import math
import random
import asyncio
from typing import Optional, List, Dict, Any
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from settings_manager import load_settings
from downloader.models import DownloadStatus, NotificationStatus, DownloadJob
from downloader.database import (
    enqueue_message,
    get_next_queued_job,
    get_unfinished_jobs,
    get_job_by_id,
    update_job_status,
    recalculate_queue_positions,
    get_pending_notifications,
    get_checkpoint,
    set_checkpoint,
    commit_job_completion
)
from downloader.extractor import extract_message_video
from downloader.engine import (
    TelethonDownloadEngine,
    DiskSpaceExhaustedError,
    SourceDeletedError,
    FileCorruptedError,
    DownloadError,
    check_disk_space,
    calculate_sha256
)
from downloader.organizer import MediaOrganizer
from downloader.notifications import TelegramNotifier

class DownloaderWorker:
    def __init__(self, client: TelegramClient):
        self.client = client
        self._running = False
        self._worker_task: Optional[asyncio.Task] = None
        self._reconcile_task: Optional[asyncio.Task] = None
        self._notify_task: Optional[asyncio.Task] = None
        self.engine: Optional[TelethonDownloadEngine] = None
        self._active_job: Optional[DownloadJob] = None
        self._wake_event = asyncio.Event()

    @property
    def is_running(self) -> bool:
        return self._running

    def wake(self):
        self._wake_event.set()

    def get_active_job(self) -> Optional[DownloadJob]:
        return self._active_job

    def get_active_transfer(self) -> Optional[dict]:
        if self.engine and self.engine.active_progress:
            return self.engine.active_progress.copy()
        if self._active_job:
            job = self._active_job
            pct = round((job.bytes_downloaded / job.file_size) * 100, 1) if job.file_size else 0.0
            bytes_left = max(0, job.file_size - job.bytes_downloaded)
            eta = int(bytes_left / job.download_speed) if job.download_speed > 1000 else 0
            return {
                "id": job.id,
                "job_id": job.id,
                "file_name": job.file_name,
                "file_size": job.file_size,
                "bytes_downloaded": job.bytes_downloaded,
                "downloaded_bytes": job.bytes_downloaded,
                "download_speed": job.download_speed,
                "speed_bytes_sec": job.download_speed,
                "progress_pct": pct,
                "eta_seconds": eta,
                "status": job.status
            }
        return None

    async def start(self):
        if self._running:
            return
        self._running = True
        self.engine = TelethonDownloadEngine(self.client)

        settings = load_settings()
        if settings.get("startup_recovery", True):
            print("[Downloader Worker] Running automatic startup recovery...")
            await self.run_startup_recovery()

        self._worker_task = asyncio.create_task(self._process_queue_loop())
        self._reconcile_task = asyncio.create_task(self._reconciliation_loop())
        self._notify_task = asyncio.create_task(self._notification_retry_loop())
        print("[Downloader Worker] Started background sequential FIFO worker.")

    async def stop(self):
        self._running = False
        if self.engine:
            self.engine.cancel_active_download()
        for task in (self._worker_task, self._reconcile_task, self._notify_task):
            if task and not task.done():
                task.cancel()
        print("[Downloader Worker] Stopped background worker.")

    async def run_startup_recovery(self):
        """
        Comprehensive startup recovery after power failures, server crashes, or Docker restarts:
        1. Inspects all unfinished jobs.
        2. Reconciles with filesystem (checks if file was already committed to final path).
        3. Validates partial .part files.
        4. Re-checks disk space.
        5. Restores original queue positions.
        """
        settings = load_settings()
        media_dir = settings.get("media_dir") or os.path.join(settings.get("data_dir", "data"), "media")
        unfinished_jobs = await get_unfinished_jobs()

        if not unfinished_jobs:
            print("[Downloader Recovery] No unfinished jobs found.")
            await recalculate_queue_positions()
            return

        print(f"[Downloader Recovery] Found {len(unfinished_jobs)} unfinished download jobs. Reconciling...")
        organizer = MediaOrganizer(media_dir)

        for job in unfinished_jobs:
            try:
                # Check 0: Check if file already exists anywhere in media directory (Movies, TV Shows, Unsorted)
                from downloader.archive_check import find_existing_media_file, check_db_for_existing_archive
                existing_media = find_existing_media_file(
                    media_dir, job.file_name, job.file_size, job.identified_title, job.identified_year
                )
                if not existing_media:
                    existing_media = await check_db_for_existing_archive(job.file_name, job.file_size)
                if existing_media:
                    print(f"[Downloader Recovery] Job #{job.id} '{job.file_name}' already exists in library at '{existing_media}'. Marking COMPLETED.")
                    await commit_job_completion(job.id, existing_media)
                    await update_download_progress(job.id, job.file_size, 0.0)
                    continue

                # Check A: Was final file already committed to final destination?
                if job.final_file_path and os.path.exists(job.final_file_path):
                    fsize = os.path.getsize(job.final_file_path)
                    if fsize == job.file_size:
                        print(f"[Downloader Recovery] Job #{job.id} already committed at {job.final_file_path}. Marking COMPLETED.")
                        await commit_job_completion(job.id, job.final_file_path)
                        continue

                # Check B: Does temp .part file exist?
                part_path = job.temp_file_path
                if part_path and os.path.exists(part_path):
                    actual_size = os.path.getsize(part_path)
                    if actual_size == job.file_size:
                        # Fully downloaded, interrupted during verification, identification, or organizing
                        print(f"[Downloader Recovery] Job #{job.id} is fully downloaded ({actual_size} bytes). Resuming organization...")
                        sha = calculate_sha256(part_path)
                        meta = await organizer.identify_metadata(job, auto_tmdb=settings.get("auto_tmdb_identify", True))
                        final_dest = await organizer.commit_and_organize(
                            job, part_path, meta, auto_organize=settings.get("auto_organize", True)
                        )
                        print(f"[Downloader Recovery] Successfully recovered job #{job.id} -> {final_dest}")
                        continue
                    elif actual_size > job.file_size:
                        # Corrupted or invalid size, reset partial file
                        print(f"[Downloader Recovery] Job #{job.id} part size exceeds expected size. Resetting partial file...")
                        with open(part_path, "wb") as f:
                            f.truncate(0)
                        await update_job_status(job.id, DownloadStatus.QUEUED.value)
                        continue
                    else:
                        # Incomplete download: truncate partial chunk to aligned 512KB boundary
                        chunk_size = self.engine.chunk_size if self.engine else 512 * 1024
                        aligned = (actual_size // chunk_size) * chunk_size
                        if aligned != actual_size:
                            with open(part_path, "a+b") as f:
                                f.truncate(aligned)
                        print(f"[Downloader Recovery] Job #{job.id} verified at offset {aligned}/{job.file_size} bytes. Ready to resume.")
                        await update_job_status(job.id, DownloadStatus.QUEUED.value)
                        continue

                # Default fallback: reset to QUEUED
                await update_job_status(job.id, DownloadStatus.QUEUED.value)

            except Exception as rec_err:
                print(f"[Downloader Recovery Warning] Error recovering job #{job.id}: {rec_err}")
                await update_job_status(job.id, DownloadStatus.QUEUED.value, error=str(rec_err))

        await recalculate_queue_positions()
        print("[Downloader Recovery] Startup recovery complete. Queue restored.")

    async def _process_queue_loop(self):
        """
        Strict sequential FIFO download loop.
        Downloads the oldest eligible message first.
        Only ONE media download runs at a time initially.
        """
        while self._running:
            try:
                # Streaming priority coordination: If active streaming playback is ongoing,
                # briefly yield background downloading so real-time playback receives full bandwidth.
                try:
                    from streaming_engine import is_streaming_active
                    if is_streaming_active():
                        await asyncio.sleep(1.0)
                        continue
                except Exception:
                    pass

                settings = load_settings()
                if not settings.get("downloader_enabled", True):
                    await asyncio.sleep(5)
                    continue

                if not self.client or not self.client.is_connected():
                    await asyncio.sleep(3)
                    continue

                job = await get_next_queued_job()
                if not job:
                    # Nothing queued, wait for new messages or wake trigger
                    try:
                        await asyncio.wait_for(self._wake_event.wait(), timeout=3.0)
                        self._wake_event.clear()
                    except asyncio.TimeoutError:
                        pass
                    continue

                # Process this job sequentially
                self._active_job = job
                await self._process_single_job(job, settings)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[Downloader Worker Loop] Unexpected error: {e}")
                await asyncio.sleep(5)
            finally:
                self._active_job = None

    async def _process_single_job(self, job: DownloadJob, settings: dict):
        job_id = job.id
        media_dir = settings.get("media_dir") or os.path.join(settings.get("data_dir", "data"), "media")
        min_free_gb = float(settings.get("min_free_disk_gb", 5.0))
        max_retries = int(settings.get("max_retries", 5))
        backoff_base = float(settings.get("retry_backoff_base", 2))
        backoff_max = float(settings.get("retry_backoff_max_sec", 300))
        auto_tmdb = settings.get("auto_tmdb_identify", True)
        auto_organize = settings.get("auto_organize", True)

        organizer = MediaOrganizer(media_dir)
        notifier = TelegramNotifier(self.client, media_dir)

        print(f"[Downloader Worker] >>> Starting sequential download #{job_id}: '{job.file_name}' ({job.file_size} bytes)")

        try:
            # 0. Deduplication check: Was this file already downloaded or committed to library?
            from downloader.archive_check import find_existing_media_file, check_db_for_existing_archive
            existing_media = find_existing_media_file(
                media_dir, job.file_name, job.file_size, job.identified_title, job.identified_year
            )
            if not existing_media:
                existing_media = await check_db_for_existing_archive(job.file_name, job.file_size)
            if existing_media:
                print(f"[Downloader Worker] '{job.file_name}' already exists in library at '{existing_media}'. Skipping download and marking COMPLETED!")
                await commit_job_completion(job_id, existing_media)
                await update_download_progress(job_id, job.file_size, 0.0)
                return

            # 1. Download & verify
            async def on_progress(dl_bytes, speed, pct, eta):
                if self._active_job and self._active_job.id == job_id:
                    self._active_job.bytes_downloaded = dl_bytes
                    self._active_job.download_speed = speed

            part_path = await self.engine.download_job(
                job=job,
                media_dir=media_dir,
                min_free_disk_gb=min_free_gb,
                progress_callback=on_progress
            )

            # Re-fetch updated job with sha256_hash
            fresh_job = await get_job_by_id(job_id)
            if fresh_job:
                job = fresh_job

            # 2. Metadata Identification (independent from download)
            try:
                meta = await organizer.identify_metadata(job, auto_tmdb=auto_tmdb)
            except Exception as tmdb_err:
                print(f"[Downloader Worker] TMDB lookup temporarily unavailable ({tmdb_err}). Organizing as Unsorted for now...")
                meta = {"is_confident": False, "media_type": "unsorted", "title": job.file_name}

            # 3. Media Directory Organization & Atomic Rename
            final_path = await organizer.commit_and_organize(
                job=job,
                part_path=part_path,
                metadata=meta,
                auto_organize=auto_organize
            )

            # Re-fetch updated completed job
            fresh_job = await get_job_by_id(job_id)
            if fresh_job:
                job = fresh_job

            # 4. Telegram Completion Notification
            if settings.get("notifications_enabled", True):
                fallback_chat = settings.get("notification_chat_id")
                await notifier.send_completion_notification(job, fallback_chat_id=fallback_chat)

            print(f"[Downloader Worker] <<< Successfully finished #{job_id}: '{job.file_name}' saved to {final_path}")

        except DiskSpaceExhaustedError as dse:
            print(f"[Downloader Worker] Paused job #{job_id} due to insufficient disk space: {dse}")
            await update_job_status(job_id, DownloadStatus.PAUSED_DISK.value, error=str(dse))

        except SourceDeletedError as sde:
            print(f"[Downloader Worker] Job #{job_id} source message was deleted: {sde}")
            await update_job_status(job_id, DownloadStatus.SOURCE_DELETED.value, error=str(sde))

        except Exception as err:
            err_str = str(err)
            print(f"[Downloader Worker] Error processing job #{job_id}: {err_str}")

            new_retry_count = job.retry_count + 1
            if new_retry_count >= max_retries:
                print(f"[Downloader Worker] Job #{job_id} permanently failed after {max_retries} attempts.")
                await update_job_status(
                    job_id,
                    DownloadStatus.FAILED.value,
                    error=f"Exceeded max retries ({max_retries}): {err_str}",
                    increment_retry=True
                )
            else:
                # Exponential backoff with jitter
                delay = min(backoff_max, (backoff_base ** new_retry_count) + random.uniform(1.0, 3.0))
                next_retry = int(time.time() + delay)
                print(f"[Downloader Worker] Scheduling retry {new_retry_count}/{max_retries} for job #{job_id} in {delay:.1f}s...")
                await update_job_status(
                    job_id,
                    DownloadStatus.RETRY_PENDING.value,
                    error=err_str,
                    next_retry_at=next_retry,
                    increment_retry=True
                )

        finally:
            await recalculate_queue_positions()

    async def _notification_retry_loop(self):
        """Periodically retries failed completion notifications independently without re-downloading media."""
        while self._running:
            try:
                await asyncio.sleep(60)
                settings = load_settings()
                if not settings.get("notifications_enabled", True):
                    continue
                if not self.client or not self.client.is_connected():
                    continue

                pending = await get_pending_notifications()
                if pending:
                    media_dir = settings.get("media_dir") or os.path.join(settings.get("data_dir", "data"), "media")
                    notifier = TelegramNotifier(self.client, media_dir)
                    for job in pending:
                        await notifier.send_completion_notification(
                            job, fallback_chat_id=settings.get("notification_chat_id")
                        )
                        await asyncio.sleep(2)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[Notification Retry Loop] Warning: {e}")

    async def reconcile_channels(self) -> int:
        """
        Performs channel history reconciliation across all configured download channels.
        Discovers media posts missed during outages or application downtime.
        """
        settings = load_settings()
        channels = settings.get("download_channel_ids", [])
        if not channels or not self.client or not self.client.is_connected():
            return 0

        total_discovered = 0
        try:
            me = await self.client.get_me()
            is_user_account = not getattr(me, "bot", False)
        except Exception:
            return 0

        for chat_id in channels:
            try:
                entity = await self.client.get_entity(chat_id)
                title = getattr(entity, "title", str(chat_id))
                last_checkpoint = await get_checkpoint(chat_id)
                print(f"[Downloader Reconcile] Checking history of '{title}' (Chat ID: {chat_id}, Last Checkpoint: #{last_checkpoint})...")

                if is_user_account:
                    async for msg in self.client.iter_messages(entity, min_id=last_checkpoint or 0, limit=200):
                        if msg and not getattr(msg, "out", False):
                            extracted = extract_message_video(msg)
                            if extracted:
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
                                    total_discovered += 1
                            if not last_checkpoint or msg.id > last_checkpoint:
                                last_checkpoint = msg.id
                else:
                    latest_id = None
                    try:
                        probe = await self.client.get_messages(entity, limit=1)
                        if probe and len(probe) > 0 and probe[0]:
                            latest_id = probe[0].id
                    except Exception:
                        pass

                    if not latest_id:
                        probe_id = 100
                        high_bound = 100
                        while probe_id <= 30000:
                            try:
                                probe_msgs = await self.client.get_messages(entity, ids=[probe_id])
                                if probe_msgs and len(probe_msgs) > 0 and probe_msgs[0]:
                                    high_bound = probe_id
                                    probe_id *= 2
                                else:
                                    break
                            except Exception:
                                break
                        latest_id = high_bound + 150

                    start_scan_id = (last_checkpoint + 1) if last_checkpoint else max(1, latest_id - 300)
                    batch_size = 100

                    for cur_id in range(start_scan_id, latest_id + 1, batch_size):
                        batch_ids = list(range(cur_id, min(cur_id + batch_size, latest_id + 1)))
                        if not batch_ids:
                            continue
                        try:
                            msgs = await self.client.get_messages(entity, ids=batch_ids)
                            for m in msgs:
                                if m and getattr(m, "id", None) and not getattr(m, "out", False):
                                    extracted = extract_message_video(m)
                                    if extracted:
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
                                            total_discovered += 1
                                    last_checkpoint = max(last_checkpoint or 0, m.id)
                        except FloodWaitError as e:
                            await asyncio.sleep(e.seconds)
                        except Exception as b_err:
                            print(f"[Downloader Reconcile] Batch warning for {chat_id}: {b_err}")

                if last_checkpoint:
                    await set_checkpoint(chat_id, last_checkpoint)

            except Exception as c_err:
                print(f"[Downloader Reconcile] Warning reconciling chat {chat_id}: {c_err}")

        if total_discovered > 0:
            await recalculate_queue_positions()
            print(f"[Downloader Reconcile] Discovered and enqueued {total_discovered} media posts.")
            self.wake()
        return total_discovered

    async def _reconciliation_loop(self):
        """
        Periodically reconciles channel history to discover media posts
        missed during server downtime or connection outages.
        """
        while self._running:
            try:
                # Streaming priority coordination: If active streaming playback is ongoing,
                # briefly yield background downloading so real-time playback receives full bandwidth.
                try:
                    from streaming_engine import is_streaming_active
                    if is_streaming_active():
                        await asyncio.sleep(1.0)
                        continue
                except Exception:
                    pass

                settings = load_settings()
                interval_min = int(settings.get("reconciliation_interval_min", 30))
                await asyncio.sleep(max(60, interval_min * 60))

                if not settings.get("history_reconciliation", True):
                    continue
                if not self.client or not self.client.is_connected():
                    continue

                await self.reconcile_channels()

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[Reconciliation Loop] Warning: {e}")
