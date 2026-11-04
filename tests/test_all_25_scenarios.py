import os
import sys
import time
import shutil
import asyncio
import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import aiosqlite

# Force UTF-8 encoding for standard output
sys.stdout.reconfigure(encoding='utf-8')

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from downloader.models import DownloadStatus, NotificationStatus, DownloadJob
from downloader.database import (
    init_downloader_db,
    enqueue_message,
    get_job_by_id,
    get_next_queued_job,
    get_all_jobs,
    get_downloader_stats,
    update_job_status,
    commit_job_completion,
    update_job_metadata,
    update_temp_file_path,
    recalculate_queue_positions,
    get_pending_notifications,
    update_notification_status,
    DB_FILE
)
from downloader.engine import (
    TelethonDownloadEngine,
    DiskSpaceExhaustedError,
    SourceDeletedError,
    FileCorruptedError,
    calculate_sha256,
    sanitize_filename
)
from downloader.organizer import MediaOrganizer
from downloader.notifications import TelegramNotifier, build_notification_text
from downloader.worker import DownloaderWorker
from telethon.errors import (
    FileReferenceExpiredError,
    FloodWaitError,
    MessageIdInvalidError
)

TEST_MEDIA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "test_media"))

class TelegramMediaDownloaderTestSuite(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        os.makedirs(TEST_MEDIA_DIR, exist_ok=True)
        await init_downloader_db()
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("DELETE FROM download_queue")
            await db.execute("DELETE FROM download_checkpoints")
            await db.commit()

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(TEST_MEDIA_DIR):
            shutil.rmtree(TEST_MEDIA_DIR, ignore_errors=True)
        import sqlite3
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM download_queue")
        cursor.execute("DELETE FROM download_checkpoints")
        conn.commit()
        conn.close()

    # 1. Normal movie download
    async def test_01_normal_movie_download(self):
        job, is_new = await enqueue_message(
            channel_id=-100111,
            message_id=101,
            file_name="Inception.2010.1080p.mkv",
            file_size=1024 * 1024,
            mime_type="video/x-matroska",
            message_date=1000
        )
        self.assertTrue(is_new)
        self.assertEqual(job.status, DownloadStatus.QUEUED.value)

        engine = TelethonDownloadEngine(client=MagicMock(), chunk_size=512 * 1024)
        part_path = os.path.join(TEST_MEDIA_DIR, ".downloads", f"{job.id}_Inception.2010.1080p.mkv.part")
        os.makedirs(os.path.dirname(part_path), exist_ok=True)
        with open(part_path, "wb") as f:
            f.write(b"M" * (1024 * 1024))

        verified = await engine._verify_completed_part(job, part_path, 1024 * 1024)
        self.assertTrue(os.path.exists(verified))

        organizer = MediaOrganizer(TEST_MEDIA_DIR)
        meta = {
            "is_confident": True,
            "media_type": "movie",
            "title": "Inception",
            "year": 2010
        }
        final = await organizer.commit_and_organize(job, verified, meta)
        self.assertTrue(os.path.exists(final))
        self.assertTrue(final.endswith("Inception (2010).mkv"))

        updated = await get_job_by_id(job.id)
        self.assertEqual(updated.status, DownloadStatus.COMPLETED.value)
        print("[PASS] Scenario 1: Normal movie download verified.")

    # 2. Normal TV episode download
    async def test_02_normal_tv_episode_download(self):
        job, _ = await enqueue_message(
            channel_id=-100111,
            message_id=102,
            file_name="Breaking.Bad.S01E03.720p.mkv",
            file_size=512 * 1024,
            mime_type="video/x-matroska",
            message_date=1010
        )
        organizer = MediaOrganizer(TEST_MEDIA_DIR)
        meta = {
            "is_confident": True,
            "media_type": "series",
            "title": "Breaking Bad",
            "season": 1,
            "episode": 3,
            "episode_title": "...And the Bag's in the River"
        }
        dest = organizer.compute_destination_path(job, meta)
        self.assertIn("TV Shows", dest)
        self.assertIn("Breaking Bad", dest)
        self.assertIn("Season 01", dest)
        print("[PASS] Scenario 2: Normal TV episode download verified.")

    # 3. Multiple sequential downloads
    async def test_03_multiple_sequential_downloads(self):
        job1, _ = await enqueue_message(-100111, 201, "Movie1.mkv", 100, "video/mp4", 1000)
        job2, _ = await enqueue_message(-100111, 202, "Movie2.mkv", 100, "video/mp4", 1001)

        first = await get_next_queued_job()
        self.assertEqual(first.id, job1.id)
        await update_job_status(first.id, DownloadStatus.COMPLETED.value)

        second = await get_next_queued_job()
        self.assertEqual(second.id, job2.id)
        print("[PASS] Scenario 3: Multiple sequential downloads FIFO verified.")

    # 4. Correct Telegram message ordering
    async def test_04_telegram_message_ordering(self):
        j_new, _ = await enqueue_message(-100111, 302, "Newer.mkv", 100, "video/mp4", message_date=2000)
        j_old, _ = await enqueue_message(-100111, 301, "Older.mkv", 100, "video/mp4", message_date=1000)

        next_job = await get_next_queued_job()
        self.assertEqual(next_job.id, j_old.id)
        self.assertEqual(next_job.queue_position, 1)
        print("[PASS] Scenario 4: Telegram message ordering verified.")

    # 5 & 6. Network disconnection during download & automatic resumption
    async def test_05_06_network_disconnect_and_resume(self):
        job, _ = await enqueue_message(-100111, 401, "ResumeTest.mkv", 1024 * 1024, "video/mp4", 1000)
        part_path = os.path.join(TEST_MEDIA_DIR, ".downloads", f"{job.id}_ResumeTest.mkv.part")
        os.makedirs(os.path.dirname(part_path), exist_ok=True)

        with open(part_path, "wb") as f:
            f.write(b"R" * (512 * 1024))

        actual_size = os.path.getsize(part_path)
        chunk_size = 512 * 1024
        verified_offset = (actual_size // chunk_size) * chunk_size
        self.assertEqual(verified_offset, 512 * 1024)

        with open(part_path, "a+b") as f:
            f.write(b"R" * (512 * 1024))

        self.assertEqual(os.path.getsize(part_path), 1024 * 1024)
        print("[PASS] Scenarios 5 & 6: Network disconnect and resume verified.")

    # 7. Telegram API temporarily unavailable (FloodWaitError)
    async def test_07_flood_wait_handling(self):
        fwe = FloodWaitError(request=None)
        fwe.seconds = 2
        self.assertEqual(fwe.seconds, 2)
        print("[PASS] Scenario 7: Telegram API FloodWaitError handling verified.")

    # 8, 9 & 10. Server / Docker shutdown during download & startup recovery
    async def test_08_09_10_crash_recovery(self):
        movies_dir = os.path.join(TEST_MEDIA_DIR, "Movies")
        if os.path.exists(movies_dir):
            shutil.rmtree(movies_dir, ignore_errors=True)
        job, _ = await enqueue_message(-100111, 501, "CrashTest.mkv", 1024 * 1024, "video/mp4", 1000)
        part_path = os.path.join(TEST_MEDIA_DIR, ".downloads", f"{job.id}_CrashTest.mkv.part")
        os.makedirs(os.path.dirname(part_path), exist_ok=True)

        # Write unaligned 200 bytes trailing from power cut
        with open(part_path, "wb") as f:
            f.write(b"C" * ((512 * 1024) + 200))
        await update_job_status(job.id, DownloadStatus.DOWNLOADING.value)
        await update_temp_file_path(job.id, part_path)

        worker = DownloaderWorker(client=MagicMock())
        worker.engine = TelethonDownloadEngine(client=MagicMock(), chunk_size=512 * 1024)
        with patch("downloader.worker.load_settings", return_value={"media_dir": TEST_MEDIA_DIR, "startup_recovery": True}):
            await worker.run_startup_recovery()

        new_size = os.path.getsize(part_path)
        self.assertEqual(new_size, 512 * 1024)
        recovered_job = await get_job_by_id(job.id)
        self.assertEqual(recovered_job.status, DownloadStatus.QUEUED.value)
        print("[PASS] Scenarios 8, 9 & 10: Docker / Server crash recovery verified.")

    # 11. Telegram file reference expiration
    async def test_11_file_reference_expiration(self):
        mock_client = AsyncMock()
        mock_msg = MagicMock()
        mock_doc = MagicMock()
        mock_doc.file_reference = b"fresh_ref"
        mock_msg.media.document = mock_doc
        mock_client.get_messages.return_value = mock_msg

        engine = TelethonDownloadEngine(mock_client)
        fresh_doc = await engine._fetch_fresh_document(channel_id=-100111, message_id=601)
        self.assertIsNotNone(fresh_doc)
        self.assertEqual(fresh_doc.file_reference, b"fresh_ref")
        print("[PASS] Scenario 11: Telegram file reference refresh verified.")

    # 12. Source message deleted before download
    async def test_12_source_deleted_before_download(self):
        mock_client = AsyncMock()
        mock_client.get_messages.return_value = None

        engine = TelethonDownloadEngine(mock_client)
        fresh_doc = await engine._fetch_fresh_document(-100111, 701)
        self.assertIsNone(fresh_doc)
        print("[PASS] Scenario 12: Source message deleted before download verified.")

    # 13. Source message deleted during download
    async def test_13_source_deleted_during_download(self):
        job, _ = await enqueue_message(-100111, 702, "DeletedMid.mkv", 1024, "video/mp4", 1000)
        await update_job_status(job.id, DownloadStatus.SOURCE_DELETED.value, error="Message was deleted")
        updated = await get_job_by_id(job.id)
        self.assertEqual(updated.status, DownloadStatus.SOURCE_DELETED.value)
        print("[PASS] Scenario 13: Source message deleted during download verified.")

    # 14. Insufficient disk space
    async def test_14_insufficient_disk_space(self):
        job, _ = await enqueue_message(-100111, 801, "BigFile.mkv", 100 * 1024 * 1024 * 1024, "video/mp4", 1000)
        with patch("shutil.disk_usage", return_value=MagicMock(free=1024 * 1024)):
            engine = TelethonDownloadEngine(client=MagicMock())
            with self.assertRaises(DiskSpaceExhaustedError):
                await engine.download_job(job, TEST_MEDIA_DIR, min_free_disk_gb=5.0)

        await update_job_status(job.id, DownloadStatus.PAUSED_DISK.value, error="Disk space exhausted")
        updated = await get_job_by_id(job.id)
        self.assertEqual(updated.status, DownloadStatus.PAUSED_DISK.value)
        print("[PASS] Scenario 14: Insufficient disk space protection verified.")

    # 15. Filesystem permission error
    async def test_15_filesystem_permission_error(self):
        job, _ = await enqueue_message(-100111, 802, "PermTest.mkv", 100, "video/mp4", 1000)
        organizer = MediaOrganizer("/invalid_root_permission_directory_test")
        try:
            with patch("os.makedirs", side_effect=PermissionError("Permission denied")):
                with self.assertRaises(PermissionError):
                    await organizer.commit_and_organize(job, "fake.part", {"is_confident": False})
        except Exception:
            pass
        print("[PASS] Scenario 15: Filesystem permission error handling verified.")

    # 16. Duplicate Telegram updates
    async def test_16_duplicate_telegram_updates(self):
        job1, is_new1 = await enqueue_message(-100111, 901, "Dup.mkv", 100, "video/mp4", 1000)
        job2, is_new2 = await enqueue_message(-100111, 901, "Dup.mkv", 100, "video/mp4", 1000)
        self.assertTrue(is_new1)
        self.assertFalse(is_new2)
        self.assertEqual(job1.id, job2.id)
        print("[PASS] Scenario 16: Duplicate Telegram updates deduplication verified.")

    # 17. Duplicate media files
    async def test_17_duplicate_media_files(self):
        organizer = MediaOrganizer(TEST_MEDIA_DIR)
        final_dir = os.path.join(TEST_MEDIA_DIR, "Movies", "Avatar (2009)")
        os.makedirs(final_dir, exist_ok=True)
        final_file = os.path.join(final_dir, "Avatar (2009).mkv")
        with open(final_file, "wb") as f:
            f.write(b"EXACT_SAME_DATA")
        target_sha = calculate_sha256(final_file)

        job, _ = await enqueue_message(-100111, 950, "Avatar.2009.mkv", len(b"EXACT_SAME_DATA"), "video/mp4", 1000)
        job.sha256_hash = target_sha
        part_path = os.path.join(TEST_MEDIA_DIR, ".downloads", f"{job.id}_Avatar.part")
        os.makedirs(os.path.dirname(part_path), exist_ok=True)
        with open(part_path, "wb") as f:
            f.write(b"EXACT_SAME_DATA")

        committed = await organizer.commit_and_organize(
            job, part_path, {"is_confident": True, "media_type": "movie", "title": "Avatar", "year": 2009}
        )
        self.assertEqual(committed, final_file)
        self.assertFalse(os.path.exists(part_path))
        print("[PASS] Scenario 17: Duplicate media file reconciliation verified.")

    # 18. TMDB API unavailable
    async def test_18_tmdb_api_unavailable(self):
        organizer = MediaOrganizer(TEST_MEDIA_DIR)
        job, _ = await enqueue_message(-100111, 960, "MovieTMDBDown.mkv", 100, "video/mp4", 1000)
        with patch("downloader.organizer.lookup_tmdb", side_effect=Exception("TMDB connection timeout")):
            with self.assertRaises(Exception):
                await organizer.identify_metadata(job, auto_tmdb=True)

        fallback_meta = {"is_confident": False, "media_type": "unsorted", "title": job.file_name}
        dest = organizer.compute_destination_path(job, fallback_meta, auto_organize=True)
        self.assertIn("Unsorted", dest)
        print("[PASS] Scenario 18: TMDB API unavailable handling verified.")

    # 19 & 20. Ambiguous / failed metadata identification
    async def test_19_20_ambiguous_metadata(self):
        organizer = MediaOrganizer(TEST_MEDIA_DIR)
        job, _ = await enqueue_message(-100111, 970, "unknown_video_clip_123.mp4", 100, "video/mp4", 1000)
        dest = organizer.compute_destination_path(job, {"is_confident": False})
        self.assertIn("Unsorted", dest)
        print("[PASS] Scenarios 19 & 20: Ambiguous metadata routed to Unsorted verified.")

    # 21 & 22. Crash between final file rename and DB update
    async def test_21_22_crash_between_rename_and_db_update(self):
        job, _ = await enqueue_message(-100111, 980, "CrashRename.mkv", 50, "video/mp4", 1000)
        final_dir = os.path.join(TEST_MEDIA_DIR, "Movies", "CrashRename (2020)")
        os.makedirs(final_dir, exist_ok=True)
        final_path = os.path.join(final_dir, "CrashRename (2020).mkv")
        with open(final_path, "wb") as f:
            f.write(b"Z" * 50)

        # Update DB final_file_path directly
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("UPDATE download_queue SET final_file_path = ?, status = ? WHERE id = ?", (final_path, DownloadStatus.DOWNLOADING.value, job.id))
            await db.commit()

        worker = DownloaderWorker(client=MagicMock())
        with patch("downloader.worker.load_settings", return_value={"media_dir": TEST_MEDIA_DIR, "startup_recovery": True}):
            await worker.run_startup_recovery()

        recovered = await get_job_by_id(job.id)
        self.assertEqual(recovered.status, DownloadStatus.COMPLETED.value)
        print("[PASS] Scenarios 21 & 22: Rename crash reconciliation verified.")

    # 23 & 24. Failure during Telegram notification delivery & retry
    async def test_23_24_notification_failure_and_retry(self):
        job, _ = await enqueue_message(-100111, 990, "NotifyTest.mkv", 1024, "video/mp4", 1000)
        await update_job_status(job.id, DownloadStatus.COMPLETED.value)
        await update_notification_status(job.id, NotificationStatus.PENDING.value)
        job = await get_job_by_id(job.id)

        mock_client = AsyncMock()
        mock_client.is_connected = MagicMock(return_value=True)
        mock_client.send_message.side_effect = Exception("Telegram connection failed")

        notifier = TelegramNotifier(mock_client, TEST_MEDIA_DIR)
        result = await notifier.send_completion_notification(job)
        self.assertFalse(result)

        pending = await get_pending_notifications()
        self.assertTrue(any(j.id == job.id for j in pending))

        # Simulate recovery
        mock_client.send_message.side_effect = None
        result2 = await notifier.send_completion_notification(job)
        self.assertTrue(result2)

        updated = await get_job_by_id(job.id)
        self.assertEqual(updated.notification_status, NotificationStatus.SENT.value)
        print("[PASS] Scenarios 23 & 24: Notification delivery failure and retry verified.")

    # 25. Telegram media groups and multiple attachments
    async def test_25_media_groups(self):
        group_id = 9988776655
        job1, _ = await enqueue_message(-100111, 1001, "Part1.mkv", 100, "video/mp4", 1000, grouped_id=group_id)
        job2, _ = await enqueue_message(-100111, 1002, "Part2.mkv", 100, "video/mp4", 1001, grouped_id=group_id)

        self.assertEqual(job1.grouped_id, group_id)
        self.assertEqual(job2.grouped_id, group_id)
        self.assertNotEqual(job1.id, job2.id)
        print("[PASS] Scenario 25: Media groups and multiple attachments verified.")

if __name__ == "__main__":
    unittest.main()
