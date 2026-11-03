import os
import time
import json
from typing import Optional, List, Dict, Any
import aiosqlite
from config import DATA_DIR
from downloader.models import DownloadStatus, NotificationStatus, DownloadJob

DB_FILE = os.path.join(DATA_DIR, "library.db")

async def init_downloader_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS download_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                grouped_id INTEGER,
                file_id TEXT,
                file_name TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                mime_type TEXT NOT NULL,
                message_date INTEGER NOT NULL,
                file_reference TEXT,
                sender_id INTEGER,
                sender_name TEXT,
                status TEXT NOT NULL,
                queue_position INTEGER NOT NULL,
                bytes_downloaded INTEGER DEFAULT 0,
                download_speed REAL DEFAULT 0.0,
                temp_file_path TEXT,
                final_file_path TEXT,
                sha256_hash TEXT,
                media_type TEXT,
                identified_title TEXT,
                identified_year INTEGER,
                identified_season INTEGER,
                identified_episode INTEGER,
                identified_episode_title TEXT,
                tmdb_id INTEGER,
                imdb_id TEXT,
                metadata_json TEXT,
                retry_count INTEGER DEFAULT 0,
                last_error TEXT,
                last_error_time INTEGER,
                next_retry_at INTEGER,
                notification_status TEXT DEFAULT 'NOT_REQUIRED',
                notification_error TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER,
                UNIQUE(channel_id, message_id)
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS download_checkpoints (
                channel_id INTEGER PRIMARY KEY,
                last_scanned_message_id INTEGER NOT NULL,
                last_checkpoint_timestamp INTEGER NOT NULL
            )
        """)

        # Add indexes for queue ordering, fast lookups and notifications
        await db.execute("CREATE INDEX IF NOT EXISTS idx_dq_status_queue ON download_queue(status, queue_position)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_dq_ordering ON download_queue(message_date ASC, message_id ASC)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_dq_channel_msg ON download_queue(channel_id, message_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_dq_notification ON download_queue(notification_status)")
        await db.commit()

def _row_to_job(row: aiosqlite.Row) -> DownloadJob:
    d = dict(row)
    return DownloadJob(**d)

async def enqueue_message(
    channel_id: int,
    message_id: int,
    file_name: str,
    file_size: int,
    mime_type: str,
    message_date: int,
    file_id: Optional[str] = None,
    grouped_id: Optional[int] = None,
    file_reference: Optional[str] = None,
    sender_id: Optional[int] = None,
    sender_name: Optional[str] = None
) -> tuple[DownloadJob, bool]:
    """
    Persists a detected media message before any downloading begins.
    Returns (DownloadJob, is_new).
    If the message was already recorded, returns existing job with is_new=False.
    """
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM download_queue WHERE channel_id = ? AND message_id = ?",
            (channel_id, message_id)
        ) as cur:
            existing = await cur.fetchone()
            if existing:
                return _row_to_job(existing), False

        # Calculate queue position
        async with db.execute("SELECT COALESCE(MAX(queue_position), 0) FROM download_queue") as cur:
            max_pos = (await cur.fetchone())[0]
        pos = max_pos + 1

        cursor = await db.execute("""
            INSERT INTO download_queue (
                channel_id, message_id, grouped_id, file_id, file_name, file_size, mime_type,
                message_date, file_reference, sender_id, sender_name,
                status, queue_position, bytes_downloaded, download_speed,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0.0, ?, ?)
        """, (
            channel_id, message_id, grouped_id, file_id, file_name, file_size, mime_type,
            message_date, file_reference, sender_id, sender_name,
            DownloadStatus.QUEUED.value, pos, now, now
        ))
        job_id = cursor.lastrowid
        await db.commit()

        # Recalculate queue positions to guarantee strict message_date order
        await recalculate_queue_positions_internal(db)

        async with db.execute("SELECT * FROM download_queue WHERE id = ?", (job_id,)) as cur:
            row = await cur.fetchone()
            return _row_to_job(row), True

async def recalculate_queue_positions_internal(db: aiosqlite.Connection):
    """
    Internal helper to maintain deterministic FIFO ordering based on
    message_date ASC, message_id ASC, id ASC across all active/queued jobs.
    """
    active_statuses = (
        DownloadStatus.QUEUED.value,
        DownloadStatus.VALIDATING.value,
        DownloadStatus.DOWNLOADING.value,
        DownloadStatus.PAUSED_NETWORK.value,
        DownloadStatus.PAUSED_DISK.value,
        DownloadStatus.VERIFYING.value,
        DownloadStatus.IDENTIFYING.value,
        DownloadStatus.ORGANIZING.value,
        DownloadStatus.RETRY_PENDING.value,
    )
    placeholders = ",".join(["?"] * len(active_statuses))
    async with db.execute(f"""
        SELECT id FROM download_queue
        WHERE status IN ({placeholders})
        ORDER BY message_date ASC, message_id ASC, id ASC
    """, active_statuses) as cur:
        rows = await cur.fetchall()

    for idx, r in enumerate(rows, start=1):
        await db.execute("UPDATE download_queue SET queue_position = ? WHERE id = ?", (idx, r[0]))
    await db.commit()

async def recalculate_queue_positions():
    async with aiosqlite.connect(DB_FILE) as db:
        await recalculate_queue_positions_internal(db)

async def get_next_queued_job() -> Optional[DownloadJob]:
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        # Pick the oldest eligible job: queue_position ASC, message_date ASC, message_id ASC
        query = """
            SELECT * FROM download_queue
            WHERE status = ?
               OR (status = ? AND (next_retry_at IS NULL OR next_retry_at <= ?))
            ORDER BY queue_position ASC, message_date ASC, message_id ASC, id ASC
            LIMIT 1
        """
        async with db.execute(query, (
            DownloadStatus.QUEUED.value,
            DownloadStatus.RETRY_PENDING.value,
            now
        )) as cur:
            row = await cur.fetchone()
            return _row_to_job(row) if row else None

async def get_active_job() -> Optional[DownloadJob]:
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        active_statuses = (
            DownloadStatus.VALIDATING.value,
            DownloadStatus.DOWNLOADING.value,
            DownloadStatus.VERIFYING.value,
            DownloadStatus.IDENTIFYING.value,
            DownloadStatus.ORGANIZING.value,
        )
        placeholders = ",".join(["?"] * len(active_statuses))
        async with db.execute(f"""
            SELECT * FROM download_queue
            WHERE status IN ({placeholders})
            ORDER BY queue_position ASC, id ASC
            LIMIT 1
        """, active_statuses) as cur:
            row = await cur.fetchone()
            return _row_to_job(row) if row else None

async def update_job_status(
    job_id: int,
    status: str,
    error: Optional[str] = None,
    next_retry_at: Optional[int] = None,
    increment_retry: bool = False
):
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        if increment_retry:
            await db.execute("""
                UPDATE download_queue SET
                    status = ?,
                    last_error = COALESCE(?, last_error),
                    last_error_time = CASE WHEN ? IS NOT NULL THEN ? ELSE last_error_time END,
                    next_retry_at = ?,
                    retry_count = retry_count + 1,
                    updated_at = ?
                WHERE id = ?
            """, (status, error, error, now, next_retry_at, now, job_id))
        else:
            await db.execute("""
                UPDATE download_queue SET
                    status = ?,
                    last_error = COALESCE(?, last_error),
                    last_error_time = CASE WHEN ? IS NOT NULL THEN ? ELSE last_error_time END,
                    next_retry_at = ?,
                    updated_at = ?
                WHERE id = ?
            """, (status, error, error, now, next_retry_at, now, job_id))
        await db.commit()

async def update_download_progress(job_id: int, bytes_downloaded: int, download_speed: float = 0.0):
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            UPDATE download_queue SET
                bytes_downloaded = ?,
                download_speed = ?,
                updated_at = ?
            WHERE id = ?
        """, (bytes_downloaded, download_speed, now, job_id))
        await db.commit()

async def update_temp_file_path(job_id: int, temp_file_path: str):
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            UPDATE download_queue SET
                temp_file_path = ?,
                updated_at = ?
            WHERE id = ?
        """, (temp_file_path, now, job_id))
        await db.commit()

async def record_checksum(job_id: int, sha256_hash: str):
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            UPDATE download_queue SET
                sha256_hash = ?,
                updated_at = ?
            WHERE id = ?
        """, (sha256_hash, now, job_id))
        await db.commit()

async def update_job_metadata(
    job_id: int,
    media_type: Optional[str],
    identified_title: Optional[str],
    identified_year: Optional[int],
    identified_season: Optional[int],
    identified_episode: Optional[int],
    identified_episode_title: Optional[str],
    tmdb_id: Optional[int],
    imdb_id: Optional[str],
    metadata_json: Optional[str]
):
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            UPDATE download_queue SET
                media_type = ?,
                identified_title = ?,
                identified_year = ?,
                identified_season = ?,
                identified_episode = ?,
                identified_episode_title = ?,
                tmdb_id = ?,
                imdb_id = ?,
                metadata_json = ?,
                updated_at = ?
            WHERE id = ?
        """, (
            media_type, identified_title, identified_year, identified_season,
            identified_episode, identified_episode_title, tmdb_id, imdb_id,
            metadata_json, now, job_id
        ))
        await db.commit()

async def commit_job_completion(job_id: int, final_file_path: str, notification_status: str = NotificationStatus.PENDING.value):
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            UPDATE download_queue SET
                status = ?,
                final_file_path = ?,
                notification_status = ?,
                download_speed = 0.0,
                completed_at = ?,
                updated_at = ?
            WHERE id = ?
        """, (DownloadStatus.COMPLETED.value, final_file_path, notification_status, now, now, job_id))
        await db.commit()

async def update_notification_status(job_id: int, status: str, error: Optional[str] = None):
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            UPDATE download_queue SET
                notification_status = ?,
                notification_error = ?,
                updated_at = ?
            WHERE id = ?
        """, (status, error, now, job_id))
        await db.commit()

async def get_pending_notifications() -> List[DownloadJob]:
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM download_queue
            WHERE status = ? AND notification_status IN (?, ?)
            ORDER BY completed_at ASC
        """, (
            DownloadStatus.COMPLETED.value,
            NotificationStatus.PENDING.value,
            NotificationStatus.FAILED.value
        )) as cur:
            rows = await cur.fetchall()
            return [_row_to_job(r) for r in rows]

async def get_unfinished_jobs() -> List[DownloadJob]:
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        unfinished = (
            DownloadStatus.DOWNLOADING.value,
            DownloadStatus.VALIDATING.value,
            DownloadStatus.VERIFYING.value,
            DownloadStatus.IDENTIFYING.value,
            DownloadStatus.ORGANIZING.value,
            DownloadStatus.PAUSED_NETWORK.value,
            DownloadStatus.PAUSED_DISK.value,
            DownloadStatus.RETRY_PENDING.value,
        )
        placeholders = ",".join(["?"] * len(unfinished))
        async with db.execute(f"""
            SELECT * FROM download_queue
            WHERE status IN ({placeholders})
            ORDER BY queue_position ASC, message_date ASC, message_id ASC
        """, unfinished) as cur:
            rows = await cur.fetchall()
            return [_row_to_job(r) for r in rows]

async def get_all_jobs(status: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[DownloadJob]:
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        if status:
            query = "SELECT * FROM download_queue WHERE status = ? ORDER BY queue_position ASC, id DESC LIMIT ? OFFSET ?"
            params = (status, limit, offset)
        else:
            query = "SELECT * FROM download_queue ORDER BY queue_position ASC, id DESC LIMIT ? OFFSET ?"
            params = (limit, offset)
        async with db.execute(query, params) as cur:
            rows = await cur.fetchall()
            return [_row_to_job(r) for r in rows]

async def get_job_by_id(job_id: int) -> Optional[DownloadJob]:
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM download_queue WHERE id = ?", (job_id,)) as cur:
            row = await cur.fetchone()
            return _row_to_job(row) if row else None

async def get_job_by_location(channel_id: int, message_id: int) -> Optional[DownloadJob]:
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM download_queue WHERE channel_id = ? AND message_id = ?",
            (channel_id, message_id)
        ) as cur:
            row = await cur.fetchone()
            return _row_to_job(row) if row else None

async def delete_job(job_id: int) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM download_queue WHERE id = ?", (job_id,))
        await db.commit()
        await recalculate_queue_positions_internal(db)
        return True

async def retry_job(job_id: int) -> bool:
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            UPDATE download_queue SET
                status = ?,
                last_error = NULL,
                next_retry_at = NULL,
                updated_at = ?
            WHERE id = ?
        """, (DownloadStatus.QUEUED.value, now, job_id))
        await db.commit()
        await recalculate_queue_positions_internal(db)
        return True

async def skip_job(job_id: int) -> bool:
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            UPDATE download_queue SET
                status = ?,
                updated_at = ?
            WHERE id = ?
        """, (DownloadStatus.SKIPPED.value, now, job_id))
        await db.commit()
        await recalculate_queue_positions_internal(db)
        return True

async def pause_job(job_id: int) -> bool:
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            UPDATE download_queue SET
                status = ?,
                download_speed = 0.0,
                updated_at = ?
            WHERE id = ?
        """, (DownloadStatus.PAUSED_NETWORK.value, now, job_id))
        await db.commit()
        return True

async def resume_job(job_id: int) -> bool:
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            UPDATE download_queue SET
                status = ?,
                next_retry_at = NULL,
                updated_at = ?
            WHERE id = ?
        """, (DownloadStatus.QUEUED.value, now, job_id))
        await db.commit()
        await recalculate_queue_positions_internal(db)
        return True

async def get_downloader_stats() -> Dict[str, Any]:
    async with aiosqlite.connect(DB_FILE) as db:
        counts = {}
        for status_val in DownloadStatus:
            async with db.execute("SELECT COUNT(*) FROM download_queue WHERE status = ?", (status_val.value,)) as cur:
                counts[status_val.value.lower()] = (await cur.fetchone())[0]

        async with db.execute("SELECT COUNT(*) FROM download_queue") as cur:
            counts["total"] = (await cur.fetchone())[0]

        async with db.execute(
            "SELECT COALESCE(SUM(file_size), 0) FROM download_queue WHERE status = ?",
            (DownloadStatus.COMPLETED.value,)
        ) as cur:
            counts["total_completed_bytes"] = (await cur.fetchone())[0]

        async with db.execute(
            "SELECT COALESCE(SUM(bytes_downloaded), 0) FROM download_queue WHERE status = ?",
            (DownloadStatus.DOWNLOADING.value,)
        ) as cur:
            counts["current_downloading_bytes"] = (await cur.fetchone())[0]

        return counts

async def get_checkpoint(channel_id: int) -> Optional[int]:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute(
            "SELECT last_scanned_message_id FROM download_checkpoints WHERE channel_id = ?",
            (channel_id,)
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else None

async def set_checkpoint(channel_id: int, last_scanned_message_id: int):
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            INSERT INTO download_checkpoints (channel_id, last_scanned_message_id, last_checkpoint_timestamp)
            VALUES (?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
                last_scanned_message_id = excluded.last_scanned_message_id,
                last_checkpoint_timestamp = excluded.last_checkpoint_timestamp
        """, (channel_id, last_scanned_message_id, now))
        await db.commit()
