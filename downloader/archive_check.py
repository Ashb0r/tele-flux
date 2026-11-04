import os
import aiosqlite
from typing import Optional
from downloader.database import DB_FILE

MEDIA_EXTENSIONS = {".mkv", ".mp4", ".avi", ".mov", ".ts", ".m4v", ".webm"}

def find_existing_media_file(
    media_dir: str,
    file_name: str,
    file_size: int,
    title: Optional[str] = None,
    year: Optional[int] = None
) -> Optional[str]:
    """
    Exhaustively scans the media archive directory to determine if a media file
    has already been downloaded and committed to disk:
    1. Fast-path: Exact byte size match across all completed media in media_dir.
    2. Exact or sanitized filename match with matching size.
    3. Formatted Title & Year folder/filename match.
    Returns the absolute path to the existing file on disk if found, else None.
    """
    if not media_dir or not os.path.exists(media_dir):
        # Fallback to local media folder if running outside container
        if os.path.exists("media"):
            media_dir = os.path.abspath("media")
        elif os.path.exists("./media"):
            media_dir = os.path.abspath("./media")
        else:
            return None

    if file_size <= 0:
        return None

    if not title:
        try:
            from tmdb import parse_filename
            parsed = parse_filename(file_name)
            title = parsed.get("raw_title")
            year = year or parsed.get("year")
        except Exception:
            pass

    target_base = os.path.splitext(file_name)[0].lower().strip()
    target_clean = "".join(c for c in target_base if c.isalnum()).lower()

    # Search through media directory (Movies/, TV Shows/, Unsorted/)
    try:
        for root, dirs, files in os.walk(media_dir):
            # Never check into temporary download staging directories (.downloads)
            path_parts = [p.lower() for p in root.replace("\\", "/").split("/")]
            if any(p in {".downloads", ".tmp"} or p.startswith(".download") for p in path_parts):
                continue

            for f in files:
                ext = os.path.splitext(f)[1].lower()
                if ext not in MEDIA_EXTENSIONS:
                    continue

                full_path = os.path.join(root, f)
                try:
                    disk_size = os.path.getsize(full_path)
                except OSError:
                    continue

                # 1. Exact byte-size match (guaranteed match for video files)
                if disk_size == file_size:
                    return full_path

                # 2. Filename match with exact or near-identical size
                curr_base = os.path.splitext(f)[0].lower().strip()
                if curr_base == target_base and abs(disk_size - file_size) < 1024 * 1024:
                    return full_path

                curr_clean = "".join(c for c in curr_base if c.isalnum()).lower()
                if target_clean and curr_clean == target_clean and abs(disk_size - file_size) < 1024 * 1024:
                    return full_path

                # 3. Formatted Title (Year) match in filename or folder
                if title:
                    t_lower = title.lower()
                    if year:
                        pattern = f"{t_lower} ({year})"
                        if (pattern in curr_base or pattern in root.lower()) and abs(disk_size - file_size) < 1024 * 1024:
                            return full_path
                    if t_lower in curr_base and abs(disk_size - file_size) < 1024 * 1024:
                        return full_path
    except Exception as e:
        print(f"[Archive Check Warning] Error scanning {media_dir}: {e}")

    return None

async def check_db_for_existing_archive(file_name: str, file_size: int) -> Optional[str]:
    """
    Checks the SQLite database for any previously completed job with matching
    filename or file size, verifying that the physical file still exists on disk.
    """
    if not os.path.exists(DB_FILE) or file_size <= 0:
        return None

    try:
        async with aiosqlite.connect(DB_FILE) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT final_file_path, file_size FROM download_queue
                WHERE status = 'COMPLETED'
                  AND (file_size = ? OR file_name = ?)
                  AND final_file_path IS NOT NULL
                ORDER BY completed_at DESC
                LIMIT 10
            """, (file_size, file_name)) as cur:
                rows = await cur.fetchall()
                for r in rows:
                    path = r["final_file_path"]
                    if path:
                        candidates = [path]
                        if not os.path.exists(path) and path.startswith("/media"):
                            candidates.append(os.path.abspath(path.replace("/media", "media", 1)))
                        for c in candidates:
                            if os.path.exists(c) and os.path.getsize(c) == file_size:
                                return c
    except Exception as e:
        print(f"[Archive Check Warning] DB lookup notice: {e}")

    return None
