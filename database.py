import os
import time
import aiosqlite
from config import DATA_DIR

DB_FILE = os.path.join(DATA_DIR, "library.db")

def parse_stremio_id(item_id: str):
    """
    Safely parses Stremio ID formats:
      - 'tt1234567' -> (is_imdb=True, id_val='tt1234567', season=None, episode=None)
      - 'tt1234567:1:2' -> (is_imdb=True, id_val='tt1234567', season=1, episode=2)
      - 'tmdb:1032906' -> (is_imdb=False, id_val=1032906, season=None, episode=None)
      - 'tmdb:1032906:1:2' -> (is_imdb=False, id_val=1032906, season=1, episode=2)
    """
    if item_id.startswith("tmdb:"):
        rest = item_id[5:]
        parts = rest.split(":")
        tmdb_id = int(parts[0]) if parts[0].isdigit() else 0
        season = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else None
        episode = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else None
        return False, tmdb_id, season, episode
    elif item_id.startswith("tt"):
        parts = item_id.split(":")
        imdb_id = parts[0]
        season = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else None
        episode = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else None
        return True, imdb_id, season, episode
    else:
        parts = item_id.split(":")
        val = int(parts[0]) if parts[0].isdigit() else 0
        season = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else None
        episode = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else None
        return False, val, season, episode

async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                file_name TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                mime_type TEXT NOT NULL,
                tmdb_id INTEGER,
                imdb_id TEXT,
                media_type TEXT NOT NULL,
                title TEXT NOT NULL,
                episode_title TEXT,
                year INTEGER,
                season INTEGER,
                episode INTEGER,
                poster TEXT,
                backdrop TEXT,
                overview TEXT,
                rating REAL,
                genres TEXT,
                indexed_at INTEGER NOT NULL,
                UNIQUE(chat_id, message_id)
            )
        """)
        # Safe migration for existing databases
        try:
            await db.execute("ALTER TABLE videos ADD COLUMN episode_title TEXT")
        except Exception:
            pass
        await db.execute("CREATE INDEX IF NOT EXISTS idx_imdb_id ON videos(imdb_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tmdb_id ON videos(tmdb_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_media_type ON videos(media_type)")
        await db.commit()

async def upsert_video(
    chat_id: int,
    message_id: int,
    file_name: str,
    file_size: int,
    mime_type: str,
    tmdb_info: dict
):
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            INSERT INTO videos (
                chat_id, message_id, file_name, file_size, mime_type,
                tmdb_id, imdb_id, media_type, title, episode_title, year, season, episode,
                poster, backdrop, overview, rating, genres, indexed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id, message_id) DO UPDATE SET
                file_name = excluded.file_name,
                file_size = excluded.file_size,
                mime_type = excluded.mime_type,
                tmdb_id = excluded.tmdb_id,
                imdb_id = excluded.imdb_id,
                media_type = excluded.media_type,
                title = excluded.title,
                episode_title = excluded.episode_title,
                year = excluded.year,
                season = excluded.season,
                episode = excluded.episode,
                poster = excluded.poster,
                backdrop = excluded.backdrop,
                overview = excluded.overview,
                rating = excluded.rating,
                genres = excluded.genres,
                indexed_at = excluded.indexed_at
        """, (
            chat_id,
            message_id,
            file_name,
            file_size,
            mime_type,
            tmdb_info.get("tmdb_id"),
            tmdb_info.get("imdb_id"),
            tmdb_info.get("media_type", "movie"),
            tmdb_info.get("title", file_name),
            tmdb_info.get("episode_title"),
            tmdb_info.get("year"),
            tmdb_info.get("season"),
            tmdb_info.get("episode"),
            tmdb_info.get("poster"),
            tmdb_info.get("backdrop"),
            tmdb_info.get("overview", ""),
            tmdb_info.get("rating", 0.0),
            ",".join(tmdb_info.get("genres", [])),
            now
        ))
        await db.commit()

async def get_catalog_items(media_type: str = "movie", limit: int = 100, skip: int = 0) -> list[dict]:
    """
    Returns grouped list of titles for the Stremio catalog row.
    """
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        query = """
            SELECT 
                COALESCE(imdb_id, 'tmdb:' || tmdb_id) as stremio_id,
                media_type,
                title,
                year,
                poster,
                backdrop,
                overview,
                rating,
                genres,
                MAX(indexed_at) as latest_add
            FROM videos 
            WHERE media_type = ?
            GROUP BY COALESCE(imdb_id, 'tmdb:' || tmdb_id)
            ORDER BY latest_add DESC
            LIMIT ? OFFSET ?
        """
        async with db.execute(query, (media_type, limit, skip)) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

async def get_meta_by_id(item_id: str, media_type: str) -> dict | None:
    """
    Fetches rich metadata for an item. Handles 'tt1234567' or 'tmdb:12345'.
    """
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        
        is_imdb, id_val, season, episode = parse_stremio_id(item_id)
        condition = "imdb_id = ?" if is_imdb else "tmdb_id = ?"

        query = f"SELECT * FROM videos WHERE {condition} AND media_type = ? ORDER BY id DESC LIMIT 1"
        async with db.execute(query, (id_val, media_type)) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            res = dict(row)
            
            # If it's a TV series, attach available episodes in the library
            if media_type == "series":
                ep_query = f"""
                    SELECT DISTINCT season, episode, title, episode_title, overview 
                    FROM videos 
                    WHERE {condition} AND season IS NOT NULL AND episode IS NOT NULL
                    ORDER BY season ASC, episode ASC
                """
                async with db.execute(ep_query, (id_val,)) as ep_cur:
                    ep_rows = await ep_cur.fetchall()
                    res["videos"] = [
                        {
                            "id": f"{item_id}:{ep['season']}:{ep['episode']}",
                            "title": (ep["episode_title"] or ep["title"] or f"Episode {ep['episode']}").strip(),
                            "season": ep["season"],
                            "episode": ep["episode"],
                            "overview": ep["overview"] or ""
                        }
                        for ep in ep_rows
                    ]
            return res

async def get_streams_by_id(item_id: str, media_type: str) -> list[dict]:
    """
    Finds video files for a Stremio stream request.
    Handles:
      - Movie: 'tt1234567' or 'tmdb:12345'
      - Series: 'tt1234567:1:2' or 'tmdb:12345:1:2'
    """
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        
        is_imdb, id_val, season, episode = parse_stremio_id(item_id)
        condition = "imdb_id = ?" if is_imdb else "tmdb_id = ?"

        if media_type == "series" and season is not None and episode is not None:
            query = f"""
                SELECT * FROM videos 
                WHERE {condition} AND media_type = 'series' AND season = ? AND episode = ?
                ORDER BY file_size DESC
            """
            params = (id_val, season, episode)
        else:
            query = f"""
                SELECT * FROM videos 
                WHERE {condition} AND media_type = 'movie'
                ORDER BY file_size DESC
            """
            params = (id_val,)

        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

async def get_video_by_location(chat_id: int, message_id: int) -> dict | None:
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM videos WHERE chat_id = ? AND message_id = ?", (chat_id, message_id)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

async def get_library_stats() -> dict:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT COUNT(*) FROM videos") as c1:
            total = (await c1.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM videos WHERE media_type = 'movie'") as c2:
            movies = (await c2.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM videos WHERE media_type = 'series'") as c3:
            series = (await c3.fetchone())[0]
        return {"total_videos": total, "movies": movies, "series_episodes": series}

async def clear_library_database():
    """Wipes indexed videos to allow a clean re-scan."""
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM videos")
        await db.commit()
