import os
import time
import aiosqlite
from config import DATA_DIR, PUBLIC_URL

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
                resolution TEXT,
                codec TEXT,
                audio TEXT,
                languages TEXT,
                subtitles TEXT,
                duration INTEGER,
                stream_badge TEXT,
                stream_desc TEXT,
                UNIQUE(chat_id, message_id)
            )
        """)
        # Safe migration for existing databases
        for col_def in [
            "episode_title TEXT",
            "resolution TEXT",
            "codec TEXT",
            "audio TEXT",
            "languages TEXT",
            "subtitles TEXT",
            "duration INTEGER",
            "stream_badge TEXT",
            "stream_desc TEXT"
        ]:
            try:
                await db.execute(f"ALTER TABLE videos ADD COLUMN {col_def}")
            except Exception:
                pass

        await db.execute("CREATE INDEX IF NOT EXISTS idx_imdb_id ON videos(imdb_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tmdb_id ON videos(tmdb_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_media_type ON videos(media_type)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_file_lookup ON videos(file_name, file_size)")
        await db.commit()

async def upsert_video(
    chat_id: int,
    message_id: int,
    file_name: str,
    file_size: int,
    mime_type: str,
    tmdb_info: dict,
    specs: dict | None = None,
    stream_badge: str | None = None,
    stream_desc: str | None = None
):
    now = int(time.time())
    specs = specs or {}
    langs_str = ", ".join(specs.get("languages", [])) if isinstance(specs.get("languages"), list) else (specs.get("languages") or "")

    async with aiosqlite.connect(DB_FILE) as db:
        # Deduplicate exact duplicate forwarded messages (same file name and byte size forwarded again)
        async with db.execute(
            "SELECT id, message_id FROM videos WHERE file_name = ? AND file_size = ?",
            (file_name, file_size)
        ) as cur:
            existing = await cur.fetchone()
            if existing and existing[1] != message_id:
                # Update existing record with latest message location
                await db.execute(
                    "UPDATE videos SET chat_id = ?, message_id = ?, indexed_at = ? WHERE id = ?",
                    (chat_id, message_id, now, existing[0])
                )
                await db.commit()
                return

        await db.execute("""
            INSERT INTO videos (
                chat_id, message_id, file_name, file_size, mime_type,
                tmdb_id, imdb_id, media_type, title, episode_title, year, season, episode,
                poster, backdrop, overview, rating, genres, indexed_at,
                resolution, codec, audio, languages, subtitles, duration,
                stream_badge, stream_desc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                indexed_at = excluded.indexed_at,
                resolution = excluded.resolution,
                codec = excluded.codec,
                audio = excluded.audio,
                languages = excluded.languages,
                subtitles = excluded.subtitles,
                duration = excluded.duration,
                stream_badge = excluded.stream_badge,
                stream_desc = excluded.stream_desc
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
            now,
            specs.get("resolution"),
            specs.get("codec"),
            specs.get("audio"),
            langs_str,
            specs.get("subtitles"),
            specs.get("duration"),
            stream_badge,
            stream_desc
        ))
        await db.commit()

async def get_catalog_items(media_type: str = "movie", limit: int = 100, skip: int = 0) -> list[dict]:
    """
    Returns grouped list of titles for the Stremio catalog row.
    Strictly isolated: Series will only appear in tg_series, Movies only in tg_movies.
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
    For series, orders and attaches available library episodes without duplicates.
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
            
            # If it's a TV series, attach distinct episodes in the library
            if media_type == "series":
                ep_query = f"""
                    SELECT season, episode, title, episode_title, overview 
                    FROM videos 
                    WHERE {condition} AND season IS NOT NULL AND episode IS NOT NULL
                    ORDER BY season ASC, episode ASC, id ASC
                """
                async with db.execute(ep_query, (id_val,)) as ep_cur:
                    ep_rows = await ep_cur.fetchall()
                    seen_episodes = set()
                    unique_episodes = []
                    for ep in ep_rows:
                        ep_key = (ep["season"], ep["episode"])
                        if ep_key in seen_episodes:
                            continue
                        seen_episodes.add(ep_key)
                        unique_episodes.append({
                            "id": f"{item_id}:{ep['season']}:{ep['episode']}",
                            "title": (ep["episode_title"] or f"Episode {ep['episode']}").strip(),
                            "season": ep["season"],
                            "episode": ep["episode"],
                            "overview": ep["overview"] or ""
                        })
                    res["videos"] = unique_episodes
            return res

async def get_streams_by_id(item_id: str, media_type: str) -> list[dict]:
    """
    Finds video files for a Stremio stream request.
    Handles:
      - Movie: 'tt1234567' or 'tmdb:12345'
      - Series: 'tt1234567:1:2' or 'tmdb:12345:1:2'
    Deduplicates identical forwarded messages while preserving distinct releases/qualities.
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
            seen_signatures = set()
            unique_videos = []
            for r in rows:
                v = dict(r)
                sig = (v["file_name"], v["file_size"])
                if sig in seen_signatures:
                    continue
                seen_signatures.add(sig)
                unique_videos.append(v)
            return unique_videos

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

async def get_series_master_metadata(series_title: str) -> dict | None:
    """
    Finds existing episode of this series that already has verified TMDB / IMDb metadata.
    """
    clean_q = series_title.strip()
    if not clean_q:
        return None
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        query = """
            SELECT tmdb_id, imdb_id, media_type, title, year, poster, backdrop, overview, rating, genres
            FROM videos
            WHERE media_type = 'series'
              AND (LOWER(title) = LOWER(?) OR LOWER(title) LIKE LOWER(?))
              AND tmdb_id IS NOT NULL
              AND (poster LIKE 'https://image.tmdb.org/%' OR imdb_id IS NOT NULL)
            ORDER BY id ASC LIMIT 1
        """
        async with db.execute(query, (clean_q, f"%{clean_q}%")) as cur:
            row = await cur.fetchone()
            if row:
                d = dict(row)
                if isinstance(d.get("genres"), str):
                    d["genres"] = [g.strip() for g in d["genres"].split(",") if g.strip()]
                return d
            return None

async def heal_series_episodes(series_title: str, tmdb_info: dict):
    """
    Propagates valid TMDB/IMDb metadata across ALL episodes of this series.
    Guarantees no episode is orphaned into a separate card.
    """
    if not tmdb_info.get("tmdb_id") or not tmdb_info.get("poster"):
        return
    # Only heal if tmdb_info is a real TMDB match (not a fallback SVG)
    if "/poster/" in str(tmdb_info.get("poster", "")):
        return

    genres_str = ",".join(tmdb_info.get("genres", [])) if isinstance(tmdb_info.get("genres"), list) else (tmdb_info.get("genres") or "")
    clean_t = series_title.strip()

    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            UPDATE videos SET
                tmdb_id = ?,
                imdb_id = COALESCE(?, imdb_id),
                title = ?,
                year = COALESCE(?, year),
                poster = ?,
                backdrop = COALESCE(?, backdrop),
                overview = COALESCE(?, overview),
                rating = COALESCE(?, rating),
                genres = COALESCE(?, genres)
            WHERE media_type = 'series'
              AND (LOWER(title) = LOWER(?) OR LOWER(title) LIKE LOWER(?))
        """, (
            tmdb_info["tmdb_id"],
            tmdb_info.get("imdb_id"),
            tmdb_info["title"],
            tmdb_info.get("year"),
            tmdb_info["poster"],
            tmdb_info.get("backdrop"),
            tmdb_info.get("overview"),
            tmdb_info.get("rating"),
            genres_str,
            clean_t,
            f"%{clean_t}%"
        ))
        await db.commit()

async def reconcile_library():
    """
    Comprehensive self-healing library reconciliation:
    1. Unifies all episodes of each TV series under the best resolved TMDB/IMDb identity.
    2. Re-resolves any movies or series that currently have fallback SVG posters.
    """
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row

        # 1. TV Series healing: propagate verified metadata to all episodes
        cur = await db.execute("SELECT DISTINCT title FROM videos WHERE media_type = 'series'")
        series_rows = await cur.fetchall()
        for srow in series_rows:
            st = srow["title"]
            cur_best = await db.execute("""
                SELECT tmdb_id, imdb_id, title, year, poster, backdrop, overview, rating, genres
                FROM videos
                WHERE media_type = 'series'
                  AND title = ?
                  AND poster LIKE 'https://image.tmdb.org/%'
                ORDER BY id ASC LIMIT 1
            """, (st,))
            best = await cur_best.fetchone()
            if best:
                b = dict(best)
                await db.execute("""
                    UPDATE videos SET
                        tmdb_id = ?,
                        imdb_id = COALESCE(?, imdb_id),
                        year = COALESCE(?, year),
                        poster = ?,
                        backdrop = COALESCE(?, backdrop),
                        overview = COALESCE(?, overview),
                        rating = COALESCE(?, rating),
                        genres = COALESCE(?, genres)
                    WHERE media_type = 'series' AND title = ?
                """, (
                    b["tmdb_id"], b["imdb_id"], b["year"], b["poster"],
                    b["backdrop"], b["overview"], b["rating"], b["genres"],
                    st
                ))
        await db.commit()

        # 2. Heal movies that currently have fallback SVG posters
        cur_movies = await db.execute("""
            SELECT id, file_name, title, media_type
            FROM videos
            WHERE media_type = 'movie'
              AND (poster LIKE '%/poster/%' OR poster IS NULL OR tmdb_id IS NULL OR imdb_id IS NULL)
        """)
        fallback_movies = await cur_movies.fetchall()

        if fallback_movies:
            from tmdb import lookup_tmdb, parse_filename
            for frow in fallback_movies:
                fn = frow["file_name"]
                vid_id = frow["id"]
                parsed = parse_filename(fn)
                hit = await lookup_tmdb(fn, parsed_override=parsed)
                if hit and hit.get("poster") and not hit["poster"].endswith(".svg"):
                    await db.execute("""
                        UPDATE videos SET
                            tmdb_id = ?,
                            imdb_id = COALESCE(?, imdb_id),
                            title = ?,
                            year = COALESCE(?, year),
                            poster = ?,
                            backdrop = COALESCE(?, backdrop),
                            overview = COALESCE(?, overview),
                            rating = COALESCE(?, rating),
                            genres = COALESCE(?, genres)
                        WHERE id = ?
                    """, (
                        hit["tmdb_id"], hit.get("imdb_id"), hit["title"], hit.get("year"),
                        hit["poster"], hit.get("backdrop"), hit.get("overview"),
                        hit.get("rating"), ",".join(hit.get("genres", [])) if isinstance(hit.get("genres"), list) else hit.get("genres"),
                        vid_id
                    ))
                    await db.commit()
                    print(f"[Library Healed Movie] #{vid_id} '{fn}' -> '{hit['title']}' ({hit.get('year')}) [IMDb: {hit.get('imdb_id')}]")

        # 3. Heal any movie where the assigned title doesn't match the filename's movie name
        # (e.g. 'The Handmaiden' misassigned to 'The Dark Knight')
        cur_all_movies = await db.execute("SELECT id, file_name, title, media_type FROM videos WHERE media_type = 'movie'")
        all_movies = await cur_all_movies.fetchall()

        if all_movies:
            from tmdb import lookup_tmdb, parse_filename, normalize_title, strip_article, format_stremio_stream, extract_media_specs
            for mrow in all_movies:
                fn = mrow["file_name"]
                curr_title = mrow["title"]
                vid_id = mrow["id"]
                parsed = parse_filename(fn)
                clean_parsed_title = parsed.get("raw_title", "")
                
                # Check if current title is misaligned with the parsed title
                norm_curr = normalize_title(strip_article(curr_title))
                norm_parsed = normalize_title(strip_article(clean_parsed_title))
                
                if norm_curr and norm_parsed and (norm_curr not in norm_parsed and norm_parsed not in norm_curr):
                    print(f"[Library Healing] Misassigned title detected on #{vid_id}: '{curr_title}' vs filename '{clean_parsed_title}'. Re-resolving...")
                    hit = await lookup_tmdb(fn, parsed_override=parsed)
                    if hit:
                        y_str = f" ({hit['year']})" if hit.get("year") else ""
                        header = f"{hit['title']}{y_str}"
                        specs = extract_media_specs(fn)
                        badge, desc = format_stremio_stream(header, specs)
                        await db.execute("""
                            UPDATE videos SET
                                tmdb_id = ?,
                                imdb_id = ?,
                                title = ?,
                                year = ?,
                                poster = ?,
                                backdrop = ?,
                                overview = ?,
                                rating = ?,
                                genres = ?,
                                stream_badge = ?,
                                stream_desc = ?
                            WHERE id = ?
                        """, (
                            hit["tmdb_id"], hit.get("imdb_id"), hit["title"], hit.get("year"),
                            hit["poster"], hit.get("backdrop"), hit.get("overview"),
                            hit.get("rating"), ",".join(hit.get("genres", [])) if isinstance(hit.get("genres"), list) else hit.get("genres"),
                            badge, desc, vid_id
                        ))
                        await db.commit()
                        print(f"[Library Healed Misassignment] #{vid_id} '{fn}' -> '{hit['title']}' ({hit.get('year')}) [TMDB: {hit.get('tmdb_id')}, IMDb: {hit.get('imdb_id')}]")
