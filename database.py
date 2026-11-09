import os
import re
import time
import aiosqlite
from config import DATA_DIR, PUBLIC_URL

DB_FILE = os.path.join(DATA_DIR, "library.db")

def is_uuid_str(val: str) -> bool:
    clean = str(val).strip()
    return bool(re.match(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$', clean)) or \
           bool(re.match(r'^[0-9a-fA-F]{32}$', clean))

def parse_stremio_id(item_id: str):
    """
    Safely parses Stremio ID formats:
      - 'stash_12345' / 'stash:12345' -> ("stash", 'stash_12345', None, None)
      - 'stashdb_12345' / 'stashdb:12345' -> ("stash", 'stash_12345', None, None)
      - '01a1b034-095c-7781-b0b7-ac3519d83112' (UUID) -> ("stash", 'stash_01a1b034-095c-7781-b0b7-ac3519d83112', None, None)
      - 'tt1234567' -> ("imdb", 'tt1234567', None, None)
      - 'tt1234567:1:2' -> ("imdb", 'tt1234567', 1, 2)
      - 'tmdb:1032906' -> ("tmdb", 1032906, None, None)
      - 'tmdb:1032906:1:2' -> ("tmdb", 1032906, 1, 2)
    """
    item_str = str(item_id).strip()
    if item_str.startswith("stash_") or item_str.startswith("stash:"):
        clean_id = item_str.replace("stash:", "stash_")
        return "stash", clean_id, None, None
    elif item_str.startswith("stashdb_") or item_str.startswith("stashdb:"):
        clean_id = "stash_" + item_str.replace("stashdb:", "").replace("stashdb_", "")
        return "stash", clean_id, None, None
    elif is_uuid_str(item_str):
        return "stash", f"stash_{item_str}", None, None
    elif item_str.startswith("tmdb:"):
        rest = item_str[5:]
        parts = rest.split(":")
        tmdb_id = int(parts[0]) if parts[0].isdigit() else 0
        season = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else None
        episode = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else None
        return "tmdb", tmdb_id, season, episode
    elif item_str.startswith("tt"):
        parts = item_str.split(":")
        imdb_id = parts[0]
        season = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else None
        episode = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else None
        return "imdb", imdb_id, season, episode
    else:
        parts = item_str.split(":")
        val = int(parts[0]) if parts[0].isdigit() else 0
        season = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else None
        episode = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else None
        return "tmdb", val, season, episode

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
            "stream_desc TEXT",
            "stash_id TEXT"
        ]:
            try:
                await db.execute(f"ALTER TABLE videos ADD COLUMN {col_def}")
            except Exception:
                pass

        await db.execute("CREATE INDEX IF NOT EXISTS idx_imdb_id ON videos(imdb_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tmdb_id ON videos(tmdb_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_stash_id ON videos(stash_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_media_type ON videos(media_type)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_file_lookup ON videos(file_name, file_size)")

        # Persistent Manual Title Corrections Table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS title_corrections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                message_id INTEGER,
                file_name TEXT,
                series_key TEXT,
                source_title TEXT,
                media_type TEXT NOT NULL,
                tmdb_id INTEGER NOT NULL,
                imdb_id TEXT,
                canonical_title TEXT NOT NULL,
                year INTEGER,
                poster TEXT,
                backdrop TEXT,
                overview TEXT,
                rating REAL,
                genres TEXT,
                user_query TEXT,
                created_at INTEGER NOT NULL
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_corr_msg ON title_corrections(chat_id, message_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_corr_file ON title_corrections(file_name)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_corr_series ON title_corrections(series_key)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_corr_canonical ON title_corrections(canonical_title)")

        await db.commit()

async def upsert_video(
    chat_id: int,
    message_id: int,
    file_name: str,
    file_size: int,
    mime_type: str,
    tmdb_info: dict | None = None,
    specs: dict | None = None,
    stream_badge: str | None = None,
    stream_desc: str | None = None,
    stash_info: dict | None = None
):
    now = int(time.time())
    specs = specs or {}
    tmdb_info = tmdb_info or {}
    langs_str = ", ".join(specs.get("languages", [])) if isinstance(specs.get("languages"), list) else (specs.get("languages") or "")

    is_adult = bool(stash_info or tmdb_info.get("media_type") == "porn" or tmdb_info.get("stash_id"))
    active_info = stash_info if stash_info else tmdb_info

    media_type = "porn" if is_adult else active_info.get("media_type", "movie")
    stash_id = active_info.get("stash_id") or active_info.get("id") if is_adult else active_info.get("stash_id")
    tmdb_id = active_info.get("tmdb_id") if not is_adult else None
    imdb_id = active_info.get("imdb_id") if not is_adult else None
    title = active_info.get("title", file_name)
    episode_title = active_info.get("episode_title") if not is_adult else None
    year = active_info.get("year")
    season = active_info.get("season") if not is_adult else None
    episode = active_info.get("episode") if not is_adult else None
    poster = active_info.get("poster")
    backdrop = active_info.get("backdrop")
    overview = active_info.get("overview", "")
    rating = active_info.get("rating", 0.0)
    genres_raw = active_info.get("genres", [])
    genres = ",".join(genres_raw) if isinstance(genres_raw, list) else (genres_raw or "")
    duration = specs.get("duration") or active_info.get("duration")

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
                tmdb_id, imdb_id, stash_id, media_type, title, episode_title, year, season, episode,
                poster, backdrop, overview, rating, genres, indexed_at,
                resolution, codec, audio, languages, subtitles, duration,
                stream_badge, stream_desc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id, message_id) DO UPDATE SET
                file_name = excluded.file_name,
                file_size = excluded.file_size,
                mime_type = excluded.mime_type,
                tmdb_id = excluded.tmdb_id,
                imdb_id = excluded.imdb_id,
                stash_id = excluded.stash_id,
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
            tmdb_id,
            imdb_id,
            stash_id,
            media_type,
            title,
            episode_title,
            year,
            season,
            episode,
            poster,
            backdrop,
            overview,
            rating,
            genres,
            now,
            specs.get("resolution"),
            specs.get("codec"),
            specs.get("audio"),
            langs_str,
            specs.get("subtitles"),
            duration,
            stream_badge,
            stream_desc
        ))
        await db.commit()

async def get_catalog_items(media_type: str = "movie", limit: int = 100, skip: int = 0) -> list[dict]:
    """
    Returns grouped list of titles for the Stremio catalog row.
    Strictly isolated: Series will only appear in tg_series, Movies only in tg_movies, Adult in tg_adult.
    """
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        query = """
            SELECT 
                CASE 
                    WHEN stash_id IS NOT NULL AND stash_id != '' THEN 
                        (CASE WHEN stash_id LIKE 'stash_%' THEN stash_id ELSE 'stash_' || stash_id END)
                    WHEN imdb_id IS NOT NULL AND imdb_id != '' THEN imdb_id
                    WHEN tmdb_id IS NOT NULL THEN 'tmdb:' || tmdb_id
                    ELSE 'vid_' || id
                END as stremio_id,
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
            GROUP BY stremio_id
            ORDER BY latest_add DESC
            LIMIT ? OFFSET ?
        """
        async with db.execute(query, (media_type, limit, skip)) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

async def get_meta_by_id(item_id: str, media_type: str) -> dict | None:
    """
    Fetches rich metadata for an item. Handles 'tt1234567', 'tmdb:12345', or 'stash_12345'.
    For series, orders and attaches available library episodes without duplicates.
    """
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        
        id_type, id_val, season, episode = parse_stremio_id(item_id)

        if id_type == "stash" or media_type == "porn":
            raw_uuid = str(id_val).replace("stash_", "").replace("stash:", "").replace("stashdb_", "").replace("stashdb:", "").strip()
            query = "SELECT * FROM videos WHERE (stash_id = ? OR stash_id = ? OR stash_id = ?) AND media_type = 'porn' ORDER BY id DESC LIMIT 1"
            params = (str(item_id), f"stash_{raw_uuid}", raw_uuid)
        elif id_type == "imdb":
            query = f"SELECT * FROM videos WHERE imdb_id = ? AND media_type = ? ORDER BY id DESC LIMIT 1"
            params = (id_val, media_type)
        else:
            query = f"SELECT * FROM videos WHERE tmdb_id = ? AND media_type = ? ORDER BY id DESC LIMIT 1"
            params = (id_val, media_type)

        async with db.execute(query, params) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            res = dict(row)
            
            # If it's a TV series, attach distinct episodes in the library
            if media_type == "series":
                condition = "imdb_id = ?" if id_type == "imdb" else "tmdb_id = ?"
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
      - Adult / StashDB: 'stash_12345'
    Deduplicates identical forwarded messages while preserving distinct releases/qualities.
    """
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        
        id_type, id_val, season, episode = parse_stremio_id(item_id)

        if id_type == "stash" or media_type == "porn":
            raw_uuid = str(id_val).replace("stash_", "").replace("stash:", "").replace("stashdb_", "").replace("stashdb:", "").strip()
            query = """
                SELECT * FROM videos 
                WHERE (stash_id = ? OR stash_id = ? OR stash_id = ?) AND media_type = 'porn'
                ORDER BY file_size DESC
            """
            params = (str(item_id), f"stash_{raw_uuid}", raw_uuid)
        elif media_type == "series" and season is not None and episode is not None:
            condition = "imdb_id = ?" if id_type == "imdb" else "tmdb_id = ?"
            query = f"""
                SELECT * FROM videos 
                WHERE {condition} AND media_type = 'series' AND season = ? AND episode = ?
                ORDER BY file_size DESC
            """
            params = (id_val, season, episode)
        else:
            condition = "imdb_id = ?" if id_type == "imdb" else "tmdb_id = ?"
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

            # Strict auto-healing fallback for StashDB scenes:
            # If not matched directly by UUID, fetch scene metadata from StashDB by UUID.
            # Match ONLY if an unlinked local video (no valid remote UUID yet) has the EXACT scene title
            # and matching date. NEVER do loose substring, partial, or wildcard matching!
            if not unique_videos and (id_type == "stash" or media_type == "porn") and raw_uuid and not raw_uuid.startswith("local_"):
                try:
                    from stashdb import get_stashdb_scene_by_id, normalize_scene_title, extract_scene_title_and_date
                    scene_meta = await get_stashdb_scene_by_id(raw_uuid)
                    if scene_meta and scene_meta.get("title"):
                        target_title = scene_meta["title"].strip()
                        target_norm = normalize_scene_title(target_title)
                        target_date = (scene_meta.get("date") or "").strip()

                        # ONLY inspect unlinked videos (stash_id is NULL, empty, or stash_local_*)
                        # NEVER overwrite or touch videos already linked to a real StashDB UUID!
                        query_unlinked = """
                            SELECT * FROM videos 
                            WHERE media_type = 'porn' 
                              AND (stash_id IS NULL OR stash_id = '' OR stash_id LIKE 'stash_local_%')
                            ORDER BY file_size DESC
                        """
                        async with db.execute(query_unlinked) as unlinked_cur:
                            cand_rows = await unlinked_cur.fetchall()
                            matched_ids = []
                            for r in cand_rows:
                                v = dict(r)
                                fn_title, fn_date = extract_scene_title_and_date(v["file_name"])
                                v_title = v.get("title") or ""
                                
                                # Strict title match check (normalized)
                                title_match = (
                                    normalize_scene_title(fn_title) == target_norm or
                                    normalize_scene_title(v_title) == target_norm
                                )
                                if not title_match:
                                    continue
                                
                                # Strict date match check:
                                # If both the StashDB scene and the file specify a date, they MUST match.
                                if target_date and fn_date:
                                    if fn_date != target_date:
                                        continue

                                sig = (v["file_name"], v["file_size"])
                                if sig not in seen_signatures:
                                    seen_signatures.add(sig)
                                    unique_videos.append(v)
                                    matched_ids.append(v["id"])

                            # Safely bind ONLY the verified matching video rows to this UUID
                            if matched_ids:
                                placeholders = ",".join("?" * len(matched_ids))
                                update_sql = f"UPDATE videos SET stash_id = ? WHERE id IN ({placeholders})"
                                await db.execute(update_sql, [f"stash_{raw_uuid}"] + matched_ids)
                                await db.commit()
                except Exception as fb_err:
                    print(f"[Notice] Stash on-demand stream resolution note: {fb_err}")

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
        async with db.execute("SELECT COUNT(*) FROM videos WHERE media_type = 'porn'") as c4:
            porn = (await c4.fetchone())[0]
        return {
            "total_videos": total,
            "movies": movies,
            "series_episodes": series,
            "adult_videos": porn
        }

async def clear_library_database():
    """Wipes indexed videos to allow a clean re-scan."""
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM videos")
        await db.commit()

async def reset_all_data():
    """Wipes all library videos for a clean fresh start."""
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM videos")
        try:
            await db.execute("DELETE FROM sqlite_sequence")
        except Exception:
            pass
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
            s_corr = await get_series_manual_correction(st)
            if s_corr:
                genres_str = ",".join(s_corr.get("genres", [])) if isinstance(s_corr.get("genres"), list) else (s_corr.get("genres") or "")
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
                    s_corr["tmdb_id"], s_corr.get("imdb_id"), s_corr.get("year"), s_corr.get("poster"),
                    s_corr.get("backdrop"), s_corr.get("overview"), s_corr.get("rating"), genres_str,
                    st
                ))
                continue

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
            SELECT id, chat_id, message_id, file_name, title, media_type
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
                # If this movie has a manual correction, don't overwrite with fallback
                m_corr = await get_manual_correction(frow["chat_id"], frow["message_id"], fn)
                if m_corr:
                    continue
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

        # 3. Heal any movie where TMDB resolves to a more accurate metadata match
        # (e.g. 'Fly Girls (2010)' exact year vs undated documentary, 'The Handmaiden', 'Pirates II', 'Beast')
        cur_all_movies = await db.execute("SELECT id, chat_id, message_id, file_name, title, media_type, tmdb_id, imdb_id, year FROM videos WHERE media_type = 'movie'")
        all_movies = await cur_all_movies.fetchall()

        if all_movies:
            from tmdb import lookup_tmdb, parse_filename, format_stremio_stream, extract_media_specs
            for mrow in all_movies:
                fn = mrow["file_name"]
                curr_tmdb_id = mrow["tmdb_id"]
                vid_id = mrow["id"]

                # Preserve manual corrections: NEVER overwrite with automatic lookup!
                m_corr = await get_manual_correction(mrow["chat_id"], mrow["message_id"], fn)
                if m_corr:
                    continue

                parsed = parse_filename(fn)

                hit = await lookup_tmdb(fn, parsed_override=parsed)
                if hit and hit.get("tmdb_id") and hit["tmdb_id"] != curr_tmdb_id:
                    print(f"[Library Healing] Disambiguated #{vid_id} '{fn}': TMDB {curr_tmdb_id} -> '{hit['title']}' (TMDB: {hit['tmdb_id']}, IMDb: {hit.get('imdb_id')})")
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


# ---------------------------------------------------------------------
# Manual Title Corrections Database API
# ---------------------------------------------------------------------

def normalize_title(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"[^a-z0-9]", "", s.lower())

def _row_to_tmdb_info(row: dict) -> dict:
    genres_raw = row.get("genres", "") or ""
    genres_list = [g.strip() for g in genres_raw.split(",") if g.strip()] if isinstance(genres_raw, str) else genres_raw
    return {
        "tmdb_id": row["tmdb_id"],
        "imdb_id": row["imdb_id"],
        "media_type": row["media_type"],
        "title": row["canonical_title"],
        "year": row["year"],
        "poster": row["poster"],
        "backdrop": row["backdrop"],
        "overview": row["overview"] or "",
        "rating": row["rating"] or 0.0,
        "genres": genres_list or [],
        "is_manual_correction": True,
        "user_query": row.get("user_query")
    }

async def save_movie_correction(chat_id: int, message_id: int, file_name: str, tmdb_info: dict, user_query: str):
    genres_str = ",".join(tmdb_info["genres"]) if isinstance(tmdb_info.get("genres"), list) else (tmdb_info.get("genres") or "")
    now = int(time.time())
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("DELETE FROM title_corrections WHERE chat_id = ? AND message_id = ?", (chat_id, message_id))
        await db.execute("""
            INSERT INTO title_corrections (
                chat_id, message_id, file_name, series_key, source_title,
                media_type, tmdb_id, imdb_id, canonical_title, year,
                poster, backdrop, overview, rating, genres, user_query, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            chat_id,
            message_id,
            file_name,
            None,
            tmdb_info["title"],
            "movie",
            tmdb_info["tmdb_id"],
            tmdb_info.get("imdb_id"),
            tmdb_info["title"],
            tmdb_info.get("year"),
            tmdb_info.get("poster"),
            tmdb_info.get("backdrop"),
            tmdb_info.get("overview"),
            tmdb_info.get("rating"),
            genres_str,
            user_query,
            now
        ))
        await db.commit()

async def save_series_correction(source_title: str, tmdb_info: dict, user_query: str, raw_title: str | None = None, chat_id: int | None = None, message_id: int | None = None):
    genres_str = ",".join(tmdb_info["genres"]) if isinstance(tmdb_info.get("genres"), list) else (tmdb_info.get("genres") or "")
    now = int(time.time())

    primary_key = normalize_title(raw_title or source_title)

    keys_to_save = {primary_key}
    if raw_title:
        keys_to_save.add(normalize_title(raw_title))

    async with aiosqlite.connect(DB_FILE) as db:
        if chat_id is not None and message_id is not None:
            await db.execute("DELETE FROM title_corrections WHERE chat_id = ? AND message_id = ?", (chat_id, message_id))
        for k in keys_to_save:
            if not k:
                continue
            await db.execute("DELETE FROM title_corrections WHERE series_key = ?", (k,))
            await db.execute("""
                INSERT INTO title_corrections (
                    chat_id, message_id, file_name, series_key, source_title,
                    media_type, tmdb_id, imdb_id, canonical_title, year,
                    poster, backdrop, overview, rating, genres, user_query, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                chat_id,
                message_id,
                None,
                k,
                source_title,
                "series",
                tmdb_info["tmdb_id"],
                tmdb_info.get("imdb_id"),
                tmdb_info["title"],
                tmdb_info.get("year"),
                tmdb_info.get("poster"),
                tmdb_info.get("backdrop"),
                tmdb_info.get("overview"),
                tmdb_info.get("rating"),
                genres_str,
                user_query,
                now
            ))
        await db.commit()

async def get_manual_correction(chat_id: int | None = None, message_id: int | None = None, file_name: str | None = None) -> dict | None:
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        if chat_id is not None and message_id is not None:
            async with db.execute(
                "SELECT * FROM title_corrections WHERE chat_id = ? AND message_id = ? ORDER BY id DESC LIMIT 1",
                (chat_id, message_id)
            ) as cur:
                row = await cur.fetchone()
                if row:
                    return _row_to_tmdb_info(dict(row))
        if file_name:
            async with db.execute(
                "SELECT * FROM title_corrections WHERE file_name = ? ORDER BY id DESC LIMIT 1",
                (file_name,)
            ) as cur:
                row = await cur.fetchone()
                if row:
                    return _row_to_tmdb_info(dict(row))
    return None

async def get_series_manual_correction(raw_title: str) -> dict | None:
    if not raw_title:
        return None
    k = normalize_title(raw_title)
    if not k:
        return None
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM title_corrections 
            WHERE media_type = 'series' AND (series_key = ? OR LOWER(source_title) = LOWER(?) OR LOWER(canonical_title) = LOWER(?))
            ORDER BY id DESC LIMIT 1
        """, (k, raw_title.strip(), raw_title.strip())) as cur:
            row = await cur.fetchone()
            if row:
                return _row_to_tmdb_info(dict(row))
    return None

async def apply_movie_correction_to_db(chat_id: int, message_id: int, tmdb_info: dict) -> bool:
    from tmdb import format_stremio_stream, extract_media_specs
    genres_str = ",".join(tmdb_info["genres"]) if isinstance(tmdb_info.get("genres"), list) else (tmdb_info.get("genres") or "")

    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM videos WHERE chat_id = ? AND message_id = ?", (chat_id, message_id)) as cur:
            row = await cur.fetchone()
            if not row:
                return False
            v = dict(row)

        y_str = f" ({tmdb_info['year']})" if tmdb_info.get("year") else ""
        header = f"{tmdb_info['title']}{y_str}"
        specs = extract_media_specs(f"{v['file_name']}\n{v.get('overview', '')}", file_size=v.get("file_size", 0))
        badge, desc = format_stremio_stream(header, specs)

        await db.execute("""
            UPDATE videos SET
                media_type = 'movie',
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
            WHERE chat_id = ? AND message_id = ?
        """, (
            tmdb_info["tmdb_id"],
            tmdb_info.get("imdb_id"),
            tmdb_info["title"],
            tmdb_info.get("year"),
            tmdb_info.get("poster"),
            tmdb_info.get("backdrop"),
            tmdb_info.get("overview"),
            tmdb_info.get("rating"),
            genres_str,
            badge,
            desc,
            chat_id,
            message_id
        ))
        await db.commit()
        return True

async def apply_series_correction_to_db(series_key: str, old_title: str, tmdb_info: dict, chat_id: int | None = None, message_id: int | None = None) -> int:
    from tmdb import format_stremio_stream, extract_media_specs, parse_filename
    genres_str = ",".join(tmdb_info["genres"]) if isinstance(tmdb_info.get("genres"), list) else (tmdb_info.get("genres") or "")

    norm_key = normalize_title(series_key)
    norm_old = normalize_title(old_title)

    updated_count = 0
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM videos") as cur:
            rows = await cur.fetchall()
            matching_items = []
            for r in rows:
                v = dict(r)
                # Match if replied message directly
                if chat_id is not None and message_id is not None and v["chat_id"] == chat_id and v["message_id"] == message_id:
                    matching_items.append(v)
                    continue

                v_title_norm = normalize_title(v.get("title", ""))
                v_fn = v.get("file_name", "")
                v_parsed = parse_filename(v_fn)
                v_raw_norm = normalize_title(v_parsed.get("raw_title", ""))

                if norm_key and (v_raw_norm == norm_key or v_title_norm == norm_key):
                    matching_items.append(v)
                elif not norm_key and norm_old and (v_raw_norm == norm_old or v_title_norm == norm_old):
                    matching_items.append(v)

            for v in matching_items:
                s = v.get("season")
                e = v.get("episode")
                if s is None or e is None:
                    parsed = parse_filename(v.get("file_name", ""))
                    s = s if s is not None else (parsed.get("season") or 1)
                    e = e if e is not None else (parsed.get("episode") or 1)

                ep_t = v.get("episode_title") or f"Episode {e}"
                header = f"{tmdb_info['title']} - S{s:02d}E{e:02d} - {ep_t}"
                specs = extract_media_specs(f"{v['file_name']}\n{v.get('overview', '')}", file_size=v.get("file_size", 0))
                badge, desc = format_stremio_stream(header, specs)

                await db.execute("""
                    UPDATE videos SET
                        media_type = 'series',
                        tmdb_id = ?,
                        imdb_id = ?,
                        title = ?,
                        year = ?,
                        season = ?,
                        episode = ?,
                        poster = ?,
                        backdrop = ?,
                        overview = ?,
                        rating = ?,
                        genres = ?,
                        stream_badge = ?,
                        stream_desc = ?
                    WHERE id = ?
                """, (
                    tmdb_info["tmdb_id"],
                    tmdb_info.get("imdb_id"),
                    tmdb_info["title"],
                    tmdb_info.get("year"),
                    s,
                    e,
                    tmdb_info.get("poster"),
                    tmdb_info.get("backdrop"),
                    tmdb_info.get("overview"),
                    tmdb_info.get("rating"),
                    genres_str,
                    badge,
                    desc,
                    v["id"]
                ))
                updated_count += 1

        await db.commit()
    return updated_count

async def get_all_corrections() -> list[dict]:
    async with aiosqlite.connect(DB_FILE) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM title_corrections ORDER BY id DESC") as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]
