import os
import sys
import unittest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
import httpx
import aiosqlite

# Force UTF-8 encoding
sys.stdout.reconfigure(encoding='utf-8')

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database import (
    DB_FILE,
    init_db,
    upsert_video,
    get_video_by_location,
    get_catalog_items,
    get_meta_by_id,
    get_streams_by_id,
    clear_library_database,
    save_movie_correction,
    save_series_correction,
    get_manual_correction,
    get_series_manual_correction,
    apply_movie_correction_to_db,
    apply_series_correction_to_db,
    get_all_corrections,
    reconcile_library
)
from tmdb import (
    compute_title_similarity,
    resolve_tmdb_by_hint,
    parse_filename,
    format_stremio_stream,
    extract_media_specs,
    get_series_cache,
    set_series_cache,
    clear_series_cache
)
from telegram_client import (
    index_single_message,
    apply_manual_title_correction,
    handle_reply_title_correction,
    _bot_reply_map
)
from settings_manager import save_settings, load_settings

class ManualTitleCorrectionTestSuite(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        _bot_reply_map.clear()
        self._orig_settings = load_settings().copy()
        save_settings({
            "api_id": 12345,
            "api_hash": "testhash",
            "bot_token": "testtoken",
            "tmdb_api_key": "test_tmdb_key",
            "configured": False
        })
        await init_db()
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("DELETE FROM videos")
            await db.execute("DELETE FROM title_corrections")
            await db.commit()

    async def asyncTearDown(self):
        _bot_reply_map.clear()
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("DELETE FROM videos")
            await db.execute("DELETE FROM title_corrections")
            await db.commit()
        if hasattr(self, "_orig_settings") and self._orig_settings:
            save_settings(self._orig_settings)

    # -----------------------------------------------------------------
    # 1. Spelling Variations & Similarity Metric Tests
    # -----------------------------------------------------------------
    def test_01_spelling_variation_similarity(self):
        # Exactly the user requirement example: "Naalaam Mura" vs "Naalam Mura"
        sim_naalam = compute_title_similarity("Naalaam Mura", "Naalam Mura")
        self.assertGreaterEqual(sim_naalam, 0.90)

        # Transliteration / typo in TV shows: "Breakin Bad" vs "Breaking Bad"
        sim_bb = compute_title_similarity("Breakin Bad", "Breaking Bad")
        self.assertGreaterEqual(sim_bb, 0.90)

        # Spacing / compound variation: "Velli Moonga" vs "Vellimoonga"
        sim_vm = compute_title_similarity("Velli Moonga", "Vellimoonga")
        self.assertGreaterEqual(sim_vm, 0.95)

        # Missing word / preposition: "House of dragon" vs "House of the Dragon"
        sim_hotd = compute_title_similarity("House of dragon", "House of the Dragon")
        self.assertGreaterEqual(sim_hotd, 0.85)

        # Completely unrelated movie: "Naalaam Mura" vs "The Dark Knight"
        sim_unrelated = compute_title_similarity("Naalaam Mura", "The Dark Knight")
        self.assertLess(sim_unrelated, 0.35)

    # -----------------------------------------------------------------
    # 2. TMDB Validation: Approximate Matches & Rejection of Nonsense
    # -----------------------------------------------------------------
    async def test_02_resolve_tmdb_by_hint_validation(self):
        mock_movie_results = {
            "results": [
                {
                    "id": 1032906,
                    "title": "Naalam Mura",
                    "original_title": "Naalam Mura",
                    "release_date": "2022-12-23",
                    "poster_path": "/naalam_poster.jpg",
                    "backdrop_path": "/naalam_backdrop.jpg",
                    "overview": "A tense thriller involving a hostage and an investigator.",
                    "vote_average": 6.8,
                    "popularity": 15.2
                }
            ]
        }
        mock_ext_ids = {"imdb_id": "tt24135508"}

        def mock_transport_handler(request: httpx.Request):
            url_str = str(request.url)
            if "search/movie" in url_str or "search/tv" in url_str:
                return httpx.Response(200, json=mock_movie_results)
            elif "external_ids" in url_str:
                return httpx.Response(200, json=mock_ext_ids)
            return httpx.Response(404, json={})

        transport = httpx.MockTransport(mock_transport_handler)
        async with httpx.AsyncClient(transport=transport) as mock_client:
            # Test A: Spelling variation "Naalaam Mura" resolves to "Naalam Mura" (2022)
            res = await resolve_tmdb_by_hint("Naalaam Mura", year_hint=2022, media_type_hint="movie", client=mock_client)
            self.assertIsNotNone(res)
            self.assertEqual(res["title"], "Naalam Mura")
            self.assertEqual(res["year"], 2022)
            self.assertEqual(res["tmdb_id"], 1032906)
            self.assertEqual(res["imdb_id"], "tt24135508")
            self.assertEqual(res["media_type"], "movie")

        # Test B: TMDB Validation rejects queries with no matching candidates / nonsense
        def mock_empty_handler(request: httpx.Request):
            return httpx.Response(200, json={"results": []})

        empty_transport = httpx.MockTransport(mock_empty_handler)
        async with httpx.AsyncClient(transport=empty_transport) as empty_client:
            res_none = await resolve_tmdb_by_hint("asdfghjklqwerty999", client=empty_client)
            self.assertIsNone(res_none)

    # -----------------------------------------------------------------
    # 3. Movie Manual Correction End-to-End
    # -----------------------------------------------------------------
    async def test_03_movie_manual_correction_end_to_end(self):
        chat_id = -100123456789
        msg_id = 42
        file_name = "Naalam.Mura.2022.1080p.WEBDL.x265.mkv"

        await upsert_video(
            chat_id=chat_id,
            message_id=msg_id,
            file_name=file_name,
            file_size=1500000000,
            mime_type="video/x-matroska",
            tmdb_info={
                "tmdb_id": 999999,
                "imdb_id": None,
                "media_type": "movie",
                "title": "Incorrect Raw Title",
                "year": 2020,
                "poster": "http://test/fallback.svg",
                "backdrop": None,
                "overview": "Old overview",
                "rating": 5.0,
                "genres": ["Drama"]
            },
            specs={"resolution": "1080p", "codec": "HEVC", "size_str": "1.4 GB"},
            stream_badge="[Teleflux ⚡] 1080p",
            stream_desc="📁 Incorrect Raw Title (2020)"
        )

        initial = await get_video_by_location(chat_id, msg_id)
        self.assertEqual(initial["title"], "Incorrect Raw Title")

        # Simulate Telegram user replying with spelling variation: "Naalaam Mura"
        mock_resolved = {
            "tmdb_id": 1032906,
            "imdb_id": "tt24135508",
            "media_type": "movie",
            "title": "Naalam Mura",
            "year": 2022,
            "poster": "https://image.tmdb.org/t/p/w500/poster.jpg",
            "backdrop": "https://image.tmdb.org/t/p/original/backdrop.jpg",
            "overview": "A tense interrogation thriller.",
            "rating": 7.2,
            "genres": ["Thriller", "Crime"]
        }

        with patch("telegram_client.resolve_tmdb_by_hint", new_callable=AsyncMock, return_value=mock_resolved):
            res = await apply_manual_title_correction(
                chat_id=chat_id,
                message_id=msg_id,
                title_hint="Naalaam Mura"
            )

        self.assertTrue(res["success"])
        self.assertEqual(res["title"], "Naalam Mura")
        self.assertEqual(res["tmdb_id"], 1032906)

        # Verify database reflects TMDB official title and metadata
        updated = await get_video_by_location(chat_id, msg_id)
        self.assertEqual(updated["title"], "Naalam Mura")
        self.assertEqual(updated["year"], 2022)
        self.assertEqual(updated["tmdb_id"], 1032906)
        self.assertEqual(updated["imdb_id"], "tt24135508")
        self.assertIn("Naalam Mura (2022)", updated["stream_desc"])

        # Verify Stremio stream query by IMDb ID works
        streams = await get_streams_by_id("tt24135508", media_type="movie")
        self.assertEqual(len(streams), 1)
        self.assertEqual(streams[0]["message_id"], msg_id)

    # -----------------------------------------------------------------
    # 4. Series-Wide Correction & Season/Episode Preservation
    # -----------------------------------------------------------------
    async def test_04_series_wide_correction_and_season_ep_preservation(self):
        chat_id = -100987654321
        episodes_data = [
            (101, "Brkng.Bd.S01E01.Pilot.720p.mkv", 1, 1, "Pilot"),
            (102, "Brkng.Bd.S01E02.Cat.In.The.Bag.720p.mkv", 1, 2, "Cat's in the Bag..."),
            (103, "Brkng.Bd.S01E03.And.The.Bags.720p.mkv", 1, 3, "...And the Bag's in the River"),
        ]

        for mid, fname, s, e, ept in episodes_data:
            await upsert_video(
                chat_id=chat_id,
                message_id=mid,
                file_name=fname,
                file_size=500000000,
                mime_type="video/x-matroska",
                tmdb_info={
                    "tmdb_id": 55555,
                    "imdb_id": None,
                    "media_type": "series",
                    "title": "Brkng Bd",
                    "season": s,
                    "episode": e,
                    "episode_title": ept,
                    "year": 2008,
                    "poster": "http://test/brkng.svg",
                    "rating": 8.0,
                    "genres": ["Drama"]
                },
                specs={"resolution": "720p", "codec": "AVC", "size_str": "500 MB"},
                stream_badge="[Teleflux ⚡] 720p",
                stream_desc=f"📁 Brkng Bd - S{s:02d}E{e:02d} - {ept}"
            )

        # Unrelated show in the library
        unrelated_mid = 201
        await upsert_video(
            chat_id=chat_id,
            message_id=unrelated_mid,
            file_name="Loki.S01E01.Glorious.Purpose.1080p.mkv",
            file_size=800000000,
            mime_type="video/x-matroska",
            tmdb_info={
                "tmdb_id": 84958,
                "imdb_id": "tt9140554",
                "media_type": "series",
                "title": "Loki",
                "season": 1,
                "episode": 1,
                "episode_title": "Glorious Purpose",
                "year": 2021,
                "poster": "https://image.tmdb.org/t/p/w500/loki.jpg",
                "rating": 8.2,
                "genres": ["Sci-Fi"]
            },
            specs={"resolution": "1080p", "codec": "HEVC", "size_str": "800 MB"},
            stream_badge="[Teleflux ⚡] 1080p",
            stream_desc="📁 Loki - S01E01 - Glorious Purpose"
        )

        mock_bb_resolved = {
            "tmdb_id": 1396,
            "imdb_id": "tt0903747",
            "media_type": "series",
            "title": "Breaking Bad",
            "year": 2008,
            "poster": "https://image.tmdb.org/t/p/w500/bb_poster.jpg",
            "backdrop": "https://image.tmdb.org/t/p/original/bb_backdrop.jpg",
            "overview": "A chemistry teacher diagnosed with lung cancer teams up with a former student.",
            "rating": 9.5,
            "genres": ["Drama", "Crime"]
        }

        with patch("telegram_client.resolve_tmdb_by_hint", new_callable=AsyncMock, return_value=mock_bb_resolved):
            res = await apply_manual_title_correction(
                chat_id=chat_id,
                message_id=101,
                title_hint="Breakin Bad"
            )

        self.assertTrue(res["success"])
        self.assertEqual(res["title"], "Breaking Bad")
        self.assertEqual(res["updated_episodes"], 3)

        # Verify ALL 3 episodes of Breaking Bad were updated with canonical TMDB title & ID
        for mid, _, s, e, _ in episodes_data:
            ep_row = await get_video_by_location(chat_id, mid)
            self.assertEqual(ep_row["title"], "Breaking Bad")
            self.assertEqual(ep_row["tmdb_id"], 1396)
            self.assertEqual(ep_row["imdb_id"], "tt0903747")
            # Verify Season and Episode numbers are strictly PRESERVED
            self.assertEqual(ep_row["season"], s)
            self.assertEqual(ep_row["episode"], e)
            self.assertIn(f"Breaking Bad - S{s:02d}E{e:02d}", ep_row["stream_desc"])

        # Verify UNRELATED show ("Loki") was NOT affected
        loki_row = await get_video_by_location(chat_id, unrelated_mid)
        self.assertEqual(loki_row["title"], "Loki")
        self.assertEqual(loki_row["tmdb_id"], 84958)
        self.assertEqual(loki_row["imdb_id"], "tt9140554")

    # -----------------------------------------------------------------
    # 5. Future Episodes Automatically Inherit Correction
    # -----------------------------------------------------------------
    async def test_05_future_episodes_inherit_correction(self):
        chat_id = -100987654321
        await save_series_correction(
            source_title="Brkng Bd",
            raw_title="Brkng Bd",
            tmdb_info={
                "tmdb_id": 1396,
                "imdb_id": "tt0903747",
                "media_type": "series",
                "title": "Breaking Bad",
                "year": 2008,
                "poster": "https://image.tmdb.org/t/p/w500/bb_poster.jpg",
                "backdrop": None,
                "overview": "Walter White saga",
                "rating": 9.5,
                "genres": ["Drama"]
            },
            user_query="Breakin Bad"
        )

        mock_future_msg = MagicMock()
        mock_future_msg.id = 104
        mock_future_msg.message = ""
        mock_future_msg.raw_text = ""
        mock_future_msg.media = MagicMock()
        mock_future_doc = MagicMock()
        mock_future_doc.size = 600000000
        mock_future_doc.mime_type = "video/mp4"

        from telethon.tl.types import DocumentAttributeFilename, DocumentAttributeVideo
        mock_future_doc.attributes = [
            DocumentAttributeFilename(file_name="Brkng.Bd.S01E04.Cancer.Man.720p.mp4"),
            DocumentAttributeVideo(duration=2800, w=1280, h=720)
        ]
        mock_future_msg.media.document = mock_future_doc

        indexed = await index_single_message(chat_id, mock_future_msg)
        self.assertTrue(indexed)

        future_ep = await get_video_by_location(chat_id, 104)
        self.assertIsNotNone(future_ep)
        self.assertEqual(future_ep["title"], "Breaking Bad")
        self.assertEqual(future_ep["tmdb_id"], 1396)
        self.assertEqual(future_ep["imdb_id"], "tt0903747")
        self.assertEqual(future_ep["season"], 1)
        self.assertEqual(future_ep["episode"], 4)
        self.assertIn("Breaking Bad - S01E04", future_ep["stream_desc"])

    # -----------------------------------------------------------------
    # 6. Persistence Across Container Restarts & Channel Rescans
    # -----------------------------------------------------------------
    async def test_06_persistence_across_restarts_and_rescans(self):
        chat_id = -100111222333
        msg_id = 77
        file_name = "Naalam.Mura.2022.mkv"

        await save_movie_correction(
            chat_id=chat_id,
            message_id=msg_id,
            file_name=file_name,
            tmdb_info={
                "tmdb_id": 1032906,
                "imdb_id": "tt24135508",
                "media_type": "movie",
                "title": "Naalam Mura",
                "year": 2022,
                "poster": "https://image.tmdb.org/t/p/w500/naalam.jpg",
                "backdrop": None,
                "overview": "Interrogation mystery",
                "rating": 6.8,
                "genres": ["Thriller"]
            },
            user_query="Naalaam Mura"
        )

        corr = await get_manual_correction(chat_id, msg_id)
        self.assertIsNotNone(corr)
        self.assertEqual(corr["title"], "Naalam Mura")
        self.assertEqual(corr["tmdb_id"], 1032906)

        # Trigger Channel Rescan (clears `videos` table)
        await clear_library_database()

        items_after_wipe = await get_catalog_items(media_type="movie")
        self.assertEqual(len(items_after_wipe), 0)

        # title_corrections survived rescan
        corr_after_wipe = await get_manual_correction(chat_id, msg_id)
        self.assertIsNotNone(corr_after_wipe)
        self.assertEqual(corr_after_wipe["title"], "Naalam Mura")

        # Reprocess message during rescan
        mock_rescan_msg = MagicMock()
        mock_rescan_msg.id = msg_id
        mock_rescan_msg.message = ""
        mock_rescan_msg.raw_text = ""
        mock_rescan_msg.media = MagicMock()
        mock_doc = MagicMock()
        mock_doc.size = 1200000000
        mock_doc.mime_type = "video/x-matroska"

        from telethon.tl.types import DocumentAttributeFilename, DocumentAttributeVideo
        mock_doc.attributes = [
            DocumentAttributeFilename(file_name=file_name),
            DocumentAttributeVideo(duration=7200, w=1920, h=1080)
        ]
        mock_rescan_msg.media.document = mock_doc

        indexed = await index_single_message(chat_id, mock_rescan_msg)
        self.assertTrue(indexed)

        reindexed_video = await get_video_by_location(chat_id, msg_id)
        self.assertEqual(reindexed_video["title"], "Naalam Mura")
        self.assertEqual(reindexed_video["tmdb_id"], 1032906)
        self.assertEqual(reindexed_video["imdb_id"], "tt24135508")

    # -----------------------------------------------------------------
    # 7. Priority Over Automatic Filename Parsing
    # -----------------------------------------------------------------
    async def test_07_priority_over_automatic_filename_parsing(self):
        chat_id = -100555
        msg_id = 88
        file_name = "Pushpa.The.Rule.2024.1080p.mkv"

        await save_movie_correction(
            chat_id=chat_id,
            message_id=msg_id,
            file_name=file_name,
            tmdb_info={
                "tmdb_id": 872585,
                "imdb_id": "tt16539454",
                "media_type": "movie",
                "title": "Pushpa 2: The Rule",
                "year": 2024,
                "poster": "https://image.tmdb.org/t/p/w500/pushpa2.jpg",
                "backdrop": None,
                "overview": "Pushpa Raj continues his reign.",
                "rating": 7.5,
                "genres": ["Action"]
            },
            user_query="Pushpa 2"
        )

        mock_msg = MagicMock()
        mock_msg.id = msg_id
        mock_msg.message = ""
        mock_msg.raw_text = ""
        mock_msg.media = MagicMock()
        mock_doc = MagicMock()
        mock_doc.size = 2000000000
        mock_doc.mime_type = "video/x-matroska"

        from telethon.tl.types import DocumentAttributeFilename, DocumentAttributeVideo
        mock_doc.attributes = [
            DocumentAttributeFilename(file_name=file_name),
            DocumentAttributeVideo(duration=7200, w=1920, h=1080)
        ]
        mock_msg.media.document = mock_doc

        indexed = await index_single_message(chat_id, mock_msg)
        self.assertTrue(indexed)

        v = await get_video_by_location(chat_id, msg_id)
        self.assertEqual(v["title"], "Pushpa 2: The Rule")
        self.assertEqual(v["tmdb_id"], 872585)

    # -----------------------------------------------------------------
    # 8. Library Reconcile Never Overwrites Manual Correction
    # -----------------------------------------------------------------
    async def test_08_reconcile_library_preserves_manual_correction(self):
        chat_id = -100777
        msg_id = 99
        file_name = "Tricky.Movie.Title.2021.mkv"

        await upsert_video(
            chat_id=chat_id,
            message_id=msg_id,
            file_name=file_name,
            file_size=1000000000,
            mime_type="video/mp4",
            tmdb_info={
                "tmdb_id": 777777,
                "imdb_id": "tt7777777",
                "media_type": "movie",
                "title": "Manually Corrected Movie",
                "year": 2021,
                "poster": "https://image.tmdb.org/t/p/w500/manual.jpg",
                "rating": 8.0,
                "genres": ["Drama"]
            },
            specs={"resolution": "1080p"},
            stream_badge="[Teleflux ⚡] 1080p",
            stream_desc="📁 Manually Corrected Movie (2021)"
        )

        await save_movie_correction(
            chat_id=chat_id,
            message_id=msg_id,
            file_name=file_name,
            tmdb_info={
                "tmdb_id": 777777,
                "imdb_id": "tt7777777",
                "media_type": "movie",
                "title": "Manually Corrected Movie",
                "year": 2021,
                "poster": "https://image.tmdb.org/t/p/w500/manual.jpg",
                "rating": 8.0,
                "genres": ["Drama"]
            },
            user_query="Manually Corrected Movie"
        )

        await reconcile_library()

        v = await get_video_by_location(chat_id, msg_id)
        self.assertEqual(v["title"], "Manually Corrected Movie")
        self.assertEqual(v["tmdb_id"], 777777)

    # -----------------------------------------------------------------
    # 9. Telegram Event Listener Handles Reply to Video
    # -----------------------------------------------------------------
    async def test_09_telegram_reply_event_listener_flow(self):
        chat_id = -100888999
        video_msg_id = 50
        file_name = "Naalam.Mura.2022.mkv"

        # Pre-index video
        await upsert_video(
            chat_id=chat_id,
            message_id=video_msg_id,
            file_name=file_name,
            file_size=1000000000,
            mime_type="video/mp4",
            tmdb_info={
                "tmdb_id": 111111,
                "media_type": "movie",
                "title": "Old Raw Title",
                "year": 2022
            }
        )

        # Mock reply message event
        mock_event = MagicMock()
        mock_event.chat_id = chat_id
        mock_event.is_channel = False
        mock_event.reply = AsyncMock()

        mock_reply_msg = MagicMock()
        mock_reply_msg.is_reply = True
        mock_reply_msg.message = "Naalaam Mura"
        mock_reply_msg.raw_text = "Naalaam Mura"
        mock_reply_msg.reply_to_msg_id = video_msg_id
        mock_event.message = mock_reply_msg

        # Mock replied video message
        mock_video_msg = MagicMock()
        mock_video_msg.id = video_msg_id
        mock_video_msg.fwd_from = None
        mock_event.get_reply_message = AsyncMock(return_value=mock_video_msg)

        mock_resolved = {
            "tmdb_id": 1032906,
            "imdb_id": "tt24135508",
            "media_type": "movie",
            "title": "Naalam Mura",
            "year": 2022,
            "poster": "https://image.tmdb.org/t/p/w500/naalam.jpg",
            "backdrop": None,
            "overview": "Hostage thriller",
            "rating": 7.0,
            "genres": ["Thriller"]
        }

        with patch("telegram_client.resolve_tmdb_by_hint", new_callable=AsyncMock, return_value=mock_resolved):
            res = await apply_manual_title_correction(
                chat_id=chat_id,
                message_id=video_msg_id,
                title_hint="Naalaam Mura",
                target_message=mock_video_msg
            )

        self.assertTrue(res["success"])
        self.assertEqual(res["title"], "Naalam Mura")
        self.assertEqual(res["tmdb_id"], 1032906)

        # Verify DB updated
        v = await get_video_by_location(chat_id, video_msg_id)
        self.assertEqual(v["title"], "Naalam Mura")

    # -----------------------------------------------------------------
    # 10. Message Edit Handling: Movie Reply Edit Flow
    # -----------------------------------------------------------------
    async def test_10_movie_reply_edited_flow(self):
        chat_id = -100555666
        video_msg_id = 150
        reply_msg_id = 999
        file_name = "Naalam.Mura.2022.1080p.mkv"

        # Pre-index video with incorrect title
        await upsert_video(
            chat_id=chat_id,
            message_id=video_msg_id,
            file_name=file_name,
            file_size=1500000000,
            mime_type="video/x-matroska",
            tmdb_info={
                "tmdb_id": 999999,
                "media_type": "movie",
                "title": "Wrong Title (2020)",
                "year": 2020
            }
        )

        mock_video_msg = MagicMock()
        mock_video_msg.id = video_msg_id
        mock_video_msg.fwd_from = None

        mock_bot_reply = MagicMock()
        mock_bot_reply.id = 888

        # Step 1: User sends initial reply with a typo/wrong hint
        mock_event_1 = MagicMock()
        mock_event_1.chat_id = chat_id
        mock_event_1.is_channel = False
        mock_event_1.reply = AsyncMock(return_value=mock_bot_reply)

        mock_msg_1 = MagicMock()
        mock_msg_1.id = reply_msg_id
        mock_msg_1.is_reply = True
        mock_msg_1.message = "Some Typo Movie"
        mock_msg_1.raw_text = "Some Typo Movie"
        mock_msg_1.reply_to_msg_id = video_msg_id
        mock_event_1.message = mock_msg_1
        mock_event_1.get_reply_message = AsyncMock(return_value=mock_video_msg)

        typo_resolved = {
            "tmdb_id": 111111,
            "imdb_id": "tt1111111",
            "media_type": "movie",
            "title": "Some Typo Movie",
            "year": 2019,
            "poster": "https://image.tmdb.org/t/p/w500/typo.jpg",
            "backdrop": None,
            "overview": "Typo movie overview",
            "rating": 5.0,
            "genres": ["Comedy"]
        }

        mock_telethon = MagicMock()
        mock_telethon.edit_message = AsyncMock()

        with patch("telegram_client.telethon_client", mock_telethon), \
             patch("telegram_client.resolve_tmdb_by_hint", new_callable=AsyncMock, return_value=typo_resolved):
            handled_1 = await handle_reply_title_correction(mock_event_1, is_edit=False)

        self.assertTrue(handled_1)
        mock_event_1.reply.assert_called_once()
        self.assertIn("Some Typo Movie", mock_event_1.reply.call_args[0][0])
        self.assertEqual(_bot_reply_map.get((chat_id, reply_msg_id)), 888)

        # Video in DB was temporarily updated to typo
        v1 = await get_video_by_location(chat_id, video_msg_id)
        self.assertEqual(v1["title"], "Some Typo Movie")

        # Step 2: User realizes typo and EDITS their reply in Telegram to "Naalaam Mura"
        mock_event_2 = MagicMock()
        mock_event_2.chat_id = chat_id
        mock_event_2.is_channel = False
        mock_event_2.reply = AsyncMock()

        mock_msg_2 = MagicMock()
        mock_msg_2.id = reply_msg_id  # Same Telegram message ID
        mock_msg_2.is_reply = True
        mock_msg_2.message = "Naalaam Mura"
        mock_msg_2.raw_text = "Naalaam Mura"
        mock_msg_2.reply_to_msg_id = video_msg_id
        mock_event_2.message = mock_msg_2
        mock_event_2.get_reply_message = AsyncMock(return_value=mock_video_msg)

        exact_resolved = {
            "tmdb_id": 1032906,
            "imdb_id": "tt24135508",
            "media_type": "movie",
            "title": "Naalam Mura",
            "year": 2022,
            "poster": "https://image.tmdb.org/t/p/w500/naalam.jpg",
            "backdrop": None,
            "overview": "Official hostage thriller",
            "rating": 6.8,
            "genres": ["Crime", "Thriller"]
        }

        with patch("telegram_client.telethon_client", mock_telethon), \
             patch("telegram_client.resolve_tmdb_by_hint", new_callable=AsyncMock, return_value=exact_resolved):
            handled_2 = await handle_reply_title_correction(mock_event_2, is_edit=True)

        self.assertTrue(handled_2)
        # Verify bot EDITED its previous confirmation message rather than sending a new reply
        mock_telethon.edit_message.assert_called_once()
        edit_args = mock_telethon.edit_message.call_args[0]
        self.assertEqual(edit_args[0], chat_id)
        self.assertEqual(edit_args[1], 888)  # Previous bot message ID
        self.assertIn("Naalam Mura", edit_args[2])
        self.assertIn("1032906", edit_args[2])
        self.assertIn("🔄 Updated to", edit_args[2])
        mock_event_2.reply.assert_not_called()

        # Verify DB is now re-modified to exact match from TMDB
        v2 = await get_video_by_location(chat_id, video_msg_id)
        self.assertEqual(v2["title"], "Naalam Mura")
        self.assertEqual(v2["year"], 2022)
        self.assertEqual(v2["tmdb_id"], 1032906)
        self.assertEqual(v2["imdb_id"], "tt24135508")
        self.assertIn("📁 Naalam Mura (2022)", v2["stream_desc"])

        # Verify persistent correction in title_corrections is also updated
        corr = await get_manual_correction(chat_id=chat_id, message_id=video_msg_id)
        self.assertIsNotNone(corr)
        self.assertEqual(corr["title"], "Naalam Mura")
        self.assertEqual(corr["tmdb_id"], 1032906)

    # -----------------------------------------------------------------
    # 11. Message Edit Handling: TV Series Reply Edit Flow
    # -----------------------------------------------------------------
    async def test_11_series_reply_edited_flow(self):
        chat_id = -100777888
        ep1_id = 201
        ep2_id = 202
        reply_msg_id = 300

        # Pre-index 2 episodes of Breaking Bad with raw filename
        await upsert_video(
            chat_id=chat_id,
            message_id=ep1_id,
            file_name="Breaking.Bad.S01E01.720p.mkv",
            file_size=500000000,
            mime_type="video/x-matroska",
            tmdb_info={
                "tmdb_id": 999901,
                "media_type": "series",
                "title": "Breakin Bad",
                "year": 2008,
                "season": 1,
                "episode": 1
            }
        )
        await upsert_video(
            chat_id=chat_id,
            message_id=ep2_id,
            file_name="Breaking.Bad.S01E02.720p.mkv",
            file_size=510000000,
            mime_type="video/x-matroska",
            tmdb_info={
                "tmdb_id": 999901,
                "media_type": "series",
                "title": "Breakin Bad",
                "year": 2008,
                "season": 1,
                "episode": 2
            }
        )

        # Also pre-index an UNRELATED show episode to ensure it's not affected
        unrelated_id = 299
        await upsert_video(
            chat_id=chat_id,
            message_id=unrelated_id,
            file_name="Better.Call.Saul.S01E01.720p.mkv",
            file_size=600000000,
            mime_type="video/x-matroska",
            tmdb_info={
                "tmdb_id": 60059,
                "media_type": "series",
                "title": "Better Call Saul",
                "year": 2015,
                "season": 1,
                "episode": 1
            }
        )

        mock_video_msg = MagicMock()
        mock_video_msg.id = ep1_id
        mock_video_msg.fwd_from = None

        mock_bot_reply = MagicMock()
        mock_bot_reply.id = 777

        # Step 1: User initially replies with typo hint "Dark"
        mock_event_1 = MagicMock()
        mock_event_1.chat_id = chat_id
        mock_event_1.is_channel = False
        mock_event_1.reply = AsyncMock(return_value=mock_bot_reply)

        mock_msg_1 = MagicMock()
        mock_msg_1.id = reply_msg_id
        mock_msg_1.is_reply = True
        mock_msg_1.message = "Dark"
        mock_msg_1.raw_text = "Dark"
        mock_msg_1.reply_to_msg_id = ep1_id
        mock_event_1.message = mock_msg_1
        mock_event_1.get_reply_message = AsyncMock(return_value=mock_video_msg)

        dark_resolved = {
            "tmdb_id": 70523,
            "imdb_id": "tt5753856",
            "media_type": "series",
            "title": "Dark",
            "year": 2017,
            "poster": "https://image.tmdb.org/t/p/w500/dark.jpg",
            "backdrop": None,
            "overview": "Sci-fi thriller",
            "rating": 8.7,
            "genres": ["Drama", "Sci-Fi"]
        }

        mock_telethon = MagicMock()
        mock_telethon.edit_message = AsyncMock()

        with patch("telegram_client.telethon_client", mock_telethon), \
             patch("telegram_client.resolve_tmdb_by_hint", new_callable=AsyncMock, return_value=dark_resolved):
            handled_1 = await handle_reply_title_correction(mock_event_1, is_edit=False)

        self.assertTrue(handled_1)
        self.assertEqual(_bot_reply_map.get((chat_id, reply_msg_id)), 777)

        # Step 2: User notices the typo and EDITS their reply to "Breaking Bad"
        mock_event_2 = MagicMock()
        mock_event_2.chat_id = chat_id
        mock_event_2.is_channel = False
        mock_event_2.reply = AsyncMock()

        mock_msg_2 = MagicMock()
        mock_msg_2.id = reply_msg_id
        mock_msg_2.is_reply = True
        mock_msg_2.message = "Breaking Bad"
        mock_msg_2.raw_text = "Breaking Bad"
        mock_msg_2.reply_to_msg_id = ep1_id
        mock_event_2.message = mock_msg_2
        mock_event_2.get_reply_message = AsyncMock(return_value=mock_video_msg)

        bb_resolved = {
            "tmdb_id": 1396,
            "imdb_id": "tt0903747",
            "media_type": "series",
            "title": "Breaking Bad",
            "year": 2008,
            "poster": "https://image.tmdb.org/t/p/w500/bb.jpg",
            "backdrop": None,
            "overview": "A chemistry teacher diagnosed with cancer turns to manufacturing meth.",
            "rating": 9.5,
            "genres": ["Drama", "Crime"]
        }

        with patch("telegram_client.telethon_client", mock_telethon), \
             patch("telegram_client.resolve_tmdb_by_hint", new_callable=AsyncMock, return_value=bb_resolved):
            handled_2 = await handle_reply_title_correction(mock_event_2, is_edit=True)

        self.assertTrue(handled_2)
        mock_telethon.edit_message.assert_called_once()
        edit_args = mock_telethon.edit_message.call_args[0]
        self.assertEqual(edit_args[1], 777)
        self.assertIn("Breaking Bad", edit_args[2])
        self.assertIn("1396", edit_args[2])
        self.assertIn("Updated 2 episode(s)", edit_args[2])

        # Verify all episodes of Breaking Bad are re-modified, preserving S01E01 and S01E02
        v_ep1 = await get_video_by_location(chat_id, ep1_id)
        self.assertEqual(v_ep1["title"], "Breaking Bad")
        self.assertEqual(v_ep1["tmdb_id"], 1396)
        self.assertEqual(v_ep1["season"], 1)
        self.assertEqual(v_ep1["episode"], 1)
        self.assertIn("Breaking Bad - S01E01", v_ep1["stream_desc"])

        v_ep2 = await get_video_by_location(chat_id, ep2_id)
        self.assertEqual(v_ep2["title"], "Breaking Bad")
        self.assertEqual(v_ep2["tmdb_id"], 1396)
        self.assertEqual(v_ep2["season"], 1)
        self.assertEqual(v_ep2["episode"], 2)
        self.assertIn("Breaking Bad - S01E02", v_ep2["stream_desc"])

        # Verify UNRELATED show was completely untouched!
        unrel = await get_video_by_location(chat_id, unrelated_id)
        self.assertEqual(unrel["title"], "Better Call Saul")
        self.assertEqual(unrel["tmdb_id"], 60059)

        # Verify future episode indexing inherits the edited correction
        msg_ep3 = MagicMock()
        msg_ep3.id = 203
        msg_ep3.message = ""
        msg_ep3.raw_text = ""
        msg_ep3.media = MagicMock()
        mock_doc_ep3 = MagicMock()
        mock_doc_ep3.size = 520000000
        mock_doc_ep3.mime_type = "video/x-matroska"
        from telethon.tl.types import DocumentAttributeFilename, DocumentAttributeVideo
        mock_doc_ep3.attributes = [
            DocumentAttributeFilename(file_name="Breaking.Bad.S01E03.720p.mkv"),
            DocumentAttributeVideo(duration=2800, w=1280, h=720)
        ]
        msg_ep3.media.document = mock_doc_ep3

        with patch("telegram_client.telethon_client", mock_telethon):
            indexed = await index_single_message(chat_id, msg_ep3)
        self.assertTrue(indexed)
        v_ep3 = await get_video_by_location(chat_id, 203)
        self.assertEqual(v_ep3["title"], "Breaking Bad")
        self.assertEqual(v_ep3["tmdb_id"], 1396)
        self.assertEqual(v_ep3["season"], 1)
        self.assertEqual(v_ep3["episode"], 3)

    # -----------------------------------------------------------------
    # 12. Message Edit: Typo From Invalid (Unmatched) to Valid Match
    # -----------------------------------------------------------------
    async def test_12_reply_edited_from_invalid_to_valid(self):
        chat_id = -100444333
        video_msg_id = 60
        reply_msg_id = 555
        file_name = "Naalam.Mura.2022.mkv"

        await upsert_video(
            chat_id=chat_id,
            message_id=video_msg_id,
            file_name=file_name,
            file_size=1000000000,
            mime_type="video/mp4",
            tmdb_info={
                "tmdb_id": 999123,
                "media_type": "movie",
                "title": "Old Movie",
                "year": 2022
            }
        )

        mock_video_msg = MagicMock()
        mock_video_msg.id = video_msg_id
        mock_video_msg.fwd_from = None

        mock_bot_reply = MagicMock()
        mock_bot_reply.id = 444

        # Step 1: User replies with accidental gibberish/unmatched typo
        mock_event_1 = MagicMock()
        mock_event_1.chat_id = chat_id
        mock_event_1.is_channel = False
        mock_event_1.reply = AsyncMock(return_value=mock_bot_reply)

        mock_msg_1 = MagicMock()
        mock_msg_1.id = reply_msg_id
        mock_msg_1.is_reply = True
        mock_msg_1.message = "asdfghjklqwerty999"
        mock_msg_1.raw_text = "asdfghjklqwerty999"
        mock_msg_1.reply_to_msg_id = video_msg_id
        mock_event_1.message = mock_msg_1
        mock_event_1.get_reply_message = AsyncMock(return_value=mock_video_msg)

        mock_telethon = MagicMock()
        mock_telethon.edit_message = AsyncMock()

        # resolve_tmdb_by_hint returns None for unmatched typo
        with patch("telegram_client.telethon_client", mock_telethon), \
             patch("telegram_client.resolve_tmdb_by_hint", new_callable=AsyncMock, return_value=None):
            handled_1 = await handle_reply_title_correction(mock_event_1, is_edit=False)

        self.assertTrue(handled_1)
        mock_event_1.reply.assert_called_once()
        self.assertIn("Title correction error", mock_event_1.reply.call_args[0][0])
        self.assertEqual(_bot_reply_map.get((chat_id, reply_msg_id)), 444)

        # Step 2: User edits reply to fix the typo: "Naalaam Mura"
        mock_event_2 = MagicMock()
        mock_event_2.chat_id = chat_id
        mock_event_2.is_channel = False
        mock_event_2.reply = AsyncMock()

        mock_msg_2 = MagicMock()
        mock_msg_2.id = reply_msg_id
        mock_msg_2.is_reply = True
        mock_msg_2.message = "Naalaam Mura"
        mock_msg_2.raw_text = "Naalaam Mura"
        mock_msg_2.reply_to_msg_id = video_msg_id
        mock_event_2.message = mock_msg_2
        mock_event_2.get_reply_message = AsyncMock(return_value=mock_video_msg)

        exact_resolved = {
            "tmdb_id": 1032906,
            "imdb_id": "tt24135508",
            "media_type": "movie",
            "title": "Naalam Mura",
            "year": 2022,
            "poster": "https://image.tmdb.org/t/p/w500/naalam.jpg",
            "backdrop": None,
            "overview": "Hostage thriller",
            "rating": 6.8,
            "genres": ["Thriller"]
        }

        with patch("telegram_client.telethon_client", mock_telethon), \
             patch("telegram_client.resolve_tmdb_by_hint", new_callable=AsyncMock, return_value=exact_resolved):
            handled_2 = await handle_reply_title_correction(mock_event_2, is_edit=True)

        self.assertTrue(handled_2)
        # Verify bot EDITED its previous error message into the confirmation message!
        mock_telethon.edit_message.assert_called_once()
        edit_args = mock_telethon.edit_message.call_args[0]
        self.assertEqual(edit_args[1], 444)
        self.assertIn("Naalam Mura", edit_args[2])
        self.assertIn("1032906", edit_args[2])

        # Verify DB is now updated to the exact TMDB match
        v = await get_video_by_location(chat_id, video_msg_id)
        self.assertEqual(v["title"], "Naalam Mura")
        self.assertEqual(v["tmdb_id"], 1032906)


if __name__ == "__main__":
    unittest.main()
