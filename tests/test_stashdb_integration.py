import os
import sys
import unittest
import asyncio
import httpx
from unittest.mock import patch, AsyncMock

# Force UTF-8 encoding
sys.stdout.reconfigure(encoding='utf-8')

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from main import app
from database import (
    init_db,
    upsert_video,
    get_catalog_items,
    get_meta_by_id,
    get_streams_by_id,
    get_library_stats,
    parse_stremio_id
)
from stashdb import (
    clean_stash_filename,
    format_stashdb_stream,
    generate_adult_poster_svg,
    select_media_images
)
from settings_manager import load_settings, save_settings

class StashDbIntegrationTestSuite(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await init_db()

    def test_01_filename_cleaning(self):
        self.assertEqual(clean_stash_filename("Brazzers.Exxtra.Mia.Malkova.1080p.HEVC.mp4"), "Brazzers Exxtra Mia Malkova")
        self.assertEqual(clean_stash_filename("EvilAngel - Hardcore Scene Title [1080p].mkv"), "EvilAngel - Hardcore Scene Title")
        self.assertEqual(clean_stash_filename("PureTaboo.Alexis.Texas.720p.mp4"), "PureTaboo Alexis Texas")

    def test_02_stream_formatter_and_poster_svg(self):
        badge, desc = format_stashdb_stream("Test Scene (2024)", {"resolution": "1080p", "codec": "HEVC", "size_str": "1.5 GB"})
        self.assertIn("1080p", badge)
        self.assertIn("Test Scene", desc)
        self.assertIn("1.5 GB", desc)

        svg = generate_adult_poster_svg("Alexis Texas Scene")
        self.assertIn(b"<svg", svg)
        self.assertIn(b"TELEFLUX RED", svg)
        self.assertIn(b"Alexis Texas Scene", svg)

    def test_03_stremio_id_parsing(self):
        id_type, id_val, s, e = parse_stremio_id("stash_01a1b034-095c-7781-b0b7-ac3519d83112")
        self.assertEqual(id_type, "stash")
        self.assertEqual(id_val, "stash_01a1b034-095c-7781-b0b7-ac3519d83112")

        id_type, id_val, s, e = parse_stremio_id("stash:01a1b034-095c-7781-b0b7-ac3519d83112")
        self.assertEqual(id_type, "stash")
        self.assertEqual(id_val, "stash_01a1b034-095c-7781-b0b7-ac3519d83112")

        id_type, id_val, s, e = parse_stremio_id("tmdb:1032906:1:2")
        self.assertEqual(id_type, "tmdb")
        self.assertEqual(id_val, 1032906)
        self.assertEqual(s, 1)
        self.assertEqual(e, 2)

        id_type, id_val, s, e = parse_stremio_id("tt1234567")
        self.assertEqual(id_type, "imdb")
        self.assertEqual(id_val, "tt1234567")

    async def test_04_upsert_and_retrieve_adult_video(self):
        mock_stash = {
            "stash_id": "stash_unit_test_uuid_999",
            "title": "Brazzers Unit Test Scene",
            "year": 2024,
            "poster": "https://images.stashdb.org/poster_test.jpg",
            "backdrop": "https://images.stashdb.org/bg_test.jpg",
            "overview": "Starring: Test Performer | Studio: Brazzers",
            "genres": ["Anal Sex", "Brazzers"],
            "rating": 0.0
        }

        await upsert_video(
            chat_id=-10099887766,
            message_id=999,
            file_name="Brazzers.Unit.Test.Scene.1080p.mp4",
            file_size=2000000000,
            mime_type="video/mp4",
            stash_info=mock_stash,
            specs={"resolution": "1080p", "codec": "AVC", "size_str": "1.86 GB"},
            stream_badge="[StashDB ⚡] 1080p AVC",
            stream_desc="📁 Brazzers Unit Test Scene"
        )

        # Check library stats
        stats = await get_library_stats()
        self.assertGreaterEqual(stats["adult_videos"], 1)

        # Check catalog
        items = await get_catalog_items(media_type="porn")
        matched = [i for i in items if i["stremio_id"] == "stash_unit_test_uuid_999"]
        self.assertEqual(len(matched), 1)
        self.assertEqual(matched[0]["title"], "Brazzers Unit Test Scene")

        # Check meta
        meta = await get_meta_by_id("stash_unit_test_uuid_999", media_type="porn")
        self.assertIsNotNone(meta)
        self.assertEqual(meta["title"], "Brazzers Unit Test Scene")

        # Check stream
        streams = await get_streams_by_id("stash_unit_test_uuid_999", media_type="porn")
        self.assertEqual(len(streams), 1)
        self.assertEqual(streams[0]["message_id"], 999)

    async def test_05_api_endpoints_and_manifest(self):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            # 1. Verify config endpoint returns stashdb_api_key
            res_cfg = await client.get("/api/config")
            self.assertEqual(res_cfg.status_code, 200)
            cfg_data = res_cfg.json()
            self.assertIn("stashdb_api_key", cfg_data)

            # 2. Verify empty stashdb key verification
            res_ver = await client.post("/api/verify-stashdb", json={"api_key": ""})
            self.assertEqual(res_ver.status_code, 200)
            self.assertFalse(res_ver.json()["valid"])

            # 3. Verify manifest includes adult catalog when adult videos exist
            res_manifest = await client.get("/manifest.json")
            self.assertEqual(res_manifest.status_code, 200)
            man = res_manifest.json()
            self.assertIn("porn", man["types"])
            self.assertIn("stash_", man["idPrefixes"])
            cat_ids = [c["id"] for c in man["catalogs"]]
            self.assertIn("tg_adult", cat_ids)

            token = load_settings().get("security_token", "")
            # 4. Verify catalog route returns our indexed scene
            res_cat = await client.get(f"/{token}/catalog/porn/tg_adult.json")
            self.assertEqual(res_cat.status_code, 200)
            metas = res_cat.json().get("metas", [])
            stash_metas = [m for m in metas if m["id"] == "stash_unit_test_uuid_999"]
            self.assertEqual(len(stash_metas), 1)

            # 5. Verify meta route
            res_meta = await client.get(f"/{token}/meta/porn/stash_unit_test_uuid_999.json")
            self.assertEqual(res_meta.status_code, 200)
            meta_obj = res_meta.json().get("meta", {})
            self.assertEqual(meta_obj["name"], "Brazzers Unit Test Scene")

            # 6. Verify stream route
            res_stream = await client.get(f"/{token}/stream/porn/stash_unit_test_uuid_999.json")
            self.assertEqual(res_stream.status_code, 200)
            streams = res_stream.json().get("streams", [])
            self.assertEqual(len(streams), 1)
            self.assertIn("/play/-10099887766/999/", streams[0]["url"])

if __name__ == "__main__":
    unittest.main()
