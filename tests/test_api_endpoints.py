import os
import sys
import unittest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock

# Force UTF-8 encoding
sys.stdout.reconfigure(encoding='utf-8')

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi.testclient import TestClient
from main import app
from downloader.database import init_downloader_db, enqueue_message
from database import init_db

class ApiEndpointsTestSuite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from settings_manager import load_settings
        cls._orig_settings = load_settings().copy()
        cls._cfg_patcher = patch("main.is_configured", return_value=False)
        cls._cfg_patcher.start()
        cls._tg_patcher = patch("main.start_telegram_service", new_callable=AsyncMock, return_value=False)
        cls._tg_patcher.start()
        cls._tg_client_patcher = patch("telegram_client.start_telegram_service", new_callable=AsyncMock, return_value=False)
        cls._tg_client_patcher.start()
        cls._dl_service_patcher = patch("downloader.service.start_downloader_service", new_callable=AsyncMock, return_value=None)
        cls._dl_service_patcher.start()
        cls._dl_reconcile_patcher = patch("downloader.service.reconcile_download_channels", new_callable=AsyncMock, return_value=0)
        cls._dl_reconcile_patcher.start()
        asyncio.run(init_db())
        asyncio.run(init_downloader_db())

    def setUp(self):
        asyncio.run(init_db())
        asyncio.run(init_downloader_db())

    @classmethod
    def tearDownClass(cls):
        cls._cfg_patcher.stop()
        cls._tg_patcher.stop()
        cls._tg_client_patcher.stop()
        cls._dl_service_patcher.stop()
        cls._dl_reconcile_patcher.stop()
        import sqlite3
        from settings_manager import save_settings
        from downloader.database import DB_FILE
        if hasattr(cls, "_orig_settings") and cls._orig_settings:
            save_settings(cls._orig_settings)
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM download_queue WHERE channel_id IN (-100999, -100111, -100999888)")
        cursor.execute("DELETE FROM download_checkpoints WHERE channel_id IN (-100999, -100111, -100999888)")
        conn.commit()
        conn.close()

    def test_01_stremio_manifest_and_pages(self):
        client = TestClient(app)
        # Manifest
        res = client.get("/manifest.json")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["id"], "org.homelab.telegram.streams")

        # Settings / Configure page
        res = client.get("/configure")
        self.assertEqual(res.status_code, 200)
        self.assertIn("Telegram Media Suite", res.text)
        self.assertIn("Settings & API", res.text)

        # Stremio addon page
        stremio_res = client.get("/stremio")
        self.assertEqual(stremio_res.status_code, 200)
        self.assertIn("Stremio Streaming Add-on", stremio_res.text)

        # Downloads dashboard page
        res = client.get("/downloads")
        self.assertEqual(res.status_code, 200)
        self.assertIn("Local Media Downloader & Archive", res.text)
        self.assertIn("Active Media Transfer", res.text)
        print("[PASS] Stremio manifest and HTML pages verified.")

    def test_02_downloader_config_api(self):
        client = TestClient(app)
        # GET config
        res = client.get("/api/downloader/config")
        self.assertEqual(res.status_code, 200)
        cfg = res.json()
        self.assertIn("downloader_enabled", cfg)
        self.assertIn("download_channel_ids", cfg)
        self.assertIn("min_free_disk_gb", cfg)

        # POST config
        payload = {
            "downloader_enabled": True,
            "download_channel_ids": [-100999888],
            "min_free_disk_gb": 4.5,
            "max_retries": 6
        }
        with patch("telegram_client.start_telegram_service", new_callable=AsyncMock, return_value=False):
            post_res = client.post("/api/downloader/config", json=payload)
        self.assertEqual(post_res.status_code, 200)
        saved = post_res.json()["settings"]
        self.assertEqual(saved["download_channel_ids"], [-100999888])
        self.assertEqual(saved["min_free_disk_gb"], 4.5)
        self.assertEqual(saved["max_retries"], 6)

        # Verify streaming channels were NOT affected
        get_stream_res = client.get("/api/config")
        self.assertEqual(get_stream_res.status_code, 200)
        print("[PASS] Downloader config API verified independently.")

    def test_03_downloader_queue_and_actions(self):
        job, _ = asyncio.run(enqueue_message(-100999, 5555, "ActionMovie.mkv", 1024, "video/mp4", 1000))
        client = TestClient(app)

        # GET queue
        q_res = client.get("/api/downloader/queue")
        self.assertEqual(q_res.status_code, 200)
        jobs = q_res.json()["jobs"]
        self.assertTrue(any(j["id"] == job.id for j in jobs))

        # Pause job
        p_res = client.post(f"/api/downloader/job/{job.id}/pause")
        self.assertEqual(p_res.status_code, 200)

        # Resume job
        r_res = client.post(f"/api/downloader/job/{job.id}/resume")
        self.assertEqual(r_res.status_code, 200)

        # Metadata override
        meta_payload = {
            "media_type": "movie",
            "title": "Action Movie Corrected",
            "year": 2024
        }
        m_res = client.post(f"/api/downloader/job/{job.id}/metadata", json=meta_payload)
        self.assertEqual(m_res.status_code, 200)

        # Skip job
        s_res = client.post(f"/api/downloader/job/{job.id}/skip")
        self.assertEqual(s_res.status_code, 200)

        # Retry job
        rt_res = client.post(f"/api/downloader/job/{job.id}/retry")
        self.assertEqual(rt_res.status_code, 200)

        # Delete job
        d_res = client.delete(f"/api/downloader/job/{job.id}")
        self.assertEqual(d_res.status_code, 200)

        print("[PASS] Downloader queue actions and metadata override verified.")

    def test_04_downloader_stats_api(self):
        client = TestClient(app)
        res = client.get("/api/downloader/stats")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("counts", data)
        self.assertIn("disk", data)
        print("[PASS] Downloader stats API verified.")

if __name__ == "__main__":
    unittest.main()
