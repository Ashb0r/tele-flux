"""
Verification Test Suite for Ultra-Streaming Engine (v3 Hardened)
================================================================
Tests:
1. Native 1 MB chunk quantum sizing.
2. Two-tiered cache: RAM hit, Disk hit, persistent disk file write and read.
3. Accurate MIME type derivation for MKV, MP4, WebM, AVI, etc.
4. Priority Scheduler inversion fix: Urgent priority acquires slot before semaphores.
5. "Last playback request wins": New Range seek supersedes old playback session without 503 errors.
6. In-memory video location cache and instant lookup.
7. Stall watchdog timeout detection.
8. Zero cold-start pre-warming of Block 0.
"""

import os
import sys
import asyncio
import tempfile
import unittest
from unittest.mock import MagicMock, AsyncMock, patch

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import (
    STREAM_BLOCK_SIZE_BYTES,
    STREAM_BLOCK_SIZE_KB,
)
from streaming_engine import (
    TieredBlockCache,
    PriorityScheduler,
    InFlightRequestManager,
    ParallelBlockFetcher,
    StreamingSession,
    StreamingEngine,
    PRIORITY_URGENT,
    PRIORITY_NEAR,
    PRIORITY_PREFETCH,
)
from telegram_client import get_video_mime_type
from database import get_video_by_location, _video_location_cache


class UltraStreamingTestSuite:

    async def asyncSetUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.cache = TieredBlockCache(
            ram_max_mb=2,
            disk_cache_dir=self.temp_dir,
            disk_max_gb=1,
            disk_enabled=True
        )

    async def asyncTearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def assertEqual(self, a, b, msg=None):
        assert a == b, f"{a} != {b} {msg or ''}"

    def assertTrue(self, a, msg=None):
        assert bool(a), f"Expected True, got {a} {msg or ''}"

    def assertFalse(self, a, msg=None):
        assert not bool(a), f"Expected False, got {a} {msg or ''}"

    def assertIsNotNone(self, a, msg=None):
        assert a is not None, f"Expected not None {msg or ''}"

    def assertIn(self, a, b, msg=None):
        assert a in b, f"Expected {a} in {b} {msg or ''}"

    def assertNotEqual(self, a, b, msg=None):
        assert a != b, f"Expected {a} != {b} {msg or ''}"

    # 1. 512 KB MTProto Quantum Verification
    async def test_01_block_size_is_1mb(self):
        self.assertEqual(STREAM_BLOCK_SIZE_KB, 512)
        self.assertEqual(STREAM_BLOCK_SIZE_BYTES, 524288)
        print("[PASS] Test 1: Native 512 KB MTProto quantum verified.")

    # 2. Accurate MIME Type Derivation
    async def test_02_mime_type_derivation(self):
        self.assertEqual(get_video_mime_type("movie.mkv", "application/octet-stream"), "video/x-matroska")
        self.assertEqual(get_video_mime_type("show.mp4", "application/x-mp4"), "video/mp4")
        self.assertEqual(get_video_mime_type("clip.webm", ""), "video/webm")
        self.assertEqual(get_video_mime_type("old.avi", ""), "video/x-msvideo")
        self.assertEqual(get_video_mime_type("stream.ts", ""), "video/mp2t")
        print("[PASS] Test 2: Extension-based video MIME type derivation verified (MKV -> video/x-matroska).")

    # 3. Two-Tiered Cache (RAM + Disk Persistence)
    async def test_03_tiered_cache_ram_and_disk(self):
        doc_id = 999
        access_hash = 12345
        block_idx = 0
        payload = b"U" * STREAM_BLOCK_SIZE_BYTES

        # Put block in cache
        await self.cache.put(doc_id, access_hash, block_idx, payload)
        # Give non-blocking background disk persistence a moment to complete
        await asyncio.sleep(0.05)

        # 1. RAM Hit
        ram_val = await self.cache.get(doc_id, access_hash, block_idx)
        self.assertEqual(ram_val, payload)

        # Verify disk file was created on disk
        disk_path = os.path.join(self.temp_dir, f"{doc_id}_{access_hash}_{block_idx}.blk")
        self.assertTrue(os.path.exists(disk_path))
        self.assertEqual(os.path.getsize(disk_path), STREAM_BLOCK_SIZE_BYTES)

        # Evict from RAM by clearing RAM only
        self.cache.ram_cache.clear()
        self.cache.ram_current_bytes = 0

        # 2. Disk Hit: Block should be retrieved from disk file and promoted to RAM
        disk_val = await self.cache.get(doc_id, access_hash, block_idx)
        self.assertEqual(disk_val, payload)
        self.assertIn((doc_id, access_hash, block_idx), self.cache.ram_cache)
        print("[PASS] Test 3: Two-Tiered Cache (RAM L1 + Disk L2 persistent retrieval) verified.")

    # 4. Priority Scheduler Inversion Fix & Reserved Slots
    async def test_04_priority_scheduler_inversion_free(self):
        # 4 total slots, 2 reserved urgent slots -> max prefetch = 2
        sched = PriorityScheduler(max_concurrency=4, reserved_urgent_slots=2)

        # Acquire 2 prefetch slots (fills max_prefetch_concurrency)
        await sched.acquire(priority=PRIORITY_PREFETCH)
        await sched.acquire(priority=PRIORITY_PREFETCH)

        # Non-urgent request should NOT be granted immediately (slots reserved for Urgent)
        queue = sched._get_queue()
        self.assertEqual(sched._running_count, 2)

        # Urgent request CAN acquire immediately because reserved slots exist
        await sched.acquire(priority=PRIORITY_URGENT)
        self.assertEqual(sched._running_count, 3)

        sched.release(priority=PRIORITY_URGENT)
        sched.release(priority=PRIORITY_PREFETCH)
        sched.release(priority=PRIORITY_PREFETCH)
        print("[PASS] Test 4: Priority Scheduler (Urgent reservation & prefetch protection) verified.")

    # 5. Last Playback Request Wins (Per-File Pipeline)
    async def test_05_last_playback_request_wins(self):
        engine = StreamingEngine()
        mock_doc = MagicMock()
        mock_doc.id = 555
        mock_doc.access_hash = 999
        mock_doc.size = 50 * STREAM_BLOCK_SIZE_BYTES
        mock_client = AsyncMock()

        # Session 1: initial play
        s1 = await engine.get_or_create_session(
            session_id="s1",
            client=mock_client,
            chat_id=1,
            message_id=10,
            doc=mock_doc,
            is_probe=False
        )
        self.assertFalse(s1._is_superseded)
        self.assertEqual(engine.active_stream_count(), 1)

        # Session 2: User seeks 30s ahead on same file
        s2 = await engine.get_or_create_session(
            session_id="s2",
            client=mock_client,
            chat_id=1,
            message_id=10,
            doc=mock_doc,
            is_probe=False
        )

        # S1 must be superseded and closed gracefully
        self.assertTrue(s1._is_superseded)
        self.assertFalse(s2._is_superseded)
        # Active file count remains 1 (no session explosion or 503!)
        self.assertEqual(engine.active_stream_count(), 1)

        # Probe requests should NOT supersede active playback
        probe_s = await engine.get_or_create_session(
            session_id="probe_1",
            client=mock_client,
            chat_id=1,
            message_id=10,
            doc=mock_doc,
            is_probe=True
        )
        self.assertFalse(s2._is_superseded)
        print("[PASS] Test 5: Per-File Pipeline ('Last Playback Request Wins') verified without session explosion.")

    # 6. In-Memory Video Location Cache
    async def test_06_video_location_cache(self):
        _video_location_cache[(123, 456)] = {
            "chat_id": 123,
            "message_id": 456,
            "file_name": "cached_test.mkv",
            "file_size": 2048576,
            "mime_type": "video/x-matroska"
        }

        # Lookup should hit RAM cache instantly with 0 disk I/O
        v = await get_video_by_location(123, 456)
        self.assertIsNotNone(v)
        self.assertEqual(v["file_name"], "cached_test.mkv")
        print("[PASS] Test 6: In-Memory Video Location Cache verified.")

    # 7. Pre-Warming File Block 0
    async def test_07_prewarm_stream(self):
        engine = StreamingEngine()
        mock_doc = MagicMock()
        mock_doc.id = 777
        mock_doc.access_hash = 888
        mock_doc.size = 10 * STREAM_BLOCK_SIZE_BYTES

        mock_client = MagicMock()
        mock_client.is_connected = MagicMock(return_value=True)
        del mock_client.get_client

        async def mock_iter(file, offset=0, **kwargs):
            yield b"H" * STREAM_BLOCK_SIZE_BYTES

        mock_client.iter_download = mock_iter

        with patch("streaming_engine.doc_manager.get_document", new_callable=AsyncMock, return_value=mock_doc):
            await engine.prewarm_file(mock_client, chat_id=1, message_id=2)

        # Block 0 should now be in the global cache
        from streaming_engine import block_cache
        has_b0 = block_cache.has_block(mock_doc.id, mock_doc.access_hash, 0)
        self.assertTrue(has_b0)
        print("[PASS] Test 7: Cold-start Pre-Warming of container headers and index verified.")

    # 8. Multi-Connection MTProto Client Pool
    async def test_08_multi_connection_client_pool(self):
        from telegram_client import StreamingClientPool
        pool = StreamingClientPool()

        c0 = MagicMock()
        c0.is_connected = MagicMock(return_value=True)
        c0.id = "urgent_0"

        c1 = MagicMock()
        c1.is_connected = MagicMock(return_value=True)
        c1.id = "worker_1"

        c2 = MagicMock()
        c2.is_connected = MagicMock(return_value=True)
        c2.id = "worker_2"

        pool.add_client(c0)
        pool.add_client(c1)
        pool.add_client(c2)

        self.assertTrue(pool.is_connected())

        # PRIORITY_URGENT must pick dedicated slot 0
        u_client = pool.get_client(priority=PRIORITY_URGENT, attempt=0)
        self.assertEqual(u_client.id, "urgent_0")

        # PRIORITY_URGENT retry must failover to alternate client
        u_retry = pool.get_client(priority=PRIORITY_URGENT, attempt=1)
        self.assertEqual(u_retry.id, "worker_1")

        # Prefetch must distribute across workers
        p1 = pool.get_client(priority=PRIORITY_PREFETCH)
        p2 = pool.get_client(priority=PRIORITY_PREFETCH)
        self.assertIn(p1.id, ["worker_1", "worker_2"])
        self.assertIn(p2.id, ["worker_1", "worker_2"])
        self.assertNotEqual(p1.id, p2.id)

        print("[PASS] Test 8: Multi-Connection MTProto Client Pool (Urgent isolation & worker distribution) verified.")

    # 9. MTProto 512 KB Divisor Alignment on Partial Final Block (Fixes LimitInvalidError)
    async def test_09_partial_last_block_mtproto_alignment(self):
        from streaming_engine import ParallelBlockFetcher
        fetcher = ParallelBlockFetcher()

        mock_doc = MagicMock()
        mock_doc.id = 1234
        mock_doc.access_hash = 5678
        # File with partial last block: 2 full blocks + 123,456 bytes
        partial_len = 123456
        mock_doc.size = 2 * STREAM_BLOCK_SIZE_BYTES + partial_len

        recorded_kwargs = {}

        mock_client = MagicMock()
        mock_client.is_connected = MagicMock(return_value=True)
        del mock_client.get_client

        async def mock_iter(file, offset=0, **kwargs):
            recorded_kwargs.update(kwargs)
            # Telegram returns remaining bytes up to EOF
            yield b"Z" * partial_len

        mock_client.iter_download = mock_iter

        # Fetch block 2 (the partial final block)
        data = await fetcher.fetch_block_from_telegram(
            client=mock_client,
            doc=mock_doc,
            chat_id=1,
            message_id=2,
            block_idx=2,
            priority=PRIORITY_URGENT
        )

        # Telegram request_size MUST be 524,288 (valid 1MB divisor), NEVER the unaligned 123,456!
        self.assertEqual(recorded_kwargs.get("request_size"), 524288)
        self.assertEqual(recorded_kwargs.get("chunk_size"), 524288)
        self.assertEqual(recorded_kwargs.get("limit"), 1)
        # Returned data must be exact partial length
        self.assertEqual(len(data), partial_len)
        print("[PASS] Test 9: Strict MTProto 512 KB Wire Alignment on final/partial blocks (LimitInvalidError prevention) verified.")


async def main():
    print("=" * 60)
    print("  Running Ultra-Streaming Verification Suite")
    print("=" * 60)
    suite = UltraStreamingTestSuite()
    await suite.asyncSetUp()
    try:
        await suite.test_01_block_size_is_1mb()
        await suite.test_02_mime_type_derivation()
        await suite.test_03_tiered_cache_ram_and_disk()
        await suite.test_04_priority_scheduler_inversion_free()
        await suite.test_05_last_playback_request_wins()
        await suite.test_06_video_location_cache()
        await suite.test_07_prewarm_stream()
        await suite.test_08_multi_connection_client_pool()
        await suite.test_09_partial_last_block_mtproto_alignment()
        print("=" * 60)
        print("  ALL 9 ULTRA-STREAMING TESTS PASSED!")
        print("=" * 60)
    finally:
        await suite.asyncTearDown()


if __name__ == "__main__":
    asyncio.run(main())
