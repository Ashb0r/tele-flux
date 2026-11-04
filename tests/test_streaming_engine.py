"""
Focused Unit & Integration Test Suite for Hardened Streaming Engine
===================================================================
Covers:
1. Aligned block retrieval
2. Unaligned HTTP Range retrieval
3. Multiple blocks assembled in the correct order
4. Cache hit and cache miss behavior
5. LRU eviction
6. Shared in-flight block deduplication
7. Concurrent requests for the same block
8. Parallel requests for different blocks
9. Genuine Priority scheduling (Urgent ahead of Prefetch with reserved slots)
10. Prefetch cancellation after seeking
11. Consumer cancellation without breaking other consumers
12. Expired Telegram file-reference refresh (Telethon 1.45.0)
13. Retry handling on network errors
14. Invalid HTTP ranges (416 Range Not Satisfiable)
15. Suffix Range handling (bytes=-N)
16. HEAD requests
17. Correct Content-Length and byte accuracy
18. Correct 206 and 416 responses
19. Cleanup after client disconnection
20. Multiple concurrent streams & ActiveStreamsLimitExceededError enforcement
21. Strict block length validation (rejects truncated blocks)
22. Telethon 1.45.0 InputDocumentFileLocation argument binding
"""

import os
import sys
import asyncio
import unittest
from unittest.mock import MagicMock, AsyncMock, patch

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from streaming_engine import (
    StreamingEngine,
    LRUBlockCache,
    InFlightRequestManager,
    ParallelBlockFetcher,
    PriorityScheduler,
    DocumentManager,
    StreamingSession,
    StreamingMetrics,
    ActiveStreamsLimitExceededError,
    PRIORITY_URGENT,
    PRIORITY_NEAR,
    PRIORITY_PREFETCH,
    STREAM_BLOCK_SIZE_BYTES,
    get_streaming_stats,
    is_streaming_active,
)
from telethon.tl.types import InputDocumentFileLocation
from telethon.errors import FileReferenceExpiredError
from fastapi.testclient import TestClient
from main import app


def make_mock_document(doc_id: int = 12345, size: int = 5 * 1024 * 1024, file_ref: bytes = b"initial_ref"):
    doc = MagicMock()
    doc.id = doc_id
    doc.size = size
    doc.file_reference = file_ref
    doc.access_hash = 987654321
    doc.dc_id = 2
    return doc


def generate_block_data(doc_id: int, block_idx: int, size: int = STREAM_BLOCK_SIZE_BYTES) -> bytes:
    """Deterministic payload generator: header specifies block index."""
    prefix = f"BLOCK:{doc_id}:{block_idx}:".encode("utf-8")
    return prefix + b"X" * (size - len(prefix))


class StreamingEngineTestSuite(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        # Fresh cache and engine components for isolated test runs
        self.block_cache = LRUBlockCache(max_mb=4)
        self.in_flight = InFlightRequestManager()
        self.doc_manager = DocumentManager()
        self.mock_client = AsyncMock()
        self.mock_client.is_connected = MagicMock(return_value=True)

    # -----------------------------------------------------------------
    # 1. Aligned block retrieval
    # -----------------------------------------------------------------
    async def test_01_aligned_block_retrieval(self):
        doc = make_mock_document(doc_id=1, size=2 * STREAM_BLOCK_SIZE_BYTES)
        expected_data = generate_block_data(1, 0)

        # Populate cache
        await self.block_cache.put(doc.id, 0, expected_data)

        # Retrieve block 0
        retrieved = await self.block_cache.get(doc.id, 0)
        self.assertIsNotNone(retrieved)
        self.assertEqual(len(retrieved), STREAM_BLOCK_SIZE_BYTES)
        self.assertEqual(retrieved, expected_data)
        print("[PASS] Test 1: Aligned block retrieval verified.")

    # -----------------------------------------------------------------
    # 2. Unaligned HTTP Range retrieval
    # -----------------------------------------------------------------
    async def test_02_unaligned_http_range_retrieval(self):
        engine = StreamingEngine()
        doc = make_mock_document(doc_id=2, size=3 * STREAM_BLOCK_SIZE_BYTES)
        b0 = generate_block_data(2, 0)

        async def mock_iter(file, offset=0, limit=1, **kwargs):
            b_idx = offset // STREAM_BLOCK_SIZE_BYTES
            yield generate_block_data(2, b_idx)

        self.mock_client.iter_download = mock_iter
        with patch("streaming_engine.doc_manager.get_document", new_callable=AsyncMock, return_value=doc):
            chunks = []
            async for chunk in engine.stream_file(self.mock_client, chat_id=1, message_id=1, offset=100000, length=250000):
                chunks.append(chunk)

            assembled = b"".join(chunks)
            self.assertEqual(len(assembled), 250000)
            self.assertEqual(assembled, b0[100000 : 100000 + 250000])

        print("[PASS] Test 2: Unaligned HTTP Range retrieval verified.")

    # -----------------------------------------------------------------
    # 3. Multiple blocks assembled in the correct order
    # -----------------------------------------------------------------
    async def test_03_multiple_blocks_assembled_in_order(self):
        engine = StreamingEngine()
        doc = make_mock_document(doc_id=3, size=4 * STREAM_BLOCK_SIZE_BYTES)

        async def mock_iter(file, offset=0, limit=1, **kwargs):
            b_idx = offset // STREAM_BLOCK_SIZE_BYTES
            yield generate_block_data(3, b_idx)

        self.mock_client.iter_download = mock_iter
        with patch("streaming_engine.doc_manager.get_document", new_callable=AsyncMock, return_value=doc):
            start_off = STREAM_BLOCK_SIZE_BYTES - 10000
            req_len = 10000 + STREAM_BLOCK_SIZE_BYTES + 20000

            chunks = []
            async for chunk in engine.stream_file(self.mock_client, 1, 1, offset=start_off, length=req_len):
                chunks.append(chunk)

            assembled = b"".join(chunks)
            self.assertEqual(len(assembled), req_len)

            b0 = generate_block_data(3, 0)
            b1 = generate_block_data(3, 1)
            b2 = generate_block_data(3, 2)
            expected = b0[start_off:] + b1 + b2[:20000]
            self.assertEqual(assembled, expected)

        print("[PASS] Test 3: Multiple blocks assembled in strict sequential order verified.")

    # -----------------------------------------------------------------
    # 4. Cache hit and cache miss behavior
    # -----------------------------------------------------------------
    async def test_04_cache_hit_and_miss_behavior(self):
        cache = LRUBlockCache(max_mb=2)

        # Miss
        miss_res = await cache.get(doc_id=4, access_hash_or_block=0)
        self.assertIsNone(miss_res)

        # Put
        data = generate_block_data(4, 0)
        await cache.put(doc_id=4, access_hash_or_block=0, data_or_block=data)

        # Hit
        hit_res = await cache.get(doc_id=4, access_hash_or_block=0)
        self.assertIsNotNone(hit_res)
        self.assertEqual(hit_res, data)

        print("[PASS] Test 4: Cache hit and miss behavior verified.")

    # -----------------------------------------------------------------
    # 5. LRU eviction
    # -----------------------------------------------------------------
    async def test_05_lru_eviction(self):
        small_cache = LRUBlockCache(max_mb=2)
        small_cache.max_bytes = 2 * STREAM_BLOCK_SIZE_BYTES

        b0 = b"A" * STREAM_BLOCK_SIZE_BYTES
        b1 = b"B" * STREAM_BLOCK_SIZE_BYTES
        b2 = b"C" * STREAM_BLOCK_SIZE_BYTES

        await small_cache.put(5, 0, b0)
        await small_cache.put(5, 1, b1)

        # Access block 0 to make block 1 the LRU
        await small_cache.get(5, 0)

        # Put block 2 (evicts block 1)
        await small_cache.put(5, 2, b2)

        # Block 0 and 2 must remain; Block 1 must be evicted
        self.assertIsNotNone(await small_cache.get(5, 0))
        self.assertIsNone(await small_cache.get(5, 1))
        self.assertIsNotNone(await small_cache.get(5, 2))

        print("[PASS] Test 5: LRU eviction verified.")

    # -----------------------------------------------------------------
    # 6. Shared in-flight block deduplication
    # -----------------------------------------------------------------
    async def test_06_shared_inflight_deduplication(self):
        mgr = InFlightRequestManager()
        fetch_count = 0

        async def fetch_coro():
            nonlocal fetch_count
            fetch_count += 1
            await asyncio.sleep(0.05)
            return b"DEDUPLICATED_BLOCK"

        key = (6, 0, 0)
        fut1, is_c1 = await mgr.get_or_create(key, "consumer_1", PRIORITY_URGENT, fetch_coro)
        fut2, is_c2 = await mgr.get_or_create(key, "consumer_2", PRIORITY_URGENT, fetch_coro)

        self.assertTrue(is_c1)
        self.assertFalse(is_c2)

        res1, res2 = await asyncio.gather(fut1, fut2)
        self.assertEqual(res1, b"DEDUPLICATED_BLOCK")
        self.assertEqual(res2, b"DEDUPLICATED_BLOCK")
        self.assertEqual(fetch_count, 1)

        print("[PASS] Test 6: Shared in-flight request deduplication verified.")

    # -----------------------------------------------------------------
    # 7. Concurrent requests for the same block
    # -----------------------------------------------------------------
    async def test_07_concurrent_requests_same_block(self):
        mgr = InFlightRequestManager()
        expected = b"CONCURRENT_SAME_BLOCK_PAYLOAD"

        async def fetch():
            await asyncio.sleep(0.02)
            return expected

        key = (7, 0, 0)
        futures = []
        for i in range(5):
            fut, _ = await mgr.get_or_create(key, f"consumer_{i}", PRIORITY_URGENT, fetch)
            futures.append(fut)

        results = await asyncio.gather(*futures)
        for r in results:
            self.assertEqual(r, expected)

        print("[PASS] Test 7: Concurrent requests for same block verified.")

    # -----------------------------------------------------------------
    # 8. Parallel requests for different blocks
    # -----------------------------------------------------------------
    async def test_08_parallel_requests_different_blocks(self):
        fetcher = ParallelBlockFetcher()
        doc = make_mock_document(doc_id=8)

        async def mock_iter(file, offset=0, limit=1, **kwargs):
            b_idx = offset // STREAM_BLOCK_SIZE_BYTES
            await asyncio.sleep(0.02)
            yield generate_block_data(8, b_idx)

        self.mock_client.iter_download = mock_iter

        tasks = [
            fetcher.fetch_block_from_telegram(self.mock_client, doc, 1, 1, idx)
            for idx in range(4)
        ]
        results = await asyncio.gather(*tasks)
        self.assertEqual(len(results), 4)
        for i in range(4):
            self.assertEqual(results[i], generate_block_data(8, i))

        print("[PASS] Test 8: Parallel requests for different blocks verified.")

    # -----------------------------------------------------------------
    # 9. Genuine Priority scheduling (Urgent before prefetch)
    # -----------------------------------------------------------------
    async def test_09_priority_scheduling(self):
        sched = PriorityScheduler(max_concurrency=2, reserved_urgent_slots=1)
        # Occupy non-urgent slot (max_prefetch_concurrency = 1)
        await sched.acquire(priority=PRIORITY_PREFETCH)

        # Next prefetch request must queue because non-urgent limit is reached
        order_executed = []

        async def do_prefetch():
            await sched.acquire(priority=PRIORITY_PREFETCH)
            order_executed.append("PREFETCH")
            sched.release(priority=PRIORITY_PREFETCH)

        async def do_urgent():
            # Urgent can acquire immediately because 1 reserved slot is open!
            await sched.acquire(priority=PRIORITY_URGENT)
            order_executed.append("URGENT")
            sched.release(priority=PRIORITY_URGENT)

        # Enqueue prefetch first, then urgent
        t_pref = asyncio.create_task(do_prefetch())
        await asyncio.sleep(0.01)
        t_urg = asyncio.create_task(do_urgent())

        await asyncio.sleep(0.02)
        sched.release(priority=PRIORITY_PREFETCH)  # Release initial slot

        await asyncio.gather(t_pref, t_urg)

        # Urgent executed BEFORE prefetch!
        self.assertEqual(order_executed[0], "URGENT")
        self.assertEqual(order_executed[1], "PREFETCH")

        print("[PASS] Test 9: Genuine Priority scheduling (Urgent ahead of Prefetch) verified.")

    # -----------------------------------------------------------------
    # 10. Prefetch cancellation after seeking
    # -----------------------------------------------------------------
    async def test_10_prefetch_cancellation_after_seeking(self):
        doc = make_mock_document(doc_id=10, size=100 * STREAM_BLOCK_SIZE_BYTES)
        session = StreamingSession("session_seek_test", self.mock_client, 1, 1, doc)

        # Trigger prefetch at block 1
        session.trigger_prefetch(1, count=10)
        self.assertTrue(len(session._prefetch_tasks) > 0)

        # Player seeks to block 80
        session.handle_seek(new_head_block=80)

        # Old tasks must be cancelled
        for b_idx, task in list(session._prefetch_tasks.items()):
            if b_idx < 80:
                self.assertTrue(task.cancelled() or task.done())

        session.close()
        print("[PASS] Test 10: Prefetch cancellation after seeking verified.")

    # -----------------------------------------------------------------
    # 11. Consumer cancellation without breaking other consumers
    # -----------------------------------------------------------------
    async def test_11_consumer_cancellation_safety(self):
        mgr = InFlightRequestManager()
        block_fetched = asyncio.Event()

        async def long_fetch():
            await block_fetched.wait()
            return b"RESILIENT_DATA"

        key = (11, 0, 0)
        fut1, _ = await mgr.get_or_create(key, "c1", PRIORITY_URGENT, long_fetch)
        fut2, _ = await mgr.get_or_create(key, "c2", PRIORITY_URGENT, long_fetch)

        # c1 unsubscribes / disconnects
        no_waiters = await mgr.unsubscribe(key, "c1")
        self.assertFalse(no_waiters)  # c2 still waiting!

        block_fetched.set()
        res2 = await fut2
        self.assertEqual(res2, b"RESILIENT_DATA")

        print("[PASS] Test 11: Consumer cancellation safety verified.")

    # -----------------------------------------------------------------
    # 12. Expired Telegram file-reference refresh (Telethon 1.45.0)
    # -----------------------------------------------------------------
    async def test_12_expired_file_reference_refresh(self):
        fetcher = ParallelBlockFetcher()
        doc = make_mock_document(doc_id=12, file_ref=b"old_ref")
        fresh_doc = make_mock_document(doc_id=12, file_ref=b"fresh_ref")

        attempts = 0
        async def mock_iter(file, **kwargs):
            nonlocal attempts
            attempts += 1
            if getattr(file, "file_reference", None) == b"old_ref":
                raise FileReferenceExpiredError(request=None)
            yield generate_block_data(12, 0)

        self.mock_client.iter_download = mock_iter

        with patch("streaming_engine.doc_manager.get_document", new_callable=AsyncMock, return_value=fresh_doc):
            data = await fetcher.fetch_block_from_telegram(
                client=self.mock_client,
                doc=doc,
                chat_id=1,
                message_id=1,
                block_idx=0
            )

        self.assertEqual(len(data), STREAM_BLOCK_SIZE_BYTES)
        self.assertGreaterEqual(attempts, 2)
        print("[PASS] Test 12: Telegram file reference refresh and retry (Telethon 1.45.0) verified.")

    # -----------------------------------------------------------------
    # 13. Retry handling on network errors
    # -----------------------------------------------------------------
    async def test_13_retry_handling(self):
        fetcher = ParallelBlockFetcher()
        doc = make_mock_document(doc_id=13)

        attempts = 0
        async def mock_iter(file, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ConnectionError("Temporary MTProto disconnect")
            yield generate_block_data(13, 0)

        self.mock_client.iter_download = mock_iter

        data = await fetcher.fetch_block_from_telegram(
            client=self.mock_client,
            doc=doc,
            chat_id=1,
            message_id=1,
            block_idx=0
        )
        self.assertEqual(len(data), STREAM_BLOCK_SIZE_BYTES)
        self.assertEqual(attempts, 2)
        print("[PASS] Test 13: Retry handling on network errors verified.")

    # -----------------------------------------------------------------
    # 14. Invalid HTTP ranges (416 Range Not Satisfiable)
    # -----------------------------------------------------------------
    def test_14_invalid_http_ranges(self):
        client = TestClient(app)
        mock_video = {"chat_id": 999, "message_id": 999, "file_size": 1000, "mime_type": "video/mp4"}

        with patch("main.get_video_by_location", new_callable=AsyncMock, return_value=mock_video):
            res1 = client.get("/play/999/999/test.mp4", headers={"Range": "bytes=1500-2000"})
            self.assertEqual(res1.status_code, 416)
            self.assertEqual(res1.headers["Content-Range"], "bytes */1000")

            res2 = client.get("/play/999/999/test.mp4", headers={"Range": "bytes=800-400"})
            self.assertEqual(res2.status_code, 416)

            res3 = client.get("/play/999/999/test.mp4", headers={"Range": "bytes=invalid"})
            self.assertEqual(res3.status_code, 416)

        print("[PASS] Test 14: Invalid HTTP ranges and 416 responses verified.")

    # -----------------------------------------------------------------
    # 15. Suffix Range handling (bytes=-N)
    # -----------------------------------------------------------------
    def test_15_suffix_range_handling(self):
        client = TestClient(app)
        mock_video = {"chat_id": 999, "message_id": 999, "file_size": 10000, "mime_type": "video/mp4"}

        async def dummy_gen(*args, **kwargs):
            yield b"X" * 1000

        with patch("main.get_video_by_location", new_callable=AsyncMock, return_value=mock_video):
            with patch("main.stream_telegram_file", side_effect=dummy_gen):
                res = client.get("/play/999/999/test.mp4", headers={"Range": "bytes=-1000"})
                self.assertEqual(res.status_code, 206)
                self.assertEqual(res.headers["Content-Range"], "bytes 9000-9999/10000")
                self.assertEqual(res.headers["Content-Length"], "1000")

        print("[PASS] Test 15: Suffix Range handling verified.")

    # -----------------------------------------------------------------
    # 16. HEAD requests
    # -----------------------------------------------------------------
    def test_16_head_requests(self):
        client = TestClient(app)
        mock_video = {"chat_id": 999, "message_id": 999, "file_size": 52428800, "mime_type": "video/mp4"}

        with patch("main.get_video_by_location", new_callable=AsyncMock, return_value=mock_video):
            res = client.head("/play/999/999/test.mp4")
            self.assertEqual(res.status_code, 200)
            self.assertEqual(res.headers["Content-Length"], "52428800")
            self.assertEqual(res.headers["Content-Type"], "video/mp4")
            self.assertEqual(res.headers["Accept-Ranges"], "bytes")
            self.assertEqual(len(res.content), 0)

        print("[PASS] Test 16: HEAD requests verified.")

    # -----------------------------------------------------------------
    # 17. Correct Content-Length and byte accuracy
    # -----------------------------------------------------------------
    def test_17_correct_content_length(self):
        client = TestClient(app)
        mock_video = {"chat_id": 999, "message_id": 999, "file_size": 20000, "mime_type": "video/mp4"}

        async def dummy_gen(*args, **kwargs):
            yield b"A" * 5000

        with patch("main.get_video_by_location", new_callable=AsyncMock, return_value=mock_video):
            with patch("main.stream_telegram_file", side_effect=dummy_gen):
                res = client.get("/play/999/999/test.mp4", headers={"Range": "bytes=0-4999"})
                self.assertEqual(res.status_code, 206)
                self.assertEqual(res.headers["Content-Length"], "5000")
                self.assertEqual(len(res.content), 5000)

        print("[PASS] Test 17: Content-Length and byte accuracy verified.")

    # -----------------------------------------------------------------
    # 18. Correct 206 and 416 responses
    # -----------------------------------------------------------------
    def test_18_correct_206_and_416_responses(self):
        client = TestClient(app)
        mock_video = {"chat_id": 999, "message_id": 999, "file_size": 5000, "mime_type": "video/mp4"}

        async def dummy_gen(*args, **kwargs):
            yield b"D" * 200

        with patch("main.get_video_by_location", new_callable=AsyncMock, return_value=mock_video):
            with patch("main.stream_telegram_file", side_effect=dummy_gen):
                ok_res = client.get("/play/999/999/test.mp4", headers={"Range": "bytes=100-299"})
                self.assertEqual(ok_res.status_code, 206)

                fail_res = client.get("/play/999/999/test.mp4", headers={"Range": "bytes=6000-7000"})
                self.assertEqual(fail_res.status_code, 416)

        print("[PASS] Test 18: Correct 206 and 416 response codes verified.")

    # -----------------------------------------------------------------
    # 19. Cleanup after client disconnection
    # -----------------------------------------------------------------
    async def test_19_cleanup_after_disconnection(self):
        engine = StreamingEngine()
        doc = make_mock_document(doc_id=19)

        async def mock_iter(file, **kwargs):
            yield generate_block_data(19, 0)

        self.mock_client.iter_download = mock_iter

        with patch("streaming_engine.doc_manager.get_document", new_callable=AsyncMock, return_value=doc):
            gen = engine.stream_file(self.mock_client, 1, 1, offset=0, length=STREAM_BLOCK_SIZE_BYTES, session_id="disc_session")
            await gen.__anext__()
            self.assertEqual(engine.active_stream_count(), 1)

            # Close generator (client disconnected)
            await gen.aclose()
            self.assertEqual(engine.active_stream_count(), 0)

        print("[PASS] Test 19: Cleanup after client disconnection verified.")

    # -----------------------------------------------------------------
    # 20. Multiple concurrent streams & Active-Stream Limit
    # -----------------------------------------------------------------
    async def test_20_multiple_concurrent_streams(self):
        engine = StreamingEngine()
        doc = make_mock_document(doc_id=20)

        async def mock_iter(file, **kwargs):
            yield generate_block_data(20, 0)

        self.mock_client.iter_download = mock_iter

        # Test limit enforcement
        with patch("streaming_engine.STREAM_MAX_ACTIVE_STREAMS", 2):
            s1 = await engine.get_or_create_session("s1", self.mock_client, 1, 1, doc)
            s2 = await engine.get_or_create_session("s2", self.mock_client, 1, 1, doc)
            self.assertEqual(engine.active_stream_count(), 2)

            with self.assertRaises(ActiveStreamsLimitExceededError):
                await engine.get_or_create_session("s3", self.mock_client, 1, 1, doc)

            await engine.release_session("s1")
            await engine.release_session("s2")
            self.assertEqual(engine.active_stream_count(), 0)

        print("[PASS] Test 20: Multiple concurrent streams & limit enforcement verified.")

    # -----------------------------------------------------------------
    # 21. Strict block length validation (rejects truncated blocks)
    # -----------------------------------------------------------------
    async def test_21_strict_block_length_validation(self):
        fetcher = ParallelBlockFetcher()
        doc = make_mock_document(doc_id=21, size=2 * STREAM_BLOCK_SIZE_BYTES)

        # Telegram returns truncated chunk (1000 bytes instead of 512KB)
        async def mock_iter_truncated(file, **kwargs):
            yield b"TRUNCATED" * 100

        self.mock_client.iter_download = mock_iter_truncated

        with self.assertRaises(IOError) as ctx:
            await fetcher.fetch_block_from_telegram(
                client=self.mock_client,
                doc=doc,
                chat_id=1,
                message_id=1,
                block_idx=0
            )

        self.assertIn("Truncated block", str(ctx.exception))
        print("[PASS] Test 21: Strict block length validation (truncated block rejection) verified.")

    # -----------------------------------------------------------------
    # 22. Telethon 1.45.0 InputDocumentFileLocation argument binding
    # -----------------------------------------------------------------
    async def test_22_telethon_1_45_argument_binding(self):
        fetcher = ParallelBlockFetcher()
        doc = make_mock_document(doc_id=22, size=STREAM_BLOCK_SIZE_BYTES)

        received_kwargs = {}
        async def mock_iter(file, **kwargs):
            nonlocal received_kwargs
            received_kwargs = kwargs
            received_kwargs["file"] = file
            yield generate_block_data(22, 0)

        self.mock_client.iter_download = mock_iter

        await fetcher.fetch_block_from_telegram(
            client=self.mock_client,
            doc=doc,
            chat_id=1,
            message_id=1,
            block_idx=0
        )

        # Assert no unexpected kwargs like msg_data were passed
        self.assertNotIn("msg_data", received_kwargs)
        self.assertIsInstance(received_kwargs["file"], InputDocumentFileLocation)
        self.assertEqual(received_kwargs["file"].id, doc.id)
        self.assertEqual(received_kwargs["offset"], 0)
        self.assertEqual(received_kwargs["request_size"], STREAM_BLOCK_SIZE_BYTES)
        self.assertEqual(received_kwargs["file_size"], doc.size)
        self.assertEqual(received_kwargs["dc_id"], doc.dc_id)

        print("[PASS] Test 22: Telethon 1.45.0 InputDocumentFileLocation argument binding verified.")


if __name__ == "__main__":
    unittest.main()
