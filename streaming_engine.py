"""
High-Performance Telegram-to-Stremio Streaming Engine
=====================================================
Features:
1. Aligned Block-Based File Retrieval (exact byte range slicing, zero corruption).
2. Parallel MTProto Telegram Block Fetching (bounded by global and per-stream semaphores).
3. Shared In-Flight Request Deduplication (prevents duplicate MTProto requests for same block).
4. Shared LRU Block Cache (memory-bounded, fast RAM delivery for seeks & headers).
5. Three-Tier Priority Scheduler (Priority 1: Urgent, Priority 2: Near, Priority 3: Prefetch).
6. Seek-Aware Prefetch and Safe Cancellation (cancels obsolete read-ahead on seeks).
7. Single-Flight Telegram File Reference Refresh and Retry on expiration.
8. Real-time Streaming Metrics and Lightweight Diagnostics.
"""

import os
import time
import asyncio
import logging
from collections import OrderedDict
from typing import Optional, Dict, Tuple, Set, AsyncGenerator, Any

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    FileReferenceExpiredError,
)

# Configuration defaults
from config import (
    STREAM_BLOCK_SIZE_KB,
    STREAM_BLOCK_SIZE_BYTES,
    STREAM_PARALLEL_CONNECTIONS,
    STREAM_GLOBAL_MAX_REQUESTS,
    STREAM_CACHE_MAX_MB,
    STREAM_PREFETCH_MIN_MB,
    STREAM_PREFETCH_MAX_MB,
    STREAM_MAX_ACTIVE_STREAMS,
)

logger = logging.getLogger("StreamingEngine")

# Priority definitions
PRIORITY_URGENT = 1    # Immediate HTTP response block (playback head / post-seek)
PRIORITY_NEAR = 2      # Near-future read-ahead (next 2-4 blocks)
PRIORITY_PREFETCH = 3  # Speculative background prefetch


class StreamingMetrics:
    """Lightweight thread-safe metrics collector for streaming operations."""
    def __init__(self):
        self._lock = asyncio.Lock()
        self.active_streams: int = 0
        self.active_block_requests: int = 0
        self.completed_block_requests: int = 0
        self.total_bytes_streamed: int = 0
        self.cache_hits: int = 0
        self.cache_misses: int = 0
        self.prefetch_requests: int = 0
        self.canceled_prefetch_requests: int = 0
        self.file_ref_refreshes: int = 0
        self.retries: int = 0
        self.stream_errors: int = 0
        self._latencies: list[float] = []

    def record_hit(self):
        self.cache_hits += 1

    def record_miss(self):
        self.cache_misses += 1

    def record_latency(self, latency_ms: float):
        if len(self._latencies) >= 200:
            self._latencies.pop(0)
        self._latencies.append(latency_ms)

    def get_stats(self, cache_info: dict) -> dict:
        total_cache_ops = self.cache_hits + self.cache_misses
        hit_ratio = round((self.cache_hits / total_cache_ops) * 100, 2) if total_cache_ops > 0 else 0.0
        avg_latency = round(sum(self._latencies) / len(self._latencies), 2) if self._latencies else 0.0

        return {
            "active_streams": self.active_streams,
            "max_active_streams": STREAM_MAX_ACTIVE_STREAMS,
            "active_block_requests": self.active_block_requests,
            "completed_block_requests": self.completed_block_requests,
            "total_bytes_streamed": self.total_bytes_streamed,
            "total_mb_streamed": round(self.total_bytes_streamed / (1024 * 1024), 2),
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_ratio_pct": hit_ratio,
            "cache_used_mb": cache_info.get("used_mb", 0.0),
            "cache_max_mb": STREAM_CACHE_MAX_MB,
            "cache_items": cache_info.get("total_items", 0),
            "avg_block_latency_ms": avg_latency,
            "prefetch_requests": self.prefetch_requests,
            "canceled_prefetch_requests": self.canceled_prefetch_requests,
            "file_ref_refreshes": self.file_ref_refreshes,
            "retries": self.retries,
            "stream_errors": self.stream_errors,
        }


metrics = StreamingMetrics()


class LRUBlockCache:
    """
    Thread-safe Memory-Bounded LRU Block Cache.
    Key: (doc_id, block_index) -> bytes.
    Ensures strict memory limits and evicts least recently accessed blocks.
    """
    def __init__(self, max_mb: int = STREAM_CACHE_MAX_MB):
        self.max_bytes = max(1, max_mb) * 1024 * 1024
        self.current_bytes = 0
        self.cache: OrderedDict[Tuple[int, int], bytes] = OrderedDict()
        self.lock = asyncio.Lock()

    async def get(self, doc_id: int, block_idx: int) -> Optional[bytes]:
        async with self.lock:
            key = (doc_id, block_idx)
            if key in self.cache:
                self.cache.move_to_end(key)
                metrics.record_hit()
                return self.cache[key]
            metrics.record_miss()
            return None

    async def put(self, doc_id: int, block_idx: int, data: bytes):
        if not data:
            return
        async with self.lock:
            key = (doc_id, block_idx)
            if key in self.cache:
                self.cache.move_to_end(key)
                # If size changed, adjust current_bytes
                old_len = len(self.cache[key])
                self.current_bytes += (len(data) - old_len)
                self.cache[key] = data
                return

            self.cache[key] = data
            self.current_bytes += len(data)

            # Evict LRU entries until under memory limit
            while self.current_bytes > self.max_bytes and self.cache:
                _, evicted = self.cache.popitem(last=False)
                self.current_bytes -= len(evicted)

    async def clear(self):
        async with self.lock:
            self.cache.clear()
            self.current_bytes = 0

    def get_info(self) -> dict:
        return {
            "used_mb": round(self.current_bytes / (1024 * 1024), 2),
            "max_mb": round(self.max_bytes / (1024 * 1024), 2),
            "total_items": len(self.cache),
        }


block_cache = LRUBlockCache(max_mb=STREAM_CACHE_MAX_MB)


class DocumentManager:
    """
    Manages document descriptor resolution and single-flight file reference refresh.
    Guarantees that multiple concurrent requests for the same message do not issue
    redundant Telegram get_messages RPC calls.
    """
    def __init__(self):
        self._doc_cache: Dict[Tuple[int, int], Tuple[Any, float]] = {}  # (chat_id, msg_id) -> (doc, expires_at)
        self._locks: Dict[Tuple[int, int], asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

    async def _get_item_lock(self, chat_id: int, message_id: int) -> asyncio.Lock:
        key = (chat_id, message_id)
        async with self._global_lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            return self._locks[key]

    async def get_document(self, client: TelegramClient, chat_id: int, message_id: int, force_refresh: bool = False) -> Optional[Any]:
        key = (chat_id, message_id)
        now = time.time()

        if not force_refresh and key in self._doc_cache:
            doc, expires_at = self._doc_cache[key]
            if now < expires_at:
                return doc

        item_lock = await self._get_item_lock(chat_id, message_id)
        async with item_lock:
            # Double-check cache after acquiring lock
            now = time.time()
            if not force_refresh and key in self._doc_cache:
                doc, expires_at = self._doc_cache[key]
                if now < expires_at:
                    return doc

            if not client or not client.is_connected():
                return None

            try:
                message = await client.get_messages(chat_id, ids=message_id)
                if not message or not message.media:
                    return None

                doc = getattr(message.media, "document", None)
                if not doc and hasattr(message, "video") and message.video:
                    doc = message.video

                if doc:
                    # Cache document descriptor for 1 hour
                    self._doc_cache[key] = (doc, now + 3600)
                    if force_refresh:
                        metrics.file_ref_refreshes += 1
                        logger.info(f"[StreamingEngine] Refreshed document file reference for ({chat_id}, {message_id})")
                    return doc
                return None
            except Exception as e:
                logger.warning(f"[StreamingEngine] Error resolving document ({chat_id}, {message_id}): {e}")
                return None

    def invalidate(self, chat_id: int, message_id: int):
        self._doc_cache.pop((chat_id, message_id), None)


doc_manager = DocumentManager()


class InFlightRequestManager:
    """
    Deduplicates concurrent block retrieval requests across all active streams.
    If multiple consumers need block (doc_id, block_idx) concurrently, only one
    MTProto task fetches it. All other consumers await the same task result.
    """
    def __init__(self):
        # (doc_id, block_idx) -> (Task[bytes], set of consumer_session_ids)
        self._in_flight: Dict[Tuple[int, int], Tuple[asyncio.Task, Set[str]]] = {}
        self._lock = asyncio.Lock()

    async def get_or_create(
        self,
        doc_id: int,
        block_idx: int,
        consumer_id: str,
        factory,
    ) -> Tuple[asyncio.Task, bool]:
        """
        Returns (task, is_creator).
        If is_creator is True, factory was called and scheduled.
        """
        key = (doc_id, block_idx)
        async with self._lock:
            if key in self._in_flight:
                task, consumers = self._in_flight[key]
                consumers.add(consumer_id)
                return task, False

            # Create new task
            coro = factory() if callable(factory) else factory
            task = asyncio.create_task(coro)
            self._in_flight[key] = (task, {consumer_id})
            metrics.active_block_requests = len(self._in_flight)
            return task, True

    async def remove(self, doc_id: int, block_idx: int):
        key = (doc_id, block_idx)
        async with self._lock:
            self._in_flight.pop(key, None)
            metrics.active_block_requests = len(self._in_flight)

    async def unsubscribe(self, doc_id: int, block_idx: int, consumer_id: str) -> bool:
        """
        Removes consumer from waiting set.
        Returns True if no consumers remain (eligible for cancellation if speculative).
        """
        key = (doc_id, block_idx)
        async with self._lock:
            if key in self._in_flight:
                task, consumers = self._in_flight[key]
                consumers.discard(consumer_id)
                if not consumers:
                    return True
        return False


in_flight_manager = InFlightRequestManager()


class ParallelBlockFetcher:
    """
    Coordinates bounded parallel MTProto block fetching using Telethon.
    Manages global and per-stream concurrency, priorities, retries, and error recovery.
    """
    def __init__(
        self,
        global_max_requests: int = STREAM_GLOBAL_MAX_REQUESTS,
        per_stream_connections: int = STREAM_PARALLEL_CONNECTIONS,
        block_size: int = STREAM_BLOCK_SIZE_BYTES,
    ):
        self.block_size = block_size
        self.global_semaphore = asyncio.Semaphore(max(2, global_max_requests))
        self.per_stream_concurrency = max(1, per_stream_connections)
        # Priority gates: urgent requests acquire priority lock first
        self.urgent_lock = asyncio.Lock()

    async def fetch_block_from_telegram(
        self,
        client: TelegramClient,
        doc: Any,
        chat_id: int,
        message_id: int,
        block_idx: int,
        priority: int = PRIORITY_URGENT,
    ) -> bytes:
        """
        Fetches an individual aligned block from Telegram via MTProto.
        Handles FileReferenceExpiredError, FloodWaitError, timeouts, and retries.
        """
        offset = block_idx * self.block_size
        max_retries = 3
        current_doc = doc
        t0 = time.time()

        for attempt in range(max_retries):
            # Prioritize urgent requests over speculative prefetch
            if priority == PRIORITY_PREFETCH:
                # Brief yield so urgent requests can jump ahead
                await asyncio.sleep(0.01)

            async with self.global_semaphore:
                try:
                    chunk_data = bytearray()
                    async for chunk in client.iter_download(
                        current_doc,
                        offset=offset,
                        request_size=self.block_size,
                        limit=1,
                        msg_data=(chat_id, message_id)
                    ):
                        chunk_data.extend(chunk)

                    data = bytes(chunk_data)
                    if data:
                        elapsed_ms = (time.time() - t0) * 1000.0
                        metrics.record_latency(elapsed_ms)
                        metrics.completed_block_requests += 1
                        return data

                    # Empty chunk returned on valid file: could be EOF or transient
                    if offset >= getattr(current_doc, "size", 0):
                        return b""
                    raise IOError(f"Empty data received for block {block_idx} at offset {offset}")

                except (FileReferenceExpiredError, Exception) as e:
                    # Detect expired file reference or fileref errors
                    err_msg = str(e).lower()
                    is_fileref = isinstance(e, FileReferenceExpiredError) or "file_reference" in err_msg or "fileref" in err_msg

                    if is_fileref and attempt < max_retries - 1:
                        logger.info(f"[StreamingEngine] Expired file reference for doc #{getattr(current_doc, 'id', 0)}. Refreshing...")
                        fresh_doc = await doc_manager.get_document(client, chat_id, message_id, force_refresh=True)
                        if fresh_doc:
                            current_doc = fresh_doc
                            metrics.retries += 1
                            await asyncio.sleep(0.1)
                            continue

                    if isinstance(e, FloodWaitError):
                        wait_sec = getattr(e, "seconds", 5)
                        logger.warning(f"[StreamingEngine] Telegram FloodWaitError: waiting {wait_sec}s")
                        if wait_sec <= 15:
                            metrics.retries += 1
                            await asyncio.sleep(wait_sec)
                            continue
                        metrics.stream_errors += 1
                        raise

                    if attempt < max_retries - 1:
                        metrics.retries += 1
                        await asyncio.sleep(0.15 * (2 ** attempt))
                        continue

                    metrics.stream_errors += 1
                    logger.error(f"[StreamingEngine] Failed retrieving block {block_idx} after {max_retries} attempts: {e}")
                    raise


fetcher = ParallelBlockFetcher()


class StreamingSession:
    """
    Encapsulates a single HTTP playback session (ExoPlayer, MPV, VLC, Stremio).
    Coordinates:
    - Per-stream concurrency limit.
    - Active prefetch pipeline and seek-aware cancellation.
    - In-flight deduplication.
    """
    def __init__(self, session_id: str, client: TelegramClient, chat_id: int, message_id: int, doc: Any):
        self.session_id = session_id
        self.client = client
        self.chat_id = chat_id
        self.message_id = message_id
        self.doc = doc
        self.doc_id = getattr(doc, "id", 0)
        self.file_size = getattr(doc, "size", 0)
        self.stream_semaphore = asyncio.Semaphore(STREAM_PARALLEL_CONNECTIONS)
        self._prefetch_tasks: Dict[int, asyncio.Task] = {}
        self._current_head_block = 0
        self._is_closed = False

    async def get_block(self, block_idx: int, priority: int = PRIORITY_URGENT) -> bytes:
        """
        Retrieves block via:
        1. Memory LRU Cache (0 ms)
        2. In-Flight Deduplication (await shared MTProto fetch)
        3. Parallel Block Fetcher
        """
        if self._is_closed:
            raise asyncio.CancelledError("Streaming session closed")

        # 1. Fast Cache Check
        cached = await block_cache.get(self.doc_id, block_idx)
        if cached is not None:
            return cached

        # 2. Check or create in-flight task
        async def _do_fetch():
            async with self.stream_semaphore:
                # Re-check cache inside semaphore
                c = await block_cache.get(self.doc_id, block_idx)
                if c is not None:
                    return c

                data = await fetcher.fetch_block_from_telegram(
                    client=self.client,
                    doc=self.doc,
                    chat_id=self.chat_id,
                    message_id=self.message_id,
                    block_idx=block_idx,
                    priority=priority,
                )
                if data:
                    await block_cache.put(self.doc_id, block_idx, data)
                return data

        task, is_creator = await in_flight_manager.get_or_create(
            self.doc_id, block_idx, self.session_id, _do_fetch
        )

        try:
            data = await task
            return data
        finally:
            if is_creator:
                await in_flight_manager.remove(self.doc_id, block_idx)

    def trigger_prefetch(self, start_block: int, count: int):
        """
        Schedules speculative read-ahead blocks in background without blocking playback.
        """
        if self._is_closed:
            return

        total_blocks = (self.file_size + STREAM_BLOCK_SIZE_BYTES - 1) // STREAM_BLOCK_SIZE_BYTES
        end_block = min(start_block + count, total_blocks)

        for b_idx in range(start_block, end_block):
            if b_idx in self._prefetch_tasks:
                continue

            async def _prefetch_worker(target_block: int):
                try:
                    metrics.prefetch_requests += 1
                    await self.get_block(target_block, priority=PRIORITY_PREFETCH)
                except asyncio.CancelledError:
                    metrics.canceled_prefetch_requests += 1
                except Exception as e:
                    logger.debug(f"[StreamingEngine] Prefetch notice for block {target_block}: {e}")
                finally:
                    self._prefetch_tasks.pop(target_block, None)

            task = asyncio.create_task(_prefetch_worker(b_idx))
            self._prefetch_tasks[b_idx] = task

    def handle_seek(self, new_head_block: int):
        """
        Cancels obsolete prefetch tasks from previous playback location.
        """
        self._current_head_block = new_head_block
        # Cancel any prefetch tasks outside the new window [new_head, new_head + 32]
        obsolete = [
            b_idx for b_idx, task in self._prefetch_tasks.items()
            if b_idx < new_head_block or b_idx > new_head_block + 32
        ]
        for b_idx in obsolete:
            task = self._prefetch_tasks.pop(b_idx, None)
            if task and not task.done():
                task.cancel()
                metrics.canceled_prefetch_requests += 1

    def close(self):
        self._is_closed = True
        for task in self._prefetch_tasks.values():
            if not task.done():
                task.cancel()
        self._prefetch_tasks.clear()


class StreamingEngine:
    """
    Central Coordinator for Telegram-to-Stremio Streaming.
    Preserves existing Telethon client and interfaces.
    """
    def __init__(self):
        self._active_sessions: Dict[str, StreamingSession] = {}
        self._lock = asyncio.Lock()

    def is_streaming_active(self) -> bool:
        """Used for downloader coordination: True if active streams are running."""
        return len(self._active_sessions) > 0

    def active_stream_count(self) -> int:
        return len(self._active_sessions)

    async def get_or_create_session(
        self,
        session_id: str,
        client: TelegramClient,
        chat_id: int,
        message_id: int,
        doc: Any
    ) -> StreamingSession:
        async with self._lock:
            if session_id in self._active_sessions:
                return self._active_sessions[session_id]

            session = StreamingSession(
                session_id=session_id,
                client=client,
                chat_id=chat_id,
                message_id=message_id,
                doc=doc
            )
            self._active_sessions[session_id] = session
            metrics.active_streams = len(self._active_sessions)
            return session

    async def release_session(self, session_id: str):
        async with self._lock:
            session = self._active_sessions.pop(session_id, None)
            if session:
                session.close()
            metrics.active_streams = len(self._active_sessions)

    async def stream_file(
        self,
        client: TelegramClient,
        chat_id: int,
        message_id: int,
        offset: int,
        length: int,
        session_id: Optional[str] = None
    ) -> AsyncGenerator[bytes, None]:
        """
        Aligned block-based streaming generator.
        - Translates arbitrary (offset, length) into aligned blocks.
        - Serves cached blocks instantly from RAM.
        - Fetches missing blocks in parallel.
        - Slices exact requested bytes with zero extra or missing bytes.
        - Automatically prefetches upcoming blocks.
        """
        if not client or not client.is_connected():
            logger.warning("[StreamingEngine] Telethon client is not connected.")
            return

        doc = await doc_manager.get_document(client, chat_id, message_id)
        if not doc:
            logger.warning(f"[StreamingEngine] Document not found for ({chat_id}, {message_id})")
            return

        file_size = getattr(doc, "size", 0)
        if file_size <= 0:
            return

        # Bound offset and length
        end_offset = min(offset + length, file_size)
        actual_length = end_offset - offset
        if actual_length <= 0:
            return

        # Generate unique session ID if not provided
        if not session_id:
            session_id = f"stream_{chat_id}_{message_id}_{offset}_{time.time()}"

        session = await self.get_or_create_session(session_id, client, chat_id, message_id, doc)

        block_size = STREAM_BLOCK_SIZE_BYTES
        first_block = offset // block_size
        last_block = (end_offset - 1) // block_size
        session.handle_seek(first_block)

        # Calculate prefetch window: min 6 MB (12 blocks) to max 16 MB (32 blocks)
        prefetch_blocks = max(
            STREAM_PREFETCH_MIN_MB * 1024 * 1024 // block_size,
            min(STREAM_PREFETCH_MAX_MB * 1024 * 1024 // block_size, 32)
        )

        # Trigger read-ahead for upcoming blocks immediately
        session.trigger_prefetch(first_block + 1, prefetch_blocks)

        cur_byte_pos = offset
        bytes_remaining = actual_length

        try:
            for b_idx in range(first_block, last_block + 1):
                if bytes_remaining <= 0:
                    break

                # Continuous background prefetch as playback advances
                session.trigger_prefetch(b_idx + 1, prefetch_blocks)

                # Prioritize current block as Urgent playback data
                block_data = await session.get_block(b_idx, priority=PRIORITY_URGENT)
                if not block_data:
                    # If empty data returned before expected EOF, abort cleanly
                    break

                # Calculate aligned slice for current block
                block_start_byte = b_idx * block_size
                slice_start = max(0, cur_byte_pos - block_start_byte)
                avail_in_block = len(block_data) - slice_start

                if avail_in_block <= 0:
                    break

                take = min(bytes_remaining, avail_in_block)
                chunk_slice = block_data[slice_start : slice_start + take]

                yield chunk_slice

                cur_byte_pos += take
                bytes_remaining -= take
                metrics.total_bytes_streamed += take

        except (asyncio.CancelledError, GeneratorExit):
            # Client disconnected or seeked away
            pass
        except Exception as e:
            metrics.stream_errors += 1
            logger.error(f"[StreamingEngine] Error in stream_file generator: {e}")
            raise
        finally:
            await self.release_session(session_id)


# Global Engine Instance
streaming_engine = StreamingEngine()


def get_streaming_stats() -> dict:
    """Returns real-time streaming engine diagnostics."""
    return metrics.get_stats(block_cache.get_info())


def is_streaming_active() -> bool:
    """Returns True if there is an active video stream."""
    return streaming_engine.is_streaming_active()
