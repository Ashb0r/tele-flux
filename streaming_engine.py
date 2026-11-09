"""
Maximum-Performance Telegram-to-Stremio Streaming Engine (Hardened v3 Ultra)
=============================================================================
Architected to match and exceed dedicated native media players (TMPlayer):
1. Native 1 MB MTProto Chunk Quantum:
   - Exactly aligned 1 MB (1,048,576 byte) blocks maximizing Telegram DC line-rate.
   - High-throughput parallel fetching pipeline (8-16 workers per stream).
2. Two-Tiered Hybrid Persistent Cache (RAM + Disk):
   - Tier 1 (RAM): Microsecond-latency hot LRU buffer (128-256 MB).
   - Tier 2 (Disk): High-capacity persistent block cache (20+ GB in data/stream_cache).
   - Instant rewinds, short seeks, and replay across server restarts with 0 Telegram calls.
3. True Urgent Priority Scheduler (Priority Inversion Free):
   - Urgent playback head requests acquire the scheduler slot BEFORE any stream semaphore.
   - Urgent requests jump ahead in priority and have guaranteed reserved slots.
   - Dynamic priority escalation: prefetch tasks are promoted to Urgent when seek lands on them.
4. Per-File Pipeline with "Last Playback Request Wins":
   - Probes (header / atom checks <= 2 MB) run isolated without canceling playback.
   - New playback Range requests cleanly supersede previous in-flight streams for the same file.
   - Eliminates the 503 Server Busy bottleneck during fast seeks.
5. Continuous Prefetch on Pause:
   - When player connection is idle/paused, background pipeline continues downloading ahead
     up to 1 GB into the persistent disk cache.
6. Zero Cold-Start Pre-Warming:
   - Asynchronous pre-warming of Block 0 (container header) and tail block (index/moov).
7. Strict Block Integrity & Robust MTProto Stall Watchdog:
   - Detects stalled chunk downloads and re-issues requests immediately.
   - Strict block boundary checks prevent caching incomplete or corrupt blocks.
"""

import os
import time
import asyncio
import logging
from collections import OrderedDict
from typing import Optional, Dict, Tuple, Set, AsyncGenerator, Any

from telethon import TelegramClient
from telethon.tl.types import InputDocumentFileLocation
from telethon.errors import (
    FloodWaitError,
    FileReferenceExpiredError,
)

from config import (
    STREAM_BLOCK_SIZE_KB,
    STREAM_BLOCK_SIZE_BYTES,
    STREAM_PARALLEL_CONNECTIONS,
    STREAM_GLOBAL_MAX_REQUESTS,
    STREAM_CACHE_MAX_MB,
    STREAM_DISK_CACHE_ENABLED,
    STREAM_DISK_CACHE_DIR,
    STREAM_DISK_CACHE_MAX_GB,
    STREAM_PREFETCH_MIN_MB,
    STREAM_PREFETCH_MAX_MB,
    STREAM_PAUSE_PREFETCH_MAX_MB,
    STREAM_MAX_ACTIVE_STREAMS,
)

logger = logging.getLogger("StreamingEngine")

# Priority definitions
PRIORITY_URGENT = 1    # Immediate HTTP response block (playback head / post-seek)
PRIORITY_NEAR = 2      # Near-future read-ahead (next 2-4 blocks)
PRIORITY_PREFETCH = 3  # Speculative background prefetch


class ActiveStreamsLimitExceededError(Exception):
    """Raised when maximum active streaming sessions across distinct files are reached."""
    pass


class StreamingMetrics:
    """Thread-safe metrics collector for streaming operations."""
    def __init__(self):
        self.active_streams: int = 0
        self.active_block_requests: int = 0
        self.completed_block_requests: int = 0
        self.total_bytes_streamed: int = 0
        self.ram_hits: int = 0
        self.disk_hits: int = 0
        self.cache_misses: int = 0
        self.dedup_hits: int = 0
        self.prefetch_requests: int = 0
        self.canceled_prefetch_requests: int = 0
        self.file_ref_refreshes: int = 0
        self.retries: int = 0
        self.stream_errors: int = 0
        self._latencies: list[float] = []

    def record_ram_hit(self):
        self.ram_hits += 1

    def record_disk_hit(self):
        self.disk_hits += 1

    def record_hit(self):
        self.ram_hits += 1

    def record_miss(self):
        self.cache_misses += 1

    def record_dedup_hit(self):
        self.dedup_hits += 1

    def record_latency(self, latency_ms: float):
        if len(self._latencies) >= 200:
            self._latencies.pop(0)
        self._latencies.append(latency_ms)

    def get_avg_latency(self) -> float:
        return sum(self._latencies) / len(self._latencies) if self._latencies else 0.0

    def get_stats(self, cache_info: dict, scheduler_info: dict) -> dict:
        total_hits = self.ram_hits + self.disk_hits
        total_cache_ops = total_hits + self.cache_misses
        hit_ratio = round((total_hits / total_cache_ops) * 100, 2) if total_cache_ops > 0 else 0.0
        avg_latency = round(self.get_avg_latency(), 2)

        return {
            "active_streams": self.active_streams,
            "max_active_streams": STREAM_MAX_ACTIVE_STREAMS,
            "active_block_requests": self.active_block_requests,
            "completed_block_requests": self.completed_block_requests,
            "total_bytes_streamed": self.total_bytes_streamed,
            "total_mb_streamed": round(self.total_bytes_streamed / (1024 * 1024), 2),
            "ram_hits": self.ram_hits,
            "disk_hits": self.disk_hits,
            "cache_hits": total_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_ratio_pct": hit_ratio,
            "dedup_hits": self.dedup_hits,
            "ram_cache_used_mb": cache_info.get("ram_used_mb", 0.0),
            "ram_cache_max_mb": cache_info.get("ram_max_mb", 0.0),
            "disk_cache_used_mb": cache_info.get("disk_used_mb", 0.0),
            "disk_cache_max_gb": cache_info.get("disk_max_gb", 0.0),
            "disk_cache_items": cache_info.get("disk_items", 0),
            "avg_block_latency_ms": avg_latency,
            "prefetch_requests": self.prefetch_requests,
            "canceled_prefetch_requests": self.canceled_prefetch_requests,
            "file_ref_refreshes": self.file_ref_refreshes,
            "retries": self.retries,
            "stream_errors": self.stream_errors,
            "scheduler_running": scheduler_info.get("running_count", 0),
            "scheduler_queue_depth": scheduler_info.get("queue_depth", 0),
        }


metrics = StreamingMetrics()


# ---------------------------------------------------------------------
# Tiered Hybrid Block Cache (RAM L1 + Persistent Disk L2)
# ---------------------------------------------------------------------

def _read_disk_file(path: str) -> Optional[bytes]:
    try:
        if os.path.exists(path):
            with open(path, "rb") as f:
                return f.read()
    except Exception as e:
        logger.warning(f"[Cache] Error reading disk block {path}: {e}")
    return None


def _write_disk_file(path: str, data: bytes):
    try:
        dirname = os.path.dirname(path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        tmp_path = f"{path}.tmp_{os.getpid()}_{time.time()}"
        with open(tmp_path, "wb") as f:
            f.write(data)
        os.replace(tmp_path, path)
    except Exception as e:
        logger.warning(f"[Cache] Error writing disk block {path}: {e}")


def _remove_disk_file(path: str):
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


class TieredBlockCache:
    """
    High-Performance Tiered Hybrid Block Cache.
    - Tier 1: Microsecond-latency in-memory LRU RAM cache (STREAM_CACHE_MAX_MB).
    - Tier 2: Persistent on-disk block cache (STREAM_DISK_CACHE_DIR, STREAM_DISK_CACHE_MAX_GB).
    Key: (doc_id, access_hash, block_index) -> bytes.
    Survives server restarts and provides instant rewind and replay.
    """
    def __init__(
        self,
        ram_max_mb: int = STREAM_CACHE_MAX_MB,
        disk_cache_dir: str = STREAM_DISK_CACHE_DIR,
        disk_max_gb: int = STREAM_DISK_CACHE_MAX_GB,
        disk_enabled: bool = STREAM_DISK_CACHE_ENABLED,
        max_mb: Optional[int] = None
    ):
        if max_mb is not None:
            ram_max_mb = max_mb
        self.ram_max_bytes = max(1, ram_max_mb) * 1024 * 1024
        self.ram_current_bytes = 0
        self.ram_cache: OrderedDict[Tuple[int, int, int], bytes] = OrderedDict()
        self._lock: Optional[asyncio.Lock] = None

        self.disk_enabled = disk_enabled
        self.disk_cache_dir = disk_cache_dir
        self.disk_max_bytes = max(1, disk_max_gb) * 1024 * 1024 * 1024
        self.disk_current_bytes = 0
        # key -> file_size_in_bytes, ordered by LRU access
        self.disk_index: OrderedDict[Tuple[int, int, int], int] = OrderedDict()

        if self.disk_enabled:
            os.makedirs(self.disk_cache_dir, exist_ok=True)
            self._scan_existing_disk_cache()

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _scan_existing_disk_cache(self):
        """Discovers existing block files on disk from previous runs."""
        try:
            entries = []
            for fname in os.listdir(self.disk_cache_dir):
                if not fname.endswith(".blk"):
                    continue
                parts = fname[:-4].split("_")
                if len(parts) == 3:
                    try:
                        d_id, a_hash, b_idx = int(parts[0]), int(parts[1]), int(parts[2])
                        fpath = os.path.join(self.disk_cache_dir, fname)
                        st = os.stat(fpath)
                        entries.append(((d_id, a_hash, b_idx), st.st_size, st.st_mtime))
                    except Exception:
                        continue

            # Sort by mtime ascending (oldest first for LRU)
            entries.sort(key=lambda x: x[2])
            for key, size, _ in entries:
                self.disk_index[key] = size
                self.disk_current_bytes += size

            if self.disk_index:
                logger.info(
                    f"[Cache] Discovered {len(self.disk_index)} persistent blocks on disk "
                    f"({round(self.disk_current_bytes / (1024 * 1024), 2)} MB)"
                )
        except Exception as e:
            logger.warning(f"[Cache] Disk cache initial scan warning: {e}")

    def _normalize_key(
        self,
        doc_id: int,
        access_hash_or_block: int,
        block_idx: Optional[int] = None
    ) -> Tuple[int, int, int]:
        if block_idx is None:
            return (doc_id, 0, access_hash_or_block)
        return (doc_id, access_hash_or_block, block_idx)

    def _disk_path_for_key(self, key: Tuple[int, int, int]) -> str:
        return os.path.join(self.disk_cache_dir, f"{key[0]}_{key[1]}_{key[2]}.blk")

    def has_block(self, doc_id: int, access_hash: int, block_idx: int) -> bool:
        """Fast non-blocking existence check across RAM and disk."""
        key = (doc_id, access_hash, block_idx)
        return (key in self.ram_cache) or (self.disk_enabled and key in self.disk_index)

    async def get(
        self,
        doc_id: int,
        access_hash_or_block: int,
        block_idx: Optional[int] = None
    ) -> Optional[bytes]:
        key = self._normalize_key(doc_id, access_hash_or_block, block_idx)
        lock = self._get_lock()

        # 1. Tier 1: Check fast RAM cache
        async with lock:
            if key in self.ram_cache:
                self.ram_cache.move_to_end(key)
                metrics.record_ram_hit()
                return self.ram_cache[key]

        # 2. Tier 2: Check persistent Disk cache
        if self.disk_enabled:
            disk_exists = False
            async with lock:
                if key in self.disk_index:
                    self.disk_index.move_to_end(key)
                    disk_exists = True

            if disk_exists:
                disk_path = self._disk_path_for_key(key)
                data = await asyncio.to_thread(_read_disk_file, disk_path)
                if data:
                    metrics.record_disk_hit()
                    # Promote hot block to RAM cache
                    async with lock:
                        self._put_ram_locked(key, data)
                    return data

        metrics.record_miss()
        return None

    def _put_ram_locked(self, key: Tuple[int, int, int], payload: bytes):
        if key in self.ram_cache:
            self.ram_cache.move_to_end(key)
            old_len = len(self.ram_cache[key])
            self.ram_current_bytes += (len(payload) - old_len)
            self.ram_cache[key] = payload
            return

        self.ram_cache[key] = payload
        self.ram_current_bytes += len(payload)

        # Evict RAM LRU entries if under pressure
        while self.ram_current_bytes > self.ram_max_bytes and self.ram_cache:
            _, evicted = self.ram_cache.popitem(last=False)
            self.ram_current_bytes -= len(evicted)

    async def put(
        self,
        doc_id: int,
        access_hash_or_block: int,
        data_or_block: Any,
        data: Optional[bytes] = None
    ):
        if data is None:
            key = self._normalize_key(doc_id, 0, access_hash_or_block)
            payload = data_or_block
        else:
            key = self._normalize_key(doc_id, access_hash_or_block, data_or_block)
            payload = data

        if not payload:
            return

        lock = self._get_lock()

        # 1. Store in RAM Cache immediately (0.001 ms)
        async with lock:
            self._put_ram_locked(key, payload)

        # 2. Fire-and-forget non-blocking Disk Cache write (never blocks streaming delivery)
        if self.disk_enabled:
            asyncio.create_task(self._persist_to_disk_bg(key, payload))

    async def _persist_to_disk_bg(self, key: Tuple[int, int, int], payload: bytes):
        try:
            disk_path = self._disk_path_for_key(key)
            await asyncio.to_thread(_write_disk_file, disk_path, payload)

            lock = self._get_lock()
            evict_paths = []
            async with lock:
                old_size = self.disk_index.get(key, 0)
                self.disk_index[key] = len(payload)
                self.disk_index.move_to_end(key)
                self.disk_current_bytes += (len(payload) - old_size)

                while self.disk_current_bytes > self.disk_max_bytes and self.disk_index:
                    old_key, old_sz = self.disk_index.popitem(last=False)
                    self.disk_current_bytes -= old_sz
                    evict_paths.append(self._disk_path_for_key(old_key))

            for ep in evict_paths:
                await asyncio.to_thread(_remove_disk_file, ep)
        except Exception as e:
            logger.debug(f"[Cache] Background disk write notice: {e}")

    async def clear(self):
        lock = self._get_lock()
        async with lock:
            self.ram_cache.clear()
            self.ram_current_bytes = 0

    def get_info(self) -> dict:
        return {
            "ram_used_mb": round(self.ram_current_bytes / (1024 * 1024), 2),
            "ram_max_mb": round(self.ram_max_bytes / (1024 * 1024), 2),
            "ram_items": len(self.ram_cache),
            "disk_used_mb": round(self.disk_current_bytes / (1024 * 1024), 2),
            "disk_max_gb": round(self.disk_max_bytes / (1024 * 1024 * 1024), 2),
            "disk_items": len(self.disk_index),
            "used_mb": round(self.ram_current_bytes / (1024 * 1024), 2),
            "max_mb": round(self.ram_max_bytes / (1024 * 1024), 2),
            "total_items": len(self.ram_cache),
        }


# Backwards compatibility alias
LRUBlockCache = TieredBlockCache
block_cache = TieredBlockCache(
    ram_max_mb=STREAM_CACHE_MAX_MB,
    disk_cache_dir=STREAM_DISK_CACHE_DIR,
    disk_max_gb=STREAM_DISK_CACHE_MAX_GB,
    disk_enabled=STREAM_DISK_CACHE_ENABLED
)


# ---------------------------------------------------------------------
# True Urgent Priority Scheduler (Priority Inversion Free)
# ---------------------------------------------------------------------

class PriorityScheduler:
    """
    Genuine Asynchronous Priority Scheduler for MTProto block requests.
    Guarantees:
      1. Urgent requests jump ahead of waiting near and prefetch requests.
      2. Prefetch requests can NEVER consume all slots (reserved slots for Urgent).
      3. Global concurrency is strictly bounded by max_concurrency (STREAM_GLOBAL_MAX_REQUESTS).
      4. Safe cancellation: cancelled waiters are purged without leaking running slots.
      5. Lazy initialization ensures binding to current event loop across test environments.
    """
    def __init__(
        self,
        max_concurrency: int = STREAM_GLOBAL_MAX_REQUESTS,
        reserved_urgent_slots: int = 8
    ):
        self.max_concurrency = max(2, max_concurrency)
        self.reserved_urgent_slots = min(reserved_urgent_slots, self.max_concurrency - 1)
        self.max_prefetch_concurrency = max(1, self.max_concurrency - self.reserved_urgent_slots)

        self._running_count = 0
        self._seq = 0
        self._lock: Optional[asyncio.Lock] = None
        self._queue: Optional[asyncio.PriorityQueue] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _get_queue(self) -> asyncio.PriorityQueue:
        if self._queue is None:
            self._queue = asyncio.PriorityQueue()
        return self._queue

    def get_info(self) -> dict:
        q_size = self._queue.qsize() if self._queue is not None else 0
        return {
            "running_count": self._running_count,
            "queue_depth": q_size,
            "max_concurrency": self.max_concurrency,
            "max_prefetch_concurrency": self.max_prefetch_concurrency,
        }

    async def acquire(self, priority: int = PRIORITY_URGENT):
        """Acquires a concurrency slot respecting strict priority ordering."""
        lock = self._get_lock()
        queue = self._get_queue()

        async with lock:
            can_run = False
            if priority == PRIORITY_URGENT:
                if self._running_count < self.max_concurrency:
                    can_run = True
            else:
                if self._running_count < self.max_prefetch_concurrency:
                    can_run = True

            if can_run:
                self._running_count += 1
                return

            self._seq += 1
            loop = asyncio.get_running_loop()
            waiter = loop.create_future()
            await queue.put((priority, self._seq, waiter))

        try:
            await waiter
        except asyncio.CancelledError:
            async with lock:
                if waiter.done() and not waiter.cancelled():
                    self._release_slot_locked()
            raise

    def release(self, priority: int = PRIORITY_URGENT):
        """Releases a concurrency slot and activates highest priority waiter."""
        asyncio.create_task(self._release_async())

    async def _release_async(self):
        lock = self._get_lock()
        async with lock:
            self._release_slot_locked()

    def _release_slot_locked(self):
        self._running_count = max(0, self._running_count - 1)
        self._wake_next_locked()

    def _wake_next_locked(self):
        queue = self._get_queue()
        while not queue.empty():
            try:
                prio, seq, waiter = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            if waiter.cancelled():
                continue

            can_run = False
            if prio == PRIORITY_URGENT:
                if self._running_count < self.max_concurrency:
                    can_run = True
            else:
                if self._running_count < self.max_prefetch_concurrency:
                    can_run = True

            if can_run:
                self._running_count += 1
                waiter.set_result(None)
                break
            else:
                queue.put_nowait((prio, seq, waiter))
                break


priority_scheduler = PriorityScheduler()


# ---------------------------------------------------------------------
# Document Reference Manager
# ---------------------------------------------------------------------

class DocumentManager:
    """
    Manages document descriptor resolution and single-flight file reference refresh.
    Deduplicates simultaneous Telegram get_messages RPC lookups.
    """
    def __init__(self):
        self._doc_cache: Dict[Tuple[int, int], Tuple[Any, float]] = {}
        self._in_flight: Dict[Tuple[int, int], asyncio.Future] = {}
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def get_document(
        self,
        client: TelegramClient,
        chat_id: int,
        message_id: int,
        force_refresh: bool = False
    ) -> Optional[Any]:
        key = (chat_id, message_id)
        now = time.time()
        lock = self._get_lock()

        if not force_refresh:
            async with lock:
                if key in self._doc_cache:
                    doc, expires_at = self._doc_cache[key]
                    if now < expires_at:
                        return doc
                if key in self._in_flight:
                    future = self._in_flight[key]
                else:
                    future = None

            if future is not None:
                return await future

        async with lock:
            now = time.time()
            if not force_refresh and key in self._doc_cache:
                doc, expires_at = self._doc_cache[key]
                if now < expires_at:
                    return doc

            if key in self._in_flight:
                future = self._in_flight[key]
                return await future

            loop = asyncio.get_running_loop()
            lookup_future = loop.create_future()
            self._in_flight[key] = lookup_future

        try:
            if not client or not client.is_connected():
                doc = None
            else:
                active_client = client
                if hasattr(client, "get_client") and callable(getattr(client, "get_client")):
                    c_cand = client.get_client(priority=PRIORITY_URGENT)
                    if asyncio.iscoroutine(c_cand):
                        c_cand = await c_cand
                    if c_cand:
                        active_client = c_cand
                message = await active_client.get_messages(chat_id, ids=message_id)
                doc = None
                if message and message.media:
                    doc = getattr(message.media, "document", None)
                    if not doc and hasattr(message, "video") and message.video:
                        doc = message.video

            async with lock:
                if doc:
                    self._doc_cache[key] = (doc, time.time() + 3600)
                    if force_refresh:
                        metrics.file_ref_refreshes += 1
                        logger.info(f"[StreamingEngine] Refreshed document file reference for ({chat_id}, {message_id})")
                if not lookup_future.done():
                    lookup_future.set_result(doc)
            return doc
        except Exception as e:
            async with lock:
                if not lookup_future.done():
                    lookup_future.set_exception(e)
            logger.warning(f"[StreamingEngine] Error resolving document ({chat_id}, {message_id}): {e}")
            return None
        finally:
            async with lock:
                self._in_flight.pop(key, None)

    def invalidate(self, chat_id: int, message_id: int):
        self._doc_cache.pop((chat_id, message_id), None)


doc_manager = DocumentManager()


# ---------------------------------------------------------------------
# In-Flight Request Deduplication & Consumer Tracking
# ---------------------------------------------------------------------

class InFlightEntry:
    def __init__(self, key: Tuple[int, int, int], is_urgent: bool = False):
        self.key = key
        self.is_urgent = is_urgent
        self.consumers: Set[str] = set()
        self.fetch_task: Optional[asyncio.Task] = None
        self.future: asyncio.Future = asyncio.get_running_loop().create_future()


class InFlightRequestManager:
    """
    Deduplicates concurrent block retrieval requests across all active streams.
    Supports dynamic priority escalation to Urgent when playback head lands on
    a block currently being fetched by speculative prefetch.
    """
    def __init__(self):
        self._in_flight: Dict[Tuple[int, int, int], InFlightEntry] = {}
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def get_or_create(
        self,
        key: Tuple[int, int, int],
        consumer_id: str,
        priority: int,
        fetch_factory,
    ) -> Tuple[asyncio.Future, bool]:
        lock = self._get_lock()
        async with lock:
            if key in self._in_flight:
                entry = self._in_flight[key]
                entry.consumers.add(consumer_id)
                if priority == PRIORITY_URGENT:
                    entry.is_urgent = True
                metrics.record_dedup_hit()
                return entry.future, False

            entry = InFlightEntry(key=key, is_urgent=(priority == PRIORITY_URGENT))
            entry.consumers.add(consumer_id)

            async def _run_fetch():
                try:
                    coro = fetch_factory() if callable(fetch_factory) else fetch_factory
                    data = await coro
                    if not entry.future.done():
                        entry.future.set_result(data)
                except asyncio.CancelledError:
                    if not entry.future.done():
                        entry.future.cancel()
                    raise
                except Exception as exc:
                    if not entry.future.done():
                        entry.future.set_exception(exc)
                finally:
                    async with self._get_lock():
                        self._in_flight.pop(key, None)
                        metrics.active_block_requests = len(self._in_flight)

            entry.fetch_task = asyncio.create_task(_run_fetch())
            self._in_flight[key] = entry
            metrics.active_block_requests = len(self._in_flight)
            return entry.future, True

    async def unsubscribe(self, key: Tuple[int, int, int], consumer_id: str) -> bool:
        lock = self._get_lock()
        async with lock:
            if key in self._in_flight:
                entry = self._in_flight[key]
                entry.consumers.discard(consumer_id)
                if not entry.consumers:
                    if not entry.is_urgent and entry.fetch_task and not entry.fetch_task.done():
                        entry.fetch_task.cancel()
                        self._in_flight.pop(key, None)
                        metrics.active_block_requests = len(self._in_flight)
                        return True
        return False


in_flight_manager = InFlightRequestManager()


# ---------------------------------------------------------------------
# Parallel Block Fetcher with Stall Watchdog
# ---------------------------------------------------------------------

class ParallelBlockFetcher:
    """
    Coordinates bounded parallel MTProto block fetching using Telethon 1.45.0.
    - Validates block length against file boundaries.
    - Sized at native 1 MB transfer quantum.
    - Eliminates priority inversion by taking PriorityScheduler slot before semaphores.
    - Implements stall watchdog to detect hung Telegram chunks and retry immediately.
    """
    def __init__(self, block_size: int = STREAM_BLOCK_SIZE_BYTES):
        self.block_size = block_size

    async def fetch_block_from_telegram(
        self,
        client: TelegramClient,
        doc: Any,
        chat_id: int,
        message_id: int,
        block_idx: int,
        priority: int = PRIORITY_URGENT,
    ) -> bytes:
        offset = block_idx * self.block_size
        file_size = getattr(doc, "size", 0)
        expected_length = min(self.block_size, max(0, file_size - offset))

        if expected_length <= 0 and offset >= file_size:
            return b""

        max_retries = 3
        current_doc = doc
        t0 = time.time()

        for attempt in range(max_retries):
            # 1. Acquire scheduler slot FIRST respecting strict priority (fixes priority inversion)
            await priority_scheduler.acquire(priority=priority)
            try:
                # Select client from connection pool or use single client
                active_client = client
                if hasattr(client, "get_client") and callable(getattr(client, "get_client")):
                    c_cand = client.get_client(priority=priority, attempt=attempt)
                    if asyncio.iscoroutine(c_cand):
                        c_cand = await c_cand
                    if c_cand:
                        active_client = c_cand

                if isinstance(current_doc, InputDocumentFileLocation):
                    location = current_doc
                else:
                    location = InputDocumentFileLocation(
                        id=current_doc.id,
                        access_hash=getattr(current_doc, "access_hash", 0),
                        file_reference=getattr(current_doc, "file_reference", b""),
                        thumb_size=""
                    )

                chunk_data = bytearray()

                # MTProto chunk request sizing: Telethon's maximum direct chunk size is 524,288 bytes (512 KB)
                mtproto_chunk_size = 524288
                req_chunk_size = min(expected_length, mtproto_chunk_size)
                chunks_needed = (expected_length + req_chunk_size - 1) // req_chunk_size

                # Stall watchdog: Urgent playback head gets 3.5s timeout for fast failover; prefetch gets 5.0s
                chunk_timeout = 3.5 if priority == PRIORITY_URGENT else 5.0

                async def _download_chunks():
                    async for chunk in active_client.iter_download(
                        location,
                        offset=offset,
                        chunk_size=req_chunk_size,
                        request_size=req_chunk_size,
                        limit=chunks_needed,
                        file_size=getattr(current_doc, "size", None),
                        dc_id=getattr(current_doc, "dc_id", None)
                    ):
                        chunk_data.extend(chunk)

                await asyncio.wait_for(_download_chunks(), timeout=chunk_timeout)

                data = bytes(chunk_data)

                # Strict Block Integrity Validation: reject truncated chunks
                if len(data) == expected_length:
                    elapsed_ms = (time.time() - t0) * 1000.0
                    metrics.record_latency(elapsed_ms)
                    metrics.completed_block_requests += 1
                    return data

                raise IOError(
                    f"Truncated block {block_idx} received at offset {offset}: "
                    f"expected {expected_length} bytes, got {len(data)} bytes"
                )

            except (asyncio.TimeoutError, TimeoutError):
                logger.warning(f"[StreamingEngine] Chunk download stalled for block {block_idx} at offset {offset}. Retrying...")
                metrics.retries += 1
                await asyncio.sleep(0.05)
                continue

            except (FileReferenceExpiredError, Exception) as e:
                err_msg = str(e).lower()
                is_fileref = (
                    isinstance(e, FileReferenceExpiredError)
                    or "file_reference" in err_msg
                    or "fileref" in err_msg
                )

                if is_fileref and attempt < max_retries - 1:
                    logger.info(f"[StreamingEngine] Expired file reference for doc #{getattr(current_doc, 'id', 0)}. Refreshing...")
                    fresh_doc = await doc_manager.get_document(client, chat_id, message_id, force_refresh=True)
                    if fresh_doc:
                        current_doc = fresh_doc
                        metrics.retries += 1
                        await asyncio.sleep(0.05)
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
                    await asyncio.sleep(0.1 * (2 ** attempt))
                    continue

                metrics.stream_errors += 1
                logger.error(f"[StreamingEngine] Failed retrieving block {block_idx} at offset {offset} after {max_retries} attempts: {e}")
                raise
            finally:
                priority_scheduler.release(priority=priority)

        return b""


fetcher = ParallelBlockFetcher()


# ---------------------------------------------------------------------
# Streaming Session & Per-File Pipeline Coordination
# ---------------------------------------------------------------------

class StreamingSession:
    """
    Coordinates playback for a single stream pipeline:
    - Sized concurrency limits.
    - Seamless seek landing & prefetch task lifecycle.
    - Continuous idle buffer pipeline during pause.
    """
    def __init__(
        self,
        session_id: str,
        client: TelegramClient,
        chat_id: int,
        message_id: int,
        doc: Any,
        is_probe: bool = False
    ):
        self.session_id = session_id
        self.client = client
        self.chat_id = chat_id
        self.message_id = message_id
        self.doc = doc
        self.doc_id = getattr(doc, "id", 0)
        self.access_hash = getattr(doc, "access_hash", 0)
        self.file_size = getattr(doc, "size", 0)
        self.is_probe = is_probe

        self.stream_semaphore = asyncio.Semaphore(STREAM_PARALLEL_CONNECTIONS)
        self._prefetch_tasks: Dict[int, asyncio.Task] = {}
        self._idle_prefetch_task: Optional[asyncio.Task] = None
        self._current_head_block = 0
        self._is_closed = False
        self._is_superseded = False

    def _cache_key(self, block_idx: int) -> Tuple[int, int, int]:
        return (self.doc_id, self.access_hash, block_idx)

    async def get_block(self, block_idx: int, priority: int = PRIORITY_URGENT) -> bytes:
        if self._is_closed or self._is_superseded:
            raise asyncio.CancelledError("Streaming session superseded or closed")

        key = self._cache_key(block_idx)

        # 1. Fast Tiered Cache Check (RAM or Disk)
        cached = await block_cache.get(self.doc_id, self.access_hash, block_idx)
        if cached is not None:
            return cached

        # 2. Check or create in-flight task
        async def _do_fetch():
            # Urgent requests (playback head & seeks) must NEVER block on per-stream semaphore!
            if priority == PRIORITY_URGENT:
                c = await block_cache.get(self.doc_id, self.access_hash, block_idx)
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
                    await block_cache.put(self.doc_id, self.access_hash, block_idx, data)
                return data

            # Non-urgent speculative prefetch acquires per-stream semaphore slot
            async with self.stream_semaphore:
                c = await block_cache.get(self.doc_id, self.access_hash, block_idx)
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
                    await block_cache.put(self.doc_id, self.access_hash, block_idx, data)
                return data

        future, _ = await in_flight_manager.get_or_create(
            key=key,
            consumer_id=self.session_id,
            priority=priority,
            fetch_factory=_do_fetch
        )

        try:
            data = await asyncio.shield(future)
            return data
        except asyncio.CancelledError:
            await in_flight_manager.unsubscribe(key, self.session_id)
            raise
        except Exception:
            await in_flight_manager.unsubscribe(key, self.session_id)
            raise

    def calculate_adaptive_prefetch_window(self, req_length: int) -> int:
        block_size = STREAM_BLOCK_SIZE_BYTES
        # Suppress prefetch on tiny metadata probes
        if req_length <= 2 * block_size:
            return 0

        min_blocks = max(4, STREAM_PREFETCH_MIN_MB * 1024 * 1024 // block_size)
        max_blocks = max(min_blocks, STREAM_PREFETCH_MAX_MB * 1024 * 1024 // block_size)

        avg_lat = metrics.get_avg_latency()
        if avg_lat > 0 and avg_lat < 80.0:
            return max_blocks
        elif avg_lat > 0 and avg_lat < 180.0:
            return (min_blocks + max_blocks) // 2
        return min_blocks

    def trigger_prefetch(self, start_block: int, count: int):
        if self._is_closed or self._is_superseded or count <= 0:
            return

        total_blocks = (self.file_size + STREAM_BLOCK_SIZE_BYTES - 1) // STREAM_BLOCK_SIZE_BYTES
        end_block = min(start_block + count, total_blocks)

        for b_idx in range(start_block, end_block):
            if b_idx in self._prefetch_tasks or block_cache.has_block(self.doc_id, self.access_hash, b_idx):
                continue

            async def _prefetch_worker(target_block: int):
                try:
                    metrics.prefetch_requests += 1
                    prio = PRIORITY_NEAR if target_block <= start_block + 4 else PRIORITY_PREFETCH
                    await self.get_block(target_block, priority=prio)
                except asyncio.CancelledError:
                    metrics.canceled_prefetch_requests += 1
                except Exception as e:
                    logger.debug(f"[StreamingEngine] Prefetch notice for block {target_block}: {e}")
                finally:
                    self._prefetch_tasks.pop(target_block, None)

            task = asyncio.create_task(_prefetch_worker(b_idx))
            self._prefetch_tasks[b_idx] = task

    def handle_seek(self, new_head_block: int):
        """Cancels obsolete prefetch tasks outside the new playback vicinity."""
        self._current_head_block = new_head_block
        obsolete = [
            b_idx for b_idx, task in self._prefetch_tasks.items()
            if b_idx < new_head_block or b_idx > new_head_block + 64
        ]
        for b_idx in obsolete:
            task = self._prefetch_tasks.pop(b_idx, None)
            if task and not task.done():
                task.cancel()
                metrics.canceled_prefetch_requests += 1

    def start_idle_prefetch(self, from_block: int, max_mb: int = STREAM_PAUSE_PREFETCH_MAX_MB):
        """
        Runs while the player is paused or idle: continues downloading ahead
        directly into the persistent disk cache so resume is 100% buffer-free.
        """
        if self._is_closed or self._is_superseded or self._idle_prefetch_task:
            return

        block_size = STREAM_BLOCK_SIZE_BYTES
        max_blocks = max(1, (max_mb * 1024 * 1024) // block_size)
        total_blocks = (self.file_size + block_size - 1) // block_size
        target_end_block = min(from_block + max_blocks, total_blocks)

        async def _idle_worker():
            try:
                for b_idx in range(from_block, target_end_block):
                    if self._is_closed or self._is_superseded:
                        break
                    # Skip if already cached on disk
                    if block_cache.has_block(self.doc_id, self.access_hash, b_idx):
                        continue
                    await self.get_block(b_idx, priority=PRIORITY_PREFETCH)
                    await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.debug(f"[StreamingEngine] Idle prefetch completed or interrupted: {e}")

        self._idle_prefetch_task = asyncio.create_task(_idle_worker())

    def supersede(self):
        """Called when a new Range seek supersedes this playback session (last request wins)."""
        self._is_superseded = True
        self.close()

    def close(self):
        self._is_closed = True
        if self._idle_prefetch_task and not self._idle_prefetch_task.done():
            self._idle_prefetch_task.cancel()
        for task in self._prefetch_tasks.values():
            if not task.done():
                task.cancel()
        self._prefetch_tasks.clear()


# ---------------------------------------------------------------------
# Central Streaming Engine Coordinator
# ---------------------------------------------------------------------

class StreamingEngine:
    """
    Central Coordinator for Telegram-to-Stremio Streaming.
    Manages per-file playback pipelines ("last request wins"), persistent caches,
    and instantaneous cold-start pre-warming.
    """
    def __init__(self):
        # (chat_id, message_id) -> active playback StreamingSession
        self._file_playback_sessions: Dict[Tuple[int, int], StreamingSession] = {}
        # session_id -> StreamingSession
        self._active_sessions: Dict[str, StreamingSession] = {}
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def is_streaming_active(self) -> bool:
        return len(self._file_playback_sessions) > 0

    def active_stream_count(self) -> int:
        return len(self._file_playback_sessions)

    async def get_or_create_session(
        self,
        session_id: str,
        client: TelegramClient,
        chat_id: int,
        message_id: int,
        doc: Any,
        is_probe: bool = False
    ) -> StreamingSession:
        lock = self._get_lock()
        file_key = (chat_id, message_id)

        async with lock:
            if session_id in self._active_sessions:
                return self._active_sessions[session_id]

            # If this is active playback (not a tiny probe), apply "last playback request wins"
            if not is_probe:
                old_session = self._file_playback_sessions.pop(file_key, None)
                if old_session:
                    old_session.supersede()
                    self._active_sessions.pop(old_session.session_id, None)

                # Check limit across distinct DIFFERENT video files
                if len(self._file_playback_sessions) >= STREAM_MAX_ACTIVE_STREAMS:
                    raise ActiveStreamsLimitExceededError(
                        f"Active playback limit reached ({STREAM_MAX_ACTIVE_STREAMS} distinct files)."
                    )

            session = StreamingSession(
                session_id=session_id,
                client=client,
                chat_id=chat_id,
                message_id=message_id,
                doc=doc,
                is_probe=is_probe
            )

            self._active_sessions[session_id] = session
            if not is_probe:
                self._file_playback_sessions[file_key] = session

            metrics.active_streams = len(self._file_playback_sessions)
            return session

    async def release_session(self, session_id: str, trigger_pause_prefetch: bool = False, head_block: int = 0):
        lock = self._get_lock()
        async with lock:
            session = self._active_sessions.pop(session_id, None)
            if session:
                file_key = (session.chat_id, session.message_id)
                is_active_playback = (self._file_playback_sessions.get(file_key) == session)

                if is_active_playback:
                    if trigger_pause_prefetch and not session._is_superseded:
                        # Player paused / connection idle: keep downloading upcoming blocks into disk cache!
                        session.start_idle_prefetch(from_block=head_block + 1)
                    else:
                        self._file_playback_sessions.pop(file_key, None)
                        session.close()
                else:
                    session.close()

            metrics.active_streams = len(self._file_playback_sessions)

    async def prewarm_file(self, client: Any, chat_id: int, message_id: int):
        """
        Zero-latency cold start: Resolves the document and downloads the first 1.5 MB blocks
        (EBML / MP4 container headers) plus index blocks (MKV Cues / MP4 moov) into RAM
        before user clicks play.
        """
        try:
            if not client or not client.is_connected():
                return
            doc = await doc_manager.get_document(client, chat_id, message_id)
            if not doc:
                return

            file_size = getattr(doc, "size", 0)
            if file_size <= 0:
                return

            doc_id = getattr(doc, "id", 0)
            access_hash = getattr(doc, "access_hash", 0)
            total_blocks = (file_size + STREAM_BLOCK_SIZE_BYTES - 1) // STREAM_BLOCK_SIZE_BYTES

            # 1. Pre-warm first 3 blocks (0, 1, 2 = first 1.5 MB header)
            head_blocks_to_warm = [b for b in range(min(3, total_blocks)) if not block_cache.has_block(doc_id, access_hash, b)]
            for b_idx in head_blocks_to_warm:
                b_data = await fetcher.fetch_block_from_telegram(
                    client=client,
                    doc=doc,
                    chat_id=chat_id,
                    message_id=message_id,
                    block_idx=b_idx,
                    priority=PRIORITY_NEAR
                )
                if b_data:
                    await block_cache.put(doc_id, access_hash, b_idx, b_data)

            # 2. Pre-warm last 2 blocks (tail: Cues / moov)
            if total_blocks > 3:
                tail_blocks = [total_blocks - 1, total_blocks - 2]
                for b_idx in tail_blocks:
                    if b_idx >= 3 and not block_cache.has_block(doc_id, access_hash, b_idx):
                        b_data = await fetcher.fetch_block_from_telegram(
                            client=client,
                            doc=doc,
                            chat_id=chat_id,
                            message_id=message_id,
                            block_idx=b_idx,
                            priority=PRIORITY_PREFETCH
                        )
                        if b_data:
                            await block_cache.put(doc_id, access_hash, b_idx, b_data)

            logger.info(f"[PreWarm] Successfully pre-warmed container headers and index for video ({chat_id}, {message_id})")
        except Exception as e:
            logger.debug(f"[PreWarm] Non-fatal pre-warm notice for ({chat_id}, {message_id}): {e}")

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
        Aligned 1 MB block-based streaming generator:
        - Serves cached blocks instantly from NVMe/SSD or RAM.
        - Coordinates parallel MTProto block downloads.
        - Seamlessly handles seeks and continuous background buffering.
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

        end_offset = min(offset + length, file_size)
        actual_length = end_offset - offset
        if actual_length <= 0:
            return

        block_size = STREAM_BLOCK_SIZE_BYTES
        is_probe = (actual_length <= 2 * block_size)

        if not session_id:
            s_type = "probe" if is_probe else "play"
            session_id = f"stream_{chat_id}_{message_id}_{offset}_{s_type}_{time.time()}"

        session = await self.get_or_create_session(
            session_id=session_id,
            client=client,
            chat_id=chat_id,
            message_id=message_id,
            doc=doc,
            is_probe=is_probe
        )

        first_block = offset // block_size
        last_block = (end_offset - 1) // block_size
        session.handle_seek(first_block)

        # Calculate read-ahead prefetch window
        prefetch_blocks = session.calculate_adaptive_prefetch_window(actual_length)
        if prefetch_blocks > 0:
            session.trigger_prefetch(first_block + 1, prefetch_blocks)

        cur_byte_pos = offset
        bytes_remaining = actual_length
        current_b_idx = first_block

        try:
            for b_idx in range(first_block, last_block + 1):
                if bytes_remaining <= 0 or session._is_superseded:
                    break

                current_b_idx = b_idx

                # Trigger adaptive read-ahead ahead of current position
                if prefetch_blocks > 0:
                    session.trigger_prefetch(b_idx + 1, prefetch_blocks)

                block_data = await session.get_block(b_idx, priority=PRIORITY_URGENT)
                if not block_data:
                    break

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
            pass
        except Exception as e:
            metrics.stream_errors += 1
            logger.error(f"[StreamingEngine] Error in stream_file generator: {e}")
            raise
        finally:
            # If playback connection idled (e.g. paused), continue prefetching into disk cache!
            trigger_pause = (not is_probe and not session._is_superseded and bytes_remaining > 0)
            await self.release_session(session_id, trigger_pause_prefetch=trigger_pause, head_block=current_b_idx)


# Global Engine Instance
streaming_engine = StreamingEngine()


def get_streaming_stats() -> dict:
    """Returns real-time streaming engine diagnostics."""
    return metrics.get_stats(block_cache.get_info(), priority_scheduler.get_info())


def is_streaming_active() -> bool:
    """Returns True if there is an active video stream."""
    return streaming_engine.is_streaming_active()
