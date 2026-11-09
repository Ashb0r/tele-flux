import os
from dotenv import load_dotenv

# Load .env file if present
load_dotenv()

# Data directory for persistent storage (SQLite, settings.json, bot session)
DATA_DIR = os.getenv("DATA_DIR")
if not DATA_DIR:
    DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

os.makedirs(DATA_DIR, exist_ok=True)

# Server Network Settings
SERVER_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
try:
    SERVER_PORT = int(os.getenv("SERVER_PORT", "7000"))
except ValueError:
    SERVER_PORT = 7000

PUBLIC_URL = os.getenv("PUBLIC_URL", f"http://127.0.0.1:{SERVER_PORT}").rstrip("/")

# Mode Toggle: If True, operates in Adult / StashDB mode; If False, Movies & TV Series (TMDB)
ADULT_ONLY = os.getenv("ADULT_ONLY", "False").strip().lower() in ("true", "1", "yes")

# StashDB configuration
STASHDB_API_KEY = os.getenv("STASHDB_API_KEY", "")
STASHDB_GRAPHQL_URL = os.getenv("STASHDB_GRAPHQL_URL", "https://stashdb.org/graphql")

# High-Performance Streaming Engine Configuration
# 512 KB (524,288 bytes) is the EXACT maximum wire limit for Telegram MTProto upload.getFile RPCs.
# Matching this quantum ensures Telethon uses direct zero-buffer direct downloads with zero chunk truncation.
STREAM_BLOCK_SIZE_KB = 512
STREAM_BLOCK_SIZE_BYTES = STREAM_BLOCK_SIZE_KB * 1024
CHUNK_SIZE_KB = STREAM_BLOCK_SIZE_KB
CHUNK_SIZE_BYTES = STREAM_BLOCK_SIZE_BYTES

# Concurrency & Scheduler Tuning
STREAM_CLIENT_POOL_SIZE = int(os.getenv("STREAM_CLIENT_POOL_SIZE", "4"))
STREAM_PARALLEL_CONNECTIONS = 8
STREAM_GLOBAL_MAX_REQUESTS = 32

# Multi-Tiered Cache: Fast RAM (L1) + Persistent Disk (L2)
STREAM_CACHE_MAX_MB = 256
STREAM_DISK_CACHE_ENABLED = True
STREAM_DISK_CACHE_DIR = os.getenv("STREAM_DISK_CACHE_DIR", os.path.join(DATA_DIR, "stream_cache"))
STREAM_DISK_CACHE_MAX_GB = int(os.getenv("STREAM_DISK_CACHE_MAX_GB", "20"))

# Dynamic Read-Ahead & Idle Buffer Pipeline
STREAM_PREFETCH_MIN_MB = 8
STREAM_PREFETCH_MAX_MB = 32
STREAM_PAUSE_PREFETCH_MAX_MB = int(os.getenv("STREAM_PAUSE_PREFETCH_MAX_MB", "512"))  # Up to 1 GB idle prefetch
STREAM_MAX_ACTIVE_STREAMS = 8
