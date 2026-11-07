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

# High-Performance Streaming Engine Configuration (hardcoded for optimal buffer-free streaming)
STREAM_BLOCK_SIZE_KB = 512
STREAM_BLOCK_SIZE_BYTES = STREAM_BLOCK_SIZE_KB * 1024
CHUNK_SIZE_KB = STREAM_BLOCK_SIZE_KB
CHUNK_SIZE_BYTES = STREAM_BLOCK_SIZE_BYTES

STREAM_PARALLEL_CONNECTIONS = 4
STREAM_GLOBAL_MAX_REQUESTS = 8
STREAM_CACHE_MAX_MB = 128
STREAM_PREFETCH_MIN_MB = 6
STREAM_PREFETCH_MAX_MB = 16
STREAM_MAX_ACTIVE_STREAMS = 4
