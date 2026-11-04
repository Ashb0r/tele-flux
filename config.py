import os
from dotenv import load_dotenv

# Load .env file if present
load_dotenv()

# Data directory for persistent storage (SQLite, settings.json, bot session)
DATA_DIR = os.getenv("DATA_DIR")
if not DATA_DIR:
    # Default to a "data" subdirectory inside the project
    DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

os.makedirs(DATA_DIR, exist_ok=True)

# Server Network Settings
SERVER_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
try:
    SERVER_PORT = int(os.getenv("SERVER_PORT", "7000"))
except ValueError:
    SERVER_PORT = 7000

PUBLIC_URL = os.getenv("PUBLIC_URL", f"http://127.0.0.1:{SERVER_PORT}").rstrip("/")

# Streaming chunk size (512 KB is the optimal MTProto request size for instant startup and rapid seeks)
try:
    CHUNK_SIZE_KB = int(os.getenv("CHUNK_SIZE_KB", "512"))
    CHUNK_SIZE_BYTES = CHUNK_SIZE_KB * 1024
except ValueError:
    CHUNK_SIZE_BYTES = 512 * 1024

# Media directory for downloader storage (Movies, TV Shows, Unsorted, and .downloads)
MEDIA_DIR = os.getenv("MEDIA_DIR")
if not MEDIA_DIR:
    MEDIA_DIR = os.path.join(DATA_DIR, "media")
os.makedirs(MEDIA_DIR, exist_ok=True)

# Downloader chunk size: 512 KB is optimal for multi-connection MTProto downloads
try:
    DOWNLOAD_CHUNK_SIZE_KB = int(os.getenv("DOWNLOAD_CHUNK_SIZE_KB", "512"))
    DOWNLOAD_CHUNK_SIZE_BYTES = DOWNLOAD_CHUNK_SIZE_KB * 1024
except ValueError:
    DOWNLOAD_CHUNK_SIZE_BYTES = 512 * 1024

# Parallel MTProto connections for maximum download speed (FastTelethon multi-connection acceleration)
try:
    DOWNLOAD_PARALLEL_CONNECTIONS = int(os.getenv("DOWNLOAD_PARALLEL_CONNECTIONS", "6"))
except ValueError:
    DOWNLOAD_PARALLEL_CONNECTIONS = 6
