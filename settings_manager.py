import os
import json
from config import DATA_DIR, MEDIA_DIR

SETTINGS_FILE = os.path.join(DATA_DIR, "settings.json")

DEFAULT_SETTINGS = {
    "api_id": None,
    "api_hash": "",
    "bot_token": "",
    "tmdb_api_key": "",
    "channel_ids": [],              # Streaming channels (strictly for Stremio)
    "download_channel_ids": [],     # Download channels (strictly for local archive, mutually exclusive)
    "downloader_enabled": False,    # OFF by default until user enables it in UI
    "media_dir": MEDIA_DIR,
    "min_free_disk_gb": 5.0,
    "max_concurrent_downloads": 1,
    "auto_tmdb_identify": True,
    "auto_organize": True,
    "max_retries": 5,
    "retry_backoff_base": 2,
    "retry_backoff_max_sec": 300,
    "startup_recovery": True,
    "history_reconciliation": True,
    "reconciliation_interval_min": 30,
    "notifications_enabled": True,
    "notification_mode": "reply",
    "notification_chat_id": None,
    "login_mode": "bot",
    "session_string": "",
    "configured": False
}

def load_settings() -> dict:
    # 1. Base defaults
    settings = DEFAULT_SETTINGS.copy()

    # 2. Pre-fill from .env if defined
    env_api_id = os.getenv("TELEGRAM_API_ID")
    env_api_hash = os.getenv("TELEGRAM_API_HASH")
    env_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    env_channels = os.getenv("CHANNEL_IDS", "")
    env_dl_channels = os.getenv("DOWNLOAD_CHANNEL_IDS", "")
    env_tmdb = os.getenv("TMDB_API_KEY", "")

    if env_api_id:
        try:
            settings["api_id"] = int(env_api_id.strip())
        except ValueError:
            pass
    if env_api_hash:
        settings["api_hash"] = env_api_hash.strip()
    if env_bot_token:
        settings["bot_token"] = env_bot_token.strip()
    if env_tmdb:
        settings["tmdb_api_key"] = env_tmdb.strip()
    if env_channels:
        clist = []
        for cid in env_channels.split(","):
            c = cid.strip()
            if c:
                try:
                    clist.append(int(c))
                except ValueError:
                    pass
        if clist:
            settings["channel_ids"] = clist

    if env_dl_channels:
        dl_list = []
        for cid in env_dl_channels.split(","):
            c = cid.strip()
            if c:
                try:
                    dl_list.append(int(c))
                except ValueError:
                    pass
        if dl_list:
            settings["download_channel_ids"] = dl_list

    # 3. Layer in saved settings from settings.json (UI selections & persistence)
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                for k, v in data.items():
                    if v is not None and v != "":
                        settings[k] = v
        except Exception as e:
            print(f"[Warning] Failed to read {SETTINGS_FILE}: {e}")

    # Ensure download_channel_ids is always a list of ints
    if isinstance(settings.get("download_channel_ids"), list):
        settings["download_channel_ids"] = [
            int(x) for x in settings["download_channel_ids"] if str(x).lstrip("-").isdigit()
        ]
    else:
        settings["download_channel_ids"] = []

    # Determine if configuration has valid credentials
    has_creds = bool(
        settings.get("api_id")
        and settings.get("api_hash")
        and (settings.get("bot_token") or settings.get("session_string"))
    )
    settings["configured"] = has_creds
    return settings

def save_settings(new_settings: dict) -> dict:
    os.makedirs(DATA_DIR, exist_ok=True)
    current = load_settings()
    current.update(new_settings)
    
    # Mutual exclusion: A streaming channel CANNOT be a download channel
    streaming_channels = set(current.get("channel_ids", []))
    download_channels = current.get("download_channel_ids", [])
    current["download_channel_ids"] = [cid for cid in download_channels if cid not in streaming_channels]

    # Check if credentials exist
    has_creds = bool(
        current.get("api_id")
        and current.get("api_hash")
        and (current.get("bot_token") or current.get("session_string"))
    )
    current["configured"] = has_creds

    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(current, f, indent=2)

    return current

def reset_settings() -> dict:
    os.makedirs(DATA_DIR, exist_ok=True)
    settings = DEFAULT_SETTINGS.copy()
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=2)
    return settings

def is_configured() -> bool:
    s = load_settings()
    return bool(s.get("configured", False))
