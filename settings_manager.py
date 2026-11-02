import os
import json
from config import DATA_DIR

SETTINGS_FILE = os.path.join(DATA_DIR, "settings.json")

DEFAULT_SETTINGS = {
    "api_id": None,
    "api_hash": "",
    "bot_token": "",
    "tmdb_api_key": "",
    "channel_ids": [],
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

    # Determine if addon is fully configured
    is_conf = bool(
        settings.get("api_id")
        and settings.get("api_hash")
        and (settings.get("bot_token") or settings.get("session_string"))
        and settings.get("channel_ids")
    )
    settings["configured"] = is_conf
    return settings

def save_settings(new_settings: dict) -> dict:
    os.makedirs(DATA_DIR, exist_ok=True)
    current = load_settings()
    current.update(new_settings)
    
    # Check if configured
    is_conf = bool(
        current.get("api_id")
        and current.get("api_hash")
        and (current.get("bot_token") or current.get("session_string"))
        and current.get("channel_ids")
    )
    current["configured"] = is_conf

    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(current, f, indent=2)

    return current

def is_configured() -> bool:
    s = load_settings()
    return bool(s.get("configured", False))
