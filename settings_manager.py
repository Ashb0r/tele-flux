import os
import json
from config import DATA_DIR

SETTINGS_FILE = os.path.join(DATA_DIR, "settings.json")

DEFAULT_SETTINGS = {
    "api_id": None,
    "api_hash": "",
    "bot_token": "",
    "tmdb_api_key": "",
    "stashdb_api_key": "",          # Optional StashDB API Key for adult scene identification
    "channel_ids": [],              # Streaming channels (strictly for Stremio)
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
    env_stashdb = os.getenv("STASHDB_API_KEY", "")

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
    if env_stashdb:
        settings["stashdb_api_key"] = env_stashdb.strip()
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

    # Ensure channel_ids is always a list of ints
    if isinstance(settings.get("channel_ids"), list):
        settings["channel_ids"] = [
            int(x) for x in settings["channel_ids"] if str(x).lstrip("-").isdigit()
        ]
    else:
        settings["channel_ids"] = []

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
    for k, v in new_settings.items():
        if v is not None:
            current[k] = v

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
