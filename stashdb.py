import os
import re
import html
import certifi
import httpx
import urllib3
from config import DATA_DIR, PUBLIC_URL

STASHDB_GRAPHQL_URL = os.getenv("STASHDB_GRAPHQL_URL", "https://stashdb.org/graphql")

# User agent for StashDB queries
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) TelegramStremio/1.0"

# In-memory scene cache to avoid duplicate remote queries during scanning
_SCENE_CACHE = {}


def select_media_images(images: list) -> tuple[str, str]:
    """
    Intelligently select high-resolution poster (portrait) and widescreen backdrop (landscape)
    from StashDB image assets.
    """
    if not images:
        default_img = "https://stashdb.org/favicon.ico"
        return default_img, default_img

    def get_res(img):
        w = img.get("width") or 0
        h = img.get("height") or 0
        return w * h

    sorted_by_res = sorted(images, key=get_res, reverse=True)

    # Prefer landscape (width >= height) for backdrops
    landscape = [img for img in sorted_by_res if (img.get("width") or 0) >= (img.get("height") or 0)]
    # Prefer portrait (height > width) for posters
    portrait = [img for img in sorted_by_res if (img.get("height") or 0) > (img.get("width") or 0)]

    background = landscape[0]["url"] if landscape else sorted_by_res[0]["url"]
    poster = portrait[0]["url"] if portrait else sorted_by_res[0]["url"]

    return poster, background


async def execute_graphql(query: str, variables: dict | None = None, api_key: str | None = None, timeout: float = 15.0):
    """
    Executes an outbound GraphQL query to StashDB with SSL resilience:
    Attempts first with certifi CA bundle, falls back to verify=False on SSLError.
    """
    headers = {
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT
    }
    if api_key:
        headers["ApiKey"] = api_key.strip()

    payload = {"query": query}
    if variables is not None:
        payload["variables"] = variables

    # Attempt 1: Standard verified SSL with certifi
    try:
        async with httpx.AsyncClient(verify=certifi.where(), timeout=timeout) as client:
            resp = await client.post(STASHDB_GRAPHQL_URL, json=payload, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                if "errors" in data:
                    print(f"[StashDB GraphQL Error] {data['errors']}")
                return data, resp.status_code
            return None, resp.status_code
    except httpx.ConnectError:
        pass
    except Exception as e:
        print(f"[StashDB Notice] Primary SSL connection attempt note: {e}. Retrying with verify=False...")

    # Attempt 2: Resilient fallback
    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        async with httpx.AsyncClient(verify=False, timeout=timeout) as client:
            resp = await client.post(STASHDB_GRAPHQL_URL, json=payload, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                return data, resp.status_code
            return None, resp.status_code
    except Exception as fallback_err:
        print(f"[StashDB Request Failed] {fallback_err}")
        return None, 0


async def verify_stashdb_key(api_key: str) -> dict:
    """
    Verifies that the provided StashDB API key is valid and authorized.
    """
    key = (api_key or "").strip()
    if not key:
        return {"valid": False, "message": "StashDB API Key cannot be empty."}

    query = """
    query VerifyKey {
      queryStudios(input: { page: 1, per_page: 1 }) {
        count
      }
    }
    """
    data, status_code = await execute_graphql(query, api_key=key, timeout=10.0)
    if data and "data" in data and "queryStudios" in data["data"]:
        return {
            "valid": True,
            "message": "StashDB API Key verified successfully! Connection established."
        }
    elif status_code == 401 or status_code == 403:
        return {
            "valid": False,
            "message": "Invalid StashDB API Key or unauthorized access."
        }
    else:
        return {
            "valid": False,
            "message": f"Could not verify StashDB key (HTTP status {status_code}). Verify your key and internet connection."
        }


def clean_stash_filename(file_name: str) -> str:
    """
    Cleans a video file name to extract the exact real scene title as named in StashDB:
    - Strips container file extensions (.mp4, .mkv, etc.)
    - Extracts date if present (e.g. 2018-11-06 or 2018)
    - Strips parenthesized date and brackets
    - Removes common quality tokens (1080p, 720p, 2160p, 4k, HEVC, AAC, x264, x265)
    - Normalizes dots and underscores into spaces
    """
    # Remove file extension
    base = re.sub(r'(?i)\.(mkv|mp4|avi|mov|webm|ts|m2ts|m4v|wmv|flv)$', '', file_name.strip())

    # Strip parenthesized date and brackets
    clean = re.sub(r'\(\d{4}(?:-\d{2}-\d{2})?\)', '', base)
    clean = re.sub(r'\[[^\]]*\]', '', clean)

    # Replace underscores and dots with spaces
    clean = clean.replace('_', ' ').replace('.', ' ')

    # Strip resolution, codec, and quality suffixes if appended
    clean = re.sub(r'(?i)\b(2160p|4k|1080p|720p|480p|360p|uhd|fhd|hd|hevc|x265|x264|aac|ac3|dvdrip|web-dl|webrip)\b', '', clean)

    # Clean double spaces
    clean = re.sub(r'\s+', ' ', clean).strip()

    return clean if clean else base


def extract_filename_date(file_name: str) -> str | None:
    """Extracts date YYYY-MM-DD or YYYY if present in filename parentheses."""
    match = re.search(r'\((\d{4}(?:-\d{2}-\d{2})?)\)', file_name)
    return match.group(1) if match else None


async def search_stashdb_scene(file_name: str, caption: str = "", api_key: str | None = None) -> dict | None:
    """
    Searches StashDB using GraphQL for a scene matching the exact video file name.
    """
    from settings_manager import load_settings
    if not api_key:
        api_key = load_settings().get("stashdb_api_key") or os.getenv("STASHDB_API_KEY", "")

    cleaned_title = clean_stash_filename(file_name)
    date_hint = extract_filename_date(file_name)
    cache_key = f"{cleaned_title.lower()}_{date_hint or ''}"

    if cache_key in _SCENE_CACHE:
        return _SCENE_CACHE[cache_key]

    # Attempt 1: Check running local StashDB catalog addon (port 7000)
    for host in ("host.docker.internal", "localhost", "127.0.0.1", "stash_catalog_addon"):
        try:
            from urllib.parse import quote
            catalog_search_url = f"http://{host}:7000/catalog/porn/catalog_realwifestories/search={quote(cleaned_title)}.json"
            async with httpx.AsyncClient(timeout=3.0) as http:
                c_resp = await http.get(catalog_search_url)
                if c_resp.status_code == 200:
                    metas = c_resp.json().get("metas", [])
                    if metas:
                        top = metas[0]
                        res_scene = {
                            "stash_id": top["id"],
                            "media_type": "porn",
                            "title": top.get("name") or cleaned_title,
                            "year": int(top["releaseInfo"]) if top.get("releaseInfo", "").isdigit() else None,
                            "poster": top.get("poster"),
                            "backdrop": top.get("background") or top.get("poster"),
                            "overview": top.get("description") or f"Adult Scene: {top.get('name')}",
                            "genres": top.get("genres", ["Adult"]),
                            "rating": 0.0,
                            "studio": top.get("genres", ["Studio"])[0] if top.get("genres") else "Studio"
                        }
                        _SCENE_CACHE[cache_key] = res_scene
                        return res_scene
        except Exception:
            pass

    graphql_query = """
    query SearchScene($input: SceneQueryInput!) {
      queryScenes(input: $input) {
        count
        scenes {
          id
          title
          details
          date
          duration
          director
          code
          images {
            id
            url
            width
            height
          }
          studio {
            id
            name
            parent {
              id
              name
            }
          }
          performers {
            as
            performer {
              id
              name
              gender
              images {
                url
              }
            }
          }
          tags {
            id
            name
          }
        }
      }
    }
    """

    # Generate search queries:
    # 1. Exact cleaned title
    # 2. If title has a dash "Studio - Scene Title", try both parts
    search_candidates = [cleaned_title]
    if " - " in cleaned_title:
        parts = cleaned_title.split(" - ")
        if len(parts) >= 2:
            search_candidates.append(parts[-1].strip())  # Scene title part
            search_candidates.append(parts[0].strip())   # Studio part
            search_candidates.append(" ".join(parts).strip())

    found_scene = None

    for search_text in search_candidates:
        if not search_text or len(search_text) < 2:
            continue

        variables = {
            "input": {
                "text": search_text,
                "page": 1,
                "per_page": 15,
                "sort": "DATE",
                "direction": "DESC"
            }
        }

        data, status = await execute_graphql(graphql_query, variables=variables, api_key=api_key, timeout=12.0)
        if not data or "data" not in data:
            continue

        scenes = data.get("data", {}).get("queryScenes", {}).get("scenes", [])
        if not scenes:
            continue

        # Look for exact date match first if date hint is present
        if date_hint:
            for s in scenes:
                if s.get("date") == date_hint:
                    found_scene = s
                    break

        # Look for exact title match
        if not found_scene:
            target_lower = cleaned_title.lower()
            for s in scenes:
                s_title = (s.get("title") or "").strip().lower()
                if s_title and (s_title == target_lower or s_title in target_lower or target_lower in s_title):
                    found_scene = s
                    break

        # If not matched directly, check if performers/studio match
        if not found_scene:
            for s in scenes:
                studio_name = (s.get("studio") or {}).get("name", "").lower()
                performer_names = [p["performer"]["name"].lower() for p in s.get("performers", []) if p.get("performer", {}).get("name")]
                if studio_name and studio_name in target_lower:
                    found_scene = s
                    break
                if any(p in target_lower for p in performer_names):
                    found_scene = s
                    break

        # If still not chosen, use the top search result
        if not found_scene and scenes:
            found_scene = scenes[0]

        if found_scene:
            break

    if not found_scene:
        return None

    # Parse scene details
    scene_id = found_scene["id"]
    poster_url, bg_url = select_media_images(found_scene.get("images", []))

    performers = [p["performer"]["name"] for p in found_scene.get("performers", []) if p.get("performer", {}).get("name")]
    performer_str = ", ".join(performers) if performers else ""

    studio_obj = found_scene.get("studio") or {}
    studio_name = studio_obj.get("name") or "Studio"
    parent_studio = studio_obj.get("parent", {}).get("name") if studio_obj.get("parent") else None
    studio_full = f"{parent_studio} - {studio_name}" if parent_studio and parent_studio != studio_name else studio_name

    tag_names = [t["name"] for t in found_scene.get("tags", []) if t.get("name")]
    genres = [studio_name] + [t for t in tag_names if t.lower() != studio_name.lower()]

    date_str = found_scene.get("date") or ""
    year = int(date_str[:4]) if len(date_str) >= 4 and date_str[:4].isdigit() else None

    synopsis = (found_scene.get("details") or "").strip()
    desc_lines = []
    if synopsis:
        desc_lines.append(synopsis)
    if performer_str:
        desc_lines.append(f"Starring: {performer_str}")
    desc_lines.append(f"Studio: {studio_full}")
    if date_str:
        desc_lines.append(f"Release Date: {date_str}")
    if tag_names:
        desc_lines.append(f"Tags: {', '.join(tag_names[:6])}")
    overview = "\n\n".join(desc_lines)

    result = {
        "stash_id": f"stash_{scene_id}",
        "raw_id": scene_id,
        "media_type": "porn",
        "title": found_scene.get("title") or cleaned_title,
        "year": year,
        "date": date_str,
        "duration": found_scene.get("duration"),
        "poster": poster_url,
        "backdrop": bg_url,
        "overview": overview,
        "rating": 0.0,
        "genres": genres,
        "performers": performers,
        "studio": studio_full,
        "director": found_scene.get("director")
    }

    _SCENE_CACHE[cache_key] = result
    return result


async def get_stashdb_scene_by_id(stash_id: str, api_key: str | None = None) -> dict | None:
    """
    Fetches full scene metadata directly by StashDB UUID.
    """
    raw_id = stash_id.replace("stash_", "").strip()
    from settings_manager import load_settings
    if not api_key:
        api_key = load_settings().get("stashdb_api_key") or os.getenv("STASHDB_API_KEY", "")

    graphql_query = """
    query FindScene($id: ID!) {
      findScene(id: $id) {
        id
        title
        details
        date
        duration
        director
        code
        images {
          id
          url
          width
          height
        }
        studio {
          id
          name
          parent {
            id
            name
          }
        }
        performers {
          as
          performer {
            id
            name
            gender
            images {
              url
            }
          }
        }
        tags {
          id
          name
        }
      }
    }
    """

    data, status = await execute_graphql(graphql_query, variables={"id": raw_id}, api_key=api_key, timeout=12.0)
    if not data or "data" not in data:
        return None

    scene = data.get("data", {}).get("findScene")
    if not scene:
        return None

    poster_url, bg_url = select_media_images(scene.get("images", []))
    performers = [p["performer"]["name"] for p in scene.get("performers", []) if p.get("performer", {}).get("name")]
    performer_str = ", ".join(performers) if performers else ""

    studio_obj = scene.get("studio") or {}
    studio_name = studio_obj.get("name") or "Studio"
    parent_studio = studio_obj.get("parent", {}).get("name") if studio_obj.get("parent") else None
    studio_full = f"{parent_studio} - {studio_name}" if parent_studio and parent_studio != studio_name else studio_name

    tag_names = [t["name"] for t in scene.get("tags", []) if t.get("name")]
    genres = [studio_name] + [t for t in tag_names if t.lower() != studio_name.lower()]

    date_str = scene.get("date") or ""
    year = int(date_str[:4]) if len(date_str) >= 4 and date_str[:4].isdigit() else None

    synopsis = (scene.get("details") or "").strip()
    desc_lines = []
    if synopsis:
        desc_lines.append(synopsis)
    if performer_str:
        desc_lines.append(f"Starring: {performer_str}")
    desc_lines.append(f"Studio: {studio_full}")
    if date_str:
        desc_lines.append(f"Release Date: {date_str}")
    if tag_names:
        desc_lines.append(f"Tags: {', '.join(tag_names[:6])}")
    overview = "\n\n".join(desc_lines)

    return {
        "stash_id": f"stash_{scene['id']}",
        "raw_id": scene["id"],
        "media_type": "porn",
        "title": scene.get("title") or "Adult Scene",
        "year": year,
        "date": date_str,
        "duration": scene.get("duration"),
        "poster": poster_url,
        "backdrop": bg_url,
        "overview": overview,
        "rating": 0.0,
        "genres": genres,
        "performers": performers,
        "studio": studio_full,
        "director": scene.get("director")
    }


def format_stashdb_stream(header: str, specs: dict) -> tuple[str, str]:
    """
    Constructs ultra-informative Stremio stream card badges and descriptions.
    """
    res = specs.get("resolution") or "1080p"
    codec = specs.get("codec") or ""
    badge_core = f"{res} {codec}".strip()
    badge = f"[StashDB ⚡] {badge_core}" if badge_core else "[StashDB ⚡] HD"

    lines = [f"🔞 {header}"]

    size_str = specs.get("size_str")
    if size_str:
        lines.append(f"📦 {size_str}")

    audio = specs.get("audio")
    if audio:
        lines.append(f"🔊 {audio}")

    desc = "\n".join(lines)
    return badge, desc


def generate_adult_poster_svg(title: str) -> bytes:
    """
    Generates a dark-themed SVG poster card for adult / StashDB media
    ensuring no poster is ever blank in Stremio.
    """
    clean_title = html.escape(title[:45])
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="600" height="900" viewBox="0 0 600 900">
  <defs>
    <linearGradient id="bg" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#1a0b1e"/>
      <stop offset="50%" stop-color="#0f0714"/>
      <stop offset="100%" stop-color="#050208"/>
    </linearGradient>
    <linearGradient id="accent" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#ec4899"/>
      <stop offset="100%" stop-color="#8b5cf6"/>
    </linearGradient>
  </defs>
  <rect width="600" height="900" fill="url(#bg)"/>
  <rect x="20" y="20" width="560" height="860" rx="16" fill="none" stroke="#2d1538" stroke-width="2"/>
  <g transform="translate(300, 360)">
    <circle r="70" fill="url(#accent)" opacity="0.15"/>
    <circle r="50" fill="url(#accent)" opacity="0.3"/>
    <text text-anchor="middle" dy="18" font-family="-apple-system, system-ui, sans-serif" font-size="48">🔞</text>
  </g>
  <g transform="translate(300, 520)">
    <text text-anchor="middle" fill="#f43f5e" font-family="-apple-system, system-ui, sans-serif" font-size="16" font-weight="800" letter-spacing="3">STASHDB VAULT</text>
    <text text-anchor="middle" dy="45" fill="#f8fafc" font-family="-apple-system, system-ui, sans-serif" font-size="24" font-weight="700">{clean_title}</text>
  </g>
</svg>"""
    return svg.encode("utf-8")
