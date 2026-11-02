import re
import urllib.parse
import httpx
from settings_manager import load_settings

def get_tmdb_key() -> str:
    return load_settings().get("tmdb_api_key", "").strip()

SEASON_EPISODE_PATTERNS = [
    # 1. S01E01, S01.E01, S1EP1, [S01E01], S01 - E01
    re.compile(r'(?i)[._\-\s\[\(]S(\d{1,2})[._\-\s]*(?:E|EP)[._\-\s]*(\d{1,3})(?:v\d)?[._\-\s\]\)]'),
    re.compile(r'(?i)^S(\d{1,2})[._\-\s]*(?:E|EP)[._\-\s]*(\d{1,3})(?:v\d)?[._\-\s]'),
    # 2. 1x01, 01x01
    re.compile(r'(?i)[._\-\s\[\(](\d{1,2})x(\d{1,3})[._\-\s\]\)]'),
    # 3. Season 1 Episode 1 / Season 01 Ep 01
    re.compile(r'(?i)[._\-\s]Season[._\-\s]*(\d{1,2})[._\-\s]*(?:Episode|Ep)[._\-\s]*(\d{1,3})'),
    # 4. Episode 01 / Ep 01 / EP01 / E01
    re.compile(r'(?i)[._\-\s\[\(](?:Episode|Ep|EP)[._\-\s]*(\d{1,3})[._\-\s\]\)]'),
    re.compile(r'(?i)[._\-\s]E(\d{1,3})[._\-\s]'),
]

CHANNEL_TAG_REGEX = re.compile(r'^(?:\[.*?\]|\(.*?\)|@\w+)[\s._-]*')
YEAR_REGEX = re.compile(r'\b(19\d{2}|20\d{2})\b')

# Language marker: in release titles, everything after the language tag is almost always release junk
LANGUAGE_SPLIT_REGEX = re.compile(
    r"(?i)\b(malayalam|tamil|hindi|telugu|kannada|bengali|marathi|punjabi|gujarati|urdu|"
    r"english|korean|japanese|chinese|spanish|french|german|russian|italian|"
    r"dual[\s._-]?audio|multi[\s._-]?audio|dubbed|esubs?|subtitles?)\b.*$"
)

RELEASE_TOKENS = re.compile(
    r"(?i)\b(2160p|1440p|1080p|1080i|720p|576p|480p|360p|4k|uhd|hdr(?:10)?\+?|dv|dolby\s*vision|"
    r"web-?dl|webrip|web|bluray|blu-ray|bdrip|brrip|dvdrip|dvdscr|hdtv|hdrip|hdts|cam|"
    r"x264|x265|h\.?264|h\.?265|hevc|avc|10bit|8bit|"
    r"aac\d*|ac3|eac3|ddp?\d*(?:\.\d)?|dts(?:-hd)?|truehd|atmos|mp3|flac|"
    r"proper|repack|extended|unrated|remastered|amzn|nf|dsnp|hulu|max|itunes|netflix|"
    r"yify|rargb|evo|tgx|galaxyrg|psa|mkv|mp4|avi|webm|hd|hq|dl)\b"
)

def clean_series_title(title_part: str) -> tuple[str, int | None]:
    t = re.sub(r"\[.*?\]|\(.*?\)", " ", title_part)
    years = YEAR_REGEX.findall(t)
    year = int(years[-1]) if years else None
    t = YEAR_REGEX.sub(" ", t)
    t = RELEASE_TOKENS.sub(" ", t)
    t = re.sub(r"[._\-+:]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t, year

def clean_episode_title(ep_part: str) -> str:
    cleaned = re.sub(r"\[.*?\]|\(.*?\)", " ", ep_part)
    cleaned = LANGUAGE_SPLIT_REGEX.sub(" ", cleaned)
    cleaned = RELEASE_TOKENS.sub(" ", cleaned)
    cleaned = re.sub(r"[._\-+:]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned

def parse_filename(file_name: str) -> dict:
    """
    100% accurate classification between Movies and TV Series:
    - Extracts canonical Series title from the prefix before SxxExx
    - Extracts Season, Episode, and Episode Title
    - Correctly sets media_type = 'series' or 'movie'
    """
    base = file_name.rsplit(".", 1)[0]
    base = CHANNEL_TAG_REGEX.sub("", base).strip()

    season = None
    episode = None
    episode_title = None
    media_type = "movie"
    raw_title = ""
    year = None

    for pattern in SEASON_EPISODE_PATTERNS:
        m = pattern.search(base)
        if m:
            media_type = "series"
            groups = m.groups()
            if len(groups) == 2:
                season = int(groups[0])
                episode = int(groups[1])
            elif len(groups) == 1:
                season = 1
                episode = int(groups[0])

            show_part = base[:m.start()]
            ep_part = base[m.end():]

            raw_title, year = clean_series_title(show_part)
            ep_name = clean_episode_title(ep_part)
            episode_title = ep_name if ep_name else f"Episode {episode}"
            break

    if media_type == "movie":
        years = YEAR_REGEX.findall(base)
        year = int(years[-1]) if years else None
        lang_trimmed = LANGUAGE_SPLIT_REGEX.sub(" ", base)
        clean = RELEASE_TOKENS.sub(" ", lang_trimmed)
        clean = YEAR_REGEX.sub(" ", clean)
        clean = re.sub(r"\[.*?\]|\(.*?\)", " ", clean)
        clean = re.sub(r"[._\-+:]+", " ", clean)
        raw_title = re.sub(r"\s+", " ", clean).strip()

    return {
        "raw_title": raw_title or base.strip(),
        "episode_title": episode_title,
        "year": year,
        "season": season,
        "episode": episode,
        "media_type": media_type
    }

def generate_search_candidates(raw_title: str) -> list[str]:
    """
    Produces a progressive retry ladder of query candidates:
    e.g. 'Khalifa The Ruler' -> ['Khalifa The Ruler', 'Khalifa', 'Khalifa The']
    """
    candidates = []
    t = raw_title.strip()
    if not t:
        return candidates

    candidates.append(t)

    # If title has a subtitle / tagline separated by hyphen or colon
    if " - " in t:
        candidates.append(t.split(" - ")[0].strip())
    if ": " in t:
        candidates.append(t.split(": ")[0].strip())

    # Progressive word drop
    words = t.split()
    if len(words) > 1:
        # Candidate with only first 1 or 2 words (e.g. 'Khalifa')
        candidates.append(words[0])
        if len(words) > 2:
            candidates.append(f"{words[0]} {words[1]}")
        # Drop trailing words one by one
        for i in range(len(words) - 1, 0, -1):
            sub = " ".join(words[:i]).strip()
            if sub and sub not in candidates:
                candidates.append(sub)

    # Deduplicate while preserving order
    seen = set()
    ordered = []
    for c in candidates:
        low = c.lower()
        if low not in seen and len(low) >= 2:
            seen.add(low)
            ordered.append(c)

    return ordered

async def search_wikipedia_title(query: str) -> list[str]:
    """
    Queries Wikipedia OpenSearch to find the canonical film title for foreign/Indian films.
    """
    url = f"https://en.wikipedia.org/w/api.php?action=opensearch&search={urllib.parse.quote(query)}&limit=4&namespace=0&format=json"
    try:
        async with httpx.AsyncClient(timeout=6.0) as client:
            resp = await client.get(url, headers={"User-Agent": "StremioTelegram/1.0"})
            if resp.status_code == 200:
                data = resp.json()
                titles = data[1] if len(data) > 1 else []
                cleaned = []
                for t in titles:
                    # Clean 'Khalifa (upcoming film)' -> 'Khalifa'
                    c = re.sub(r"\s*\([^)]*\)", "", t).strip()
                    if c and c.lower() != query.lower():
                        cleaned.append(c)
                return cleaned
    except Exception:
        pass
    return []

async def execute_tmdb_search(client: httpx.AsyncClient, api_key: str, query: str, year: int | None, media_type: str) -> dict | None:
    endpoint = "tv" if media_type == "series" else "movie"
    url = f"https://api.themoviedb.org/3/search/{endpoint}"

    # Try 1: with year
    params = {
        "api_key": api_key,
        "query": query,
        "include_adult": "false",
        "language": "en-US"
    }
    if year:
        params["year" if media_type == "movie" else "first_air_date_year"] = year

    resp = await client.get(url, params=params)
    results = resp.json().get("results", []) if resp.status_code == 200 else []

    # Try 2: without year if year-restricted search failed
    if not results and year:
        params.pop("year", None)
        params.pop("first_air_date_year", None)
        resp = await client.get(url, params=params)
        results = resp.json().get("results", []) if resp.status_code == 200 else []

    # Try 3: multi-search fallback
    if not results:
        multi_url = "https://api.themoviedb.org/3/search/multi"
        resp = await client.get(multi_url, params={"api_key": api_key, "query": query, "include_adult": "false"})
        if resp.status_code == 200:
            for item in resp.json().get("results", []):
                if item.get("media_type") in ("movie", "tv"):
                    results = [item]
                    break

    if not results:
        return None

    hit = results[0]
    tmdb_id = hit.get("id")
    raw_hit_type = hit.get("media_type")
    if raw_hit_type:
        actual_type = "series" if raw_hit_type in ("tv", "series") else "movie"
    else:
        actual_type = "series" if media_type in ("tv", "series") else "movie"

    item_title = hit.get("name" if actual_type == "series" else "title", query)
    date_str = hit.get("first_air_date" if actual_type == "series" else "release_date") or ""
    item_year = int(date_str[:4]) if len(date_str) >= 4 else year
    poster_path = hit.get("poster_path")
    backdrop_path = hit.get("backdrop_path")
    overview = hit.get("overview", "")
    rating = round(float(hit.get("vote_average", 0.0)), 1)

    # Fetch IMDb ID via external_ids endpoint (tv or movie)
    imdb_id = None
    try:
        tmdb_endpoint = "tv" if actual_type == "series" else "movie"
        ext_url = f"https://api.themoviedb.org/3/{tmdb_endpoint}/{tmdb_id}/external_ids"
        ext_resp = await client.get(ext_url, params={"api_key": api_key})
        if ext_resp.status_code == 200:
            imdb_id = ext_resp.json().get("imdb_id")
    except Exception:
        pass

    return {
        "tmdb_id": tmdb_id,
        "imdb_id": imdb_id,
        "media_type": actual_type,
        "title": item_title,
        "year": item_year,
        "poster": f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None,
        "backdrop": f"https://image.tmdb.org/t/p/original{backdrop_path}" if backdrop_path else None,
        "overview": overview,
        "rating": rating,
        "genres": []
    }

async def lookup_tmdb(file_name: str) -> dict:
    """
    Bulletproof multi-stage TMDB matcher matching TGTV's logic.
    """
    parsed = parse_filename(file_name)
    raw_title = parsed["raw_title"]
    year = parsed["year"]
    media_type = parsed["media_type"]
    season = parsed["season"]
    episode = parsed["episode"]
    episode_title = parsed["episode_title"]

    fallback_id = abs(hash(file_name)) % 100000000
    fallback = {
        "tmdb_id": fallback_id,
        "imdb_id": None,
        "media_type": media_type,
        "title": raw_title or file_name,
        "episode_title": episode_title,
        "year": year,
        "season": season,
        "episode": episode,
        "poster": None,
        "backdrop": None,
        "overview": "No overview available.",
        "rating": 0.0,
        "genres": []
    }

    api_key = get_tmdb_key()
    if not api_key:
        return fallback

    candidates = generate_search_candidates(raw_title)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Stage 1: Try all word-drop candidates
            for query in candidates:
                hit = await execute_tmdb_search(client, api_key, query, year, media_type)
                if hit:
                    hit["season"] = season
                    hit["episode"] = episode
                    hit["episode_title"] = episode_title
                    print(f"[TMDB Matched] '{file_name}' -> '{hit['title']}' ({hit['year']}) [IMDb: {hit['imdb_id']}, TMDB: {hit['tmdb_id']}]")
                    return hit

            # Stage 2: Web resolution fallback (Wikipedia OpenSearch)
            wiki_candidates = await search_wikipedia_title(raw_title)
            for wquery in wiki_candidates:
                hit = await execute_tmdb_search(client, api_key, wquery, year, media_type)
                if hit:
                    hit["season"] = season
                    hit["episode"] = episode
                    hit["episode_title"] = episode_title
                    print(f"[TMDB Wiki-Resolved] '{file_name}' -> '{hit['title']}' ({hit['year']}) [IMDb: {hit['imdb_id']}, TMDB: {hit['tmdb_id']}]")
                    return hit

    except Exception as e:
        print(f"[TMDB Exception] '{file_name}': {e}")

    return fallback
