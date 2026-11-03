import asyncio
import re
import html
import urllib.parse
import httpx
from config import PUBLIC_URL
from settings_manager import load_settings

def get_tmdb_key() -> str:
    return load_settings().get("tmdb_api_key", "").strip()

# Refined regex patterns strictly matching series and episodes
# Prevents false positive matching of movie sequels (e.g. 'Pushpa - 2', 'Drishyam - 2', 'KGF - 2')
SEASON_EPISODE_PATTERNS = [
    # 1. Standard S01E01, S1E1, S01.E01, S01 - E01, S01E01-E02, S01EP01, S01_E01, [S01E01]
    re.compile(r'(?i)(?:[._\-\s\[\(]|^)S(\d{1,2})[._\-\s]*(?:E|EP)[._\-\s]*(\d{1,3})(?:(?:-[._\-\s]*(?:E|EP)?\d{1,3})|v\d)?(?=$|[._\-\s\]\)])'),
    # 2. 1x01, 01x01, [01x01]
    re.compile(r'(?i)(?:[._\-\s\[\(]|^)(\d{1,2})x(\d{1,3})(?=$|[._\-\s\]\)])'),
    # 3. Season 1 Episode 1 / Season 01 Ep 01 / Season 1 - 01 / Season 1 [01]
    re.compile(r'(?i)(?:[._\-\s\[\(]|^)Season[._\-\s]*(\d{1,2})[._\-\s]*(?:Episode|Ep|Part|Pt|-)[._\-\s]*(\d{1,3})(?=$|[._\-\s\]\)])'),
    # 4. Standalone Episode 01 / Ep 01 / EP01 (with word Episode or Ep)
    re.compile(r'(?i)(?:[._\-\s\[\(]|^)(?:Episode|Ep|EP)[._\-\s]*(\d{1,3})(?=$|[._\-\s\]\)])'),
    # 5. Standalone E01, E02 (must have at least 2 digits to prevent collision with movie tokens)
    re.compile(r'(?i)(?:[._\-\s\[\(])E(\d{2,3})(?=$|[._\-\s\]\)])'),
]

# Strips channel tags, bots, watermarks, sites at start of filename: [MM], [TR_HDS], @channel, www.site.com
CHANNEL_TAG_REGEX = re.compile(
    r'^(?:(?:\[.*?\]|\(.*?\)|@[\w_]+(?:\s+(?:ALL\s+)?(?:MOVIES\d*|FILMS|CHANNELS?|OFFICIAL|HUB|MEDIA|BOT|GROUP|LINKS|CORNER|WORLD|ZONE|HD|HQ)\b)?|www\.\S+[\s._-]*)[\s._-]*)+',
    re.IGNORECASE
)
URL_WATERMARK_REGEX = re.compile(r'(?i)\b(?:https?://\S+|www\.\S+|t\.me/\S+|@[a-zA-Z0-9_]+)\b')
YEAR_REGEX = re.compile(r'\b(19\d{2}|20\d{2})\b')
YEAR_BRACKET_REGEX = re.compile(r'\s*[\(\[]\s*(19\d{2}|20\d{2})\s*[\)\]]')

# High-confidence technical markers that signal where a title ends when no year is present
TECH_SPLIT_REGEX = re.compile(
    r'(?i)\b('
    r'2160p|1440p|1080p|1080i|720p|576p|480p|360p|4k|2k|uhd|fhd|hd|sd|'
    r'web-?dl|webrip|web|bluray|blu-ray|bdrip|brrip|dvdrip|dvdscr|dvd|hdtv|hdrip|cam|hdcam|'
    r'x264|x265|h\.?264|h\.?265|hevc|avc|av1|vp9|10bit|8bit|'
    r'aac\d*|ac3|eac3|ddp?\d*(?:\.\d)?|dts(?:-hd)?|truehd|atmos'
    r')\b'
)

LANGUAGE_TOKENS = (
    r"malayalam|tamil|hindi|telugu|kannada|bengali|marathi|punjabi|gujarati|urdu|"
    r"english|korean|japanese|chinese|spanish|french|german|russian|italian|"
    r"tam|tel|hin|kan|mal|eng|"
    r"dual[\s._-]?audio|multi[\s._-]?audio|dubbed|esubs?|subtitles?"
)

# Matches trailing language tag that is NOT at the start of the title:
# Requires preceding word character and delimiters, preventing titles starting with languages
# (e.g. 'Hindi Medium', 'English Vinglish') from getting erased.
TRAILING_LANG_REGEX = re.compile(rf"(?i)(?<=\w)[\s._\-]+(?:\b(?:{LANGUAGE_TOKENS})\b.*)$")

# Strips standalone language tags inside series titles (e.g. 'Nadu Center Tamil' -> 'Nadu Center')
SERIES_LANG_STRIP = re.compile(rf"(?i)\b(?:{LANGUAGE_TOKENS})\b")

RELEASE_TOKENS = re.compile(
    r"(?i)\b("
    r"2160p|1440p|1080p|1080i|720p|576p|480p|360p|4k|uhd|hdr(?:10)?\+?|dv|dolby\s*vision|"
    r"web-?dl|webrip|web|bluray|blu-ray|bdrip|brrip|dvdrip|dvdscr|hdtv|hdrip|hdts|cam|hdcam|"
    r"x264|x265|h\.?264|h\.?265|hevc|avc|av1|vp9|10bit|8bit|"
    r"aac\d*|ac3|eac3|ddp?\d*(?:\.\d)?|dts(?:-hd)?|truehd|atmos|mp3|flac|"
    r"\d+(?:\.\d+)?ch|"
    r"proper|repack|extended|unrated|remastered|theatrical|directors\.?cut|amzn|nf|dsnp|hulu|max|itunes|netflix|"
    r"yify|rargb|evo|tgx|galaxyrg|psa|mkv|mp4|avi|webm|hd|hq|dl|org"
    r")\b"
)

# Filters out spam / non-title lines from telegram message captions
JUNK_LINE_REGEX = re.compile(
    r'(?i)^\s*(?:'
    r'https?://\S+|www\.\S+|t\.me/\S+|@[\w_]+|'
    r'(?:forwarded\s*from|sent\s*from|downloaded\s*from).*|'
    r'(?:join|subscribe|download|click|share|uploaded|watch|size|quality|audio|subtitles?|format|codec|duration|imdb|rating|resolution|encoder|source|credit|powered)\s*[:\-].*|'
    r'[📩🎞🎧📜📦📁⏱️🎬✨🔍🔗⚡👉▶️]+\s*(?:size|quality|audio|subtitles?|quality|format|codec|duration|bitrate)?\s*[:\-]?.*|'
    r'(?:download|watch online|join channel|support group|how to download|channel link)\s*$'
    r')\s*$'
)

LANGUAGE_MAP = {
    "tam": ("Tamil", "🇮🇳"),
    "tamil": ("Tamil", "🇮🇳"),
    "tel": ("Telugu", "🇮🇳"),
    "telugu": ("Telugu", "🇮🇳"),
    "hin": ("Hindi", "🇮🇳"),
    "hindi": ("Hindi", "🇮🇳"),
    "mal": ("Malayalam", "🇮🇳"),
    "malayalam": ("Malayalam", "🇮🇳"),
    "kan": ("Kannada", "🇮🇳"),
    "kannada": ("Kannada", "🇮🇳"),
    "eng": ("English", "🇬🇧"),
    "english": ("English", "🇬🇧"),
    "ben": ("Bengali", "🇮🇳"),
    "bengali": ("Bengali", "🇮🇳"),
    "mar": ("Marathi", "🇮🇳"),
    "marathi": ("Marathi", "🇮🇳"),
    "pun": ("Punjabi", "🇮🇳"),
    "punjabi": ("Punjabi", "🇮🇳"),
    "guj": ("Gujarati", "🇮🇳"),
    "gujarati": ("Gujarati", "🇮🇳"),
    "urd": ("Urdu", "🇵🇰"),
    "urdu": ("Urdu", "🇵🇰"),
    "kor": ("Korean", "🇰🇷"),
    "korean": ("Korean", "🇰🇷"),
    "jap": ("Japanese", "🇯🇵"),
    "japanese": ("Japanese", "🇯🇵"),
    "spa": ("Spanish", "🇪🇸"),
    "spanish": ("Spanish", "🇪🇸"),
    "fre": ("French", "🇫🇷"),
    "french": ("French", "🇫🇷"),
    "ger": ("German", "🇩🇪"),
    "german": ("German", "🇩🇪"),
    "ita": ("Italian", "🇮🇹"),
    "italian": ("Italian", "🇮🇹"),
    "rus": ("Russian", "🇷🇺"),
    "russian": ("Russian", "🇷🇺"),
    "chi": ("Chinese", "🇨🇳"),
    "chinese": ("Chinese", "🇨🇳"),
}

LANGUAGE_CODE_MAP = {
    "malayalam": "ml", "mal": "ml",
    "tamil": "ta", "tam": "ta",
    "telugu": "te", "tel": "te",
    "hindi": "hi", "hin": "hi",
    "kannada": "kn", "kan": "kn",
    "english": "en", "eng": "en",
    "bengali": "bn", "ben": "bn",
    "marathi": "mr", "mar": "mr",
    "punjabi": "pa", "pun": "pa",
    "gujarati": "gu", "guj": "gu",
    "urdu": "ur", "urd": "ur",
    "korean": "ko", "kor": "ko",
    "japanese": "ja", "jap": "ja",
    "spanish": "es", "spa": "es",
    "french": "fr", "fre": "fr",
    "german": "de", "ger": "de",
    "italian": "it", "ita": "it",
    "russian": "ru", "rus": "ru",
    "chinese": "zh", "chi": "zh",
}

# Famous Malayalam dubbed releases that were given completely different titles in Kerala
# (Mapped to their canonical TMDB titles, particularly Allu Arjun & Telugu blockbusters)
MALAYALAM_RENAMED_DUBS = {
    # Allu Arjun
    ("krishna", 2008): "Parugu",
    ("hero", 2007): "Desamuduru",
    ("varan", 2010): "Varudu",
    ("killadi", 2010): "Vedam",
    ("romeoo", 2013): "Iddarammayilatho",
    ("romeoo & juliets", 2013): "Iddarammayilatho",
    ("romeo & juliets", 2013): "Iddarammayilatho",
    ("gajapokkiri", 2012): "Julayi",
    ("lucky the racer", 2014): "Race Gurram",
    ("yodhavu", 2016): "Sarrainodu",
    ("dhruvaraja jagannadh", 2017): "DJ: Duvvada Jagannadham",
    ("dhruvaraja jagannadham", 2017): "DJ: Duvvada Jagannadham",
    ("angu vaikuntapurathu", 2020): "Ala Vaikunthapurramuloo",
    ("angu vaikunthapurathu", 2020): "Ala Vaikunthapurramuloo",
    # Ram Charan
    ("bhaiyya", 2014): "Yevadu",
    ("ekalavya", 2014): "Govindudu Andarivadele",
    # Mahesh Babu
    ("bharat enna njan", 2018): "Bharat Ane Nenu",
    # Prabhas / Others
    ("chatrapati", 2005): "Chatrapathi",
}

def normalize_title(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"[^a-z0-9]", "", s.lower())

def strip_article(s: str) -> str:
    return re.sub(r"(?i)^(?:the|a|an)\s+", "", s.strip())

def choose_best_tmdb_hit(results: list[dict], query: str, year: int | None, file_lang: str | None, media_type: str) -> dict | None:
    """
    Clean, Strict Hierarchical TMDB Matching:
    1. Filter results by exact / normalized name match (no fuzzy or fake/partial close calls).
       - Exact string match (e.g. 'The Handmaiden' == 'The Handmaiden')
       - Punctuation-stripped match (e.g. 'The Raid: Redemption' == 'The Raid Redemption')
       - Article-stripped match (e.g. 'Handmaiden' == 'The Handmaiden')
    2. If NO results match the title, return None (never guess random popular movies like The Dark Knight!).
    3. If exactly 1 title matches, pick it immediately.
    4. If multiple match the same title:
       - Check year (allowing ±1 year release margin).
       - If still multiple, check language.
       - Otherwise pick top TMDB popularity.
    """
    if not results:
        return None

    q_clean = query.strip().lower()
    q_norm = normalize_title(query)
    q_no_art_norm = normalize_title(strip_article(query))

    name_matches = []
    for item in results:
        t = (item.get("name") if media_type == "series" else item.get("title") or "").strip()
        orig = (item.get("original_name") if media_type == "series" else item.get("original_title") or "").strip()
        t_clean = t.lower()
        orig_clean = orig.lower()
        t_norm = normalize_title(t)
        orig_norm = normalize_title(orig)
        t_no_art_norm = normalize_title(strip_article(t))
        orig_no_art_norm = normalize_title(strip_article(orig))

        # Check exact string match
        if t_clean == q_clean or orig_clean == q_clean:
            name_matches.append(item)
            continue
        # Check normalized match (ignoring punctuation/symbols)
        if (t_norm and t_norm == q_norm) or (orig_norm and orig_norm == q_norm):
            name_matches.append(item)
            continue
        # Check article-stripped match (e.g. 'The Handmaiden' vs 'Handmaiden')
        if (t_no_art_norm and t_no_art_norm == q_no_art_norm) or (orig_no_art_norm and orig_no_art_norm == q_no_art_norm):
            name_matches.append(item)
            continue

    # Strict: Never accept a result if the name doesn't match the query!
    if not name_matches:
        return None

    pool = name_matches

    # If release year was provided, strictly prioritize candidates with matching release year
    if year:
        # Tier 1: Exact release year match (item_yr == year)
        exact_year_matches = []
        for item in pool:
            d = (item.get("first_air_date" if media_type == "series" else "release_date") or "").strip()
            item_yr = int(d[:4]) if len(d) >= 4 and d[:4].isdigit() else None
            if item_yr and item_yr == year:
                exact_year_matches.append(item)

        if exact_year_matches:
            pool = exact_year_matches
            if len(pool) == 1:
                return pool[0]
        else:
            # Tier 2: Within ±1 year margin (e.g. film festival premiere vs general release)
            close_year_matches = []
            for item in pool:
                d = (item.get("first_air_date" if media_type == "series" else "release_date") or "").strip()
                item_yr = int(d[:4]) if len(d) >= 4 and d[:4].isdigit() else None
                if item_yr and abs(item_yr - year) <= 1:
                    close_year_matches.append(item)

            if close_year_matches:
                pool = close_year_matches
                if len(pool) == 1:
                    return pool[0]
            else:
                # Tier 3: Within ±2 years margin
                margin2_matches = []
                for item in pool:
                    d = (item.get("first_air_date" if media_type == "series" else "release_date") or "").strip()
                    item_yr = int(d[:4]) if len(d) >= 4 and d[:4].isdigit() else None
                    if item_yr and abs(item_yr - year) <= 2:
                        margin2_matches.append(item)

                if margin2_matches:
                    pool = margin2_matches
                    if len(pool) == 1:
                        return pool[0]
                else:
                    # Tier 4: Only if NO candidate has a release date within ±2 years,
                    # fallback to candidates with unannounced / missing release dates
                    no_date_matches = [
                        item for item in pool
                        if not (item.get("first_air_date" if media_type == "series" else "release_date") or "").strip()[:4].isdigit()
                    ]
                    if no_date_matches:
                        pool = no_date_matches
                        if len(pool) == 1:
                            return pool[0]
                    else:
                        # Release year mismatch by > 2 years (e.g. 1986 vs 2008). Reject!
                        return None
    elif len(pool) == 1:
        return pool[0]

    # If multiple still match the same name and year (wall hit!), disambiguate by language
    if file_lang:
        if isinstance(file_lang, (list, tuple, set)):
            lang_tokens = [str(x).lower() for x in file_lang]
        else:
            lang_tokens = re.split(r'[,+\s/|]+', str(file_lang).lower())

        target_codes = set()
        for tok in lang_tokens:
            cleaned_tok = re.sub(r'[^a-z]', '', tok)
            if cleaned_tok in LANGUAGE_CODE_MAP:
                target_codes.add(LANGUAGE_CODE_MAP[cleaned_tok])
            elif len(cleaned_tok) == 2:
                target_codes.add(cleaned_tok)

        if target_codes:
            lang_matches = [
                item for item in pool
                if (item.get("original_language") or "").lower() in target_codes
            ]
            if lang_matches:
                pool = lang_matches
                if len(pool) == 1:
                    return pool[0]

    # Fallback to top-ranked TMDB popularity
    pool.sort(key=lambda x: float(x.get("popularity", 0.0)), reverse=True)
    return pool[0]

def generate_poster_svg(title: str, media_type: str, year: int | None = None) -> str:
    """
    Generates a sleek, high-resolution SVG poster dynamically.
    Ensures that unindexed or fallback titles NEVER show as empty black cards in Stremio/Nuvio.
    """
    escaped_title = html.escape(title)
    badge_text = "📺 SERIES" if media_type == "series" else "🎬 MOVIE"
    badge_bg = "#6366f1" if media_type == "series" else "#06b6d4"
    year_str = f"({year})" if year else ""

    words = escaped_title.split()
    lines = []
    current_line = []
    current_len = 0
    for w in words:
        if current_len + len(w) > 18 and current_line:
            lines.append(" ".join(current_line))
            current_line = [w]
            current_len = len(w)
        else:
            current_line.append(w)
            current_len += len(w) + 1
    if current_line:
        lines.append(" ".join(current_line))

    start_y = 380 - (len(lines) - 1) * 25
    text_elements = []
    for idx, l in enumerate(lines[:4]):
        y = start_y + idx * 46
        text_elements.append(f'<text x="250" y="{y}" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, sans-serif" font-size="32" font-weight="800" fill="#ffffff" text-anchor="middle">{l}</text>')

    title_svg = "\n    ".join(text_elements)
    year_element = f'<text x="250" y="{start_y + len(lines[:4]) * 46 + 10}" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif" font-size="20" font-weight="600" fill="#94a3b8" text-anchor="middle">{year_str}</text>' if year_str else ""

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="500" height="750" viewBox="0 0 500 750">
  <defs>
    <linearGradient id="bg" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#1e1e2f"/>
      <stop offset="50%" stop-color="#14141e"/>
      <stop offset="100%" stop-color="#0a0a0f"/>
    </linearGradient>
    <linearGradient id="glow" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="{badge_bg}" stop-opacity="0.8"/>
      <stop offset="100%" stop-color="#a855f7" stop-opacity="0.8"/>
    </linearGradient>
  </defs>

  <rect width="500" height="750" rx="20" fill="url(#bg)"/>
  <rect x="10" y="10" width="480" height="730" rx="16" fill="none" stroke="#2a2a3e" stroke-width="2"/>

  <rect x="180" y="40" width="140" height="36" rx="18" fill="url(#glow)"/>
  <text x="250" y="64" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif" font-size="14" font-weight="bold" fill="#ffffff" text-anchor="middle" letter-spacing="1">{badge_text}</text>

  <circle cx="250" cy="220" r="48" fill="#1f2233" stroke="{badge_bg}" stroke-width="2"/>
  <polygon points="242,204 266,220 242,236" fill="#ffffff"/>

  {title_svg}
  {year_element}

  <line x1="150" y1="670" x2="350" y2="670" stroke="#2e344e" stroke-width="1"/>
  <text x="250" y="700" font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif" font-size="13" font-weight="bold" fill="#64748b" text-anchor="middle" letter-spacing="2">TELEGRAM LIBRARY TV</text>
</svg>"""

def format_file_size(size_bytes: int) -> str:
    if not size_bytes:
        return ""
    if size_bytes >= 1024 ** 3:
        return f"{size_bytes / (1024 ** 3):.2f} GB"
    return f"{size_bytes / (1024 ** 2):.1f} MB"

def format_duration(seconds: float | int | None) -> str:
    if not seconds or seconds <= 0:
        return ""
    try:
        total_sec = int(round(float(seconds)))
    except (ValueError, TypeError):
        return ""
    if total_sec <= 0:
        return ""
    h = total_sec // 3600
    m = (total_sec % 3600) // 60
    s = total_sec % 60
    if h > 0:
        return f"{h}h:{m:02d}m:{s:02d}s"
    return f"{m}m:{s:02d}s"

def extract_media_specs(text_corpus: str, file_size: int = 0, video_attr: dict | None = None) -> dict:
    """
    Extracts deep technical specifications from filenames, captions, and MTProto metadata:
      - Resolution: 4K/2160p, 1440p, 1080p, 720p, 480p
      - Source: WEB-DL, WEBRip, BluRay, HDRip, HDTV, CAM
      - Codec: HEVC/x265, AVC/x264, AV1, VP9, 10bit
      - Audio: Dolby Atmos, DD+ 5.1, DD 5.1, DTS-HD, AAC 2.0, bitrates (192kbps)
      - Audio Languages & Emojis: Tamil, Telugu, Hindi, Malayalam, Kannada, English
      - Subtitles: English ESub, Multi Sub
      - Size and Duration
    """
    t = text_corpus
    specs = {
        "resolution": None,
        "source": None,
        "codec": None,
        "bit_depth": None,
        "hdr": None,
        "audio": None,
        "audio_bitrate": None,
        "languages": [],
        "language_flags": [],
        "subtitles": None,
        "duration_str": "",
        "size_str": format_file_size(file_size)
    }

    if video_attr:
        w = video_attr.get("w", 0)
        h = video_attr.get("h", 0)
        dur = video_attr.get("duration", 0)
        if dur:
            specs["duration_str"] = format_duration(dur)
        if h >= 2100 or w >= 3800:
            specs["resolution"] = "4K"
        elif h >= 1400 or w >= 2500:
            specs["resolution"] = "1440p"
        elif h >= 1000 or w >= 1900:
            specs["resolution"] = "1080p"
        elif h >= 700 or w >= 1200:
            specs["resolution"] = "720p"
        elif h >= 460 or w >= 700:
            specs["resolution"] = "480p"

    # Resolution from text tokens
    if not specs["resolution"]:
        m_res = re.search(r'(?i)\b(2160p|4k|uhd)\b', t)
        if m_res:
            specs["resolution"] = "4K"
        elif re.search(r'(?i)\b(1440p|2k)\b', t):
            specs["resolution"] = "1440p"
        elif re.search(r'(?i)\b(1080p|1080i|fhd)\b', t):
            specs["resolution"] = "1080p"
        elif re.search(r'(?i)\b(720p|hd)\b', t):
            specs["resolution"] = "720p"
        elif re.search(r'(?i)\b(576p|480p|sd|360p)\b', t):
            m_sd = re.search(r'(?i)\b(576p|480p|sd|360p)\b', t)
            specs["resolution"] = m_sd.group(1).upper()
        m_q = re.search(r'(?i)Quality\s*:\s*([0-9a-z]+)', t)
        if m_q and not specs["resolution"]:
            specs["resolution"] = m_q.group(1).strip()

    # Source / Rip type
    m_src = re.search(r'(?i)\b(web-?dl|webrip|blu-?ray|bdrip|brrip|hdrip|dvdrip|hdtv|telesync|cam|hdcam)\b', t)
    if m_src:
        raw_s = m_src.group(1).lower().replace("-", "")
        if "webdl" in raw_s:
            specs["source"] = "WEB-DL"
        elif "webrip" in raw_s:
            specs["source"] = "WEBRip"
        elif "bluray" in raw_s or "bdrip" in raw_s or "brrip" in raw_s:
            specs["source"] = "BluRay"
        elif "hdrip" in raw_s:
            specs["source"] = "HDRip"
        elif "dvdrip" in raw_s:
            specs["source"] = "DVDRip"
        elif "hdtv" in raw_s:
            specs["source"] = "HDTV"
        elif "cam" in raw_s or "telesync" in raw_s:
            specs["source"] = "CAM"

    # Video Codec
    if re.search(r'(?i)\b(x265|hevc|h\.?265)\b', t):
        specs["codec"] = "HEVC"
    elif re.search(r'(?i)\b(x264|avc|h\.?264)\b', t):
        specs["codec"] = "AVC"
    elif re.search(r'(?i)\bav1\b', t):
        specs["codec"] = "AV1"
    elif re.search(r'(?i)\bvp9\b', t):
        specs["codec"] = "VP9"

    # Bit Depth
    if re.search(r'(?i)\b(10bit|10-bit|hi10p)\b', t):
        specs["bit_depth"] = "10bit"
    elif re.search(r'(?i)\b(8bit|8-bit)\b', t):
        specs["bit_depth"] = "8bit"

    # HDR / Dolby Vision
    if re.search(r'(?i)\b(dv|dolby\s*vision)\b', t):
        specs["hdr"] = "DV"
    elif re.search(r'(?i)\bhdr10\+\b', t):
        specs["hdr"] = "HDR10+"
    elif re.search(r'(?i)\bhdr(?:10)?\b', t):
        specs["hdr"] = "HDR"

    # Audio format & channels
    audio_parts = []
    if re.search(r'(?i)\b(atmos|dolby\s*atmos)\b', t):
        audio_parts.append("Dolby Atmos")
    elif re.search(r'(?i)(?:\bdd\+|ddp|eac-?3|dolby\s*digital\s*plus)\b', t) or "dd+" in t.lower():
        audio_parts.append("DD+")
    elif re.search(r'(?i)\b(dd|ac-?3|dolby\s*digital)\b', t):
        audio_parts.append("DD")
    elif re.search(r'(?i)\bdts-hd\b', t):
        audio_parts.append("DTS-HD")
    elif re.search(r'(?i)\bdts\b', t):
        audio_parts.append("DTS")
    elif re.search(r'(?i)\btruehd\b', t):
        audio_parts.append("TrueHD")
    elif re.search(r'(?i)\baac\b', t):
        audio_parts.append("AAC")
    elif re.search(r'(?i)\bflac\b', t):
        audio_parts.append("FLAC")
    elif re.search(r'(?i)\bmp3\b', t):
        audio_parts.append("MP3")

    m_ch = re.search(r'\b(7\.1|5\.1|2\.0|2\.1)\b', t)
    channels = m_ch.group(1) if m_ch else None

    m_br = re.search(r'(?i)\b(\d{2,4}\s*kbps)\b', t)
    if m_br:
        specs["audio_bitrate"] = m_br.group(1).replace(" ", "")

    if audio_parts:
        base_aud = audio_parts[0]
        if channels and channels not in base_aud:
            specs["audio"] = f"{base_aud} {channels}"
        else:
            specs["audio"] = base_aud
    elif channels:
        specs["audio"] = f"Audio {channels}"

    # Languages
    found_langs = []
    flags = []

    m_cap_aud = re.search(r'(?i)Audio\s*:\s*([^\n\r]+)', t)
    aud_text = m_cap_aud.group(1) if m_cap_aud else ""

    bracket_matches = re.findall(r'\[(.*?)\]|\((.*?)\)', t)
    bracket_text = " ".join([b[0] or b[1] for b in bracket_matches if not re.search(r'(?i)\b(sub|esub|msub)\b', b[0] or b[1])])

    t_no_sub = re.sub(r'(?i)subtitles?\s*:[^\n\r]*', ' ', t)
    t_no_sub = re.sub(r'(?i)\b[em]subs?\b', ' ', t_no_sub)

    search_zone = f"{aud_text} {bracket_text} {t_no_sub}"
    tokens = re.split(r'[,+\s/|]+', search_zone)
    for tok in tokens:
        cleaned = re.sub(r'[^a-zA-Z]', '', tok).lower()
        if cleaned in LANGUAGE_MAP:
            name, flag = LANGUAGE_MAP[cleaned]
            if name not in found_langs:
                found_langs.append(name)
                if flag not in flags:
                    flags.append(flag)

    if not found_langs:
        if re.search(r'(?i)\b(dual[\s._-]?audio)\b', t):
            found_langs.append("Dual Audio")
        elif re.search(r'(?i)\b(multi[\s._-]?audio)\b', t):
            found_langs.append("Multi Audio")

    specs["languages"] = found_langs
    specs["language_flags"] = flags

    # Subtitles
    if re.search(r'(?i)\b(esub|esubs)\b', t) or re.search(r'(?i)subtitles?\s*:\s*english', t):
        specs["subtitles"] = "English ESub"
    elif re.search(r'(?i)\b(msub|msubs)\b', t) or re.search(r'(?i)subtitles?\s*:\s*multi', t):
        specs["subtitles"] = "Multi Sub"
    elif re.search(r'(?i)subtitles?\s*:\s*([^\n\r]+)', t):
        sub_m = re.search(r'(?i)subtitles?\s*:\s*([^\n\r]+)', t)
        specs["subtitles"] = sub_m.group(1).strip()

    return specs

def format_stremio_stream(title_header: str, specs: dict) -> tuple[str, str]:
    """
    Produces clean, polished TMPlayer / AIOStreams style stream item:
    Returns (name_badge, full_description)
    """
    res = specs.get("resolution") or "HD"
    name_badge = f"[Telegram ⚡] {res}"

    lines = [f"📁 {title_header}"]

    v_parts = []
    if specs.get("source"):
        v_parts.append(f"🎥 {specs['source']}")
    if specs.get("codec"):
        v_parts.append(f"🎞️ {specs['codec']}")
    if specs.get("bit_depth"):
        v_parts.append(specs["bit_depth"])
    if specs.get("hdr"):
        v_parts.append(f"✨ {specs['hdr']}")
    if v_parts:
        lines.append(" • ".join(v_parts))

    a_parts = []
    if specs.get("audio"):
        a_parts.append(f"🎧 {specs['audio']}")
    if specs.get("audio_bitrate"):
        a_parts.append(f"({specs['audio_bitrate']})")
    if a_parts:
        lines.append(" ".join(a_parts))

    meta_parts = []
    if specs.get("languages"):
        langs_str = ", ".join(specs["languages"])
        flags_str = "".join(specs["language_flags"])
        meta_parts.append(f"🌐 {langs_str} {flags_str}".strip())
    if specs.get("subtitles"):
        meta_parts.append(f"📜 {specs['subtitles']}")
    if meta_parts:
        lines.append(" | ".join(meta_parts))

    file_parts = []
    if specs.get("size_str"):
        file_parts.append(f"📦 {specs['size_str']}")
    if specs.get("duration_str"):
        file_parts.append(f"⏱️ {specs['duration_str']}")
    if file_parts:
        lines.append(" • ".join(file_parts))

    full_desc = "\n".join(lines)
    return name_badge, full_desc

def clean_series_title(title_part: str) -> tuple[str, int | None]:
    t = URL_WATERMARK_REGEX.sub(" ", title_part)
    t = re.sub(r'[_]+', ' ', t)
    t = re.sub(r'(19\d{2}|20\d{2})([a-zA-Z])', r'\1 \2', t)
    t = re.sub(r'([a-zA-Z])(19\d{2}|20\d{2})', r'\1 \2', t)
    year_matches = list(YEAR_REGEX.finditer(t))
    year = None
    if year_matches:
        last_m = year_matches[-1]
        if last_m.start() > 0:
            prefix = t[:last_m.start()].strip()
            prefix = re.sub(r'[\(\[\{\s._\-+:]+$', '', prefix).strip()
            if len(prefix) >= 2:
                year = int(last_m.group(1))
                t = prefix
        else:
            year = int(last_m.group(1))

    clean = YEAR_BRACKET_REGEX.sub(" ", t)
    clean = YEAR_REGEX.sub(" ", clean)
    clean = re.sub(r"\[.*?\]|\(.*?\)", " ", clean)
    clean = TRAILING_LANG_REGEX.sub(" ", clean)
    clean = SERIES_LANG_STRIP.sub(" ", clean)
    clean = RELEASE_TOKENS.sub(" ", clean)
    clean = re.sub(r"[._\-+:]+", " ", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean, year

def clean_episode_title(ep_part: str) -> str:
    cleaned = URL_WATERMARK_REGEX.sub(" ", ep_part)
    cleaned = re.sub(r"\[.*?\]|\(.*?\)", " ", cleaned)
    cleaned = TRAILING_LANG_REGEX.sub(" ", cleaned)
    cleaned = SERIES_LANG_STRIP.sub(" ", cleaned)
    cleaned = RELEASE_TOKENS.sub(" ", cleaned)
    cleaned = re.sub(r"[._\-+:]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned

def parse_filename(file_name: str, caption: str = "") -> dict:
    """
    100% Reliable TV Show vs Movie Classification & Title Extraction:
    - Extracts explicit caption fields ("Movie: ...", "Title: ...", "Year: ...") if present.
    - If any candidate contains Season/Episode indicators, STRICTLY classified as TV Series.
    - Movie sequels ('Pushpa 2', 'Drishyam 2') remain strictly Movies.
    - Universal Movie Title Resolution:
        1. Release Year boundary split: Title <Year> Specs
        2. Technical Spec token boundary split: Title <Resolution/Codec/Rip> Specs
        3. All variations of the same show map to the clean canonical title.
    """
    candidates = []
    caption_year = None
    explicit_title = None

    if caption:
        for line in caption.split("\n"):
            line = line.strip()
            if not line:
                continue
            # Check for explicit Movie / Series / Title / Film field in caption
            m_field = re.match(r'(?i)^\s*(?:[🎬📺🎥🎞️])?\s*(?:movie|title|film|series|name|show)\s*[:\-]\s*(.+)$', line)
            if m_field:
                explicit_title = m_field.group(1).strip()
            # Check for explicit Year field in caption
            m_yr = re.match(r'(?i)^\s*(?:[📅🗓️])?\s*(?:year|release)\s*[:\-]\s*(19\d{2}|20\d{2})', line)
            if m_yr:
                caption_year = int(m_yr.group(1))

    if explicit_title:
        candidates.append(explicit_title)

    # If file_name is not generic Telegram placeholder
    if file_name and not re.match(r'(?i)^video_\d+\.\w+$', file_name):
        candidates.append(file_name)

    if caption:
        for line in caption.split("\n"):
            line = line.strip()
            if not line or JUNK_LINE_REGEX.match(line):
                continue
            cleaned = re.sub(r'[^\w\s]', '', line).strip()
            if len(cleaned) < 2:
                continue
            if line not in candidates:
                candidates.append(line)

    if not candidates:
        candidates = [file_name or "Unknown"]

    # If first candidate was truncated by Telegram ('...'), prefer untruncated candidate from caption
    if "..." in candidates[0] or "…" in candidates[0]:
        untruncated = next((c for c in candidates[1:] if "..." not in c and "…" not in c), None)
        if untruncated:
            candidates.remove(untruncated)
            candidates.insert(0, untruncated)

    # Detect language and dubbed indicators if mentioned in filename or caption
    full_text = " ".join(candidates) + " " + caption
    lang_m = re.search(rf"(?i)\b({LANGUAGE_TOKENS})\b", full_text)
    found_lang = lang_m.group(1).lower() if lang_m else None
    is_dubbed = bool(re.search(r'(?i)\bdub(?:bed)?\b', full_text))

    # Stage 1: Check for Series patterns across candidates
    series_results = []
    for candidate in candidates:
        base = candidate.rsplit(".", 1)[0] if "." in candidate else candidate
        base = CHANNEL_TAG_REGEX.sub("", base).strip()
        base = URL_WATERMARK_REGEX.sub("", base).strip()

        for pattern in SEASON_EPISODE_PATTERNS:
            m = pattern.search(base)
            if m:
                groups = m.groups()
                if len(groups) == 2:
                    season = int(groups[0])
                    episode = int(groups[1])
                elif len(groups) == 1:
                    season = 1
                    episode = int(groups[0])
                else:
                    season = 1
                    episode = 1

                show_part = base[:m.start()]
                ep_part = base[m.end():]

                raw_title, year = clean_series_title(show_part)
                if not year and caption_year:
                    year = caption_year
                ep_name = clean_episode_title(ep_part)

                series_results.append({
                    "raw_title": raw_title or base.strip(),
                    "episode_title": ep_name if ep_name else None,
                    "year": year,
                    "season": season,
                    "episode": episode,
                    "media_type": "series",
                    "language": found_lang,
                    "is_dubbed": is_dubbed
                })
                break

    if series_results:
        # Prefer candidate that has an untruncated, explicit episode title
        best_series = next((sr for sr in series_results if sr["episode_title"] and "..." not in sr["episode_title"]), series_results[0])
        if not best_series["episode_title"]:
            best_series["episode_title"] = f"Episode {best_series['episode']}"
        return best_series

    # Stage 2: Classify as Movie
    primary = candidates[0]
    base = primary.rsplit(".", 1)[0] if "." in primary else primary
    base = CHANNEL_TAG_REGEX.sub("", base).strip()
    base = URL_WATERMARK_REGEX.sub("", base).strip()
    base = re.sub(r'[_]+', ' ', base)
    base = re.sub(r'(19\d{2}|20\d{2})([a-zA-Z])', r'\1 \2', base)
    base = re.sub(r'([a-zA-Z])(19\d{2}|20\d{2})', r'\1 \2', base)

    # Strategy 1: Release Year boundary split (Title <Year> Specs)
    year_matches = list(YEAR_REGEX.finditer(base))
    year = None
    if year_matches:
        last_m = year_matches[-1]
        # Only treat as release year boundary if it's not the very start of the title
        if last_m.start() > 0:
            prefix = base[:last_m.start()].strip()
            # Clean trailing brackets/delimiters before year: e.g. "Title (2022)" -> "Title"
            prefix = re.sub(r'[\(\[\{\s._\-+:]+$', '', prefix).strip()
            if len(prefix) >= 2:
                year = int(last_m.group(1))
                base = prefix
        else:
            year = int(last_m.group(1))

    if not year and caption_year:
        year = caption_year

    # Strategy 2: Tech Token boundary split (when no release year exists)
    if not year:
        tech_m = TECH_SPLIT_REGEX.search(base)
        if tech_m and tech_m.start() > 2:
            prefix = base[:tech_m.start()].strip()
            prefix = re.sub(r'[\(\[\{\s._\-+:]+$', '', prefix).strip()
            if len(prefix) >= 2:
                base = prefix

    clean = YEAR_BRACKET_REGEX.sub(" ", base)
    clean = YEAR_REGEX.sub(" ", clean)
    clean = re.sub(r"\[.*?\]|\(.*?\)", " ", clean)
    clean = TRAILING_LANG_REGEX.sub(" ", clean)
    clean = RELEASE_TOKENS.sub(" ", clean)
    clean = re.sub(r"[._\-+:]+", " ", clean)
    raw_title = re.sub(r"\s+", " ", clean).strip()

    return {
        "raw_title": raw_title or base.strip(),
        "episode_title": None,
        "year": year,
        "season": None,
        "episode": None,
        "media_type": "movie",
        "language": found_lang,
        "is_dubbed": is_dubbed
    }

STOP_WORDS = {
    "the", "a", "an", "and", "or", "of", "in", "to", "for", "with", "on", "at",
    "by", "from", "as", "is", "it", "part", "vol", "volume", "season", "episode",
    "series", "movie", "film", "org"
}

def clean_sub_candidate(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[\s:._\-]+$", "", s)
    s = re.sub(r"(?i)\b(?:the|a|an|of|in|to|for|and|or|&)\b$", "", s).strip()
    return re.sub(r"[\s:._\-]+$", "", s).strip()

def generate_search_candidates(raw_title: str) -> list[str]:
    """
    Produces clean progressive ladder of search query candidates:
    1. Full raw title (e.g. 'The Handmaiden')
    2. Subtitle prefix before ':' or ' - ' (e.g. 'Khalifa' from 'Khalifa: The Ruler')
    3. Article-stripped title (e.g. 'Handmaiden' from 'The Handmaiden')
    4. Word prefix ladder, stripping trailing stop-words and punctuation
    STRICT: Never generates bare stop words like 'The' or 'A'.
    """
    candidates = []
    t = raw_title.strip()
    if not t:
        return candidates

    candidates.append(t)

    # Subtitle separators
    if " - " in t:
        sub = clean_sub_candidate(t.split(" - ")[0])
        if sub:
            candidates.append(sub)
    if ": " in t:
        sub = clean_sub_candidate(t.split(": ")[0])
        if sub:
            candidates.append(sub)

    # Title without leading article ('The Handmaiden' -> 'Handmaiden')
    no_article = re.sub(r"(?i)^(?:the|a|an)\s+", "", t).strip()
    if no_article and no_article.lower() != t.lower():
        candidates.append(no_article)

    # Word prefix ladder (e.g. 'The Raid Redemption' -> 'The Raid')
    words = t.split()
    if len(words) > 1:
        for i in range(len(words) - 1, 0, -1):
            sub = clean_sub_candidate(" ".join(words[:i]))
            if sub:
                candidates.append(sub)
                sub_no_art = re.sub(r"(?i)^(?:the|a|an)\s+", "", sub).strip()
                if sub_no_art and sub_no_art.lower() != sub.lower():
                    candidates.append(sub_no_art)

    # Filter invalid candidates: never search bare stop words like 'The' or single letters
    filtered = []
    seen = set()
    for c in candidates:
        c_clean = c.strip()
        low = c_clean.lower()
        if not low or low in seen:
            continue
        # Disallow if candidate is in STOP_WORDS
        if low in STOP_WORDS:
            continue
        # Disallow if candidate is a single word and is too short or in STOP_WORDS
        c_words = low.split()
        if len(c_words) == 1 and (len(low) < 2 or low in STOP_WORDS):
            continue
        seen.add(low)
        filtered.append(c_clean)

    return filtered

async def search_wikipedia_title(query: str) -> list[str]:
    url = f"https://en.wikipedia.org/w/api.php?action=opensearch&search={urllib.parse.quote(query)}&limit=4&namespace=0&format=json"
    try:
        async with httpx.AsyncClient(timeout=6.0) as client:
            resp = await client.get(url, headers={"User-Agent": "StremioTelegram/1.0"})
            if resp.status_code == 200:
                data = resp.json()
                titles = data[1] if len(data) > 1 else []
                cleaned = []
                for t in titles:
                    c = re.sub(r"\s*\([^)]*\)", "", t).strip()
                    if c and c.lower() != query.lower():
                        cleaned.append(c)
                return cleaned
    except Exception:
        pass
    return []

_series_meta_cache: dict[str, dict] = {}

def get_series_cache(title: str) -> dict | None:
    norm_key = re.sub(r"[^a-z0-9]", "", title.lower())
    return _series_meta_cache.get(norm_key)

def set_series_cache(title: str, meta: dict):
    norm_key = re.sub(r"[^a-z0-9]", "", title.lower())
    _series_meta_cache[norm_key] = meta

async def tmdb_get_with_retry(client: httpx.AsyncClient, url: str, params: dict, max_retries: int = 4) -> dict | None:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }
    for attempt in range(max_retries):
        try:
            resp = await client.get(url, params=params, headers=headers, timeout=12.0)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait_s = 1.5 * (attempt + 1)
                await asyncio.sleep(wait_s)
                continue
            elif resp.status_code == 404:
                return None
            else:
                pass
        except Exception as e:
            wait_s = 0.8 * (attempt + 1)
            if attempt == max_retries - 1:
                print(f"[TMDB Notice] Request failed after {max_retries} attempts on {url}: {e}")
            await asyncio.sleep(wait_s)
    return None

async def execute_tmdb_search(client: httpx.AsyncClient, api_key: str, query: str, year: int | None, media_type: str, language: str | None = None) -> dict | None:
    """
    Executes TMDB search strictly constrained by media_type:
    - If media_type == 'series': ONLY searches /3/search/tv. NEVER considers movies.
    - If media_type == 'movie': ONLY searches /3/search/movie. NEVER considers TV shows.
    - Hierarchical matching: Name -> Year -> Language -> Popularity.
    """
    endpoint = "tv" if media_type == "series" else "movie"
    url = f"https://api.themoviedb.org/3/search/{endpoint}"

    params = {
        "api_key": api_key,
        "query": query,
        "include_adult": "true"
    }

    all_results = []
    seen_ids = set()

    # Step 1: If year was provided, query with year filter to surface same-year candidates
    if year:
        params_year = dict(params)
        params_year["year" if media_type == "movie" else "first_air_date_year"] = year
        data_y = await tmdb_get_with_retry(client, url, params_year)
        if data_y:
            for item in data_y.get("results", []):
                iid = item.get("id")
                if iid and iid not in seen_ids:
                    seen_ids.add(iid)
                    all_results.append(item)

    # Step 2: Also query general search to cover broad catalog
    data = await tmdb_get_with_retry(client, url, params)
    if data:
        for item in data.get("results", []):
            iid = item.get("id")
            if iid and iid not in seen_ids:
                seen_ids.add(iid)
                all_results.append(item)

    results = all_results

    # Fallback to multi-search ONLY filtering for the exact requested media_type
    if not results:
        multi_url = "https://api.themoviedb.org/3/search/multi"
        mdata = await tmdb_get_with_retry(client, multi_url, {"api_key": api_key, "query": query, "include_adult": "true"})
        if mdata:
            target_raw = "tv" if media_type == "series" else "movie"
            for item in mdata.get("results", []):
                if item.get("media_type") == target_raw:
                    results.append(item)

    if not results:
        return None

    # Step-by-step hierarchical candidate selection (Name -> Year -> Language -> Top TMDB Hit)
    hit = choose_best_tmdb_hit(results, query, year, language, media_type)

    # Step-by-step fallback: Check official alternative / dubbed titles in TMDB
    # Handles regional releases with altered titles (e.g. Malayalam 'Yodhavu' -> Telugu 'Sarrainodu')
    if not hit and results:
        q_norm = normalize_title(query)
        q_no_art_norm = normalize_title(strip_article(query))
        q_clean = query.strip().lower()

        for cand in results[:4]:
            cand_d = (cand.get("first_air_date" if media_type == "series" else "release_date") or "").strip()
            cand_yr = int(cand_d[:4]) if len(cand_d) >= 4 and cand_d[:4].isdigit() else None
            # If release year was provided in filename, candidate must match within ±1 year
            if year and cand_yr and abs(cand_yr - year) > 1:
                continue

            cand_id = cand.get("id")
            if not cand_id:
                continue

            alt_endpoint = "tv" if media_type == "series" else "movie"
            alt_url = f"https://api.themoviedb.org/3/{alt_endpoint}/{cand_id}/alternative_titles"
            alt_data = await tmdb_get_with_retry(client, alt_url, {"api_key": api_key})
            if alt_data:
                titles_list = alt_data.get("titles") or alt_data.get("results") or []
                for alt_entry in titles_list:
                    alt_title = (alt_entry.get("title") or alt_entry.get("name") or "").strip()
                    if not alt_title:
                        continue
                    alt_norm = normalize_title(alt_title)
                    alt_no_art_norm = normalize_title(strip_article(alt_title))
                    if q_norm == alt_norm or q_no_art_norm == alt_no_art_norm or q_clean == alt_title.lower():
                        print(f"[TMDB Dubbed/Alternative Title Matched] '{query}' ({year}) -> '{cand.get('title')}' (TMDB: {cand_id})")
                        hit = cand
                        break
            if hit:
                break

    if not hit:
        return None

    tmdb_id = hit.get("id")
    actual_type = "series" if media_type == "series" else "movie"

    item_title = hit.get("name" if actual_type == "series" else "title", query)
    date_str = hit.get("first_air_date" if actual_type == "series" else "release_date") or ""
    item_year = int(date_str[:4]) if len(date_str) >= 4 else year
    poster_path = hit.get("poster_path")
    backdrop_path = hit.get("backdrop_path")
    overview = hit.get("overview", "")
    rating = round(float(hit.get("vote_average", 0.0)), 1)

    poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else f"{PUBLIC_URL}/poster/{actual_type}/{urllib.parse.quote(item_title)}.svg"
    backdrop_url = f"https://image.tmdb.org/t/p/original{backdrop_path}" if backdrop_path else None

    # Fetch IMDb ID via external_ids endpoint
    imdb_id = None
    try:
        tmdb_endpoint = "tv" if actual_type == "series" else "movie"
        ext_url = f"https://api.themoviedb.org/3/{tmdb_endpoint}/{tmdb_id}/external_ids"
        ext_data = await tmdb_get_with_retry(client, ext_url, {"api_key": api_key})
        if ext_data:
            imdb_id = ext_data.get("imdb_id")
    except Exception:
        pass

    return {
        "tmdb_id": tmdb_id,
        "imdb_id": imdb_id,
        "media_type": actual_type,
        "title": item_title,
        "year": item_year,
        "poster": poster_url,
        "backdrop": backdrop_url,
        "overview": overview,
        "rating": rating,
        "genres": []
    }

async def lookup_tmdb(file_name: str, caption: str = "", parsed_override: dict | None = None) -> dict:
    """
    Bulletproof multi-stage TMDB matcher matching TGTV's logic:
    - Enforces 100% strict media_type fidelity (Series never become Movie).
    - Caches series metadata so all episodes of the same show share the exact same card.
    - Generates dynamic SVG poster if no image is available so no card is ever blank.
    """
    parsed = parsed_override or parse_filename(file_name, caption)
    raw_title = parsed["raw_title"]
    year = parsed["year"]
    media_type = parsed["media_type"]
    season = parsed["season"]
    episode = parsed["episode"]
    episode_title = parsed["episode_title"]
    language = parsed.get("language")

    # 1. Series Cache Check: Guarantee all episodes of the same show share the EXACT same card
    if media_type == "series":
        cached_hit = get_series_cache(raw_title)
        if cached_hit:
            hit = dict(cached_hit)
            hit["season"] = season
            hit["episode"] = episode
            hit["episode_title"] = episode_title or (f"Episode {episode}" if episode else None)
            return hit

    # Canonical show-level fallback ID ensures all episodes of 'Love' or 'Nadu Center' group together
    if media_type == "series":
        fallback_id = abs(hash(f"series:{raw_title.lower()}")) % 100000000
    else:
        fallback_id = abs(hash(f"movie:{raw_title.lower()}")) % 100000000

    fallback_poster = f"{PUBLIC_URL}/poster/{media_type}/{urllib.parse.quote(raw_title or file_name)}.svg"

    fallback = {
        "tmdb_id": fallback_id,
        "imdb_id": None,
        "media_type": media_type,
        "title": raw_title or file_name,
        "episode_title": episode_title or (f"Episode {episode}" if episode else None),
        "year": year,
        "season": season,
        "episode": episode,
        "poster": fallback_poster,
        "backdrop": None,
        "overview": f"{'Series' if media_type == 'series' else 'Movie'}: {raw_title}",
        "rating": 0.0,
        "genres": []
    }

    api_key = get_tmdb_key()
    if not api_key:
        return fallback

    candidates = generate_search_candidates(raw_title)

    # Dubbed / Renamed release mapping:
    # If the file is tagged as a Malayalam release or Malayalam dubbed,
    # check known renamed theatrical releases (e.g. 'Krishna (2008)' -> 'Parugu')
    norm_title = raw_title.strip().lower()
    norm_key = (norm_title, year)
    if (language == "malayalam" or parsed.get("is_dubbed")) and norm_key in MALAYALAM_RENAMED_DUBS:
        renamed = MALAYALAM_RENAMED_DUBS[norm_key]
        if renamed not in candidates:
            candidates.insert(0, renamed)

    try:
        async with httpx.AsyncClient(timeout=14.0) as client:
            # Stage 1: Search progressive query candidates
            for query in candidates:
                hit = await execute_tmdb_search(client, api_key, query, year, media_type, language=language)
                if hit:
                    hit["season"] = season
                    hit["episode"] = episode
                    hit["episode_title"] = episode_title
                    if media_type == "series":
                        set_series_cache(raw_title, hit)
                        set_series_cache(hit["title"], hit)
                    print(f"[TMDB Matched] '{file_name}' -> '{hit['title']}' ({hit['year']}) [{media_type.upper()}] [IMDb: {hit['imdb_id']}, TMDB: {hit['tmdb_id']}]")
                    return hit

            # Stage 2: Web resolution fallback (Wikipedia OpenSearch)
            wiki_candidates = await search_wikipedia_title(raw_title)
            for wquery in wiki_candidates:
                hit = await execute_tmdb_search(client, api_key, wquery, year, media_type, language=language)
                if hit:
                    hit["season"] = season
                    hit["episode"] = episode
                    hit["episode_title"] = episode_title
                    if media_type == "series":
                        set_series_cache(raw_title, hit)
                        set_series_cache(hit["title"], hit)
                    print(f"[TMDB Wiki-Resolved] '{file_name}' -> '{hit['title']}' ({hit['year']}) [{media_type.upper()}] [IMDb: {hit['imdb_id']}, TMDB: {hit['tmdb_id']}]")
                    return hit

    except Exception as e:
        print(f"[TMDB Exception] '{file_name}': {e}")

    return fallback
