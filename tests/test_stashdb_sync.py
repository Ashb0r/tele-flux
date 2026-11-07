import os
import sys
import sqlite3

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from stashdb import clean_stash_filename, format_stashdb_stream, generate_adult_poster_svg, select_media_images
from database import parse_stremio_id
from settings_manager import load_settings, save_settings

print("--- Test 1: StashDB Filename Cleaning ---", flush=True)
assert clean_stash_filename("Brazzers.Exxtra.Mia.Malkova.1080p.HEVC.mp4") == "Brazzers Exxtra Mia Malkova"
assert clean_stash_filename("EvilAngel - Hardcore Scene Title [1080p].mkv") == "EvilAngel - Hardcore Scene Title"
assert clean_stash_filename("PureTaboo.Alexis.Texas.720p.mp4") == "PureTaboo Alexis Texas"
print("✓ clean_stash_filename passed!", flush=True)

print("--- Test 2: Stream Formatter & SVG Poster ---", flush=True)
badge, desc = format_stashdb_stream("Scene Title (2024)", {"resolution": "1080p", "codec": "HEVC", "size_str": "1.5 GB"})
assert "[StashDB ⚡] 1080p HEVC" in badge
assert "🔞 Scene Title (2024)" in desc
assert "📦 1.5 GB" in desc
svg = generate_adult_poster_svg("Alexis Texas Scene")
assert b"<svg" in svg
assert b"STASHDB VAULT" in svg
assert b"Alexis Texas Scene" in svg
print("✓ format_stashdb_stream & generate_adult_poster_svg passed!", flush=True)

print("--- Test 3: Stremio ID Parsing ---", flush=True)
id_type, id_val, s, e = parse_stremio_id("stash_01a1b034-095c-7781-b0b7-ac3519d83112")
assert id_type == "stash" and id_val == "stash_01a1b034-095c-7781-b0b7-ac3519d83112"
id_type, id_val, s, e = parse_stremio_id("stash:01a1b034-095c-7781-b0b7-ac3519d83112")
assert id_type == "stash" and id_val == "stash_01a1b034-095c-7781-b0b7-ac3519d83112"
id_type, id_val, s, e = parse_stremio_id("tmdb:1032906:1:2")
assert id_type == "tmdb" and id_val == 1032906 and s == 1 and e == 2
id_type, id_val, s, e = parse_stremio_id("tt1234567")
assert id_type == "imdb" and id_val == "tt1234567"
print("✓ parse_stremio_id passed!", flush=True)

print("--- Test 4: Database Schema & StashDB Column ---", flush=True)
con = sqlite3.connect("data/library.db")
cols = [c[1] for c in con.execute("PRAGMA table_info(videos)").fetchall()]
assert "stash_id" in cols, f"stash_id not in columns: {cols}"

# Insert mock adult video into SQLite
con.execute("""
    INSERT OR REPLACE INTO videos (
        chat_id, message_id, file_name, file_size, mime_type,
        stash_id, media_type, title, year, poster, backdrop, overview, genres, indexed_at
    ) VALUES (
        -100888999, 501, 'Brazzers.Sample.1080p.mp4', 1800000000, 'video/mp4',
        'stash_test_uuid_501', 'porn', 'Brazzers Sample Scene', 2024,
        'https://images.stashdb.org/poster.jpg', 'https://images.stashdb.org/bg.jpg',
        'Sample adult scene description', 'Anal,Brazzers', 1700000000
    )
""")
con.commit()

# Verify query for porn catalog
row = con.execute("SELECT stash_id, media_type, title FROM videos WHERE media_type = 'porn' AND stash_id = 'stash_test_uuid_501'").fetchone()
assert row is not None
assert row[0] == "stash_test_uuid_501"
assert row[1] == "porn"
assert row[2] == "Brazzers Sample Scene"
con.close()
print("✓ Database adult record inserted and queried with stash_id successfully!", flush=True)

print("--- Test 5: Settings Manager StashDB Key ---", flush=True)
s = save_settings({"stashdb_api_key": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.dummy_test_key"})
loaded = load_settings()
assert loaded["stashdb_api_key"] == "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.dummy_test_key"
print("✓ settings_manager stashdb_api_key persistence passed!", flush=True)

print("\n==============================================")
print("🎯 ALL 5 STASHDB CORE INTEGRATION TESTS PASSED!")
print("==============================================")
