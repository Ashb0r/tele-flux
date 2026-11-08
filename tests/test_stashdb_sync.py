import os
import sys
import sqlite3

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from stashdb import (
    clean_stash_filename,
    format_stashdb_stream,
    generate_adult_poster_svg,
    select_media_images,
    extract_scene_title_and_date,
    extract_filename_date,
    normalize_scene_title
)
from database import parse_stremio_id, is_uuid_str
from settings_manager import load_settings, save_settings

print("--- Test 1: StashDB Filename Cleaning ---", flush=True)
assert clean_stash_filename("Brazzers.Exxtra.Mia.Malkova.1080p.HEVC.mp4") == "Brazzers Exxtra Mia Malkova"
assert clean_stash_filename("EvilAngel - Hardcore Scene Title [1080p].mkv") == "EvilAngel - Hardcore Scene Title"
assert clean_stash_filename("PureTaboo.Alexis.Texas.720p.mp4") == "PureTaboo Alexis Texas"
print("✓ clean_stash_filename passed!", flush=True)

print("--- Test 2: Stream Formatter & SVG Poster ---", flush=True)
badge, desc = format_stashdb_stream("Scene Title (2024)", {"resolution": "1080p", "codec": "HEVC", "size_str": "1.5 GB"})
assert "1080p" in badge and "HEVC" in badge
assert "🔞 Scene Title (2024)" in desc
assert "📦 1.5 GB" in desc
svg = generate_adult_poster_svg("Alexis Texas Scene")
assert b"<svg" in svg
assert b"TELEFLUX RED" in svg
assert b"Alexis Texas Scene" in svg
print("✓ format_stashdb_stream & generate_adult_poster_svg passed!", flush=True)

print("--- Test 3: Stremio ID Parsing & UUID Detection ---", flush=True)
uuid_sample = "01a1b034-095c-7781-b0b7-ac3519d83112"
id_type, id_val, s, e = parse_stremio_id(f"stash_{uuid_sample}")
assert id_type == "stash" and id_val == f"stash_{uuid_sample}"
id_type, id_val, s, e = parse_stremio_id(f"stash:{uuid_sample}")
assert id_type == "stash" and id_val == f"stash_{uuid_sample}"
id_type, id_val, s, e = parse_stremio_id(f"stashdb_{uuid_sample}")
assert id_type == "stash" and id_val == f"stash_{uuid_sample}"
id_type, id_val, s, e = parse_stremio_id(f"stashdb:{uuid_sample}")
assert id_type == "stash" and id_val == f"stash_{uuid_sample}"
id_type, id_val, s, e = parse_stremio_id(uuid_sample)
assert id_type == "stash" and id_val == f"stash_{uuid_sample}"
assert is_uuid_str(uuid_sample)
assert not is_uuid_str("tt1234567")
id_type, id_val, s, e = parse_stremio_id("tmdb:1032906:1:2")
assert id_type == "tmdb" and id_val == 1032906 and s == 1 and e == 2
id_type, id_val, s, e = parse_stremio_id("tt1234567")
assert id_type == "imdb" and id_val == "tt1234567"
print("✓ parse_stremio_id and UUID detection passed!", flush=True)

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
old_settings = load_settings()
old_key = old_settings.get("stashdb_api_key", "")
try:
    save_settings({"stashdb_api_key": "test_persistence_key"})
    loaded = load_settings()
    assert loaded["stashdb_api_key"] == "test_persistence_key"
finally:
    # Always restore original settings!
    save_settings({"stashdb_api_key": old_key})
print("✓ settings_manager stashdb_api_key persistence passed!", flush=True)

print("--- Test 6: Strict Filename Parsing & Stream Isolation ---", flush=True)
# 1. Parsing user-specified format EXACT_TITLE_OF_SCENE (YYYY-MM-DD).mp4
t1, d1 = extract_scene_title_and_date("Take A Seat On My Dick 2 (2018-11-15).mp4")
assert t1 == "Take A Seat On My Dick 2" and d1 == "2018-11-15"

t2, d2 = extract_scene_title_and_date("Take A Seat On My Dick (2017-05-04).mp4")
assert t2 == "Take A Seat On My Dick" and d2 == "2017-05-04"

t3, d3 = extract_scene_title_and_date("Syren DeMer's Second Appearance (2022-01-30).m2ts")
assert t3 == "Syren DeMer's Second Appearance" and d3 == "2022-01-30"

t4, d4 = extract_scene_title_and_date("A Bollywood Tail (2023-10-30).mp4")
assert t4 == "A Bollywood Tail" and d4 == "2023-10-30"

# 2. Strict distinction between Scene 1 and Scene 2
norm_1 = normalize_scene_title("Take A Seat On My Dick")
norm_2 = normalize_scene_title("Take A Seat On My Dick 2")
assert norm_1 == "take a seat on my dick"
assert norm_2 == "take a seat on my dick 2"
assert norm_1 != norm_2, "Scene 1 and Scene 2 must never equal each other!"

# 3. Database query isolation: ensure querying for Scene 1 never returns Scene 2
con = sqlite3.connect("data/library.db")
con.row_factory = sqlite3.Row

# Insert Part 2 with test UUID
part2_uuid = "22222222-2222-2222-2222-222222222222"
part1_uuid = "11111111-1111-1111-1111-111111111111"

con.execute("DELETE FROM videos WHERE message_id = 9991")
con.execute("""
    INSERT INTO videos (
        chat_id, message_id, file_name, file_size, mime_type,
        stash_id, media_type, title, year, poster, backdrop, overview, genres, indexed_at
    ) VALUES (
        -100888999, 9991, 'Take A Seat On My Dick 2 (2018-11-15).mp4', 1500000000, 'video/mp4',
        ?, 'porn', 'Take A Seat On My Dick 2', 2018,
        'https://images.stashdb.org/poster2.jpg', 'https://images.stashdb.org/bg2.jpg',
        'Overview for Part 2', 'Adult', 1700000000
    )
""", (f"stash_{part2_uuid}",))
con.commit()

# When querying for Part 1 by UUID:
part1_rows = con.execute("""
    SELECT * FROM videos 
    WHERE (stash_id = ? OR stash_id = ? OR stash_id = ?) AND media_type = 'porn'
""", (f"stash_{part1_uuid}", f"stash_{part1_uuid}", part1_uuid)).fetchall()
assert len(part1_rows) == 0, f"Expected 0 rows for Part 1, got {len(part1_rows)}"

# When querying for Part 2 by UUID:
part2_rows = con.execute("""
    SELECT * FROM videos 
    WHERE (stash_id = ? OR stash_id = ? OR stash_id = ?) AND media_type = 'porn'
""", (f"stash_{part2_uuid}", f"stash_{part2_uuid}", part2_uuid)).fetchall()
assert len(part2_rows) == 1, f"Expected 1 row for Part 2, got {len(part2_rows)}"
assert part2_rows[0]["title"] == "Take A Seat On My Dick 2"

# Verify that Part 2 was never considered an unlinked candidate:
unlinked = con.execute("""
    SELECT * FROM videos 
    WHERE media_type = 'porn' 
      AND (stash_id IS NULL OR stash_id = '' OR stash_id LIKE 'stash_local_%')
""").fetchall()
unlinked_ids = [r["id"] for r in unlinked]
assert part2_rows[0]["id"] not in unlinked_ids, "Part 2 must not be treated as unlinked candidate!"

# Clean up test row
con.execute("DELETE FROM videos WHERE message_id = 9991")
con.commit()

con.close()
print("✓ Strict filename parsing, title distinction, and database stream isolation passed!", flush=True)

print("\n==============================================")
print("🎯 ALL 6 STASHDB CORE INTEGRATION TESTS PASSED!")
print("==============================================")
