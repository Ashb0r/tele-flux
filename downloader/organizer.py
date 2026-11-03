import os
import json
import shutil
import asyncio
from typing import Optional, Dict, Any, Tuple
from tmdb import lookup_tmdb, parse_filename, extract_media_specs
from downloader.models import DownloadStatus, DownloadJob
from downloader.engine import sanitize_filename, calculate_sha256
from downloader.database import (
    update_job_status,
    update_job_metadata,
    commit_job_completion
)

def sanitize_path_segment(segment: str) -> str:
    """Sanitizes directory and file names for filesystem safety."""
    clean = segment.strip()
    for ch in r'<>:"/\|?*':
        clean = clean.replace(ch, "_")
    while "__" in clean:
        clean = clean.replace("__", "_")
    return clean.strip(" .") or "Unknown"

class MediaOrganizer:
    def __init__(self, media_dir: str):
        self.media_dir = media_dir

    def _get_extension(self, filename: str) -> str:
        base, ext = os.path.splitext(filename)
        return ext.lower() if ext else ".mp4"

    async def identify_metadata(self, job: DownloadJob, auto_tmdb: bool = True) -> Dict[str, Any]:
        """
        Parses filename and queries TMDB with rigorous validation.
        Returns a dict of resolved metadata fields.
        """
        await update_job_status(job.id, DownloadStatus.IDENTIFYING.value)

        # 1. Parse filename tokens
        parsed = parse_filename(file_name=job.file_name)
        media_type = parsed.get("media_type") or "movie"
        raw_title = parsed.get("raw_title") or job.file_name
        year = parsed.get("year")
        season = parsed.get("season")
        episode = parsed.get("episode")
        ep_title = parsed.get("episode_title")

        tmdb_info = None
        if auto_tmdb:
            try:
                # Query TMDB using parsed information
                tmdb_info = await lookup_tmdb(job.file_name, parsed_override=parsed)
            except Exception as e:
                print(f"[Media Organizer] TMDB lookup error for #{job.id} '{job.file_name}': {e}")
                # Don't fail the download; allow retry or fallback
                raise

        meta_result: Dict[str, Any] = {
            "media_type": media_type,
            "title": raw_title,
            "year": year,
            "season": season,
            "episode": episode,
            "episode_title": ep_title,
            "tmdb_id": None,
            "imdb_id": None,
            "poster": None,
            "backdrop": None,
            "overview": None,
            "rating": None,
            "genres": [],
            "is_confident": False,
            "raw_tmdb": None
        }

        # 2. Validate identification confidence
        if tmdb_info and tmdb_info.get("tmdb_id"):
            # Check if it was a real TMDB hit rather than a fallback SVG
            poster = tmdb_info.get("poster") or ""
            is_real_hit = bool(poster and "/poster/" not in poster)

            if is_real_hit:
                # TMDB hit is verified
                meta_result["media_type"] = tmdb_info.get("media_type") or media_type
                meta_result["title"] = tmdb_info.get("title") or raw_title
                meta_result["year"] = tmdb_info.get("year") or year
                meta_result["season"] = tmdb_info.get("season") or season
                meta_result["episode"] = tmdb_info.get("episode") or episode
                meta_result["episode_title"] = tmdb_info.get("episode_title") or ep_title
                meta_result["tmdb_id"] = tmdb_info.get("tmdb_id")
                meta_result["imdb_id"] = tmdb_info.get("imdb_id")
                meta_result["poster"] = tmdb_info.get("poster")
                meta_result["backdrop"] = tmdb_info.get("backdrop")
                meta_result["overview"] = tmdb_info.get("overview")
                meta_result["rating"] = tmdb_info.get("rating")
                meta_result["genres"] = tmdb_info.get("genres") or []
                meta_result["is_confident"] = True
                meta_result["raw_tmdb"] = tmdb_info
            else:
                # Fallback / low-confidence identification
                meta_result["is_confident"] = False
                meta_result["raw_tmdb"] = tmdb_info

        # Persist identification result in database
        await update_job_metadata(
            job_id=job.id,
            media_type=meta_result["media_type"],
            identified_title=meta_result["title"],
            identified_year=meta_result["year"],
            identified_season=meta_result["season"],
            identified_episode=meta_result["episode"],
            identified_episode_title=meta_result["episode_title"],
            tmdb_id=meta_result["tmdb_id"],
            imdb_id=meta_result["imdb_id"],
            metadata_json=json.dumps(meta_result)
        )

        return meta_result

    def compute_destination_path(self, job: DownloadJob, metadata: Dict[str, Any], auto_organize: bool = True) -> str:
        """
        Determines the destination path according to media library folder conventions:
          - Movies/Movie Name (Year)/Movie Name (Year).ext
          - TV Shows/Series Name/Season 01/Series Name - S01E01.ext
          - Unsorted/Original Filename.ext
        """
        ext = self._get_extension(job.file_name)

        if not auto_organize or not metadata.get("is_confident"):
            # Move to Unsorted
            safe_orig = sanitize_filename(job.file_name)
            return os.path.join(self.media_dir, "Unsorted", safe_orig)

        m_type = metadata.get("media_type")
        title = sanitize_path_segment(metadata.get("title") or "Unknown")
        year = metadata.get("year")

        if m_type == "series":
            season = metadata.get("season") or 1
            episode = metadata.get("episode") or 1
            ep_title = metadata.get("episode_title")

            season_folder = f"Season {season:02d}"
            if ep_title:
                safe_ep_title = sanitize_path_segment(ep_title)
                filename = f"{title} - S{season:02d}E{episode:02d} - {safe_ep_title}{ext}"
            else:
                filename = f"{title} - S{season:02d}E{episode:02d}{ext}"

            return os.path.join(self.media_dir, "TV Shows", title, season_folder, filename)

        else:
            # Movie
            folder_name = f"{title} ({year})" if year else title
            filename = f"{folder_name}{ext}"
            return os.path.join(self.media_dir, "Movies", folder_name, filename)

    async def commit_and_organize(
        self,
        job: DownloadJob,
        part_path: str,
        metadata: Dict[str, Any],
        auto_organize: bool = True
    ) -> str:
        """
        Atomically commits and moves the verified temporary file into the final media structure.
        Handles destination collision and prevents overwriting existing files.
        """
        await update_job_status(job.id, DownloadStatus.ORGANIZING.value)

        dest_path = self.compute_destination_path(job, metadata, auto_organize)
        dest_dir = os.path.dirname(dest_path)
        os.makedirs(dest_dir, exist_ok=True)

        final_path = dest_path

        # Check for destination collision
        if os.path.exists(dest_path):
            existing_size = os.path.getsize(dest_path)
            # If same size, verify SHA-256
            if existing_size == job.file_size:
                existing_sha = calculate_sha256(dest_path)
                if job.sha256_hash and existing_sha == job.sha256_hash:
                    print(f"[Media Organizer] Exact duplicate detected at {dest_path}. Removing temp .part and linking...")
                    try:
                        if os.path.exists(part_path):
                            os.remove(part_path)
                    except Exception as e:
                        print(f"[Media Organizer] Temp file cleanup notice: {e}")
                    await commit_job_completion(job.id, dest_path)
                    return dest_path

            # Different content or duplicate release: find non-colliding destination
            base_no_ext, ext = os.path.splitext(dest_path)
            counter = 1
            while os.path.exists(final_path):
                final_path = f"{base_no_ext} ({counter}){ext}"
                counter += 1
            print(f"[Media Organizer] Destination collision resolved: {dest_path} -> {final_path}")

        # Atomic filesystem rename (both part_path and final_path live inside media_dir)
        print(f"[Media Organizer] Atomically moving #{job.id} to final library destination: {final_path}")
        os.replace(part_path, final_path)

        # Sync directory metadata if supported by OS
        try:
            if hasattr(os, "sync"):
                os.sync()
        except Exception:
            pass

        # Update database to COMPLETED
        await commit_job_completion(job.id, final_path)
        print(f"[Media Organizer] Job #{job.id} committed successfully as COMPLETED.")
        return final_path
