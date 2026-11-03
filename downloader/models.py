from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any

class DownloadStatus(str, Enum):
    DETECTED = "DETECTED"
    QUEUED = "QUEUED"
    VALIDATING = "VALIDATING"
    DOWNLOADING = "DOWNLOADING"
    PAUSED_NETWORK = "PAUSED_NETWORK"
    PAUSED_DISK = "PAUSED_DISK"
    VERIFYING = "VERIFYING"
    IDENTIFYING = "IDENTIFYING"
    ORGANIZING = "ORGANIZING"
    COMPLETED = "COMPLETED"
    RETRY_PENDING = "RETRY_PENDING"
    SOURCE_DELETED = "SOURCE_DELETED"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"

class NotificationStatus(str, Enum):
    NOT_REQUIRED = "NOT_REQUIRED"
    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"

@dataclass
class DownloadJob:
    id: Optional[int] = None
    channel_id: int = 0
    message_id: int = 0
    grouped_id: Optional[int] = None
    file_id: Optional[str] = None
    file_name: str = ""
    file_size: int = 0
    mime_type: str = ""
    message_date: int = 0
    file_reference: Optional[str] = None
    sender_id: Optional[int] = None
    sender_name: Optional[str] = None
    status: str = DownloadStatus.QUEUED.value
    queue_position: int = 0
    bytes_downloaded: int = 0
    download_speed: float = 0.0
    temp_file_path: Optional[str] = None
    final_file_path: Optional[str] = None
    sha256_hash: Optional[str] = None
    media_type: Optional[str] = None
    identified_title: Optional[str] = None
    identified_year: Optional[int] = None
    identified_season: Optional[int] = None
    identified_episode: Optional[int] = None
    identified_episode_title: Optional[str] = None
    tmdb_id: Optional[int] = None
    imdb_id: Optional[str] = None
    metadata_json: Optional[str] = None
    retry_count: int = 0
    last_error: Optional[str] = None
    last_error_time: Optional[int] = None
    next_retry_at: Optional[int] = None
    notification_status: str = NotificationStatus.NOT_REQUIRED.value
    notification_error: Optional[str] = None
    created_at: int = 0
    updated_at: int = 0
    completed_at: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
