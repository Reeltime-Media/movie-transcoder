from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # App
    app_name: str = "Transcode Service"
    api_key: str = ""  # If set, required as X-Api-Key header on all job endpoints
    debug: bool = False
    secret_key: str = ""
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 1440

    # Direct asyncpg connection (not pooler) for long-running worker.
    # Not required when R2_SCAN_MODE=true.
    database_url: str = ""

    # R2-only mode: scan bucket for source.mp4, transcode to HLS, no Supabase writes.
    r2_scan_mode: bool = False
    # Seconds between R2 scans when no work is available.
    r2_scan_interval: int = 5
    # Reclaim a stale .transcode.lock after this many seconds.
    r2_lock_timeout_seconds: int = 7200

    # Cloudflare R2
    r2_account_id: str
    r2_access_key_id: str
    r2_secret_access_key: str
    r2_bucket_name: str
    r2_public_url: str

    # FFmpeg
    ffmpeg_path: str = "ffmpeg"
    ffprobe_path: str = "ffprobe"
    # "libx264" (CPU), "h264_nvenc", "h264_qsv", "h264_vaapi", etc.
    video_codec: str = "libx264"
    # Used for software x264 encodes (e.g. ultrafast, superfast, veryfast, faster)
    x264_preset: str = "veryfast"
    hls_segment_time: int = 6
    # Segment upload concurrency to R2
    r2_upload_concurrency: int = 12
    # Resolution label -> scale filter value
    renditions: dict[str, str] = {
        "1080p": "1920:1080",
        "720p": "1280:720",
        "480p": "854:480",
        "360p": "640:360",
    }

    # How long to sleep between polling loops (seconds)
    poll_interval: int = 5
    # Max concurrent jobs
    max_concurrent: int = 2

    # ── Retry / reliability ───────────────────────────────────────────────────
    # Max transcode attempts before a job is marked permanently failed.
    max_attempts: int = 3
    # Per-attempt linear backoff before a failed job is eligible to retry.
    # Effective delay before re-claim = retry_backoff_seconds * attempts.
    retry_backoff_seconds: int = 60
    # A job stuck in 'running' longer than this (e.g. the worker was killed
    # mid-transcode) is reclaimed by the reaper. MUST exceed your longest
    # expected transcode, or a live job could be reclaimed and run twice.
    running_timeout_seconds: int = 3600
    # How often the reaper scans for stuck 'running' jobs.
    reaper_interval: int = 120

    # Dashboard / multi-worker cluster
    worker_name: str = "transcode"
    # This worker's public base URL (e.g. http://35.240.137.149:8001)
    worker_public_url: str = ""
    # Comma-separated peer worker URLs for aggregated progress on /dashboard
    peer_worker_urls: str = ""

    @model_validator(mode="after")
    def _require_database_unless_r2_scan(self) -> "Settings":
        if not self.r2_scan_mode and not self.database_url.strip():
            raise ValueError("DATABASE_URL is required unless R2_SCAN_MODE=true")
        return self


settings = Settings()
