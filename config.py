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

    # Direct asyncpg URL (db.*.supabase.co). Prefer pooler_database_url when set.
    # Not required when R2_SCAN_MODE=true.
    database_url: str = ""
    # Session pooler (IPv4, port 5432) — same as movie-api POOLER_DATABASE_URL /
    # TRANSCODE_DATABASE_URL. Cuts connection churn vs the direct host.
    pooler_database_url: str | None = None

    # R2-only mode: scan bucket for source.mp4, transcode to HLS, no Supabase writes.
    r2_scan_mode: bool = False
    # Seconds between claim attempts when the worker is free (not a full-bucket LIST).
    r2_scan_interval: int = 60
    # How often to refresh dashboard inventory (source discovery + HEAD checks).
    # Keep this high — inventory no longer needs to walk every HLS segment.
    r2_stats_interval: int = 1800
    # Cache TTL for delimiter-based source.mp4 discovery.
    r2_source_list_ttl_seconds: int = 1800
    # When pending is empty, stop claim/HEAD churn for this long.
    r2_idle_backoff_seconds: int = 300
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
    poll_interval: int = 12
    # Max concurrent jobs
    max_concurrent: int = 2
    # Keep the asyncpg pool small — Supabase session pooler has limited slots.
    db_pool_min_size: int = 1
    db_pool_max_size: int = 4
    # Recycle idle connections before the pooler drops them (seconds).
    db_pool_max_inactive_lifetime: int = 180

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
    # This worker's public base URL (e.g. http://35.240.137.149:8001). Also
    # used to build live channel HLS URLs — see live_manager.py.
    worker_public_url: str = ""
    # Comma-separated peer worker URLs for aggregated progress on /dashboard
    peer_worker_urls: str = ""

    # ── Live TV restream (separate from the VOD job queue above) ─────────────
    # Local directory where each channel's rolling HLS output is written and
    # served from (see main.py's /live static mount). Ephemeral by design —
    # live segments aren't meant to persist, so no volume mount is required.
    live_output_dir: str = "/tmp/live"
    # Shorter segments = faster join time on mobile (was 6).
    live_hls_segment_time: int = 3
    live_hls_list_size: int = 8
    # Cap encoded live height for faster mobile buffering (logo path re-encodes).
    live_max_height: int = 720
    # Burned-in watermark on live restreams (top-left). Empty = bundled asset
    # if present, otherwise remux-only with no logo.
    live_logo_path: str = ""
    live_logo_width: int = 96
    live_logo_margin: int = 24
    # Live overlay forces a video re-encode; keep this fast for low latency.
    live_x264_preset: str = "ultrafast"

    @property
    def effective_database_url(self) -> str:
        """Prefer session pooler when set (IPv4 + less direct-host churn)."""
        return (self.pooler_database_url or self.database_url).strip()

    @model_validator(mode="after")
    def _require_database_unless_r2_scan(self) -> "Settings":
        if not self.r2_scan_mode and not self.effective_database_url:
            raise ValueError(
                "DATABASE_URL or POOLER_DATABASE_URL is required unless R2_SCAN_MODE=true"
            )
        return self


settings = Settings()
