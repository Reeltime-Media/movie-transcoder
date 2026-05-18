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

    # Direct asyncpg connection (no pooler) for long-running worker
    database_url: str  # postgresql+asyncpg://...

    # Cloudflare R2
    r2_account_id: str
    r2_access_key_id: str
    r2_secret_access_key: str
    r2_bucket_name: str
    r2_public_url: str

    # FFmpeg
    ffmpeg_path: str = "ffmpeg"
    hls_segment_time: int = 6
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


settings = Settings()
