"""Transcode worker.

Flow per job:
  1. Claim a queued job (status → running, increment attempts).
  2. Download raw source from R2 to a temp file.
  3. Run ffmpeg to produce HLS with multiple renditions + master playlist.
  4. Upload HLS to R2 under movies/{slug}/hls/ or series/.../hls/ (legacy: hls/<content_id>/).
  5. Update content (hls_master_key, transcode_status → ready) and job (status → success).
  6. On any error: mark job failed, set content.transcode_status = failed.
"""

import asyncio
import concurrent.futures
import json
import re
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig

from transcode_service.config import settings


# ── R2 helpers ────────────────────────────────────────────────────────────────

def _r2():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{settings.r2_account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=settings.r2_access_key_id,
        aws_secret_access_key=settings.r2_secret_access_key,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def _download(key: str, dest: Path) -> None:
    _r2().download_file(settings.r2_bucket_name, key, str(dest))


def _hls_prefix_for_source(source_key: str, content_id: uuid.UUID) -> str:
    """Match movie-api app.services.r2_keys.hls_prefix_for_source_key."""
    movie_match = re.fullmatch(r"movies/([^/]+)/source\.mp4", source_key)
    if movie_match:
        return f"movies/{movie_match.group(1)}/hls"

    episode_match = re.fullmatch(
        r"series/([^/]+)/episodes/([^/]+)/source\.mp4",
        source_key,
    )
    if episode_match:
        return (
            f"series/{episode_match.group(1)}/episodes/{episode_match.group(2)}/hls"
        )

    return f"hls/{content_id}"


def _upload_dir(local_dir: Path, prefix: str) -> None:
    client = _r2()
    files = [path for path in local_dir.rglob("*") if path.is_file()]
    if not files:
        return

    transfer_config = TransferConfig(
        max_concurrency=max(1, settings.r2_upload_concurrency),
        use_threads=True,
    )

    def upload_one(path: Path) -> None:
        relative = path.relative_to(local_dir)
        r2_key = f"{prefix}/{relative}"
        content_type = "application/x-mpegURL" if path.suffix == ".m3u8" else "video/MP2T"
        client.upload_file(
            str(path),
            settings.r2_bucket_name,
            r2_key,
            ExtraArgs={"ContentType": content_type},
            Config=transfer_config,
        )

    workers = min(max(1, settings.r2_upload_concurrency), len(files))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(upload_one, path) for path in files]
        for future in concurrent.futures.as_completed(futures):
            future.result()


# ── In-memory job state (keyed by str(job_id)) ───────────────────────────────

job_progress: dict[str, int] = {}
_job_procs: dict[str, asyncio.subprocess.Process] = {}
_cancelled: set[str] = set()


def cancel_job(job_id: str) -> bool:
    """Kill the running ffmpeg process for job_id. Returns False if not running."""
    proc = _job_procs.get(job_id)
    if proc is None:
        return False
    _cancelled.add(job_id)
    proc.kill()
    return True


# ── FFmpeg ────────────────────────────────────────────────────────────────────

async def _probe(source: Path) -> tuple[bool, float]:
    """Return (has_audio, duration_seconds)."""
    proc = await asyncio.create_subprocess_exec(
        settings.ffprobe_path, "-v", "quiet",
        "-show_entries", "format=duration:stream=codec_type",
        "-of", "json", str(source),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await proc.communicate()
    try:
        data = json.loads(stdout)
        has_audio = any(s.get("codec_type") == "audio" for s in data.get("streams", []))
        duration = float(data.get("format", {}).get("duration", 0) or 0)
        return has_audio, duration
    except Exception:
        return False, 0.0


def _video_codec_args(index: int) -> list[str]:
    codec = settings.video_codec.strip().lower()
    args = [f"-c:v:{index}", codec]

    if codec == "libx264":
        args += [f"-preset:v:{index}", settings.x264_preset]
    elif codec == "h264_nvenc":
        args += [f"-preset:v:{index}", "p4"]

    return args


async def _transcode(source: Path, out_dir: Path, job_id: str) -> None:
    """Produce per-rendition HLS streams and a master playlist."""
    out_dir.mkdir(parents=True, exist_ok=True)

    has_audio, duration = await _probe(source)

    # Build filter_complex + output map for each rendition
    filter_parts: list[str] = []
    output_args: list[str] = []
    variant_streams: list[str] = []
    rendition_items = list(settings.renditions.items())
    for i, (label, _scale) in enumerate(rendition_items):
        filter_parts.append(f"[split{i}]")
        output_args += [
            f"-map", f"[out{i}]",
            *_video_codec_args(i),
            f"-b:v:{i}", _bitrate(label),
        ]
        if has_audio:
            output_args += [
                f"-map", "a:0",
                f"-c:a:{i}", "aac",
                f"-b:a:{i}", "128k",
            ]
        variant_streams.append(
            f"v:{i},a:{i},name:{label}" if has_audio else f"v:{i},name:{label}"
        )

    splits = "".join(filter_parts)
    filter_complex = f"[0:v]split={len(rendition_items)}{splits};" + ";".join(
        f"[split{i}]scale={scale}[out{i}]"
        for i, (_label, scale) in enumerate(rendition_items)
    )

    cmd = [
        settings.ffmpeg_path,
        "-threads", "0",
        "-i", str(source),
        "-filter_complex", filter_complex,
        *output_args,
        "-var_stream_map", " ".join(variant_streams),
        "-hls_time", str(settings.hls_segment_time),
        "-hls_playlist_type", "vod",
        "-hls_segment_filename", str(out_dir / "%v_%03d.ts"),
        str(out_dir / "%v.m3u8"),
    ]

    job_progress[job_id] = 0

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _job_procs[job_id] = proc

    stderr_lines: list[str] = []
    assert proc.stderr is not None
    async for raw in proc.stderr:
        line = raw.decode(errors="replace").rstrip()
        stderr_lines.append(line)
        if duration > 0:
            m = re.search(r"time=(\d+):(\d+):([\d.]+)", line)
            if m:
                secs = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))
                job_progress[job_id] = min(99, int(secs / duration * 100))

    _job_procs.pop(job_id, None)
    await proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg failed:\n" + "\n".join(stderr_lines[-60:]))

    # Write master playlist
    master_lines = ["#EXTM3U", "#EXT-X-VERSION:3"]
    for i, (label, scale) in enumerate(rendition_items):
        w, h = scale.split(":")
        master_lines.append(
            f'#EXT-X-STREAM-INF:BANDWIDTH={_bandwidth(label)},RESOLUTION={w}x{h}'
        )
        master_lines.append(f"{label}.m3u8")

    (out_dir / "master.m3u8").write_text("\n".join(master_lines))


def _bandwidth(label: str) -> int:
    return {"1080p": 5_000_000, "720p": 3_000_000, "480p": 1_500_000, "360p": 800_000}.get(label, 2_000_000)


def _bitrate(label: str) -> str:
    return {"1080p": "5000k", "720p": "3000k", "480p": "1500k", "360p": "800k"}.get(label, "2000k")


# ── DB helpers (asyncpg direct) ───────────────────────────────────────────────

async def _claim_job(conn) -> dict | None:
    """Atomically claim the oldest queued job."""
    row = await conn.fetchrow("""
        UPDATE transcode_jobs
        SET status = 'running',
            attempts = attempts + 1,
            started_at = now()
        WHERE id = (
            SELECT id FROM transcode_jobs
            WHERE status = 'queued'
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, content_id, source_key, attempts
    """)
    return dict(row) if row else None


async def _mark_success(conn, job_id: uuid.UUID, content_id: uuid.UUID, hls_master_key: str) -> None:
    await conn.execute("""
        UPDATE transcode_jobs
        SET status = 'success', finished_at = now(), error = NULL
        WHERE id = $1
    """, job_id)
    await conn.execute("""
        UPDATE content
        SET hls_master_key = $1,
            transcode_status = 'ready',
            updated_at = now()
        WHERE id = $2
    """, hls_master_key, content_id)


async def _mark_failed(conn, job_id: uuid.UUID, content_id: uuid.UUID, error: str) -> None:
    await conn.execute("""
        UPDATE transcode_jobs
        SET status = 'failed', finished_at = now(), error = $2
        WHERE id = $1
    """, job_id, error)
    await conn.execute("""
        UPDATE content
        SET transcode_status = 'failed', updated_at = now()
        WHERE id = $1
    """, content_id)


# ── Main job handler ──────────────────────────────────────────────────────────

async def process_job(conn, job: dict) -> None:
    job_id: uuid.UUID = job["id"]
    content_id: uuid.UUID = job["content_id"]
    source_key: str = job["source_key"]

    tmpdir = tempfile.mkdtemp()
    try:
        source_path = Path(tmpdir) / "source.mp4"
        out_dir = Path(tmpdir) / "hls"

        # 1. Download
        await asyncio.get_event_loop().run_in_executor(None, _download, source_key, source_path)

        # 2. Transcode
        await _transcode(source_path, out_dir, str(job_id))
        job_progress[str(job_id)] = 100

        # 3. Upload HLS output (path derived from source_key layout)
        hls_prefix = _hls_prefix_for_source(source_key, content_id)
        await asyncio.get_event_loop().run_in_executor(None, _upload_dir, out_dir, hls_prefix)

        hls_master_key = f"{hls_prefix}/master.m3u8"

        # 4. Update DB
        await _mark_success(conn, job_id, content_id, hls_master_key)
        print(f"[transcode] job {job_id} → success")

    except Exception as exc:
        jid = str(job_id)
        if jid in _cancelled:
            error_msg = "Cancelled by admin"
            _cancelled.discard(jid)
        else:
            error_msg = str(exc)
        print(f"[transcode] job {job_id} → failed: {error_msg}")
        await _mark_failed(conn, job_id, content_id, error_msg)

    finally:
        jid = str(job_id)
        shutil.rmtree(tmpdir, ignore_errors=True)
        job_progress.pop(jid, None)
        _job_procs.pop(jid, None)


# ── Module-level pool (set by run_worker, used by HTTP handlers) ──────────────

pool: asyncpg.Pool | None = None


# ── Polling loop ──────────────────────────────────────────────────────────────

async def run_worker() -> None:
    global pool
    # asyncpg uses the raw postgresql:// URL (strip the +asyncpg driver prefix)
    dsn = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
    pool = await asyncpg.create_pool(
        dsn,
        min_size=2,
        max_size=settings.max_concurrent + 1,
        statement_cache_size=0,
        # Recycle idle connections every 5 min so the DB server never drops them first
        max_inactive_connection_lifetime=300,
    )
    semaphore = asyncio.Semaphore(settings.max_concurrent)

    print("[transcode] worker started")

    async def handle(job):
        async with semaphore:
            async with pool.acquire() as conn:
                await process_job(conn, job)

    try:
        while True:
            try:
                async with pool.acquire() as conn:
                    job = await _claim_job(conn)
            except (
                asyncpg.ConnectionDoesNotExistError,
                asyncpg.InterfaceError,
                asyncpg.TooManyConnectionsError,
                OSError,
            ) as exc:
                print(f"[transcode] DB connection error, retrying in 5 s: {exc}")
                await asyncio.sleep(5)
                continue

            if job:
                asyncio.create_task(handle(job))
            else:
                await asyncio.sleep(settings.poll_interval)
    except asyncio.CancelledError:
        print("[transcode] worker shutting down")
    finally:
        await pool.close()
        pool = None
