"""Transcode worker.

Flow per job:
  1. Claim a queued job (status → running, increment attempts).
  2. Download raw source from R2 to a temp file.
  3. Run ffmpeg to produce HLS with multiple renditions + master playlist.
  4. Upload all .ts segments and .m3u8 playlists to R2 under hls/<content_id>/.
  5. Update content (hls_master_key, transcode_status → ready) and job (status → success).
  6. On any error: mark job failed, set content.transcode_status = failed.
"""

import asyncio
import os
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import boto3
from botocore.config import Config

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


def _upload_dir(local_dir: Path, prefix: str) -> None:
    client = _r2()
    for path in local_dir.rglob("*"):
        if path.is_file():
            relative = path.relative_to(local_dir)
            r2_key = f"{prefix}/{relative}"
            content_type = "application/x-mpegURL" if path.suffix == ".m3u8" else "video/MP2T"
            client.upload_file(
                str(path),
                settings.r2_bucket_name,
                r2_key,
                ExtraArgs={"ContentType": content_type},
            )


# ── FFmpeg ────────────────────────────────────────────────────────────────────

async def _transcode(source: Path, out_dir: Path) -> None:
    """Produce per-rendition HLS streams and a master playlist."""
    out_dir.mkdir(parents=True, exist_ok=True)

    # Build filter_complex + output map for each rendition
    filter_parts: list[str] = []
    output_args: list[str] = []
    variant_streams: list[str] = []
    rendition_items = list(settings.renditions.items())
    for i, (label, _scale) in enumerate(rendition_items):
        filter_parts.append(f"[split{i}]")
        output_args += [
            f"-map", f"[out{i}]",
            f"-map", "a:0",
            f"-c:v:{i}", "h264_videotoolbox",
            f"-b:v:{i}", _bitrate(label),
            f"-c:a:{i}", "aac",
            f"-b:a:{i}", "128k",
        ]
        variant_streams.append(f"v:{i},a:{i},name:{label}")

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

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg failed:\n{stderr.decode()}")

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
        await _transcode(source_path, out_dir)

        # 3. Upload HLS output
        hls_prefix = f"hls/{content_id}"
        await asyncio.get_event_loop().run_in_executor(None, _upload_dir, out_dir, hls_prefix)

        hls_master_key = f"{hls_prefix}/master.m3u8"

        # 4. Update DB
        await _mark_success(conn, job_id, content_id, hls_master_key)
        print(f"[transcode] job {job_id} → success")

    except Exception as exc:
        error_msg = str(exc)
        print(f"[transcode] job {job_id} → failed: {error_msg}")
        await _mark_failed(conn, job_id, content_id, error_msg)

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


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
    )
    semaphore = asyncio.Semaphore(settings.max_concurrent)

    print("[transcode] worker started")

    async def handle(job):
        async with semaphore:
            async with pool.acquire() as conn:
                await process_job(conn, job)

    try:
        while True:
            async with pool.acquire() as conn:
                job = await _claim_job(conn)

            if job:
                asyncio.create_task(handle(job))
            else:
                await asyncio.sleep(settings.poll_interval)
    except asyncio.CancelledError:
        print("[transcode] worker shutting down")
    finally:
        await pool.close()
        pool = None
