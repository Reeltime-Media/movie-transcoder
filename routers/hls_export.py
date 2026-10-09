"""Remux the highest HLS variant into an MP4 and upload as source.mp4."""

from __future__ import annotations

import asyncio
import re
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from fastapi import APIRouter, Depends, HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, Field

from transcode_service.config import settings
from transcode_service import worker as worker_module

router = APIRouter(prefix="/hls-export", tags=["hls-export"])

_api_key_header = APIKeyHeader(name="X-Api-Key", auto_error=False)
_BANDWIDTH_RE = re.compile(r"BANDWIDTH=(\d+)")
_TIME_RE = re.compile(r"time=(\d+):(\d+):([\d.]+)")

# In-memory export jobs (single worker is fine for admin one-offs).
_exports: dict[str, dict[str, Any]] = {}
_dest_in_flight: dict[str, str] = {}  # dest_source_key -> export_id
_lock = asyncio.Lock()


def _require_api_key(key: str | None = Security(_api_key_header)) -> None:
    if settings.api_key and key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )


class HlsExportRequest(BaseModel):
    hls_master_key: str = Field(min_length=1)
    dest_source_key: str = Field(min_length=1)
    export_id: str | None = None


class HlsExportStartResponse(BaseModel):
    export_id: str
    status: str
    dest_source_key: str


class HlsExportStatusResponse(BaseModel):
    export_id: str
    status: str
    progress: int = 0
    error: str | None = None
    dest_source_key: str
    started_at: str | None = None
    finished_at: str | None = None


def _public_object_url(key: str) -> str:
    base = (settings.r2_public_url or "").strip().rstrip("/")
    if not base:
        raise RuntimeError("R2_PUBLIC_URL is not configured")
    return f"{base}/{key.lstrip('/')}"


def _stream_inf_bandwidth(inf_line: str) -> int:
    match = _BANDWIDTH_RE.search(inf_line)
    return int(match.group(1)) if match else 0


def _is_uri_line(line: str) -> bool:
    stripped = line.strip()
    return bool(stripped) and not stripped.startswith("#")


def pick_highest_variant_uri(master_text: str) -> str | None:
    """Return the URI of the highest-BANDWIDTH STREAM-INF, or None if media playlist."""
    lines = master_text.splitlines()
    variants: list[tuple[int, str]] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("#EXT-X-STREAM-INF"):
            inf = line
            i += 1
            uri = ""
            while i < len(lines):
                candidate = lines[i]
                i += 1
                if _is_uri_line(candidate):
                    uri = candidate.strip()
                    break
            if uri:
                variants.append((_stream_inf_bandwidth(inf), uri))
            continue
        i += 1
    if not variants:
        return None
    variants.sort(key=lambda item: item[0], reverse=True)
    return variants[0][1]


def resolve_variant_key(hls_master_key: str, variant_uri: str) -> str:
    if variant_uri.startswith("http://") or variant_uri.startswith("https://"):
        raise ValueError("Absolute variant URLs are not supported")
    if variant_uri.startswith("/"):
        raise ValueError("Root-absolute variant URIs are not supported")
    prefix = str(Path(hls_master_key).parent).replace("\\", "/")
    if prefix == ".":
        prefix = ""
    joined = urljoin(f"{prefix}/", variant_uri)
    return joined.lstrip("/")


def _r2():
    return worker_module._r2()


def _get_object_text(key: str) -> str:
    obj = _r2().get_object(Bucket=settings.r2_bucket_name, Key=key)
    body = obj["Body"].read()
    return body.decode("utf-8", errors="replace")


async def _run_ffmpeg_remux(export_id: str, input_url: str, out_path: Path) -> None:
    # Probe duration for progress (best-effort).
    duration = 0.0
    try:
        _, duration, _, _ = await worker_module._probe(input_url)
    except Exception:
        duration = 0.0

    cmd = [
        settings.ffmpeg_path,
        "-y",
        *worker_module._RECONNECT_ARGS,
        "-i",
        input_url,
        "-c",
        "copy",
        "-bsf:a",
        "aac_adtstoasc",
        "-movflags",
        "+faststart",
        str(out_path),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    worker_module._job_procs[export_id] = proc
    stderr_lines: list[str] = []
    assert proc.stderr is not None
    async for raw in proc.stderr:
        line = raw.decode(errors="replace").rstrip()
        stderr_lines.append(line)
        if duration > 0:
            m = _TIME_RE.search(line)
            if m:
                secs = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))
                _exports[export_id]["progress"] = min(99, int(secs / duration * 100))
    worker_module._job_procs.pop(export_id, None)
    await proc.wait()
    if proc.returncode != 0:
        raise RuntimeError("ffmpeg remux failed:\n" + "\n".join(stderr_lines[-60:]))


async def _export_worker(export_id: str) -> None:
    job = _exports[export_id]
    dest_key = job["dest_source_key"]
    master_key = job["hls_master_key"]
    try:
        job["status"] = "running"
        job["started_at"] = datetime.now(timezone.utc).isoformat()
        job["progress"] = 1

        master_text = await asyncio.to_thread(_get_object_text, master_key)
        variant_uri = pick_highest_variant_uri(master_text)
        if variant_uri:
            variant_key = resolve_variant_key(master_key, variant_uri)
        else:
            # Media playlist (single rendition) — remux the master itself.
            variant_key = master_key

        input_url = _public_object_url(variant_key)
        with tempfile.TemporaryDirectory(prefix="hls-export-") as tmp:
            out_path = Path(tmp) / "export.mp4"
            await _run_ffmpeg_remux(export_id, input_url, out_path)
            job["progress"] = 99

            def upload() -> None:
                _r2().upload_file(
                    str(out_path),
                    settings.r2_bucket_name,
                    dest_key,
                    ExtraArgs={"ContentType": "video/mp4"},
                )

            await asyncio.to_thread(upload)

        job["status"] = "success"
        job["progress"] = 100
        job["finished_at"] = datetime.now(timezone.utc).isoformat()
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = str(exc)[:2000]
        job["finished_at"] = datetime.now(timezone.utc).isoformat()
    finally:
        async with _lock:
            if _dest_in_flight.get(dest_key) == export_id:
                _dest_in_flight.pop(dest_key, None)


@router.post(
    "",
    response_model=HlsExportStartResponse,
    dependencies=[Depends(_require_api_key)],
)
async def start_hls_export(body: HlsExportRequest):
    master_key = body.hls_master_key.strip()
    dest_key = body.dest_source_key.strip()
    if not master_key.endswith(".m3u8"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="hls_master_key must be an .m3u8 playlist",
        )
    if not dest_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="dest_source_key is required",
        )

    async with _lock:
        existing_id = _dest_in_flight.get(dest_key)
        if existing_id and _exports.get(existing_id, {}).get("status") in ("queued", "running"):
            existing = _exports[existing_id]
            return HlsExportStartResponse(
                export_id=existing_id,
                status=str(existing["status"]),
                dest_source_key=dest_key,
            )

        export_id = (body.export_id or "").strip() or str(uuid.uuid4())
        _exports[export_id] = {
            "export_id": export_id,
            "status": "queued",
            "progress": 0,
            "error": None,
            "dest_source_key": dest_key,
            "hls_master_key": master_key,
            "started_at": None,
            "finished_at": None,
        }
        _dest_in_flight[dest_key] = export_id

    asyncio.create_task(_export_worker(export_id))
    return HlsExportStartResponse(
        export_id=export_id,
        status="queued",
        dest_source_key=dest_key,
    )


@router.get(
    "/{export_id}",
    response_model=HlsExportStatusResponse,
    dependencies=[Depends(_require_api_key)],
)
async def get_hls_export(export_id: str):
    job = _exports.get(export_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Export job not found",
        )
    return HlsExportStatusResponse(
        export_id=export_id,
        status=str(job["status"]),
        progress=int(job.get("progress") or 0),
        error=job.get("error"),
        dest_source_key=str(job["dest_source_key"]),
        started_at=job.get("started_at"),
        finished_at=job.get("finished_at"),
    )
