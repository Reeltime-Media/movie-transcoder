import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel

from transcode_service.config import settings
from transcode_service import worker as worker_module

router = APIRouter(prefix="/jobs", tags=["jobs"])

_api_key_header = APIKeyHeader(name="X-Api-Key", auto_error=False)


def _require_api_key(key: str | None = Security(_api_key_header)) -> None:
    """Enforce API key if one is configured in settings."""
    if settings.api_key and key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )


class JobRead(BaseModel):
    id: uuid.UUID
    content_id: uuid.UUID
    source_key: str
    status: str
    attempts: int
    error: str | None
    started_at: datetime | None
    finished_at: datetime | None
    created_at: datetime


def _pool():
    if worker_module.pool is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Worker pool not ready",
        )
    return worker_module.pool


@router.get("", response_model=list[JobRead], dependencies=[Depends(_require_api_key)])
async def list_jobs(job_status: str | None = None):
    """List transcode jobs, optionally filtered by status (queued/running/success/failed)."""
    pool = _pool()
    if job_status:
        rows = await pool.fetch(
            "SELECT * FROM transcode_jobs WHERE status = $1 ORDER BY created_at DESC",
            job_status,
        )
    else:
        rows = await pool.fetch(
            "SELECT * FROM transcode_jobs ORDER BY created_at DESC"
        )
    return [dict(r) for r in rows]


@router.get("/{job_id}", response_model=JobRead, dependencies=[Depends(_require_api_key)])
async def get_job(job_id: uuid.UUID):
    """Get a single transcode job by ID."""
    pool = _pool()
    row = await pool.fetchrow(
        "SELECT * FROM transcode_jobs WHERE id = $1", job_id
    )
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return dict(row)


@router.post("/{job_id}/retry", response_model=JobRead, dependencies=[Depends(_require_api_key)])
async def retry_job(job_id: uuid.UUID):
    """Re-queue a failed or stuck transcode job."""
    pool = _pool()
    row = await pool.fetchrow(
        "SELECT * FROM transcode_jobs WHERE id = $1", job_id
    )
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    updated = await pool.fetchrow(
        """
        UPDATE transcode_jobs
        SET status = 'queued', error = NULL, started_at = NULL, finished_at = NULL
        WHERE id = $1
        RETURNING *
        """,
        job_id,
    )
    return dict(updated)
