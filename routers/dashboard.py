"""Simple transcode progress dashboard (HTML + JSON API)."""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime

from pydantic import BaseModel

import httpx
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import HTMLResponse

from transcode_service.config import settings
from transcode_service import r2_scan
from transcode_service import worker as worker_module

router = APIRouter(tags=["dashboard"])


def _peer_urls() -> list[str]:
    raw = settings.peer_worker_urls.strip()
    if not raw:
        return []
    return [url.strip().rstrip("/") for url in raw.split(",") if url.strip()]


def _all_worker_urls() -> list[tuple[str, str]]:
    """Return (label, base_url) for this worker and configured peers."""
    self_url = settings.worker_public_url.strip().rstrip("/")
    workers: list[tuple[str, str]] = []
    seen: set[str] = set()

    if self_url:
        label = settings.worker_name or "local"
        workers.append((label, self_url))
        seen.add(self_url)

    for url in _peer_urls():
        if url in seen:
            continue
        seen.add(url)
        host = url.removeprefix("http://").removeprefix("https://").split(":")[0]
        workers.append((host, url))

    if not workers:
        workers.append((settings.worker_name or "local", ""))
    return workers


async def _fetch_peer_progress(
    client: httpx.AsyncClient, label: str, base_url: str, *, timeout: float = 2.0
) -> dict:
    self_url = settings.worker_public_url.strip().rstrip("/")
    if not base_url or (self_url and base_url.rstrip("/") == self_url):
        return {
            "name": label,
            "url": base_url or None,
            "health": "ok",
            "progress": dict(worker_module.job_progress),
        }

    headers = {}
    if settings.api_key:
        headers["X-Api-Key"] = settings.api_key

    health = "unknown"
    progress: dict[str, int] = {}
    try:
        health_resp = await client.get(
            f"{base_url}/health", headers=headers, timeout=timeout
        )
        health = "ok" if health_resp.status_code == 200 else f"http_{health_resp.status_code}"
        prog_resp = await client.get(
            f"{base_url}/jobs/progress", headers=headers, timeout=timeout
        )
        if prog_resp.status_code == 200:
            progress = prog_resp.json()
    except Exception as exc:
        health = f"error: {exc.__class__.__name__}"

    return {"name": label, "url": base_url, "health": health, "progress": progress}


def _job_worker(
    job_id: str,
    job_status: str,
    db_worker: str | None,
    progress_by_job: dict[str, tuple[int, str]],
) -> str:
    if job_status == "running":
        live = progress_by_job.get(job_id)
        if live and live[1]:
            return live[1]
    if db_worker:
        return db_worker
    return "-"


def _title_from_source_key(source_key: str) -> str:
    if source_key in worker_module.r2_jobs:
        return worker_module.r2_jobs[source_key].get("title") or source_key
    parts = source_key.split("/")
    if len(parts) >= 2 and parts[-1] == "source.mp4":
        return parts[-2]
    return source_key


async def _dashboard_data_r2() -> dict:
    if not worker_module.worker_ready:
        return {"ready": False, "message": "R2 scan worker starting...", "r2_scan_mode": True}

    stats = r2_scan.get_dashboard_stats()
    counts = dict(stats.get("counts") or {})
    inv_pending = stats.get("pending") or []
    inv_failed = stats.get("failed") or []
    inv_in_progress = stats.get("in_progress") or []

    async with httpx.AsyncClient(timeout=httpx.Timeout(2.5)) as client:
        worker_snapshots = await asyncio.gather(
            *[
                _fetch_peer_progress(client, label, url, timeout=2.0)
                for label, url in _all_worker_urls()
            ]
        )

    progress_by_job: dict[str, tuple[int, str]] = {}
    for snap in worker_snapshots:
        for job_id, pct in snap.get("progress", {}).items():
            progress_by_job[job_id] = (int(pct), snap["name"])

    # Live running count from workers (more accurate than stale lock scan)
    live_running = len(progress_by_job)
    if live_running:
        counts["running"] = live_running

    for snap in worker_snapshots:
        snap["active_jobs"] = [
            {
                "id": job_id,
                "progress": int(pct),
                "title": _title_from_source_key(job_id),
            }
            for job_id, pct in snap.get("progress", {}).items()
        ]

    recent_jobs: list[dict] = []

    # Active jobs first (from live worker progress)
    seen: set[str] = set()
    for source_key, (pct, worker_name) in progress_by_job.items():
        seen.add(source_key)
        meta = worker_module.r2_jobs.get(source_key, {})
        recent_jobs.append(
            {
                "id": source_key,
                "title": meta.get("title") or _title_from_source_key(source_key),
                "source_key": source_key,
                "status": "running",
                "attempts": meta.get("attempts", 1),
                "error": None,
                "worker": worker_name,
                "progress": pct,
                "created_at": None,
                "finished_at": None,
            }
        )

    for source_key in inv_in_progress:
        if source_key in seen:
            continue
        seen.add(source_key)
        meta = worker_module.r2_jobs.get(source_key, {})
        recent_jobs.append(
            {
                "id": source_key,
                "title": meta.get("title") or _title_from_source_key(source_key),
                "source_key": source_key,
                "status": "running",
                "attempts": meta.get("attempts", 1),
                "error": None,
                "worker": meta.get("worker", "-"),
                "progress": 0,
                "created_at": None,
                "finished_at": None,
            }
        )

    for source_key in inv_failed[:30]:
        if source_key in seen:
            continue
        seen.add(source_key)
        meta = worker_module.r2_jobs.get(source_key, {})
        recent_jobs.append(
            {
                "id": source_key,
                "title": meta.get("title") or _title_from_source_key(source_key),
                "source_key": source_key,
                "status": "failed",
                "attempts": meta.get("attempts", 0),
                "error": meta.get("error"),
                "worker": meta.get("worker", "-"),
                "progress": 0,
                "created_at": None,
                "finished_at": None,
            }
        )

    for source_key in inv_pending[:20]:
        if source_key in seen:
            continue
        recent_jobs.append(
            {
                "id": source_key,
                "title": _title_from_source_key(source_key),
                "source_key": source_key,
                "status": "queued",
                "attempts": 0,
                "error": None,
                "worker": "-",
                "progress": 0,
                "created_at": None,
                "finished_at": None,
            }
        )

    running_jobs = [j for j in recent_jobs if j["status"] == "running"]
    total = counts.get("total") or 0
    done = counts.get("success") or 0
    stats_updated = stats.get("updated_at")
    return {
        "ready": True,
        "r2_scan_mode": True,
        "worker_name": settings.worker_name,
        "updated_at": datetime.now().astimezone().isoformat(),
        "stats_updated_at": stats_updated,
        "stats_refreshing": stats.get("refreshing", False),
        "counts": counts,
        "overall_percent": round(done / total * 100, 1) if total else 0,
        "workers": worker_snapshots,
        "running_jobs": running_jobs,
        "recent_jobs": recent_jobs,
    }


async def _dashboard_data() -> dict:
    pool = worker_module.pool
    if pool is None:
        return {"ready": False, "message": "Worker pool starting..."}

    async with pool.acquire() as conn:
        counts_rows = await conn.fetch(
            """
            SELECT status, COUNT(*)::int AS n
            FROM transcode_jobs
            GROUP BY status
            """
        )
        counts = {row["status"]: row["n"] for row in counts_rows}

        recent = await conn.fetch(
            """
            SELECT
                tj.id,
                tj.content_id,
                tj.source_key,
                tj.status,
                tj.attempts,
                tj.error,
                tj.worker_name,
                tj.started_at,
                tj.finished_at,
                tj.created_at,
                c.title,
                c.slug
            FROM transcode_jobs tj
            LEFT JOIN content c ON c.id = tj.content_id
            ORDER BY
                CASE tj.status
                    WHEN 'running' THEN 0
                    WHEN 'queued' THEN 1
                    WHEN 'failed' THEN 2
                    ELSE 3
                END,
                tj.created_at DESC
            LIMIT 100
            """
        )

    async with httpx.AsyncClient(timeout=httpx.Timeout(2.5)) as client:
        worker_snapshots = await asyncio.gather(
            *[
                _fetch_peer_progress(client, label, url, timeout=2.0)
                for label, url in _all_worker_urls()
            ]
        )

    progress_by_job: dict[str, tuple[int, str]] = {}
    for snap in worker_snapshots:
        for job_id, pct in snap.get("progress", {}).items():
            progress_by_job[job_id] = (int(pct), snap["name"])

    titles_by_id = {
        str(row["id"]): row["title"] or row["slug"] or row["source_key"]
        for row in recent
    }

    for snap in worker_snapshots:
        snap["active_jobs"] = [
            {
                "id": job_id,
                "progress": int(pct),
                "title": titles_by_id.get(job_id, job_id[:8]),
            }
            for job_id, pct in snap.get("progress", {}).items()
        ]

    running_jobs = []
    for row in recent:
        if row["status"] != "running":
            continue
        jid = str(row["id"])
        pct = progress_by_job.get(jid, (0, ""))[0]
        worker_name = _job_worker(jid, "running", row["worker_name"], progress_by_job)
        running_jobs.append(
            {
                "id": jid,
                "title": row["title"] or row["slug"] or row["source_key"],
                "source_key": row["source_key"],
                "progress": pct,
                "worker": worker_name,
                "attempts": row["attempts"],
                "started_at": row["started_at"].isoformat() if row["started_at"] else None,
            }
        )

    recent_jobs = []
    for row in recent:
        jid = str(row["id"])
        job_status = row["status"]
        recent_jobs.append(
            {
                "id": jid,
                "title": row["title"] or row["slug"] or row["source_key"],
                "source_key": row["source_key"],
                "status": job_status,
                "attempts": row["attempts"],
                "error": row["error"],
                "worker": _job_worker(jid, job_status, row["worker_name"], progress_by_job),
                "progress": progress_by_job.get(jid, (0, ""))[0]
                if job_status == "running"
                else (100 if job_status == "success" else 0),
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
            }
        )

    total = sum(counts.values())
    done = counts.get("success", 0)
    return {
        "ready": True,
        "worker_name": settings.worker_name,
        "updated_at": datetime.now().astimezone().isoformat(),
        "counts": {
            "queued": counts.get("queued", 0),
            "running": counts.get("running", 0),
            "success": counts.get("success", 0),
            "failed": counts.get("failed", 0),
            "total": total,
        },
        "overall_percent": round(done / total * 100, 1) if total else 0,
        "workers": worker_snapshots,
        "running_jobs": running_jobs,
        "recent_jobs": recent_jobs,
        "r2_scan_mode": False,
    }


class R2RetryBody(BaseModel):
    source_key: str


async def _retry_r2_source(source_key: str) -> dict:
    r2_scan.clear_failed_marker(source_key)
    r2_scan.release_lock(source_key)
    worker_module._r2_attempts.pop(source_key, None)
    worker_module.r2_jobs.pop(source_key, None)
    return {"source_key": source_key, "status": "queued", "retried": True}


async def _retry_job(job_id: uuid.UUID) -> dict:
    pool = worker_module.pool
    if pool is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Worker pool not ready",
        )

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, status FROM transcode_jobs WHERE id = $1", job_id
        )
        if not row:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

        updated = await conn.fetchrow(
            """
            UPDATE transcode_jobs
            SET status = 'queued', error = NULL, started_at = NULL, finished_at = NULL,
                attempts = 0, worker_name = NULL
            WHERE id = $1
            RETURNING id, status
            """,
            job_id,
        )
    return {"job_id": str(updated["id"]), "status": updated["status"], "retried": True}


@router.get("/api/dashboard")
async def dashboard_api():
    if settings.r2_scan_mode:
        return await _dashboard_data_r2()
    return await _dashboard_data()


@router.post("/api/dashboard/jobs/{job_id}/retry")
async def dashboard_retry_job(job_id: uuid.UUID):
    if settings.r2_scan_mode:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Use POST /api/dashboard/r2/retry in R2 scan mode",
        )
    return await _retry_job(job_id)


@router.post("/api/dashboard/r2/retry")
async def dashboard_r2_retry(body: R2RetryBody):
    if not settings.r2_scan_mode:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="R2 retry is only available in R2 scan mode",
        )
    return await _retry_r2_source(body.source_key)


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page():
    return HTMLResponse(DASHBOARD_HTML)


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Transcode Dashboard</title>
  <style>
    * { box-sizing: border-box; }
    body {
      margin: 0; font-family: ui-sans-serif, system-ui, sans-serif;
      background: #0f1115; color: #e8eaed;
    }
    header {
      padding: 1rem 1.25rem; border-bottom: 1px solid #2a2f3a;
      display: flex; justify-content: space-between; align-items: center;
    }
    h1 { margin: 0; font-size: 1.1rem; font-weight: 600; }
    .muted { color: #9aa0a6; font-size: 0.85rem; }
    main { padding: 1.25rem; max-width: 1280px; margin: 0 auto; }
    .cards {
      display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 0.75rem; margin-bottom: 1.25rem;
    }
    .card {
      background: #171a21; border: 1px solid #2a2f3a; border-radius: 10px;
      padding: 0.9rem 1rem;
    }
    .card .label { color: #9aa0a6; font-size: 0.8rem; }
    .card .value { font-size: 1.6rem; font-weight: 700; margin-top: 0.2rem; }
    .bar-wrap {
      background: #171a21; border: 1px solid #2a2f3a; border-radius: 10px;
      padding: 1rem; margin-bottom: 1.25rem;
    }
    .bar-bg {
      height: 12px; background: #2a2f3a; border-radius: 999px; overflow: hidden;
    }
    .bar-fill {
      height: 100%; background: linear-gradient(90deg, #3b82f6, #22c55e);
      width: 0%; transition: width 0.4s ease;
    }
    table {
      width: 100%; border-collapse: collapse; font-size: 0.88rem;
      background: #171a21; border: 1px solid #2a2f3a; border-radius: 10px;
      overflow: hidden;
    }
    th, td { padding: 0.65rem 0.75rem; text-align: left; border-bottom: 1px solid #232833; vertical-align: middle; }
    th { color: #9aa0a6; font-weight: 600; background: #141820; }
    tr:last-child td { border-bottom: none; }
    .pill {
      display: inline-block; padding: 0.15rem 0.5rem; border-radius: 999px;
      font-size: 0.75rem; font-weight: 600;
    }
    .queued { background: #3b3a1f; color: #facc15; }
    .running { background: #1e3a5f; color: #60a5fa; }
    .success { background: #14352a; color: #4ade80; }
    .failed { background: #3f1d24; color: #f87171; }
    .workers {
      display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 0.75rem; margin-bottom: 1.25rem;
    }
    .worker {
      background: #171a21; border: 1px solid #2a2f3a; border-radius: 10px;
      padding: 0.75rem 0.9rem; font-size: 0.82rem;
    }
    .worker.ok { border-color: #166534; }
    .worker.bad { border-color: #7f1d1d; }
    .worker h3 { margin: 0 0 0.35rem; font-size: 0.95rem; }
    .worker-job {
      margin-top: 0.45rem; padding-top: 0.45rem; border-top: 1px solid #2a2f3a;
      color: #cbd5e1;
    }
    .instance-tag {
      display: inline-block; background: #1e293b; color: #93c5fd;
      border-radius: 6px; padding: 0.1rem 0.45rem; font-size: 0.75rem; font-weight: 600;
    }
    .mini-bar {
      height: 6px; background: #2a2f3a; border-radius: 999px; margin-top: 0.35rem;
      overflow: hidden; width: 120px; display: inline-block; vertical-align: middle;
    }
    .mini-fill { height: 100%; background: #3b82f6; }
    .btn {
      background: #2563eb; color: #fff; border: none; border-radius: 6px;
      padding: 0.3rem 0.65rem; font-size: 0.78rem; cursor: pointer;
    }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }
    .btn:hover:not(:disabled) { background: #1d4ed8; }
    .error-text { color: #f87171; font-size: 0.75rem; margin-top: 0.2rem; }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Transcode Dashboard</h1>
      <div class="muted" id="updated">Loading...</div>
    </div>
    <div class="muted" id="worker-name"></div>
  </header>
  <main>
    <div class="cards">
      <div class="card"><div class="label">Queued</div><div class="value" id="c-queued">-</div></div>
      <div class="card"><div class="label">Running</div><div class="value" id="c-running">-</div></div>
      <div class="card"><div class="label">Success</div><div class="value" id="c-success">-</div></div>
      <div class="card"><div class="label">Failed</div><div class="value" id="c-failed">-</div></div>
      <div class="card"><div class="label">Total</div><div class="value" id="c-total">-</div></div>
    </div>

    <div class="bar-wrap">
      <div style="display:flex;justify-content:space-between;margin-bottom:0.5rem;">
        <strong>Overall progress</strong>
        <span id="overall-pct">0%</span>
      </div>
      <div class="bar-bg"><div class="bar-fill" id="overall-bar"></div></div>
    </div>

    <h2 style="font-size:1rem;margin:0 0 0.75rem;">Workers</h2>
    <div class="workers" id="workers"></div>

    <h2 style="font-size:1rem;margin:0 0 0.75rem;">Jobs</h2>
    <table>
      <thead>
        <tr>
          <th>Title / source</th>
          <th>Instance</th>
          <th>Status</th>
          <th>Progress</th>
          <th>Attempts</th>
          <th>Updated</th>
          <th></th>
        </tr>
      </thead>
      <tbody id="jobs-body"></tbody>
    </table>
  </main>
  <script>
    function pill(status) {
      return '<span class="pill ' + status + '">' + status + '</span>';
    }
    function progressCell(job) {
      if (job.status !== 'running') {
        return job.status === 'success' ? '100%' : '-';
      }
      const pct = job.progress || 0;
      return pct + '% <span class="mini-bar"><span class="mini-fill" style="width:' + pct + '%"></span></span>';
    }
    function fmtTime(iso) {
      if (!iso) return '-';
      return new Date(iso).toLocaleString();
    }
    function instanceCell(worker) {
      if (!worker || worker === '-') return '-';
      return '<span class="instance-tag">' + worker + '</span>';
    }
    async function retryJob(sourceKey, jobId, btn) {
      btn.disabled = true;
      const prev = btn.textContent;
      btn.textContent = 'Retrying...';
      try {
        let res;
        if (window.__r2ScanMode && sourceKey) {
          res = await fetch('/api/dashboard/r2/retry', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ source_key: sourceKey }),
          });
        } else {
          res = await fetch('/api/dashboard/jobs/' + jobId + '/retry', { method: 'POST' });
        }
        if (!res.ok) throw new Error('retry failed');
        await refresh();
      } catch (e) {
        btn.textContent = 'Failed';
        setTimeout(() => { btn.disabled = false; btn.textContent = prev; }, 1500);
        return;
      }
      btn.disabled = false;
      btn.textContent = prev;
    }
    async function refresh() {
      try {
        const res = await fetch('/api/dashboard', { signal: AbortSignal.timeout(10000) });
        if (!res.ok) throw new Error('http ' + res.status);
        const data = await res.json();
        if (!data.ready) {
          document.getElementById('updated').textContent = data.message || 'Starting...';
          return;
        }
        window.__r2ScanMode = !!data.r2_scan_mode;
        const modeLabel = data.r2_scan_mode ? 'R2 scan mode' : 'DB queue mode';
        let statsNote = '';
        if (data.stats_refreshing) statsNote = ' (refreshing bucket stats...)';
        else if (data.stats_updated_at) statsNote = ' | bucket stats: ' + new Date(data.stats_updated_at).toLocaleTimeString();
        document.getElementById('updated').textContent =
          'Live ' + new Date(data.updated_at).toLocaleTimeString() + ' (' + modeLabel + ')' + statsNote;
        document.getElementById('worker-name').textContent = 'Viewing from: ' + (data.worker_name || 'worker');
        document.getElementById('c-queued').textContent = data.counts.queued;
        document.getElementById('c-running').textContent = data.counts.running;
        document.getElementById('c-success').textContent = data.counts.success;
        document.getElementById('c-failed').textContent = data.counts.failed;
        document.getElementById('c-total').textContent = data.counts.total;
        document.getElementById('overall-pct').textContent = data.overall_percent + '%';
        document.getElementById('overall-bar').style.width = data.overall_percent + '%';

        const workersEl = document.getElementById('workers');
        workersEl.innerHTML = (data.workers || []).map(w => {
          const ok = w.health === 'ok';
          const jobs = (w.active_jobs || []).map(j =>
            '<div class="worker-job"><strong>' + j.progress + '%</strong> ' + j.title + '</div>'
          ).join('');
          return '<div class="worker ' + (ok ? 'ok' : 'bad') + '">' +
            '<h3><span class="instance-tag">' + w.name + '</span></h3>' +
            '<div class="muted">' + (w.health || 'unknown') + '</div>' +
            (jobs || '<div class="muted" style="margin-top:0.5rem">Idle</div>') +
            '</div>';
        }).join('');

        const tbody = document.getElementById('jobs-body');
        tbody.innerHTML = (data.recent_jobs || []).map(job => {
          const sk = (job.source_key || '').replace(/'/g, "\\'");
          const retryBtn = job.status === 'failed'
            ? '<button class="btn" onclick="retryJob(\\'' + sk + '\\', \\'' + job.id + '\\', this)">Retry</button>'
            : '';
          const err = job.error
            ? '<div class="error-text" title="' + job.error.replace(/"/g, '&quot;') + '">' +
              job.error.slice(0, 80) + (job.error.length > 80 ? '...' : '') + '</div>'
            : '';
          return '<tr>' +
            '<td title="' + job.source_key + '">' + (job.title || job.source_key) + err + '</td>' +
            '<td>' + instanceCell(job.worker) + '</td>' +
            '<td>' + pill(job.status) + '</td>' +
            '<td>' + progressCell(job) + '</td>' +
            '<td>' + job.attempts + '</td>' +
            '<td>' + fmtTime(job.finished_at || job.created_at) + '</td>' +
            '<td>' + retryBtn + '</td>' +
          '</tr>';
        }).join('');
      } catch (e) {
        document.getElementById('updated').textContent =
          'Refresh failed — retrying in 5s' + (e && e.message ? ' (' + e.message + ')' : '');
      }
    }
    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>
"""
