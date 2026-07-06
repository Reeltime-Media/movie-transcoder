"""Simple transcode progress dashboard (HTML + JSON API)."""

from __future__ import annotations

import asyncio
from datetime import datetime

import httpx
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from transcode_service.config import settings
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
    client: httpx.AsyncClient, label: str, base_url: str
) -> dict:
    if not base_url:
        return {
            "name": label,
            "url": None,
            "health": "ok",
            "progress": dict(worker_module.job_progress),
        }

    headers = {}
    if settings.api_key:
        headers["X-Api-Key"] = settings.api_key

    health = "unknown"
    progress: dict[str, int] = {}
    try:
        health_resp = await client.get(f"{base_url}/health", headers=headers, timeout=5.0)
        health = "ok" if health_resp.status_code == 200 else f"http_{health_resp.status_code}"
        prog_resp = await client.get(f"{base_url}/jobs/progress", headers=headers, timeout=5.0)
        if prog_resp.status_code == 200:
            progress = prog_resp.json()
    except Exception as exc:
        health = f"error: {exc.__class__.__name__}"

    return {"name": label, "url": base_url, "health": health, "progress": progress}


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

    async with httpx.AsyncClient() as client:
        worker_snapshots = await asyncio.gather(
            *[
                _fetch_peer_progress(client, label, url)
                for label, url in _all_worker_urls()
            ]
        )

    progress_by_job: dict[str, tuple[int, str]] = {}
    for snap in worker_snapshots:
        for job_id, pct in snap.get("progress", {}).items():
            progress_by_job[job_id] = (int(pct), snap["name"])

    running_jobs = []
    for row in recent:
        if row["status"] != "running":
            continue
        jid = str(row["id"])
        pct, worker_name = progress_by_job.get(jid, (0, "?"))
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

    recent_jobs = [
        {
            "id": str(row["id"]),
            "title": row["title"] or row["slug"] or row["source_key"],
            "source_key": row["source_key"],
            "status": row["status"],
            "attempts": row["attempts"],
            "error": row["error"],
            "progress": progress_by_job.get(str(row["id"]), (0, ""))[0]
            if row["status"] == "running"
            else (100 if row["status"] == "success" else 0),
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
        }
        for row in recent
    ]

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
    }


@router.get("/api/dashboard")
async def dashboard_api():
    return await _dashboard_data()


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
    main { padding: 1.25rem; max-width: 1200px; margin: 0 auto; }
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
    th, td { padding: 0.65rem 0.75rem; text-align: left; border-bottom: 1px solid #232833; }
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
    .workers { display: flex; flex-wrap: wrap; gap: 0.5rem; margin-bottom: 1rem; }
    .worker {
      background: #171a21; border: 1px solid #2a2f3a; border-radius: 8px;
      padding: 0.5rem 0.75rem; font-size: 0.82rem;
    }
    .worker.ok { border-color: #166534; }
    .worker.bad { border-color: #7f1d1d; }
    .mini-bar {
      height: 6px; background: #2a2f3a; border-radius: 999px; margin-top: 0.35rem;
      overflow: hidden; width: 120px; display: inline-block; vertical-align: middle;
    }
    .mini-fill { height: 100%; background: #3b82f6; }
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

    <div class="workers" id="workers"></div>

    <h2 style="font-size:1rem;margin:0 0 0.75rem;">Jobs</h2>
    <table>
      <thead>
        <tr>
          <th>Title / source</th>
          <th>Status</th>
          <th>Progress</th>
          <th>Attempts</th>
          <th>Updated</th>
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
    async function refresh() {
      try {
        const res = await fetch('/api/dashboard');
        const data = await res.json();
        if (!data.ready) {
          document.getElementById('updated').textContent = data.message || 'Starting...';
          return;
        }
        document.getElementById('updated').textContent = 'Updated ' + new Date(data.updated_at).toLocaleString();
        document.getElementById('worker-name').textContent = 'View: ' + (data.worker_name || 'worker');
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
          const active = Object.keys(w.progress || {}).length;
          return '<div class="worker ' + (ok ? 'ok' : 'bad') + '">' +
            '<strong>' + w.name + '</strong> ' + w.health +
            (active ? ' (' + active + ' active)' : '') +
            '</div>';
        }).join('');

        const tbody = document.getElementById('jobs-body');
        tbody.innerHTML = (data.recent_jobs || []).map(job =>
          '<tr>' +
            '<td title="' + job.source_key + '">' + (job.title || job.source_key) + '</td>' +
            '<td>' + pill(job.status) + '</td>' +
            '<td>' + progressCell(job) + '</td>' +
            '<td>' + job.attempts + '</td>' +
            '<td>' + fmtTime(job.finished_at || job.created_at) + '</td>' +
          '</tr>'
        ).join('');
      } catch (e) {
        document.getElementById('updated').textContent = 'Refresh failed';
      }
    }
    refresh();
    setInterval(refresh, 3000);
  </script>
</body>
</html>
"""
