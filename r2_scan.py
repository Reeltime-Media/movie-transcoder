"""R2-only transcode scan: discover sources, coordinate locks, skip existing HLS.

Class A cost rules:
- Never ListObjects over movies/ or series/ without Delimiter — that walks every
  HLS .ts segment and was the main Class A bill driver.
- Prefer HEAD (Class B) to check source.mp4 / master.m3u8 existence.
- Cache source lists and dashboard inventory; idle workers back off hard.
"""

from __future__ import annotations

import json
import random
import re
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from transcode_service.config import settings

MOVIE_SOURCE = re.compile(r"^movies/([^/]+)/source\.mp4$")
EPISODE_SOURCE = re.compile(r"^series/([^/]+)/episodes/([^/]+)/source\.mp4$")
LOCK_NAME = ".transcode.lock"
FAILED_NAME = ".transcode.failed"


def _r2():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{settings.r2_account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=settings.r2_access_key_id,
        aws_secret_access_key=settings.r2_secret_access_key,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def hls_prefix_for_source(source_key: str) -> str:
    movie_match = MOVIE_SOURCE.match(source_key)
    if movie_match:
        return f"movies/{movie_match.group(1)}/hls"
    episode_match = EPISODE_SOURCE.match(source_key)
    if episode_match:
        return (
            f"series/{episode_match.group(1)}/episodes/{episode_match.group(2)}/hls"
        )
    raise ValueError(f"Unsupported source key layout: {source_key}")


def hls_master_key(source_key: str) -> str:
    return f"{hls_prefix_for_source(source_key)}/master.m3u8"


def lock_key(source_key: str) -> str:
    return f"{hls_prefix_for_source(source_key)}/{LOCK_NAME}"


def failed_key(source_key: str) -> str:
    return f"{hls_prefix_for_source(source_key)}/{FAILED_NAME}"


def delete_hls_output(source_key: str) -> int:
    """Delete HLS objects under the source's hls/ prefix so it becomes pending again.

    Used for forced re-transcodes (e.g. after fixing letterbox). Returns deleted count.
    """
    prefix = f"{hls_prefix_for_source(source_key)}/"
    client = _r2()
    deleted = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=settings.r2_bucket_name, Prefix=prefix):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if not objects:
            continue
        # delete_objects accepts up to 1000 keys per call
        for i in range(0, len(objects), 1000):
            chunk = objects[i : i + 1000]
            client.delete_objects(
                Bucket=settings.r2_bucket_name,
                Delete={"Objects": chunk, "Quiet": True},
            )
            deleted += len(chunk)
    return deleted


def _object_exists(key: str) -> bool:
    try:
        _r2().head_object(Bucket=settings.r2_bucket_name, Key=key)
        return True
    except ClientError:
        return False


def _list_common_prefixes(client, prefix: str) -> list[str]:
    """List immediate child prefixes only (Delimiter='/') — cheap Class A."""
    prefixes: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=settings.r2_bucket_name,
        Prefix=prefix,
        Delimiter="/",
    ):
        for cp in page.get("CommonPrefixes", []):
            p = cp.get("Prefix")
            if p:
                prefixes.append(p)
    return prefixes


def list_source_keys() -> list[str]:
    """Discover source.mp4 keys without walking HLS segment trees.

    Uses delimiter listings (folder pages only) + HEAD for source.mp4 (Class B).
    """
    client = _r2()
    keys: list[str] = []

    for movie_prefix in _list_common_prefixes(client, "movies/"):
        source_key = f"{movie_prefix}source.mp4"
        if _object_exists(source_key):
            keys.append(source_key)

    for series_prefix in _list_common_prefixes(client, "series/"):
        episodes_root = f"{series_prefix}episodes/"
        for episode_prefix in _list_common_prefixes(client, episodes_root):
            source_key = f"{episode_prefix}source.mp4"
            if _object_exists(source_key):
                keys.append(source_key)

    return sorted(keys)


def _read_lock(lock_object_key: str) -> dict | None:
    try:
        resp = _r2().get_object(Bucket=settings.r2_bucket_name, Key=lock_object_key)
        return json.loads(resp["Body"].read())
    except ClientError:
        return None


def _lock_is_stale(lock_data: dict | None) -> bool:
    if not lock_data:
        return True
    started = lock_data.get("started_at")
    if not started:
        return True
    try:
        started_at = datetime.fromisoformat(started.replace("Z", "+00:00"))
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - started_at).total_seconds()
        return age > settings.r2_lock_timeout_seconds
    except (TypeError, ValueError):
        return True


def release_lock(source_key: str) -> None:
    try:
        _r2().delete_object(Bucket=settings.r2_bucket_name, Key=lock_key(source_key))
    except ClientError:
        pass


def clear_failed_marker(source_key: str) -> None:
    try:
        _r2().delete_object(Bucket=settings.r2_bucket_name, Key=failed_key(source_key))
    except ClientError:
        pass


def write_failed_marker(source_key: str, error: str, attempts: int) -> None:
    body = json.dumps(
        {
            "error": error[:2000],
            "attempts": attempts,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "worker": settings.worker_name,
        }
    ).encode()
    _r2().put_object(
        Bucket=settings.r2_bucket_name,
        Key=failed_key(source_key),
        Body=body,
        ContentType="application/json",
    )


def acquire_lock(source_key: str, worker_name: str) -> bool:
    """Claim a source for this worker (create-if-not-exists, reclaim stale locks)."""
    lk = lock_key(source_key)
    existing = _read_lock(lk)
    if existing and not _lock_is_stale(existing):
        return existing.get("worker") == worker_name

    if existing and _lock_is_stale(existing):
        release_lock(source_key)

    body = json.dumps(
        {
            "worker": worker_name,
            "source_key": source_key,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
    ).encode()
    try:
        _r2().put_object(
            Bucket=settings.r2_bucket_name,
            Key=lk,
            Body=body,
            ContentType="application/json",
            IfNoneMatch="*",
        )
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        http = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in ("PreconditionFailed", "412") or http == 412:
            current = _read_lock(lk)
            if current and current.get("worker") == worker_name:
                return True
            if current and _lock_is_stale(current):
                release_lock(source_key)
                return acquire_lock(source_key, worker_name)
            return False
        raise


_inventory_cache: dict | None = None
_inventory_cache_at: float = 0.0
_source_keys_cache: list[str] | None = None
_source_keys_cache_at: float = 0.0
_no_work_until: float = 0.0

# Updated in background — dashboard reads this instantly (never blocks on full scan).
_dashboard_stats: dict = {
    "counts": {"total": 0, "pending": 0, "running": 0, "success": 0, "failed": 0},
    "pending": [],
    "failed": [],
    "in_progress": [],
    "done_count": 0,
    "updated_at": None,
    "refreshing": False,
}


def list_source_keys_cached(ttl_seconds: int | None = None) -> list[str]:
    global _source_keys_cache, _source_keys_cache_at
    ttl = settings.r2_source_list_ttl_seconds if ttl_seconds is None else ttl_seconds
    now = time.time()
    if _source_keys_cache is not None and now - _source_keys_cache_at < ttl:
        return _source_keys_cache
    _source_keys_cache = list_source_keys()
    _source_keys_cache_at = now
    return _source_keys_cache


def invalidate_source_keys_cache() -> None:
    global _source_keys_cache, _source_keys_cache_at
    _source_keys_cache = None
    _source_keys_cache_at = 0.0


def scan_inventory() -> dict:
    """Inventory via source discovery + HEAD checks (no full-tree LIST)."""
    sources = list_source_keys_cached()
    done_sources: set[str] = set()
    pending: list[str] = []
    failed: list[str] = []
    in_progress: list[str] = []

    for source_key in sources:
        # Class B HEADs — never list hls/ trees
        if _object_exists(hls_master_key(source_key)):
            done_sources.add(source_key)
            continue
        if _object_exists(failed_key(source_key)):
            failed.append(source_key)
            continue
        lk = lock_key(source_key)
        if _object_exists(lk):
            lock_data = _read_lock(lk)
            if lock_data and not _lock_is_stale(lock_data):
                in_progress.append(source_key)
                continue
        pending.append(source_key)

    return {
        "total_sources": len(sources),
        "pending": pending,
        "done": sorted(done_sources),
        "failed": failed,
        "in_progress": in_progress,
        "counts": {
            "total": len(sources),
            "pending": len(pending),
            "running": len(in_progress),
            "success": len(done_sources),
            "failed": len(failed),
        },
    }


def refresh_dashboard_stats() -> dict:
    """Refresh background stats (call from worker thread, not HTTP handler)."""
    global _dashboard_stats, _inventory_cache, _inventory_cache_at, _no_work_until
    _dashboard_stats = {**_dashboard_stats, "refreshing": True}
    try:
        inv = scan_inventory()
        _inventory_cache = inv
        _inventory_cache_at = time.time()
        _dashboard_stats = {
            "counts": inv["counts"],
            "pending": inv["pending"],
            "failed": inv["failed"],
            "in_progress": inv["in_progress"],
            "done_count": len(inv["done"]),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "refreshing": False,
        }
        if not inv["pending"]:
            _no_work_until = time.time() + settings.r2_idle_backoff_seconds
    except Exception as exc:
        _dashboard_stats = {
            **_dashboard_stats,
            "refreshing": False,
            "error": str(exc),
        }
    return _dashboard_stats


def get_dashboard_stats() -> dict:
    """Instant read for HTTP handlers."""
    return dict(_dashboard_stats)


def scan_inventory_cached(ttl_seconds: int | None = None) -> dict:
    """Cached inventory; does not force a refresh on every call."""
    global _inventory_cache, _inventory_cache_at
    ttl = settings.r2_stats_interval if ttl_seconds is None else ttl_seconds
    now = time.time()
    if _inventory_cache is not None and now - _inventory_cache_at < ttl:
        return _inventory_cache
    return refresh_dashboard_stats() or _inventory_cache or scan_inventory()


def _remove_pending(source_key: str) -> None:
    pending = _dashboard_stats.get("pending")
    if isinstance(pending, list) and source_key in pending:
        _dashboard_stats["pending"] = [k for k in pending if k != source_key]
        counts = dict(_dashboard_stats.get("counts") or {})
        counts["pending"] = max(0, int(counts.get("pending") or 0) - 1)
        _dashboard_stats["counts"] = counts


def claim_next_source(worker_name: str) -> str | None:
    """Pick a pending source and try to acquire its lock.

    Prefers the last inventory pending list so we do not re-HEAD every source
    every few seconds when the catalog is already complete.
    """
    global _no_work_until
    now = time.time()
    if now < _no_work_until:
        return None

    pending = list(_dashboard_stats.get("pending") or [])
    if pending:
        candidates = pending
    else:
        # No fresh pending list yet — cheap discovery, then HEAD masters (Class B).
        sources = list_source_keys_cached()
        candidates = []
        for source_key in sources:
            if _object_exists(hls_master_key(source_key)):
                continue
            if _object_exists(failed_key(source_key)):
                continue
            candidates.append(source_key)
        if not candidates:
            _no_work_until = now + settings.r2_idle_backoff_seconds
            return None

    random.shuffle(candidates)
    for source_key in candidates:
        if _object_exists(hls_master_key(source_key)):
            _remove_pending(source_key)
            continue
        if _object_exists(failed_key(source_key)):
            _remove_pending(source_key)
            continue
        lk = lock_key(source_key)
        if _object_exists(lk):
            lock_data = _read_lock(lk)
            if lock_data and not _lock_is_stale(lock_data):
                continue
        if acquire_lock(source_key, worker_name):
            _remove_pending(source_key)
            print(f"[transcode] claimed {source_key} on {worker_name}")
            return source_key

    _no_work_until = time.time() + settings.r2_idle_backoff_seconds
    return None
