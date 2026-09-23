#!/usr/bin/env python3
"""Force re-transcode so vertical sources get letterboxed HLS (never stretched).

Deletes each source's HLS output so R2 scan mode picks them up again with the
letterbox filter in worker.py.

Usage (from movie-transcoder/):
  python scripts/force_requeue_aspect.py --dry-run
  python scripts/force_requeue_aspect.py
  python scripts/force_requeue_aspect.py --series wow-the-ancient-imperial-concubine-traveled-through-time-and-space-to-my-home-or-help-my-harem-traveled-to-modern-times-2024 --dry-run
  python scripts/force_requeue_aspect.py --prefix series/i-can-hear-the-voices-of-animals-2024/
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parent


def _load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_dotenv(REPO / "movie-api" / ".env")
_load_dotenv(ROOT / ".env")
# Transcoder settings expect these names from the environment.
os.environ.setdefault("R2_SCAN_MODE", "true")

_parent = str(REPO)
if _parent not in sys.path:
    sys.path.insert(0, _parent)
# Local tree is movie-transcoder/; Docker maps it to transcode_service/.
if "transcode_service" not in sys.modules:
    pkg = types.ModuleType("transcode_service")
    pkg.__path__ = [str(ROOT)]  # type: ignore[attr-defined]
    sys.modules["transcode_service"] = pkg

from transcode_service.r2_scan import (  # noqa: E402
    clear_failed_marker,
    delete_hls_output,
    invalidate_source_keys_cache,
    list_episode_sources_for_series,
    release_lock,
)


def _sources_from_audit(audit_path: Path) -> list[dict]:
    data = json.loads(audit_path.read_text(encoding="utf-8"))
    return list(data.get("needs_retranscode") or [])


def _sources_from_series(slug: str) -> list[dict]:
    return [{"source_key": k} for k in list_episode_sources_for_series(slug)]


def _sources_from_prefix(prefix: str) -> list[dict]:
    """Match audit keys / known keys by string prefix (no full-bucket LIST)."""
    prefix = prefix.strip()
    if prefix.startswith("series/") and prefix.count("/") >= 1:
        # series/<slug>/... → resolve via cheap per-series listing
        parts = prefix.strip("/").split("/")
        if len(parts) >= 2 and parts[0] == "series":
            keys = list_episode_sources_for_series(parts[1])
            return [
                {"source_key": k}
                for k in keys
                if k.startswith(prefix) or prefix.rstrip("/") in k
            ]
    return []


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete HLS so vertical sources re-encode with letterbox."
    )
    parser.add_argument(
        "--audit",
        type=Path,
        default=ROOT / "aspect_ratio_audit.json",
        help="Path to aspect_ratio_audit.json (default mode)",
    )
    parser.add_argument(
        "--series",
        action="append",
        default=[],
        help="Series slug to requeue (can repeat). Lists R2 sources under series/<slug>/",
    )
    parser.add_argument(
        "--prefix",
        action="append",
        default=[],
        help="R2 key prefix to requeue (can repeat), e.g. series/foo-2024/",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List sources only; do not delete HLS",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max sources to process (0 = all)",
    )
    args = parser.parse_args()

    items: list[dict] = []
    seen: set[str] = set()

    def add_items(batch: list[dict], label: str) -> None:
        added = 0
        for item in batch:
            key = item.get("source_key")
            if not key or key in seen:
                continue
            seen.add(key)
            items.append(item)
            added += 1
        print(f"Loaded {added} from {label}")

    use_audit = not args.series and not args.prefix
    if use_audit:
        if not args.audit.is_file():
            print(f"Missing audit file: {args.audit}", file=sys.stderr)
            sys.exit(1)
        add_items(_sources_from_audit(args.audit), f"audit {args.audit.name}")

    for slug in args.series:
        add_items(_sources_from_series(slug.strip().strip("/")), f"series/{slug.strip()}/")

    for prefix in args.prefix:
        add_items(_sources_from_prefix(prefix.strip()), prefix.strip())

    if args.limit and args.limit > 0:
        items = items[: args.limit]

    if not items:
        print("No sources to requeue.")
        return

    print(
        f"Total {len(items)} sources to force-requeue"
        f"{' [dry-run]' if args.dry_run else ''}"
    )
    for item in items:
        key = item["source_key"]
        ratio = item.get("display_ratio") or item.get("storage_ratio")
        dims = f"{item.get('width')}x{item.get('height')}" if item.get("width") else ""
        meta = f" ({dims}, ratio={ratio})" if dims or ratio else ""
        if args.dry_run:
            print(f"  [dry-run] {key}{meta}")
            continue
        deleted = delete_hls_output(key)
        clear_failed_marker(key)
        release_lock(key)
        print(f"  queued {key} (deleted {deleted} HLS objects)")

    if not args.dry_run:
        invalidate_source_keys_cache()
        print("Done. Worker will pick these up on the next R2 scan poll.")


if __name__ == "__main__":
    main()
