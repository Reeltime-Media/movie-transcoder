#!/usr/bin/env python3
"""Force re-transcode for sources flagged in aspect_ratio_audit.json.

Deletes each source's HLS output so R2 scan mode picks them up again with the
letterbox filter. Deploy the letterbox worker fix BEFORE running this.

Usage (from movie-transcoder/):
  python scripts/force_requeue_aspect.py --dry-run
  python scripts/force_requeue_aspect.py
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
    release_lock,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--audit",
        type=Path,
        default=ROOT / "aspect_ratio_audit.json",
        help="Path to aspect_ratio_audit.json",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List sources only; do not delete HLS",
    )
    args = parser.parse_args()

    if not args.audit.is_file():
        print(f"Missing audit file: {args.audit}", file=sys.stderr)
        sys.exit(1)

    data = json.loads(args.audit.read_text(encoding="utf-8"))
    needs = data.get("needs_retranscode") or []
    if not needs:
        print("No sources in needs_retranscode.")
        return

    print(f"Found {len(needs)} sources to force-requeue")
    for item in needs:
        key = item["source_key"]
        ratio = item.get("display_ratio") or item.get("storage_ratio")
        dims = f"{item.get('width')}x{item.get('height')}"
        if args.dry_run:
            print(f"  [dry-run] {key} ({dims}, ratio={ratio})")
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
