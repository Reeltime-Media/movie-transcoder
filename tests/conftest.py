import os
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

os.environ.setdefault("R2_ACCOUNT_ID", "test")
os.environ.setdefault("R2_ACCESS_KEY_ID", "test")
os.environ.setdefault("R2_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("R2_BUCKET_NAME", "test")
os.environ.setdefault("R2_PUBLIC_URL", "https://cdn.test")
os.environ.setdefault("R2_SCAN_MODE", "true")
os.environ.setdefault("WORKER_PUBLIC_URL", "https://api.reeltime.fun")

pkg = types.ModuleType("transcode_service")
pkg.__path__ = [str(ROOT)]  # type: ignore[attr-defined]
sys.modules.setdefault("transcode_service", pkg)
