"""Long-running FFmpeg live-restream process manager, one process per channel.

Unlike worker.py's VOD job queue (download once, transcode once, upload once
to R2), a live channel runs an indefinite ffmpeg process that keeps writing a
rolling window of HLS segments to local disk until explicitly stopped.
Segments are served directly by this service's /live static mount (see
main.py) — no R2 involved, since live output is ephemeral by nature.

Source is pulled with -c copy (remux only, no re-encode) since the source is
already an HLS stream — this is a restream, not a transcode.
"""

import asyncio
import shutil
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from transcode_service.config import settings

_PLAYLIST_POLL_INTERVAL = 0.5
_PLAYLIST_WAIT_TIMEOUT = 20  # seconds a channel can sit in "starting" before we give up watching
_STOP_GRACE_SECONDS = 10  # time to let ffmpeg exit after SIGTERM before SIGKILL


@dataclass
class _LiveChannel:
    process: asyncio.subprocess.Process
    out_dir: Path
    hls_url: str
    source_url: str
    status: str = "starting"  # starting | live | offline | error
    error: str | None = None
    stopping: bool = False
    log_lines: deque = field(default_factory=lambda: deque(maxlen=40))
    monitor_task: "asyncio.Task | None" = None


_channels: dict[str, _LiveChannel] = {}


def _out_dir(channel_id: str) -> Path:
    return Path(settings.live_output_dir) / channel_id


def _hls_url(channel_id: str) -> str:
    base = settings.worker_public_url.strip().rstrip("/")
    if not base:
        raise RuntimeError(
            "WORKER_PUBLIC_URL is not configured — required to build live HLS URLs"
        )
    return f"{base}/live/{channel_id}/index.m3u8"


def _build_cmd(source_url: str, out_dir: Path) -> list[str]:
    return [
        settings.ffmpeg_path,
        "-y",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-i", source_url,
        "-c", "copy",
        "-f", "hls",
        "-hls_time", str(settings.live_hls_segment_time),
        "-hls_list_size", str(settings.live_hls_list_size),
        "-hls_flags", "delete_segments+append_list+omit_endlist",
        "-hls_segment_filename", str(out_dir / "seg_%05d.ts"),
        str(out_dir / "index.m3u8"),
    ]


async def _watch_for_playlist(channel: _LiveChannel) -> None:
    """Flip starting -> live once ffmpeg has actually written the playlist."""
    playlist = channel.out_dir / "index.m3u8"
    elapsed = 0.0
    while elapsed < _PLAYLIST_WAIT_TIMEOUT:
        if playlist.exists():
            if channel.status == "starting":
                channel.status = "live"
            return
        await asyncio.sleep(_PLAYLIST_POLL_INTERVAL)
        elapsed += _PLAYLIST_POLL_INTERVAL


async def _monitor(channel_id: str) -> None:
    """Drain ffmpeg's stderr for diagnostics and detect when it exits."""
    channel = _channels[channel_id]
    watchdog = asyncio.create_task(_watch_for_playlist(channel))
    proc = channel.process
    assert proc.stderr is not None
    async for raw in proc.stderr:
        line = raw.decode(errors="replace").rstrip()
        if line:
            channel.log_lines.append(line)
    watchdog.cancel()
    await proc.wait()

    if channel.stopping:
        channel.status = "offline"
        channel.error = None
    else:
        channel.status = "error"
        channel.error = "\n".join(channel.log_lines) or "ffmpeg exited unexpectedly"


async def start_channel(channel_id: str, source_url: str) -> dict:
    existing = _channels.get(channel_id)
    if existing and existing.process.returncode is None:
        if existing.source_url == source_url:
            return {"status": existing.status, "hls_url": existing.hls_url}
        await stop_channel(channel_id)

    out_dir = _out_dir(channel_id)
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    hls_url = _hls_url(channel_id)
    process = await asyncio.create_subprocess_exec(
        *_build_cmd(source_url, out_dir),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )

    channel = _LiveChannel(
        process=process, out_dir=out_dir, hls_url=hls_url, source_url=source_url
    )
    _channels[channel_id] = channel
    channel.monitor_task = asyncio.create_task(_monitor(channel_id))

    return {"status": "starting", "hls_url": hls_url}


async def stop_channel(channel_id: str) -> dict:
    channel = _channels.get(channel_id)
    if not channel or channel.process.returncode is not None:
        _channels.pop(channel_id, None)
        return {"status": "offline"}

    channel.stopping = True
    channel.process.terminate()
    try:
        if channel.monitor_task:
            await asyncio.wait_for(channel.monitor_task, timeout=_STOP_GRACE_SECONDS)
    except asyncio.TimeoutError:
        channel.process.kill()
        if channel.monitor_task:
            await channel.monitor_task

    shutil.rmtree(channel.out_dir, ignore_errors=True)
    _channels.pop(channel_id, None)
    return {"status": "offline"}


def get_status(channel_id: str) -> dict:
    channel = _channels.get(channel_id)
    if not channel:
        return {"status": "offline", "hls_url": None, "error": None}
    hls_url = channel.hls_url if channel.status in ("starting", "live") else None
    return {"status": channel.status, "hls_url": hls_url, "error": channel.error}


async def stop_all() -> None:
    """Best-effort cleanup on app shutdown so ffmpeg processes aren't orphaned."""
    for channel_id in list(_channels.keys()):
        await stop_channel(channel_id)
