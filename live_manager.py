"""Long-running FFmpeg live-restream process manager, one process per channel.

Unlike worker.py's VOD job queue (download once, transcode once, upload once
to R2), a live channel runs an indefinite ffmpeg process that keeps writing a
rolling window of HLS segments to local disk until explicitly stopped.
Segments are served directly by this service's /live static mount (see
main.py) — no R2 involved, since live output is ephemeral by nature.

When a logo asset is available, video is lightly re-encoded with an overlay
(top-left). Otherwise we remux with -c copy for minimum CPU.
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
_BUNDLED_LOGO = Path(__file__).resolve().parent / "assets" / "reeltime_live_logo.png"


def ffmpeg_restart_delay_seconds(restart_count: int) -> float:
    """Backoff before respawning ffmpeg. Never gives up — published channels stay up."""
    return float(min(30, 2**restart_count))


def prepare_output_dir(out_dir: Path, *, wipe: bool) -> None:
    """Create the HLS output dir. Wipe only on a deliberate start/stop, not a crash."""
    if wipe:
        shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class _LiveChannel:
    process: asyncio.subprocess.Process
    out_dir: Path
    hls_url: str
    source_url: str
    status: str = "starting"  # starting | live | offline | error
    error: str | None = None
    stopping: bool = False
    restarts: int = 0
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


def _resolve_logo_path() -> Path | None:
    configured = (settings.live_logo_path or "").strip()
    if configured.lower() == "none":
        return None
    if configured:
        path = Path(configured)
        return path if path.is_file() else None
    return _BUNDLED_LOGO if _BUNDLED_LOGO.is_file() else None


def _build_cmd(source_url: str, out_dir: Path, channel_id: str) -> list[str]:
    # Absolute segment URLs so players can hit the origin directly after authorize
    # (skips the API playlist-rewrite hop).
    public_base = settings.worker_public_url.strip().rstrip("/")
    hls_base = f"{public_base}/live/{channel_id}/" if public_base else ""

    # Essential live HLS muxer flags:
    # - delete_segments: deletes obsolete segments from disk based on list_size + delete_threshold
    # - temp_file: writes playlist and segments to temporary files before atomic rename to avoid partial reads
    # - omit_endlist: no EXT-X-ENDLIST for rolling live sliding window
    # - independent_segments: adds EXT-X-INDEPENDENT-SEGMENTS for instant decoder synchronization
    # split_by_time cannot be combined with independent_segments (ffmpeg disables it).
    hls_flags = "delete_segments+temp_file+omit_endlist+split_by_time"
    hls_args = [
        "-f", "hls",
        "-hls_time", str(settings.live_hls_segment_time),
        "-hls_list_size", str(settings.live_hls_list_size),
        "-hls_delete_threshold", str(settings.live_delete_threshold),
        "-hls_flags", hls_flags,
        "-hls_segment_filename", str(out_dir / "seg_%05d.ts"),
    ]
    if hls_base:
        hls_args.extend(["-hls_base_url", hls_base])
    hls_args.append(str(out_dir / "index.m3u8"))

    # Fast input analysis and low-latency buffer flags (drastically cuts stream startup time)
    input_args = [
        "-analyzeduration", "1000000",
        "-probesize", "1000000",
        "-fflags", "+nobuffer+discardcorrupt+genpts",
        "-rw_timeout", "10000000",
    ]
    if source_url.startswith("http://") or source_url.startswith("https://"):
        input_args.extend([
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_on_network_error", "1",
            "-reconnect_delay_max", "5",
            "-multiple_requests", "1",
            # Malimar (and similar CDNs) serve segments as extensionless URLs
            # like ".../me". ffmpeg 6+ rejects those unless we allow ALL.
            "-allowed_extensions", "ALL",
            "-allowed_segment_extensions", "ALL",
        ])
    input_args.extend(["-i", source_url])

    logo = _resolve_logo_path()
    if logo is None:
        # Remux-only — lowest CPU, instant start, no watermark.
        return [
            settings.ffmpeg_path,
            "-y",
            *input_args,
            "-c", "copy",
            "-muxdelay", "0",
            "-muxpreload", "0",
            *hls_args,
        ]

    # Burn Reeltime logo top-left with hardware/multi-threaded x264 re-encode.
    # Cap height for fast mobile join; force yuv420p for browser/ExoPlayer compatibility.
    # Force keyframes on HLS segment boundaries so chunks cut cleanly without delay.
    width = max(32, int(settings.live_logo_width))
    margin = max(0, int(settings.live_logo_margin))
    max_h = max(360, int(settings.live_max_height))
    seg = max(1, int(settings.live_hls_segment_time))
    gop_size = seg * 30  # approximate 30fps GOP

    filter_complex = (
        f"[0:v]scale=-2:'min({max_h},ih)'[base];"
        f"[1:v]scale={width}:-1[lg];"
        f"[base][lg]overlay={margin}:{margin}:shortest=1,format=yuv420p"
    )
    return [
        settings.ffmpeg_path,
        "-y",
        "-threads", "0",
        *input_args,
        "-i", str(logo),
        "-filter_complex", filter_complex,
        "-c:v", settings.video_codec,
        "-preset", settings.live_x264_preset,
        "-tune", "zerolatency",
        "-pix_fmt", "yuv420p",
        "-profile:v", "main",
        "-b:v", settings.live_video_bitrate,
        "-maxrate", settings.live_maxrate,
        "-bufsize", settings.live_bufsize,
        "-g", str(gop_size),
        "-keyint_min", str(gop_size),
        "-force_key_frames", f"expr:gte(t,n_forced*{seg})",
        "-sc_threshold", "0",
        "-c:a", "aac",
        "-b:a", "128k",
        "-ar", "44100",
        "-ac", "2",
        *hls_args,
    ]


async def _spawn_ffmpeg(
    channel_id: str, source_url: str, out_dir: Path
) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        *_build_cmd(source_url, out_dir, channel_id),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )


def _playlist_text(playlist: Path) -> str:
    try:
        return playlist.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""


async def _watch_for_playlist(
    channel: _LiveChannel, *, stale_fingerprint: str | None = None
) -> None:
    """Flip starting -> live once ffmpeg has written a playable playlist.

    On crash-restart we keep the previous playlist on disk so players do not
    404. Ignore that stale copy until ffmpeg writes new contents.
    """
    playlist = channel.out_dir / "index.m3u8"
    elapsed = 0.0
    while elapsed < _PLAYLIST_WAIT_TIMEOUT:
        content = _playlist_text(playlist)
        if "#EXTINF:" in content and content != stale_fingerprint:
            if channel.status == "starting":
                channel.status = "live"
            # Recovered — allow a fresh burst of retries if ffmpeg dies later.
            channel.restarts = 0
            return
        await asyncio.sleep(_PLAYLIST_POLL_INTERVAL)
        elapsed += _PLAYLIST_POLL_INTERVAL


async def _respawn_channel(channel_id: str) -> None:
    """Restart ffmpeg after an unexpected exit, with backoff."""
    channel = _channels.get(channel_id)
    if not channel or channel.stopping:
        return
    delay = ffmpeg_restart_delay_seconds(channel.restarts)
    channel.restarts += 1
    print(f"[live] ffmpeg died for {channel_id}; restart {channel.restarts} in {delay:.0f}s")
    await asyncio.sleep(delay)
    if _channels.get(channel_id) is not channel or channel.stopping:
        return

    stale = _playlist_text(channel.out_dir / "index.m3u8") or None
    prepare_output_dir(channel.out_dir, wipe=False)
    # Keep serving the last playlist while ffmpeg comes back.
    if not stale:
        channel.status = "starting"
    channel.error = None
    channel.process = await _spawn_ffmpeg(
        channel_id, channel.source_url, channel.out_dir
    )
    channel.monitor_task = asyncio.create_task(_monitor(channel_id))
    await _watch_for_playlist(channel, stale_fingerprint=stale)


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
        return

    # A newer start_channel() replaced this object — do not touch its output.
    if _channels.get(channel_id) is not channel:
        return

    channel.error = "\n".join(channel.log_lines) or "ffmpeg exited unexpectedly"
    print(f"[live] ffmpeg exited for {channel_id}: {channel.error[-400:]}")
    await _respawn_channel(channel_id)


async def start_channel(channel_id: str, source_url: str) -> dict:
    existing = _channels.get(channel_id)
    if existing and existing.process.returncode is None:
        if existing.source_url == source_url:
            return {"status": existing.status, "hls_url": existing.hls_url}
        await stop_channel(channel_id)
    elif existing:
        # Process already exited; stop the old monitor from respawning into us.
        existing.stopping = True

    out_dir = _out_dir(channel_id)
    prepare_output_dir(out_dir, wipe=True)

    hls_url = _hls_url(channel_id)
    process = await _spawn_ffmpeg(channel_id, source_url, out_dir)

    channel = _LiveChannel(
        process=process, out_dir=out_dir, hls_url=hls_url, source_url=source_url
    )
    _channels[channel_id] = channel
    channel.monitor_task = asyncio.create_task(_monitor(channel_id))
    await _watch_for_playlist(channel)
    return {"status": channel.status, "hls_url": hls_url}


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
