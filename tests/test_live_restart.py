import asyncio
from pathlib import Path

from transcode_service import live_manager
from transcode_service.live_manager import (
    _build_cmd,
    ffmpeg_restart_delay_seconds,
    prepare_output_dir,
)


def test_backoff_caps_at_30s_and_never_gives_up():
    assert ffmpeg_restart_delay_seconds(0) == 1
    assert ffmpeg_restart_delay_seconds(1) == 2
    assert ffmpeg_restart_delay_seconds(2) == 4
    assert ffmpeg_restart_delay_seconds(4) == 16
    assert ffmpeg_restart_delay_seconds(5) == 30
    assert ffmpeg_restart_delay_seconds(9) == 30


def test_respawn_keeps_existing_playlist(tmp_path: Path):
    playlist = tmp_path / "index.m3u8"
    playlist.write_text("#EXTM3U\n#EXTINF:2.0,\nseg_00001.ts\n")
    prepare_output_dir(tmp_path, wipe=False)
    assert playlist.exists()


def test_fresh_start_wipes_output_dir(tmp_path: Path):
    (tmp_path / "index.m3u8").write_text("#EXTM3U\n")
    prepare_output_dir(tmp_path, wipe=True)
    assert not (tmp_path / "index.m3u8").exists()
    assert tmp_path.is_dir()


def _http_cmd() -> list[str]:
    return _build_cmd(
        "https://live.example/chunklist.m3u8", Path("/tmp/live-test"), "cid"
    )


def test_http_hls_allows_extensionless_segments():
    cmd = _http_cmd()
    assert cmd[cmd.index("-allowed_extensions") + 1] == "ALL"
    assert cmd[cmd.index("-allowed_segment_extensions") + 1] == "ALL"
    assert cmd[cmd.index("-reconnect_on_network_error") + 1] == "1"


def test_default_live_cmd_reencodes_independent_short_segments():
    """Players can start on any 2s chunk instead of waiting on a source GOP."""
    cmd = _http_cmd()
    flags = cmd[cmd.index("-hls_flags") + 1]
    joined = " ".join(cmd)
    assert "independent_segments" in flags
    assert "split_by_time" not in flags
    assert "program_date_time" in flags
    assert " -c copy" not in joined
    assert cmd[cmd.index("-tune") + 1] == "zerolatency"
    assert "-force_key_frames" in cmd
    assert "-vf" in cmd
    assert cmd[cmd.index("-threads") + 1] == "1"


def test_http_hls_joins_source_at_live_edge():
    cmd = _http_cmd()
    assert cmd[cmd.index("-live_start_index") + 1] == "-1"


def test_copy_mode_remuxes_when_explicitly_enabled(monkeypatch):
    from transcode_service.config import settings

    monkeypatch.setattr(settings, "live_copy", True)
    cmd = _http_cmd()
    flags = cmd[cmd.index("-hls_flags") + 1]
    assert cmd[cmd.index("-c") + 1] == "copy"
    assert "split_by_time" in flags
    assert "independent_segments" not in flags


def _channel(tmp_path: Path, process, status: str) -> live_manager._LiveChannel:
    return live_manager._LiveChannel(
        process=process,
        out_dir=tmp_path,
        hls_url="https://api.reeltime.fun/live/cid/index.m3u8",
        source_url="http://source.example/live.m3u8",
        status=status,
    )


def test_source_that_never_connects_reports_error(tmp_path: Path, monkeypatch):
    async def no_respawn(channel_id: str) -> None:
        return None

    monkeypatch.setattr(live_manager, "_respawn_channel", no_respawn)

    async def run() -> live_manager._LiveChannel:
        proc = await asyncio.create_subprocess_exec(
            "sh", "-c", "echo 'Connection timed out' >&2; exit 1",
            stderr=asyncio.subprocess.PIPE,
        )
        channel = _channel(tmp_path, proc, "starting")
        live_manager._channels["cid"] = channel
        try:
            await live_manager._monitor("cid")
        finally:
            live_manager._channels.pop("cid", None)
        return channel

    channel = asyncio.run(run())
    assert channel.status == "error"
    assert "Connection timed out" in (channel.error or "")


def _run_respawn_chain(tmp_path: Path, monkeypatch, cycles: int) -> list[int]:
    """Let a source that dies instantly (HTTP 404) be respawned `cycles` times.

    Returns the restart counts the backoff was asked about, in order.
    """
    (tmp_path / "index.m3u8").write_text("#EXTM3U\n#EXTINF:2.0,\nseg_00001.ts\n")
    asked: list[int] = []

    def record_delay(restart_count: int) -> float:
        asked.append(restart_count)
        return 0.0

    monkeypatch.setattr(live_manager, "ffmpeg_restart_delay_seconds", record_delay)
    dying = ["sh", "-c", "echo 'Server returned 404 Not Found' >&2; exit 1"]
    spawned = 0

    async def fake_spawn(channel_id, source_url, out_dir):
        nonlocal spawned
        spawned += 1
        if spawned >= cycles:
            live_manager._channels[channel_id].stopping = True
        return await asyncio.create_subprocess_exec(*dying, stderr=asyncio.subprocess.PIPE)

    monkeypatch.setattr(live_manager, "_spawn_ffmpeg", fake_spawn)

    async def run() -> None:
        proc = await asyncio.create_subprocess_exec(*dying, stderr=asyncio.subprocess.PIPE)
        channel = _channel(tmp_path, proc, "live")
        live_manager._channels["cid"] = channel
        try:
            await live_manager._monitor("cid")
            while spawned < cycles or not channel.monitor_task.done():
                await asyncio.sleep(0.01)
        finally:
            live_manager._channels.pop("cid", None)

    asyncio.run(asyncio.wait_for(run(), timeout=10))
    return asked


def test_dead_source_keeps_backing_off_despite_stale_playlist(tmp_path: Path, monkeypatch):
    """The playlist left on disk by the last good run must not look like a recovery.

    Before the fix every respawn saw that old playlist, reset the counter and
    retried after 1s forever: four dead channels cost ~1.8 ffmpeg launches/s.
    """
    asked = _run_respawn_chain(tmp_path, monkeypatch, cycles=5)
    assert asked == [0, 1, 2, 3, 4]


def test_ffmpeg_that_ran_stably_restarts_fast_again(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(live_manager, "_STABLE_SECONDS", 0.0)
    asked = _run_respawn_chain(tmp_path, monkeypatch, cycles=4)
    assert asked == [0, 0, 0, 0]


def test_stale_playlist_is_not_treated_as_fresh_output(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(live_manager, "_PLAYLIST_WAIT_TIMEOUT", 0.2)
    stale = "#EXTM3U\n#EXTINF:2.0,\nseg_00001.ts\n"
    (tmp_path / "index.m3u8").write_text(stale)
    channel = _channel(tmp_path, process=None, status="error")
    channel.error = "Server returned 404 Not Found"
    channel.restarts = 3

    asyncio.run(live_manager._watch_for_playlist(channel, stale_fingerprint=stale))
    assert channel.status == "error"
    assert channel.error == "Server returned 404 Not Found"
    assert channel.restarts == 3


def test_ffmpeg_cmd_hides_the_version_banner():
    """The banner made every failed launch write ~12 lines to the container log."""
    assert "-hide_banner" in _http_cmd()


def test_fresh_output_recovers_an_errored_channel(tmp_path: Path):
    (tmp_path / "index.m3u8").write_text("#EXTM3U\n#EXTINF:2.0,\nseg_00001.ts\n")
    channel = _channel(tmp_path, process=None, status="error")
    channel.error = "Connection timed out"

    asyncio.run(live_manager._watch_for_playlist(channel))
    assert channel.status == "live"
    assert channel.error is None
