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


def test_fresh_output_recovers_an_errored_channel(tmp_path: Path):
    (tmp_path / "index.m3u8").write_text("#EXTM3U\n#EXTINF:2.0,\nseg_00001.ts\n")
    channel = _channel(tmp_path, process=None, status="error")
    channel.error = "Connection timed out"

    asyncio.run(live_manager._watch_for_playlist(channel))
    assert channel.status == "live"
    assert channel.error is None
