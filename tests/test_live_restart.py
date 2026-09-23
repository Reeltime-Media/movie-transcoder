from pathlib import Path

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
