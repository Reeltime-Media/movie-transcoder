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


def test_http_hls_allows_extensionless_segments():
    cmd = _build_cmd(
        "https://live.example/chunklist.m3u8", Path("/tmp/live-test"), "cid"
    )
    assert cmd[cmd.index("-allowed_extensions") + 1] == "ALL"
    assert cmd[cmd.index("-allowed_segment_extensions") + 1] == "ALL"
    assert cmd[cmd.index("-reconnect_on_network_error") + 1] == "1"
