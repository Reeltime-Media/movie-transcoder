from pathlib import Path

from transcode_service.live_manager import _build_cmd, ffmpeg_restart_delay_seconds


def test_backoff_then_give_up():
    assert ffmpeg_restart_delay_seconds(0) == 1
    assert ffmpeg_restart_delay_seconds(1) == 2
    assert ffmpeg_restart_delay_seconds(2) == 4
    assert ffmpeg_restart_delay_seconds(4) == 16
    assert ffmpeg_restart_delay_seconds(5) is None
    assert ffmpeg_restart_delay_seconds(9) is None


def test_http_hls_allows_extensionless_segments():
    cmd = _build_cmd(
        "https://live.example/chunklist.m3u8", Path("/tmp/live-test"), "cid"
    )
    assert cmd[cmd.index("-allowed_extensions") + 1] == "ALL"
    assert cmd[cmd.index("-allowed_segment_extensions") + 1] == "ALL"
    assert cmd[cmd.index("-reconnect_on_network_error") + 1] == "1"
