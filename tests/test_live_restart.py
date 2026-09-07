from transcode_service.live_manager import ffmpeg_restart_delay_seconds


def test_backoff_then_give_up():
    assert ffmpeg_restart_delay_seconds(0) == 1
    assert ffmpeg_restart_delay_seconds(1) == 2
    assert ffmpeg_restart_delay_seconds(2) == 4
    assert ffmpeg_restart_delay_seconds(4) == 16
    assert ffmpeg_restart_delay_seconds(5) is None
    assert ffmpeg_restart_delay_seconds(9) is None
