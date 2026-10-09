from transcode_service.routers.hls_export import pick_highest_variant_uri, resolve_variant_key


def test_pick_highest_variant_uri():
    master = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=854x480
480p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080
1080p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1280x720
720p.m3u8
"""
    assert pick_highest_variant_uri(master) == "1080p.m3u8"


def test_pick_highest_returns_none_for_media_playlist():
    media = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXTINF:6.0,
seg000.ts
#EXT-X-ENDLIST
"""
    assert pick_highest_variant_uri(media) is None


def test_resolve_variant_key_relative():
    assert (
        resolve_variant_key("movies/demo/hls/master.m3u8", "1080p.m3u8")
        == "movies/demo/hls/1080p.m3u8"
    )
