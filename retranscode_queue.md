# Reeltime re-transcode queue (aspect ratio audit)

Generated from R2 `source.mp4` probes (display aspect ratio vs 16:9).

## Summary

- **Movies scanned:** 14
- **Episodes scanned:** 597
- **OK (16:9):** 270
- **Needs re-transcode:** 108
- **Probe errors (retry):** 233

## Movies

**None.** All 14 movies in R2 are already 1920×1080 (16:9).

| Title (slug) | Resolution | Status |
|---|---|---|
| Movies/appleseed (`movies/appleseed-2004`) | 1920×1080 | OK |
| Movies/beach Spike (`movies/beach-spike-2011`) | 1920×1080 | OK |
| Movies/curse Of Chucky (`movies/curse-of-chucky-2013`) | 1920×1080 | OK |
| Movies/deadpool (`movies/deadpool-2016`) | 1920×1080 | OK |
| Movies/fight Back To School 2 (`movies/fight-back-to-school-2-1992`) | 1920×1080 | OK |
| Movies/fight Back To School Iii (`movies/fight-back-to-school-iii-1993`) | 1920×1080 | OK |
| Movies/future Cops (`movies/future-cops-1993`) | 1920×1080 | OK |
| Movies/haunted (`movies/haunted-2011`) | 1920×1080 | OK |
| Movies/hitman Agent Jun (`movies/hitman-agent-jun-2020`) | 1920×1080 | OK |
| Movies/i Am Soldier (`movies/i-am-soldier-2014`) | 1920×1080 | OK |
| Movies/jeet Kune Do (`movies/jeet-kune-do-2010`) | 1920×1080 | OK |
| Movies/the Package (`movies/the-package-2012`) | 1920×1080 | OK |
| Movies/tune In For Love (`movies/tune-in-for-love-2019`) | 1920×1080 | OK |
| Movies/ultimate Hero (`movies/ultimate-hero-2016`) | 1920×1080 | OK |

## Series / episodes that NEED re-transcode

These sources are **vertical 9:16 (1080×1920)**. The current transcoder forces 16:9, so they look zoomed/wrong on TV.

### I Crafted The Ancient Xuanyuan Sword
### I Can Hear The Voices Of Animals
### Live Streaming In The Spirit Realm
### My Connections Span Three Realms
### Dual Travel Ultimate Fortune Or Double Dimension Tycoon
- **Show slug:** `dual-travel-ultimate-fortune-or-double-dimension-tycoon-2025`
- **Episodes:** 5

| Episode slug |
|---|
| `dual-travel-ultimate-fortune-or-double-dimension-tycoon-2025-s01e01` |
| `dual-travel-ultimate-fortune-or-double-dimension-tycoon-2025-s01e03` |
| `dual-travel-ultimate-fortune-or-double-dimension-tycoon-2025-s01e07` |
| `dual-travel-ultimate-fortune-or-double-dimension-tycoon-2025-s01e08` |
| `dual-travel-ultimate-fortune-or-double-dimension-tycoon-2025-s01e10` |

## Probe errors (could not read — likely timeout)

233 files failed ffprobe during bulk scan. Re-run audit on these if needed.

| Slug prefix | Failed probes |
|---|---|
| `strange-hero-ouyang-de-2012` | 41 |
| `royal-tramp-2008` | 31 |
| `the-great-revival-2007` | 30 |
| `the-road-of-love-in-reverse-2025` | 26 |
| `su-dong-po-2012` | 24 |
| `the-luckiest-man-2003` | 19 |
| `wow-the-ancient-imperial-concubine-traveled-through-time-and-space-to-my-home-or-help-my-harem-traveled-to-modern-times-2024` | 18 |
| `tears-of-a-bride-2011` | 11 |
| `return-of-love-2025` | 10 |
| `shaolin-king-of-martial-arts-2002` | 9 |
| `black-white-2009` | 6 |
| `the-gifted-quadruplets-2024` | 4 |
| `the-last-love-2025` | 4 |
