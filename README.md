# plex-cli

Interactive CLI for Plex Media Server with Radarr, Sonarr, and TMDB integration.

## Features

- Browse and search your Plex libraries
- Deep media analysis: codecs, bitrates, resolutions, HDR, audio formats
- Watch statistics, user stats, and play history
- Radarr integration: import TMDB lists, director filmographies, franchise completion
- Sonarr integration: missing episodes, quality upgrades, show search
- Smart playlist builder with combined filters
- Library maintenance: metadata audits, duplicate detection
- Live session monitoring and playback control
- One-page health dashboard

## Requirements

- Python 3.10+
- Plex Media Server (local network or remote)

```bash
pip install -r requirements.txt
```

Dependencies: `requests`, `rich`, `questionary`

> **Windows tab completion:** `pip install pyreadline3`

## Configuration

On first run the CLI prompts for your Plex token and saves it to `~/.plex_cli.json`.
Radarr, Sonarr, and TMDB credentials are prompted on first use of the relevant commands
and saved to the same file.

| Key | Source | Default |
|---|---|---|
| `plex_url` | Prompted or `PLEX_URL` env var | `http://localhost:32400` |
| `plex_token` | Prompted or `PLEX_TOKEN` env var | — |
| `radarr_url` | Prompted or `RADARR_URL` env var | `http://localhost:7878` |
| `radarr_api_key` | Prompted or `RADARR_API_KEY` env var | — |
| `sonarr_url` | Prompted or `SONARR_URL` env var | `http://localhost:8989` |
| `sonarr_api_key` | Prompted or `SONARR_API_KEY` env var | — |
| `tmdb_api_key` | Prompted or `TMDB_API_KEY` env var | — |
| `tautulli_url` | Prompted or `TAUTULLI_URL` env var | `http://localhost:8181` |
| `tautulli_api_key` | Prompted or `TAUTULLI_API_KEY` env var | — |

To find your Plex token: Plex Web → any item → Get Info → View XML → look for `X-Plex-Token` in the URL.

TMDB API key: register free at [themoviedb.org](https://www.themoviedb.org/) → Settings → API.

## Usage

**Interactive shell** (recommended):
```bash
python plex_cli.py
```

**Single command:**
```bash
python plex_cli.py <command> [args]
```

**Tab completion** is available for all commands, library names, list names, and flags.

---

## Commands

### Dashboard

| Command | Description |
|---|---|
| `health` | One-page summary: library stats, zero-duration items, stale shows, Radarr sync and upgrade gaps |
| `doctor` | Read-only configuration, connectivity, and storage diagnostics |

### Basic

| Command | Args | Description |
|---|---|---|
| `status` | | Server info and version |
| `libraries` | | List all media libraries |
| `browse` | `<id>` | Browse a library |
| `search` | `[query] [--title] [--actor] [--director] [--genre] [--studio] [--year] [--library] [--type]` | Search content |
| `info` | `<key>` | Detailed info for an item |
| `sessions` | | Active playback sessions |
| `recent` | `[count]` | Recently added content |
| `ondeck` | | Continue watching |
| `ondeck_clear` | | Select On Deck items and mark them watched or reset progress |
| `hubs` | `[--library <name>] [--count N]` | Home screen hubs such as Continue Watching, Recently Added, and top picks |
| `children` | `<key>` | Seasons / episodes for a show |
| `url` | `<key>` | Print stream URL |
| `token` | `<token>` | Set or update Plex token |

### Library Health

| Command | Args | Description |
|---|---|---|
| `dupetitles` | | Items sharing the same title and year |
| `duplicates_smart` | `[--tolerance s] [--match-name] [--library name]` | Fuzzy duplicate detection |
| `missing` | | Items with incomplete metadata |
| `quality` | | Resolution breakdown per library |
| `orphans` | | Items with no associated media files |
| `zero_duration` | `[library_id]` | Items with no detected duration (likely corrupt) |

### Watch Statistics

| Command | Args | Description |
|---|---|---|
| `stats` | | Library totals and watch history summary |
| `history` | `[user] [count]` | Recent watch history |
| `unwatched` | `[count] [--library id\|name] [--sort added\|title\|library\|year]` | Content never played (default: all) |
| `toprated` | `[count] [--library id\|name] [--all] [--sort rating\|title\|year]` | Highest-rated items (default 50) |
| `popularity` | `[count] [--library id\|name] [--all] [--sort plays\|title]` | Most-watched titles by play count |
| `watch_calendar` | `[days]` | Day-by-day view of what was watched (default 7) |
| `watched_by_decade` | `[count]` | Which release decade you watch most |
| `recommendations` | `[count] [--library id\|name] [--all] [--sort rating\|title\|year]` | Highly rated unwatched content (≥7.5, default 50) |
| `rewatched` | `[count] [--library id\|name] [--all] [--sort plays\|title\|year]` | Titles played more than once |
| `show_progress` | `[library_id]` | Every TV show with watched %, episode counts, last watched |
| `binge_candidates` | `[library_id]` | TV shows ranked by unwatched episode count |
| `overdue` | `[months] [library_id]` | Items added >N months ago, never played (default 12) |
| `added_trend` | `[months]` | Items added per month — library growth over time |

### Storage

| Command | Args | Description |
|---|---|---|
| `largest` | `[count] [--library id\|name] [--all] [--sort size\|title\|library]` | Titles with the biggest file sizes |
| `smallest` | `[count] [--library id\|name] [--all] [--sort size\|title\|library]` | Titles with the smallest file sizes |
| `tvlargest` | `[count] [--library id\|name] [--all] [--sort size\|title\|episodes]` | TV shows with the most total disk usage |
| `tvsmallest` | `[count] [--library id\|name] [--all] [--sort size\|title\|episodes]` | TV shows with the least total disk usage |
| `longest` | `[count] [--library id\|name] [--all] [--sort duration\|title\|year]` | Titles with the longest runtime |
| `shortest` | `[count] [--library id\|name] [--all] [--sort duration\|title\|year]` | Titles with the shortest runtime |
| `storage` | | Disk usage breakdown by library |
| `diskspace` | | Free and used disk space on Radarr/Sonarr mounts |
| `bycodec` | `<codec>` | Titles using a given video or audio codec |
| `codecs` | | Video / audio codec distribution |
| `transcode` | | Items likely to require transcoding |
| `oversized` | `[--library id] [--4k GB] [--1080p GB] [--720p GB] [--sd GB] [--count N]` | Files larger than resolution-specific size thresholds |

### Collection Tools

| Command | Args | Description |
|---|---|---|
| `export` | `<library_id> [file]` | Export library to CSV or JSON |
| `fixtitles` | `[library_id]` | Find and fix filename-style titles |
| `settitle` | `<key> <title>` | Manually set the title for one item |
| `stale` | `[months]` | Shows with no updates in N months |

### Monitoring

| Command | Args | Description |
|---|---|---|
| `watch` | `[seconds]` | Live-refresh active sessions (Ctrl+C to stop) |
| `alert` | `[seconds]` | Alert when a transcode session starts |
| `activities` | | Show currently running background tasks |
| `logs` | `[lines] [--level debug\|info\|warn\|error]` | Recent server log entries |

### Playback Control

| Command | Args | Description |
|---|---|---|
| `clients` | | List available Plex clients |
| `play` | `<key> [--client name]` | Start playback on a client |
| `pause` | `[session]` | Pause a session |
| `resume` | `[session]` | Resume a paused session |
| `stop` | `[session]` | Stop a session |

### Analysis & Reports

| Command | Args | Description |
|---|---|---|
| `refresh` | `[library_id] [--force]` | Scan libraries; `--force` re-downloads metadata |
| `analyze` | `<key> \| --library <id>` | Trigger deep media analysis |
| `report` | `[--html filename.html]` | Comprehensive library report |
| `changelog` | `[days]` | Everything added/updated in the last N days |

### Ratings & Tags

| Command | Args | Description |
|---|---|---|
| `setrating` | `<key> <0-10>` | Set user rating on an item |
| `bygenre` | `<genre> [library_id]` | Browse by genre |
| `byactor` | `<name> [library_id]` | Browse by actor |
| `bydirector` | `<name> [library_id]` | Browse by director |
| `byyear` | `<year> [library_id]` | Browse by release year |
| `bycontentrating` | `<rating> [library_id]` | Browse by content rating |
| `byresolution` | `<res> [library_id]` | Browse by resolution (4K, 1080p, 720p, SD) |
| `director_stats` | `[library_id]` | Directors ranked by titles owned |
| `actor_stats` | `[library_id]` | Actors ranked by titles owned |

### Deeper Analysis

| Command | Args | Description |
|---|---|---|
| `bitrate` | `[library_id]` | Bitrate distribution with outlier flagging |
| `bitrate_outliers` | `[--library id] [--count N]` | Files above expected bitrate limits for their resolution |
| `subtitles` | `[library_id]` | Items missing subtitle tracks |
| `foreign_subtitle_audit` | `[--language en] [--library id] [--forced-ok] [--include-unknown] [--count N]` | Non-target-language audio missing full target-language subtitles |
| `hdr` | `[library_id]` | HDR and Dolby Vision content |
| `audioformat` | `<format>` | Items with a specific audio format |
| `audio_bloat_audit` | `[--library id] [--count N] [--min-size GB]` | Large files with lossless/high-channel audio ranked by estimated savings |
| `multiversion` | `[library_id]` | Items with more than one media version |
| `genres` | `[library_id]` | Genre distribution |
| `studios` | `[library_id]` | Studio distribution |
| `decade` | `[library_id]` | Content count by decade of release |
| `content_rating` | `[library_id]` | Content rating distribution |
| `missing_episodes` | `[library_id]` | TV seasons with gaps in episode numbering |
| `incomplete_seasons` | `[library_id]` | Seasons with fewer episodes than typical |
| `season_quality` | `[library_id] [--count N] [--audio]` | TV seasons with mixed resolution or video codec quality |
| `abandoned` | `[threshold%] [--library id]` | Shows started but not finished (<80% default) |
| `duration_outliers` | `[library_id]` | Episodes with runtime far from the show's median |
| `4k_audit` | `[library_id]` | 4K content breakdown by HDR, audio, and codec |
| `framerate` | `[library_id]` | Content broken down by frame rate |
| `aspect_ratio` | `[library_id]` | Video aspect ratio distribution |
| `pan_and_scan` | `[library_id]` | Movies stored in ~4:3, likely pan-and-scan crops of widescreen films |
| `audio_languages` | `[library_id]` | Audio track language breakdown |
| `resolution_trend` | `[library_id]` | 4K/1080p/720p/SD share by year added |
| `container_format` | `[library_id]` | File container format distribution |
| `size_by_codec` | `[library_id]` | Total and average file size by video codec |
| `codec_migration_plan` | `[--library id] [--count N] [--min-size GB] [--target hevc\|av1]` | Legacy-codec files ranked by estimated savings and watch popularity |
| `channel_dist` | `[library_id]` | Audio channel layout distribution |

### Cross-API Intelligence

Commands that combine Plex, TMDB, Tautulli, and Radarr for richer insights.

| Command | Args | Description |
|---|---|---|
| `watch_next` | `[--count N] [--library id] [--runtime min] [--genre g] [--type movie\|episode]` | Ranked Plex picks from On Deck, unwatched ratings, recency, and filters |
| `quality_upgrade_plan` | `[--limit N] [--movies\|--shows]` | Prioritize Radarr/Sonarr cutoff upgrades using Plex play stats |
| `premiere_watchlist` | `[--days N] [--past N] [--missing-only]` | Radarr release calendar cross-checked against Plex indexing |
| `collection_gaps` | `[--import] [--dry-run] [--limit N]` | TMDB franchise gaps across Plex and Radarr; optionally add Radarr gaps |
| `smart_recommendations` | `[--count N] [--min-rating R]` | Personalized picks: builds a genre profile from your Tautulli history, queries TMDB for popular matching films, filters out anything already in Plex or Radarr |
| `trending_in_library` | `[--window day\|week]` | TMDB trending movies split into owned vs. not-yet-owned (default: week) |
| `trending_missing` | `[--window day\|week] [--count N] [--import] [--dry-run] [--profile id] [--search]` | TMDB trending movies missing from Radarr, optionally importing them |
| `tmdb_movie` | `<title>` | Search TMDB by title and display full details: overview, rating, cast, director, studios, budget/revenue, keywords, plus Plex and Radarr status |
| `director_deep_dive` | `<name>` | Full directed filmography with Plex ownership, Tautulli watch status, and Radarr presence per title |
| `actor_deep_dive` | `<name>` | Acting credits with character names, Plex ownership, Tautulli watch status, and Radarr presence |

### Item Extras

| Command | Args | Description |
|---|---|---|
| `extras` | `<key>` | Trailers, featurettes, and interviews |
| `related` | `<key>` | Related / recommended content |

### Users & Sharing

| Command | Args | Description |
|---|---|---|
| `users` | | List all server accounts |
| `userstats` | `[username]` | Watch stats per user, or detail for one user |
| `sharing` | | Libraries each managed user can access |

### Playlists & Collections

| Command | Args | Description |
|---|---|---|
| `playlists` | | List all playlists |
| `playlist` | `<id>` | Show playlist contents |
| `playlist_create` | `<name> [key]` | Create a new playlist |
| `playlist_add` | `<playlist_id> <key>` | Add an item to a playlist |
| `playlist_remove` | `<playlist_id> <item_id>` | Remove an item from a playlist |
| `collections` | `[library_id]` | List collections |
| `collection` | `<key>` | Show items in a collection |
| `playlist_build` | `<name> [--genre g] [--director d] [--actor a] [--year y] [--decade Xd] [--studio s] [--contentrating r] [--unwatched] [--rating n] [--library id] [--limit n]` | Build a playlist from combined filters |

`playlist_build` examples:
```
playlist_build "Unseen Kubrick" --director "Stanley Kubrick" --unwatched
playlist_build "90s Comedies" --genre Comedy --decade 1990s
playlist_build "Top Horror" --genre Horror --rating 7.5 --limit 25
```

### Sonarr

Credentials are prompted on first use and saved to `~/.plex_cli.json`.

| Command | Args | Description |
|---|---|---|
| `sonarr_status` | | Connection info, quality profiles, root folders |
| `sonarr_sync` | | Shows downloaded in Sonarr missing from Plex, and Plex shows not in Sonarr |
| `sonarr_history` | `[--count N] [--event grabbed\|imported\|failed\|deleted\|renamed\|ignored] [--failures]` | Recent grabs, imports, failures, and file events |
| `sonarr_missing` | | Shows with monitored but missing episodes; select to trigger search |
| `sonarr_upgrade` | | Shows with episodes below quality cutoff; select to trigger re-search |
| `sonarr_add` | `<name>` | Search for a TV show and add it to Sonarr |
| `sonarr_calendar` | `[--days N]` | Upcoming monitored and unmonitored episode air dates |

### Tautulli

Credentials are prompted on first use and saved to `~/.plex_cli.json`.

| Command | Args | Description |
|---|---|---|
| `tautulli_status` | | Tautulli connection info |
| `tautulli_history` | `[--user <name>] [--count N]` | Play history with stream decision, platform, watched %, and duration |
| `tautulli_stats` | `[--days N]` | Top movies, shows, and users over the last N days |
| `tautulli_plays` | `[--days N]` | Plays per day as a compact bar chart |
| `tautulli_users` | | Users ranked by total plays and watch time |

### Overseerr

Credentials are prompted on first use and saved to `~/.plex_cli.json`.

| Command | Args | Description |
|---|---|---|
| `overseerr_status` | | Overseerr connection info and request summary |
| `overseerr_requests` | `[--filter all\|pending\|approved\|declined\|available] [--count N]` | Movie requests enriched with TMDB, IMDb, and Rotten Tomatoes ratings |

### Radarr

Credentials are prompted on first use and saved to `~/.plex_cli.json`.

| Command | Args | Description |
|---|---|---|
| `radarr_status` | | Connection info, quality profiles, root folders |
| `radarr_lists` | | Show available named TMDB lists |
| `radarr_list_info` | `<id\|url>` | Name, description, and item count for any TMDB list |
| `radarr_list_add` | `<name> <id\|url>` | Add or update a named list in the config |
| `radarr_list_remove` | `<name>` | Remove a named list |
| `radarr_list` | `<name>` | Preview a list: title / year / in-Radarr / in-Plex |
| `radarr_import` | `<name> [--dry-run] [--profile <id>] [--search]` | Add missing movies from a named list to Radarr |
| `radarr_director` | `<name> [--dry-run] [--profile <id>] [--search]` | Import a director's filmography from TMDB |
| `radarr_download` | `<name> [--dry-run] [--profile <id>]` | Search Radarr for all missing movies from a list |
| `radarr_pick` | `<name> [--profile <id>]` | Interactive checkbox to select movies to download |
| `radarr_upgrade_4k` | `<title> [--search]` | Switch a Radarr movie to a 4K quality profile and optionally search |
| `radarr_upgrade` | | Movies below quality cutoff; select to trigger upgrade searches |
| `radarr_sync` | | Radarr downloads missing from Plex, and Plex movies not in Radarr |
| `radarr_history` | `[--count N] [--event grabbed\|imported\|failed\|deleted\|renamed\|ignored] [--failures]` | Recent grabs, imports, failures, and file events |
| `radarr_calendar` | `[--days N]` | Upcoming monitored and unmonitored movie releases |
| `radarr_monitored` | `[--missing] [--downloaded]` | Monitored Radarr movies, optionally filtered by file status |
| `radarr_collections` | `[--import]` | TMDB franchise completion per series; `--import` adds missing |

#### Built-in TMDB Lists

Lists are stored in `~/.plex_cli_lists.json` and can be extended with `radarr_list_add`.

| Name | Description |
|---|---|
| `imdb_top_250` | IMDb Top 250 |
| `bfi_top_100` | BFI / Sight & Sound Top 100 |
| `rotten_tomatoes_100` | Rotten Tomatoes Top 100 |
| `nyt_21st_century` | New York Times 100 Best of the 21st Century |
| `afi_top_100` | AFI 100 Greatest American Films |
| `oscar_best_picture` | Academy Award Best Picture winners |
| `oscar_foreign_film` | Oscar Best International Feature winners |
| `cannes_palme_dor` | Cannes Palme d'Or winners |
| `berlin_golden_bear` | Berlin Golden Bear winners |
| `sundance_grand_jury` | Sundance Grand Jury Prize winners |
| `golden_globe_drama` | Golden Globe Best Drama winners |
| `golden_globe_foreign` | Golden Globe Best Foreign Language winners |
| `bafta_best_film` | BAFTA Best Film winners |
| `afi_thrillers` | AFI 100 Thrills |
| `afi_comedies` | AFI 100 Laughs |
| `studio_ghibli` | Studio Ghibli |
| `pixar` | Pixar |
| `disney_animation` | Disney Animation |
| `a24` | A24 Films |
| `criterion` | The Complete Criterion Collection (~1,350 titles) |
| `marvel` | Marvel Cinematic Universe (maintained by Travis Bell, TMDB founder) |
| `dc` | DC Comics Universe (maintained by Travis Bell, TMDB founder) |

### Maintenance

| Command | Args | Description |
|---|---|---|
| `plex_metadata` | `[--library <name>] [--fix] [--episodes]` | Audit for missing metadata fields; `--fix` triggers Plex refresh |
| `plex_duplicates` | `[--library <name>]` | Find duplicate movies; shows resolution, size, and reclaimable space |
| `omdb_cache` | | Summarize the local OMDB ratings cache: size, types, rating coverage, top genres |

`plex_metadata` checks: poster, summary, genres, content rating, director, cast, release date, and audience rating. `--fix` calls `PUT /library/metadata/{id}/refresh` on every flagged item. `--episodes` extends the audit to episode level (slow on large libraries).

`plex_duplicates` detects both cross-library duplicates and same-library multi-version items. Output is sorted by total size so the biggest storage wins appear first.
