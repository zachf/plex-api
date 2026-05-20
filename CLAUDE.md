# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the CLI

```powershell
# Activate the venv first (Windows)
.venv\Scripts\activate

# Interactive shell (recommended)
python plex_cli.py

# Single command
python plex_cli.py <command> [args]
```

There are no tests, no build step, and no linter configured. The only file that matters is `plex_cli.py`.

## Architecture

The entire program lives in a single file: `plex_cli.py` (~6,500 lines). The structure top-to-bottom is:

1. **Module-level helpers** — `load_config` / `save_config`, `format_duration`, `format_size`, `format_ts`, `parse_search_args`, `clean_title`, `get_media_rows`, etc.

2. **API client classes** (in order):
   - `PlexClient` — wraps Plex HTTP API at `base_url` (stored in config as `plex_url`). Auth via `X-Plex-Token` query param. All responses are JSON (`Accept: application/json`).
   - `RadarrClient` — Radarr v3 API at `/api/v3/`. Auth via `X-Api-Key` header.
   - `SonarrClient` — Sonarr v3 API, same shape as `RadarrClient`.
   - `TautulliClient` — Tautulli API v2 at `/api/v2?apikey=KEY&cmd=COMMAND`. Response envelope: `{"response": {"data": ...}}`. Note the history endpoint has a double `.get("data", {}).get("data", [])`.
   - `TMDBClient` — TMDB v3 REST API. Auth via `api_key` query param (no OAuth).

3. **`_DEFAULT_TMDB_LISTS`** — dict mapping friendly names to TMDB list IDs. Persisted to `~/.plex_cli_lists.json`; `load_lists()` / `save_lists()` manage it.

4. **Display helpers** — standalone functions (`print_libraries`, `print_media_table`, `build_sessions_table`, `print_item_detail`, `_distribution_table`) that render Rich tables. These are module-level, not methods.

5. **`_HELP_SECTIONS`** — list of `(section_name, [(cmd, args_str, desc), ...])` tuples. This is the sole source of truth for `help` output. Every new command must be added here.

6. **`PlexShell(cmd.Cmd)`** — the shell. Each command is a `do_<name>(self, arg: str)` method. Tab completion is a paired `complete_<name>(self, text, line, begidx, endidx)` method.

## Key patterns

**Config** is stored at `~/.plex_cli.json`. Keys: `plex_url`, `token`, `radarr_url`, `radarr_api_key`, `sonarr_url`, `sonarr_api_key`, `tmdb_api_key`, `tautulli_url`, `tautulli_api_key`. The `_get_*_client()` helpers on `PlexShell` read config + env vars, prompt for missing values, and save back. Use these helpers rather than reading config directly in `do_*` methods — except for silent cross-reference checks (Radarr/Tautulli presence in deep-dive commands), where config is read directly without prompting.

**URL normalization** — all three external clients apply this in `__init__`:
```python
self.base_url = (base_url if "://" in base_url else f"http://{base_url}").rstrip("/")
```

**Plex movie fuzzy matching** — `_plex_movie_set()` returns `set[tuple[str, int]]` of `(lower_title, year)`. `_in_plex(title, year, plex_set)` does exact match first, then `SequenceMatcher ratio >= 0.88` with ±1 year tolerance, plus a colon/dash prefix check for subtitle variants. Use these for all Plex cross-references; do not roll your own matching.

**`questionary` in VS Code terminal** — the VS Code integrated terminal is not a Windows console, so `questionary.select()` / `questionary.checkbox()` raise `NoConsoleScreenBufferError`. Always wrap questionary calls in `try/except Exception:` with a sensible fallback (e.g. best `SequenceMatcher` match, or first item).

**Arg parsing in `do_*` methods** — most commands use `parse_search_args(arg)` which calls `shlex.split` and separates positional tokens from `--flag value` pairs. For simpler flag parsing (just checking `--dry-run` etc.) use `tokens = arg.strip().split()` and `"--flag" in tokens` directly.

**Adding a new command** checklist:
1. Add `do_<name>(self, arg: str)` method to `PlexShell`
2. Add a matching entry to `_HELP_SECTIONS`
3. Add `complete_<name>` if the command takes completable arguments (library IDs, list names, etc.)
4. Update `README.md`
