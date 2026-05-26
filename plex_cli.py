#!/usr/bin/env python3
# Run with:  .venv\Scripts\python plex_cli.py
# Or first:  .venv\Scripts\activate  then:  python plex_cli.py
"""Interactive Plex Media Server CLI — opus2.local:32400"""

import cmd
import csv
from difflib import SequenceMatcher
import json
import os
import re
import shlex
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path

try:
    import requests
    from rich import box
    from rich.console import Console
    from rich.live import Live
    from rich.table import Table
    from rich.panel import Panel
    from rich.prompt import Prompt
except ImportError as _e:
    print(f"Missing dependency: {_e}")
    print("Run:  py -m pip install -r requirements.txt")
    sys.exit(1)

# Ensure Unicode output works on Windows (cp1252 terminals reject many Rich chars)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── Config ────────────────────────────────────────────────────────────────────

CONFIG_FILE = Path.home() / ".plex_cli.json"
LISTS_FILE  = Path.home() / ".plex_cli_lists.json"
PLEX_HEADERS = {
    "X-Plex-Client-Identifier": "plex-cli-interactive",
    "X-Plex-Product": "Plex CLI",
    "X-Plex-Version": "1.0",
    "Accept": "application/json",
}

console = Console()

# ── Helpers ───────────────────────────────────────────────────────────────────

def load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text())
        except Exception:
            pass
    return {}

def save_config(cfg: dict):
    CONFIG_FILE.write_text(json.dumps(cfg, indent=2))

def format_duration(ms: int | None) -> str:
    if ms is None:
        return "—"
    s = ms // 1000
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"

def format_size(bytes_: int | None) -> str:
    if bytes_ is None:
        return "—"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if bytes_ < 1024:
            return f"{bytes_:.1f} {unit}"
        bytes_ /= 1024
    return f"{bytes_:.1f} PB"

def format_ts(ts: int | None) -> str:
    if not ts:
        return "—"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")

def year(item: dict) -> str:
    return str(item.get("year", "")) or "—"

def rating(item: dict) -> str:
    r = item.get("rating") or item.get("audienceRating")
    return f"{r:.1f}" if r is not None else "—"

def full_title(item: dict) -> str:
    """Compose grandparent — parent — title for display."""
    title = item.get("title", "")
    gp = item.get("grandparentTitle", "")
    p = item.get("parentTitle", "")
    if gp and p:
        return f"[dim]{gp} — {p} —[/dim] {title}"
    if p:
        return f"[dim]{p} —[/dim] {title}"
    return title

def months_ago(ts: int | None) -> str:
    if not ts:
        return "—"
    delta = datetime.now() - datetime.fromtimestamp(ts)
    months = delta.days // 30
    if months == 0:
        return f"{delta.days}d ago"
    if months < 12:
        return f"{months}mo ago"
    return f"{months // 12}yr {months % 12}mo ago"

def resolution_label(res: str | None) -> str:
    if not res:
        return "Unknown"
    res = str(res).lower()
    return {"4k": "4K", "2160": "4K", "1080": "1080p", "720": "720p",
            "480": "SD", "576": "SD"}.get(res, res.upper())

SEARCH_FLAGS = {"--actor", "--director", "--genre", "--studio", "--year",
                "--library", "--type", "--title", "--tolerance", "--level",
                "--client", "--html", "--match-name", "--force"}
BOOL_FLAGS = {"--match-name", "--force"}

def parse_search_args(arg: str) -> tuple:
    """Parse search args into (query, flags). Supports --flag value pairs and standalone --bool-flags."""
    try:
        tokens = shlex.split(arg)
    except ValueError:
        tokens = arg.split()
    filters: dict = {}
    query_parts: list = []
    i = 0
    while i < len(tokens):
        t = tokens[i]
        if t in SEARCH_FLAGS:
            if t in BOOL_FLAGS:
                filters[t.lstrip("-")] = True
                i += 1
            elif i + 1 < len(tokens):
                filters[t.lstrip("-")] = tokens[i + 1]
                i += 2
            else:
                i += 1
        else:
            query_parts.append(t)
            i += 1
    return " ".join(query_parts), filters

_TECH_TOKENS = re.compile(
    r"^(1080[pi]?|720[pi]?|480[pi]?|2160[pi]?|4k|uhd|hdr10?|"
    r"blu[-.]?ray|bdrip|brrip|web[-.]?dl|webrip|web|hdtv|dvdrip|dvdscr|hdrip|"
    r"x26[45]|h\.?26[45]|hevc|avc|xvid|divx|"
    r"aac|dts|ac3|mp3|truehd|atmos|flac|"
    r"extended|theatrical|directors\.cut|unrated|remastered|proper|"
    r"rarbg|yts|yify|eztv|"
    r"\d{3,4}p)$",
    re.IGNORECASE,
)

def clean_title(title: str) -> str | None:
    """Return a cleaned title for dot-separated filename-style titles, or None if unchanged."""
    if title.count(".") < 2 or " " in title or title.endswith("."):
        return None
    segments = [p for p in title.split(".") if p]
    if all(len(p) <= 2 for p in segments):
        return None
    clean: list[str] = []
    for part in title.split("."):
        if re.match(r"^(19|20)\d{2}$", part) or _TECH_TOKENS.match(part):
            break
        clean.append(part)
    cleaned = " ".join(clean).strip()
    return cleaned if cleaned and cleaned != title else None

def get_media_rows(item: dict, library: str = "") -> list:
    """Flatten Media/Part elements into analysis-friendly dicts."""
    rows = []
    for media in item.get("Media", []):
        for part in media.get("Part", []):
            rows.append({
                "ratingKey": item.get("ratingKey", ""),
                "title": item.get("title", ""),
                "year": item.get("year"),
                "type": item.get("type", ""),
                "library": library,
                "videoCodec": (media.get("videoCodec") or "").lower(),
                "audioCodec": (media.get("audioCodec") or "").lower(),
                "videoResolution": media.get("videoResolution", ""),
                "container": (media.get("container") or "").lower(),
                "audioChannels": media.get("audioChannels"),
                "bitrate": media.get("bitrate"),
                "file": part.get("file", ""),
                "size": part.get("size"),
                "duration": media.get("duration") or item.get("duration"),
                "videoFrameRate": media.get("videoFrameRate", ""),
                "aspectRatio": media.get("aspectRatio"),
            })
    return rows

# ── API client ────────────────────────────────────────────────────────────────

class PlexClient:
    def __init__(self, token: str, base_url: str):
        self.token = token
        self.base_url = (base_url if "://" in base_url else f"http://{base_url}").rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(PLEX_HEADERS)
        self.session.params = {"X-Plex-Token": token}  # type: ignore

    def _request(self, method: str, path: str, silent: bool = False, **params):
        """Generic request; returns requests.Response or None on error."""
        url = f"{self.base_url}{path}"
        try:
            r = self.session.request(method, url, params=params, timeout=15)
            r.raise_for_status()
            return r
        except requests.exceptions.ConnectionError:
            if not silent:
                console.print(f"[red]Cannot reach {self.base_url}[/red]")
        except requests.exceptions.HTTPError as e:
            if not silent:
                console.print(f"[red]HTTP {e.response.status_code}:[/red] {path}")
        return None

    def get(self, path: str, silent: bool = False, **params) -> dict:
        r = self._request("GET", path, silent=silent, **params)
        if r is None:
            return {}
        try:
            return r.json()
        except requests.exceptions.JSONDecodeError:
            if not silent:
                console.print("[red]Server returned non-JSON response[/red]")
            return {}

    def put(self, path: str, **params) -> bool:
        return self._request("PUT", path, **params) is not None

    def post(self, path: str, **params) -> dict:
        r = self._request("POST", path, **params)
        if r is None:
            return {}
        try:
            return r.json()
        except requests.exceptions.JSONDecodeError:
            return {}

    def delete(self, path: str, **params) -> bool:
        return self._request("DELETE", path, **params) is not None

    def get_text(self, path: str, silent: bool = False, **params) -> str | None:
        """Fetch a plain-text endpoint, bypassing the JSON Accept header."""
        url = f"{self.base_url}{path}"
        headers = {"Accept": "text/plain, */*"}
        try:
            r = self.session.get(url, params=params, headers=headers, timeout=15)
            r.raise_for_status()
            return r.text
        except requests.exceptions.ConnectionError:
            if not silent:
                console.print(f"[red]Cannot reach {self.base_url}[/red]")
            return None
        except requests.exceptions.HTTPError as e:
            if not silent:
                console.print(f"[red]HTTP {e.response.status_code}:[/red] {path}")
            return None

    # ── Convenience wrappers ─────────────────────────────────────────────────

    def _mc(self, path: str, key: str = "Metadata", **params) -> list:
        """GET path and return MediaContainer[key] as a list."""
        return self.get(path, **params).get("MediaContainer", {}).get(key, [])

    def server_info(self) -> dict:
        return self.get("/").get("MediaContainer", {})

    def libraries(self) -> list:
        return self._mc("/library/sections", "Directory")

    def library_contents(self, section_id: str, sort: str = "titleSort") -> list:
        return self._mc(f"/library/sections/{section_id}/all", sort=sort)

    def library_episodes(self, section_id: str) -> list:
        """Fetch all episodes in a TV library (type=4 returns leaf items with Media/Part)."""
        return self._mc(f"/library/sections/{section_id}/all", type=4)

    def search(self, query: str) -> list:
        return self._mc("/search", query=query)

    def section_search(self, section_id: str, query: str = "", **filters) -> list:
        params = {k: v for k, v in filters.items() if v}
        if query:
            params["query"] = query
            return self._mc(f"/library/sections/{section_id}/search", **params)
        return self._mc(f"/library/sections/{section_id}/all", **params)

    def title_filter(self, section_id: str, substring: str) -> list:
        q = substring.lower()
        return [i for i in self.library_contents(section_id) if q in (i.get("title") or "").lower()]

    def update_title(self, rating_key: str, new_title: str) -> bool:
        return self.put(f"/library/metadata/{rating_key}", **{"title.value": new_title, "title.locked": 1})

    def set_rating(self, rating_key: str, val: float) -> bool:
        return self.put(f"/library/metadata/{rating_key}", **{"userRating.value": val, "userRating.locked": 1})

    def scrobble(self, rating_key: str) -> bool:
        return self._request("GET", "/:/scrobble",
                             key=rating_key, identifier="com.plexapp.plugins.library") is not None

    def unscrobble(self, rating_key: str) -> bool:
        return self._request("GET", "/:/unscrobble",
                             key=rating_key, identifier="com.plexapp.plugins.library") is not None

    def refresh_metadata(self, rating_key: str) -> bool:
        return self._request("PUT", f"/library/metadata/{rating_key}/refresh") is not None

    def clients(self) -> list:
        discovered = {
            c["machineIdentifier"]: c
            for c in self._mc("/clients", "Server") if c.get("machineIdentifier")
        }
        for s in self.sessions():
            p = s.get("Player", {})
            mid = p.get("machineIdentifier", "")
            if mid and mid not in discovered:
                discovered[mid] = {
                    "machineIdentifier": mid, "name": p.get("title", ""),
                    "product": p.get("product", ""), "address": p.get("address", ""),
                    "port": p.get("port", ""), "deviceClass": p.get("deviceClass", ""),
                    "state": p.get("state", ""),
                }
        return list(discovered.values())

    def player_command(self, command: str, machine_id: str = "",
                       client_address: str = "", client_port: int = 0, **params) -> bool:
        self._cmd_id = getattr(self, "_cmd_id", 0) + 1
        params["commandID"] = self._cmd_id
        attempts = []
        if client_address and client_port:
            attempts.append((f"http://{client_address}:{client_port}/player/playback/{command}", {}))
        if machine_id:
            attempts.append((f"{self.base_url}/player/playback/{command}",
                             {"X-Plex-Target-Client-Identifier": machine_id}))
        last_err = ""
        for url, extra_headers in attempts:
            try:
                r = self.session.get(url, params=params, headers=extra_headers, timeout=10)
                r.raise_for_status()
                return True
            except requests.exceptions.ConnectionError:
                last_err = f"connection refused at {url}"
            except requests.exceptions.HTTPError as e:
                last_err = f"HTTP {e.response.status_code} from {url}"
        console.print(f"[red]Player command failed:[/red] {last_err}")
        return False

    def play_media(self, machine_id: str, rating_key: str,
                   client_address: str = "", client_port: int = 0) -> bool:
        info = self.server_info()
        host = self.base_url.split("://")[-1]
        return self.player_command(
            "playMedia", machine_id, client_address, client_port,
            key=f"/library/metadata/{rating_key}", offset=0,
            machineIdentifier=info.get("machineIdentifier", ""),
            address=host.split(":")[0],
            port=int(host.split(":")[-1]) if ":" in host else 32400,
            protocol="http", containerKey=f"/library/metadata/{rating_key}",
            token=self.token,
        )

    def pause_playback(self, mid: str, addr: str = "", port: int = 0) -> bool:
        return self.player_command("pause", mid, addr, port, type="video")

    def resume_playback(self, mid: str, addr: str = "", port: int = 0) -> bool:
        return self.player_command("play", mid, addr, port, type="video")

    def stop_playback(self, mid: str, addr: str = "", port: int = 0) -> bool:
        return self.player_command("stop", mid, addr, port, type="video")

    def stop_transcode(self, key: str) -> bool:
        return self.delete(f"/transcode/sessions/{key}")

    def analyze_item(self, key: str) -> bool:
        return self.put(f"/library/metadata/{key}/analyze")

    def analyze_library(self, sid: str) -> bool:
        return self.put(f"/library/sections/{sid}/analyze")

    def refresh_library(self, sid: str, force: bool = False) -> bool:
        """Refresh library metadata. force=True re-downloads from agents for every item."""
        params = {"force": 1} if force else {}
        return self._request("GET", f"/library/sections/{sid}/refresh", **params) is not None

    def sessions(self) -> list:            return self._mc("/status/sessions")
    def recent(self, count: int = 20) -> list:
        return self._mc("/library/recentlyAdded", **{"X-Plex-Container-Size": count})
    def metadata(self, key: str) -> dict:
        items = self._mc(f"/library/metadata/{key}")
        return items[0] if items else {}
    def on_deck(self) -> list:
        all_items: list = []
        start, page = 0, 50
        while True:
            data  = self.get("/library/onDeck",
                             **{"X-Plex-Container-Start": start, "X-Plex-Container-Size": page})
            mc    = data.get("MediaContainer", {})
            batch = mc.get("Metadata", [])
            if not batch:
                break
            all_items.extend(batch)
            total = mc.get("totalSize") or mc.get("size") or 0
            if total and start + len(batch) >= int(total):
                break
            if len(batch) < page:
                break
            start += page
        return all_items
    def hubs(self, section_id: str = "", count: int = 10) -> list:
        if section_id:
            return self._mc(f"/hubs/sections/{section_id}", "Hub", count=count)
        return self._mc("/hubs", "Hub", count=count)

    def children(self, key: str) -> list:  return self._mc(f"/library/metadata/{key}/children")
    def duplicates(self, sid: str) -> list: return self._mc(f"/library/sections/{sid}/duplicates")
    def extras(self, key: str) -> list:    return self._mc(f"/library/metadata/{key}/extras")
    def related(self, key: str) -> list:   return self._mc(f"/library/metadata/{key}/related")
    def get_playlists(self) -> list:       return self._mc("/playlists/all")
    def playlist_items(self, pid: str) -> list: return self._mc(f"/playlists/{pid}/items")
    def accounts(self) -> list:            return self._mc("/accounts", "Account")

    def history(self, count: int = 50, account_id: int | None = None) -> list:
        params: dict = {"sort": "viewedAt:desc", "X-Plex-Container-Size": count}
        if account_id:
            params["accountID"] = account_id
        return self._mc("/status/sessions/history/all", **params)

    # Map library type → Plex item type number for leaf items with Media/Part
    _LEAF_TYPE = {"show": 4, "artist": 10}   # episodes, tracks

    def _leaf_items(self, lib: dict) -> list:
        """Return the items that actually have Media/Part for a library.
        TV and music libraries need leaf items (episodes/tracks); others use top-level."""
        lid = lib.get("key", "")
        leaf = self._LEAF_TYPE.get(lib.get("type", ""))
        if leaf:
            return self._mc(f"/library/sections/{lid}/all", type=leaf)
        return self.library_contents(lid)

    def all_media_rows(self) -> list:
        rows = []
        for lib in self.libraries():
            for item in self._leaf_items(lib):
                rows.extend(get_media_rows(item, lib.get("title", lib.get("key", ""))))
        return rows

    def media_rows_for(self, section_id: str | None = None) -> list:
        """Media rows for one library (correctly using leaf items) or all libraries."""
        if not section_id:
            return self.all_media_rows()
        lib = next((l for l in self.libraries() if l.get("key") == section_id),
                   {"key": section_id, "title": section_id, "type": ""})
        return [row for item in self._leaf_items(lib)
                for row in get_media_rows(item, lib.get("title", section_id))]

    def all_items_by_library(self) -> dict:
        result = {}
        for lib in self.libraries():
            lid = lib.get("key", "")
            result[lib.get("title", lid)] = {"info": lib, "items": self.library_contents(lid)}
        return result

    def _server_uri(self, rating_key: str) -> str:
        mid = self.server_info().get("machineIdentifier", "")
        return f"server://{mid}/com.plexapp.plugins.library/library/metadata/{rating_key}"

    def create_playlist(self, name: str, rating_key: str = "") -> dict:
        params: dict = {"title": name, "type": "video", "smart": 0}
        if rating_key:
            params["uri"] = self._server_uri(rating_key)
        items = self.post("/playlists", **params).get("MediaContainer", {}).get("Metadata", [])
        return items[0] if items else {}

    def playlist_add_item(self, pid: str, key: str) -> bool:
        return self.put(f"/playlists/{pid}/items", uri=self._server_uri(key))

    def playlist_remove_item(self, pid: str, item_id: str) -> bool:
        return self.delete(f"/playlists/{pid}/items/{item_id}")

    def get_collections(self, section_id: str | None = None) -> list:
        if section_id:
            return self._mc(f"/library/sections/{section_id}/collections")
        result = []
        for lib in self.libraries():
            result.extend(self._mc(f"/library/sections/{lib.get('key','')}/collections"))
        return result

# ── Radarr client ─────────────────────────────────────────────────────────────

class RadarrClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = (base_url if "://" in base_url else f"http://{base_url}").rstrip("/")
        self.session  = requests.Session()
        self.session.headers.update({"X-Api-Key": api_key, "Content-Type": "application/json"})

    def _request(self, method: str, path: str, silent: bool = False, **kwargs):
        url = f"{self.base_url}/api/v3{path}"
        try:
            r = self.session.request(method, url, timeout=15, **kwargs)
            r.raise_for_status()
            return r
        except requests.exceptions.ConnectionError:
            if not silent:
                console.print(f"[red]Cannot reach Radarr at {self.base_url}[/red]")
        except requests.exceptions.HTTPError as e:
            if not silent:
                console.print(f"[red]Radarr HTTP {e.response.status_code}:[/red] {path}")
        return None

    def get(self, path: str, silent: bool = False, **params):
        r = self._request("GET", path, silent=silent, params=params)
        if r is None: return {}
        try: return r.json()
        except Exception: return {}

    def post(self, path: str, payload: dict) -> dict:
        r = self._request("POST", path, json=payload)
        if r is None: return {}
        try: return r.json()
        except Exception: return {}

    def status(self)           -> dict: return self.get("/system/status") or {}
    def movies(self)           -> list: r = self.get("/movie"); return r if isinstance(r, list) else []
    def quality_profiles(self) -> list: r = self.get("/qualityprofile"); return r if isinstance(r, list) else []
    def root_folders(self)     -> list: r = self.get("/rootfolder"); return r if isinstance(r, list) else []

    def add_movie(self, tmdb_id: int, title: str, year: int,
                  quality_profile_id: int, root_folder_path: str,
                  monitored: bool = True, search_on_add: bool = False) -> dict:
        return self.post("/movie", {
            "tmdbId": tmdb_id, "title": title, "year": year,
            "qualityProfileId": quality_profile_id, "rootFolderPath": root_folder_path,
            "monitored": monitored, "addOptions": {"searchForMovie": search_on_add},
        })

    def put(self, path: str, payload: dict) -> dict:
        r = self._request("PUT", path, json=payload)
        if r is None: return {}
        try: return r.json()
        except Exception: return {}

    def update_movie(self, movie: dict) -> dict:
        """PUT the full movie object back to Radarr (used to change quality profile etc.)."""
        return self.put(f"/movie/{movie['id']}", movie)

    def search_movies(self, movie_ids: list[int]) -> dict:
        """Trigger an immediate search for movies already in Radarr by their Radarr IDs."""
        return self.post("/command", {"name": "MoviesSearch", "movieIds": movie_ids})

    def calendar(self, start: str, end: str) -> list:
        r = self.get("/calendar", start=start, end=end)
        return r if isinstance(r, list) else []

# ── Sonarr client ─────────────────────────────────────────────────────────────

class SonarrClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = (base_url if "://" in base_url else f"http://{base_url}").rstrip("/")
        self.session  = requests.Session()
        self.session.headers.update({"X-Api-Key": api_key, "Content-Type": "application/json"})

    def _request(self, method: str, path: str, silent: bool = False, **kwargs):
        url = f"{self.base_url}/api/v3{path}"
        try:
            r = self.session.request(method, url, timeout=15, **kwargs)
            r.raise_for_status()
            return r
        except requests.exceptions.ConnectionError:
            if not silent:
                console.print(f"[red]Cannot reach Sonarr at {self.base_url}[/red]")
        except requests.exceptions.HTTPError as e:
            if not silent:
                console.print(f"[red]Sonarr HTTP {e.response.status_code}:[/red] {path}")
        return None

    def get(self, path: str, silent: bool = False, **params):
        r = self._request("GET", path, silent=silent, params=params)
        if r is None: return {}
        try: return r.json()
        except Exception: return {}

    def post(self, path: str, payload: dict) -> dict:
        r = self._request("POST", path, json=payload)
        if r is None: return {}
        try: return r.json()
        except Exception: return {}

    def status(self)           -> dict: return self.get("/system/status") or {}
    def series(self)           -> list: r = self.get("/series"); return r if isinstance(r, list) else []
    def quality_profiles(self) -> list: r = self.get("/qualityprofile"); return r if isinstance(r, list) else []
    def root_folders(self)     -> list: r = self.get("/rootfolder"); return r if isinstance(r, list) else []

    def wanted_missing(self, page_size: int = 1000) -> list:
        r = self.get("/wanted/missing", pageSize=page_size,
                     sortKey="series.sortTitle", sortDirection="ascending")
        return r.get("records", []) if isinstance(r, dict) else []

    def wanted_missing_count(self) -> int:
        r = self.get("/wanted/missing", pageSize=1)
        return r.get("totalRecords", 0) if isinstance(r, dict) else 0

    def wanted_cutoff(self, page_size: int = 1000) -> list:
        r = self.get("/wanted/cutoff", pageSize=page_size,
                     sortKey="series.sortTitle", sortDirection="ascending")
        return r.get("records", []) if isinstance(r, dict) else []

    def series_lookup(self, term: str) -> list:
        r = self.get("/series/lookup", term=term)
        return r if isinstance(r, list) else []

    def add_series(self, tvdb_id: int, title: str, year: int,
                   quality_profile_id: int, root_folder_path: str,
                   seasons: list | None = None, monitored: bool = True,
                   search_on_add: bool = False) -> dict:
        return self.post("/series", {
            "tvdbId": tvdb_id, "title": title, "year": year,
            "qualityProfileId": quality_profile_id, "rootFolderPath": root_folder_path,
            "monitored": monitored, "seasons": seasons or [],
            "addOptions": {"searchForMissingEpisodes": search_on_add},
        })

    def search_series(self, series_id: int) -> dict:
        return self.post("/command", {"name": "SeriesSearch", "seriesId": series_id})

# ── Tautulli client ───────────────────────────────────────────────────────────

class TautulliClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = (base_url if "://" in base_url else f"http://{base_url}").rstrip("/")
        self.api_key  = api_key
        self.session  = requests.Session()

    def _cmd(self, cmd: str, **params) -> dict:
        try:
            r = self.session.get(
                f"{self.base_url}/api/v2",
                params={"apikey": self.api_key, "cmd": cmd, **params},
                timeout=15,
            )
            r.raise_for_status()
            return r.json().get("response", {})
        except requests.exceptions.ConnectionError:
            console.print(f"[red]Cannot reach Tautulli at {self.base_url}[/red]"); return {}
        except requests.exceptions.HTTPError as e:
            console.print(f"[red]Tautulli HTTP {e.response.status_code}:[/red] {cmd}"); return {}

    def server_info(self) -> dict:
        return self._cmd("get_server_info").get("data", {})
    def home_stats(self, time_range: int = 30, count: int = 10) -> list:
        return self._cmd("get_home_stats", time_range=time_range, count=count).get("data", [])
    def plays_by_date(self, time_range: int = 30) -> dict:
        return self._cmd("get_plays_by_date", time_range=time_range).get("data", {})
    def users(self) -> list:
        return self._cmd("get_users").get("data", [])
    def history(self, length: int = 25, user: str = "") -> list:
        params: dict = {"start": 0, "length": length, "order_column": "date", "order_dir": "desc"}
        if user:
            params["search"] = user
        return self._cmd("get_history", **params).get("data", {}).get("data", [])

# ── TMDB client ───────────────────────────────────────────────────────────────

class TMDBClient:
    BASE = "https://api.themoviedb.org/3"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()

    def _get(self, path: str, **params) -> dict:
        try:
            r = self.session.get(f"{self.BASE}{path}",
                                 params={"api_key": self.api_key, **params}, timeout=10)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.ConnectionError:
            console.print("[red]Cannot reach TMDB.[/red]"); return {}
        except requests.exceptions.HTTPError as e:
            console.print(f"[red]TMDB HTTP {e.response.status_code}:[/red] {path}"); return {}

    def list_movies(self, list_id: int) -> list[dict]:
        def _parse(items):
            return [{"tmdb_id": m["id"], "title": m["title"],
                     "year": int((m.get("release_date") or "0")[:4] or 0)}
                    for m in items]
        first = self._get(f"/list/{list_id}")
        results = _parse(first.get("items", []))
        total_pages = first.get("total_pages", 1)
        for page in range(2, total_pages + 1):
            data = self._get(f"/list/{list_id}", page=page)
            results.extend(_parse(data.get("items", [])))
        return results

    def list_info(self, list_id: int) -> dict:
        """Fetch metadata for a TMDB list (name, description, item_count, created_by)."""
        return self._get(f"/list/{list_id}") or {}

    def search_person(self, name: str) -> list[dict]:
        return self._get("/search/person", query=name).get("results", [])

    def director_filmography(self, person_id: int) -> list[dict]:
        data = self._get(f"/person/{person_id}/movie_credits")
        return sorted(
            [{"tmdb_id": m["id"], "title": m["title"],
              "year":   int((m.get("release_date") or "0")[:4] or 0),
              "rating": round(float(m.get("vote_average") or 0), 1)}
             for m in data.get("crew", [])
             if m.get("job") == "Director" and m.get("release_date")],
            key=lambda x: x["year"],
        )

    def actor_filmography(self, person_id: int, include_self: bool = False) -> list[dict]:
        data = self._get(f"/person/{person_id}/movie_credits")
        return sorted(
            [{"tmdb_id": m["id"], "title": m["title"],
              "year":      int((m.get("release_date") or "0")[:4] or 0),
              "rating":    round(float(m.get("vote_average") or 0), 1),
              "character": m.get("character") or ""}
             for m in data.get("cast", [])
             if m.get("release_date") and m.get("title")
             and (include_self or "self" not in (m.get("character") or "").lower())],
            key=lambda x: x["year"],
        )

    def collection(self, collection_id: int) -> dict:
        """Fetch a TMDB franchise/collection → {name, parts: [{tmdb_id, title, year}]}"""
        data = self._get(f"/collection/{collection_id}")
        parts = sorted(
            [{"tmdb_id": m["id"], "title": m["title"],
              "year": int((m.get("release_date") or "0")[:4] or 0)}
             for m in data.get("parts", [])],
            key=lambda x: x["year"],
        )
        return {"name": data.get("name", ""), "parts": parts}

    def genre_ids(self) -> dict[str, int]:
        """Fetch TMDB movie genre name → ID mapping."""
        return {g["name"]: g["id"]
                for g in self._get("/genre/movie/list").get("genres", [])}

    def discover_movies(self, genre_filter: str, min_votes: int = 1000,
                        min_rating: float = 6.5, pages: int = 8) -> list[dict]:
        """Discover movies by pipe-separated genre IDs (OR logic), sorted by popularity."""
        results = []
        for page in range(1, pages + 1):
            data = self._get("/discover/movie", **{
                "with_genres":       genre_filter,
                "sort_by":           "popularity.desc",
                "vote_count.gte":    min_votes,
                "vote_average.gte":  min_rating,
                "language":          "en-US",
                "page":              page,
            })
            for m in data.get("results", []):
                results.append({
                    "tmdb_id":  m["id"],
                    "title":    m.get("title", "?"),
                    "year":     int((m.get("release_date") or "0")[:4] or 0),
                    "rating":   round(float(m.get("vote_average") or 0), 1),
                    "overview": m.get("overview", ""),
                })
        return results

    def trending(self, time_window: str = "week") -> list[dict]:
        """Fetch trending movies for 'day' or 'week'."""
        data = self._get(f"/trending/movie/{time_window}")
        return [{"tmdb_id":    m["id"],
                 "title":      m.get("title", "?"),
                 "year":       int((m.get("release_date") or "0")[:4] or 0),
                 "rating":     round(float(m.get("vote_average") or 0), 1),
                 "overview":   m.get("overview", ""),
                 "popularity": m.get("popularity", 0)}
                for m in data.get("results", [])]

    def search_movie(self, query: str) -> list[dict]:
        """Search TMDB for movies by title → list of result dicts."""
        return self._get("/search/movie", query=query).get("results", [])

    def movie_detail(self, tmdb_id: int) -> dict:
        """Fetch full movie detail including credits and keywords."""
        return self._get(f"/movie/{tmdb_id}", append_to_response="credits,keywords")

# ── TMDB named lists ──────────────────────────────────────────────────────────

_DEFAULT_TMDB_LISTS: dict[str, int] = {
    # Rankings & critics
    "imdb_top_250":         10265,   # IMDb Top 250 (community-maintained snapshot)
    "bfi_top_100":          6221,    # BFI / Sight & Sound Top 100 Greatest Films
    "rotten_tomatoes_100":  3697,    # Rotten Tomatoes Top 100 Movies of All Time
    "nyt_21st_century":     8648998, # New York Times — 100 Best Movies of the 21st Century
    # Academy Awards
    "afi_top_100":          145406,  # AFI 100 Greatest American Films (2007 ed.)
    "oscar_best_picture":   28,      # Academy Award Best Picture winners
    "oscar_foreign_film":   264,     # Academy Award Best Foreign Language Film
    # Film festivals
    "cannes_palme_dor":     229,     # Cannes Film Festival — Palme d'Or winners
    "berlin_golden_bear":   267,     # Berlin International Film Festival — Golden Bear
    "sundance_grand_jury":  44681,   # Sundance Film Festival — Grand Jury Prize
    # TV awards
    "golden_globe_drama":   2469,    # Golden Globe Best Motion Picture — Drama
    "golden_globe_foreign": 237,     # Golden Globe Best Foreign Language Film
    "bafta_best_film":      3681,    # BAFTA Best Film winners
    # AFI genre lists
    "afi_thrillers":        43,      # AFI 100 Most Thrilling American Films
    "afi_comedies":         3682,    # AFI 100 Years … 100 Laughs
    # Studio collections
    "studio_ghibli":        9078,    # Studio Ghibli — all theatrical features
    "pixar":                3700,    # Pixar Animation Studios — theatrical features
    "disney_animation":     5905,    # Walt Disney Animation Studios
    "a24":                  8270212, # A24 Films — full studio filmography
    "criterion":            8551925, # The Complete Criterion Collection (~1,350 titles)
    # Franchise universes (maintained by Travis Bell, TMDB founder)
    "marvel":               1,       # Marvel Cinematic Universe
    "dc":                   3,       # DC Comics Universe
}

def load_lists() -> dict[str, int]:
    """Load TMDB lists from LISTS_FILE; fall back to defaults if missing."""
    if not LISTS_FILE.exists():
        return dict(_DEFAULT_TMDB_LISTS)
    try:
        data = json.loads(LISTS_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return {k: int(v) for k, v in data.items()}
    except Exception:
        console.print(f"[yellow]Warning: could not parse {LISTS_FILE} — using defaults.[/yellow]")
    return dict(_DEFAULT_TMDB_LISTS)

def save_lists(lists: dict[str, int]) -> None:
    LISTS_FILE.write_text(json.dumps(lists, indent=2), encoding="utf-8")

# ── Display helpers ───────────────────────────────────────────────────────────

def print_libraries(libs: list):
    t = Table(title="Libraries", box=box.ROUNDED)
    t.add_column("ID", style="dim", width=4)
    t.add_column("Name", style="bold cyan")
    t.add_column("Type", style="yellow")
    t.add_column("Items", justify="right")
    for lib in libs:
        t.add_row(lib.get("key", ""), lib.get("title", ""), lib.get("type", ""), str(lib.get("count", "?")))
    console.print(t)

def print_media_table(items: list, title: str = "Results"):
    if not items:
        console.print("[yellow]No results.[/yellow]")
        return
    noun = "result" if len(items) == 1 else "results"
    t = Table(title=title, caption=f"{len(items)} {noun}", caption_justify="right", box=box.ROUNDED)
    t.add_column("Key", style="dim", width=7)
    t.add_column("Title", style="bold white", min_width=30)
    t.add_column("Type", style="yellow", width=10)
    t.add_column("Year", width=6, justify="right")
    t.add_column("Rating", width=7, justify="right")
    t.add_column("Duration", width=9, justify="right")
    for item in items:
        t.add_row(item.get("ratingKey", ""), full_title(item), item.get("type", ""),
                  year(item), rating(item), format_duration(item.get("duration")))
    console.print(t)

def build_sessions_table(sessions: list) -> Table:
    t = Table(title="Active Sessions", box=box.ROUNDED, expand=True)
    t.add_column("User", style="cyan", width=16)
    t.add_column("Title", style="bold white", min_width=28)
    t.add_column("Player", style="dim", width=16)
    t.add_column("State", width=10)
    t.add_column("Progress", width=24)
    t.add_column("Stream", width=10)
    for s in sessions:
        state = s.get("Player", {}).get("state", "unknown")
        offset = s.get("viewOffset", 0)
        dur = s.get("duration", 0) or 1
        pct = max(0, min(100, int(offset / dur * 100)))
        bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
        color = {"playing": "green", "paused": "yellow", "buffering": "magenta"}.get(state, "white")
        parent = s.get("grandparentTitle", "")
        title = s.get("title", "")
        t.add_row(
            s.get("User", {}).get("title", "Unknown"),
            f"{parent} — {title}" if parent else title,
            s.get("Player", {}).get("title", "Unknown"),
            f"[{color}]{state}[/{color}]",
            f"[dim]{bar}[/dim] {pct}%",
            "[red]transcode[/red]" if s.get("TranscodeSession") else "[green]direct[/green]",
        )
    return t

def print_sessions(sessions: list):
    if not sessions:
        console.print("[yellow]No active sessions.[/yellow]")
        return
    console.print(build_sessions_table(sessions))

def print_item_detail(item: dict):
    if not item:
        console.print("[yellow]Item not found.[/yellow]")
        return
    synopsis = item.get("summary", "No description available.")
    L = [f"[bold cyan]Type:[/bold cyan] {item.get('type','')}",
         f"[bold cyan]Year:[/bold cyan] {year(item)}",
         f"[bold cyan]Rating:[/bold cyan] {rating(item)}",
         f"[bold cyan]Duration:[/bold cyan] {format_duration(item.get('duration'))}",
         f"[bold cyan]Added:[/bold cyan] {format_ts(item.get('addedAt'))}"]
    for key, label in [("studio","Studio"),("contentRating","Content Rating")]:
        if item.get(key):
            L.append(f"[bold cyan]{label}:[/bold cyan] {item[key]}")
    for key, label, sub in [("Genre","Genres","tag"),("Director","Director","tag"),("Role","Cast","tag")]:
        vals = ", ".join(e[sub] for e in item.get(key, [])[:5])
        if vals:
            L.append(f"[bold cyan]{label}:[/bold cyan] {vals}")
    for media in item.get("Media", []):
        for part in media.get("Part", []):
            L.append(f"[bold cyan]File:[/bold cyan] [dim]{part.get('file','—')}[/dim]")
            L.append(f"[bold cyan]Size:[/bold cyan] {format_size(part.get('size'))}")
        L.append(f"[bold cyan]Video:[/bold cyan] {media.get('videoCodec','?').upper()} "
                 f"{media.get('videoResolution','?')}p  "
                 f"[bold cyan]Audio:[/bold cyan] {media.get('audioCodec','?').upper()} "
                 f"{media.get('audioChannels','?')}ch")
    console.print(Panel("\n".join(L) + f"\n\n[italic dim]{synopsis}[/italic dim]",
                        title=f"[bold white]{item.get('title','Unknown')}[/bold white]", border_style="cyan"))

def _distribution_table(title: str, counts: Counter, cap: int = 0):
    """Print a Label / Count / Share table from a Counter."""
    total = sum(counts.values())
    if not total:
        console.print(f"[yellow]No {title.lower()} data found.[/yellow]")
        return
    t = Table(title=title, box=box.ROUNDED)
    t.add_column(title.split()[0], style="bold cyan")
    t.add_column("Count", justify="right", width=8)
    t.add_column("Share", justify="right", width=8)
    for label, cnt in (counts.most_common(cap) if cap else counts.most_common()):
        t.add_row(label, str(cnt), f"{cnt / total * 100:.1f}%")
    console.print(t)
    if cap and len(counts) > cap:
        console.print(f"[dim]Showing top {cap} of {len(counts)}.[/dim]")

# ── CLI ───────────────────────────────────────────────────────────────────────

_HELP_SECTIONS = [
    ("Dashboard", [
        ("health",                "",                                "One-page health summary: library stats, zero-duration, stale shows, Radarr sync and upgrade gaps"),
        ("watch_next",            "[--count N] [--library id] [--runtime min] [--genre g] [--type movie|episode]",
                                                                 "Ranked picks from On Deck, unwatched ratings, recency, and runtime filters"),
        ("quality_upgrade_plan",  "[--limit N] [--movies|--shows]", "Prioritize Radarr/Sonarr quality upgrades using Plex watch stats"),
        ("premiere_watchlist",    "[--days N] [--past N] [--missing-only]", "Radarr release calendar with Plex indexed/missing status"),
        ("smart_recommendations", "[--count N] [--min-rating R]",   "Personalized movie picks from Tautulli history × TMDB (filters against Plex + Radarr)"),
        ("trending_in_library",   "[--window day|week]",            "TMDB trending movies split by owned / not owned (default: week)"),
        ("tmdb_movie",            "<title>",                        "Search TMDB by title and display full details (rating, cast, crew, budget, Plex/Radarr status)"),
        ("director_deep_dive",    "<name> [--sort-rating]",         "Full filmography: Plex ownership, Tautulli watch status, Radarr presence"),
        ("actor_deep_dive",       "<name> [--sort-rating]",         "Acting credits with character names: Plex ownership, Tautulli watch status, Radarr presence"),
    ]),
    ("Basic", [
        ("status",          "",                                  "Server info and version"),
        ("libraries",       "",                                  "List all media libraries"),
        ("browse",          "<id>",                              "Browse a library by ID"),
        ("search",          "[query] [--title s] [--actor n] [--director n] [--genre n] [--studio n] [--year Y] [--library id] [--type t]",
                                                                 "Search content"),
        ("info",            "<key>",                             "Detailed info for an item"),
        ("sessions",        "",                                  "Active playback sessions"),
        ("recent",          "[count]",                           "Recently added content"),
        ("ondeck",          "",                                  "Continue watching"),
        ("ondeck_clear",    "",                                  "Interactively remove items from On Deck (mark watched or reset progress)"),
        ("hubs",            "[--library <name>] [--count N]",    "Home screen hubs: continue watching, recently added, top rated, etc."),
        ("children",        "<key>",                             "Seasons / episodes for a show"),
        ("url",             "<key>",                             "Print stream URL for an item"),
        ("token",           "<token>",                           "Set or update your Plex token"),
    ]),
    ("Library health", [
        ("dupes",           "",                                  "Items Plex flagged as duplicate files"),
        ("dupetitles",      "",                                  "Items sharing the same title and year"),
        ("duplicates_smart","[--tolerance s] [--match-name] [--library name]","--match-name alone: title-only search; --tolerance: duration grouping (±30s default)"),
        ("missing",         "",                                  "Items with incomplete metadata"),
        ("quality",         "",                                  "Resolution breakdown per library"),
        ("orphans",         "",                                  "Items with no associated media files"),
        ("zero_duration",   "[library_id]",                      "Items with no detected duration (likely corrupt)"),
    ]),
    ("Watch statistics", [
        ("stats",           "",                                  "Library totals and watch history summary"),
        ("history",         "[user] [count]",                    "Recent watch history"),
        ("unwatched",       "[library_id]",                      "Content never played"),
        ("toprated",        "[library_id]",                      "Highest-rated items"),
        ("popularity",      "[library_id]",                      "Most-watched titles ranked by play count"),
        ("watch_calendar",    "[days]",                          "Day-by-day view of what was watched (default 7)"),
        ("watched_by_decade", "[count]",                         "Which release decade you watch most, from play history"),
        ("recommendations",   "[library_id]",                    "Highly rated unwatched content (≥7.5)"),
        ("rewatched",         "[library_id]",                    "Titles played more than once, ranked by play count"),
        ("show_progress",     "[library_id]",                    "Every TV show with watched %, episode counts, last watched"),
        ("binge_candidates",  "[library_id]",                    "TV shows ranked by number of unwatched episodes"),
        ("overdue",           "[months] [library_id]",           "Items added >N months ago never played (default 12)"),
        ("added_trend",       "[months]",                        "Items added per month — library growth over time (default 12)"),
    ]),
    ("Storage", [
        ("largest",         "[count] [--library name]",           "Titles with the biggest file sizes"),
        ("smallest",        "[count] [--library name]",           "Titles with the smallest file sizes"),
        ("tv_storage",      "[--library name] [--count N]",       "Disk usage grouped by TV show"),
        ("tvlargest",       "[count] [--library name]",           "TV shows with the most total disk usage"),
        ("tvsmallest",      "[count] [--library name]",           "TV shows with the least total disk usage"),
        ("longest",         "[count] [--library name]",           "Titles with the longest runtime"),
        ("shortest",        "[count] [--library name]",           "Titles with the shortest runtime"),
        ("storage",         "",                                  "Disk usage breakdown by library"),
        ("bycodec",         "<codec>",                           "Titles using a given video or audio codec"),
        ("codecs",          "",                                  "Video / audio codec distribution"),
        ("transcode",       "",                                  "Items likely to require transcoding"),
    ]),
    ("Collection tools", [
        ("export",          "<library_id> [file]",               "Export library to CSV or JSON"),
        ("fixtitles",       "[library_id]",                      "Find and fix filename-style titles"),
        ("settitle",        "<key> <title>",                     "Manually set the title for one item"),
        ("stale",           "[months]",                          "Shows with no updates in N months"),
    ]),
    ("Monitoring", [
        ("watch",           "[seconds]",                         "Live-refresh sessions (Ctrl+C to stop)"),
        ("alert",           "[seconds]",                         "Alert when a transcode session starts"),
        ("activities",      "",                                  "Show currently running background tasks"),
        ("logs",            "[lines] [--level debug|info|warn|error]", "Show recent server log entries"),
    ]),
    ("Users", [
        ("sharing",         "",                                  "Libraries each managed user can access"),
    ]),
    ("Playback control", [
        ("clients",         "",                                  "List available Plex clients"),
        ("play",            "<key> [--client name]",             "Start playback on a client"),
        ("pause",           "[session]",                         "Pause a session"),
        ("resume",          "[session]",                         "Resume a paused session"),
        ("stop",            "[session]",                         "Stop a session"),
    ]),
    ("Analysis & reports", [
        ("refresh",         "[library_id] [--force]",             "Scan one or all libraries; --force re-downloads metadata from agents"),
        ("analyze",         "<key> | --library <id>",            "Trigger deep media analysis"),
        ("report",          "[--html filename.html]",            "Comprehensive library report"),
        ("changelog",       "[days]",                            "Everything added/updated in last N days"),
    ]),
    ("Ratings & Tags", [
        ("setrating",       "<key> <0-10>",                      "Set user rating on an item"),
        ("bygenre",         "<genre> [library_id]",              "Browse items by genre"),
        ("byactor",         "<name> [library_id]",               "Browse items by actor"),
        ("bydirector",      "<name> [library_id]",               "Browse items by director"),
        ("byyear",          "<year> [library_id]",               "Browse items by release year"),
        ("bycontentrating", "<rating> [library_id]",             "Browse items by content rating (PG-13, TV-MA, etc.)"),
        ("byresolution",    "<res> [library_id]",                "Browse items by resolution (4K, 1080p, 720p, SD)"),
        ("director_stats",  "[library_id]",                      "Directors ranked by titles owned, with watched counts"),
        ("actor_stats",     "[library_id]",                      "Actors ranked by titles owned, with watched counts"),
    ]),
    ("Deeper analysis", [
        ("bitrate",         "[library_id]",                      "Bitrate distribution with outlier flagging"),
        ("subtitles",       "[library_id]",                      "Items missing subtitle tracks"),
        ("hdr",             "[library_id]",                      "List HDR and Dolby Vision content"),
        ("audioformat",     "<format>",                          "Items with a specific audio format"),
        ("multiversion",    "[library_id]",                      "Items with more than one media version"),
        ("genres",          "[library_id]",                      "Genre distribution across libraries"),
        ("studios",         "[library_id]",                      "Studio distribution across libraries"),
        ("decade",          "[library_id]",                      "Content count broken down by decade of release"),
        ("content_rating",  "[library_id]",                      "Content rating distribution (G, PG, R, TV-MA, etc.)"),
        ("missing_episodes",  "[library_id]",                    "TV seasons with gaps in episode numbering"),
        ("incomplete_seasons","[library_id]",                    "Seasons with fewer episodes than the show's typical season"),
        ("abandoned",         "[threshold%] [--library id]",     "Shows started but not finished (default <80% watched)"),
        ("duration_outliers", "[library_id]",                    "TV episodes with runtime far from the show's median"),
        ("4k_audit",          "[library_id]",                    "4K content breakdown by HDR type, audio, and codec"),
        ("framerate",         "[library_id]",                    "Content broken down by frame rate"),
        ("aspect_ratio",      "[library_id]",                    "Video aspect ratio distribution (16:9, 2.35:1, 4:3, etc.)"),
        ("audio_languages",   "[library_id]",                    "Audio track language breakdown across the library"),
        ("resolution_trend",  "[library_id]",                    "4K/1080p/720p/SD share by year items were added"),
        ("container_format",  "[library_id]",                    "Distribution of file container formats (mkv, mp4, avi, etc.)"),
        ("size_by_codec",     "[library_id]",                    "Total and average file size grouped by video codec"),
        ("codec_migration_plan", "[--library id] [--count N] [--min-size GB] [--target hevc|av1]",
                                                                 "Legacy-codec files ranked by estimated savings and watch popularity"),
        ("oversized",            "[--library id] [--4k GB] [--1080p GB] [--720p GB] [--sd GB] [--count N]",
                                                                 "Files above size thresholds per resolution (4K>80 GB, 1080p>30 GB, 720p>15 GB, SD>8 GB)"),
        ("bitrate_outliers",     "[--library id] [--count N]",   "Files with bitrate above expected max per resolution (4K>25, 1080p>12, 720p>6, SD>3 Mbps)"),
        ("channel_dist",      "[library_id]",                    "Audio channel layout distribution (stereo, 5.1, 7.1, etc.)"),
    ]),
    ("Item extras", [
        ("extras",          "<key>",                             "Trailers, featurettes, and interviews"),
        ("related",         "<key>",                             "Related / recommended content"),
    ]),
    ("Users & sharing", [
        ("users",           "",                                  "List all server accounts"),
        ("userstats",       "[username]",                        "Watch stats per user, or detail for one user"),
    ]),
    ("Playlists & Collections", [
        ("playlists",       "",                                  "List all playlists"),
        ("playlist",        "<id>",                              "Show playlist contents"),
        ("playlist_create", "<name> [key]",                      "Create a new playlist"),
        ("playlist_add",    "<playlist_id> <key>",               "Add an item to a playlist"),
        ("playlist_remove", "<playlist_id> <item_id>",           "Remove an item from a playlist"),
        ("collections",     "[library_id]",                      "List collections (all or by library)"),
        ("collection",      "<key>",                             "Show items in a collection"),
        ("collection_gaps",  "[--import] [--dry-run] [--limit N]", "TMDB franchise gaps across Plex and Radarr; optionally import Radarr gaps"),
        ("playlist_build",  "<name> [--genre g] [--director d] [--actor a] [--year y] [--decade Xd] [--studio s] [--contentrating r] [--unwatched] [--rating n] [--library id] [--limit n]",
                                                                 "Build a playlist from combined filters"),
    ]),
    ("Sonarr", [
        ("sonarr_status",  "",           "Sonarr connection info, quality profiles, root folders"),
        ("sonarr_sync",    "",           "Find Sonarr downloads missing from Plex, and Plex shows not in Sonarr"),
        ("sonarr_missing", "",           "Shows with monitored but missing episodes; select to trigger search"),
        ("sonarr_upgrade", "",           "Shows with episodes below quality cutoff; select to trigger re-search"),
        ("sonarr_add",     "<name>",     "Search for a TV show and add it to Sonarr"),
    ]),
    ("Tautulli", [
        ("tautulli_status",  "",                              "Tautulli connection info"),
        ("tautulli_history", "[--user <name>] [--count N]",  "Rich play history: stream type, duration, platform"),
        ("tautulli_stats",   "[--days N]",                   "Top movies, shows, and users (default 30 days)"),
        ("tautulli_plays",   "[--days N]",                   "Plays per day as a bar chart (default 30 days)"),
        ("tautulli_users",   "",                              "All users ranked by total plays and watch time"),
    ]),
    ("Radarr", [
        ("radarr_status",   "",                                                "Radarr connection info, quality profiles, root folders"),
        ("radarr_lists",       "",                                             "Show available named TMDB lists"),
        ("radarr_list_info",   "<id|url>",                                       "Show name, description and item count for any TMDB list ID or URL"),
        ("radarr_list_add",    "<name> <id|url>",                              "Add or update a named list in the config"),
        ("radarr_list_remove", "<name>",                                       "Remove a named list from the config"),
        ("radarr_list",        "<name>",                                       "Preview a list: title/year/in-Radarr/in-Plex (fetched live from TMDB)"),
        ("radarr_import",   "<name> [--dry-run] [--profile <id>] [--search]", "Add missing movies from a named list to Radarr"),
        ("radarr_director", "<name> [--dry-run] [--profile <id>] [--search]", "Import a director's filmography from TMDB into Radarr"),
        ("radarr_download", "<name> [--dry-run] [--profile <id>]",            "Search Radarr for all missing movies from a list"),
        ("radarr_pick",    "<name> [--profile <id>]",                         "Interactively scroll and select movies to download"),
        ("radarr_upgrade",    "",                                                "List movies below quality cutoff; select to trigger upgrade searches"),
        ("radarr_upgrade_4k", "<title> [--search]",                             "Switch a movie's quality profile to 4K and optionally trigger a search"),
        ("radarr_sync",        "",           "Find Radarr downloads missing from Plex, and Plex movies not in Radarr"),
        ("radarr_collections", "[--import]", "TMDB franchise completion: show what's missing per series; --import adds them"),
        ("radarr_calendar",    "[--days N]", "Upcoming movie releases for monitored titles (default 30 days)"),
    ]),
    ("Maintenance", [
        ("plex_metadata",    "[--library <name>] [--fix] [--episodes]",
                                                                 "Audit library for missing metadata fields; --fix triggers Plex refresh"),
        ("plex_duplicates",  "[--library <name>]",               "Find duplicate movies; shows resolution, size, and reclaimable space"),
    ]),
    ("Shell", [
        ("help",            "",                                  "Show this help"),
        ("quit / exit",     "",                                  "Exit"),
    ]),
]

class PlexShell(cmd.Cmd):
    intro = ""
    prompt = "[plex]> "
    ruler = ""

    _AUDIO_FORMATS = ("aac", "ac3", "eac3", "dts", "truehd", "flac", "mp3", "opus", "atmos", "vorbis", "pcm")
    _VIDEO_CODECS  = ("h264", "hevc", "h265", "av1", "mpeg4", "vc1", "vp9", "xvid", "divx")
    _ALL_CODECS    = _AUDIO_FORMATS + _VIDEO_CODECS

    def __init__(self, client: PlexClient):
        super().__init__()
        self.client = client
        try:
            import readline
            readline.set_completer_delims(readline.get_completer_delims().replace("-", ""))
        except ImportError:
            pass

    def emptyline(self): pass
    def default(self, line: str):
        console.print(f"[red]Unknown command:[/red] {line}  (type [yellow]help[/yellow])")

    def do_help(self, _):
        t = Table(box=None, show_header=False, padding=(0, 2), expand=False)
        t.add_column(style="yellow", no_wrap=True, min_width=16)
        t.add_column(style="dim", min_width=14)
        t.add_column(min_width=20)
        for section, commands in _HELP_SECTIONS:
            t.add_row(f"[bold cyan]{section}[/bold cyan]", "", "")
            for cmd_name, args, desc in commands:
                t.add_row(cmd_name, args, desc)
            t.add_row("", "", "")
        console.print(t)

    def do_quit(self, _):
        console.print("[dim]Goodbye.[/dim]")
        return True
    do_exit = do_quit
    def do_EOF(self, _):
        console.print()
        return self.do_quit(_)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _libs_for(self, section_id: str | None) -> list[dict]:
        """Return a one-element list for a section id, or all libraries."""
        if section_id:
            return [{"key": section_id, "title": f"Library {section_id}"}]
        return self.client.libraries()

    def _all_items(self, section_id: str | None) -> list[dict]:
        """All items from one library or all libraries."""
        if section_id:
            return self.client.library_contents(section_id)
        return [item for d in self.client.all_items_by_library().values() for item in d["items"]]

    def _browse_by(self, arg: str, field: str, label: str):
        """Shared implementation for bygenre / byactor / bydirector."""
        try:
            parts = shlex.split(arg.strip()) if arg.strip() else []
        except ValueError:
            parts = arg.strip().split()
        if not parts:
            console.print(f"[yellow]Usage: {label.lower()} <{field}> [library_id][/yellow]")
            return
        if len(parts) > 1 and parts[-1].isdigit():
            value, section_id = " ".join(parts[:-1]), parts[-1]
        else:
            value, section_id = " ".join(parts), None
        libs = self._libs_for(section_id)
        with console.status(f"Browsing by {field} [cyan]{value}[/cyan]..."):
            results = []
            for lib in libs:
                results.extend(self.client.section_search(lib.get("key", ""), **{field: value}))
        print_media_table(results, f"{label}: {value}")

    def _history_table(self, records: list, title: str):
        """Render a watch-history table."""
        t = Table(title=title, box=box.ROUNDED)
        t.add_column("When", style="dim", width=17)
        t.add_column("User", style="cyan", width=14)
        t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Type", style="yellow", width=8)
        for r in records:
            parent = r.get("grandparentTitle", "")
            title_str = r.get("title", "")
            t.add_row(format_ts(r.get("viewedAt")), r.get("User", {}).get("title", "—"),
                      f"{parent} — {title_str}" if parent else title_str, r.get("type", ""))
        console.print(t)

    def _player_args(self, s: dict) -> tuple:
        p = s.get("Player", {})
        return p.get("machineIdentifier", ""), p.get("address", ""), int(p.get("port") or 0)

    def _pick_session(self, arg: str) -> dict | None:
        sessions = self.client.sessions()
        if not sessions:
            console.print("[yellow]No active sessions.[/yellow]")
            return None
        if arg.strip():
            q = arg.strip().lower()
            matches = [s for s in sessions
                       if q in s.get("sessionKey", "").lower()
                       or q in s.get("Player", {}).get("title", "").lower()
                       or q in (s.get("User", {}).get("title") or "").lower()]
        else:
            matches = sessions
        if len(matches) == 1:
            return matches[0]
        if not matches:
            console.print(f"[yellow]No session matching '{arg}'.[/yellow]")
            return None
        console.print(build_sessions_table(matches))
        choices = {str(i): s for i, s in enumerate(matches, 1)}
        for i, s in choices.items():
            console.print(f"  [dim]{i}.[/dim] {s.get('User',{}).get('title','?')} on {s.get('Player',{}).get('title','?')}")
        return choices.get(Prompt.ask("Select session", choices=list(choices.keys())))

    def _parse_size_args(self, arg: str) -> tuple[int, str]:
        _, flags = parse_search_args(arg)
        return next((int(t) for t in arg.split() if t.isdigit()), 25), flags.get("library", "")

    def _tokens(self, arg: str) -> list[str]:
        try:
            return shlex.split(arg.strip()) if arg.strip() else []
        except ValueError:
            return arg.strip().split()

    def _has_flag(self, tokens: list[str], flag: str) -> bool:
        return flag in tokens

    def _flag_value(self, tokens: list[str], flag: str, default: str = "") -> str:
        if flag not in tokens:
            return default
        idx = tokens.index(flag)
        if idx + 1 >= len(tokens) or tokens[idx + 1].startswith("--"):
            return default
        return tokens[idx + 1]

    def _int_flag(self, tokens: list[str], flag: str, default: int,
                  minimum: int = 0) -> int:
        raw = self._flag_value(tokens, flag, "")
        if not raw:
            return default
        try:
            return max(minimum, int(raw))
        except ValueError:
            return default

    def _configured_radarr_client(self) -> "RadarrClient | None":
        cfg = load_config()
        url = cfg.get("radarr_url", "") or os.environ.get("RADARR_URL", "")
        key = cfg.get("radarr_api_key", "") or os.environ.get("RADARR_API_KEY", "")
        return RadarrClient(url, key) if url and key else None

    def _configured_sonarr_client(self) -> "SonarrClient | None":
        cfg = load_config()
        url = cfg.get("sonarr_url", "") or os.environ.get("SONARR_URL", "")
        key = cfg.get("sonarr_api_key", "") or os.environ.get("SONARR_API_KEY", "")
        return SonarrClient(url, key) if url and key else None

    def _get_radarr_client(self) -> "RadarrClient | None":
        cfg = load_config()
        url = cfg.get("radarr_url", "") or os.environ.get("RADARR_URL", "")
        key = cfg.get("radarr_api_key", "") or os.environ.get("RADARR_API_KEY", "")
        if not url or not key:
            console.print(Panel(
                "Radarr URL and API key are required.\n\n"
                "Find your API key: Radarr → Settings → General → Security → API Key",
                title="[yellow]Radarr Setup[/yellow]", border_style="yellow"))
            url = Prompt.ask("[yellow]Radarr URL[/yellow]", default="http://localhost:7878")
            key = Prompt.ask("[yellow]Radarr API Key[/yellow]")
            if not url or not key:
                console.print("[red]Radarr URL and API key are required.[/red]"); return None
            cfg["radarr_url"] = url; cfg["radarr_api_key"] = key; save_config(cfg)
        rc = RadarrClient(url, key)
        with console.status("Connecting to Radarr..."):
            info = rc.status()
        if not info:
            return None
        return rc

    def _get_tautulli_client(self) -> "TautulliClient | None":
        cfg = load_config()
        url = cfg.get("tautulli_url", "") or os.environ.get("TAUTULLI_URL", "")
        key = cfg.get("tautulli_api_key", "") or os.environ.get("TAUTULLI_API_KEY", "")
        if not url or not key:
            console.print(Panel(
                "Tautulli URL and API key are required.\n\n"
                "Find your API key: Tautulli → Settings → Web Interface → API Key",
                title="[yellow]Tautulli Setup[/yellow]", border_style="yellow"))
            url = Prompt.ask("[yellow]Tautulli URL[/yellow]", default="http://localhost:8181")
            key = Prompt.ask("[yellow]Tautulli API Key[/yellow]")
            if not url or not key:
                console.print("[red]Tautulli URL and API key are required.[/red]"); return None
            cfg["tautulli_url"] = url; cfg["tautulli_api_key"] = key; save_config(cfg)
        tc = TautulliClient(url, key)
        with console.status("Connecting to Tautulli..."):
            info = tc.server_info()
        if not info:
            return None
        return tc

    def _get_sonarr_client(self) -> "SonarrClient | None":
        cfg = load_config()
        url = cfg.get("sonarr_url", "") or os.environ.get("SONARR_URL", "")
        key = cfg.get("sonarr_api_key", "") or os.environ.get("SONARR_API_KEY", "")
        if not url or not key:
            console.print(Panel(
                "Sonarr URL and API key are required.\n\n"
                "Find your API key: Sonarr → Settings → General → Security → API Key",
                title="[yellow]Sonarr Setup[/yellow]", border_style="yellow"))
            url = Prompt.ask("[yellow]Sonarr URL[/yellow]", default="http://localhost:8989")
            key = Prompt.ask("[yellow]Sonarr API Key[/yellow]")
            if not url or not key:
                console.print("[red]Sonarr URL and API key are required.[/red]"); return None
            cfg["sonarr_url"] = url; cfg["sonarr_api_key"] = key; save_config(cfg)
        sc = SonarrClient(url, key)
        with console.status("Connecting to Sonarr..."):
            info = sc.status()
        if not info:
            return None
        return sc

    def _plex_show_set(self) -> set[tuple[str, int]]:
        result: set[tuple[str, int]] = set()
        for lib in self.client.libraries():
            if lib.get("type") != "show":
                continue
            for item in self.client.library_contents(lib.get("key", "")):
                t = (item.get("title") or "").lower().strip()
                y = item.get("year") or 0
                if t:
                    result.add((t, y))
        return result

    def _in_plex_show(self, title: str, plex_show_set: set) -> bool:
        t = title.lower().strip()
        for pt, _ in plex_show_set:
            if t == pt or SequenceMatcher(None, t, pt).ratio() >= 0.88:
                return True
        return False

    def _get_tmdb_client(self) -> "TMDBClient | None":
        cfg = load_config()
        key = cfg.get("tmdb_api_key", "") or os.environ.get("TMDB_API_KEY", "")
        if not key:
            console.print(Panel(
                "A TMDB API key is required for director and list lookups.\n\n"
                "Get a free key: themoviedb.org → Settings → API",
                title="[yellow]TMDB Setup[/yellow]", border_style="yellow"))
            key = Prompt.ask("[yellow]TMDB API Key[/yellow]")
            if not key:
                console.print("[red]TMDB API key is required.[/red]"); return None
            cfg["tmdb_api_key"] = key; save_config(cfg)
        return TMDBClient(key)

    def _plex_movie_set(self) -> set[tuple[str, int]]:
        result: set[tuple[str, int]] = set()
        for lib in self.client.libraries():
            if lib.get("type") != "movie":
                continue
            for item in self.client.library_contents(lib.get("key", "")):
                t = (item.get("title") or "").lower().strip()
                y = item.get("year") or 0
                if t:
                    result.add((t, y))
        return result

    def _in_plex(self, title: str, year: int, plex_set: set) -> bool:
        t = title.lower().strip()
        if (t, year) in plex_set:
            return True
        for pt, py in plex_set:
            if abs(py - year) > 1:
                continue
            if SequenceMatcher(None, t, pt).ratio() >= 0.90:
                return True
            # Handle "Star Wars" ↔ "Star Wars: Episode IV - A New Hope"
            shorter, longer = (t, pt) if len(t) <= len(pt) else (pt, t)
            if longer.startswith(shorter + ":") or longer.startswith(shorter + " -"):
                return True
        return False

    def _radarr_import_workflow(self, movies: list[dict], rc: "RadarrClient",
                                dry_run: bool, profile_id: int | None,
                                search: bool) -> None:
        with console.status("Loading Radarr state..."):
            existing_ids = {m.get("tmdbId") for m in rc.movies()}
            profiles     = rc.quality_profiles()
            folders      = rc.root_folders()
        if not profiles:
            console.print("[red]No quality profiles found in Radarr.[/red]"); return
        if not folders:
            console.print("[red]No root folders configured in Radarr.[/red]"); return

        to_add = [m for m in movies if m.get("tmdb_id") not in existing_ids]
        if not to_add:
            console.print(f"[green]All {len(movies)} titles are already in Radarr.[/green]"); return

        # Resolve quality profile
        if profile_id is None:
            if len(profiles) == 1:
                profile_id = profiles[0]["id"]
                console.print(f"[dim]Quality profile: {profiles[0]['name']}[/dim]")
            else:
                pt = Table(title="Quality Profiles", box=box.ROUNDED)
                pt.add_column("ID", style="dim", width=6); pt.add_column("Name", style="bold cyan")
                for p in profiles: pt.add_row(str(p["id"]), p["name"])
                console.print(pt)
                profile_id = int(Prompt.ask("Select profile ID",
                                            choices=[str(p["id"]) for p in profiles],
                                            default=str(profiles[0]["id"])))

        # Resolve root folder
        if len(folders) == 1:
            root_folder = folders[0]["path"]
            console.print(f"[dim]Root folder: {root_folder}[/dim]")
        else:
            ft = Table(title="Root Folders", box=box.ROUNDED)
            ft.add_column("ID", style="dim", width=6); ft.add_column("Path", style="bold white")
            for f in folders: ft.add_row(str(f["id"]), f["path"])
            console.print(ft)
            folder_map = {str(f["id"]): f["path"] for f in folders}
            root_folder = folder_map[Prompt.ask("Select root folder ID",
                                                choices=list(folder_map),
                                                default=list(folder_map)[0])]

        # Preview table
        with console.status("Loading Plex movie library..."):
            plex_set = self._plex_movie_set()
        tag = "[dim][DRY RUN][/dim] " if dry_run else ""
        t = Table(title=f"{tag}Adding {len(to_add)} of {len(movies)} movies", box=box.ROUNDED)
        t.add_column("Title",   style="bold white", min_width=32)
        t.add_column("Year",    width=6, justify="right")
        t.add_column("TMDB ID", style="dim", width=9, justify="right")
        t.add_column("In Plex", width=9, justify="center")
        for m in to_add:
            in_plex = self._in_plex(m["title"], m["year"], plex_set)
            t.add_row(m["title"], str(m["year"]), str(m.get("tmdb_id", "")),
                      "[green]yes[/green]" if in_plex else "[dim]no[/dim]")
        console.print(t)

        if dry_run:
            console.print(f"[yellow]Dry run — {len(to_add)} movies would be added. "
                          "Re-run without --dry-run to import.[/yellow]"); return

        if Prompt.ask(f"Add {len(to_add)} movies to Radarr?",
                      choices=["y", "n"], default="n") != "y":
            console.print("[dim]Cancelled.[/dim]"); return

        ok = fail = already = 0
        for m in to_add:
            with console.status(f"Adding [cyan]{m['title']}[/cyan]..."):
                result = rc.add_movie(m["tmdb_id"], m["title"], m["year"],
                                      profile_id, root_folder, search_on_add=search)
            if result.get("id"):
                ok += 1
            elif "already" in str(result).lower() or result.get("isExisting"):
                already += 1
            else:
                fail += 1
                console.print(f"[red]Failed:[/red] {m['title']} — {result}")

        parts = [f"[green]{ok} added[/green]"]
        if already: parts.append(f"[dim]{already} already existed[/dim]")
        if fail:    parts.append(f"[red]{fail} failed[/red]")
        console.print(", ".join(parts) + ".")
        if search and ok:
            console.print("[dim]Radarr will search for files in the background.[/dim]")

    # ── Basic commands ────────────────────────────────────────────────────────

    def do_status(self, _):
        info = self.client.server_info()
        if not info:
            return
        console.print(Panel(
            f"[bold cyan]Server:[/bold cyan] {info.get('friendlyName', 'Unknown')}\n"
            f"[bold cyan]Version:[/bold cyan] {info.get('version', '—')}\n"
            f"[bold cyan]Platform:[/bold cyan] {info.get('platform', '—')} {info.get('platformVersion', '')}\n"
            f"[bold cyan]My Plex:[/bold cyan] {'[green]yes[/green]' if info.get('myPlex') else '[dim]no[/dim]'}\n"
            f"[bold cyan]URL:[/bold cyan] {self.client.base_url}",
            title="[bold white]Plex Media Server[/bold white]", border_style="green"))

    def do_libraries(self, _):
        libs = self.client.libraries()
        if libs:
            print_libraries(libs)

    def do_browse(self, arg: str):
        if not arg.strip():
            libs = self.client.libraries()
            if libs:
                print_libraries(libs)
                console.print("[dim]Usage: browse <id>[/dim]")
            return
        print_media_table(self.client.library_contents(arg.strip()), f"Library {arg.strip()}")

    def do_search(self, arg: str):
        if not arg.strip():
            console.print(
                "[yellow]Usage:[/yellow] search [dim][query][/dim] "
                "[dim][--title substring] [--actor name] [--director name] [--genre name] "
                "[--studio name] [--year YYYY] [--library id] [--type movie|show|episode][/dim]\n"
                "[dim]  query        smart search (indexed, misses tokens like '1080p')[/dim]\n"
                "[dim]  --title      literal substring match against the title field[/dim]")
            return
        query, filters = parse_search_args(arg.strip())
        section_id = filters.pop("library", None)
        type_filter = filters.pop("type", None)
        label_parts = [f'"{query}"'] if query else []
        label_parts += [f"--{k} {v}" for k, v in filters.items()]
        if type_filter:
            label_parts.append(f"--type {type_filter}")
        title_substring = filters.pop("title", None)
        tag_filters = {"actor", "director", "genre"}
        if not query and not title_substring and filters.keys() & tag_filters:
            console.print("[yellow]--actor, --director, and --genre require a title query[/yellow]")
            return
        if title_substring:
            libs = [{"key": section_id}] if section_id else self.client.libraries()
            with console.status(f"Scanning for title containing [cyan]{title_substring}[/cyan]..."):
                results = []
                for lib in libs:
                    results.extend(self.client.title_filter(lib.get("key", ""), title_substring))
        elif not filters and not section_id:
            if not query:
                console.print("[yellow]Provide a query or at least one filter flag.[/yellow]")
                return
            with console.status(f"Searching [cyan]{query}[/cyan]..."):
                results = self.client.search(query)
        elif section_id:
            with console.status(f"Searching library {section_id}..."):
                results = self.client.section_search(section_id, query, **filters)
        else:
            with console.status("Searching all libraries..."):
                results = []
                for lib in self.client.libraries():
                    results.extend(self.client.section_search(lib.get("key", ""), query, **filters))
        if type_filter:
            results = [r for r in results if r.get("type", "").lower() == type_filter.lower()]
        print_media_table(results, "Search: " + " ".join(label_parts))

    def do_info(self, arg: str):
        if not arg.strip():
            console.print("[yellow]Usage: info <key>[/yellow]")
            return
        with console.status("Fetching..."):
            item = self.client.metadata(arg.strip())
        print_item_detail(item)

    def do_sessions(self, _):      print_sessions(self.client.sessions())
    def do_recent(self, arg: str):
        count = int(arg.strip()) if arg.strip().isdigit() else 20
        with console.status("Fetching recently added..."):
            items = self.client.recent(count)
        print_media_table(items, f"Recently Added (last {count})")

    def do_ondeck(self, _):
        with console.status("Fetching on deck..."):
            items = self.client.on_deck()
        print_media_table(items, "On Deck")

    def do_watch_next(self, arg: str):
        """watch_next [--count N] [--library id] [--runtime min] [--genre g] [--type movie|episode]"""
        tokens = self._tokens(arg)
        if self._has_flag(tokens, "--help"):
            console.print("[yellow]Usage: watch_next [--count N] [--library id] "
                          "[--runtime minutes] [--genre name] [--type movie|episode][/yellow]")
            return
        count = self._int_flag(tokens, "--count", 15, minimum=1)
        runtime_limit = self._int_flag(tokens, "--runtime", 0, minimum=0)
        library_filter = self._flag_value(tokens, "--library", "").lower()
        genre_filter = self._flag_value(tokens, "--genre", "").lower()
        type_filter = self._flag_value(tokens, "--type", "").lower()
        if type_filter and type_filter not in ("movie", "episode"):
            console.print("[yellow]--type must be movie or episode.[/yellow]")
            return

        def _rating_value(item: dict) -> float | None:
            raw = item.get("audienceRating")
            if raw is None:
                raw = item.get("rating")
            try:
                return float(raw)
            except (TypeError, ValueError):
                return None

        def _has_genre(item: dict) -> bool:
            if not genre_filter:
                return True
            return any(genre_filter in (g.get("tag") or "").lower()
                       for g in item.get("Genre", []))

        def _lib_matches(lib: dict) -> bool:
            if not library_filter:
                return True
            return (library_filter == str(lib.get("key", "")).lower()
                    or library_filter in (lib.get("title", "") or "").lower())

        now = int(time.time())
        cutoff_recent = now - 45 * 86400
        candidates: dict[str, dict] = {}

        with console.status("Scoring watch-next candidates..."):
            on_deck = self.client.on_deck()
            on_deck_keys = {str(i.get("ratingKey", "")) for i in on_deck if i.get("ratingKey")}
            libs = [lib for lib in self.client.libraries()
                    if lib.get("type") in ("movie", "show") and _lib_matches(lib)]

            for lib in libs:
                lib_type = lib.get("type", "")
                if type_filter == "movie" and lib_type != "movie":
                    continue
                if type_filter == "episode" and lib_type != "show":
                    continue
                items = (self.client.library_episodes(lib.get("key", ""))
                         if lib_type == "show" else self.client.library_contents(lib.get("key", "")))
                for item in items:
                    item_type = item.get("type", "")
                    if item_type not in ("movie", "episode"):
                        continue
                    if type_filter and item_type != type_filter:
                        continue
                    duration = item.get("duration") or 0
                    if runtime_limit and duration > runtime_limit * 60_000:
                        continue
                    if not _has_genre(item):
                        continue

                    rating_val = _rating_value(item)
                    view_count = item.get("viewCount") or 0
                    view_offset = item.get("viewOffset") or 0
                    key = str(item.get("ratingKey", ""))
                    is_continue = key in on_deck_keys or view_offset > 0
                    if view_count and not is_continue:
                        continue

                    reasons = []
                    score = (rating_val or 0) * 10
                    if is_continue:
                        score += 95
                        reasons.append("continue")
                        if duration:
                            pct = max(0, min(100, int(view_offset / duration * 100)))
                            score += max(0, 20 - abs(45 - pct) // 3)
                    else:
                        score += 30
                        reasons.append("unwatched")

                    added = item.get("addedAt") or 0
                    if added >= cutoff_recent:
                        score += 8
                        reasons.append("recent")
                    if item_type == "episode":
                        score += 6
                        reasons.append("episode")
                    if rating_val is not None and rating_val >= 8:
                        reasons.append("high rated")

                    candidates[key or f"{lib.get('key','')}:{item.get('title','')}"] = {
                        "score": score,
                        "item": item,
                        "library": lib.get("title", ""),
                        "rating": rating_val,
                        "reason": ", ".join(dict.fromkeys(reasons)),
                    }

        picks = sorted(candidates.values(), key=lambda r: (-r["score"], -(r["rating"] or 0),
                                                           r["item"].get("title", "")))[:count]
        if not picks:
            console.print("[yellow]No watch-next candidates matched those filters.[/yellow]")
            return

        t = Table(title=f"Watch Next ({len(picks)} picks)", box=box.ROUNDED)
        t.add_column("#", style="dim", width=4, justify="right")
        t.add_column("Key", style="dim", width=7)
        t.add_column("Title", style="bold white", min_width=30)
        t.add_column("Type", style="yellow", width=8)
        t.add_column("Runtime", width=9, justify="right")
        t.add_column("Rating", width=7, justify="right")
        t.add_column("Why", style="cyan", min_width=16)
        for i, row in enumerate(picks, 1):
            item = row["item"]
            t.add_row(str(i), str(item.get("ratingKey", "")), full_title(item),
                      item.get("type", ""), format_duration(item.get("duration")),
                      f"{row['rating']:.1f}" if row["rating"] is not None else "-",
                      row["reason"])
        console.print(t)

    def do_ondeck_clear(self, _):
        """ondeck_clear — interactively remove items from On Deck (mark watched or reset progress)"""
        try:
            import questionary
        except ImportError:
            console.print("[red]questionary not installed.[/red] Run: pip install questionary"); return

        with console.status("Fetching On Deck..."):
            items = self.client.on_deck()

        if not items:
            console.print("[green]On Deck is empty.[/green]"); return

        def _label(item: dict) -> str:
            pct = int(item.get("viewOffset", 0) / (item.get("duration") or 1) * 100)
            show = item.get("grandparentTitle", "")
            title = item.get("title", "?")
            full = f"{show} — {title}" if show else title
            return f"{full}  [{pct}%]"

        choices = [questionary.Choice(_label(i), value=i) for i in items]
        selected = questionary.checkbox("Select items to remove from On Deck:", choices=choices).ask()
        if not selected:
            console.print("[dim]Nothing selected.[/dim]"); return

        MARK_WATCHED = "Mark as watched"
        action = questionary.select(
            "Action for selected items:",
            choices=[MARK_WATCHED, "Reset progress (as if never started)"],
        ).ask()
        if not action:
            return

        ok = err = 0
        for item in selected:
            rk = item.get("ratingKey", "")
            if not rk:
                err += 1; continue
            success = self.client.scrobble(rk) if action == MARK_WATCHED else self.client.unscrobble(rk)
            if success: ok += 1
            else: err += 1

        verb = "marked watched" if action == MARK_WATCHED else "reset"
        console.print(f"[green]{ok} item(s) {verb}.[/green]"
                      + (f"  [red]{err} failed.[/red]" if err else ""))

    def do_hubs(self, arg: str):
        """hubs [--library <name>] [--count N] — home screen hubs: continue watching, recently added, top rated, etc."""
        tokens = arg.strip().split() if arg.strip() else []

        section_id = ""
        if "--library" in tokens:
            idx = tokens.index("--library")
            if idx + 1 < len(tokens):
                lib_filter = tokens[idx + 1]
                for lib in self._cached_libs():
                    if lib.get("key") == lib_filter or lib.get("title", "").lower() == lib_filter.lower():
                        section_id = lib.get("key", "")
                        break
                if not section_id:
                    console.print(f"[yellow]Library '{lib_filter}' not found.[/yellow]"); return

        count = 10
        if "--count" in tokens:
            idx = tokens.index("--count")
            if idx + 1 < len(tokens):
                try: count = int(tokens[idx + 1])
                except ValueError: pass

        with console.status("Fetching hubs..."):
            hubs_list = self.client.hubs(section_id, count)

        if not hubs_list:
            console.print("[yellow]No hubs returned.[/yellow]"); return

        any_shown = False
        for hub in hubs_list:
            items = hub.get("Metadata") or []
            if not items:
                continue

            hub_title = hub.get("title", "Hub")
            more      = str(hub.get("more", "")).lower() in ("1", "true")

            t = Table(
                title=f"[bold]{hub_title}[/bold]" + ("  [dim]+more[/dim]" if more else ""),
                box=box.SIMPLE_HEAD,
            )
            t.add_column("Title",  style="bold cyan", min_width=30)
            t.add_column("Year",   width=6,  justify="right", style="dim")
            t.add_column("Type",   width=9,  style="dim")
            t.add_column("Rating", width=7,  justify="right", style="dim")

            for item in items:
                parent = item.get("grandparentTitle") or item.get("parentTitle") or ""
                title  = item.get("title", "?")
                label  = f"{parent} — {title}" if parent else title
                yr     = str(item.get("year") or item.get("parentYear") or "")
                itype  = item.get("type", "")
                rat    = str(round(float(item["audienceRating"]), 1)) if item.get("audienceRating") else "—"
                t.add_row(label, yr, itype, rat)

            console.print(t)
            any_shown = True

        if not any_shown:
            console.print("[yellow]All hubs are empty.[/yellow]")

    def do_children(self, arg: str):
        if not arg.strip():
            console.print("[yellow]Usage: children <key>[/yellow]")
            return
        with console.status("Fetching..."):
            items = self.client.children(arg.strip())
        print_media_table(items, f"Children of {arg.strip()}")

    def do_url(self, arg: str):
        if not arg.strip():
            console.print("[yellow]Usage: url <key>[/yellow]")
            return
        console.print(f"[cyan]{self.client.base_url}/library/metadata/{arg.strip()}/stream?X-Plex-Token={self.client.token}[/cyan]")

    def do_token(self, arg: str):
        if not arg.strip():
            console.print("[yellow]Usage: token <your-plex-token>[/yellow]")
            return
        cfg = load_config()
        cfg["token"] = arg.strip()
        save_config(cfg)
        self.client.token = arg.strip()
        self.client.session.params["X-Plex-Token"] = arg.strip()  # type: ignore
        console.print("[green]Token saved.[/green]")

    # ── Library health ────────────────────────────────────────────────────────

    def do_dupetitles(self, _):
        with console.status("Scanning all libraries..."):
            data = self.client.all_items_by_library()
        found_any = False
        for lib_title, d in data.items():
            groups: dict[tuple, list] = defaultdict(list)
            for item in d["items"]:
                tk = (item.get("title") or "").lower().strip()
                if tk:
                    groups[(tk, item.get("year"))].append(item)
            dupes = {k: v for k, v in groups.items() if len(v) > 1}
            if not dupes:
                continue
            found_any = True
            t = Table(title=f"Duplicate Titles in '{lib_title}'", box=box.ROUNDED, show_lines=True)
            t.add_column("Key", style="dim", width=7)
            t.add_column("Title", style="bold white", min_width=24)
            t.add_column("Year", width=6, justify="right")
            t.add_column("Size", width=10, justify="right")
            t.add_column("File", style="dim")
            for items in sorted(dupes.values(), key=lambda v: (v[0].get("title","").lower(), v[0].get("year") or 0)):
                for item in items:
                    parts = [p for m in item.get("Media",[]) for p in m.get("Part",[])]
                    t.add_row(item.get("ratingKey",""), item.get("title",""), year(item),
                              format_size(parts[0].get("size") if parts else None),
                              parts[0].get("file","—") if parts else "—")
                t.add_section()
            console.print(t)
        if not found_any:
            console.print("[green]No duplicate titles found.[/green]")

    def do_dupes(self, _):
        libs = self.client.libraries()
        if not libs:
            return
        found_any = False
        with console.status("Scanning for duplicates..."):
            for lib in libs:
                lid, lib_title = lib.get("key",""), lib.get("title","")
                dupes = self.client.duplicates(lid)
                if not dupes:
                    continue
                found_any = True
                t = Table(title=f"Duplicates in '{lib_title}'", box=box.ROUNDED)
                t.add_column("Key", style="dim", width=7)
                t.add_column("Title", style="bold white", min_width=28)
                t.add_column("Year", width=6)
                t.add_column("Files", justify="right", width=6)
                t.add_column("Total Size", justify="right", width=12)
                for item in dupes:
                    sz = sum(p.get("size",0) for m in item.get("Media",[]) for p in m.get("Part",[]))
                    fc = sum(len(m.get("Part",[])) for m in item.get("Media",[]))
                    t.add_row(item.get("ratingKey",""), item.get("title",""), year(item), str(fc), format_size(sz))
                console.print(t)
        if not found_any:
            console.print("[green]No duplicates found.[/green]")

    def do_missing(self, _):
        with console.status("Scanning all libraries..."):
            data = self.client.all_items_by_library()
        t = Table(title="Incomplete Metadata", box=box.ROUNDED)
        t.add_column("Library", style="cyan", width=16)
        t.add_column("Key", style="dim", width=7)
        t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Missing", style="yellow")
        count = 0
        for lib_title, d in data.items():
            for item in d["items"]:
                missing = []
                if not item.get("summary","").strip(): missing.append("summary")
                if not item.get("thumb") and not item.get("art"): missing.append("poster")
                if not item.get("rating") and not item.get("audienceRating"): missing.append("rating")
                if not item.get("Genre"): missing.append("genres")
                if missing:
                    t.add_row(lib_title, item.get("ratingKey",""), item.get("title",""), ", ".join(missing))
                    count += 1
        if count == 0:
            console.print("[green]All items have complete metadata.[/green]")
        else:
            console.print(t)
            console.print(f"[yellow]{count} items with incomplete metadata.[/yellow]")

    def do_quality(self, _):
        with console.status("Scanning all libraries..."):
            rows = self.client.all_media_rows()
        lib_res: dict = defaultdict(Counter)
        total_res: Counter = Counter()
        for row in rows:
            label = resolution_label(row["videoResolution"])
            lib_res[row["library"]][label] += 1
            total_res[label] += 1
        t = Table(title="Video Quality by Library", box=box.ROUNDED)
        t.add_column("Library", style="cyan")
        for label in ("4K", "1080p", "720p", "SD", "Unknown"):
            t.add_column(label, justify="right", width=8)
        t.add_column("Total", justify="right", width=7)
        for lib_title, counts in sorted(lib_res.items()):
            t.add_row(lib_title,
                      *[str(counts.get(l,0)) if counts.get(l) else "[dim]—[/dim]" for l in ("4K","1080p","720p","SD","Unknown")],
                      str(sum(counts.values())))
        t.add_section()
        gt = sum(total_res.values())
        t.add_row("[bold]TOTAL[/bold]", *[str(total_res.get(l,0)) for l in ("4K","1080p","720p","SD","Unknown")], f"[bold]{gt}[/bold]")
        console.print(t)

    def do_duplicates_smart(self, arg: str):
        _, flags = parse_search_args(arg)
        match_name = bool(flags.get("match-name"))
        library_filter = flags.get("library", "").lower()
        tol_raw = flags.get("tolerance")
        use_duration = tol_raw is not None or not match_name
        tolerance_ms = int(tol_raw) * 1000 if tol_raw is not None else 30_000

        with console.status("Scanning all media..."):
            rows = [r for r in self.client.all_media_rows() if r.get("duration")]
        if library_filter:
            rows = [r for r in rows if library_filter in r["library"].lower()]
            if not rows:
                console.print(f"[yellow]No media found in library '{flags['library']}'.[/yellow]")
                return
        if not rows:
            console.print("[yellow]No media found.[/yellow]")
            return

        THRESHOLD = 0.82

        def _title_matches(group: list[dict]) -> list[dict]:
            keep: set[int] = set()
            for i, r1 in enumerate(group):
                for j in range(i+1, len(group)):
                    if SequenceMatcher(None, r1["title"].lower(), group[j]["title"].lower()).ratio() >= THRESHOLD:
                        keep.update((i, j))
            return [group[k] for k in sorted(keep)] if keep else []

        if use_duration:
            rows.sort(key=lambda r: r["duration"])
            groups: list[list[dict]] = []
            current: list[dict] = [rows[0]]
            for row in rows[1:]:
                if row["duration"] - current[0]["duration"] <= tolerance_ms:
                    current.append(row)
                else:
                    if len(current) > 1:
                        groups.append(current)
                    current = [row]
            if len(current) > 1:
                groups.append(current)
            groups = [g for g in groups if len({r["ratingKey"] for r in g}) > 1]
            if match_name:
                groups = [m for g in groups if (m := _title_matches(g))]
        else:
            used: set[int] = set()
            groups = []
            with console.status("Comparing titles..."):
                for i, r1 in enumerate(rows):
                    if i in used:
                        continue
                    idxs = [i]
                    for j in range(i+1, len(rows)):
                        if j not in used and SequenceMatcher(None, r1["title"].lower(), rows[j]["title"].lower()).ratio() >= THRESHOLD:
                            idxs.append(j)
                    if len(idxs) > 1:
                        group = [rows[k] for k in idxs]
                        if len({r["ratingKey"] for r in group}) > 1:
                            used.update(idxs)
                            groups.append(group)

        if not groups:
            console.print("[green]No smart duplicates found.[/green]")
            return
        mode = " (duration + title match)" if use_duration and match_name else \
               f" (duration ±{tolerance_ms//1000}s)" if use_duration else " (title match)"
        total = sum(len(g) for g in groups)
        console.print(f"[yellow]{len(groups)} potential duplicate groups{mode} ({total} items)[/yellow]\n")
        for group in groups:
            t = Table(box=box.ROUNDED, show_lines=True)
            t.add_column("Key", style="dim", width=7)
            t.add_column("Title", style="bold white", min_width=24)
            t.add_column("Library", style="cyan", width=14)
            t.add_column("Duration", width=10, justify="right")
            t.add_column("Size", width=10, justify="right")
            t.add_column("Video", width=8)
            t.add_column("File", style="dim")
            for r in group:
                t.add_row(r["ratingKey"], r["title"], r["library"], format_duration(r["duration"]),
                          format_size(r["size"]), r["videoCodec"].upper() or "?", r["file"])
            console.print(t)

    def do_orphans(self, _):
        with console.status("Scanning all libraries..."):
            data = self.client.all_items_by_library()
        t = Table(title="Orphaned Items (no media files)", box=box.ROUNDED)
        t.add_column("Library", style="cyan", width=16)
        t.add_column("Key", style="dim", width=7)
        t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Type", style="yellow", width=10)
        SKIP = {"show", "season", "artist", "album", "collection"}
        count = 0
        for lib_title, d in data.items():
            for item in d["items"]:
                if item.get("type") in SKIP:
                    continue
                if not any(m.get("Part") for m in item.get("Media", [])):
                    t.add_row(lib_title, item.get("ratingKey",""), item.get("title",""), item.get("type",""))
                    count += 1
        if count == 0:
            console.print("[green]No orphaned items found.[/green]")
        else:
            console.print(t)
            console.print(f"[yellow]{count} orphaned items.[/yellow]")

    def do_zero_duration(self, arg: str):
        """zero_duration [library_id] — items with no detected duration (likely corrupt or stub files)"""
        section_id = arg.strip() or None
        with console.status("Scanning for zero-duration items..."):
            rows = self.client.media_rows_for(section_id)
        bad = [r for r in rows if not r.get("duration")]
        if not bad:
            console.print("[green]No zero-duration items found.[/green]")
            return
        t = Table(title=f"Zero-Duration Items ({len(bad)})", box=box.ROUNDED)
        t.add_column("Key", style="dim", width=7)
        t.add_column("Title", style="bold white", min_width=30)
        t.add_column("Library", style="cyan", width=16)
        t.add_column("File", style="dim", min_width=40, overflow="fold")
        for r in sorted(bad, key=lambda x: x.get("title", "").lower()):
            t.add_row(r.get("ratingKey", ""), r.get("title", "—"),
                      r.get("library", "—"), r.get("file", "—"))
        console.print(t)
        console.print(f"[yellow]{len(bad)} item(s) with no detected duration — "
                      "check files for corruption or re-run Analyze.[/yellow]")

    # ── Watch statistics ──────────────────────────────────────────────────────

    def do_stats(self, _):
        with console.status("Gathering stats..."):
            data = self.client.all_items_by_library()
            media_rows = self.client.all_media_rows()
            hist = self.client.history(count=500)
        # Aggregate size/duration from leaf-level media rows (correct for TV/music)
        lib_size: dict = defaultdict(int)
        lib_dur: dict = defaultdict(int)
        for row in media_rows:
            lib_size[row["library"]] += row.get("size") or 0
            lib_dur[row["library"]] += row.get("duration") or 0
        t = Table(title="Library Summary", box=box.ROUNDED)
        t.add_column("Library", style="cyan"); t.add_column("Type", style="yellow", width=8)
        t.add_column("Items", justify="right", width=7); t.add_column("Total Duration", justify="right", width=14)
        t.add_column("Total Size", justify="right", width=12)
        gi, gms, gb = 0, 0, 0
        for lib_title, d in data.items():
            items = d["items"]
            ms = lib_dur[lib_title]
            byt = lib_size[lib_title]
            gi += len(items); gms += ms; gb += byt
            t.add_row(lib_title, d["info"].get("type",""), str(len(items)), format_duration(ms), format_size(byt))
        t.add_section()
        t.add_row("[bold]TOTAL[/bold]", "", f"[bold]{gi}[/bold]", format_duration(gms), format_size(gb))
        console.print(t)
        if hist:
            uc: Counter = Counter(); tc: Counter = Counter()
            for h in hist:
                uc[h.get("User",{}).get("title","Unknown")] += 1
                tc[h.get("title","?")] += 1
            console.print(Panel(
                f"[bold cyan]Total plays in history:[/bold cyan] {len(hist)}\n\n"
                "[bold cyan]Most active users:[/bold cyan]\n" +
                "\n".join(f"  {u}: {c}" for u,c in uc.most_common(5)) +
                "\n\n[bold cyan]Most played titles:[/bold cyan]\n" +
                "\n".join(f"  {t_}: {c}" for t_,c in tc.most_common(5)),
                title="[bold white]Watch History[/bold white]", border_style="magenta"))

    def do_history(self, arg: str):
        parts = arg.strip().split()
        count, username_filter = 50, None
        for p in parts:
            if p.isdigit(): count = int(p)
            else: username_filter = p.lower()
        with console.status("Fetching history..."):
            records = self.client.history(count=count)
        if not records:
            console.print("[yellow]No history available (may require Plex Pass).[/yellow]")
            return
        if username_filter:
            records = [r for r in records if username_filter in r.get("User",{}).get("title","").lower()]
        self._history_table(records, "Watch History")

    def do_unwatched(self, arg: str):
        libs = self._libs_for(arg.strip() or None)
        t = Table(title="Unwatched Content", box=box.ROUNDED)
        t.add_column("Library", style="cyan", width=16); t.add_column("Key", style="dim", width=7)
        t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Year", width=6, justify="right"); t.add_column("Added", width=17, style="dim")
        count = 0
        with console.status("Scanning..."):
            for lib in libs:
                for item in self.client.library_contents(lib.get("key","")):
                    if not item.get("viewCount"):
                        t.add_row(lib.get("title",""), item.get("ratingKey",""), item.get("title",""),
                                  year(item), format_ts(item.get("addedAt")))
                        count += 1
        if count == 0:
            console.print("[green]Everything has been watched![/green]")
        else:
            console.print(t); console.print(f"[yellow]{count} unwatched items.[/yellow]")

    def do_toprated(self, arg: str):
        libs = self._libs_for(arg.strip() or None)
        all_items = []
        with console.status("Fetching ratings..."):
            for lib in libs:
                for item in self.client.library_contents(lib.get("key",""), sort="rating:desc"):
                    r = item.get("rating") or item.get("audienceRating")
                    if r:
                        all_items.append((float(r), lib.get("title",""), item))
        all_items.sort(key=lambda x: x[0], reverse=True)
        t = Table(title="Top Rated", box=box.ROUNDED)
        t.add_column("#", style="dim", width=4); t.add_column("Rating", width=7, justify="right", style="bold green")
        t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Year", width=6, justify="right"); t.add_column("Library", style="cyan", width=16)
        for i, (r, lt, item) in enumerate(all_items[:50], 1):
            t.add_row(str(i), f"{r:.1f}", item.get("title",""), year(item), lt)
        console.print(t)

    def do_watch_calendar(self, arg: str):
        """watch_calendar [days] — day-by-day view of what was watched (default 7 days)"""
        days = int(arg.strip()) if arg.strip().isdigit() else 7
        cutoff = int(time.time()) - days * 86400
        with console.status("Fetching watch history..."):
            hist = [h for h in self.client.history(count=2000)
                    if (h.get("viewedAt") or 0) >= cutoff]
        if not hist:
            console.print(f"[yellow]No watch history in the last {days} days.[/yellow]")
            return
        by_date: dict = defaultdict(list)
        for h in hist:
            d = datetime.fromtimestamp(h["viewedAt"]).strftime("%a  %b %d")
            by_date[d].append(h)
        t = Table(title=f"Watch Calendar — Last {days} Days ({len(hist)} plays)",
                  box=box.ROUNDED, show_lines=True)
        t.add_column("Date", style="bold cyan", width=12, no_wrap=True)
        t.add_column("User", style="dim", width=12)
        t.add_column("Title", style="bold white", min_width=32)
        t.add_column("Type", style="yellow", width=9)
        for date in sorted(by_date, reverse=True):
            for i, h in enumerate(by_date[date]):
                parent = h.get("grandparentTitle", "")
                title = h.get("title", "")
                t.add_row(
                    date if i == 0 else "",
                    h.get("User", {}).get("title", "—"),
                    f"{parent} — {title}" if parent else title,
                    h.get("type", ""),
                )
            t.add_section()
        console.print(t)

    def do_watched_by_decade(self, arg: str):
        """watched_by_decade [count] — which release decade you watch most, based on play history"""
        count = int(arg.strip()) if arg.strip().isdigit() else 2000
        with console.status("Fetching watch history..."):
            hist = self.client.history(count=count)
        if not hist:
            console.print("[yellow]No history available (may require Plex Pass).[/yellow]"); return
        decade_counts: Counter = Counter()
        no_year = 0
        for h in hist:
            y = h.get("year")
            if y:
                decade_counts[(int(y) // 10) * 10] += 1
            else:
                no_year += 1
        if not decade_counts:
            console.print("[yellow]No year data in history.[/yellow]"); return
        total = sum(decade_counts.values())
        peak  = max(decade_counts.values())
        t = Table(title=f"Watch History by Decade ({total} plays)", box=box.ROUNDED)
        t.add_column("Decade", style="bold cyan", width=10)
        t.add_column("Plays",  justify="right",   width=8)
        t.add_column("Share",  justify="right",   width=7)
        t.add_column("",       min_width=30)
        for decade in sorted(decade_counts):
            cnt     = decade_counts[decade]
            pct     = cnt / total * 100
            bar_len = int(cnt / peak * 30) if peak else 0
            t.add_row(f"{decade}s", str(cnt), f"{pct:.1f}%", f"[cyan]{'█' * bar_len}[/cyan]")
        t.add_section()
        t.add_row("[bold]Total[/bold]", f"[bold]{total}[/bold]", "", "")
        console.print(t)
        if no_year:
            console.print(f"[dim]{no_year} history entries had no year data.[/dim]")

    def do_recommendations(self, arg: str):
        """recommendations [library_id] — highly rated unwatched content (audience rating ≥ 7.5)"""
        section_id = arg.strip() or None
        MIN_RATING = 7.5
        with console.status("Finding recommendations..."):
            items = self._all_items(section_id)
        recs = []
        for item in items:
            if item.get("viewCount"):
                continue
            r = item.get("audienceRating") or item.get("rating")
            if r and float(r) >= MIN_RATING:
                recs.append((float(r), item))
        if not recs:
            console.print(f"[yellow]No unwatched items with rating ≥ {MIN_RATING}.[/yellow]")
            return
        recs.sort(key=lambda x: x[0], reverse=True)
        t = Table(title=f"Recommendations — Unwatched, Rating ≥ {MIN_RATING}",
                  caption=f"{len(recs)} items", caption_justify="right", box=box.ROUNDED)
        t.add_column("#", style="dim", width=4)
        t.add_column("Rating", width=7, justify="right", style="bold green")
        t.add_column("Title", style="bold white", min_width=30)
        t.add_column("Year", width=6, justify="right")
        t.add_column("Type", style="yellow", width=10)
        for i, (r, item) in enumerate(recs[:50], 1):
            t.add_row(str(i), f"{r:.1f}", item.get("title", ""), year(item), item.get("type", ""))
        console.print(t)

    def do_rewatched(self, arg: str):
        """rewatched [library_id] — titles played more than once, ranked by play count"""
        section_id = arg.strip() or None
        with console.status("Scanning watch counts..."):
            items = self._all_items(section_id)
        rows = sorted(
            [(item.get("viewCount", 0), item) for item in items if (item.get("viewCount") or 0) > 1],
            key=lambda x: x[0], reverse=True,
        )
        if not rows:
            console.print("[yellow]No rewatched titles found.[/yellow]"); return
        t = Table(title=f"Most Rewatched ({len(rows)} titles)", box=box.ROUNDED,
                  caption=f"showing top {min(50, len(rows))}", caption_justify="right")
        t.add_column("#", style="dim", width=4)
        t.add_column("Plays", width=6, justify="right", style="bold green")
        t.add_column("Title", style="bold white", min_width=30)
        t.add_column("Year", width=6, justify="right")
        t.add_column("Type", style="yellow", width=10)
        for i, (plays, item) in enumerate(rows[:50], 1):
            t.add_row(str(i), str(plays), item.get("title", ""), year(item), item.get("type", ""))
        console.print(t)

    def do_show_progress(self, arg: str):
        """show_progress [library_id] — every TV show with episode count, watched %, and last watched date"""
        section_id = arg.strip() or None
        with console.status("Fetching TV libraries..."):
            tv_libs = [l for l in self.client.libraries() if l.get("type") == "show"]
        if section_id:
            tv_libs = [l for l in tv_libs if l.get("key") == section_id]
        if not tv_libs:
            console.print("[yellow]No TV show libraries found.[/yellow]"); return
        shows = []
        with console.status("Scanning shows..."):
            for lib in tv_libs:
                for show in self.client.library_contents(lib.get("key", "")):
                    total   = show.get("leafCount") or 0
                    watched = show.get("viewedLeafCount") or 0
                    shows.append((lib.get("title", ""), show, watched, total))
        shows.sort(key=lambda x: (x[0], x[1].get("title", "").lower()))
        t = Table(title=f"TV Show Progress ({len(shows)} shows)", box=box.ROUNDED)
        t.add_column("Library", style="cyan", width=14)
        t.add_column("Show", style="bold white", min_width=28)
        t.add_column("Watched", width=9, justify="right")
        t.add_column("Total", width=7, justify="right")
        t.add_column("Progress", min_width=24)
        t.add_column("Last Watched", width=17, style="dim")
        for lib_title, show, watched, total in shows:
            pct  = watched / total * 100 if total else 0
            fill = int(pct / 5)
            bar  = f"[green]{'█' * fill}[/green][dim]{'░' * (20 - fill)}[/dim] {pct:.0f}%"
            t.add_row(lib_title, show.get("title", ""), str(watched), str(total),
                      bar, format_ts(show.get("lastViewedAt")))
        console.print(t)

    def do_added_trend(self, arg: str):
        """added_trend [months] — items added per month, showing library growth over time (default 12)"""
        months = int(arg.strip()) if arg.strip().isdigit() else 12
        cutoff = int(time.time()) - months * 30 * 86400
        with console.status("Scanning added dates..."):
            libs_data = self.client.all_items_by_library()
        month_counts: Counter = Counter()
        for d in libs_data.values():
            for item in d["items"]:
                added = item.get("addedAt") or 0
                if added >= cutoff:
                    month_counts[datetime.fromtimestamp(added).strftime("%Y-%m")] += 1
        if not month_counts:
            console.print(f"[yellow]No items added in the last {months} months.[/yellow]"); return
        total = sum(month_counts.values())
        peak  = max(month_counts.values())
        t = Table(title=f"Items Added per Month — Last {months} Months", box=box.ROUNDED)
        t.add_column("Month", style="bold cyan", width=10)
        t.add_column("Added", justify="right", width=8)
        t.add_column("", min_width=32)
        for month in sorted(month_counts):
            cnt     = month_counts[month]
            bar_len = int(cnt / peak * 30) if peak else 0
            t.add_row(month, str(cnt), f"[cyan]{'█' * bar_len}[/cyan]")
        t.add_section()
        t.add_row("[bold]Total[/bold]", f"[bold]{total}[/bold]", "")
        console.print(t)

    def do_overdue(self, arg: str):
        """overdue [months] [library_id] — items added more than N months ago that have never been played (default 12)"""
        parts = arg.strip().split()
        months, section_id = 12, None
        for p in parts:
            if p.isdigit(): months = int(p)
            elif not p.startswith("-"): section_id = p
        cutoff = int(time.time()) - months * 30 * 86400
        SKIP = {"show", "season", "artist", "album"}
        with console.status(f"Finding content unplayed for >{months} months..."):
            items = self._all_items(section_id)
        results = [i for i in items
                   if i.get("type") not in SKIP
                   and not i.get("viewCount")
                   and (i.get("addedAt") or 0) < cutoff]
        results.sort(key=lambda x: x.get("addedAt") or 0)
        if not results:
            console.print(f"[green]No unplayed items older than {months} months.[/green]"); return
        now = int(time.time())
        t = Table(title=f"Overdue — Unplayed >{months} Months ({len(results)} items)", box=box.ROUNDED)
        t.add_column("Key",      style="dim",        width=7)
        t.add_column("Title",    style="bold white",  min_width=30)
        t.add_column("Year",     width=6,             justify="right")
        t.add_column("Added",    width=12,            style="dim")
        t.add_column("Age (mo)", width=9,             justify="right", style="yellow")
        for item in results:
            added      = item.get("addedAt") or 0
            age_months = (now - added) // (30 * 86400)
            t.add_row(item.get("ratingKey", ""), item.get("title", "—"),
                      str(item.get("year") or "—"),
                      datetime.fromtimestamp(added).strftime("%Y-%m-%d") if added else "—",
                      str(age_months))
        console.print(t)

    def do_binge_candidates(self, arg: str):
        """binge_candidates [library_id] — TV shows ranked by number of unwatched episodes"""
        section_id = arg.strip() or None
        with console.status("Scanning TV shows..."):
            items = self._all_items(section_id)
        candidates = []
        for item in items:
            if item.get("type") != "show":
                continue
            total     = item.get("leafCount", 0) or 0
            watched   = item.get("viewedLeafCount", 0) or 0
            unwatched = total - watched
            if unwatched > 0:
                candidates.append({"title": item.get("title", ""), "total": total,
                                   "watched": watched, "unwatched": unwatched})
        if not candidates:
            console.print("[green]No unwatched episodes.[/green]"); return
        candidates.sort(key=lambda x: x["unwatched"], reverse=True)
        t = Table(title=f"Binge Candidates — {len(candidates)} Shows with Unwatched Episodes",
                  box=box.ROUNDED)
        t.add_column("#",         style="dim",        width=4)
        t.add_column("Title",     style="bold white",  min_width=32)
        t.add_column("Unwatched", justify="right",     width=10, style="yellow")
        t.add_column("Total Eps", justify="right",     width=10)
        t.add_column("Progress",  min_width=24)
        for i, c in enumerate(candidates[:50], 1):
            pct  = c["watched"] / c["total"] * 100 if c["total"] else 0
            fill = int(pct / 5)
            bar  = f"[green]{'█' * fill}[/green][dim]{'░' * (20 - fill)}[/dim] {pct:.0f}%"
            t.add_row(str(i), c["title"], str(c["unwatched"]), str(c["total"]), bar)
        console.print(t)

    # ── Storage analysis ──────────────────────────────────────────────────────

    def _size_table(self, count: int, largest: bool, library_filter: str = ""):
        label = "Largest" if largest else "Smallest"
        with console.status(f"Fetching {label.lower()} files..."):
            rows = [r for r in self.client.all_media_rows() if r.get("size")]
        if library_filter:
            rows = [r for r in rows if library_filter.lower() in r["library"].lower()]
            if not rows:
                console.print(f"[yellow]No results for library '{library_filter}'.[/yellow]"); return
        rows.sort(key=lambda r: r["size"], reverse=largest)
        rows = rows[:count]
        title = f"{label} {count} Files" + (f" — {library_filter}" if library_filter else "")
        t = Table(title=title, box=box.ROUNDED)
        t.add_column("#", style="dim", width=4); t.add_column("Size", width=10, justify="right", style="bold yellow")
        t.add_column("Title", style="bold white", min_width=28); t.add_column("Library", style="cyan", width=16)
        t.add_column("Video", width=8); t.add_column("Audio", width=8)
        t.add_column("Resolution", width=10, justify="right")
        for i, r in enumerate(rows, 1):
            t.add_row(str(i), format_size(r["size"]), r["title"], r["library"],
                      r["videoCodec"].upper() or "—", r["audioCodec"].upper() or "—",
                      resolution_label(r["videoResolution"]))
        total = sum(r["size"] for r in rows)
        t.add_section()
        t.add_row("", f"[bold]{format_size(total)}[/bold]", f"[dim]Total ({len(rows)} files)[/dim]", "", "", "", "")
        console.print(t)

    def do_largest(self, arg: str):
        count, lib = self._parse_size_args(arg); self._size_table(count, True, lib)
    def do_smallest(self, arg: str):
        count, lib = self._parse_size_args(arg); self._size_table(count, False, lib)

    def _show_size_table(self, count: int, largest: bool, library_filter: str = "",
                         title: str | None = None):
        label = "Largest" if largest else "Smallest"
        with console.status("Fetching TV libraries..."):
            tv_libs = [l for l in self.client.libraries() if l.get("type") == "show"]
        if library_filter:
            tv_libs = [l for l in tv_libs if library_filter.lower() in l.get("title", "").lower()]
        if not tv_libs:
            suffix = f" matching '{library_filter}'" if library_filter else ""
            console.print(f"[yellow]No TV libraries found{suffix}.[/yellow]")
            return

        show_data: dict = {}
        with console.status("Summing episode sizes..."):
            for lib in tv_libs:
                for ep in self.client.library_episodes(lib.get("key", "")):
                    show = ep.get("grandparentTitle") or ep.get("title", "Unknown")
                    show_key = (lib.get("key", ""), ep.get("grandparentRatingKey") or show.lower().strip())
                    rec = show_data.setdefault(show_key, {
                        "title": show, "ratingKey": ep.get("grandparentRatingKey", ""),
                        "library": lib.get("title", ""), "size": 0, "episodes": 0,
                    })
                    rec["size"] += sum(
                        p.get("size", 0) or 0
                        for m in ep.get("Media", []) for p in m.get("Part", [])
                    )
                    rec["episodes"] += 1

        if not show_data:
            console.print("[yellow]No episode data found.[/yellow]")
            return

        rows = sorted(show_data.values(), key=lambda x: x["size"], reverse=largest)
        if count > 0:
            rows = rows[:count]
        if title is None:
            title = (f"{label} {count} TV Shows by Disk Usage"
                     if count > 0 else "TV Show Disk Usage")
        if library_filter:
            title += f" - {library_filter}"
        t = Table(title=title, box=box.ROUNDED)
        t.add_column("#", style="dim", width=4)
        t.add_column("Total Size", width=12, justify="right", style="bold yellow")
        t.add_column("Show", style="bold white", min_width=30)
        t.add_column("Library", style="cyan", width=16)
        t.add_column("Episodes", width=9, justify="right")
        t.add_column("Avg/Ep", width=10, justify="right")
        for i, row in enumerate(rows, 1):
            avg = row["size"] // row["episodes"] if row["episodes"] else 0
            t.add_row(str(i), format_size(row["size"]), row["title"],
                      row["library"], str(row["episodes"]), format_size(avg))
        total = sum(r["size"] for r in rows)
        t.add_section()
        t.add_row("", f"[bold]{format_size(total)}[/bold]", f"[dim]Total ({len(rows)} shows)[/dim]", "", "", "")
        console.print(t)

    def do_tv_storage(self, arg: str):
        """tv_storage [--library name] [--count N] - disk usage grouped by TV show"""
        tokens = self._tokens(arg)
        count = self._int_flag(tokens, "--count", 0, minimum=0)
        if count == 0:
            for token in tokens:
                if token.isdigit():
                    count = int(token)
                    break
        _, flags = parse_search_args(arg)
        library_filter = self._flag_value(tokens, "--library", flags.get("library", ""))
        title = "TV Show Disk Usage" if count == 0 else f"TV Show Disk Usage (top {count})"
        self._show_size_table(count, True, library_filter, title=title)

    def do_tvlargest(self, arg: str):
        """tvlargest [count] [--library name] — TV shows with the most total disk usage (default 25)"""
        count, lib = self._parse_size_args(arg)
        self._show_size_table(count, True, lib)

    def do_tvsmallest(self, arg: str):
        """tvsmallest [count] [--library name] — TV shows with the least total disk usage (default 25)"""
        count, lib = self._parse_size_args(arg)
        self._show_size_table(count, False, lib)

    def _duration_table(self, count: int, longest: bool, library_filter: str = ""):
        label = "Longest" if longest else "Shortest"
        with console.status(f"Fetching {label.lower()} titles..."):
            data = self.client.all_items_by_library()
        rows = []
        for lt, d in data.items():
            if library_filter and library_filter.lower() not in lt.lower():
                continue
            for item in d["items"]:
                dur = item.get("duration")
                if dur:
                    rows.append((lt, item, dur))
        if not rows:
            console.print(f"[yellow]No results{f' for library {library_filter!r}' if library_filter else ''}.[/yellow]"); return
        rows.sort(key=lambda x: x[2], reverse=longest)
        rows = rows[:count]
        title = f"{label} {count} Titles" + (f" — {library_filter}" if library_filter else "")
        t = Table(title=title, box=box.ROUNDED)
        t.add_column("#", style="dim", width=4); t.add_column("Duration", width=10, justify="right", style="bold yellow")
        t.add_column("Title", style="bold white", min_width=28); t.add_column("Library", style="cyan", width=16)
        t.add_column("Year", width=6, justify="right"); t.add_column("Type", style="yellow", width=8)
        for i, (lt, item, dur) in enumerate(rows, 1):
            t.add_row(str(i), format_duration(dur), item.get("title",""), lt, year(item), item.get("type",""))
        console.print(t)

    def do_longest(self, arg: str):
        count, lib = self._parse_size_args(arg); self._duration_table(count, True, lib)
    def do_shortest(self, arg: str):
        count, lib = self._parse_size_args(arg); self._duration_table(count, False, lib)

    def do_storage(self, _):
        with console.status("Calculating storage..."):
            rows = self.client.all_media_rows()
        lib_sizes: dict = defaultdict(int); lib_counts: dict = defaultdict(int)
        for row in rows:
            lib_sizes[row["library"]] += row.get("size") or 0
            lib_counts[row["library"]] += 1
        t = Table(title="Storage by Library", box=box.ROUNDED)
        t.add_column("Library", style="cyan"); t.add_column("Files", justify="right", width=7)
        t.add_column("Total Size", justify="right", width=12, style="bold")
        t.add_column("Avg Size", justify="right", width=12)
        tb, tf = 0, 0
        for lt in sorted(lib_sizes):
            sz, cnt = lib_sizes[lt], lib_counts[lt]
            tb += sz; tf += cnt
            t.add_row(lt, str(cnt), format_size(sz), format_size(sz//cnt if cnt else 0))
        t.add_section()
        t.add_row("[bold]TOTAL[/bold]", f"[bold]{tf}[/bold]", f"[bold]{format_size(tb)}[/bold]",
                  format_size(tb//tf) if tf else "—")
        console.print(t)
        top = sorted(rows, key=lambda r: r.get("size") or 0, reverse=True)[:10]
        if top:
            t2 = Table(title="Top 10 Largest Files", box=box.ROUNDED)
            t2.add_column("Size", justify="right", width=12, style="bold yellow")
            t2.add_column("Title", style="bold white", min_width=28)
            t2.add_column("Library", style="cyan", width=16); t2.add_column("Codec", width=8)
            for row in top:
                t2.add_row(format_size(row.get("size")), row["title"], row["library"], row["videoCodec"].upper())
            console.print(t2)

    def do_bycodec(self, arg: str):
        if not arg.strip():
            console.print("[yellow]Usage: bycodec <codec>  (e.g. bycodec hevc, bycodec dts)[/yellow]"); return
        target = arg.strip().lower()
        with console.status(f"Scanning for codec [cyan]{target}[/cyan]..."):
            rows = self.client.all_media_rows()
        matches = [r for r in rows if target in r["videoCodec"] or target in r["audioCodec"]]
        if not matches:
            console.print(f"[yellow]No items found with codec '{target}'.[/yellow]"); return
        t = Table(title=f"Items with codec '{target}' ({len(matches)} found)", box=box.ROUNDED)
        t.add_column("Key", style="dim", width=7); t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Library", style="cyan", width=16); t.add_column("Video", width=8)
        t.add_column("Audio", width=8); t.add_column("Resolution", width=10, justify="right")
        t.add_column("Size", width=10, justify="right")
        for r in sorted(matches, key=lambda x: x["title"].lower()):
            t.add_row(r["ratingKey"], r["title"], r["library"], r["videoCodec"].upper() or "—",
                      r["audioCodec"].upper() or "—", resolution_label(r["videoResolution"]), format_size(r["size"]))
        console.print(t)

    def do_codecs(self, _):
        with console.status("Scanning codecs..."):
            rows = self.client.all_media_rows()
        total = len(rows)
        def mk(title, counts):
            t = Table(title=title, box=box.ROUNDED)
            t.add_column("Codec", style="bold cyan"); t.add_column("Count", justify="right", width=8)
            t.add_column("Share", justify="right", width=8)
            for c, n in counts.most_common():
                t.add_row(c.upper(), str(n), f"{n/total*100:.1f}%")
            return t
        console.print(mk("Video Codecs", Counter(r["videoCodec"] or "unknown" for r in rows)))
        console.print(mk("Audio Codecs", Counter(r["audioCodec"] or "unknown" for r in rows)))
        console.print(mk("Containers", Counter(r["container"] or "unknown" for r in rows)))

    def do_transcode(self, _):
        SAFE_V = {"h264","hevc","av1"}; SAFE_A = {"aac","ac3","eac3","mp3","opus","vorbis"}
        SAFE_C = {"mkv","mp4","m4v","mov"}
        with console.status("Analysing codec compatibility..."):
            rows = self.client.all_media_rows()
        t = Table(title="Likely Transcode Required", box=box.ROUNDED)
        t.add_column("Library", style="cyan", width=14); t.add_column("Title", style="bold white", min_width=26)
        t.add_column("Video", width=10); t.add_column("Audio", width=10)
        t.add_column("Container", width=10); t.add_column("Reason", style="yellow")
        count = 0
        for row in rows:
            vc, ac, ct = row["videoCodec"], row["audioCodec"], row["container"]
            reasons = []
            if vc and vc not in SAFE_V: reasons.append(f"video:{vc.upper()}")
            if ac and ac not in SAFE_A: reasons.append(f"audio:{ac.upper()}")
            if ct and ct not in SAFE_C: reasons.append(f"container:{ct.upper()}")
            if reasons:
                t.add_row(row["library"], row["title"], vc.upper() if vc else "?",
                          ac.upper() if ac else "?", ct.upper() if ct else "?", ", ".join(reasons))
                count += 1
        if count == 0:
            console.print("[green]All items should direct-play on most clients.[/green]")
        else:
            console.print(t); console.print(f"[yellow]{count} items may require transcoding.[/yellow]")

    # ── Collection tools ──────────────────────────────────────────────────────

    def do_export(self, arg: str):
        parts = arg.strip().split()
        if not parts:
            console.print("[yellow]Usage: export <library_id> [filename][/yellow]"); return
        section_id = parts[0]
        filename = parts[1] if len(parts) > 1 else None
        with console.status(f"Fetching library {section_id}..."):
            items = self.client.library_contents(section_id)
        if not items:
            console.print("[yellow]No items found.[/yellow]"); return
        use_json = filename and filename.endswith(".json")
        if not filename:
            filename = f"plex_export_{section_id}_{datetime.now().strftime('%Y%m%d')}.csv"
        fields = ["ratingKey","title","year","type","rating","audienceRating",
                  "duration","addedAt","summary","studio","contentRating"]
        if use_json:
            Path(filename).write_text(json.dumps(items, indent=2))
        else:
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                writer.writeheader()
                for item in items:
                    row = {k: item.get(k,"") for k in fields}
                    row["duration"] = format_duration(item.get("duration"))
                    row["addedAt"] = format_ts(item.get("addedAt"))
                    writer.writerow(row)
        console.print(f"[green]Exported {len(items)} items to[/green] [bold]{filename}[/bold]")

    def do_fixtitles(self, arg: str):
        if arg.strip():
            libs = [l for l in self.client.libraries() if l.get("key") == arg.strip()]
        else:
            libs = [l for l in self.client.libraries() if l.get("type") == "movie"]
        if not libs:
            console.print("[yellow]No matching libraries found.[/yellow]"); return
        proposals: list[tuple[str,str,str]] = []
        with console.status("Scanning for filename-style titles..."):
            for lib in libs:
                for item in self.client.library_contents(lib.get("key","")):
                    old = item.get("title","")
                    new = clean_title(old)
                    if new:
                        proposals.append((item.get("ratingKey",""), old, new))
        if not proposals:
            console.print("[green]No filename-style titles found.[/green]"); return
        t = Table(title=f"{len(proposals)} titles to fix", box=box.ROUNDED, show_lines=True)
        t.add_column("#", style="dim", width=4); t.add_column("Key", style="dim", width=7)
        t.add_column("Current Title", style="yellow", min_width=30)
        t.add_column("Proposed Title", style="bold green", min_width=30)
        for i, (key, old, new) in enumerate(proposals, 1):
            t.add_row(str(i), key, old, new)
        console.print(t)
        answer = Prompt.ask("\nApply all? [bold green]y[/bold green]=yes, [bold yellow]e[/bold yellow]=edit, [bold red]n[/bold red]=cancel",
                            choices=["y","e","n"], default="n")
        if answer == "n":
            console.print("[dim]Cancelled.[/dim]"); return
        if answer == "e":
            console.print("[dim]Enter new title, Enter to accept, 's' to skip.[/dim]")
            to_apply = []
            for key, old, new in proposals:
                inp = Prompt.ask(f"  [yellow]{old}[/yellow] →", default=new)
                if inp.lower() != "s":
                    to_apply.append((key, inp))
        else:
            to_apply = [(k, n) for k, _, n in proposals]
        ok = fail = 0
        for key, new_title in to_apply:
            if self.client.update_title(key, new_title): ok += 1
            else: fail += 1
        console.print(f"[green]{ok} updated[/green]" + (f", [red]{fail} failed[/red]" if fail else "") + ".")

    def do_settitle(self, arg: str):
        parts = arg.strip().split(None, 1)
        if len(parts) < 2:
            console.print("[yellow]Usage: settitle <key> <new title>[/yellow]"); return
        if self.client.update_title(parts[0], parts[1].strip()):
            console.print(f"[green]Updated[/green] {parts[0]} → [bold]{parts[1].strip()}[/bold]")

    def do_stale(self, arg: str):
        months = int(arg.strip()) if arg.strip().isdigit() else 6
        cutoff_ts = int(time.time()) - months * 30 * 86400
        with console.status("Fetching TV libraries..."):
            libs = [l for l in self.client.libraries() if l.get("type") == "show"]
        if not libs:
            console.print("[yellow]No TV show libraries found.[/yellow]"); return
        t = Table(title=f"Shows Not Updated in {months}+ Months", box=box.ROUNDED)
        t.add_column("Library", style="cyan", width=16); t.add_column("Key", style="dim", width=7)
        t.add_column("Show", style="bold white", min_width=28)
        t.add_column("Last Updated", width=14, style="yellow"); t.add_column("Age", width=14, style="dim")
        count = 0
        with console.status("Scanning shows..."):
            for lib in libs:
                stale = sorted([i for i in self.client.library_contents(lib.get("key",""))
                                if (i.get("updatedAt") or 0) < cutoff_ts],
                               key=lambda i: i.get("updatedAt") or 0)
                for item in stale:
                    t.add_row(lib.get("title",""), item.get("ratingKey",""), item.get("title",""),
                              format_ts(item.get("updatedAt")), months_ago(item.get("updatedAt")))
                    count += 1
        if count == 0:
            console.print(f"[green]All shows updated within the last {months} months.[/green]")
        else:
            console.print(t); console.print(f"[yellow]{count} stale shows.[/yellow]")

    # ── Monitoring ────────────────────────────────────────────────────────────

    def do_watch(self, arg: str):
        interval = int(arg.strip()) if arg.strip().isdigit() else 5
        console.print(f"[dim]Refreshing every {interval}s — Ctrl+C to stop[/dim]")
        try:
            with Live(console=console, refresh_per_second=2, screen=False) as live:
                while True:
                    sessions = self.client.sessions()
                    live.update(build_sessions_table(sessions) if sessions else "[yellow]No active sessions.[/yellow]")
                    time.sleep(interval)
        except KeyboardInterrupt:
            console.print("\n[dim]Watch stopped.[/dim]")

    def do_alert(self, arg: str):
        interval = int(arg.strip()) if arg.strip().isdigit() else 10
        if interval < 1:
            interval = 1
        console.print(f"[dim]Monitoring for transcodes every {interval}s — Ctrl+C to stop[/dim]")
        known: set = set()
        try:
            while True:
                sessions = self.client.sessions()
                current = set()
                for s in sessions:
                    key = s.get("sessionKey","")
                    is_tc = bool(s.get("TranscodeSession"))
                    if not is_tc:
                        is_tc = any(st.get("decision")=="transcode" for m in s.get("Media",[])
                                    for p in m.get("Part",[]) for st in p.get("Stream",[]))
                    if is_tc:
                        current.add(key)
                        if key not in known:
                            parent = s.get("grandparentTitle","")
                            title = s.get("title","?")
                            ts = s.get("TranscodeSession",{})
                            console.print(Panel(
                                f"[bold red]TRANSCODE STARTED[/bold red]\n"
                                f"User:   [cyan]{s.get('User',{}).get('title','?')}[/cyan]\n"
                                f"Title:  [white]{f'{parent} — {title}' if parent else title}[/white]\n"
                                f"Player: [cyan]{s.get('Player',{}).get('title','?')}[/cyan]\n"
                                f"Speed:  {ts.get('speed','?')}x  Progress: {int(ts.get('progress') or 0)}%",
                                title=f"[bold red]Alert — {format_ts(int(time.time()))}[/bold red]",
                                border_style="red"))
                known = current
                time.sleep(interval)
        except KeyboardInterrupt:
            console.print("\n[dim]Alert stopped.[/dim]")

    def do_logs(self, arg: str):
        _, flags = parse_search_args(arg)
        lines = next((int(t) for t in arg.split() if t.isdigit()), 50)
        level_map = {"debug": 0, "info": 2, "warn": 3, "warning": 3, "error": 4}
        level_name = flags.get("level", "").lower()
        if not level_name:
            for shorthand in level_map:
                if f"--{shorthand}" in arg.lower().split():
                    level_name = shorthand; break
        if not level_name:
            level_name = "info"
        min_level = level_map.get(level_name, 2)

        entries: list = []
        raw_text: str | None = None
        server_filtered = False

        with console.status("Fetching server logs..."):
            # 1. JSON with minLevel (most specific; uses session Accept: application/json)
            data = self.client.get("/log", silent=True, minLevel=min_level)
            entries = data.get("MediaContainer", {}).get("Log", [])
            server_filtered = bool(entries)

            # 2. JSON without params
            if not entries:
                data = self.client.get("/log", silent=True)
                entries = data.get("MediaContainer", {}).get("Log", [])

            # 3. Plain-text fallback (some older PMS versions return text/plain)
            if not entries:
                raw_text = self.client.get_text("/log", silent=True)

        if not entries and raw_text is None:
            console.print("[red]Could not retrieve server logs.[/red]")
            console.print(
                "[dim]The /log endpoint is not available on this server.\n"
                "Find logs manually in the Plex data directory:\n"
                "  Windows: %LOCALAPPDATA%\\Plex Media Server\\Logs\\\n"
                "  Linux/Mac: ~/Library/Logs/Plex Media Server/[/dim]")
            return

        styles = {"DEBUG": "dim", "INFO": "white", "WARN": "yellow",
                  "WARNING": "yellow", "ERROR": "red", "FATAL": "bold red"}

        if entries:
            if not server_filtered and min_level > 0:
                entries = [e for e in entries if (e.get("level") or 0) >= min_level]
            log_lines = [
                f"{format_ts(e.get('time'))}  [{str(e.get('level','?')):5}]  {e.get('msg','')}"
                for e in entries
            ][-lines:]
        else:
            # raw_text path
            if raw_text and raw_text.lstrip().startswith("{"):
                try:
                    j_entries = json.loads(raw_text).get("MediaContainer", {}).get("Log", [])
                    if not server_filtered and min_level > 0:
                        j_entries = [e for e in j_entries if (e.get("level") or 0) >= min_level]
                    log_lines = [
                        f"{format_ts(e.get('time'))}  [{str(e.get('level','?')):5}]  {e.get('msg','')}"
                        for e in j_entries
                    ][-lines:]
                except json.JSONDecodeError:
                    log_lines = []
            else:
                all_lines = [l for l in (raw_text or "").splitlines() if l.strip()]
                if not server_filtered and min_level > 0:
                    above = {k.upper() for k, v in level_map.items() if v >= min_level}
                    all_lines = [l for l in all_lines if any(kw in l.upper() for kw in above)]
                log_lines = all_lines[-lines:]

        if not log_lines:
            console.print("[yellow]No log entries returned.[/yellow]"); return

        console.print(f"[bold cyan]Server Log[/bold cyan] [dim](last {len(log_lines)} lines, level≥{level_name})[/dim]")
        for line in log_lines:
            style = next((sty for kw, sty in styles.items() if kw in line.upper()), "white")
            console.print(f"[{style}]{line}[/{style}]")

    def do_activities(self, _):
        activities = self.client.get("/activities").get("MediaContainer",{}).get("Activity",[])
        if not activities:
            console.print("[yellow]No activities running.[/yellow]"); return
        t = Table(title="Server Activities", box=box.ROUNDED)
        t.add_column("Type", style="yellow", min_width=20); t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Subtitle", style="dim", min_width=24); t.add_column("Progress", width=22)
        t.add_column("Cancel", width=8)
        for a in activities:
            pct = max(0, min(100, int(a.get("progress",0))))
            t.add_row(a.get("type",""), a.get("title",""), a.get("subtitle",""),
                      f"[cyan]{'█'*(pct//5)}{'░'*(20-pct//5)}[/cyan] {pct}%",
                      "yes" if a.get("cancellable") else "—")
        console.print(t)

    def do_sharing(self, _):
        users = self.client.get("/api/v2/home/users").get("MediaContainer",{}).get("User",[])
        if not users:
            console.print("[yellow]No managed users found.[/yellow]"); return
        all_libs = {lib.get("key"): lib.get("title", lib.get("key")) for lib in self.client.libraries()}
        t = Table(title="User Library Access", box=box.ROUNDED, show_lines=True)
        t.add_column("User", style="bold cyan", min_width=18); t.add_column("Email", style="dim", min_width=22)
        t.add_column("Libraries", min_width=30)
        for user in users:
            uid = user.get("id") or user.get("uuid","")
            shared = self.client.get(f"/api/v2/home/users/{uid}/sharing").get("MediaContainer",{}).get("Section",[])
            if shared:
                lib_names = ", ".join(all_libs.get(str(s.get("id","")), s.get("title","")) for s in shared)
            else:
                lib_names = "[dim]all[/dim]" if user.get("allLibraries") else "[dim]none[/dim]"
            t.add_row(user.get("title", user.get("username","?")), user.get("email","—"), lib_names)
        console.print(t)

    # ── Playback control ──────────────────────────────────────────────────────

    def do_clients(self, _):
        clients = self.client.clients()
        if not clients:
            console.print("[yellow]No clients found. Clients must be active/online to appear.[/yellow]"); return
        t = Table(title="Plex Clients", box=box.ROUNDED)
        t.add_column("Name", style="bold cyan"); t.add_column("Product", style="yellow")
        t.add_column("Address", style="dim"); t.add_column("Device Class", style="dim")
        t.add_column("State", width=10); t.add_column("Machine ID", style="dim")
        for c in clients:
            state = c.get("state","")
            color = {"playing":"green","paused":"yellow"}.get(state, "dim")
            t.add_row(c.get("name",""), c.get("product",""), f"{c.get('address','')}:{c.get('port','')}",
                      c.get("deviceClass",""), f"[{color}]{state}[/{color}]" if state else "—",
                      c.get("machineIdentifier",""))
        console.print(t)

    def do_play(self, arg: str):
        if not arg.strip():
            console.print("[yellow]Usage: play <key> [--client <name_or_id>][/yellow]"); return
        _, flags = parse_search_args(arg)
        key = arg.strip().split()[0]
        client_filter = flags.get("client","")
        clients = self.client.clients()
        if not clients:
            console.print("[yellow]No active clients found.[/yellow]"); return
        matches = [c for c in clients if client_filter.lower() in c.get("name","").lower()
                   or client_filter.lower() in c.get("machineIdentifier","").lower()] if client_filter else clients
        if len(matches) == 1:
            target = matches[0]
        elif not matches:
            console.print(f"[yellow]No client matching '{client_filter}'.[/yellow]"); return
        else:
            t = Table(title="Choose a client", box=box.ROUNDED)
            t.add_column("#", style="dim", width=4); t.add_column("Name", style="bold cyan")
            t.add_column("Product", style="yellow")
            for i, c in enumerate(matches, 1):
                t.add_row(str(i), c.get("name",""), c.get("product",""))
            console.print(t)
            choices = {str(i): c for i,c in enumerate(matches, 1)}
            target = choices[Prompt.ask("Select client", choices=list(choices.keys()))]
        mid = target.get("machineIdentifier","")
        with console.status(f"Starting playback on [cyan]{target.get('name')}[/cyan]..."):
            ok = self.client.play_media(mid, key)
        if ok:
            console.print(f"[green]Playing[/green] {key} on [cyan]{target.get('name')}[/cyan]")

    def do_pause(self, arg: str):
        s = self._pick_session(arg)
        if s and self.client.pause_playback(*self._player_args(s)):
            console.print(f"[yellow]Paused[/yellow] {s.get('Player',{}).get('title','')}")

    def do_resume(self, arg: str):
        s = self._pick_session(arg)
        if s and self.client.resume_playback(*self._player_args(s)):
            console.print(f"[green]Resumed[/green] {s.get('Player',{}).get('title','')}")

    def do_stop(self, arg: str):
        s = self._pick_session(arg)
        if not s:
            return
        self.client.stop_playback(*self._player_args(s))
        ts = s.get("TranscodeSession",{})
        if ts:
            self.client.stop_transcode(ts.get("key",""))
        console.print(f"[red]Stopped[/red] {s.get('Player',{}).get('title','')}")

    # ── Analysis & reports ────────────────────────────────────────────────────

    def do_analyze(self, arg: str):
        if not arg.strip():
            console.print("[yellow]Usage: analyze <key>  or  analyze --library <id>[/yellow]"); return
        _, flags = parse_search_args(arg)
        lib_id = flags.get("library")
        if lib_id:
            with console.status(f"Queuing analysis for library [cyan]{lib_id}[/cyan]..."):
                ok = self.client.analyze_library(lib_id)
            if ok:
                console.print(f"[green]Analysis queued[/green] for library {lib_id}")
        else:
            key = arg.strip().split()[0]
            with console.status(f"Queuing analysis for [cyan]{key}[/cyan]..."):
                ok = self.client.analyze_item(key)
            if ok:
                console.print(f"[green]Analysis queued[/green] for item {key}")

    def do_refresh(self, arg: str):
        """refresh [library_id] [--force] — scan one or all libraries; --force re-downloads metadata from agents"""
        _, flags = parse_search_args(arg)
        force = bool(flags.get("force"))
        parts = [p for p in arg.strip().split() if not p.startswith("-")]
        lib_id = parts[0] if parts else None
        libs = self._libs_for(lib_id) if lib_id else self.client.libraries()
        if not libs:
            console.print("[yellow]No libraries found.[/yellow]")
            return
        mode = "full metadata refresh" if force else "scan for new/changed files"
        results = []
        with console.status(f"Queuing {mode}..."):
            for lib in libs:
                lid   = lib.get("key", "")
                title = lib.get("title", lid or "?")
                ok    = self.client.refresh_library(lid, force=force)
                results.append((title, lid, ok))
        if len(results) == 1:
            title, lid, ok = results[0]
            if ok:
                console.print(f"[green]Refresh queued[/green] for library {title}"
                              + (" [dim](force)[/dim]" if force else "") + " — runs in background")
        else:
            t = Table(title=f"Libraries — {mode}", box=box.ROUNDED)
            t.add_column("Library", style="bold white", min_width=24)
            t.add_column("ID", style="dim", width=6)
            t.add_column("Status", width=10)
            for title, lid, ok in results:
                t.add_row(title, lid, "[green]queued[/green]" if ok else "[red]failed[/red]")
            console.print(t)
            console.print("[dim]Runs in the background.[/dim]")

    def do_report(self, arg: str):
        html_file = None
        if "--html" in arg:
            html_file = arg.split("--html",1)[1].strip() or f"plex_report_{datetime.now().strftime('%Y%m%d')}.html"
        target = Console(record=True, width=120) if html_file else console
        with console.status("Compiling report..."):
            info = self.client.server_info()
            libs_data = self.client.all_items_by_library()
            media_rows = self.client.all_media_rows()
            sessions = self.client.sessions()
            on_deck = self.client.on_deck()
            hist = self.client.history(count=200)
            cutoff = int(time.time()) - 7 * 86400
            recent = [(lib,item) for lib,d in libs_data.items() for item in d["items"]
                      if (item.get("addedAt") or 0) >= cutoff]
        target.print(Panel(
            f"[bold cyan]Server:[/bold cyan]   {info.get('friendlyName','')}  [dim]v{info.get('version','')}[/dim]\n"
            f"[bold cyan]Platform:[/bold cyan] {info.get('platform','')} {info.get('platformVersion','')}\n"
            f"[bold cyan]Generated:[/bold cyan] {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            title="[bold white]Plex Report[/bold white]", border_style="cyan"))
        # Aggregate size/duration from leaf-level media rows (correct for TV/music)
        lib_size: dict = defaultdict(int)
        lib_dur: dict = defaultdict(int)
        for row in media_rows:
            lib_size[row["library"]] += row.get("size") or 0
            lib_dur[row["library"]] += row.get("duration") or 0
        lt = Table(title="Library Summary", box=box.ROUNDED)
        lt.add_column("Library", style="cyan"); lt.add_column("Type", style="yellow", width=8)
        lt.add_column("Items", justify="right", width=7); lt.add_column("Duration", justify="right", width=14)
        lt.add_column("Size", justify="right", width=12); lt.add_column("Unwatched", justify="right", width=10)
        g = {"i":0,"ms":0,"b":0,"uw":0}
        for lib_title, d in libs_data.items():
            items = d["items"]
            ms = lib_dur[lib_title]
            byt = lib_size[lib_title]
            uw = sum(1 for i in items if not i.get("viewCount"))
            g["i"]+=len(items); g["ms"]+=ms; g["b"]+=byt; g["uw"]+=uw
            lt.add_row(lib_title, d["info"].get("type",""), str(len(items)), format_duration(ms), format_size(byt), str(uw))
        lt.add_section()
        lt.add_row("[bold]TOTAL[/bold]","",f"[bold]{g['i']}[/bold]",format_duration(g["ms"]),format_size(g["b"]),str(g["uw"]))
        target.print(lt)
        rc = Counter(resolution_label(r["videoResolution"]) for r in media_rows)
        vc = Counter(r["videoCodec"].upper() or "?" for r in media_rows)
        ac = Counter(r["audioCodec"].upper() or "?" for r in media_rows)
        ct = Table(title="Codec & Quality Summary", box=box.ROUNDED)
        ct.add_column("Category", style="cyan"); ct.add_column("Value", style="bold white"); ct.add_column("Count", justify="right")
        for lbl,cnt in rc.most_common(): ct.add_row("Resolution",lbl,str(cnt))
        ct.add_section()
        for cd,cnt in vc.most_common(5): ct.add_row("Video",cd,str(cnt))
        ct.add_section()
        for cd,cnt in ac.most_common(5): ct.add_row("Audio",cd,str(cnt))
        target.print(ct)
        if recent:
            rt = Table(title="Added in Last 7 Days", box=box.ROUNDED)
            rt.add_column("Added", style="dim", width=17); rt.add_column("Library", style="cyan", width=16)
            rt.add_column("Title", style="bold white", min_width=28)
            for lib_title,item in sorted(recent, key=lambda x: x[1].get("addedAt",0), reverse=True)[:50]:
                rt.add_row(format_ts(item.get("addedAt")), lib_title, item.get("title",""))
            target.print(rt)
        if on_deck:
            dt = Table(title="On Deck", box=box.ROUNDED)
            dt.add_column("Title", style="bold white", min_width=28); dt.add_column("Progress", justify="right", width=10)
            for item in on_deck[:10]:
                pct = int(item.get("viewOffset",0) / (item.get("duration",0) or 1) * 100)
                p = item.get("grandparentTitle",""); ti = item.get("title","")
                dt.add_row(f"{p} — {ti}" if p else ti, f"{pct}%")
            target.print(dt)
        if sessions: target.print(build_sessions_table(sessions))
        if hist:
            uc = Counter(h.get("User",{}).get("title","?") for h in hist)
            ht = Table(title=f"Watch History (last {len(hist)} plays)", box=box.ROUNDED)
            ht.add_column("User", style="cyan"); ht.add_column("Plays", justify="right")
            for u,c in uc.most_common(): ht.add_row(u, str(c))
            target.print(ht)
        if html_file:
            Path(html_file).write_text(target.export_html(inline_styles=True), encoding="utf-8")
            console.print(f"[green]Report saved to[/green] [bold]{html_file}[/bold]")

    def do_changelog(self, arg: str):
        days = int(arg.strip()) if arg.strip().isdigit() else 7
        cutoff = int(time.time()) - days * 86400
        with console.status(f"Fetching changes from last {days} days..."):
            libs_data = self.client.all_items_by_library()
        added, updated = [], []
        for lib_title, d in libs_data.items():
            for item in d["items"]:
                if (item.get("addedAt") or 0) >= cutoff: added.append((lib_title, item))
                elif (item.get("updatedAt") or 0) >= cutoff: updated.append((lib_title, item))
        if not added and not updated:
            console.print(f"[yellow]No changes in the last {days} days.[/yellow]"); return
        for label, ts_key, data in [("Added","addedAt",added), ("Updated","updatedAt",updated)]:
            if not data:
                continue
            data.sort(key=lambda x: x[1].get(ts_key,0), reverse=True)
            t = Table(title=f"{label} (last {days} days — {len(data)} items)", box=box.ROUNDED)
            t.add_column("When", style="dim", width=17); t.add_column("Library", style="cyan", width=16)
            t.add_column("Title", style="bold white", min_width=28)
            if label == "Added":
                t.add_column("Type", style="yellow", width=10)
            for lt, item in data:
                row = [format_ts(item.get(ts_key)), lt, item.get("title","")]
                if label == "Added":
                    row.append(item.get("type",""))
                t.add_row(*row)
            console.print(t)

    # ── Ratings & Tags ────────────────────────────────────────────────────────

    def do_setrating(self, arg: str):
        parts = arg.strip().split(None, 1)
        if len(parts) != 2:
            console.print("[yellow]Usage: setrating <key> <0-10>[/yellow]"); return
        try:
            val = float(parts[1])
            if not 0 <= val <= 10: raise ValueError
        except ValueError:
            console.print("[yellow]Rating must be a number between 0 and 10.[/yellow]"); return
        if self.client.set_rating(parts[0], val):
            console.print(f"[green]Rating set to {val:.1f} for item {parts[0]}.[/green]")

    def do_bygenre(self, arg: str):   self._browse_by(arg, "genre", "Genre")
    def do_byactor(self, arg: str):   self._browse_by(arg, "actor", "Actor")
    def do_bydirector(self, arg: str): self._browse_by(arg, "director", "Director")

    def do_byyear(self, arg: str):
        parts = arg.strip().split()
        if not parts or not parts[0].isdigit():
            console.print("[yellow]Usage: byyear <year> [library_id][/yellow]"); return
        year_val, section_id = parts[0], parts[1] if len(parts) > 1 else None
        libs = self._libs_for(section_id)
        with console.status(f"Browsing year [cyan]{year_val}[/cyan]..."):
            results = []
            for lib in libs:
                results.extend(self.client.section_search(lib.get("key",""), year=year_val))
        print_media_table(results, f"Year: {year_val}")

    def do_bycontentrating(self, arg: str):
        """bycontentrating <rating> [library_id] — browse items by content rating (e.g. PG-13, TV-MA)"""
        parts = arg.strip().split()
        if not parts:
            console.print("[yellow]Usage: bycontentrating <rating> [library_id][/yellow]")
            console.print("[dim]Movies: G  PG  PG-13  R  NC-17  NR[/dim]")
            console.print("[dim]TV:     TV-Y  TV-Y7  TV-G  TV-PG  TV-14  TV-MA[/dim]")
            return
        rating_val = parts[0]
        section_id = parts[1] if len(parts) > 1 else None
        libs = self._libs_for(section_id)
        with console.status(f"Browsing content rating [cyan]{rating_val}[/cyan]..."):
            results = []
            for lib in libs:
                results.extend(self.client.section_search(lib.get("key", ""), contentRating=rating_val))
        print_media_table(results, f"Content Rating: {rating_val}")

    def do_byresolution(self, arg: str):
        """byresolution <resolution> [library_id] — list items at a given resolution (4K, 1080p, 720p, SD)"""
        _ALIASES = {
            "4k": "4K", "2160": "4K", "2160p": "4K",
            "1080": "1080p", "1080p": "1080p",
            "720": "720p", "720p": "720p",
            "sd": "SD", "480": "SD", "480p": "SD", "576": "SD", "576p": "SD",
        }
        parts = arg.strip().split()
        if not parts:
            console.print("[yellow]Usage: byresolution <resolution> [library_id][/yellow]")
            console.print("[dim]Resolutions: 4K  1080p  720p  SD[/dim]")
            return
        target = _ALIASES.get(parts[0].lower())
        if not target:
            console.print(f"[yellow]Unknown resolution '{parts[0]}'. Try: 4K, 1080p, 720p, SD[/yellow]")
            return
        section_id = parts[1] if len(parts) > 1 else None
        with console.status(f"Finding [cyan]{target}[/cyan] items..."):
            items = self._all_items(section_id)
        results = [i for i in items
                   if i.get("Media") and resolution_label(i["Media"][0].get("videoResolution")) == target]
        print_media_table(results, f"Resolution: {target}")

    def _tag_stats_table(self, title: str, tag_key: str, items: list):
        """Shared helper for director_stats / actor_stats."""
        data: dict = {}
        for item in items:
            for tag in item.get(tag_key, []):
                name = tag.get("tag", "")
                if not name:
                    continue
                rec = data.setdefault(name, {"total": 0, "watched": 0})
                rec["total"] += 1
                if item.get("viewCount"):
                    rec["watched"] += 1
        if not data:
            console.print(f"[yellow]No {title.lower()} data found.[/yellow]")
            return
        rows = sorted(data.items(), key=lambda x: x[1]["total"], reverse=True)[:50]
        col = title.split()[0]   # "Director" or "Actor"
        t = Table(title=title, box=box.ROUNDED)
        t.add_column("#", style="dim", width=4)
        t.add_column(col, style="bold cyan", min_width=24)
        t.add_column("Titles", justify="right", width=7)
        t.add_column("Watched", justify="right", width=9)
        t.add_column("Progress", min_width=24)
        for i, (name, rec) in enumerate(rows, 1):
            pct = rec["watched"] / rec["total"] * 100
            fill = int(pct / 5)
            bar = f"[green]{'█' * fill}[/green][dim]{'░' * (20 - fill)}[/dim] {pct:.0f}%"
            t.add_row(str(i), name, str(rec["total"]), str(rec["watched"]), bar)
        console.print(t)

    def do_director_stats(self, arg: str):
        """director_stats [library_id] — directors ranked by number of titles owned, with watched counts"""
        with console.status("Scanning directors..."):
            items = self._all_items(arg.strip() or None)
        self._tag_stats_table("Director Stats (top 50)", "Director", items)

    def do_actor_stats(self, arg: str):
        """actor_stats [library_id] — actors ranked by number of titles owned, with watched counts"""
        with console.status("Scanning cast..."):
            items = self._all_items(arg.strip() or None)
        self._tag_stats_table("Actor Stats (top 50)", "Role", items)

    # ── Deeper Analysis ───────────────────────────────────────────────────────

    def do_bitrate(self, arg: str):
        section_id = arg.strip() or None
        with console.status("Analysing bitrates..."):
            if section_id:
                rows = []
                for item in self.client.library_contents(section_id):
                    rows.extend(get_media_rows(item, section_id))
            else:
                rows = self.client.all_media_rows()
        rows_with_br = [r for r in rows if r.get("bitrate")]
        if not rows_with_br:
            console.print("[yellow]No bitrate data available.[/yellow]"); return
        bitrates = [r["bitrate"] for r in rows_with_br]
        avg_br = sum(bitrates) / len(bitrates)
        buckets = [("< 2 Mbps",0,2000),("2–5 Mbps",2000,5000),("5–10 Mbps",5000,10000),
                   ("10–20 Mbps",10000,20000),("20–40 Mbps",20000,40000),("> 40 Mbps",40000,float("inf"))]
        t = Table(title="Bitrate Distribution", box=box.ROUNDED)
        t.add_column("Range", style="cyan", width=14); t.add_column("Count", justify="right", width=8)
        t.add_column("Share", justify="right", width=8)
        for label, lo, hi in buckets:
            cnt = sum(1 for b in bitrates if lo <= b < hi)
            t.add_row(label, str(cnt), f"{cnt/len(bitrates)*100:.1f}%")
        console.print(t)
        console.print(f"[dim]Average: {avg_br/1000:.1f} Mbps  Min: {min(bitrates)/1000:.1f} Mbps  Max: {max(bitrates)/1000:.1f} Mbps[/dim]")
        for outliers, label, rev in [
            ([r for r in rows_with_br if r["bitrate"] > avg_br*3 and avg_br > 0], "High Bitrate Outliers (>3× avg)", True),
            ([r for r in rows_with_br if r["bitrate"] < 500], "Low Bitrate Outliers (<500 kbps)", False),
        ]:
            if not outliers: continue
            top = sorted(outliers, key=lambda x: x["bitrate"], reverse=rev)[:10]
            t2 = Table(title=f"{label} — top {len(top)}", box=box.ROUNDED)
            t2.add_column("Bitrate", justify="right", width=12); t2.add_column("Title", style="bold white", min_width=28)
            t2.add_column("Library", style="cyan", width=16)
            for r in top:
                t2.add_row(f"{r['bitrate']/1000:.1f} Mbps", r["title"], r["library"])
            console.print(t2)

    def do_oversized(self, arg: str):
        """oversized [--library id] [--4k GB] [--1080p GB] [--720p GB] [--sd GB] [--count N]"""
        tokens = self._tokens(arg)
        lib_id = self._flag_value(tokens, "--library")
        count = self._int_flag(tokens, "--count", 50, minimum=1)
        try:
            thresh_4k = float(self._flag_value(tokens, "--4k", "80"))
        except ValueError:
            thresh_4k = 80.0
        try:
            thresh_1080 = float(self._flag_value(tokens, "--1080p", "30"))
        except ValueError:
            thresh_1080 = 30.0
        try:
            thresh_720 = float(self._flag_value(tokens, "--720p", "15"))
        except ValueError:
            thresh_720 = 15.0
        try:
            thresh_sd = float(self._flag_value(tokens, "--sd", "8"))
        except ValueError:
            thresh_sd = 8.0
        thresholds = {
            "4K":    int(thresh_4k   * 1024 ** 3),
            "1080p": int(thresh_1080 * 1024 ** 3),
            "720p":  int(thresh_720  * 1024 ** 3),
            "SD":    int(thresh_sd   * 1024 ** 3),
        }
        with console.status("Scanning libraries for oversized files..."):
            if lib_id:
                rows = []
                for item in self.client.library_contents(lib_id):
                    rows.extend(get_media_rows(item, lib_id))
            else:
                rows = self.client.all_media_rows()
        flagged = []
        for r in rows:
            size = r.get("size") or 0
            if not size:
                continue
            label = resolution_label(r["videoResolution"])
            limit = thresholds.get(label)
            if limit and size > limit:
                flagged.append({**r, "_label": label, "_limit": limit, "_excess": size - limit})
        if not flagged:
            console.print("[green]No oversized files found with current thresholds.[/green]")
            thresh_str = "  ".join(f"{k}>{v/1024**3:.0f} GB" for k, v in thresholds.items())
            console.print(f"[dim]Thresholds: {thresh_str}[/dim]")
            return
        flagged.sort(key=lambda r: r["size"], reverse=True)
        total_found = len(flagged)
        flagged = flagged[:count]
        t = Table(title=f"Oversized Files — {total_found} found, showing {len(flagged)}", box=box.ROUNDED)
        t.add_column("#", style="dim", width=4)
        t.add_column("Size", justify="right", width=10, style="bold yellow")
        t.add_column("Limit", justify="right", width=10, style="dim")
        t.add_column("Excess", justify="right", width=10, style="red")
        t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Library", style="cyan", width=16)
        t.add_column("Res", width=7, justify="right")
        t.add_column("Codec", width=8)
        for i, r in enumerate(flagged, 1):
            t.add_row(
                str(i),
                format_size(r["size"]),
                format_size(r["_limit"]),
                format_size(r["_excess"]),
                r["title"],
                r["library"],
                r["_label"],
                (r["videoCodec"] or "—").upper(),
            )
        total_size = sum(r["size"] for r in flagged)
        total_excess = sum(r["_excess"] for r in flagged)
        t.add_section()
        t.add_row("", f"[bold]{format_size(total_size)}[/bold]", "", f"[bold red]{format_size(total_excess)}[/bold red]",
                  f"[dim]Total ({len(flagged)} files)[/dim]", "", "", "")
        console.print(t)
        thresh_str = "  ".join(f"{k}>{v/1024**3:.0f} GB" for k, v in thresholds.items())
        console.print(f"[dim]Thresholds: {thresh_str}[/dim]")

    def do_bitrate_outliers(self, arg: str):
        """bitrate_outliers [--library id] [--count N]"""
        tokens = self._tokens(arg)
        lib_id = self._flag_value(tokens, "--library")
        count = self._int_flag(tokens, "--count", 50, minimum=1)
        # Per-resolution max-expected bitrates in kbps
        limits = {"4K": 25000, "1080p": 12000, "720p": 6000, "SD": 3000}
        with console.status("Scanning bitrates..."):
            if lib_id:
                rows = []
                for item in self.client.library_contents(lib_id):
                    rows.extend(get_media_rows(item, lib_id))
            else:
                rows = self.client.all_media_rows()
        flagged = []
        for r in rows:
            br = r.get("bitrate")
            if not br:
                size = r.get("size") or 0
                dur = r.get("duration") or 0
                if size and dur:
                    br = int(size * 8 / (dur / 1000) / 1000)
            if not br:
                continue
            label = resolution_label(r["videoResolution"])
            limit = limits.get(label)
            if limit and br > limit:
                flagged.append({**r, "_br": br, "_label": label, "_limit": limit,
                                "_excess": br - limit, "_ratio": br / limit})
        if not flagged:
            console.print("[green]No bitrate outliers found.[/green]")
            limits_str = "  ".join(f"{k}>{v//1000} Mbps" for k, v in limits.items())
            console.print(f"[dim]Thresholds: {limits_str}[/dim]")
            return
        flagged.sort(key=lambda r: r["_ratio"], reverse=True)
        total_found = len(flagged)
        flagged = flagged[:count]
        t = Table(title=f"Bitrate Outliers — {total_found} found, showing {len(flagged)}", box=box.ROUNDED)
        t.add_column("#", style="dim", width=4)
        t.add_column("Bitrate", justify="right", width=12, style="bold yellow")
        t.add_column("Limit", justify="right", width=10, style="dim")
        t.add_column("Excess", justify="right", width=12, style="red")
        t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Library", style="cyan", width=16)
        t.add_column("Res", width=7, justify="right")
        t.add_column("Codec", width=8)
        for i, r in enumerate(flagged, 1):
            br_mbps = r["_br"] / 1000
            lim_mbps = r["_limit"] / 1000
            exc_mbps = r["_excess"] / 1000
            t.add_row(
                str(i),
                f"{br_mbps:.1f} Mbps",
                f"{lim_mbps:.0f} Mbps",
                f"+{exc_mbps:.1f} Mbps",
                r["title"],
                r["library"],
                r["_label"],
                (r["videoCodec"] or "—").upper(),
            )
        console.print(t)
        limits_str = "  ".join(f"{k}>{v//1000} Mbps" for k, v in limits.items())
        console.print(f"[dim]Thresholds: {limits_str}[/dim]")

    def do_subtitles(self, arg: str):
        section_id = arg.strip() or None
        libs = self._libs_for(section_id) if section_id else self.client.libraries()
        missing = []
        with console.status("Checking for subtitle tracks..."):
            for lib in libs:
                lid, lt = lib.get("key",""), lib.get("title","")
                for item in self.client.library_contents(lid):
                    has_sub = any(st.get("streamType") == 3 for m in item.get("Media",[])
                                 for p in m.get("Part",[]) for st in p.get("Stream",[]))
                    if not has_sub:
                        missing.append((lt, item))
        if not missing:
            console.print("[green]All items have subtitle tracks.[/green]"); return
        t = Table(title=f"Items Missing Subtitles ({len(missing)})", box=box.ROUNDED)
        t.add_column("Library", style="cyan", width=16); t.add_column("Key", style="dim", width=7)
        t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Year", width=6, justify="right"); t.add_column("Type", style="yellow", width=10)
        for lt, item in sorted(missing, key=lambda x: x[1].get("title","").lower()):
            t.add_row(lt, item.get("ratingKey",""), item.get("title",""), year(item), item.get("type",""))
        console.print(t)

    def do_hdr(self, arg: str):
        section_id = arg.strip() or None
        libs = self._libs_for(section_id) if section_id else self.client.libraries()
        hdr_items = []
        with console.status("Scanning for HDR content..."):
            for lib in libs:
                lid, lt = lib.get("key",""), lib.get("title","")
                for item in self.client.library_contents(lid):
                    for media in item.get("Media",[]):
                        hdr_type = None
                        for part in media.get("Part",[]):
                            for stream in part.get("Stream",[]):
                                if stream.get("streamType") != 1: continue
                                ctrc = (stream.get("colorTrc") or "").lower()
                                if stream.get("DOVIPresent") or stream.get("doviPresent"): hdr_type = "Dolby Vision"
                                elif "smpte2084" in ctrc or ctrc == "pq": hdr_type = "HDR10"
                                elif "arib-std-b67" in ctrc or ctrc == "hlg": hdr_type = "HLG"
                        if not hdr_type:
                            profile = (media.get("videoProfile") or "").lower()
                            if "main 10" in profile or "high 10" in profile:
                                hdr_type = "HDR (10-bit)"
                        if hdr_type:
                            hdr_items.append((lt, hdr_type, media, item)); break
        if not hdr_items:
            console.print("[yellow]No HDR content detected.[/yellow]"); return
        t = Table(title=f"HDR Content ({len(hdr_items)} items)", box=box.ROUNDED)
        t.add_column("Library", style="cyan", width=16); t.add_column("Key", style="dim", width=7)
        t.add_column("Title", style="bold white", min_width=28); t.add_column("Year", width=6, justify="right")
        t.add_column("HDR Type", style="yellow", width=14); t.add_column("Resolution", width=10, justify="right")
        for lt, ht, media, item in sorted(hdr_items, key=lambda x: x[3].get("title","").lower()):
            t.add_row(lt, item.get("ratingKey",""), item.get("title",""), year(item), ht,
                      resolution_label(media.get("videoResolution")))
        console.print(t)

    def do_audioformat(self, arg: str):
        if not arg.strip():
            console.print("[yellow]Usage: audioformat <format>  (e.g. truehd, dts, atmos, flac, aac, eac3)[/yellow]"); return
        fmt = arg.strip().lower()
        with console.status(f"Scanning for audio format [cyan]{fmt}[/cyan]..."):
            rows = self.client.all_media_rows()
        matches = [r for r in rows if fmt in r["audioCodec"]]
        if not matches:
            console.print(f"[yellow]No items found with audio format '{fmt}'.[/yellow]"); return
        t = Table(title=f"Audio Format: {fmt.upper()} ({len(matches)} items)", box=box.ROUNDED)
        t.add_column("Key", style="dim", width=7); t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Library", style="cyan", width=16); t.add_column("Audio", width=10)
        t.add_column("Ch", width=5, justify="right"); t.add_column("Video", width=8)
        t.add_column("Resolution", width=10, justify="right")
        for r in sorted(matches, key=lambda x: x["title"].lower()):
            t.add_row(r["ratingKey"], r["title"], r["library"], r["audioCodec"].upper(),
                      str(r.get("audioChannels") or "?"), r["videoCodec"].upper() or "—",
                      resolution_label(r["videoResolution"]))
        console.print(t)

    def do_multiversion(self, arg: str):
        section_id = arg.strip() or None
        libs = self._libs_for(section_id) if section_id else self.client.libraries()
        multi = []
        with console.status("Scanning for multi-version items..."):
            for lib in libs:
                lid, lt = lib.get("key",""), lib.get("title","")
                for item in self.client.library_contents(lid):
                    ml = item.get("Media",[])
                    if len(ml) > 1: multi.append((lt, item, ml))
        if not multi:
            console.print("[yellow]No multi-version items found.[/yellow]"); return
        t = Table(title=f"Multi-Version Items ({len(multi)})", box=box.ROUNDED, show_lines=True)
        t.add_column("Library", style="cyan", width=16); t.add_column("Key", style="dim", width=7)
        t.add_column("Title", style="bold white", min_width=26); t.add_column("Year", width=6, justify="right")
        t.add_column("Ver", width=4, justify="right"); t.add_column("Resolutions", style="dim")
        for lt, item, ml in sorted(multi, key=lambda x: x[1].get("title","").lower()):
            t.add_row(lt, item.get("ratingKey",""), item.get("title",""), year(item), str(len(ml)),
                      ", ".join(resolution_label(m.get("videoResolution")) for m in ml))
        console.print(t)

    # ── TV episode analysis ───────────────────────────────────────────────────

    def _tv_episode_index(self, section_id: str | None) -> dict:
        """Return {show: {season_num: [episode_indices]}} for all TV libraries (or one)."""
        tv_libs = [l for l in self.client.libraries() if l.get("type") == "show"]
        if section_id:
            tv_libs = [l for l in tv_libs if l.get("key") == section_id]
        by_show: dict = {}
        for lib in tv_libs:
            for ep in self.client.library_episodes(lib.get("key", "")):
                show = ep.get("grandparentTitle", "?")
                season = ep.get("parentIndex") or 0
                idx = ep.get("index") or 0
                if season == 0:
                    continue  # skip specials (season 0)
                by_show.setdefault(show, {}).setdefault(season, []).append(idx)
        return by_show

    def do_missing_episodes(self, arg: str):
        """missing_episodes [library_id] — TV seasons with gaps in episode numbering"""
        section_id = arg.strip() or None
        with console.status("Scanning episode numbers..."):
            by_show = self._tv_episode_index(section_id)
        if not by_show:
            console.print("[yellow]No TV libraries found.[/yellow]"); return

        gaps = []
        for show, seasons in sorted(by_show.items()):
            for season_num, indices in sorted(seasons.items()):
                s = set(indices)
                mn, mx = min(s), max(s)
                missing = [i for i in range(mn, mx + 1) if i not in s]
                if missing:
                    gaps.append((show, season_num, len(indices), missing))

        if not gaps:
            console.print("[green]No missing episodes detected.[/green]"); return

        t = Table(title=f"Missing Episodes ({len(gaps)} seasons affected)", box=box.ROUNDED)
        t.add_column("Show", style="bold white", min_width=28)
        t.add_column("Season", width=8, justify="center")
        t.add_column("Have", width=6, justify="right")
        t.add_column("Missing", style="yellow")
        for show, season, have, missing in gaps:
            t.add_row(show, f"S{season:02d}", str(have),
                      ", ".join(f"E{m:02d}" for m in missing))
        console.print(t)

    def do_incomplete_seasons(self, arg: str):
        """incomplete_seasons [library_id] — seasons with noticeably fewer episodes than the show's typical season"""
        section_id = arg.strip() or None
        with console.status("Scanning season lengths..."):
            by_show = self._tv_episode_index(section_id)
        if not by_show:
            console.print("[yellow]No TV libraries found.[/yellow]"); return

        incomplete = []
        for show, seasons in sorted(by_show.items()):
            if len(seasons) < 2:
                continue  # can't compare without at least 2 seasons
            counts = sorted(len(eps) for eps in seasons.values())
            # Median season length for this show
            n = len(counts)
            median = counts[n // 2] if n % 2 else (counts[n // 2 - 1] + counts[n // 2]) / 2
            if median < 4:
                continue  # mini-series — skip
            for season_num, indices in sorted(seasons.items()):
                count = len(indices)
                if count < median * 0.6 and count < median - 2:
                    incomplete.append((show, season_num, count, int(median)))

        if not incomplete:
            console.print("[green]No incomplete seasons detected.[/green]"); return

        incomplete.sort(key=lambda x: (x[0], x[1]))
        t = Table(title=f"Incomplete Seasons ({len(incomplete)})", box=box.ROUNDED)
        t.add_column("Show", style="bold white", min_width=28)
        t.add_column("Season", width=8, justify="center")
        t.add_column("Have", width=6, justify="right")
        t.add_column("Typical", width=8, justify="right", style="dim")
        t.add_column("Missing est.", width=12, justify="right", style="yellow")
        for show, season, have, typical in incomplete:
            t.add_row(show, f"S{season:02d}", str(have), str(typical), str(typical - have))
        console.print(t)

    def do_abandoned(self, arg: str):
        """abandoned [threshold%] [--library id] — shows started but not finished (default < 80% watched)"""
        _, flags = parse_search_args(arg)
        section_id = flags.get("library")
        threshold = next((int(t) / 100 for t in arg.split() if t.isdigit()), 0.80)

        with console.status("Fetching TV libraries..."):
            tv_libs = [l for l in self.client.libraries() if l.get("type") == "show"]
        if section_id:
            tv_libs = [l for l in tv_libs if l.get("key") == section_id]
        if not tv_libs:
            console.print("[yellow]No TV libraries found.[/yellow]"); return

        abandoned = []
        with console.status("Scanning shows..."):
            for lib in tv_libs:
                for show in self.client.library_contents(lib.get("key", "")):
                    total = show.get("leafCount") or 0
                    watched = show.get("viewedLeafCount") or 0
                    if total > 0 and 0 < watched < total * threshold:
                        abandoned.append((lib.get("title", ""), show, watched, total, watched / total * 100))

        if not abandoned:
            console.print(f"[green]No abandoned shows (threshold {threshold*100:.0f}%).[/green]"); return

        abandoned.sort(key=lambda x: x[4])
        t = Table(title=f"Abandoned Shows ({len(abandoned)})",
                  caption=f"Started but < {threshold*100:.0f}% watched",
                  caption_justify="right", box=box.ROUNDED)
        t.add_column("Library", style="cyan", width=14)
        t.add_column("Show", style="bold white", min_width=28)
        t.add_column("Watched", width=9, justify="right")
        t.add_column("Total", width=7, justify="right")
        t.add_column("Progress", min_width=24)
        for lib_title, show, watched, total, pct in abandoned:
            fill = int(pct / 5)
            bar = f"[green]{'█' * fill}[/green][dim]{'░' * (20 - fill)}[/dim] {pct:.0f}%"
            t.add_row(lib_title, show.get("title", ""), str(watched), str(total), bar)
        console.print(t)

    def do_duration_outliers(self, arg: str):
        """duration_outliers [library_id] — TV episodes with runtime significantly different from the show's median"""
        section_id = arg.strip() or None
        with console.status("Fetching TV libraries..."):
            tv_libs = [l for l in self.client.libraries() if l.get("type") == "show"]
        if section_id:
            tv_libs = [l for l in tv_libs if l.get("key") == section_id]
        if not tv_libs:
            console.print("[yellow]No TV libraries found.[/yellow]"); return

        by_show: dict = {}
        with console.status("Scanning episode durations..."):
            for lib in tv_libs:
                for ep in self.client.library_episodes(lib.get("key", "")):
                    if not ep.get("duration"):
                        continue
                    by_show.setdefault(ep.get("grandparentTitle", "?"), []).append(ep)

        THRESHOLD = 0.5   # flag if runtime deviates > 50% from the show median
        outliers = []
        for show, episodes in by_show.items():
            durs = sorted(ep["duration"] for ep in episodes)
            if len(durs) < 3:
                continue
            n = len(durs)
            med = durs[n // 2] if n % 2 else (durs[n // 2 - 1] + durs[n // 2]) / 2
            if not med:
                continue
            for ep in episodes:
                dev = abs(ep["duration"] - med) / med
                if dev > THRESHOLD:
                    outliers.append({
                        "show": show,
                        "season": ep.get("parentIndex", 0),
                        "episode": ep.get("index", 0),
                        "title": ep.get("title", ""),
                        "duration": ep["duration"],
                        "median": int(med),
                        "deviation": dev,
                        "ratingKey": ep.get("ratingKey", ""),
                    })

        if not outliers:
            console.print("[green]No duration outliers detected.[/green]"); return

        outliers.sort(key=lambda x: x["deviation"], reverse=True)
        t = Table(title=f"Duration Outliers ({len(outliers)} episodes)", box=box.ROUNDED)
        t.add_column("Key", style="dim", width=7)
        t.add_column("Show", style="bold white", min_width=24)
        t.add_column("Episode", width=9, style="dim")
        t.add_column("Runtime", width=9, justify="right")
        t.add_column("Show Median", width=12, justify="right", style="dim")
        t.add_column("Deviation", width=10, justify="right", style="yellow")
        for o in outliers:
            arrow = "▲" if o["duration"] > o["median"] else "▼"
            t.add_row(o["ratingKey"], o["show"],
                      f"S{o['season']:02d}E{o['episode']:02d}",
                      format_duration(o["duration"]), format_duration(o["median"]),
                      f"{arrow} {o['deviation']*100:.0f}%")
        console.print(t)

    def do_4k_audit(self, arg: str):
        """4k_audit [library_id] — 4K content breakdown by HDR type, audio format, and codec"""
        section_id = arg.strip() or None
        libs = self._libs_for(section_id) if section_id else self.client.libraries()

        items_4k = []
        with console.status("Scanning for 4K content..."):
            for lib in libs:
                lid, lt = lib.get("key", ""), lib.get("title", "")
                for item in self.client.library_contents(lid):
                    for media in item.get("Media", []):
                        if (media.get("videoResolution") or "").lower() not in ("4k", "2160"):
                            continue
                        hdr_type = None
                        for part in media.get("Part", []):
                            for stream in part.get("Stream", []):
                                if stream.get("streamType") != 1:
                                    continue
                                ctrc = (stream.get("colorTrc") or "").lower()
                                if stream.get("DOVIPresent") or stream.get("doviPresent"):
                                    hdr_type = "Dolby Vision"
                                elif "smpte2084" in ctrc or ctrc == "pq":
                                    hdr_type = "HDR10"
                                elif "arib-std-b67" in ctrc or ctrc == "hlg":
                                    hdr_type = "HLG"
                        if not hdr_type:
                            profile = (media.get("videoProfile") or "").lower()
                            if "main 10" in profile or "high 10" in profile:
                                hdr_type = "HDR10 (profile)"
                        items_4k.append({
                            "ratingKey": item.get("ratingKey", ""),
                            "title": item.get("title", ""),
                            "year": item.get("year"),
                            "library": lt,
                            "hdr": hdr_type or "None",
                            "videoCodec": (media.get("videoCodec") or "?").upper(),
                            "audioCodec": (media.get("audioCodec") or "?").upper(),
                            "size": sum(p.get("size", 0) or 0 for p in media.get("Part", [])),
                        })
                        break  # one media entry per item is enough

        if not items_4k:
            console.print("[yellow]No 4K content found.[/yellow]"); return

        hdr_counts  = Counter(i["hdr"] for i in items_4k)
        codec_counts = Counter(i["videoCodec"] for i in items_4k)
        audio_counts = Counter(i["audioCodec"] for i in items_4k)
        total_size  = sum(i["size"] for i in items_4k)

        console.print(Panel(
            f"[bold cyan]Total 4K items:[/bold cyan] {len(items_4k)}  "
            f"[bold cyan]Total size:[/bold cyan] {format_size(total_size)}\n\n"
            "[bold cyan]HDR:[/bold cyan]   " +
            "  ".join(f"{tag}: {cnt}" for tag, cnt in hdr_counts.most_common()) + "\n"
            "[bold cyan]Video:[/bold cyan] " +
            "  ".join(f"{tag}: {cnt}" for tag, cnt in codec_counts.most_common()) + "\n"
            "[bold cyan]Audio:[/bold cyan] " +
            "  ".join(f"{tag}: {cnt}" for tag, cnt in audio_counts.most_common()),
            title="[bold white]4K Content Audit[/bold white]", border_style="cyan"))

        HDR_STYLE = {
            "Dolby Vision":  "[bright_blue]Dolby Vision[/bright_blue]",
            "HDR10":         "[yellow]HDR10[/yellow]",
            "HDR10 (profile)": "[dim yellow]HDR10?[/dim yellow]",
            "HLG":           "[yellow]HLG[/yellow]",
            "None":          "[dim]—[/dim]",
        }
        t = Table(title="4K Items", box=box.ROUNDED)
        t.add_column("Key", style="dim", width=7)
        t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Year", width=6, justify="right")
        t.add_column("Library", style="cyan", width=14)
        t.add_column("HDR", width=16)
        t.add_column("Video", width=7)
        t.add_column("Audio", width=8)
        t.add_column("Size", width=10, justify="right")
        for i in sorted(items_4k, key=lambda x: x["title"].lower()):
            t.add_row(i["ratingKey"], i["title"], str(i["year"] or "—"), i["library"],
                      HDR_STYLE.get(i["hdr"], i["hdr"]),
                      i["videoCodec"], i["audioCodec"], format_size(i["size"]))
        console.print(t)

    def do_framerate(self, arg: str):
        """framerate [library_id] — content broken down by frame rate (23.976, 25, 30, 60fps, etc.)"""
        section_id = arg.strip() or None
        with console.status("Scanning frame rates..."):
            rows = self.client.media_rows_for(section_id)
        fps_counts: Counter = Counter(
            (r.get("videoFrameRate") or "Unknown") for r in rows
        )
        _distribution_table("Frame Rate Distribution", fps_counts)

    def do_aspect_ratio(self, arg: str):
        """aspect_ratio [library_id] — distribution of video aspect ratios (16:9, 2.35:1, 4:3, etc.)"""
        def _label(ar) -> str:
            if ar is None: return "Unknown"
            if abs(ar - 1.33) < 0.05: return "4:3   (1.33)"
            if abs(ar - 1.78) < 0.05: return "16:9  (1.78)"
            if abs(ar - 1.85) < 0.06: return "1.85:1"
            if abs(ar - 2.35) < 0.06: return "2.35:1"
            if abs(ar - 2.39) < 0.04: return "2.39:1"
            if abs(ar - 2.40) < 0.04: return "2.40:1"
            return f"{ar:.2f}:1"

        section_id = arg.strip() or None
        with console.status("Scanning aspect ratios..."):
            rows = self.client.media_rows_for(section_id)
        ar_counts: Counter = Counter(_label(r.get("aspectRatio")) for r in rows)
        _distribution_table("Aspect Ratio Distribution", ar_counts)

    def do_audio_languages(self, arg: str):
        """audio_languages [library_id] — breakdown of audio track languages across the library"""
        section_id = arg.strip() or None
        libs = self._libs_for(section_id) if section_id else self.client.libraries()
        lang_counts: Counter = Counter()
        with console.status("Scanning audio tracks..."):
            for lib in libs:
                for item in self.client._leaf_items(lib):
                    seen: set = set()
                    for media in item.get("Media", []):
                        for part in media.get("Part", []):
                            for stream in part.get("Stream", []):
                                if stream.get("streamType") != 2:
                                    continue   # 2 = audio
                                lang = (stream.get("language")
                                        or stream.get("languageCode")
                                        or "Unknown")
                                if lang not in seen:
                                    lang_counts[lang] += 1
                                    seen.add(lang)
        _distribution_table("Audio Language Distribution", lang_counts)

    def do_resolution_trend(self, arg: str):
        """resolution_trend [library_id] — 4K/1080p/720p/SD share by year items were added"""
        section_id = arg.strip() or None
        TIERS = ("4K", "1080p", "720p", "SD", "Unknown")
        SKIP  = {"show", "season", "artist", "album"}

        def _bucket(res) -> str:
            lbl = resolution_label(res)
            if lbl in TIERS:
                return lbl
            return "SD"

        with console.status("Scanning resolutions by year added..."):
            items = self._all_items(section_id)

        by_year: dict[int, Counter] = {}
        for item in items:
            if item.get("type") in SKIP or not item.get("Media"):
                continue
            added = item.get("addedAt")
            if not added:
                continue
            year  = datetime.fromtimestamp(added).year
            res   = item["Media"][0].get("videoResolution")
            by_year.setdefault(year, Counter())[_bucket(res)] += 1

        if not by_year:
            console.print("[yellow]No data found.[/yellow]")
            return

        title = "Resolution Trend by Year Added" + (f" — Library {section_id}" if section_id else "")
        t = Table(title=title, box=box.ROUNDED)
        t.add_column("Year",  style="bold cyan",   width=6)
        t.add_column("4K",    justify="right",      width=7,  style="bright_blue")
        t.add_column("1080p", justify="right",      width=7,  style="green")
        t.add_column("720p",  justify="right",      width=7,  style="yellow")
        t.add_column("SD",    justify="right",      width=7,  style="dim")
        t.add_column("Total", justify="right",      width=7)
        t.add_column("HD%",   justify="right",      width=7)

        grand: Counter = Counter()
        for year in sorted(by_year):
            c = by_year[year]
            total = sum(c.values())
            hd    = c["4K"] + c["1080p"]
            def _v(n): return str(n) if n else "[dim]—[/dim]"
            t.add_row(
                str(year),
                _v(c["4K"]), _v(c["1080p"]), _v(c["720p"]), _v(c["SD"]),
                str(total),
                f"{hd/total*100:.0f}%" if total else "—",
            )
            grand.update(c)

        t.add_section()
        g_total = sum(grand.values())
        g_hd    = grand["4K"] + grand["1080p"]
        t.add_row(
            "[bold]All[/bold]",
            str(grand["4K"]) or "—", str(grand["1080p"]) or "—",
            str(grand["720p"]) or "—", str(grand["SD"]) or "—",
            f"[bold]{g_total}[/bold]",
            f"[bold]{g_hd/g_total*100:.0f}%[/bold]" if g_total else "—",
        )
        console.print(t)

    def do_container_format(self, arg: str):
        """container_format [library_id] — distribution of file container formats (mkv, mp4, avi, etc.)"""
        section_id = arg.strip() or None
        with console.status("Scanning file containers..."):
            rows = self.client.media_rows_for(section_id)
        counts: Counter = Counter((r.get("container") or "unknown").lower() for r in rows)
        _distribution_table("File Container Distribution", counts)

    def do_size_by_codec(self, arg: str):
        """size_by_codec [library_id] — total and average file size grouped by video codec"""
        section_id = arg.strip() or None
        with console.status("Scanning codecs and sizes..."):
            rows = [r for r in self.client.media_rows_for(section_id)
                    if r.get("size") and r.get("videoCodec")]
        if not rows:
            console.print("[yellow]No data found.[/yellow]"); return
        groups: dict = {}
        for r in rows:
            codec = r["videoCodec"].upper()
            rec   = groups.setdefault(codec, {"total": 0, "count": 0})
            rec["total"] += r["size"]
            rec["count"] += 1
        grand_total = sum(v["total"] for v in groups.values())
        t = Table(title="Size by Video Codec", box=box.ROUNDED)
        t.add_column("Codec",      style="bold cyan",   width=10)
        t.add_column("Files",      justify="right",     width=8)
        t.add_column("Total Size", justify="right",     width=12, style="bold yellow")
        t.add_column("Avg Size",   justify="right",     width=12)
        t.add_column("Share",      justify="right",     width=7)
        for codec, rec in sorted(groups.items(), key=lambda x: x[1]["total"], reverse=True):
            avg = rec["total"] // rec["count"]
            pct = rec["total"] / grand_total * 100 if grand_total else 0
            t.add_row(codec, str(rec["count"]), format_size(rec["total"]), format_size(avg), f"{pct:.1f}%")
        t.add_section()
        t.add_row("[bold]All[/bold]", str(len(rows)), f"[bold]{format_size(grand_total)}[/bold]", "", "")
        console.print(t)

    def do_codec_migration_plan(self, arg: str):
        """codec_migration_plan [--library id] [--count N] [--min-size GB] [--target hevc|av1]"""
        tokens = self._tokens(arg)
        count = self._int_flag(tokens, "--count", 25, minimum=1)
        library_filter = self._flag_value(tokens, "--library", "").lower()
        target = self._flag_value(tokens, "--target", "hevc").lower()
        if target in ("h265", "h.265"):
            target = "hevc"
        if target not in ("hevc", "av1"):
            console.print("[yellow]--target must be hevc or av1.[/yellow]")
            return

        try:
            min_size_gb = max(0.0, float(self._flag_value(tokens, "--min-size", "4")))
        except ValueError:
            min_size_gb = 4.0
        min_size_bytes = int(min_size_gb * 1024 ** 3)

        source_savings = {
            "h264": 0.35, "avc": 0.35,
            "mpeg2video": 0.55, "mpeg2": 0.55, "mpeg1video": 0.55,
            "vc1": 0.45, "mpeg4": 0.45, "xvid": 0.45, "divx": 0.45,
        }
        skip_codecs = {"hevc", "h265", "av1"}

        def _lib_matches(lib: dict) -> bool:
            if not library_filter:
                return True
            return (library_filter == str(lib.get("key", "")).lower()
                    or library_filter in (lib.get("title", "") or "").lower())

        def _rating_value(item: dict) -> float:
            raw = item.get("audienceRating")
            if raw is None:
                raw = item.get("rating")
            try:
                return float(raw)
            except (TypeError, ValueError):
                return 0.0

        def _bitrate(media: dict, size: int, duration: int) -> int:
            raw = media.get("bitrate")
            if raw:
                try:
                    return int(raw)
                except (TypeError, ValueError):
                    pass
            if size and duration:
                return int(size * 8 / (duration / 1000) / 1000)
            return 0

        def _high_bitrate(resolution: str, bitrate: int) -> bool:
            label = resolution_label(resolution)
            thresholds = {"4K": 25000, "1080p": 12000, "720p": 6000, "SD": 3000}
            return bitrate >= thresholds.get(label, 9000)

        candidates = []
        with console.status("Scanning legacy video codecs..."):
            libs = [lib for lib in self.client.libraries()
                    if lib.get("type") in ("movie", "show") and _lib_matches(lib)]
            for lib in libs:
                lib_title = lib.get("title", "")
                for item in self.client._leaf_items(lib):
                    if item.get("type") not in ("movie", "episode"):
                        continue
                    plays = item.get("viewCount") or 0
                    rating_val = _rating_value(item)
                    for media in item.get("Media", []):
                        codec = (media.get("videoCodec") or "").lower()
                        if not codec or codec in skip_codecs or codec not in source_savings:
                            continue
                        duration = media.get("duration") or item.get("duration") or 0
                        resolution = media.get("videoResolution", "")
                        for part in media.get("Part", []):
                            size = part.get("size") or 0
                            if size < min_size_bytes:
                                continue
                            savings_ratio = source_savings[codec]
                            if target == "av1":
                                savings_ratio = min(0.65, savings_ratio + 0.10)
                            est_saved = int(size * savings_ratio)
                            bitrate = _bitrate(media, size, duration)
                            saved_gb = est_saved / (1024 ** 3)
                            score = saved_gb * 7 + min(plays, 20) * 3 + rating_val
                            reasons = [f"{codec.upper()}->{target.upper()}"]
                            if _high_bitrate(resolution, bitrate):
                                score += 6
                                reasons.append("high bitrate")
                            if plays:
                                reasons.append(f"{plays} plays")
                            else:
                                reasons.append("unwatched")
                            candidates.append({
                                "score": score,
                                "key": item.get("ratingKey", ""),
                                "title": full_title(item),
                                "library": lib_title,
                                "codec": codec.upper(),
                                "resolution": resolution_label(resolution),
                                "bitrate": bitrate,
                                "size": size,
                                "saved": est_saved,
                                "plays": plays,
                                "reason": ", ".join(reasons),
                            })

        candidates.sort(key=lambda r: (-r["score"], -r["saved"], r["title"]))
        if not candidates:
            console.print("[yellow]No migration candidates found.[/yellow]")
            console.print("[dim]Try lowering --min-size or checking a different --library.[/dim]")
            return

        shown = candidates[:count]
        total_size = sum(r["size"] for r in shown)
        total_saved = sum(r["saved"] for r in shown)
        title = (f"Codec Migration Plan -> {target.upper()} "
                 f"(top {len(shown)} of {len(candidates)}, min {min_size_gb:g} GB)")
        t = Table(title=title, box=box.ROUNDED)
        t.add_column("#", style="dim", width=4, justify="right")
        t.add_column("Key", style="dim", width=7)
        t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Library", style="cyan", width=14)
        t.add_column("Codec", width=7)
        t.add_column("Res", width=7, justify="right")
        t.add_column("Size", width=10, justify="right", style="bold yellow")
        t.add_column("Est Save", width=10, justify="right", style="green")
        t.add_column("Plays", width=6, justify="right")
        t.add_column("Why", min_width=18)
        for i, row in enumerate(shown, 1):
            bitrate = f"{row['bitrate']//1000} Mbps" if row["bitrate"] >= 1000 else (
                f"{row['bitrate']} kbps" if row["bitrate"] else "?")
            t.add_row(
                str(i), str(row["key"]), row["title"], row["library"],
                row["codec"], row["resolution"], format_size(row["size"]),
                format_size(row["saved"]), str(row["plays"]),
                f"{row['reason']} ({bitrate})",
            )
        t.add_section()
        t.add_row("", "", "[bold]Shown total[/bold]", "", "", "",
                  f"[bold]{format_size(total_size)}[/bold]",
                  f"[bold]{format_size(total_saved)}[/bold]", "", "")
        console.print(t)
        console.print("[dim]Estimates are rough: actual savings depend on encode settings, source grain, HDR, and audio copy choices.[/dim]")

    def do_channel_dist(self, arg: str):
        """channel_dist [library_id] — audio channel layout distribution (stereo, 5.1, 7.1, etc.)"""
        LABELS = {1: "Mono (1.0)", 2: "Stereo (2.0)", 6: "Surround (5.1)", 8: "Surround (7.1)"}
        section_id = arg.strip() or None
        with console.status("Scanning audio channels..."):
            rows = self.client.media_rows_for(section_id)
        counts: Counter = Counter()
        for r in rows:
            ch    = r.get("audioChannels")
            label = LABELS.get(ch, f"{ch}ch" if ch else "Unknown")
            counts[label] += 1
        _distribution_table("Audio Channel Distribution", counts)

    # ── Breakdown views ───────────────────────────────────────────────────────

    def do_popularity(self, arg: str):
        section_id = arg.strip() or None
        with console.status("Fetching watch history..."):
            hist = self.client.history(count=5000)
        if section_id:
            hist = [h for h in hist if str(h.get("librarySectionID","")) == section_id]
        if not hist:
            console.print("[yellow]No history available (may require Plex Pass).[/yellow]"); return
        counts: Counter = Counter()
        type_map: dict[str,str] = {}
        for h in hist:
            key = h.get("grandparentTitle") or h.get("title") or "?"
            counts[key] += 1
            if key not in type_map:
                type_map[key] = "show" if h.get("grandparentTitle") else h.get("type","")
        t = Table(title="Most Watched Titles", caption=f"Based on {len(hist)} history entries",
                  caption_justify="right", box=box.ROUNDED)
        t.add_column("#", style="dim", width=4); t.add_column("Title", style="bold white", min_width=30)
        t.add_column("Type", style="yellow", width=10); t.add_column("Plays", justify="right", width=8, style="bold green")
        for i, (ts, cnt) in enumerate(counts.most_common(50), 1):
            t.add_row(str(i), ts, type_map.get(ts,""), str(cnt))
        console.print(t)

    def do_genres(self, arg: str):
        items = self._all_items(arg.strip() or None)
        genre_counts: Counter = Counter()
        for item in items:
            for g in item.get("Genre",[]):
                genre_counts[g["tag"]] += 1
        _distribution_table("Genre Distribution", genre_counts)

    def do_studios(self, arg: str):
        items = self._all_items(arg.strip() or None)
        studio_counts: Counter = Counter()
        for item in items:
            s = (item.get("studio") or "").strip()
            if s: studio_counts[s] += 1
        _distribution_table("Studio Distribution", studio_counts, cap=30)

    def do_decade(self, arg: str):
        """decade [library_id] — content count broken down by decade of release"""
        section_id = arg.strip() or None
        with console.status("Scanning release years..."):
            items = self._all_items(section_id)

        decade_counts: Counter = Counter()
        no_year = 0
        for item in items:
            y = item.get("year")
            if y:
                decade_counts[(y // 10) * 10] += 1
            else:
                no_year += 1

        if not decade_counts:
            console.print("[yellow]No year data found.[/yellow]"); return

        total = sum(decade_counts.values())
        t = Table(
            title="Content by Decade" + (f" — Library {section_id}" if section_id else ""),
            box=box.ROUNDED,
        )
        t.add_column("Decade", style="bold cyan", width=10)
        t.add_column("Count", justify="right", width=8)
        t.add_column("Share", justify="right", width=7)
        t.add_column("", min_width=30)   # bar

        for decade in sorted(decade_counts):
            cnt = decade_counts[decade]
            pct = cnt / total * 100
            bar_len = int(pct / 2)   # 50 chars = 100%
            t.add_row(
                f"{decade}s",
                str(cnt),
                f"{pct:.1f}%",
                f"[cyan]{'█' * bar_len}[/cyan]",
            )

        t.add_section()
        t.add_row("[bold]Total[/bold]", f"[bold]{total}[/bold]", "", "")
        console.print(t)

        if no_year:
            console.print(f"[dim]{no_year} items have no year and are excluded.[/dim]")

    def do_content_rating(self, arg: str):
        """content_rating [library_id] — content rating distribution (G, PG, PG-13, R, TV-MA, etc.)"""
        with console.status("Scanning content ratings..."):
            items = self._all_items(arg.strip() or None)
        counts: Counter = Counter()
        unrated = 0
        for item in items:
            cr = (item.get("contentRating") or "").strip()
            if cr:
                counts[cr] += 1
            else:
                unrated += 1
        if not counts:
            console.print("[yellow]No content rating data found.[/yellow]"); return
        _distribution_table("Content Rating Distribution", counts)
        if unrated:
            console.print(f"[dim]{unrated} items have no content rating.[/dim]")

    # ── Item extras ───────────────────────────────────────────────────────────

    def do_extras(self, arg: str):
        if not arg.strip():
            console.print("[yellow]Usage: extras <key>[/yellow]"); return
        with console.status("Fetching extras..."):
            items = self.client.extras(arg.strip())
        if not items:
            console.print("[yellow]No extras found for this item.[/yellow]"); return
        noun = "item" if len(items) == 1 else "items"
        t = Table(title=f"Extras — item {arg.strip()}", caption=f"{len(items)} {noun}",
                  caption_justify="right", box=box.ROUNDED)
        t.add_column("Key", style="dim", width=7); t.add_column("Title", style="bold white", min_width=30)
        t.add_column("Subtype", style="yellow", width=18); t.add_column("Duration", width=9, justify="right")
        for item in items:
            t.add_row(item.get("ratingKey",""), item.get("title",""),
                      item.get("subtype") or item.get("extraType") or item.get("type","—"),
                      format_duration(item.get("duration")))
        console.print(t)

    def do_related(self, arg: str):
        if not arg.strip():
            console.print("[yellow]Usage: related <key>[/yellow]"); return
        with console.status("Fetching related content..."):
            items = self.client.related(arg.strip())
        print_media_table(items, f"Related to {arg.strip()}")

    # ── Users & Sharing ───────────────────────────────────────────────────────

    def do_users(self, _):
        with console.status("Fetching accounts..."):
            accounts = self.client.accounts()
        if not accounts:
            console.print("[yellow]No accounts found (may require Plex Pass or admin token).[/yellow]"); return
        t = Table(title=f"Server Accounts ({len(accounts)})", box=box.ROUNDED)
        t.add_column("ID", style="dim", width=6, justify="right"); t.add_column("Name", style="bold cyan", min_width=20)
        t.add_column("Admin", width=7, justify="center")
        for acct in accounts:
            t.add_row(str(acct.get("id","?")), acct.get("name") or acct.get("title") or "—",
                      "[green]admin[/green]" if acct.get("id") == 1 else "")
        console.print(t)
        console.print("[dim]Use [bold]userstats <name>[/bold] for per-user watch detail.[/dim]")

    def do_userstats(self, arg: str):
        username_filter = arg.strip().lower()
        with console.status("Fetching accounts and history..."):
            all_hist = self.client.history(count=2000)
        if not all_hist:
            console.print("[yellow]No history available (may require Plex Pass).[/yellow]"); return
        if not username_filter:
            user_records: dict = {}
            for h in all_hist:
                uname = h.get("User",{}).get("title") or str(h.get("accountID") or "Unknown")
                rec = user_records.setdefault(uname, {"plays":0,"movies":0,"episodes":0,"last_at":0})
                rec["plays"] += 1
                if h.get("type") == "movie": rec["movies"] += 1
                elif h.get("type") == "episode": rec["episodes"] += 1
                va = h.get("viewedAt") or 0
                if va > rec["last_at"]: rec["last_at"] = va
            t = Table(title="Watch Stats by User", box=box.ROUNDED)
            t.add_column("User", style="bold cyan", min_width=20); t.add_column("Total Plays", justify="right", width=12)
            t.add_column("Movies", justify="right", width=8); t.add_column("Episodes", justify="right", width=10)
            t.add_column("Last Watched", style="dim", width=18)
            for uname, rec in sorted(user_records.items(), key=lambda x: x[1]["plays"], reverse=True):
                t.add_row(uname, str(rec["plays"]), str(rec["movies"]), str(rec["episodes"]),
                          format_ts(rec["last_at"]) if rec["last_at"] else "—")
            console.print(t)
            console.print(f"[dim]Based on the last {len(all_hist)} history entries. Run [bold]userstats <name>[/bold] for detail.[/dim]")
            return
        user_hist = [h for h in all_hist if (h.get("User",{}).get("title") or "").lower() == username_filter]
        if not user_hist:
            console.print(f"[yellow]No history found for user '{arg.strip()}'.[/yellow]"); return
        display_name = user_hist[0].get("User",{}).get("title", arg.strip())
        movies = [h for h in user_hist if h.get("type") == "movie"]
        episodes = [h for h in user_hist if h.get("type") == "episode"]
        other = [h for h in user_hist if h.get("type") not in ("movie","episode")]
        last = user_hist[0]
        last_title = last.get("grandparentTitle") or last.get("title","?")
        if last.get("grandparentTitle"):
            last_title += f" — {last.get('title','')}"
        console.print(Panel(
            f"[bold cyan]Total plays:[/bold cyan] {len(user_hist)}\n"
            f"[bold cyan]Movies:[/bold cyan] {len(movies)}  [bold cyan]Episodes:[/bold cyan] {len(episodes)}  "
            f"[bold cyan]Other:[/bold cyan] {len(other)}\n"
            f"[bold cyan]Last watched:[/bold cyan] {last_title}  [dim]{format_ts(last.get('viewedAt'))}[/dim]",
            title=f"[bold white]{display_name}[/bold white]", border_style="cyan"))
        tc: Counter = Counter()
        for h in user_hist:
            tc[h.get("grandparentTitle") or h.get("title") or "?"] += 1
        t2 = Table(title="Most Watched", box=box.ROUNDED)
        t2.add_column("Title", style="bold white", min_width=30); t2.add_column("Plays", justify="right", width=7)
        for ts, cnt in tc.most_common(10):
            t2.add_row(ts, str(cnt))
        console.print(t2)
        t3 = Table(title="Recent Plays (last 20)", box=box.ROUNDED)
        t3.add_column("When", style="dim", width=17); t3.add_column("Type", style="yellow", width=9)
        t3.add_column("Title", style="bold white", min_width=30)
        for h in user_hist[:20]:
            ts = h.get("title","")
            if h.get("grandparentTitle"):
                ts = f"{h['grandparentTitle']} — {h.get('parentTitle','')} — {ts}"
            t3.add_row(format_ts(h.get("viewedAt")), h.get("type",""), ts)
        console.print(t3)

    # ── Playlists & Collections ───────────────────────────────────────────────

    def do_playlists(self, _):
        with console.status("Fetching playlists..."):
            playlists = self.client.get_playlists()
        if not playlists:
            console.print("[yellow]No playlists found.[/yellow]"); return
        t = Table(title=f"Playlists ({len(playlists)})", box=box.ROUNDED)
        t.add_column("Key", style="dim", width=7); t.add_column("Title", style="bold cyan", min_width=28)
        t.add_column("Type", style="yellow", width=10); t.add_column("Items", justify="right", width=7)
        t.add_column("Duration", justify="right", width=10)
        for pl in playlists:
            t.add_row(pl.get("ratingKey",""), pl.get("title",""),
                      pl.get("playlistType", pl.get("type","")), str(pl.get("leafCount","?")),
                      format_duration(pl.get("duration")))
        console.print(t)

    def do_playlist(self, arg: str):
        if not arg.strip():
            console.print("[yellow]Usage: playlist <id>[/yellow]"); return
        with console.status("Fetching playlist..."):
            items = self.client.playlist_items(arg.strip())
        if not items:
            console.print("[yellow]Playlist is empty or not found.[/yellow]"); return
        noun = "item" if len(items) == 1 else "items"
        t = Table(title=f"Playlist {arg.strip()}", caption=f"{len(items)} {noun}",
                  caption_justify="right", box=box.ROUNDED)
        t.add_column("Item ID", style="dim", width=9); t.add_column("Key", style="dim", width=7)
        t.add_column("Title", style="bold white", min_width=28); t.add_column("Type", style="yellow", width=10)
        t.add_column("Year", width=6, justify="right"); t.add_column("Duration", width=9, justify="right")
        for item in items:
            t.add_row(str(item.get("playlistItemID","—")), item.get("ratingKey",""), full_title(item),
                      item.get("type",""), year(item), format_duration(item.get("duration")))
        console.print(t)
        console.print("[dim]Use the Item ID with [bold]playlist_remove[/bold] to remove an item.[/dim]")

    def do_playlist_create(self, arg: str):
        tokens = arg.strip().split()
        if not tokens:
            console.print("[yellow]Usage: playlist_create <name> [key][/yellow]"); return
        if len(tokens) > 1 and tokens[-1].isdigit():
            rating_key, name = tokens[-1], " ".join(tokens[:-1])
        else:
            rating_key, name = "", " ".join(tokens)
        with console.status(f"Creating playlist [cyan]{name}[/cyan]..."):
            result = self.client.create_playlist(name, rating_key)
        if result:
            console.print(f"[green]Playlist '{name}' created[/green] (key: {result.get('ratingKey','')})")
        else:
            console.print("[red]Failed to create playlist.[/red]")

    def do_playlist_add(self, arg: str):
        parts = arg.strip().split()
        if len(parts) < 2:
            console.print("[yellow]Usage: playlist_add <playlist_id> <key>[/yellow]"); return
        with console.status(f"Adding item [cyan]{parts[1]}[/cyan] to playlist {parts[0]}..."):
            ok = self.client.playlist_add_item(parts[0], parts[1])
        if ok:
            console.print(f"[green]Item {parts[1]} added to playlist {parts[0]}.[/green]")

    def do_playlist_remove(self, arg: str):
        parts = arg.strip().split()
        if len(parts) < 2:
            console.print("[yellow]Usage: playlist_remove <playlist_id> <item_id>[/yellow]")
            console.print("[dim]Use [bold]playlist <id>[/bold] to see item IDs.[/dim]"); return
        with console.status(f"Removing item {parts[1]} from playlist {parts[0]}..."):
            ok = self.client.playlist_remove_item(parts[0], parts[1])
        if ok:
            console.print(f"[green]Item {parts[1]} removed from playlist {parts[0]}.[/green]")

    def do_playlist_build(self, arg: str):
        """playlist_build <name> [filters] — build a Plex playlist from combined filters"""
        try:
            tokens = shlex.split(arg.strip()) if arg.strip() else []
        except ValueError:
            tokens = arg.strip().split()

        if not tokens:
            console.print(
                "[yellow]Usage: playlist_build <name> [filters][/yellow]\n"
                "[dim]Filters (all optional, combined with AND):[/dim]\n"
                "[dim]  --genre <g>          e.g. Comedy, Horror, Drama[/dim]\n"
                "[dim]  --director <name>    e.g. \"Stanley Kubrick\"[/dim]\n"
                "[dim]  --actor <name>       e.g. \"Tom Hanks\"[/dim]\n"
                "[dim]  --year <y>           exact release year[/dim]\n"
                "[dim]  --decade <Xd>        e.g. 1990s or 90s[/dim]\n"
                "[dim]  --studio <s>         studio name[/dim]\n"
                "[dim]  --contentrating <r>  e.g. R, PG-13, TV-MA[/dim]\n"
                "[dim]  --unwatched          only items with no plays[/dim]\n"
                "[dim]  --rating <n>         minimum audience rating (0-10)[/dim]\n"
                "[dim]  --library <id>       restrict to one library[/dim]\n"
                "[dim]  --limit <n>          cap number of results[/dim]\n"
                "[dim]Examples:[/dim]\n"
                "[dim]  playlist_build \"Unseen Kubrick\" --director \"Stanley Kubrick\" --unwatched[/dim]\n"
                "[dim]  playlist_build \"90s Comedies\" --genre Comedy --decade 1990s[/dim]\n"
                "[dim]  playlist_build \"Top Horror\" --genre Horror --rating 7.5 --limit 25[/dim]"
            )
            return

        # Name = everything before the first --flag
        name_parts, i = [], 0
        while i < len(tokens) and not tokens[i].startswith("--"):
            name_parts.append(tokens[i]); i += 1
        playlist_name = " ".join(name_parts)
        if not playlist_name:
            console.print("[red]Playlist name required (must precede any flags).[/red]"); return

        VALUE_FLAGS = {"--genre", "--director", "--actor", "--year", "--decade",
                       "--studio", "--contentrating", "--library", "--rating", "--limit"}
        server_filters: dict   = {}
        unwatched              = False
        min_rating: float | None = None
        library_id: str | None   = None
        limit: int | None        = None

        flag_tokens, j = tokens[i:], 0
        while j < len(flag_tokens):
            flag = flag_tokens[j]
            val  = flag_tokens[j + 1] if flag in VALUE_FLAGS and j + 1 < len(flag_tokens) else None

            if   flag == "--genre"         and val: server_filters["genre"]         = val;  j += 2
            elif flag == "--director"      and val: server_filters["director"]      = val;  j += 2
            elif flag == "--actor"         and val: server_filters["actor"]         = val;  j += 2
            elif flag == "--studio"        and val: server_filters["studio"]        = val;  j += 2
            elif flag == "--year"          and val: server_filters["year"]          = val;  j += 2
            elif flag == "--contentrating" and val: server_filters["contentRating"] = val;  j += 2
            elif flag == "--decade"        and val:
                m = re.match(r"^(\d{4})s?$", val)
                if m:
                    server_filters["decade"] = str(int(m.group(1)) // 10 * 10)
                else:
                    m2 = re.match(r"^(\d{2})s$", val)
                    if m2:
                        n = int(m2.group(1))
                        server_filters["decade"] = str(1900 + n if n >= 20 else 2000 + n)
                    else:
                        console.print(f"[yellow]Cannot parse decade '{val}' — use e.g. 1990s or 90s.[/yellow]")
                j += 2
            elif flag == "--unwatched": unwatched = True; j += 1
            elif flag == "--rating" and val:
                try: min_rating = float(val)
                except ValueError: console.print(f"[yellow]--rating must be a number.[/yellow]")
                j += 2
            elif flag == "--library" and val: library_id = val; j += 2
            elif flag == "--limit"   and val:
                try: limit = int(val)
                except ValueError: console.print(f"[yellow]--limit must be an integer.[/yellow]")
                j += 2
            else: j += 1

        if not server_filters and not unwatched and min_rating is None:
            console.print("[yellow]At least one filter flag is required.[/yellow]"); return

        if library_id:
            libs = [{"key": library_id, "title": f"Library {library_id}"}]
        else:
            libs = [l for l in self.client.libraries() if l.get("type") in ("movie", "show")]

        with console.status(f"Searching {len(libs)} librar{'y' if len(libs)==1 else 'ies'}..."):
            results: list[dict] = []
            for lib in libs:
                results.extend(self.client.section_search(lib.get("key", ""), **server_filters))

        # Client-side filters
        if unwatched:
            results = [r for r in results if not (r.get("viewCount") or 0)]
        if min_rating is not None:
            results = [r for r in results if (r.get("audienceRating") or 0) >= min_rating]

        # Deduplicate
        seen: set[str] = set()
        deduped: list[dict] = []
        for r in results:
            k = r.get("ratingKey", "")
            if k and k not in seen:
                seen.add(k); deduped.append(r)
        results = deduped[:limit] if limit else deduped

        if not results:
            console.print("[yellow]No items match the given filters.[/yellow]"); return

        # Preview
        t = Table(
            title=f"Preview: [cyan]{playlist_name}[/cyan]  ({len(results)} items)",
            box=box.ROUNDED,
        )
        t.add_column("Title",  style="bold white", min_width=30)
        t.add_column("Year",   width=6,  justify="right")
        t.add_column("Rating", width=8,  justify="right", style="dim")
        for r in results[:25]:
            rating = r.get("audienceRating")
            t.add_row(full_title(r), str(r.get("year") or ""),
                      f"{rating:.1f}" if rating else "—")
        if len(results) > 25:
            t.add_row(f"[dim]… and {len(results) - 25} more[/dim]", "", "")
        console.print(t)

        if Prompt.ask(
            f"Create playlist [cyan]'{playlist_name}'[/cyan] with {len(results)} item(s)?",
            choices=["y", "n"], default="y",
        ) != "y":
            console.print("[dim]Cancelled.[/dim]"); return

        # Create with first item, then add the rest
        with console.status(f"Creating playlist [cyan]{playlist_name}[/cyan]..."):
            pl = self.client.create_playlist(playlist_name, results[0].get("ratingKey", ""))
        if not pl:
            console.print("[red]Failed to create playlist.[/red]"); return

        pl_key = pl.get("ratingKey", "")
        added, failed = 1, 0
        with console.status("Adding items...") as status:
            for n, item in enumerate(results[1:], 2):
                item_key = item.get("ratingKey", "")
                if not item_key: continue
                if self.client.playlist_add_item(pl_key, item_key): added += 1
                else: failed += 1
                if n % 10 == 0:
                    status.update(f"Adding items… {n}/{len(results)}")

        desc = ", ".join(f"{k}={v}" for k, v in server_filters.items())
        if unwatched:   desc += (", " if desc else "") + "unwatched"
        if min_rating:  desc += (", " if desc else "") + f"rating≥{min_rating}"
        console.print(f"[green]'{playlist_name}'[/green] created — {added} item(s)."
                      + (f"  [red]{failed} failed.[/red]" if failed else ""))
        console.print(f"[dim]Filters: {desc}[/dim]")
        console.print(f"[dim]View with: [bold]playlist {pl_key}[/bold][/dim]")

    def do_plex_metadata(self, arg: str):
        """plex_metadata [--library <name>] [--fix] [--episodes] — audit library for missing metadata"""
        tokens = arg.strip().split() if arg.strip() else []
        fix              = "--fix" in tokens
        include_episodes = "--episodes" in tokens

        lib_filter = None
        if "--library" in tokens:
            idx = tokens.index("--library")
            if idx + 1 < len(tokens):
                lib_filter = tokens[idx + 1]

        MOVIE_FIELDS = [
            ("thumb",                 "Poster"),
            ("summary",               "Summary"),
            ("Genre",                 "Genres"),
            ("contentRating",         "Content Rating"),
            ("Director",              "Director"),
            ("Role",                  "Cast"),
            ("originallyAvailableAt", "Release Date"),
            ("audienceRating",        "Rating"),
        ]
        SHOW_FIELDS = [
            ("thumb",                 "Poster"),
            ("summary",               "Summary"),
            ("Genre",                 "Genres"),
            ("contentRating",         "Content Rating"),
            ("Role",                  "Cast"),
            ("originallyAvailableAt", "Release Date"),
        ]
        EP_FIELDS = [("thumb", "Poster"), ("summary", "Summary")]

        def _missing(item: dict, fields: list) -> list[str]:
            out = []
            for field, label in fields:
                val = item.get(field)
                if val is None or val == "" or val == []:
                    out.append(label)
            return out

        libs = self.client.libraries()
        if lib_filter:
            libs = [l for l in libs if l.get("key") == lib_filter
                    or l.get("title", "").lower() == lib_filter.lower()]
        libs = [l for l in libs if l.get("type") in ("movie", "show")]

        if not libs:
            console.print("[yellow]No matching libraries found.[/yellow]"); return

        flagged: list[tuple[str, str, int, list[str], str]] = []

        with console.status("Scanning libraries...") as status:
            for lib in libs:
                lib_key  = lib.get("key", "")
                lib_name = lib.get("title", lib_key)
                lib_type = lib.get("type", "movie")
                fields   = MOVIE_FIELDS if lib_type == "movie" else SHOW_FIELDS

                status.update(f"Scanning [bold]{lib_name}[/bold]…")
                for item in self.client.library_contents(lib_key):
                    missing = _missing(item, fields)
                    if missing:
                        flagged.append((
                            lib_type,
                            item.get("title", "?"),
                            item.get("year") or 0,
                            missing,
                            item.get("ratingKey", ""),
                        ))

                if include_episodes and lib_type == "show":
                    status.update(f"Scanning episodes in [bold]{lib_name}[/bold]…")
                    for ep in self.client.library_episodes(lib_key):
                        missing = _missing(ep, EP_FIELDS)
                        if missing:
                            show   = ep.get("grandparentTitle", "?")
                            season = ep.get("parentIndex")
                            ep_num = ep.get("index")
                            if isinstance(season, int) and isinstance(ep_num, int):
                                label = f"{show} S{season:02d}E{ep_num:02d}"
                            else:
                                label = f"{show} — {ep.get('title', '?')}"
                            flagged.append(("episode", label, 0, missing, ep.get("ratingKey", "")))

        if not flagged:
            console.print("[green]No metadata issues found.[/green]"); return

        flagged.sort(key=lambda x: -len(x[3]))

        t = Table(title=f"Metadata Issues ({len(flagged)} items)", box=box.ROUNDED)
        t.add_column("Type",    width=8, style="dim")
        t.add_column("Title",   style="bold cyan", min_width=30)
        t.add_column("Year",    width=6, justify="right", style="dim")
        t.add_column("Missing", style="yellow")
        for item_type, title, year, missing, _ in flagged:
            t.add_row(item_type, title, str(year) if year else "", ", ".join(missing))
        console.print(t)

        if fix:
            keys = [rk for _, _, _, _, rk in flagged if rk]
            with console.status(f"Refreshing metadata for {len(keys)} item(s)…"):
                for rk in keys:
                    self.client.refresh_metadata(rk)
            console.print(f"[green]Triggered metadata refresh for {len(keys)} item(s).[/green]")
            console.print("[dim]Plex will re-fetch metadata in the background.[/dim]")
        else:
            console.print(f"[dim]Run with [bold]--fix[/bold] to trigger Plex metadata refresh on all flagged items.[/dim]")
            if not include_episodes:
                console.print("[dim]Run with [bold]--episodes[/bold] to also audit episode-level metadata (slow on large libraries).[/dim]")

    def do_plex_duplicates(self, arg: str):
        """plex_duplicates [--library <name>] — find duplicate movies across Plex libraries"""
        tokens = arg.strip().split() if arg.strip() else []

        lib_filter = None
        if "--library" in tokens:
            idx = tokens.index("--library")
            if idx + 1 < len(tokens):
                lib_filter = tokens[idx + 1]

        libs = self.client.libraries()
        if lib_filter:
            libs = [l for l in libs if l.get("key") == lib_filter
                    or l.get("title", "").lower() == lib_filter.lower()]
        libs = [l for l in libs if l.get("type") == "movie"]

        if not libs:
            console.print("[yellow]No movie libraries found.[/yellow]"); return

        groups: dict[tuple, list] = defaultdict(list)

        with console.status("Scanning movie libraries...") as status:
            for lib in libs:
                lib_key  = lib.get("key", "")
                lib_name = lib.get("title", lib_key)
                status.update(f"Scanning [bold]{lib_name}[/bold]…")
                for item in self.client.library_contents(lib_key):
                    title = (item.get("title") or "").lower().strip()
                    year  = item.get("year") or 0
                    item["_library"] = lib_name
                    groups[(title, year)].append(item)

        # Build duplicate groups; attach pre-computed per-media sizes to avoid recomputing during sort
        duplicates: list[tuple[list, list[int]]] = []
        for items in groups.values():
            sizes: list[int] = [
                sum((p.get("size") or 0) for p in (m.get("Part") or []))
                for item in items
                for m in (item.get("Media") or [])
            ]
            if len(items) > 1 or len(items[0].get("Media") or []) > 1:
                duplicates.append((items, sizes))

        if not duplicates:
            console.print("[green]No duplicates found.[/green]"); return

        duplicates.sort(key=lambda g: -sum(g[1]))

        total_wasted = 0
        for items, sizes in duplicates:
            title = items[0].get("title", "?")
            year  = items[0].get("year", 0)

            t = Table(title=f"[yellow]{title}[/yellow] ({year})", box=box.SIMPLE_HEAD)
            t.add_column("Library",    style="dim",  width=20)
            t.add_column("Resolution", width=12)
            t.add_column("Codec",      width=8,  style="dim")
            t.add_column("Size",       width=10, justify="right")
            t.add_column("Added",      width=17, style="dim")

            size_iter = iter(sizes)
            for item in items:
                lib_name = item.get("_library", "?")
                for media in (item.get("Media") or []):
                    res_raw   = media.get("videoResolution", "")
                    res       = f"{res_raw}p" if res_raw and str(res_raw).isdigit() else (res_raw or "?")
                    codec     = media.get("videoCodec", "?")
                    part_size = next(size_iter, 0)
                    t.add_row(lib_name, res, codec,
                              format_size(part_size) if part_size else "?",
                              format_ts(item.get("addedAt")))

            if len(sizes) > 1:
                total_wasted += sum(sizes) - max(sizes)
            console.print(t)


        if total_wasted:
            console.print(f"\n[yellow]Potential reclaimable space:[/yellow] {format_size(total_wasted)} "
                          f"[dim](keeping the largest copy of each duplicate)[/dim]")
        console.print(f"[dim]{len(duplicates)} duplicate group(s) found.[/dim]")

    def do_collections(self, arg: str):
        section_id = arg.strip() or None
        with console.status("Fetching collections..."):
            collections = self.client.get_collections(section_id)
        if not collections:
            msg = "No collections found" + (f" in library {section_id}" if section_id else "")
            console.print(f"[yellow]{msg}.[/yellow]"); return
        t = Table(title=f"Collections ({len(collections)})", box=box.ROUNDED)
        t.add_column("Key", style="dim", width=7); t.add_column("Title", style="bold cyan", min_width=28)
        t.add_column("Items", justify="right", width=7); t.add_column("Added", style="dim", width=17)
        for c in sorted(collections, key=lambda x: x.get("title","").lower()):
            t.add_row(c.get("ratingKey",""), c.get("title",""), str(c.get("childCount","?")),
                      format_ts(c.get("addedAt")))
        console.print(t)

    def do_collection(self, arg: str):
        if not arg.strip():
            console.print("[yellow]Usage: collection <key>[/yellow]"); return
        with console.status("Fetching collection..."):
            items = self.client.children(arg.strip())
        print_media_table(items, f"Collection {arg.strip()}")

    # ── Radarr commands ───────────────────────────────────────────────────────

    def do_radarr_status(self, _):
        """radarr_status — Radarr connection info, quality profiles, and root folders"""
        rc = self._get_radarr_client()
        if not rc: return
        with console.status("Fetching Radarr info..."):
            info     = rc.status()
            profiles = rc.quality_profiles()
            folders  = rc.root_folders()
        console.print(Panel(
            f"[bold cyan]Version:[/bold cyan]  {info.get('version','—')}\n"
            f"[bold cyan]URL:[/bold cyan]      {rc.base_url}\n"
            f"[bold cyan]OS:[/bold cyan]       {info.get('osName','—')} {info.get('osVersion','—')}",
            title="[bold white]Radarr[/bold white]", border_style="green"))
        pt = Table(title="Quality Profiles", box=box.ROUNDED)
        pt.add_column("ID", style="dim", width=6); pt.add_column("Name", style="bold cyan")
        for p in profiles: pt.add_row(str(p.get("id","")), p.get("name",""))
        console.print(pt)
        ft = Table(title="Root Folders", box=box.ROUNDED)
        ft.add_column("Path", style="bold white", min_width=28)
        ft.add_column("Free Space", justify="right", width=12)
        for f in folders: ft.add_row(f.get("path",""), format_size(f.get("freeSpace")))
        console.print(ft)

    def do_radarr_lists(self, _):
        """radarr_lists — show available named TMDB lists with movie counts"""
        lists = load_lists()

        # Silently fetch counts from TMDB if credentials are available
        counts: dict[int, str] = {}
        cfg = load_config()
        tmdb_key = cfg.get("tmdb_api_key") or os.environ.get("TMDB_API_KEY", "")
        if tmdb_key:
            tc = TMDBClient(tmdb_key)
            with console.status("Fetching list counts from TMDB..."):
                for list_id in lists.values():
                    info = tc.list_info(list_id)
                    counts[list_id] = str(info.get("item_count", "—")) if info else "—"

        t = Table(title=f"Available Lists ({len(lists)})", box=box.ROUNDED)
        t.add_column("Name",         style="bold cyan", min_width=22)
        t.add_column("TMDB List ID", style="dim",       width=14, justify="right")
        if counts:
            t.add_column("Movies", width=8, justify="right")
        for n, list_id in sorted(lists.items()):
            row = [n, str(list_id)]
            if counts:
                row.append(counts.get(list_id, "—"))
            t.add_row(*row)
        console.print(t)
        console.print(f"[dim]Config: {LISTS_FILE}[/dim]")
        console.print("[dim]Use [bold]radarr_list <name>[/bold] to preview, "
                      "[bold]radarr_list_add <name> <id>[/bold] to add a list, "
                      "[bold]radarr_import <name>[/bold] to add to Radarr, "
                      "or [bold]radarr_director <name>[/bold] for any director.[/dim]")

    def do_radarr_list_add(self, arg: str):
        """radarr_list_add <name> <tmdb_list_id|url> — add or update a named TMDB list in the config"""
        parts = arg.strip().split()
        if len(parts) < 2:
            console.print("[yellow]Usage: radarr_list_add <name> <tmdb_list_id_or_url>[/yellow]")
            console.print(f"[dim]Lists are saved to {LISTS_FILE}[/dim]"); return
        name = parts[0]
        raw_id = parts[1]
        # Accept a full TMDB URL or a bare integer ID
        m = re.search(r"/list/(\d+)", raw_id)
        if m:
            list_id = int(m.group(1))
        else:
            try:
                list_id = int(raw_id)
            except ValueError:
                console.print("[red]Second argument must be a TMDB list ID or URL.[/red]"); return
        lists = load_lists()
        existed = name in lists
        lists[name] = list_id
        save_lists(lists)
        action = "Updated" if existed else "Added"
        console.print(f"[green]{action}:[/green] [bold]{name}[/bold] → TMDB list {list_id}")
        console.print(f"[dim]Saved to {LISTS_FILE}[/dim]")
        if hasattr(self, "_c_radarr_lists"):
            del self._c_radarr_lists  # invalidate tab-completion cache

    def do_radarr_list_remove(self, arg: str):
        """radarr_list_remove <name> — remove a named list from the config"""
        name = arg.strip()
        if not name:
            console.print("[yellow]Usage: radarr_list_remove <name>[/yellow]"); return
        lists = load_lists()
        if name not in lists:
            console.print(f"[yellow]'{name}' not found in list config.[/yellow]"); return
        del lists[name]
        save_lists(lists)
        console.print(f"[green]Removed:[/green] [bold]{name}[/bold]")
        if hasattr(self, "_c_radarr_lists"):
            del self._c_radarr_lists

    def do_radarr_list_info(self, arg: str):
        """radarr_list_info <id|url> — show name, description and item count for any TMDB list"""
        raw = arg.strip()
        if not raw:
            console.print("[yellow]Usage: radarr_list_info <tmdb_list_id_or_url>[/yellow]")
            console.print("[dim]Example: radarr_list_info 145406[/dim]")
            console.print("[dim]Example: radarr_list_info https://www.themoviedb.org/list/145406-afi[/dim]")
            return
        # Accept a full TMDB URL or a bare ID
        m = re.search(r"/list/(\d+)", raw)
        list_id = int(m.group(1)) if m else None
        if not list_id:
            try: list_id = int(raw)
            except ValueError:
                console.print(f"[red]Cannot parse list ID from:[/red] {raw}"); return
        tc = self._get_tmdb_client()
        if not tc: return
        with console.status(f"Fetching TMDB list {list_id}..."):
            info = tc.list_info(list_id)
        if not info:
            console.print(f"[red]List {list_id} not found on TMDB.[/red]"); return
        console.print(Panel(
            f"[bold cyan]Name:[/bold cyan]        {info.get('name','—')}\n"
            f"[bold cyan]Created by:[/bold cyan]  {info.get('created_by','—')}\n"
            f"[bold cyan]Items:[/bold cyan]        {info.get('item_count', info.get('total_results','—'))}\n"
            f"[bold cyan]Description:[/bold cyan] {info.get('description','—') or '—'}\n"
            f"[bold cyan]TMDB ID:[/bold cyan]     {list_id}",
            title="[bold white]TMDB List Info[/bold white]", border_style="cyan"))
        safe_name = re.sub(r"[^a-z0-9]+", "_", info.get("name","list").lower()).strip("_")
        console.print(f"[dim]To add: [bold]radarr_list_add {safe_name} {list_id}[/bold][/dim]")

    def do_radarr_list(self, arg: str):
        """radarr_list <name> — preview a list: title/year/in-Radarr/in-Plex (fetched live from TMDB)"""
        name = arg.strip()
        if not name:
            console.print("[yellow]Usage: radarr_list <name>[/yellow]")
            console.print("[dim]Run [bold]radarr_lists[/bold] for available names.[/dim]"); return
        if name not in load_lists():
            console.print(f"[red]Unknown list:[/red] '{name}'")
            console.print("[dim]Run [bold]radarr_lists[/bold] for available names.[/dim]"); return
        tc = self._get_tmdb_client()
        rc = self._get_radarr_client()
        if not tc or not rc: return
        with console.status(f"Fetching list from TMDB..."):
            movies = tc.list_movies(load_lists()[name])
        if not movies:
            console.print("[yellow]No movies returned from TMDB.[/yellow]"); return
        with console.status("Loading Radarr and Plex libraries..."):
            radarr_ids = {m.get("tmdbId") for m in rc.movies()}
            plex_set   = self._plex_movie_set()
        missing = 0
        t = Table(title=f"{name}  ({len(movies)} titles)", box=box.ROUNDED)
        t.add_column("#",      style="dim",       width=4,  justify="right")
        t.add_column("Title",  style="bold white", min_width=32)
        t.add_column("Year",   width=6,            justify="right")
        t.add_column("Radarr", width=9,            justify="center")
        t.add_column("Plex",   width=9,            justify="center")
        for i, m in enumerate(movies, 1):
            in_r = m["tmdb_id"] in radarr_ids
            in_p = self._in_plex(m["title"], m["year"], plex_set)
            if not in_r: missing += 1
            t.add_row(str(i), m["title"], str(m["year"]),
                      "[green]yes[/green]" if in_r else "[red]no[/red]",
                      "[green]yes[/green]" if in_p else "[dim]no[/dim]")
        console.print(t)
        console.print(f"[yellow]{missing} titles not in Radarr.[/yellow]  "
                      f"[dim]Run [bold]radarr_import {name}[/bold] to add them.[/dim]")

    def do_radarr_import(self, arg: str):
        """radarr_import <name> [--dry-run] [--profile <id>] [--search] — add missing movies from a named list"""
        try:
            tokens = shlex.split(arg.strip()) if arg.strip() else []
        except ValueError:
            tokens = arg.strip().split()
        if not tokens:
            console.print("[yellow]Usage: radarr_import <name> [--dry-run] [--profile <id>] [--search][/yellow]")
            console.print("[dim]Run [bold]radarr_lists[/bold] for available names.[/dim]"); return
        name      = tokens[0]
        dry_run   = "--dry-run" in tokens
        search    = "--search"  in tokens
        profile_id: int | None = None
        if "--profile" in tokens:
            idx = tokens.index("--profile")
            if idx + 1 < len(tokens):
                try: profile_id = int(tokens[idx + 1])
                except ValueError:
                    console.print("[red]--profile requires an integer ID.[/red]"); return
        if name not in load_lists():
            console.print(f"[red]Unknown list:[/red] '{name}'")
            console.print("[dim]Run [bold]radarr_lists[/bold] for available names.[/dim]"); return
        tc = self._get_tmdb_client()
        rc = self._get_radarr_client()
        if not tc or not rc: return
        with console.status("Fetching list from TMDB..."):
            movies = tc.list_movies(load_lists()[name])
        if not movies:
            console.print("[yellow]No movies returned from TMDB.[/yellow]"); return
        self._radarr_import_workflow(movies, rc, dry_run, profile_id, search)

    def do_radarr_director(self, arg: str):
        """radarr_director <name> [--dry-run] [--profile <id>] [--search] — import a director's filmography"""
        try:
            tokens = shlex.split(arg.strip()) if arg.strip() else []
        except ValueError:
            tokens = arg.strip().split()
        # Separate name tokens from flags
        flag_starts = next((i for i, t in enumerate(tokens) if t.startswith("-")), len(tokens))
        name_tokens = tokens[:flag_starts]
        flag_tokens = tokens[flag_starts:]
        if not name_tokens:
            console.print("[yellow]Usage: radarr_director <name> [--dry-run] [--profile <id>] [--search][/yellow]")
            return
        name      = " ".join(name_tokens)
        dry_run   = "--dry-run" in flag_tokens
        search    = "--search"  in flag_tokens
        profile_id: int | None = None
        if "--profile" in flag_tokens:
            idx = flag_tokens.index("--profile")
            if idx + 1 < len(flag_tokens):
                try: profile_id = int(flag_tokens[idx + 1])
                except ValueError:
                    console.print("[red]--profile requires an integer ID.[/red]"); return
        tc = self._get_tmdb_client()
        rc = self._get_radarr_client()
        if not tc or not rc: return
        with console.status(f"Searching TMDB for [cyan]{name}[/cyan]..."):
            results = tc.search_person(name)
        # Filter to directors/crew
        directors = [r for r in results
                     if r.get("known_for_department") in ("Directing", "Writing", "Production")
                     or any(k.get("media_type") == "movie" for k in r.get("known_for", []))]
        if not directors:
            directors = results  # fall back to all results if no directors found
        if not directors:
            console.print(f"[yellow]No person found for '{name}'.[/yellow]"); return
        if len(directors) == 1:
            person = directors[0]
            console.print(f"[dim]Found: {person.get('name')} (TMDB #{person.get('id')})[/dim]")
        else:
            pt = Table(title=f"Multiple matches for '{name}'", box=box.ROUNDED)
            pt.add_column("#",        style="dim",       width=4)
            pt.add_column("Name",     style="bold white", min_width=24)
            pt.add_column("Known For", style="dim",      min_width=16)
            pt.add_column("TMDB ID",  style="dim",       width=10, justify="right")
            for i, p in enumerate(directors[:10], 1):
                known = p.get("known_for_department", "—")
                pt.add_row(str(i), p.get("name",""), known, str(p.get("id","")))
            console.print(pt)
            choice = Prompt.ask("Select #", choices=[str(i) for i in range(1, min(len(directors),10)+1)])
            person = directors[int(choice) - 1]
        with console.status(f"Fetching filmography for [cyan]{person.get('name')}[/cyan]..."):
            movies = tc.director_filmography(person["id"])
        if not movies:
            console.print(f"[yellow]No directed films found for {person.get('name')}.[/yellow]"); return
        console.print(f"[dim]{len(movies)} directed films found.[/dim]")
        self._radarr_import_workflow(movies, rc, dry_run, profile_id, search)

    def do_radarr_download(self, arg: str):
        """radarr_download <name> [--dry-run] [--profile <id>] — download missing movies from a list"""
        try:
            tokens = shlex.split(arg.strip()) if arg.strip() else []
        except ValueError:
            tokens = arg.strip().split()
        if not tokens:
            console.print("[yellow]Usage: radarr_download <name> [--dry-run] [--profile <id>][/yellow]")
            console.print("[dim]Run [bold]radarr_lists[/bold] for available names.[/dim]"); return
        name       = tokens[0]
        dry_run    = "--dry-run" in tokens
        # Optional pre-selected movie number: radarr_download afi_top_100 5
        preselect  = next((t for t in tokens[1:] if t.isdigit()), None)
        profile_id: int | None = None
        if "--profile" in tokens:
            idx = tokens.index("--profile")
            if idx + 1 < len(tokens):
                try: profile_id = int(tokens[idx + 1])
                except ValueError:
                    console.print("[red]--profile requires an integer ID.[/red]"); return
        if name not in load_lists():
            console.print(f"[red]Unknown list:[/red] '{name}'")
            console.print("[dim]Run [bold]radarr_lists[/bold] for available names.[/dim]"); return

        tc = self._get_tmdb_client()
        rc = self._get_radarr_client()
        if not tc or not rc: return

        with console.status("Fetching list from TMDB..."):
            movies = tc.list_movies(load_lists()[name])
        if not movies:
            console.print("[yellow]No movies returned from TMDB.[/yellow]"); return

        with console.status("Loading Radarr and Plex libraries..."):
            radarr_by_tmdb = {m.get("tmdbId"): m for m in rc.movies()}
            plex_set        = self._plex_movie_set()

        # Classify every movie in the list
        STATUS_LABEL = {
            "in_plex":        "[green]in Plex[/green]",
            "has_file":       "[green]downloaded[/green]",
            "missing":        "[yellow]missing[/yellow]",
            "not_in_radarr":  "[cyan]not in Radarr[/cyan]",
        }
        rows = []
        for m in movies:
            rm = radarr_by_tmdb.get(m["tmdb_id"])
            if self._in_plex(m["title"], m["year"], plex_set):
                status = "in_plex"
            elif rm is None:
                status = "not_in_radarr"
            elif rm.get("hasFile"):
                status = "has_file"
            else:
                status = "missing"
            rows.append({**m, "radarr": rm, "status": status})

        # Downloadable = missing or not_in_radarr
        downloadable = [r for r in rows if r["status"] in ("missing", "not_in_radarr")]

        # Assign a selection number only to downloadable entries
        dl_num: dict[int, int] = {}   # index-in-rows → selection number
        for sel_n, r in enumerate(downloadable, 1):
            dl_num[id(r)] = sel_n

        # Full list table
        t = Table(title=f"{name}  ({len(rows)} titles)", box=box.ROUNDED)
        t.add_column("#",       style="dim",       width=5,  justify="right")
        t.add_column("Title",   style="bold white", min_width=32)
        t.add_column("Year",    width=6,            justify="right")
        t.add_column("In Plex", width=9,            justify="center")
        t.add_column("Status",  width=16)
        for r in rows:
            num = dl_num.get(id(r), "")
            t.add_row(
                str(num) if num else "[dim]—[/dim]",
                r["title"], str(r["year"]),
                "[green]yes[/green]" if r["status"] == "in_plex" else "[dim]no[/dim]",
                STATUS_LABEL[r["status"]],
            )
        console.print(t)

        # Summary panel
        from collections import Counter as _C
        counts = _C(r["status"] for r in rows)
        console.print(Panel(
            f"[green]In Plex:[/green]       {counts['in_plex']}  → skipped\n"
            f"[green]Downloaded:[/green]    {counts['has_file']}  → skipped\n"
            f"[yellow]Missing:[/yellow]       {counts['missing']}  → will search\n"
            f"[cyan]Not in Radarr:[/cyan] {counts['not_in_radarr']}  → will add and search",
            title=f"[bold white]{name}[/bold white]", border_style="cyan"))

        if not downloadable:
            console.print("[green]Nothing to download.[/green]"); return

        # Selection — use preselected number if given on command line, else prompt
        # (done before dry-run exit so --dry-run shows exactly what would be downloaded)
        if preselect:
            sel_input = preselect
        elif dry_run:
            sel_input = ""   # dry-run with no number = show all
        else:
            sel_input = Prompt.ask(
                f"Enter numbers to download (e.g. [bold]1 3 5-10[/bold]), "
                f"or blank for all [bold]{len(downloadable)}[/bold] missing",
                default="",
            ).strip()

        if sel_input:
            selected_nums: set[int] = set()
            for part in sel_input.split():
                if "-" in part:
                    try:
                        a, b = part.split("-", 1)
                        selected_nums.update(range(int(a), int(b) + 1))
                    except ValueError:
                        console.print(f"[yellow]Skipping invalid range: {part}[/yellow]")
                else:
                    try: selected_nums.add(int(part))
                    except ValueError:
                        console.print(f"[yellow]Skipping invalid number: {part}[/yellow]")
            selected = [downloadable[n - 1] for n in sorted(selected_nums)
                        if 1 <= n <= len(downloadable)]
        else:
            selected = downloadable

        if not selected:
            console.print("[dim]Nothing selected.[/dim]"); return

        if dry_run:
            for r in selected:
                console.print(f"  [dim]would download:[/dim] {r['title']} ({r['year']})")
            console.print(f"[yellow]Dry run — {len(selected)} movie(s) would be downloaded. "
                          "Re-run without --dry-run to execute.[/yellow]"); return

        self._execute_radarr_download(selected, rc, profile_id)

    def _execute_radarr_download(self, selected: list[dict], rc: "RadarrClient",
                                 profile_id: "int | None" = None) -> None:
        """Trigger Radarr searches / adds for a pre-filtered list of movie dicts."""
        to_search = [(r, r["radarr"]["id"]) for r in selected if r["status"] == "missing"]
        to_add    = [r for r in selected if r["status"] == "not_in_radarr"]

        if to_search:
            rc.search_movies([rm_id for _, rm_id in to_search])
            console.print(f"[green]Search triggered[/green] for {len(to_search)} existing movies.")

        if to_add:
            profiles = rc.quality_profiles()
            folders  = rc.root_folders()
            if not profiles or not folders:
                console.print("[red]Cannot add movies — check Radarr quality profiles and root folders.[/red]")
                return
            if profile_id is None:
                if len(profiles) == 1:
                    profile_id = profiles[0]["id"]
                    console.print(f"[dim]Quality profile: {profiles[0]['name']}[/dim]")
                else:
                    pt = Table(title="Quality Profiles", box=box.ROUNDED)
                    pt.add_column("ID", style="dim", width=6); pt.add_column("Name", style="bold cyan")
                    for p in profiles: pt.add_row(str(p["id"]), p["name"])
                    console.print(pt)
                    profile_id = int(Prompt.ask("Select profile ID",
                                                choices=[str(p["id"]) for p in profiles],
                                                default=str(profiles[0]["id"])))
            if len(folders) == 1:
                root_folder = folders[0]["path"]
            else:
                ft = Table(title="Root Folders", box=box.ROUNDED)
                ft.add_column("ID", style="dim", width=6); ft.add_column("Path", style="bold white")
                for f in folders: ft.add_row(str(f["id"]), f["path"])
                console.print(ft)
                folder_map  = {str(f["id"]): f["path"] for f in folders}
                root_folder = folder_map[Prompt.ask("Select root folder ID",
                                                    choices=list(folder_map),
                                                    default=list(folder_map)[0])]
            ok = fail = 0
            for r in to_add:
                with console.status(f"Adding [cyan]{r['title']}[/cyan]..."):
                    result = rc.add_movie(r["tmdb_id"], r["title"], r["year"],
                                         profile_id, root_folder, search_on_add=True)
                if result.get("id"): ok += 1
                else:
                    fail += 1
                    console.print(f"[red]Failed:[/red] {r['title']} — {result}")
            console.print(f"[green]{ok} added and queued for search.[/green]"
                          + (f" [red]{fail} failed.[/red]" if fail else ""))

        console.print("[dim]Radarr is searching in the background.[/dim]")

    def do_radarr_pick(self, arg: str):
        """radarr_pick <name> [--profile <id>] — interactively select movies to download"""
        try:
            import questionary
        except ImportError:
            console.print("[red]questionary not installed.[/red]  "
                          "Run: [bold]pip install questionary[/bold]"); return

        try:
            tokens = shlex.split(arg.strip()) if arg.strip() else []
        except ValueError:
            tokens = arg.strip().split()
        if not tokens:
            console.print("[yellow]Usage: radarr_pick <name> [--profile <id>][/yellow]")
            console.print("[dim]Run [bold]radarr_lists[/bold] for available names.[/dim]"); return

        name       = tokens[0]
        profile_id: int | None = None
        if "--profile" in tokens:
            idx = tokens.index("--profile")
            if idx + 1 < len(tokens):
                try: profile_id = int(tokens[idx + 1])
                except ValueError:
                    console.print("[red]--profile requires an integer ID.[/red]"); return

        lists = load_lists()
        if name not in lists:
            console.print(f"[red]Unknown list:[/red] '{name}'")
            console.print("[dim]Run [bold]radarr_lists[/bold] for available names.[/dim]"); return

        tc = self._get_tmdb_client()
        rc = self._get_radarr_client()
        if not tc or not rc: return

        with console.status("Fetching list from TMDB..."):
            movies = tc.list_movies(lists[name])
        if not movies:
            console.print("[yellow]No movies returned from TMDB.[/yellow]"); return

        with console.status("Loading Radarr and Plex libraries..."):
            radarr_by_tmdb = {m.get("tmdbId"): m for m in rc.movies()}
            plex_set        = self._plex_movie_set()

        rows = []
        for m in movies:
            rm = radarr_by_tmdb.get(m["tmdb_id"])
            if self._in_plex(m["title"], m["year"], plex_set):
                status = "in_plex"
            elif rm is None:
                status = "not_in_radarr"
            elif rm.get("hasFile"):
                status = "has_file"
            else:
                status = "missing"
            rows.append({**m, "radarr": rm, "status": status})

        downloadable = [r for r in rows if r["status"] in ("missing", "not_in_radarr")]
        if not downloadable:
            console.print("[green]Nothing to download — all movies are in Plex or already downloaded.[/green]")
            return

        STATUS_TAG = {"missing": "missing", "not_in_radarr": "not in Radarr"}
        choices = [
            questionary.Choice(
                title=f"{r['title']} ({r['year']})  [{STATUS_TAG[r['status']]}]",
                value=r,
            )
            for r in downloadable
        ]

        console.print(f"\n[bold]{name}[/bold]  —  {len(downloadable)} downloadable movies")
        console.print("[dim]↑↓ scroll · Space toggle · a select all · enter confirm · ctrl-c cancel[/dim]\n")

        selected = questionary.checkbox(
            f"Select movies to download:",
            choices=choices,
            instruction=" ",
        ).ask()

        if not selected:
            console.print("[dim]Nothing selected.[/dim]"); return

        self._execute_radarr_download(selected, rc, profile_id)

    def do_health(self, _arg: str):
        """health — one-page summary: library stats, zero-duration, stale shows, Radarr sync/upgrade gaps"""
        import concurrent.futures

        cfg          = load_config()
        radarr_url   = cfg.get("radarr_url")    or os.environ.get("RADARR_URL",    "")
        radarr_key   = cfg.get("radarr_api_key") or os.environ.get("RADARR_API_KEY", "")
        sonarr_url   = cfg.get("sonarr_url")    or os.environ.get("SONARR_URL",    "")
        sonarr_key   = cfg.get("sonarr_api_key") or os.environ.get("SONARR_API_KEY", "")
        radarr_configured = bool(radarr_url and radarr_key)
        sonarr_configured = bool(sonarr_url and sonarr_key)

        with console.status("Running health checks..."):
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
                f_items = pool.submit(self.client.all_items_by_library)
                f_media = pool.submit(self.client.all_media_rows)
                if radarr_configured:
                    rc           = RadarrClient(radarr_url, radarr_key)
                    f_r_status   = pool.submit(rc.status)
                    f_r_movies   = pool.submit(rc.movies)
                    f_r_profiles = pool.submit(rc.quality_profiles)
                if sonarr_configured:
                    sc           = SonarrClient(sonarr_url, sonarr_key)
                    f_s_status   = pool.submit(sc.status)
                    f_s_missing  = pool.submit(sc.wanted_missing_count)
                    f_s_series   = pool.submit(sc.series)

                lib_data   = f_items.result()
                media_rows = f_media.result()
                if radarr_configured:
                    radarr_ok       = bool(f_r_status.result())
                    radarr_movies   = f_r_movies.result()   if radarr_ok else []
                    radarr_profiles = f_r_profiles.result() if radarr_ok else []
                else:
                    radarr_ok = False
                    radarr_movies = radarr_profiles = []
                if sonarr_configured:
                    sonarr_ok      = bool(f_s_status.result())
                    sonarr_missing = f_s_missing.result() if sonarr_ok else 0
                    sonarr_series  = f_s_series.result()  if sonarr_ok else []
                else:
                    sonarr_ok = False
                    sonarr_missing = 0
                    sonarr_series  = []

        # ── Plex aggregates ───────────────────────────────────────────────────
        lib_size: dict[str, int] = defaultdict(int)
        lib_dur:  dict[str, int] = defaultdict(int)
        for row in media_rows:
            lib_size[row["library"]] += row.get("size") or 0
            lib_dur[row["library"]]  += row.get("duration") or 0

        STALE_MONTHS  = 6
        stale_cutoff  = int(time.time()) - STALE_MONTHS * 30 * 86400
        stale_shows   = 0
        total_size    = 0
        overview_rows = []

        plex_set: set[tuple[str, int]] = set()
        plex_movie_items: list[dict]   = []

        for lib_name, d in lib_data.items():
            lib_type = d["info"].get("type", "")
            items    = d["items"]
            size     = lib_size[lib_name]
            total_size += size
            watched = sum(1 for i in items if (i.get("viewCount") or 0) > 0)

            if lib_type == "show":
                stale_shows += sum(1 for i in items if (i.get("updatedAt") or 0) < stale_cutoff)
            elif lib_type == "movie":
                for item in items:
                    t = (item.get("title") or "").lower().strip()
                    y = item.get("year") or 0
                    if t:
                        plex_set.add((t, y))
                plex_movie_items.extend(items)

            watch_str = (f"{watched}/{len(items)} ({100 * watched // len(items)}%)"
                         if lib_type == "movie" and items else "—")
            overview_rows.append((lib_name, lib_type, len(items), size, watch_str))

        zero_dur = sum(1 for r in media_rows if not r.get("duration"))

        # ── Radarr aggregates ─────────────────────────────────────────────────
        radarr_not_plex = plex_not_radarr = upgrade_candidates = 0
        if radarr_ok and radarr_movies:
            # Downloads not indexed in Plex
            radarr_not_plex = sum(
                1 for m in radarr_movies
                if m.get("hasFile")
                and not self._in_plex(m.get("title", ""), m.get("year", 0), plex_set)
            )

            # Plex movies not monitored in Radarr
            radarr_by_year: dict[int, set[str]] = defaultdict(set)
            for m in radarr_movies:
                radarr_by_year[m.get("year", 0)].add(m.get("title", "").lower().strip())

            def _in_radarr(title: str, year: int) -> bool:
                t = title.lower().strip()
                for y in (year - 1, year, year + 1):
                    for rt in radarr_by_year.get(y, set()):
                        if t == rt or SequenceMatcher(None, t, rt).ratio() >= 0.90:
                            return True
                return False

            plex_not_radarr = sum(
                1 for pm in plex_movie_items
                if not _in_radarr(pm.get("title", ""), pm.get("year", 0))
            )

            # Quality upgrade candidates
            qual_res: dict[int, int] = {}
            def _extract_q(items: list) -> None:
                for item in items:
                    if item.get("quality"):
                        q = item["quality"]
                        qual_res[q["id"]] = q.get("resolution", 0)
                    _extract_q(item.get("items", []))
            for p in radarr_profiles:
                _extract_q(p.get("items", []))

            prof_cutoff = {p["id"]: qual_res.get(p.get("cutoff", 0), 0) for p in radarr_profiles}
            upgrade_candidates = sum(
                1 for m in radarr_movies
                if m.get("hasFile")
                and prof_cutoff.get(m.get("qualityProfileId", 0), 0) > 0
                and ((m.get("movieFile") or {}).get("quality") or {}).get("quality", {})
                    .get("resolution", 0)
                    < prof_cutoff.get(m.get("qualityProfileId", 0), 0)
            )

        # ── Display ───────────────────────────────────────────────────────────
        ov = Table(box=box.SIMPLE_HEAVY, padding=(0, 1), show_header=True)
        ov.add_column("Library",     style="bold cyan")
        ov.add_column("Type",        style="dim",   width=8)
        ov.add_column("Items",       justify="right", width=7)
        ov.add_column("Size",        justify="right", width=10)
        ov.add_column("Watched",     justify="right", width=14)
        for lib_name, lib_type, n, size, watch_str in overview_rows:
            ov.add_row(lib_name, lib_type, str(n), format_size(size), watch_str)
        ov.add_section()
        ov.add_row("[bold]TOTAL[/bold]", "", "", format_size(total_size), "")
        console.print(Panel(ov, title="[bold white]Libraries[/bold white]", border_style="blue"))

        def chk(ok: bool, label: str, ok_text: str, warn_text: str, hint: str = "") -> str:
            if ok:
                return f"  [green]✓[/green]  {label:<42} [dim]{ok_text}[/dim]"
            detail = f"[yellow]{warn_text}[/yellow]"
            detail += f"  [dim]{hint}[/dim]" if hint else ""
            return f"  [yellow]⚠[/yellow]  {label:<42} {detail}"

        lines = [
            chk(zero_dur == 0,    "Zero-duration files",
                "none", f"{zero_dur} item(s)",   "→ zero_duration for details"),
            chk(stale_shows == 0, f"Stale TV shows (6+ months without update)",
                "none", f"{stale_shows} show(s)", "→ stale for details"),
        ]

        if not radarr_configured:
            lines.append("\n  [dim]Radarr not configured — run radarr_status to set up.[/dim]")
        elif not radarr_ok:
            lines.append("\n  [red]✗[/red]  [dim]Radarr: could not connect[/dim]")
        else:
            lines += [
                "",
                chk(radarr_not_plex == 0,    "Radarr downloads not indexed in Plex",
                    "all indexed",   f"{radarr_not_plex} movie(s)", "→ radarr_sync"),
                chk(plex_not_radarr == 0,    "Plex movies not monitored by Radarr",
                    "all monitored", f"{plex_not_radarr} movie(s)", "→ radarr_sync"),
                chk(upgrade_candidates == 0, "Movies below quality cutoff",
                    "none",          f"{upgrade_candidates} movie(s)", "→ radarr_upgrade"),
            ]

        # ── Sonarr section ────────────────────────────────────────────────────
        if not sonarr_configured:
            lines.append("\n  [dim]Sonarr not configured — run sonarr_status to set up.[/dim]")
        elif not sonarr_ok:
            lines.append("\n  [red]✗[/red]  [dim]Sonarr: could not connect[/dim]")
        else:
            # Sync: Sonarr has downloaded files but Plex doesn't have the show
            plex_show_set = self._plex_show_set()
            sonarr_not_plex = sum(
                1 for s in sonarr_series
                if (s.get("statistics") or {}).get("episodeFileCount", 0) > 0
                and not self._in_plex_show(s.get("title", ""), plex_show_set)
            )
            lines += [
                "",
                chk(sonarr_missing == 0,    "Sonarr monitored episodes missing",
                    "none",         f"{sonarr_missing} episode(s)", "→ sonarr_missing"),
                chk(sonarr_not_plex == 0,   "Sonarr downloads not indexed in Plex",
                    "all indexed",  f"{sonarr_not_plex} show(s)",   "→ sonarr_sync"),
            ]

        console.print(Panel(
            "\n".join(lines),
            title="[bold white]Health Checks[/bold white]",
            border_style="cyan",
        ))

    # ── Sonarr commands ───────────────────────────────────────────────────────

    def do_sonarr_status(self, _):
        """sonarr_status — Sonarr connection info, quality profiles, and root folders"""
        sc = self._get_sonarr_client()
        if not sc: return
        with console.status("Fetching Sonarr info..."):
            info     = sc.status()
            profiles = sc.quality_profiles()
            folders  = sc.root_folders()
        console.print(Panel(
            f"[bold cyan]Version:[/bold cyan]  {info.get('version','—')}\n"
            f"[bold cyan]URL:[/bold cyan]      {sc.base_url}\n"
            f"[bold cyan]OS:[/bold cyan]       {info.get('osName','—')} {info.get('osVersion','—')}",
            title="[bold white]Sonarr[/bold white]", border_style="green"))
        pt = Table(title="Quality Profiles", box=box.ROUNDED)
        pt.add_column("ID", style="dim", width=6); pt.add_column("Name", style="bold cyan")
        for p in profiles: pt.add_row(str(p.get("id", "")), p.get("name", ""))
        console.print(pt)
        ft = Table(title="Root Folders", box=box.ROUNDED)
        ft.add_column("Path", style="bold white", min_width=28)
        ft.add_column("Free Space", justify="right", width=12)
        for f in folders: ft.add_row(f.get("path", ""), format_size(f.get("freeSpace")))
        console.print(ft)

    def do_sonarr_sync(self, _arg: str):
        """sonarr_sync — find Sonarr downloads missing from Plex, and Plex shows not in Sonarr"""
        sc = self._get_sonarr_client()
        if not sc: return

        with console.status("Loading Sonarr and Plex libraries..."):
            all_series   = sc.series()
            plex_show_set = self._plex_show_set()

            plex_shows: list[dict] = []
            for lib in self.client.libraries():
                if lib.get("type") != "show": continue
                for item in self.client.library_contents(lib.get("key", "")):
                    plex_shows.append({
                        "title":   item.get("title", ""),
                        "year":    item.get("year", 0),
                        "library": lib.get("title", ""),
                    })

        # Section 1: Sonarr has downloaded files but Plex hasn't indexed the show
        sonarr_not_plex = sorted(
            [s for s in all_series
             if (s.get("statistics") or {}).get("episodeFileCount", 0) > 0
             and not self._in_plex_show(s.get("title", ""), plex_show_set)],
            key=lambda x: x.get("title", ""),
        )

        # Section 2: Plex shows not monitored by Sonarr
        sonarr_titles: set[str] = {s.get("title", "").lower().strip() for s in all_series}

        def _in_sonarr(title: str) -> bool:
            t = title.lower().strip()
            if t in sonarr_titles: return True
            for st in sonarr_titles:
                if SequenceMatcher(None, t, st).ratio() >= 0.88:
                    return True
            return False

        plex_not_sonarr = sorted(
            [ps for ps in plex_shows if not _in_sonarr(ps["title"])],
            key=lambda x: x.get("title", ""),
        )

        console.print()
        if sonarr_not_plex:
            t1 = Table(
                title=f"[yellow]Downloaded in Sonarr — not indexed in Plex[/yellow]  ({len(sonarr_not_plex)})",
                box=box.ROUNDED,
            )
            t1.add_column("Title",    style="bold white", min_width=30)
            t1.add_column("Year",     width=6, justify="right")
            t1.add_column("Episodes", width=10, justify="right")
            t1.add_column("Status",   width=12, style="dim")
            for s in sonarr_not_plex:
                stats = s.get("statistics") or {}
                t1.add_row(s.get("title", ""), str(s.get("year", "")),
                           f"{stats.get('episodeFileCount',0)}/{stats.get('totalEpisodeCount',0)}",
                           s.get("status", ""))
            console.print(t1)
            console.print("[dim]Tip: trigger a Plex library scan — the files may just need indexing.[/dim]\n")
        else:
            console.print("[green]✓ All Sonarr downloads are indexed in Plex.[/green]\n")

        if plex_not_sonarr:
            t2 = Table(
                title=f"[yellow]In Plex — not monitored by Sonarr[/yellow]  ({len(plex_not_sonarr)})",
                box=box.ROUNDED,
            )
            t2.add_column("Title",   style="bold white", min_width=30)
            t2.add_column("Year",    width=6, justify="right")
            t2.add_column("Library", width=18, style="dim")
            for ps in plex_not_sonarr:
                t2.add_row(ps["title"], str(ps["year"]), ps["library"])
            console.print(t2)
            console.print("[dim]Tip: use sonarr_add to start monitoring these shows.[/dim]")
        else:
            console.print("[green]✓ All Plex shows are monitored by Sonarr.[/green]")

    def do_sonarr_missing(self, _arg: str):
        """sonarr_missing — shows with monitored but missing episodes; select to trigger search"""
        sc = self._get_sonarr_client()
        if not sc: return

        with console.status("Fetching missing episodes from Sonarr..."):
            records = sc.wanted_missing()
        if not records:
            console.print("[green]No monitored missing episodes.[/green]"); return

        # Group by series
        by_series: dict[int, dict] = {}
        for ep in records:
            s   = ep.get("series") or {}
            sid = ep.get("seriesId", 0)
            if sid not in by_series:
                by_series[sid] = {
                    "id":     sid,
                    "title":  s.get("title", ""),
                    "year":   s.get("year", 0),
                    "status": s.get("status", ""),
                    "count":  0,
                }
            by_series[sid]["count"] += 1

        shows = sorted(by_series.values(), key=lambda x: (-x["count"], x["title"]))

        t = Table(title=f"Shows with Missing Episodes  ({len(shows)} shows, {len(records)} episodes)",
                  box=box.ROUNDED)
        t.add_column("#",       style="dim",        width=4,  justify="right")
        t.add_column("Title",   style="bold white",  min_width=30)
        t.add_column("Year",    width=6,             justify="right")
        t.add_column("Missing", width=9,             justify="right", style="yellow")
        t.add_column("Status",  width=12,            style="dim")
        for i, s in enumerate(shows, 1):
            t.add_row(str(i), s["title"], str(s["year"]), str(s["count"]), s["status"])
        console.print(t)

        try:
            import questionary
            console.print(f"\n[dim]↑↓ scroll · Space toggle · a select all · Enter confirm · Ctrl-C cancel[/dim]\n")
            choices = [
                questionary.Choice(
                    title=f"{s['title']} ({s['year']})  [{s['count']} missing]",
                    value=s,
                )
                for s in shows
            ]
            selected = questionary.checkbox(
                "Select shows to search for missing episodes:", choices=choices, instruction=" ",
            ).ask()
        except ImportError:
            sel_input = Prompt.ask(
                f"Enter numbers to search (e.g. [bold]1 3 5-10[/bold]), or blank for all {len(shows)}",
                default="",
            ).strip()
            if sel_input:
                nums: set[int] = set()
                for part in sel_input.split():
                    if "-" in part:
                        try:
                            a, b = part.split("-", 1)
                            nums.update(range(int(a), int(b) + 1))
                        except ValueError: pass
                    else:
                        try: nums.add(int(part))
                        except ValueError: pass
                selected = [shows[n - 1] for n in sorted(nums) if 1 <= n <= len(shows)]
            else:
                selected = shows

        if not selected:
            console.print("[dim]Nothing selected.[/dim]"); return
        for s in selected:
            sc.search_series(s["id"])
        console.print(f"[green]Search triggered[/green] for {len(selected)} show(s).")
        console.print("[dim]Sonarr is searching for missing episodes in the background.[/dim]")

    def do_sonarr_upgrade(self, _arg: str):
        """sonarr_upgrade — shows with episodes below quality cutoff; select to trigger re-search"""
        sc = self._get_sonarr_client()
        if not sc: return

        with console.status("Fetching below-cutoff episodes from Sonarr..."):
            records = sc.wanted_cutoff()
        if not records:
            console.print("[green]All downloaded episodes meet their quality profile cutoff.[/green]"); return

        by_series: dict[int, dict] = {}
        for ep in records:
            s   = ep.get("series") or {}
            sid = ep.get("seriesId", 0)
            if sid not in by_series:
                by_series[sid] = {
                    "id":     sid,
                    "title":  s.get("title", ""),
                    "year":   s.get("year", 0),
                    "status": s.get("status", ""),
                    "count":  0,
                }
            by_series[sid]["count"] += 1

        shows = sorted(by_series.values(), key=lambda x: (-x["count"], x["title"]))

        t = Table(title=f"Shows with Episodes Below Quality Cutoff  ({len(shows)} shows, {len(records)} episodes)",
                  box=box.ROUNDED)
        t.add_column("#",       style="dim",        width=4,  justify="right")
        t.add_column("Title",   style="bold white",  min_width=30)
        t.add_column("Year",    width=6,             justify="right")
        t.add_column("Below cutoff", width=13,      justify="right", style="yellow")
        t.add_column("Status",  width=12,            style="dim")
        for i, s in enumerate(shows, 1):
            t.add_row(str(i), s["title"], str(s["year"]), str(s["count"]), s["status"])
        console.print(t)

        try:
            import questionary
            console.print(f"\n[dim]↑↓ scroll · Space toggle · a select all · Enter confirm · Ctrl-C cancel[/dim]\n")
            choices = [
                questionary.Choice(
                    title=f"{s['title']} ({s['year']})  [{s['count']} episode(s) below cutoff]",
                    value=s,
                )
                for s in shows
            ]
            selected = questionary.checkbox(
                "Select shows to trigger upgrade search:", choices=choices, instruction=" ",
            ).ask()
        except ImportError:
            sel_input = Prompt.ask(
                f"Enter numbers to search (e.g. [bold]1 3 5-10[/bold]), or blank for all {len(shows)}",
                default="",
            ).strip()
            if sel_input:
                nums: set[int] = set()
                for part in sel_input.split():
                    if "-" in part:
                        try:
                            a, b = part.split("-", 1)
                            nums.update(range(int(a), int(b) + 1))
                        except ValueError: pass
                    else:
                        try: nums.add(int(part))
                        except ValueError: pass
                selected = [shows[n - 1] for n in sorted(nums) if 1 <= n <= len(shows)]
            else:
                selected = shows

        if not selected:
            console.print("[dim]Nothing selected.[/dim]"); return
        for s in selected:
            sc.search_series(s["id"])
        console.print(f"[green]Upgrade search triggered[/green] for {len(selected)} show(s).")
        console.print("[dim]Sonarr will replace episodes when better versions are found.[/dim]")

    def do_sonarr_add(self, arg: str):
        """sonarr_add <show name> — search for a TV show and add it to Sonarr"""
        name = arg.strip()
        if not name:
            console.print("[yellow]Usage: sonarr_add <show name>[/yellow]"); return

        sc = self._get_sonarr_client()
        if not sc: return

        with console.status(f"Searching Sonarr for [cyan]{name}[/cyan]..."):
            results  = sc.series_lookup(name)
            existing = {s.get("tvdbId") for s in sc.series() if s.get("tvdbId")}

        if not results:
            console.print(f"[yellow]No results found for:[/yellow] {name}"); return

        # Split into already-added and new
        new_shows = [r for r in results if r.get("tvdbId") not in existing]
        if not new_shows:
            console.print(f"[green]All matches are already in Sonarr.[/green]"); return

        t = Table(title=f"Search results for '{name}'  ({len(new_shows)} new)", box=box.ROUNDED)
        t.add_column("#",        style="dim",        width=4,  justify="right")
        t.add_column("Title",    style="bold white",  min_width=28)
        t.add_column("Year",     width=6,             justify="right")
        t.add_column("Network",  width=16,            style="dim")
        t.add_column("Seasons",  width=9,             justify="right")
        t.add_column("Status",   width=12,            style="dim")
        for i, r in enumerate(new_shows, 1):
            t.add_row(str(i), r.get("title", ""), str(r.get("year", "")),
                      r.get("network", "—"),
                      str(len(r.get("seasons", []))),
                      r.get("status", ""))
        console.print(t)

        if len(new_shows) == 1:
            show = new_shows[0]
            console.print(f"[dim]Auto-selected: {show.get('title')} ({show.get('year')})[/dim]")
        else:
            choice = Prompt.ask("Select #", choices=[str(i) for i in range(1, len(new_shows) + 1)])
            show = new_shows[int(choice) - 1]

        with console.status("Loading Sonarr profiles..."):
            profiles = sc.quality_profiles()
            folders  = sc.root_folders()
        if not profiles or not folders:
            console.print("[red]Cannot add show — check Sonarr quality profiles and root folders.[/red]"); return

        if len(profiles) == 1:
            profile_id = profiles[0]["id"]
            console.print(f"[dim]Quality profile: {profiles[0]['name']}[/dim]")
        else:
            pt = Table(title="Quality Profiles", box=box.ROUNDED)
            pt.add_column("ID", style="dim", width=6); pt.add_column("Name", style="bold cyan")
            for p in profiles: pt.add_row(str(p["id"]), p["name"])
            console.print(pt)
            profile_id = int(Prompt.ask("Select profile ID",
                                        choices=[str(p["id"]) for p in profiles],
                                        default=str(profiles[0]["id"])))

        if len(folders) == 1:
            root_folder = folders[0]["path"]
        else:
            ft = Table(title="Root Folders", box=box.ROUNDED)
            ft.add_column("ID", style="dim", width=6); ft.add_column("Path", style="bold white")
            for f in folders: ft.add_row(str(f["id"]), f["path"])
            console.print(ft)
            folder_map  = {str(f["id"]): f["path"] for f in folders}
            root_folder = folder_map[Prompt.ask("Select root folder ID",
                                                choices=list(folder_map),
                                                default=list(folder_map)[0])]

        search_on_add = Prompt.ask(
            "Search for missing episodes immediately?", choices=["y", "n"], default="y"
        ) == "y"

        with console.status(f"Adding [cyan]{show.get('title')}[/cyan] to Sonarr..."):
            result = sc.add_series(
                tvdb_id=show.get("tvdbId", 0),
                title=show.get("title", ""),
                year=show.get("year", 0),
                quality_profile_id=profile_id,
                root_folder_path=root_folder,
                seasons=show.get("seasons", []),
                search_on_add=search_on_add,
            )

        if result.get("id"):
            console.print(f"[green]Added:[/green] {show.get('title')} ({show.get('year')})")
            if search_on_add:
                console.print("[dim]Sonarr is searching for episodes in the background.[/dim]")
        else:
            console.print(f"[red]Failed to add show.[/red] Response: {result}")

    # ── Tautulli commands ─────────────────────────────────────────────────────

    def do_tautulli_status(self, _):
        """tautulli_status — Tautulli connection info"""
        tc = self._get_tautulli_client()
        if not tc: return
        with console.status("Fetching Tautulli info..."):
            info = tc.server_info()
        if not info:
            console.print("[yellow]No server info returned.[/yellow]"); return
        console.print(Panel(
            f"[bold cyan]Plex Server:[/bold cyan]   {info.get('pms_name', '—')}\n"
            f"[bold cyan]Plex Version:[/bold cyan]  {info.get('pms_version', '—')}\n"
            f"[bold cyan]Tautulli URL:[/bold cyan]  {tc.base_url}",
            title="[bold white]Tautulli[/bold white]", border_style="green"))

    def do_tautulli_history(self, arg: str):
        """tautulli_history [--user <name>] [--count N] — rich play history with stream type and duration"""
        tokens = arg.strip().split() if arg.strip() else []
        count = 25
        if "--count" in tokens:
            idx = tokens.index("--count")
            if idx + 1 < len(tokens):
                try: count = int(tokens[idx + 1])
                except ValueError: pass
        user = ""
        if "--user" in tokens:
            idx = tokens.index("--user")
            if idx + 1 < len(tokens):
                user = tokens[idx + 1]

        tc = self._get_tautulli_client()
        if not tc: return
        with console.status("Fetching play history..."):
            records = tc.history(length=count, user=user)

        if not records:
            console.print("[yellow]No history found.[/yellow]"); return

        _STREAM_COLOR = {"direct play": "green", "copy": "yellow", "transcode": "red"}

        heading = f"Play History — last {count}" + (f"  (user: {user})" if user else "")
        t = Table(title=heading, box=box.ROUNDED)
        t.add_column("Date",     width=11, style="dim")
        t.add_column("User",     width=14, style="cyan")
        t.add_column("Title",    style="bold white", min_width=28)
        t.add_column("Type",     width=8,  style="dim")
        t.add_column("Watched",  width=9,  justify="right")
        t.add_column("Duration", width=9,  justify="right", style="dim")
        t.add_column("Platform", width=12, style="dim")
        t.add_column("Stream",   width=13)

        for r in records:
            dec  = (r.get("transcode_decision") or "direct play").lower()
            clr  = _STREAM_COLOR.get(dec, "dim")
            t.add_row(
                (format_ts(r.get("date")) or "")[:10],
                r.get("friendly_name") or r.get("user") or "—",
                r.get("full_title") or r.get("title") or "—",
                r.get("media_type", ""),
                f"{r.get('percent_complete', 0)}%" if r.get("percent_complete") else "—",
                format_duration((r.get("duration") or 0) * 1000),
                r.get("platform") or "—",
                f"[{clr}]{dec}[/{clr}]",
            )
        console.print(t)

    def do_tautulli_stats(self, arg: str):
        """tautulli_stats [--days N] — top movies, shows, and users over the last N days (default 30)"""
        tokens = arg.strip().split() if arg.strip() else []
        days = 30
        if "--days" in tokens:
            idx = tokens.index("--days")
            if idx + 1 < len(tokens):
                try: days = int(tokens[idx + 1])
                except ValueError: pass

        tc = self._get_tautulli_client()
        if not tc: return
        with console.status(f"Fetching stats (last {days} days)..."):
            stats = tc.home_stats(time_range=days, count=10)

        if not stats:
            console.print("[yellow]No stats returned.[/yellow]"); return

        by_id = {s.get("stat_id"): s.get("rows", []) for s in stats}

        def _media_table(heading: str, rows: list, label: str) -> None:
            if not rows: return
            t = Table(title=heading, box=box.ROUNDED)
            t.add_column("#",          width=3,  justify="right", style="dim")
            t.add_column(label,        style="bold cyan", min_width=28)
            t.add_column("Plays",      width=7,  justify="right")
            t.add_column("Users",      width=7,  justify="right", style="dim")
            t.add_column("Watch Time", width=11, justify="right", style="dim")
            for i, row in enumerate(rows, 1):
                t.add_row(str(i), row.get("title", "?"),
                          str(row.get("total_plays", "")),
                          str(row.get("users_watched", "")),
                          format_duration((row.get("total_duration") or 0) * 1000))
            console.print(t)

        _media_table(f"Top Movies — last {days} days",   by_id.get("top_movies", []), "Movie")
        _media_table(f"Top TV Shows — last {days} days", by_id.get("top_tv", []),     "Show")

        user_rows = by_id.get("top_users", [])
        if user_rows:
            t = Table(title=f"Top Users — last {days} days", box=box.ROUNDED)
            t.add_column("#",          width=3,  justify="right", style="dim")
            t.add_column("User",       style="bold cyan", min_width=18)
            t.add_column("Plays",      width=7,  justify="right")
            t.add_column("Watch Time", width=11, justify="right", style="dim")
            t.add_column("Last Seen",  width=11, style="dim")
            for i, row in enumerate(user_rows, 1):
                last = (format_ts(row.get("last_play")) or "")[:10]
                t.add_row(str(i),
                          row.get("friendly_name") or row.get("user", "?"),
                          str(row.get("total_plays", "")),
                          format_duration((row.get("total_duration") or 0) * 1000),
                          last)
            console.print(t)

    def do_tautulli_plays(self, arg: str):
        """tautulli_plays [--days N] — plays per day as a bar chart (default 30 days)"""
        tokens = arg.strip().split() if arg.strip() else []
        days = 30
        if "--days" in tokens:
            idx = tokens.index("--days")
            if idx + 1 < len(tokens):
                try: days = int(tokens[idx + 1])
                except ValueError: pass

        tc = self._get_tautulli_client()
        if not tc: return
        with console.status(f"Fetching play data (last {days} days)..."):
            data = tc.plays_by_date(time_range=days)

        categories = data.get("categories", [])
        series     = data.get("series", [])
        if not categories or not series:
            console.print("[yellow]No play data returned.[/yellow]"); return

        totals    = [sum(s["data"][i] for s in series if i < len(s.get("data", [])))
                     for i in range(len(categories))]
        max_plays = max(totals) if any(totals) else 1
        BAR_WIDTH = 28

        t = Table(title=f"Plays per Day — last {days} days", box=box.SIMPLE_HEAD)
        t.add_column("Date", width=12, style="dim")
        for s in series:
            t.add_column(s.get("name", ""), width=7, justify="right", style="dim")
        t.add_column("Total", width=6, justify="right")
        t.add_column("", min_width=BAR_WIDTH)

        for i, cat in enumerate(categories):
            day_vals = [s["data"][i] if i < len(s.get("data", [])) else 0 for s in series]
            total    = totals[i]
            bar      = "█" * int(total / max_plays * BAR_WIDTH) if total else ""
            t.add_row(cat, *[str(v) for v in day_vals], str(total),
                      f"[cyan]{bar}[/cyan]" if bar else "")
        console.print(t)

    def do_tautulli_users(self, _):
        """tautulli_users — all users ranked by total plays and watch time"""
        tc = self._get_tautulli_client()
        if not tc: return
        with console.status("Fetching users..."):
            users = tc.users()

        if not users:
            console.print("[yellow]No users found.[/yellow]"); return

        users = [u for u in users if u.get("username") not in ("", "Local")]
        users.sort(key=lambda u: u.get("total_plays") or 0, reverse=True)

        t = Table(title=f"Users ({len(users)})", box=box.ROUNDED)
        t.add_column("User",        style="bold cyan", min_width=18)
        t.add_column("Plays",       width=7,  justify="right")
        t.add_column("Watch Time",  width=11, justify="right")
        t.add_column("Last Seen",   width=11, style="dim")
        t.add_column("Last Played", style="dim", min_width=25)

        for u in users:
            t.add_row(
                u.get("friendly_name") or u.get("username") or "?",
                str(u.get("total_plays") or 0),
                format_duration((u.get("total_duration") or 0) * 1000),
                (format_ts(u.get("last_seen")) or "")[:10],
                u.get("last_played") or "—",
            )
        console.print(t)

    def do_smart_recommendations(self, arg: str):
        """smart_recommendations [--count N] [--min-rating R] — personalized picks from Tautulli watch history × TMDB"""
        tokens = arg.strip().split() if arg.strip() else []
        count = 20
        if "--count" in tokens:
            idx = tokens.index("--count")
            if idx + 1 < len(tokens):
                try: count = int(tokens[idx + 1])
                except ValueError: pass
        min_rating = 6.5
        if "--min-rating" in tokens:
            idx = tokens.index("--min-rating")
            if idx + 1 < len(tokens):
                try: min_rating = float(tokens[idx + 1])
                except ValueError: pass

        tc   = self._get_tautulli_client()
        if not tc: return
        tmdb = self._get_tmdb_client()
        if not tmdb: return

        # ── Step 1: play history (movies only) ──────────────────────────────
        with console.status("Fetching play history from Tautulli..."):
            records = tc.history(length=500)

        movie_plays: Counter = Counter()
        for r in records:
            if r.get("media_type") == "movie" and r.get("rating_key"):
                movie_plays[str(r["rating_key"])] += 1

        if not movie_plays:
            console.print("[yellow]No movie play history found in Tautulli.[/yellow]"); return

        # ── Step 2: build genre profile from Plex metadata ──────────────────
        with console.status("Building genre profile from Plex library..."):
            genre_scores: Counter = Counter()
            for lib in self.client.libraries():
                if lib.get("type") != "movie": continue
                for item in self.client.library_contents(lib.get("key", "")):
                    rk = str(item.get("ratingKey", ""))
                    if rk not in movie_plays: continue
                    plays = movie_plays[rk]
                    for g in (item.get("Genre") or []):
                        tag = g.get("tag", "")
                        if tag:
                            genre_scores[tag] += plays

        if not genre_scores:
            console.print("[yellow]Could not match play history to Plex library items.[/yellow]")
            console.print("[dim]Ensure Tautulli is connected to the same Plex server.[/dim]"); return

        top_genres = [g for g, _ in genre_scores.most_common(5)]
        console.print(f"[dim]Genre profile:[/dim] {', '.join(top_genres)}")

        # ── Step 3: map genre names → TMDB IDs ──────────────────────────────
        with console.status("Fetching TMDB genre map..."):
            genre_map = tmdb.genre_ids()

        tmdb_genre_ids = [genre_map[g] for g in top_genres if g in genre_map]
        if not tmdb_genre_ids:
            console.print("[yellow]Could not map genres to TMDB IDs.[/yellow]"); return

        # ── Step 4: discover via TMDB — query each top genre separately ──────
        # Querying genres individually (not combined) gives a much wider candidate
        # pool for large libraries that already own the most popular cross-genre films.
        id_to_name = {v: k for k, v in genre_map.items()}
        seen_ids: set[int] = set()
        candidates: list[dict] = []
        for gid in tmdb_genre_ids[:3]:
            with console.status(f"Fetching TMDB {id_to_name.get(gid, gid)} films..."):
                for m in tmdb.discover_movies(str(gid), min_rating=min_rating):
                    if m["tmdb_id"] not in seen_ids:
                        seen_ids.add(m["tmdb_id"])
                        candidates.append(m)

        # ── Step 5: filter against Plex (and Radarr if configured) ──────────
        with console.status("Cross-referencing your library..."):
            plex_set = self._plex_movie_set()

        cfg = load_config()
        radarr_tmdb_ids: set = set()
        r_url = cfg.get("radarr_url") or os.environ.get("RADARR_URL", "")
        r_key = cfg.get("radarr_api_key") or os.environ.get("RADARR_API_KEY", "")
        if r_url and r_key:
            with console.status("Cross-referencing Radarr..."):
                radarr_tmdb_ids = {m.get("tmdbId") for m in RadarrClient(r_url, r_key).movies()}

        results: list[dict] = []
        for m in sorted(candidates, key=lambda x: -x["rating"]):
            if self._in_plex(m["title"], m["year"], plex_set): continue
            if m["tmdb_id"] in radarr_tmdb_ids: continue
            results.append(m)
            if len(results) >= count: break

        if not results:
            console.print("[green]No new recommendations — your library already has everything in these genres![/green]"); return

        t = Table(title=f"Recommended for You  ({len(results)} films)", box=box.ROUNDED)
        t.add_column("#",        width=3,  justify="right", style="dim")
        t.add_column("Title",    style="bold cyan", min_width=28)
        t.add_column("Year",     width=6,  justify="right", style="dim")
        t.add_column("Rating",   width=7,  justify="right")
        t.add_column("Overview", style="dim", min_width=40)

        for i, m in enumerate(results, 1):
            overview = m.get("overview", "")
            if len(overview) > 80:
                overview = overview[:79] + "…"
            t.add_row(str(i), m["title"], str(m["year"]) if m["year"] else "—",
                      str(m["rating"]), overview)
        console.print(t)
        console.print(f"\n[dim]Genres: {', '.join(top_genres[:3])} · "
                      f"min rating {min_rating} · {len(movie_plays)} movies in history[/dim]")

    def do_tmdb_movie(self, arg: str):
        """tmdb_movie <title> — search TMDB for a movie and display full details"""
        query = arg.strip()
        if not query:
            console.print("[yellow]Usage: tmdb_movie <title>[/yellow]"); return

        tmdb = self._get_tmdb_client()
        if not tmdb: return

        with console.status(f"Searching TMDB for '{query}'..."):
            results = tmdb.search_movie(query)
        if not results:
            console.print(f"[yellow]No TMDB results for '{query}'.[/yellow]"); return

        # Pick result
        if len(results) == 1:
            chosen = results[0]
        else:
            try:
                import questionary
                choices = [
                    questionary.Choice(
                        f"{m.get('title')} ({(m.get('release_date') or '?')[:4]}) — {(m.get('overview') or '')[:60]}",
                        value=m,
                    )
                    for m in results[:10]
                ]
                chosen = questionary.select("Select movie:", choices=choices).ask()
                if not chosen: return
            except Exception:
                best = min(results[:10],
                           key=lambda m: -SequenceMatcher(None, query.lower(),
                                                          (m.get("title") or "").lower()).ratio())
                chosen = best
                console.print(f"[dim]Using best match: {chosen.get('title')}[/dim]")

        with console.status("Fetching full details..."):
            d = tmdb.movie_detail(chosen["id"])
        if not d:
            console.print("[red]Could not fetch movie details.[/red]"); return

        year    = (d.get("release_date") or "")[:4]
        runtime = d.get("runtime") or 0
        budget  = d.get("budget") or 0
        revenue = d.get("revenue") or 0
        genres  = ", ".join(g["name"] for g in d.get("genres", []))
        langs   = ", ".join(l.get("english_name", l.get("name", ""))
                            for l in d.get("spoken_languages", []))
        countries = ", ".join(c.get("name", "") for c in d.get("production_countries", []))
        studios = ", ".join(c["name"] for c in d.get("production_companies", [])[:4])
        keywords = ", ".join(k["name"] for k in
                             (d.get("keywords") or {}).get("keywords", [])[:12])

        crew    = (d.get("credits") or {}).get("crew", [])
        cast    = (d.get("credits") or {}).get("cast", [])
        directors = ", ".join(p["name"] for p in crew if p.get("job") == "Director")
        writers   = ", ".join({p["name"] for p in crew
                               if p.get("department") == "Writing"}
                              )[:120]
        top_cast  = ", ".join(f"{p['name']} as {p.get('character','?')}"
                              for p in cast[:6])

        # Header panel
        rating     = d.get("vote_average") or 0
        vote_count = d.get("vote_count") or 0
        tagline    = d.get("tagline") or ""
        overview   = d.get("overview") or ""

        header = f"[bold white]{d.get('title', '?')}[/bold white]"
        if d.get("original_title") and d["original_title"] != d.get("title"):
            header += f"  [dim]({d['original_title']})[/dim]"
        if year:
            header += f"  [dim]{year}[/dim]"
        if runtime:
            header += f"  [dim]· {runtime} min[/dim]"
        if rating:
            header += f"  [yellow]★ {rating:.1f}[/yellow][dim]/{vote_count:,} votes[/dim]"

        console.print(Panel(header, box=box.ROUNDED))
        if tagline:
            console.print(f"  [italic dim]{tagline}[/italic dim]")
        if overview:
            console.print(f"\n  {overview}\n")

        # Details table
        t = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
        t.add_column("Field", style="bold dim", width=16)
        t.add_column("Value")

        def _row(label, val):
            if val:
                t.add_row(label, str(val))

        _row("TMDB ID",    str(d.get("id", "")))
        _row("IMDB ID",    d.get("imdb_id") or "")
        _row("Status",     d.get("status") or "")
        _row("Genres",     genres)
        _row("Languages",  langs)
        _row("Countries",  countries)
        _row("Director",   directors)
        _row("Writers",    writers)
        _row("Cast",       top_cast)
        _row("Studios",    studios)
        if budget:
            _row("Budget",  f"${budget:,.0f}")
        if revenue:
            _row("Revenue", f"${revenue:,.0f}")
        _row("Keywords",   keywords)

        # Collection membership
        col = d.get("belongs_to_collection")
        if col:
            _row("Collection", col.get("name", ""))

        console.print(t)

        # Cross-reference silent checks
        cfg = load_config()
        notes = []
        plex_set = self._plex_movie_set()
        if self._in_plex(d.get("title", ""), int(year) if year else 0, plex_set):
            notes.append("[green]✓ In your Plex library[/green]")
        r_url = cfg.get("radarr_url") or os.environ.get("RADARR_URL", "")
        r_key = cfg.get("radarr_api_key") or os.environ.get("RADARR_API_KEY", "")
        if r_url and r_key:
            radarr_ids = {m.get("tmdbId") for m in RadarrClient(r_url, r_key).movies()}
            if d.get("id") in radarr_ids:
                notes.append("[blue]✓ In Radarr[/blue]")
        if notes:
            console.print("  " + "  ·  ".join(notes))

    def do_trending_in_library(self, arg: str):
        """trending_in_library [--window day|week] — TMDB trending movies split by in/out of your library"""
        tokens = arg.strip().split() if arg.strip() else []
        window = "week"
        if "--window" in tokens:
            idx = tokens.index("--window")
            if idx + 1 < len(tokens) and tokens[idx + 1] in ("day", "week"):
                window = tokens[idx + 1]

        tmdb = self._get_tmdb_client()
        if not tmdb: return

        with console.status(f"Fetching TMDB trending ({window})..."):
            movies = tmdb.trending(window)
        with console.status("Cross-referencing Plex library..."):
            plex_set = self._plex_movie_set()

        in_lib     = [m for m in movies if     self._in_plex(m["title"], m["year"], plex_set)]
        not_in_lib = [m for m in movies if not self._in_plex(m["title"], m["year"], plex_set)]

        label = "this week" if window == "week" else "today"

        def _table(title: str, rows: list, name_style: str) -> None:
            if not rows: return
            t = Table(title=title, box=box.ROUNDED)
            t.add_column("#",        width=3,  justify="right", style="dim")
            t.add_column("Title",    style=name_style, min_width=28)
            t.add_column("Year",     width=6,  justify="right", style="dim")
            t.add_column("Rating",   width=7,  justify="right")
            t.add_column("Overview", style="dim", min_width=35)
            for i, m in enumerate(rows, 1):
                ov = m.get("overview", "")
                if len(ov) > 72: ov = ov[:71] + "…"
                t.add_row(str(i), m["title"], str(m["year"]) if m["year"] else "—",
                          str(m["rating"]), ov)
            console.print(t)

        _table(f"Trending {label} — [green]in your library[/green]  ({len(in_lib)})",
               in_lib, "bold green")
        _table(f"Trending {label} — [yellow]not in your library[/yellow]  ({len(not_in_lib)})",
               not_in_lib, "bold yellow")
        if not_in_lib:
            console.print("[dim]Use [bold]radarr_import[/bold] or [bold]radarr_director[/bold] to add any of these.[/dim]")

    def do_director_deep_dive(self, arg: str):
        """director_deep_dive <name> [--sort-rating] — full filmography: owned in Plex, watched, in Radarr, missing"""
        tokens = arg.strip().split()
        sort_by_rating = "--sort-rating" in tokens
        name = " ".join(t for t in tokens if t != "--sort-rating").strip()
        if not name:
            console.print("[yellow]Usage: director_deep_dive <name> [--sort-rating][/yellow]"); return

        tmdb = self._get_tmdb_client()
        if not tmdb: return

        with console.status(f"Searching TMDB for '{name}'..."):
            candidates = tmdb.search_person(name)
        if not candidates:
            console.print(f"[yellow]No TMDB results for '{name}'.[/yellow]"); return

        best = min(candidates[:8],
                   key=lambda p: -SequenceMatcher(None, name.lower(), (p.get("name") or "").lower()).ratio())
        if len(candidates) == 1:
            person = candidates[0]
            console.print(f"[dim]Found: {person.get('name')}[/dim]")
        else:
            try:
                import questionary
                choices = [
                    questionary.Choice(
                        f"{p.get('name')} — {', '.join(k.get('title') or k.get('name','') for k in p.get('known_for', [])[:2]) or '?'}",
                        value=p,
                    )
                    for p in candidates[:8]
                ]
                person = questionary.select("Select director:", choices=choices).ask()
                if not person: return
            except Exception:
                person = best
                console.print(f"[dim]Using best match: {person.get('name')}[/dim]")

        with console.status(f"Fetching {person['name']} filmography..."):
            films = tmdb.director_filmography(person["id"])
        if not films:
            console.print(f"[yellow]No directed films found for {person.get('name')}.[/yellow]"); return

        with console.status("Loading Plex library..."):
            plex_set = self._plex_movie_set()

        # Silent Radarr check
        cfg = load_config()
        radarr_ids: set = set()
        r_url = cfg.get("radarr_url") or os.environ.get("RADARR_URL", "")
        r_key = cfg.get("radarr_api_key") or os.environ.get("RADARR_API_KEY", "")
        if r_url and r_key:
            with console.status("Loading Radarr..."):
                radarr_ids = {m.get("tmdbId") for m in RadarrClient(r_url, r_key).movies()}

        # Silent Tautulli check
        watched: set[tuple[str, int]] = set()
        t_url = cfg.get("tautulli_url") or os.environ.get("TAUTULLI_URL", "")
        t_key = cfg.get("tautulli_api_key") or os.environ.get("TAUTULLI_API_KEY", "")
        if t_url and t_key:
            with console.status("Loading Tautulli watch history..."):
                for r in TautulliClient(t_url, t_key).history(length=1000):
                    if r.get("media_type") == "movie":
                        title = (r.get("full_title") or r.get("title") or "").lower().strip()
                        if title:
                            watched.add((title, int(r.get("year") or 0)))

        def _was_watched(title: str, year: int) -> bool:
            t = title.lower().strip()
            if (t, year) in watched: return True
            return any(abs(wy - year) <= 1 and SequenceMatcher(None, t, wt).ratio() >= 0.88
                       for wt, wy in watched)

        t = Table(
            title=f"[bold white]{person['name']}[/bold white] — {len(films)} directed films",
            box=box.ROUNDED,
        )
        t.add_column("Year",   width=6,  justify="right", style="dim")
        t.add_column("Title",  style="bold cyan", min_width=30)
        t.add_column("Rating", width=7,  justify="right", style="dim")
        t.add_column("Plex",   width=6,  justify="center")
        if watched:
            t.add_column("Watched", width=8, justify="center")
        if radarr_ids:
            t.add_column("Radarr", width=7, justify="center")

        ordered = sorted(films, key=lambda f: -(f.get("rating") or 0)) if sort_by_rating else reversed(films)
        in_plex = watched_count = in_radarr = 0
        for f in ordered:
            has_plex   = self._in_plex(f["title"], f["year"], plex_set)
            has_radarr = f["tmdb_id"] in radarr_ids
            seen       = _was_watched(f["title"], f["year"]) if watched else False
            if has_plex:   in_plex      += 1
            if seen:       watched_count += 1
            if has_radarr: in_radarr    += 1
            row = [
                str(f["year"]) if f["year"] else "—",
                f["title"],
                f"{f['rating']:.1f}" if f.get("rating") else "—",
                "[green]✓[/green]" if has_plex else "[dim]—[/dim]",
            ]
            if watched:
                row.append("[green]✓[/green]" if seen else "[dim]—[/dim]")
            if radarr_ids:
                row.append("[blue]✓[/blue]" if has_radarr else "[dim]—[/dim]")
            t.add_row(*row)

        console.print(t)
        summary = [f"[green]{in_plex}/{len(films)} in Plex[/green]"]
        if watched:
            summary.append(f"[cyan]{watched_count}/{len(films)} watched[/cyan]")
        if radarr_ids:
            summary.append(f"[blue]{in_radarr}/{len(films)} in Radarr[/blue]")
        console.print("  ·  ".join(summary))

    def do_actor_deep_dive(self, arg: str):
        """actor_deep_dive <name> [--sort-rating] — filmography as actor: owned in Plex, watched, in Radarr"""
        tokens = arg.strip().split()
        sort_by_rating = "--sort-rating" in tokens
        name = " ".join(t for t in tokens if t != "--sort-rating").strip()
        if not name:
            console.print("[yellow]Usage: actor_deep_dive <name> [--sort-rating][/yellow]"); return

        tmdb = self._get_tmdb_client()
        if not tmdb: return

        with console.status(f"Searching TMDB for '{name}'..."):
            candidates = tmdb.search_person(name)
        if not candidates:
            console.print(f"[yellow]No TMDB results for '{name}'.[/yellow]"); return

        best = min(candidates[:8],
                   key=lambda p: -SequenceMatcher(None, name.lower(), (p.get("name") or "").lower()).ratio())
        if len(candidates) == 1:
            person = candidates[0]
            console.print(f"[dim]Found: {person.get('name')}[/dim]")
        else:
            try:
                import questionary
                choices = [
                    questionary.Choice(
                        f"{p.get('name')} — {', '.join(k.get('title') or k.get('name','') for k in p.get('known_for', [])[:2]) or '?'}",
                        value=p,
                    )
                    for p in candidates[:8]
                ]
                person = questionary.select("Select actor:", choices=choices).ask()
                if not person: return
            except Exception:
                person = best
                console.print(f"[dim]Using best match: {person.get('name')}[/dim]")

        with console.status(f"Fetching {person['name']} filmography..."):
            films = tmdb.actor_filmography(person["id"])
        if not films:
            console.print(f"[yellow]No acting credits found for {person.get('name')}.[/yellow]"); return

        with console.status("Loading Plex library..."):
            plex_set = self._plex_movie_set()

        cfg = load_config()
        radarr_ids: set = set()
        r_url = cfg.get("radarr_url") or os.environ.get("RADARR_URL", "")
        r_key = cfg.get("radarr_api_key") or os.environ.get("RADARR_API_KEY", "")
        if r_url and r_key:
            with console.status("Loading Radarr..."):
                radarr_ids = {m.get("tmdbId") for m in RadarrClient(r_url, r_key).movies()}

        watched: set[tuple[str, int]] = set()
        t_url = cfg.get("tautulli_url") or os.environ.get("TAUTULLI_URL", "")
        t_key = cfg.get("tautulli_api_key") or os.environ.get("TAUTULLI_API_KEY", "")
        if t_url and t_key:
            with console.status("Loading Tautulli watch history..."):
                for r in TautulliClient(t_url, t_key).history(length=1000):
                    if r.get("media_type") == "movie":
                        title = (r.get("full_title") or r.get("title") or "").lower().strip()
                        if title:
                            watched.add((title, int(r.get("year") or 0)))

        def _was_watched(title: str, year: int) -> bool:
            t = title.lower().strip()
            if (t, year) in watched: return True
            return any(abs(wy - year) <= 1 and SequenceMatcher(None, t, wt).ratio() >= 0.88
                       for wt, wy in watched)

        t = Table(
            title=f"[bold white]{person['name']}[/bold white] — {len(films)} acting credits",
            box=box.ROUNDED,
        )
        t.add_column("Year",      width=6,  justify="right", style="dim")
        t.add_column("Title",     style="bold cyan", min_width=28)
        t.add_column("Character", style="italic", min_width=20)
        t.add_column("Rating",    width=7,  justify="right", style="dim")
        t.add_column("Plex",      width=6,  justify="center")
        if watched:
            t.add_column("Watched", width=8, justify="center")
        if radarr_ids:
            t.add_column("Radarr", width=7, justify="center")

        ordered = sorted(films, key=lambda f: -(f.get("rating") or 0)) if sort_by_rating else reversed(films)
        in_plex = watched_count = in_radarr = 0
        for f in ordered:
            has_plex   = self._in_plex(f["title"], f["year"], plex_set)
            has_radarr = f["tmdb_id"] in radarr_ids
            seen       = _was_watched(f["title"], f["year"]) if watched else False
            if has_plex:   in_plex      += 1
            if seen:       watched_count += 1
            if has_radarr: in_radarr    += 1
            row = [
                str(f["year"]) if f["year"] else "—",
                f["title"],
                f["character"] or "—",
                f"{f['rating']:.1f}" if f.get("rating") else "—",
                "[green]✓[/green]" if has_plex else "[dim]—[/dim]",
            ]
            if watched:
                row.append("[green]✓[/green]" if seen else "[dim]—[/dim]")
            if radarr_ids:
                row.append("[blue]✓[/blue]" if has_radarr else "[dim]—[/dim]")
            t.add_row(*row)

        console.print(t)
        summary = [f"[green]{in_plex}/{len(films)} in Plex[/green]"]
        if watched:
            summary.append(f"[cyan]{watched_count}/{len(films)} watched[/cyan]")
        if radarr_ids:
            summary.append(f"[blue]{in_radarr}/{len(films)} in Radarr[/blue]")
        console.print("  ·  ".join(summary))

    def do_radarr_upgrade_4k(self, arg: str):
        """radarr_upgrade_4k <title> [--search] — switch a movie's quality profile to 4K and optionally trigger a search"""
        tokens = arg.strip().split()
        search_now = "--search" in tokens
        query = " ".join(t for t in tokens if t != "--search").strip()
        if not query:
            console.print("[yellow]Usage: radarr_upgrade_4k <title> [--search][/yellow]"); return

        rc = self._get_radarr_client()
        if not rc: return

        with console.status("Loading Radarr library..."):
            all_movies = rc.movies()
            profiles   = rc.quality_profiles()

        # Find 4K-capable profiles (name contains "4k", "2160", or "uhd", case-insensitive)
        hd_profiles = [p for p in profiles
                       if any(k in p.get("name", "").lower() for k in ("4k", "2160", "uhd"))]
        if not hd_profiles:
            console.print("[red]No 4K quality profiles found in Radarr.[/red]")
            console.print("[dim]Available profiles: " +
                          ", ".join(p["name"] for p in profiles) + "[/dim]")
            return

        # Fuzzy-match movie by title
        matches = sorted(
            all_movies,
            key=lambda m: -SequenceMatcher(None, query.lower(),
                                           (m.get("title") or "").lower()).ratio(),
        )
        # Keep candidates with a reasonable match score
        candidates = [m for m in matches
                      if SequenceMatcher(None, query.lower(),
                                        (m.get("title") or "").lower()).ratio() >= 0.4][:10]
        if not candidates:
            console.print(f"[yellow]No Radarr movies matching '{query}'.[/yellow]"); return

        if len(candidates) == 1:
            movie = candidates[0]
            console.print(f"[dim]Found: {movie['title']} ({movie.get('year', '?')})[/dim]")
        else:
            try:
                import questionary
                choices = [
                    questionary.Choice(
                        f"{m['title']} ({m.get('year','?')}) — "
                        f"{next((p['name'] for p in profiles if p['id'] == m.get('qualityProfileId')), '?')}",
                        value=m,
                    )
                    for m in candidates
                ]
                movie = questionary.select("Select movie:", choices=choices).ask()
                if not movie: return
            except Exception:
                movie = candidates[0]
                console.print(f"[dim]Using best match: {movie['title']}[/dim]")

        # Resolve current profile name
        profile_map = {p["id"]: p["name"] for p in profiles}
        current_profile = profile_map.get(movie.get("qualityProfileId"), "?")

        # Pick 4K profile
        if len(hd_profiles) == 1:
            target_profile = hd_profiles[0]
        else:
            try:
                import questionary
                choices = [questionary.Choice(p["name"], value=p) for p in hd_profiles]
                target_profile = questionary.select("Select 4K profile:", choices=choices).ask()
                if not target_profile: return
            except Exception:
                target_profile = hd_profiles[0]
                console.print(f"[dim]Using profile: {target_profile['name']}[/dim]")

        if target_profile["id"] == movie.get("qualityProfileId"):
            console.print(f"[yellow]{movie['title']} is already on profile "
                          f"'{target_profile['name']}'.[/yellow]"); return

        console.print(
            f"[bold]{movie['title']}[/bold] ({movie.get('year','?')})\n"
            f"  [dim]{current_profile}[/dim] → [green]{target_profile['name']}[/green]"
        )
        if Prompt.ask("Apply?", choices=["y", "n"], default="n") != "y":
            console.print("[dim]Cancelled.[/dim]"); return

        updated = dict(movie)
        updated["qualityProfileId"] = target_profile["id"]
        with console.status("Updating..."):
            result = rc.update_movie(updated)

        if result.get("id"):
            console.print(f"[green]Updated.[/green] Quality profile set to "
                          f"[bold]{target_profile['name']}[/bold].")
            if search_now:
                with console.status("Triggering search..."):
                    rc.search_movies([movie["id"]])
                console.print("[green]Search triggered.[/green]")
            else:
                console.print("[dim]Run with [bold]--search[/bold] to trigger an immediate download search.[/dim]")
        else:
            console.print("[red]Update failed — Radarr returned an unexpected response.[/red]")

    def do_radarr_upgrade(self, arg: str):
        """radarr_upgrade — list downloaded movies below their quality cutoff and trigger re-searches"""
        rc = self._get_radarr_client()
        if not rc: return

        with console.status("Loading Radarr library..."):
            profiles_raw = rc.quality_profiles()
            movies       = rc.movies()

        # Build quality_id → {name, resolution} from all profiles (handles nested groups)
        qual_map: dict[int, dict] = {}
        def _extract_quals(items: list) -> None:
            for item in items:
                if item.get("quality"):
                    q = item["quality"]
                    qual_map[q["id"]] = {"name": q.get("name", "?"), "res": q.get("resolution", 0)}
                _extract_quals(item.get("items", []))
        for p in profiles_raw:
            _extract_quals(p.get("items", []))

        # Build profile_id → {name, cutoff_id, cutoff_res, cutoff_name, upgrade_allowed}
        profile_map: dict[int, dict] = {}
        for p in profiles_raw:
            cid = p.get("cutoff", 0)
            cq  = qual_map.get(cid, {})
            profile_map[p["id"]] = {
                "name":            p["name"],
                "cutoff_id":       cid,
                "cutoff_res":      cq.get("res", 0),
                "cutoff_name":     cq.get("name", "?"),
                "upgrade_allowed": p.get("upgradeAllowed", True),
            }

        upgradeable = []
        for m in movies:
            if not m.get("hasFile"): continue
            prof = profile_map.get(m.get("qualityProfileId", 0), {})
            if not prof.get("upgrade_allowed", True): continue
            mf        = m.get("movieFile") or {}
            curr_q    = (mf.get("quality") or {}).get("quality") or {}
            curr_res  = curr_q.get("resolution", 0)
            curr_name = curr_q.get("name", "?")
            cutoff_res = prof.get("cutoff_res", 0)
            if cutoff_res > 0 and curr_res < cutoff_res:
                upgradeable.append({
                    "id":         m["id"],
                    "title":      m.get("title", ""),
                    "year":       m.get("year", 0),
                    "current":    curr_name,
                    "current_res": curr_res,
                    "target":     prof["cutoff_name"],
                    "target_res": cutoff_res,
                    "profile":    prof["name"],
                })
        upgradeable.sort(key=lambda x: (-(x["target_res"] - x["current_res"]), x["title"]))

        if not upgradeable:
            console.print("[green]All downloaded movies meet their quality profile cutoff.[/green]"); return

        t = Table(title=f"Movies below quality cutoff  ({len(upgradeable)})", box=box.ROUNDED)
        t.add_column("#",       style="dim",       width=4,  justify="right")
        t.add_column("Title",   style="bold white", min_width=30)
        t.add_column("Year",    width=6,            justify="right")
        t.add_column("Have",    width=16)
        t.add_column("Want",    width=16)
        t.add_column("Profile", width=14,           style="dim")
        for i, r in enumerate(upgradeable, 1):
            t.add_row(str(i), r["title"], str(r["year"]),
                      f"[yellow]{r['current']}[/yellow]",
                      f"[cyan]{r['target']}[/cyan]",
                      r["profile"])
        console.print(t)

        try:
            import questionary
            console.print(f"\n[dim]↑↓ scroll · Space toggle · a select all · Enter confirm · Ctrl-C cancel[/dim]\n")
            choices = [
                questionary.Choice(
                    title=f"{r['title']} ({r['year']})  {r['current']} → {r['target']}",
                    value=r,
                )
                for r in upgradeable
            ]
            selected = questionary.checkbox(
                "Select movies to trigger upgrade search:", choices=choices, instruction=" ",
            ).ask()
        except ImportError:
            sel_input = Prompt.ask(
                f"Enter numbers to search (e.g. [bold]1 3 5-10[/bold]), or blank for all {len(upgradeable)}",
                default="",
            ).strip()
            if sel_input:
                selected_nums: set[int] = set()
                for part in sel_input.split():
                    if "-" in part:
                        try:
                            a, b = part.split("-", 1)
                            selected_nums.update(range(int(a), int(b) + 1))
                        except ValueError: pass
                    else:
                        try: selected_nums.add(int(part))
                        except ValueError: pass
                selected = [upgradeable[n - 1] for n in sorted(selected_nums)
                            if 1 <= n <= len(upgradeable)]
            else:
                selected = upgradeable

        if not selected:
            console.print("[dim]Nothing selected.[/dim]"); return

        rc.search_movies([r["id"] for r in selected])
        console.print(f"[green]Upgrade search triggered[/green] for {len(selected)} movie(s).")
        console.print("[dim]Radarr will replace files when a better version is found.[/dim]")

    def do_radarr_sync(self, _arg: str):
        """radarr_sync — find movies downloaded in Radarr but missing from Plex, and vice versa"""
        rc = self._get_radarr_client()
        if not rc: return

        with console.status("Loading Radarr and Plex libraries..."):
            radarr_movies = rc.movies()
            plex_set      = self._plex_movie_set()

            plex_movies: list[dict] = []
            for lib in self.client.libraries():
                if lib.get("type") != "movie": continue
                for item in self.client.library_contents(lib.get("key", "")):
                    plex_movies.append({
                        "title":   item.get("title", ""),
                        "year":    item.get("year", 0),
                        "library": lib.get("title", ""),
                    })

        # ── Section 1: Downloaded in Radarr but not indexed in Plex ──────────
        radarr_not_plex = sorted(
            [m for m in radarr_movies
             if m.get("hasFile") and not self._in_plex(m.get("title", ""), m.get("year", 0), plex_set)],
            key=lambda x: x.get("title", ""),
        )

        # ── Section 2: In Plex but not monitored by Radarr ───────────────────
        # Build year-bucketed lookup for efficient fuzzy matching
        radarr_by_year: dict[int, set[str]] = defaultdict(set)
        for m in radarr_movies:
            radarr_by_year[m.get("year", 0)].add(m.get("title", "").lower().strip())

        def _in_radarr(title: str, year: int) -> bool:
            t = title.lower().strip()
            for y in (year - 1, year, year + 1):
                for rt in radarr_by_year.get(y, set()):
                    if t == rt or SequenceMatcher(None, t, rt).ratio() >= 0.90:
                        return True
            return False

        plex_not_radarr = sorted(
            [pm for pm in plex_movies if not _in_radarr(pm["title"], pm["year"])],
            key=lambda x: x.get("title", ""),
        )

        # ── Display ───────────────────────────────────────────────────────────
        console.print()
        if radarr_not_plex:
            t1 = Table(
                title=f"[yellow]Downloaded in Radarr — not indexed in Plex[/yellow]  "
                      f"({len(radarr_not_plex)})",
                box=box.ROUNDED,
            )
            t1.add_column("Title",    style="bold white", min_width=30)
            t1.add_column("Year",     width=6, justify="right")
            t1.add_column("File path", style="dim", min_width=30)
            for m in radarr_not_plex:
                path = (m.get("movieFile") or {}).get("path") or "—"
                t1.add_row(m.get("title", ""), str(m.get("year", "")), path)
            console.print(t1)
            console.print("[dim]Tip: trigger a Plex library scan — the file may just need indexing.[/dim]\n")
        else:
            console.print("[green]✓ All Radarr downloads are indexed in Plex.[/green]\n")

        if plex_not_radarr:
            t2 = Table(
                title=f"[yellow]In Plex — not monitored by Radarr[/yellow]  "
                      f"({len(plex_not_radarr)})",
                box=box.ROUNDED,
            )
            t2.add_column("Title",   style="bold white", min_width=30)
            t2.add_column("Year",    width=6, justify="right")
            t2.add_column("Library", width=18, style="dim")
            for pm in plex_not_radarr:
                t2.add_row(pm["title"], str(pm["year"]), pm["library"])
            console.print(t2)
            console.print("[dim]Tip: use radarr_list_info + radarr_list_add to add these to a monitored list.[/dim]")
        else:
            console.print("[green]✓ All Plex movies are monitored by Radarr.[/green]")

    def do_quality_upgrade_plan(self, arg: str):
        """quality_upgrade_plan [--limit N] [--movies|--shows] - prioritize Radarr/Sonarr cutoff work"""
        tokens = self._tokens(arg)
        limit = self._int_flag(tokens, "--limit", 25, minimum=1)
        movies_only = self._has_flag(tokens, "--movies")
        shows_only = self._has_flag(tokens, "--shows")
        if movies_only and shows_only:
            console.print("[yellow]Use only one of --movies or --shows.[/yellow]")
            return

        rc = None if shows_only else self._configured_radarr_client()
        sc = None if movies_only else self._configured_sonarr_client()
        if not rc and not sc:
            console.print("[yellow]No configured Radarr or Sonarr credentials found.[/yellow]")
            console.print("[dim]Run radarr_status or sonarr_status once to save API settings.[/dim]")
            return

        movie_stats: dict[tuple[str, int], dict] = {}
        show_stats: dict[str, dict] = {}
        with console.status("Indexing Plex watch and quality stats..."):
            for lib in self.client.libraries():
                lib_type = lib.get("type", "")
                if lib_type == "movie" and rc:
                    for item in self.client.library_contents(lib.get("key", "")):
                        rows = get_media_rows(item, lib.get("title", ""))
                        key = ((item.get("title") or "").lower().strip(), item.get("year") or 0)
                        movie_stats[key] = {
                            "plays": item.get("viewCount") or 0,
                            "rating": item.get("audienceRating")
                                      if item.get("audienceRating") is not None else item.get("rating"),
                            "size": sum(r.get("size") or 0 for r in rows),
                            "resolution": rows[0].get("videoResolution", "") if rows else "",
                            "codec": rows[0].get("videoCodec", "") if rows else "",
                        }
                elif lib_type == "show" and sc:
                    for show in self.client.library_contents(lib.get("key", "")):
                        title = (show.get("title") or "").lower().strip()
                        if title:
                            show_stats[title] = {
                                "watched": show.get("viewedLeafCount") or 0,
                                "total": show.get("leafCount") or 0,
                                "last": show.get("lastViewedAt") or 0,
                            }

        def _match_movie(title: str, yr: int) -> dict:
            key = (title.lower().strip(), yr or 0)
            if key in movie_stats:
                return movie_stats[key]
            best_score = 0.0
            best: dict = {}
            for (pt, py), stats in movie_stats.items():
                if yr and py and abs(py - yr) > 1:
                    continue
                score = SequenceMatcher(None, title.lower().strip(), pt).ratio()
                if score > best_score:
                    best_score = score
                    best = stats
            return best if best_score >= 0.88 else {}

        def _match_show(title: str) -> dict:
            key = title.lower().strip()
            if key in show_stats:
                return show_stats[key]
            best_score = 0.0
            best: dict = {}
            for st, stats in show_stats.items():
                score = SequenceMatcher(None, key, st).ratio()
                if score > best_score:
                    best_score = score
                    best = stats
            return best if best_score >= 0.88 else {}

        if rc:
            with console.status("Loading Radarr quality cutoffs..."):
                profiles_raw = rc.quality_profiles()
                radarr_movies = rc.movies()

            qual_map: dict[int, dict] = {}
            def _extract_quals(items: list) -> None:
                for item in items:
                    if item.get("quality"):
                        q = item["quality"]
                        qual_map[q["id"]] = {
                            "name": q.get("name", "?"),
                            "res": q.get("resolution", 0),
                        }
                    _extract_quals(item.get("items", []))
            for profile in profiles_raw:
                _extract_quals(profile.get("items", []))

            profile_map: dict[int, dict] = {}
            for profile in profiles_raw:
                cutoff_id = profile.get("cutoff", 0)
                cutoff = qual_map.get(cutoff_id, {})
                profile_map[profile["id"]] = {
                    "name": profile.get("name", "?"),
                    "cutoff_name": cutoff.get("name", "?"),
                    "cutoff_res": cutoff.get("res", 0),
                    "upgrade_allowed": profile.get("upgradeAllowed", True),
                }

            movie_plan = []
            for movie in radarr_movies:
                if not movie.get("hasFile"):
                    continue
                prof = profile_map.get(movie.get("qualityProfileId", 0), {})
                if not prof.get("upgrade_allowed", True):
                    continue
                movie_file = movie.get("movieFile") or {}
                current_quality = (movie_file.get("quality") or {}).get("quality") or {}
                current_res = current_quality.get("resolution", 0) or 0
                cutoff_res = prof.get("cutoff_res", 0) or 0
                if not cutoff_res or current_res >= cutoff_res:
                    continue
                stats = _match_movie(movie.get("title", ""), movie.get("year") or 0)
                plays = stats.get("plays") or 0
                size = stats.get("size") or movie_file.get("size") or 0
                rating_val = stats.get("rating") or 0
                try:
                    rating_val = float(rating_val)
                except (TypeError, ValueError):
                    rating_val = 0
                size_gb = size / (1024 ** 3) if size else 0
                score = ((cutoff_res - current_res) / 100) + plays * 4 + rating_val + min(size_gb / 8, 10)
                movie_plan.append({
                    "title": movie.get("title", ""),
                    "year": movie.get("year") or 0,
                    "current": current_quality.get("name", "?"),
                    "target": prof.get("cutoff_name", "?"),
                    "profile": prof.get("name", "?"),
                    "plays": plays,
                    "size": size,
                    "score": score,
                })

            movie_plan.sort(key=lambda r: (-r["score"], r["title"]))
            if movie_plan:
                t = Table(title=f"Radarr Movie Upgrade Plan ({len(movie_plan)} below cutoff)",
                          box=box.ROUNDED)
                t.add_column("#", style="dim", width=4, justify="right")
                t.add_column("Title", style="bold white", min_width=28)
                t.add_column("Have", width=14)
                t.add_column("Want", width=14)
                t.add_column("Plays", width=7, justify="right", style="cyan")
                t.add_column("Size", width=10, justify="right")
                t.add_column("Score", width=7, justify="right", style="yellow")
                for i, row in enumerate(movie_plan[:limit], 1):
                    title = f"{row['title']} ({row['year']})" if row["year"] else row["title"]
                    t.add_row(str(i), title, row["current"], row["target"],
                              str(row["plays"]), format_size(row["size"]),
                              f"{row['score']:.1f}")
                console.print(t)
            else:
                console.print("[green]No Radarr movies are below their quality cutoff.[/green]")
        elif not shows_only:
            console.print("[dim]Radarr is not configured; movie upgrade plan skipped.[/dim]")

        if sc:
            with console.status("Loading Sonarr below-cutoff episodes..."):
                records = sc.wanted_cutoff()
            show_plan: dict[int, dict] = {}
            for episode in records:
                series = episode.get("series") or {}
                sid = episode.get("seriesId") or series.get("id") or 0
                if sid not in show_plan:
                    stats = _match_show(series.get("title", ""))
                    show_plan[sid] = {
                        "title": series.get("title", ""),
                        "year": series.get("year") or 0,
                        "status": series.get("status", ""),
                        "count": 0,
                        "watched": stats.get("watched") or 0,
                        "total": stats.get("total") or 0,
                        "last": stats.get("last") or 0,
                    }
                show_plan[sid]["count"] += 1

            show_rows = []
            for row in show_plan.values():
                total = row["total"] or 0
                watched = row["watched"] or 0
                recent_bonus = 20 if row["last"] and row["last"] >= int(time.time()) - 180 * 86400 else 0
                score = row["count"] * 8 + watched * 1.5 + recent_bonus
                row["score"] = score
                row["progress"] = f"{watched}/{total}" if total else "-"
                show_rows.append(row)
            show_rows.sort(key=lambda r: (-r["score"], r["title"]))

            if show_rows:
                t = Table(title=f"Sonarr Show Upgrade Plan ({len(records)} episodes below cutoff)",
                          box=box.ROUNDED)
                t.add_column("#", style="dim", width=4, justify="right")
                t.add_column("Show", style="bold white", min_width=28)
                t.add_column("Below", width=7, justify="right", style="yellow")
                t.add_column("Watched", width=9, justify="right", style="cyan")
                t.add_column("Last Watched", width=17, style="dim")
                t.add_column("Score", width=7, justify="right", style="yellow")
                for i, row in enumerate(show_rows[:limit], 1):
                    title = f"{row['title']} ({row['year']})" if row["year"] else row["title"]
                    t.add_row(str(i), title, str(row["count"]), row["progress"],
                              format_ts(row["last"]), f"{row['score']:.1f}")
                console.print(t)
            else:
                console.print("[green]No Sonarr episodes are below their quality cutoff.[/green]")
        elif not movies_only:
            console.print("[dim]Sonarr is not configured; show upgrade plan skipped.[/dim]")

        console.print("[dim]Use radarr_upgrade or sonarr_upgrade to select items and trigger searches.[/dim]")

    def do_premiere_watchlist(self, arg: str):
        """premiere_watchlist [--days N] [--past N] [--missing-only] - Radarr releases with Plex status"""
        tokens = self._tokens(arg)
        days = self._int_flag(tokens, "--days", 30, minimum=1)
        past = self._int_flag(tokens, "--past", 14, minimum=0)
        missing_only = self._has_flag(tokens, "--missing-only")

        rc = self._get_radarr_client()
        if not rc: return

        today = date.today()
        start = today - timedelta(days=past)
        end = today + timedelta(days=days)

        def _parse_date(value: str | None) -> date | None:
            if not value:
                return None
            try:
                return date.fromisoformat(value[:10])
            except ValueError:
                return None

        def _fmt_date(value: str | None) -> str:
            parsed = _parse_date(value)
            return parsed.isoformat() if parsed else "-"

        with console.status(f"Building premiere watchlist ({start} to {end})..."):
            movies = rc.calendar(start.isoformat(), end.isoformat())
            plex_set = self._plex_movie_set()

        rows = []
        for movie in movies:
            title = movie.get("title", "?")
            year_val = movie.get("year") or 0
            dates = [_parse_date(movie.get(k))
                     for k in ("digitalRelease", "physicalRelease", "inCinemas")]
            known_dates = [d for d in dates if d]
            release_date = min(known_dates) if known_dates else date.max
            released = any(d and d <= today for d in known_dates)
            in_plex = self._in_plex(title, year_val, plex_set)
            has_file = bool(movie.get("hasFile"))
            monitored = bool(movie.get("monitored"))
            if in_plex:
                status = "[green]In Plex[/green]"
                priority = 4
            elif has_file:
                status = "[yellow]Downloaded - scan Plex[/yellow]"
                priority = 0
            elif released and monitored:
                status = "[red]Released - missing[/red]"
                priority = 1
            elif monitored:
                status = "[cyan]Upcoming[/cyan]"
                priority = 2
            else:
                status = "[dim]Unmonitored[/dim]"
                priority = 3
            if missing_only and priority not in (0, 1):
                continue
            rows.append({
                "priority": priority,
                "release_date": release_date,
                "title": title,
                "year": year_val,
                "cinemas": _fmt_date(movie.get("inCinemas")),
                "digital": _fmt_date(movie.get("digitalRelease")),
                "physical": _fmt_date(movie.get("physicalRelease")),
                "status": status,
            })

        rows.sort(key=lambda r: (r["priority"], r["release_date"], r["title"]))
        if not rows:
            msg = "No released-but-missing movies found." if missing_only else "No Radarr releases found."
            console.print(f"[yellow]{msg}[/yellow]")
            return

        t = Table(title=f"Premiere Watchlist ({start} to {end})", box=box.ROUNDED)
        t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Year", width=6, justify="right", style="dim")
        t.add_column("Digital", width=12)
        t.add_column("Physical", width=12)
        t.add_column("Cinemas", width=12)
        t.add_column("Status", min_width=20)
        for row in rows:
            t.add_row(row["title"], str(row["year"] or "-"), row["digital"],
                      row["physical"], row["cinemas"], row["status"])
        console.print(t)
        if any(r["priority"] == 0 for r in rows):
            console.print("[dim]Downloaded titles that are not in Plex usually need a library scan.[/dim]")

    def do_radarr_calendar(self, arg: str):
        """radarr_calendar [--days N] — upcoming movie releases monitored by Radarr (default 30 days)"""
        tokens = arg.strip().split() if arg.strip() else []
        days = 30
        if "--days" in tokens:
            idx = tokens.index("--days")
            if idx + 1 < len(tokens):
                try: days = int(tokens[idx + 1])
                except ValueError: pass

        rc = self._get_radarr_client()
        if not rc: return

        today = date.today()
        end   = today + timedelta(days=days)

        with console.status(f"Fetching Radarr calendar ({today} → {end})..."):
            movies = rc.calendar(today.isoformat(), end.isoformat())

        if not movies:
            console.print(f"[yellow]No releases in the next {days} day(s).[/yellow]"); return

        def _earliest(m: dict) -> str:
            dates = [d for d in [m.get("inCinemas"), m.get("physicalRelease"), m.get("digitalRelease")] if d]
            return min(dates) if dates else "9999"

        def _fmt_date(s: str | None) -> str:
            if not s: return "—"
            try: return datetime.fromisoformat(s.replace("Z", "+00:00")).strftime("%Y-%m-%d")
            except Exception: return s[:10]

        movies.sort(key=_earliest)

        t = Table(title=f"Upcoming Releases — next {days} day(s)  ({today} → {end})", box=box.ROUNDED)
        t.add_column("Title",    style="bold cyan", min_width=28)
        t.add_column("Year",     width=6,  justify="right", style="dim")
        t.add_column("Cinemas",  width=12)
        t.add_column("Physical", width=12)
        t.add_column("Digital",  width=12)
        t.add_column("Status",   width=13)

        for m in movies:
            if m.get("hasFile"):
                status = "[green]Downloaded[/green]"
            elif m.get("monitored"):
                status = "[yellow]Monitored[/yellow]"
            else:
                status = "[dim]Unmonitored[/dim]"
            t.add_row(
                m.get("title", "?"),
                str(m.get("year", "")),
                _fmt_date(m.get("inCinemas")),
                _fmt_date(m.get("physicalRelease")),
                _fmt_date(m.get("digitalRelease")),
                status,
            )

        console.print(t)
        console.print(f"[dim]{len(movies)} release(s) found.[/dim]")

    def do_radarr_collections(self, arg: str):
        """radarr_collections [--import] — TMDB franchise completion; --import adds missing to Radarr"""
        tokens    = arg.strip().split() if arg.strip() else []
        do_import = "--import" in tokens

        rc = self._get_radarr_client()
        if not rc: return
        tc = self._get_tmdb_client()
        if not tc: return

        with console.status("Loading Radarr library..."):
            radarr_movies = rc.movies()

        radarr_by_tmdb: dict[int, dict] = {m.get("tmdbId"): m for m in radarr_movies if m.get("tmdbId")}

        by_coll: dict[int, dict] = {}
        for m in radarr_movies:
            coll    = m.get("collection") or {}
            coll_id = coll.get("tmdbId")
            if not coll_id: continue
            if coll_id not in by_coll:
                by_coll[coll_id] = {"name": coll.get("title", "?"), "have": set()}
            by_coll[coll_id]["have"].add(m.get("tmdbId"))

        if not by_coll:
            console.print("[yellow]No movies with TMDB collection data found in Radarr.[/yellow]")
            console.print("[dim]Tip: Radarr populates collection data from TMDB — try refreshing movie metadata in Radarr.[/dim]")
            return

        results:     list[dict] = []
        all_missing: list[dict] = []

        with console.status("Fetching collection details from TMDB...") as status:
            for coll_id, info in sorted(by_coll.items(), key=lambda x: x[1]["name"]):
                status.update(f"Fetching [bold]{info['name']}[/bold]…")
                coll_data = tc.collection(coll_id)
                parts     = coll_data.get("parts", [])
                missing   = [p for p in parts if p["tmdb_id"] not in radarr_by_tmdb]
                results.append({
                    "name":    coll_data.get("name") or info["name"],
                    "total":   len(parts),
                    "have":    len(parts) - len(missing),
                    "missing": missing,
                })
                all_missing.extend(missing)

        results.sort(key=lambda x: (-len(x["missing"]), x["name"]))

        t = Table(title=f"Franchise Completion ({len(results)} collections)", box=box.ROUNDED)
        t.add_column("Collection", style="bold cyan", min_width=28)
        t.add_column("Have",  width=5, justify="right")
        t.add_column("Total", width=5, justify="right", style="dim")
        t.add_column("Missing", style="yellow", min_width=35)

        for r in results:
            clr         = "green" if not r["missing"] else "yellow"
            have_str    = f"[{clr}]{r['have']}[/{clr}]"
            missing_str = (", ".join(f"{m['title']} ({m['year']})" for m in r["missing"])
                           if r["missing"] else "[green]complete[/green]")
            t.add_row(r["name"], have_str, str(r["total"]), missing_str)

        console.print(t)

        total_missing = sum(len(r["missing"]) for r in results)
        incomplete    = sum(1 for r in results if r["missing"])
        if total_missing:
            console.print(f"\n[yellow]{total_missing} film(s) missing[/yellow] across {incomplete} incomplete franchise(s).")
            if not do_import:
                console.print("[dim]Run with [bold]--import[/bold] to add all missing to Radarr.[/dim]")
        else:
            console.print("[green]All franchises complete.[/green]")

        if do_import and all_missing:
            seen: set[int] = set()
            unique: list[dict] = []
            for m in all_missing:
                if m["tmdb_id"] not in seen and m["tmdb_id"] not in radarr_by_tmdb:
                    seen.add(m["tmdb_id"])
                    unique.append(m)
            if unique:
                self._radarr_import_workflow(unique, rc, dry_run=False, profile_id=None, search=False)
            else:
                console.print("[green]Nothing new to import.[/green]")

    def do_collection_gaps(self, arg: str):
        """collection_gaps [--import] [--dry-run] [--limit N] - franchise gaps across Plex and Radarr"""
        tokens = self._tokens(arg)
        do_import = self._has_flag(tokens, "--import")
        dry_run = self._has_flag(tokens, "--dry-run")
        limit = self._int_flag(tokens, "--limit", 0, minimum=0)

        rc = self._get_radarr_client()
        if not rc: return
        tmdb = self._get_tmdb_client()
        if not tmdb: return

        with console.status("Loading Radarr and Plex movie state..."):
            radarr_movies = rc.movies()
            plex_set = self._plex_movie_set()

        radarr_by_tmdb = {m.get("tmdbId"): m for m in radarr_movies if m.get("tmdbId")}
        by_collection: dict[int, dict] = {}
        for movie in radarr_movies:
            coll = movie.get("collection") or {}
            coll_id = coll.get("tmdbId")
            if not coll_id:
                continue
            by_collection.setdefault(coll_id, {
                "name": coll.get("title") or f"Collection {coll_id}",
                "seed": set(),
            })["seed"].add(movie.get("tmdbId"))

        if not by_collection:
            console.print("[yellow]No Radarr movies with TMDB collection data found.[/yellow]")
            console.print("[dim]Refresh movie metadata in Radarr, then try again.[/dim]")
            return

        def _in_plex_part(part: dict) -> bool:
            title = part.get("title", "")
            yr = part.get("year") or 0
            if self._in_plex(title, yr, plex_set):
                return True
            return any(title.lower().strip() == pt for pt, _ in plex_set)

        def _short_list(items: list[dict], cap: int = 4) -> str:
            if not items:
                return "[green]complete[/green]"
            shown = ", ".join(f"{m['title']} ({m['year'] or '-'})" for m in items[:cap])
            if len(items) > cap:
                shown += f", +{len(items) - cap} more"
            return shown

        rows: list[dict] = []
        import_candidates: list[dict] = []
        with console.status("Fetching collection details from TMDB...") as status:
            for coll_id, info in sorted(by_collection.items(), key=lambda x: x[1]["name"]):
                status.update(f"Fetching {info['name']}...")
                data = tmdb.collection(coll_id)
                parts = data.get("parts", [])
                if not parts:
                    continue
                missing_plex = [p for p in parts if not _in_plex_part(p)]
                missing_radarr = [p for p in parts if p.get("tmdb_id") not in radarr_by_tmdb]
                rows.append({
                    "name": data.get("name") or info["name"],
                    "total": len(parts),
                    "plex_have": len(parts) - len(missing_plex),
                    "radarr_have": len(parts) - len(missing_radarr),
                    "missing_plex": missing_plex,
                    "missing_radarr": missing_radarr,
                })
                import_candidates.extend(missing_radarr)

        rows.sort(key=lambda r: (-len(r["missing_plex"]), -len(r["missing_radarr"]), r["name"]))
        if limit:
            rows = rows[:limit]

        t = Table(title=f"Collection Gaps ({len(rows)} collections shown)", box=box.ROUNDED)
        t.add_column("Collection", style="bold cyan", min_width=26)
        t.add_column("Plex", width=9, justify="right")
        t.add_column("Radarr", width=9, justify="right")
        t.add_column("Missing from Plex", style="yellow", min_width=32)
        t.add_column("Missing from Radarr", style="magenta", min_width=28)
        for row in rows:
            t.add_row(
                row["name"],
                f"{row['plex_have']}/{row['total']}",
                f"{row['radarr_have']}/{row['total']}",
                _short_list(row["missing_plex"]),
                _short_list(row["missing_radarr"]),
            )
        console.print(t)

        unique: list[dict] = []
        seen: set[int] = set()
        for movie in import_candidates:
            tmdb_id = movie.get("tmdb_id")
            if tmdb_id and tmdb_id not in seen and tmdb_id not in radarr_by_tmdb:
                seen.add(tmdb_id)
                unique.append(movie)

        if unique:
            console.print(f"[yellow]{len(unique)} missing movie(s) are not monitored in Radarr.[/yellow]")
            if do_import:
                self._radarr_import_workflow(unique, rc, dry_run=dry_run,
                                             profile_id=None, search=False)
            else:
                console.print("[dim]Run with --import to add those Radarr gaps, or --import --dry-run to preview.[/dim]")
        else:
            console.print("[green]No Radarr collection gaps found.[/green]")

    # ── Tab completion ────────────────────────────────────────────────────────

    def _cached_libs(self) -> list[dict]:
        if not hasattr(self, "_c_lib_data"):
            try: self._c_lib_data = self.client.libraries()
            except Exception: self._c_lib_data = []
        return self._c_lib_data

    def _cached_playlists(self) -> list[dict]:
        if not hasattr(self, "_c_playlist_data"):
            try: self._c_playlist_data = self.client.get_playlists()
            except Exception: self._c_playlist_data = []
        return self._c_playlist_data

    def _cached_clients(self) -> list[dict]:
        if not hasattr(self, "_c_client_data"):
            try: self._c_client_data = self.client.clients()
            except Exception: self._c_client_data = []
        return self._c_client_data

    def _c_libs(self, text: str) -> list[str]:
        return [v for lib in self._cached_libs() for v in (lib.get("key",""), lib.get("title",""))
                if v and v.lower().startswith(text.lower())]

    def _c_flags(self, text: str, flags: list[str]) -> list[str]:
        return [f for f in flags if f.startswith(text)]

    def _prev(self, line: str, begidx: int) -> str:
        tokens = line[:begidx].split()
        return tokens[-1] if tokens else ""

    def _c_lib_arg(self, text, line, begidx, endidx):   return self._c_libs(text)
    def _c_lib_second(self, text, line, begidx, endidx):
        return self._c_libs(text) if len(line[:begidx].split()) >= 2 else []
    def _c_lib_flag(self, text, line, begidx, endidx):
        prev = self._prev(line, begidx)
        if prev == "--library": return self._c_libs(text)
        return self._c_flags(text, ["--library"]) if text.startswith("-") else []

    complete_browse = complete_unwatched = complete_toprated = complete_bitrate = _c_lib_arg
    complete_subtitles = complete_hdr = complete_multiversion = complete_genres = _c_lib_arg
    complete_studios = complete_collections = complete_popularity = _c_lib_arg
    complete_fixtitles = complete_stale = _c_lib_arg
    complete_missing_episodes = complete_incomplete_seasons = _c_lib_arg
    complete_duration_outliers = complete_4k_audit = complete_decade = complete_content_rating = _c_lib_arg
    complete_framerate = complete_director_stats = complete_actor_stats = complete_recommendations = _c_lib_arg
    complete_rewatched = complete_show_progress = complete_aspect_ratio = complete_audio_languages = _c_lib_arg
    complete_zero_duration = complete_added_trend = complete_resolution_trend = _c_lib_arg
    complete_container_format = complete_size_by_codec = complete_channel_dist = _c_lib_arg
    complete_binge_candidates = complete_overdue = complete_watched_by_decade = _c_lib_arg
    complete_bygenre = complete_byactor = complete_bydirector = complete_byyear = _c_lib_second

    _CONTENT_RATINGS = (
        "G", "PG", "PG-13", "R", "NC-17", "NR",
        "TV-Y", "TV-Y7", "TV-G", "TV-PG", "TV-14", "TV-MA",
    )

    def complete_bycontentrating(self, text, line, begidx, *_):
        tokens = line[:begidx].split()
        if len(tokens) == 1:
            return [r for r in self._CONTENT_RATINGS if r.lower().startswith(text.lower())]
        if len(tokens) >= 2:
            return self._c_libs(text)
        return []

    _RESOLUTIONS = ("4K", "1080p", "720p", "SD")

    def complete_byresolution(self, text, line, begidx, *_):
        tokens = line[:begidx].split()
        if len(tokens) == 1:
            return [r for r in self._RESOLUTIONS if r.lower().startswith(text.lower())]
        if len(tokens) >= 2:
            return self._c_libs(text)
        return []
    complete_largest = complete_smallest = complete_longest = complete_shortest = _c_lib_flag
    complete_tvlargest = complete_tvsmallest = complete_analyze = complete_abandoned = _c_lib_flag

    _TV_STORAGE_FLAGS = ["--library", "--count"]

    def complete_tv_storage(self, text, line, begidx, endidx):
        prev = self._prev(line, begidx)
        if prev == "--library":
            return self._c_libs(text)
        if prev == "--count":
            return []
        return self._c_flags(text, self._TV_STORAGE_FLAGS) if text.startswith("-") else []

    def _cached_radarr_lists(self) -> list[str]:
        if not hasattr(self, "_c_radarr_lists"):
            self._c_radarr_lists = sorted(load_lists().keys())
        return self._c_radarr_lists

    def complete_radarr_list(self, text, line, begidx, endidx):
        return [n for n in self._cached_radarr_lists() if n.startswith(text)]

    def complete_radarr_import(self, text, line, begidx, endidx):
        tokens = line[:begidx].split()
        if len(tokens) == 1:
            return [n for n in self._cached_radarr_lists() if n.startswith(text)]
        if text.startswith("-"):
            return self._c_flags(text, ["--dry-run", "--profile", "--search"])
        return []

    def complete_radarr_download(self, text, line, begidx, endidx):
        tokens = line[:begidx].split()
        if len(tokens) == 1:
            return [n for n in self._cached_radarr_lists() if n.startswith(text)]
        if text.startswith("-"):
            return self._c_flags(text, ["--dry-run", "--profile"])
        return []

    _PB_FLAGS = ["--genre", "--director", "--actor", "--year", "--decade", "--studio",
                 "--contentrating", "--unwatched", "--rating", "--library", "--limit"]
    _PM_FLAGS  = ["--library", "--fix", "--episodes"]
    _PD_FLAGS  = ["--library"]
    _RC_FLAGS  = ["--import"]
    _CG_FLAGS  = ["--import", "--dry-run", "--limit"]
    _WN_FLAGS  = ["--count", "--library", "--runtime", "--genre", "--type"]
    _QUP_FLAGS = ["--limit", "--movies", "--shows"]
    _PW_FLAGS  = ["--days", "--past", "--missing-only"]
    _CMP_FLAGS = ["--library", "--count", "--min-size", "--target"]

    def complete_plex_metadata(self, text, line, begidx, endidx):
        prev = self._prev(line, begidx)
        if prev == "--library": return self._c_libs(text)
        if text.startswith("-"): return self._c_flags(text, self._PM_FLAGS)
        return []

    def complete_plex_duplicates(self, text, line, begidx, endidx):
        prev = self._prev(line, begidx)
        if prev == "--library": return self._c_libs(text)
        if text.startswith("-"): return self._c_flags(text, self._PD_FLAGS)
        return []

    def complete_radarr_collections(self, text, line, begidx, endidx):
        if text.startswith("-"): return self._c_flags(text, self._RC_FLAGS)
        return []

    def complete_collection_gaps(self, text, line, begidx, endidx):
        if text.startswith("-"): return self._c_flags(text, self._CG_FLAGS)
        return []

    def complete_quality_upgrade_plan(self, text, line, begidx, endidx):
        if text.startswith("-"): return self._c_flags(text, self._QUP_FLAGS)
        return []

    def complete_premiere_watchlist(self, text, line, begidx, endidx):
        if text.startswith("-"): return self._c_flags(text, self._PW_FLAGS)
        return []

    def complete_radarr_calendar(self, text, line, begidx, endidx):
        if text.startswith("-"): return self._c_flags(text, ["--days"])
        return []

    def complete_watch_next(self, text, line, begidx, endidx):
        prev = self._prev(line, begidx)
        if prev == "--library": return self._c_libs(text)
        if prev == "--type":
            return [v for v in ("movie", "episode") if v.startswith(text.lower())]
        if text.startswith("-"): return self._c_flags(text, self._WN_FLAGS)
        return []

    def complete_codec_migration_plan(self, text, line, begidx, endidx):
        prev = self._prev(line, begidx)
        if prev == "--library": return self._c_libs(text)
        if prev == "--target":
            return [v for v in ("hevc", "av1") if v.startswith(text.lower())]
        if text.startswith("-"): return self._c_flags(text, self._CMP_FLAGS)
        return []

    def complete_hubs(self, text, line, begidx, endidx):
        prev = self._prev(line, begidx)
        if prev == "--library": return self._c_libs(text)
        if text.startswith("-"): return self._c_flags(text, ["--library", "--count"])
        return []

    def complete_playlist_build(self, text, line, begidx, endidx):
        prev = self._prev(line, begidx)
        if prev == "--library": return self._c_libs(text)
        if prev in ("--genre", "--director", "--actor", "--year", "--decade",
                    "--studio", "--contentrating", "--rating", "--limit"): return []
        if text.startswith("-"): return self._c_flags(text, self._PB_FLAGS)
        return []

    def complete_radarr_pick(self, text, line, begidx, endidx):
        tokens = line[:begidx].split()
        if len(tokens) == 1:
            return [n for n in self._cached_radarr_lists() if n.startswith(text)]
        if text.startswith("-"):
            return self._c_flags(text, ["--profile"])
        return []

    # sonarr_status, sonarr_sync, sonarr_missing, sonarr_upgrade take no args
    complete_sonarr_status = complete_sonarr_sync = complete_sonarr_missing = \
        complete_sonarr_upgrade = lambda self, text, *_: []

    # tautulli_status and tautulli_users take no args
    complete_tautulli_status = complete_tautulli_users = lambda self, text, *_: []

    _TH_FLAGS  = ["--user", "--count"]
    _TSD_FLAGS = ["--days"]

    def complete_tautulli_history(self, text, line, begidx, endidx):
        if text.startswith("-"): return self._c_flags(text, self._TH_FLAGS)
        return []

    def complete_tautulli_stats(self, text, line, begidx, endidx):
        if text.startswith("-"): return self._c_flags(text, self._TSD_FLAGS)
        return []

    complete_tautulli_plays = complete_tautulli_stats

    _SR_FLAGS = ["--count", "--min-rating"]

    def complete_smart_recommendations(self, text, line, begidx, endidx):
        if text.startswith("-"): return self._c_flags(text, self._SR_FLAGS)
        return []

    def complete_trending_in_library(self, text, line, begidx, endidx):
        prev = self._prev(line, begidx)
        if prev == "--window": return [w for w in ("day", "week") if w.startswith(text)]
        if text.startswith("-"): return self._c_flags(text, ["--window"])
        return []

    # director_deep_dive / actor_deep_dive take a free-text name — no useful tab completion

    def complete_refresh(self, text, line, begidx, *_):
        tokens = line[:begidx].split()
        if len(tokens) == 1:
            return self._c_libs(text)
        if text.startswith("-"):
            return self._c_flags(text, ["--force"])
        return []

    def complete_export(self, text, line, begidx, endidx):
        return self._c_libs(text) if len(line[:begidx].split()) == 1 else []

    _SEARCH_FLAGS_LIST = ["--actor","--director","--genre","--studio","--year","--library","--type","--title"]

    def complete_search(self, text, line, begidx, endidx):
        prev = self._prev(line, begidx)
        if prev == "--type":
            return [v for v in ("movie","show","episode","artist","album","track") if v.startswith(text)]
        if prev == "--library": return self._c_libs(text)
        if prev in self._SEARCH_FLAGS_LIST: return []
        return self._c_flags(text, self._SEARCH_FLAGS_LIST) if text.startswith("-") else []

    def complete_logs(self, text, line, begidx, endidx):
        prev = self._prev(line, begidx)
        if prev == "--level":
            return [v for v in ("debug","info","warn","error") if v.startswith(text)]
        return self._c_flags(text, ["--level"]) if text.startswith("-") else []

    def complete_duplicates_smart(self, text, line, begidx, endidx):
        prev = self._prev(line, begidx)
        if prev == "--library": return self._c_libs(text)
        return self._c_flags(text, ["--tolerance","--match-name","--library"]) if text.startswith("-") else []

    def complete_play(self, text, line, begidx, endidx):
        prev = self._prev(line, begidx)
        if prev == "--client":
            return [c.get("name","") for c in self._cached_clients() if c.get("name","").lower().startswith(text.lower())]
        return self._c_flags(text, ["--client"]) if text.startswith("-") else []

    def complete_bycodec(self, text, line, begidx, endidx):
        return [c for c in self._ALL_CODECS if c.startswith(text.lower())]
    def complete_audioformat(self, text, line, begidx, endidx):
        return [c for c in self._AUDIO_FORMATS if c.startswith(text.lower())]

    def complete_playlist(self, text, line, begidx, endidx):
        return [p.get("ratingKey","") for p in self._cached_playlists() if p.get("ratingKey","").startswith(text)]
    def complete_playlist_add(self, text, line, begidx, endidx):
        return [p.get("ratingKey","") for p in self._cached_playlists()
                if p.get("ratingKey","").startswith(text)] if len(line[:begidx].split()) == 1 else []
    complete_playlist_remove = complete_playlist_add

# ── Entry point ───────────────────────────────────────────────────────────────

def get_base_url(cfg: dict) -> str:
    if cfg.get("plex_url"):
        return cfg["plex_url"]
    url = os.environ.get("PLEX_URL", "")
    if url:
        return url
    console.print(Panel(
        "No Plex server URL found.\n\n"
        "Example: [bold]http://192.168.1.100:32400[/bold] or [bold]http://myserver.local:32400[/bold]",
        title="[yellow]Setup Required[/yellow]", border_style="yellow"))
    url = Prompt.ask("[yellow]Enter your Plex server URL[/yellow]", default="http://localhost:32400")
    url = url.rstrip("/")
    if url:
        cfg["plex_url"] = url
        save_config(cfg)
        console.print(f"[green]URL saved to {CONFIG_FILE}[/green]")
    return url

def get_token(cfg: dict) -> str:
    if cfg.get("token"):
        return cfg["token"]
    token = os.environ.get("PLEX_TOKEN", "")
    if token:
        return token
    console.print(Panel(
        "No Plex token found.\n\n"
        "Find yours: Sign in to Plex Web → open any media → Get Info → View XML\n"
        "Look for [bold]X-Plex-Token[/bold] in the URL.",
        title="[yellow]Setup Required[/yellow]", border_style="yellow"))
    token = Prompt.ask("[yellow]Enter your Plex token[/yellow]")
    if token:
        cfg["token"] = token
        save_config(cfg)
        console.print(f"[green]Token saved to {CONFIG_FILE}[/green]")
    return token

def main():
    one_shot = sys.argv[1:]   # command + args passed on the CLI, if any

    cfg      = load_config()
    base_url = get_base_url(cfg)
    if not base_url:
        console.print("[red]No server URL provided. Exiting.[/red]"); sys.exit(1)

    if not one_shot:
        console.print(Panel("[bold white]Plex Media Server CLI[/bold white]\n"
                            f"[dim]Connecting to {base_url}[/dim]", border_style="cyan", expand=False))

    token = get_token(cfg)
    if not token:
        console.print("[red]No token provided. Exiting.[/red]"); sys.exit(1)

    client = PlexClient(token, base_url)
    with console.status("Connecting..."):
        info = client.server_info()

    if not one_shot:
        if info:
            console.print(f"[green]Connected to[/green] [bold]{info.get('friendlyName','Plex Server')}[/bold] "
                          f"[dim]v{info.get('version','')}[/dim]")
        else:
            console.print(f"[yellow]Could not reach {base_url} — commands may fail.[/yellow]")
        console.print("[dim]Type [bold]help[/bold] for available commands.[/dim]\n")

    shell = PlexShell(client)

    if one_shot:
        shell.onecmd(" ".join(one_shot))
    else:
        try:
            shell.cmdloop()
        except KeyboardInterrupt:
            console.print("\n[dim]Interrupted. Goodbye.[/dim]")

if __name__ == "__main__":
    main()
