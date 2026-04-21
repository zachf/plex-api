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
from datetime import datetime
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

BASE_URL = "http://opus2.local:32400"
CONFIG_FILE = Path.home() / ".plex_cli.json"
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
    return f"{r:.1f}" if r else "—"

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
    def __init__(self, token: str):
        self.token = token
        self.session = requests.Session()
        self.session.headers.update(PLEX_HEADERS)
        self.session.params = {"X-Plex-Token": token}  # type: ignore

    def _request(self, method: str, path: str, silent: bool = False, **params):
        """Generic request; returns requests.Response or None on error."""
        url = f"{BASE_URL}{path}"
        try:
            r = self.session.request(method, url, params=params, timeout=15)
            r.raise_for_status()
            return r
        except requests.exceptions.ConnectionError:
            if not silent:
                console.print(f"[red]Cannot reach {BASE_URL}[/red]")
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
        url = f"{BASE_URL}{path}"
        headers = {"Accept": "text/plain, */*"}
        try:
            r = self.session.get(url, params=params, headers=headers, timeout=15)
            r.raise_for_status()
            return r.text
        except requests.exceptions.ConnectionError:
            if not silent:
                console.print(f"[red]Cannot reach {BASE_URL}[/red]")
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
            attempts.append((f"{BASE_URL}/player/playback/{command}",
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
        host = BASE_URL.split("://")[-1]
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
    def on_deck(self) -> list:             return self._mc("/library/onDeck")
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
        pct = int(offset / dur * 100)
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
        ("watch_calendar",  "[days]",                            "Day-by-day view of what was watched (default 7)"),
        ("recommendations", "[library_id]",                      "Highly rated unwatched content (≥7.5)"),
        ("rewatched",       "[library_id]",                      "Titles played more than once, ranked by play count"),
        ("show_progress",   "[library_id]",                      "Every TV show with watched %, episode counts, last watched"),
        ("added_trend",     "[months]",                          "Items added per month — library growth over time (default 12)"),
    ]),
    ("Storage", [
        ("largest",         "[count] [--library name]",           "Titles with the biggest file sizes"),
        ("smallest",        "[count] [--library name]",           "Titles with the smallest file sizes"),
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
            f"[bold cyan]URL:[/bold cyan] {BASE_URL}",
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
        console.print(f"[cyan]{BASE_URL}/library/metadata/{arg.strip()}/stream?X-Plex-Token={self.client.token}[/cyan]")

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

    def _show_size_table(self, count: int, largest: bool, library_filter: str = ""):
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
                    key = ep.get("grandparentRatingKey", "")
                    rec = show_data.setdefault(show, {
                        "title": show, "ratingKey": key,
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

        rows = sorted(show_data.values(), key=lambda x: x["size"], reverse=largest)[:count]
        title = f"{label} {count} TV Shows by Disk Usage" + (f" — {library_filter}" if library_filter else "")
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
            pct = int(a.get("progress",0))
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

def get_token() -> str:
    cfg = load_config()
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

    if not one_shot:
        console.print(Panel("[bold white]Plex Media Server CLI[/bold white]\n"
                            f"[dim]Connecting to {BASE_URL}[/dim]", border_style="cyan", expand=False))

    token = get_token()
    if not token:
        console.print("[red]No token provided. Exiting.[/red]"); sys.exit(1)

    client = PlexClient(token)
    with console.status("Connecting..."):
        info = client.server_info()

    if not one_shot:
        if info:
            console.print(f"[green]Connected to[/green] [bold]{info.get('friendlyName','Plex Server')}[/bold] "
                          f"[dim]v{info.get('version','')}[/dim]")
        else:
            console.print(f"[yellow]Could not reach {BASE_URL} — commands may fail.[/yellow]")
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
