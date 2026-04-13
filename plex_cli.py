#!/usr/bin/env python3
# Run with: py -3.10 plex_cli.py  (or any Python with requests+rich installed)
"""Interactive Plex Media Server CLI — opus2.local:32400"""

import cmd
import csv
import json
import os
import shlex
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from rich import box
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt

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

def format_duration(ms: Optional[int]) -> str:
    if ms is None:
        return "—"
    s = ms // 1000
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"

def format_size(bytes_: Optional[int]) -> str:
    if bytes_ is None:
        return "—"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if bytes_ < 1024:
            return f"{bytes_:.1f} {unit}"
        bytes_ /= 1024
    return f"{bytes_:.1f} PB"

def format_ts(ts: Optional[int]) -> str:
    if not ts:
        return "—"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")

def year(item: dict) -> str:
    return str(item.get("year", "")) or "—"

def rating(item: dict) -> str:
    r = item.get("rating") or item.get("audienceRating")
    return f"{r:.1f}" if r else "—"

SEARCH_FLAGS = {"--actor", "--director", "--genre", "--studio", "--year", "--library", "--type"}

def parse_search_args(arg: str) -> tuple:
    """Parse search args into (query, filters). Supports --flag value pairs."""
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
            if i + 1 < len(tokens):
                filters[t.lstrip("-")] = tokens[i + 1]
                i += 2
            else:
                i += 1
        else:
            query_parts.append(t)
            i += 1
    return " ".join(query_parts), filters

def resolution_label(res: Optional[str]) -> str:
    if not res:
        return "Unknown"
    res = str(res).lower()
    if res in ("4k", "2160"):
        return "4K"
    if res == "1080":
        return "1080p"
    if res == "720":
        return "720p"
    if res in ("480", "576"):
        return "SD"
    return res.upper()

def months_ago(ts: Optional[int]) -> str:
    if not ts:
        return "—"
    delta = datetime.now() - datetime.fromtimestamp(ts)
    months = delta.days // 30
    if months == 0:
        return f"{delta.days}d ago"
    if months < 12:
        return f"{months}mo ago"
    return f"{months // 12}yr {months % 12}mo ago"

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
            })
    return rows

# ── API client ────────────────────────────────────────────────────────────────

class PlexClient:
    def __init__(self, token: str):
        self.token = token
        self.session = requests.Session()
        self.session.headers.update(PLEX_HEADERS)
        self.session.params = {"X-Plex-Token": token}  # type: ignore

    def get(self, path: str, **params) -> dict:
        url = f"{BASE_URL}{path}"
        try:
            r = self.session.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.ConnectionError:
            console.print(f"[red]Cannot reach {BASE_URL}[/red]")
            return {}
        except requests.exceptions.HTTPError as e:
            console.print(f"[red]HTTP {e.response.status_code}:[/red] {path}")
            return {}
        except requests.exceptions.JSONDecodeError:
            console.print("[red]Server returned non-JSON response[/red]")
            return {}

    def server_info(self) -> dict:
        data = self.get("/")
        return data.get("MediaContainer", {})

    def libraries(self) -> list:
        data = self.get("/library/sections")
        return data.get("MediaContainer", {}).get("Directory", [])

    def library_contents(self, section_id: str, sort: str = "titleSort") -> list:
        data = self.get(f"/library/sections/{section_id}/all", sort=sort)
        return data.get("MediaContainer", {}).get("Metadata", [])

    def search(self, query: str) -> list:
        data = self.get("/search", query=query)
        return data.get("MediaContainer", {}).get("Metadata", [])

    def section_search(self, section_id: str, query: str = "", **filters) -> list:
        params = {k: v for k, v in filters.items() if v}
        if query:
            params["query"] = query
            data = self.get(f"/library/sections/{section_id}/search", **params)
        else:
            # /search requires a query string; fall back to /all for simple field filters
            data = self.get(f"/library/sections/{section_id}/all", **params)
        return data.get("MediaContainer", {}).get("Metadata", [])

    def sessions(self) -> list:
        data = self.get("/status/sessions")
        return data.get("MediaContainer", {}).get("Metadata", [])

    def recent(self, count: int = 20) -> list:
        data = self.get("/library/recentlyAdded", **{"X-Plex-Container-Size": count})
        return data.get("MediaContainer", {}).get("Metadata", [])

    def metadata(self, rating_key: str) -> dict:
        data = self.get(f"/library/metadata/{rating_key}")
        items = data.get("MediaContainer", {}).get("Metadata", [])
        return items[0] if items else {}

    def on_deck(self) -> list:
        data = self.get("/library/onDeck")
        return data.get("MediaContainer", {}).get("Metadata", [])

    def children(self, rating_key: str) -> list:
        data = self.get(f"/library/metadata/{rating_key}/children")
        return data.get("MediaContainer", {}).get("Metadata", [])

    def duplicates(self, section_id: str) -> list:
        data = self.get(f"/library/sections/{section_id}/duplicates")
        return data.get("MediaContainer", {}).get("Metadata", [])

    def history(self, count: int = 50, account_id: Optional[int] = None) -> list:
        params: dict = {"sort": "viewedAt:desc", "X-Plex-Container-Size": count}
        if account_id:
            params["accountID"] = account_id
        data = self.get("/status/sessions/history/all", **params)
        return data.get("MediaContainer", {}).get("Metadata", [])

    def accounts(self) -> list:
        data = self.get("/accounts")
        return data.get("MediaContainer", {}).get("Account", [])

    def all_media_rows(self) -> list:
        """Fetch all items across all libraries and flatten to media rows."""
        rows = []
        for lib in self.libraries():
            lid = lib.get("key", "")
            lib_title = lib.get("title", lid)
            items = self.library_contents(lid)
            for item in items:
                rows.extend(get_media_rows(item, lib_title))
        return rows

    def all_items_by_library(self) -> dict:
        """Returns {lib: {"info": lib_dict, "items": [...]}} for all libraries."""
        result = {}
        for lib in self.libraries():
            lid = lib.get("key", "")
            result[lib.get("title", lid)] = {
                "info": lib,
                "items": self.library_contents(lid),
            }
        return result

# ── Display helpers ───────────────────────────────────────────────────────────

def print_libraries(libs: list):
    t = Table(title="Libraries", box=box.ROUNDED)
    t.add_column("ID", style="dim", width=4)
    t.add_column("Name", style="bold cyan")
    t.add_column("Type", style="yellow")
    t.add_column("Items", justify="right")
    for lib in libs:
        t.add_row(
            lib.get("key", ""),
            lib.get("title", ""),
            lib.get("type", ""),
            str(lib.get("count", "?")),
        )
    console.print(t)

def print_media_table(items: list, title: str = "Results"):
    if not items:
        console.print("[yellow]No results.[/yellow]")
        return
    t = Table(title=title, box=box.ROUNDED, show_lines=False)
    t.add_column("Key", style="dim", width=7)
    t.add_column("Title", style="bold white", min_width=30)
    t.add_column("Type", style="yellow", width=10)
    t.add_column("Year", width=6, justify="right")
    t.add_column("Rating", width=7, justify="right")
    t.add_column("Duration", width=9, justify="right")
    for item in items:
        title_str = item.get("title", "")
        grandparent = item.get("grandparentTitle", "")
        parent = item.get("parentTitle", "")
        if grandparent and parent:
            full_title = f"[dim]{grandparent} — {parent} —[/dim] {title_str}"
        elif parent:
            full_title = f"[dim]{parent} —[/dim] {title_str}"
        else:
            full_title = title_str
        t.add_row(
            item.get("ratingKey", ""),
            full_title,
            item.get("type", ""),
            year(item),
            rating(item),
            format_duration(item.get("duration")),
        )
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
        user = s.get("User", {}).get("title", "Unknown")
        player = s.get("Player", {}).get("title", "Unknown")
        state = s.get("Player", {}).get("state", "unknown")
        title = s.get("title", "")
        parent = s.get("grandparentTitle", "")
        full_title = f"{parent} — {title}" if parent else title
        offset = s.get("viewOffset", 0)
        duration = s.get("duration", 0) or 1
        pct = int(offset / duration * 100)
        bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
        state_color = {"playing": "green", "paused": "yellow", "buffering": "magenta"}.get(state, "white")
        ts = s.get("TranscodeSession")
        stream_type = "[red]transcode[/red]" if ts else "[green]direct[/green]"
        t.add_row(
            user,
            full_title,
            player,
            f"[{state_color}]{state}[/{state_color}]",
            f"[dim]{bar}[/dim] {pct}%",
            stream_type,
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
    title = item.get("title", "Unknown")
    itype = item.get("type", "")
    synopsis = item.get("summary", "No description available.")
    meta_lines = [
        f"[bold cyan]Type:[/bold cyan] {itype}",
        f"[bold cyan]Year:[/bold cyan] {year(item)}",
        f"[bold cyan]Rating:[/bold cyan] {rating(item)}",
        f"[bold cyan]Duration:[/bold cyan] {format_duration(item.get('duration'))}",
        f"[bold cyan]Added:[/bold cyan] {format_ts(item.get('addedAt'))}",
    ]
    if item.get("studio"):
        meta_lines.append(f"[bold cyan]Studio:[/bold cyan] {item['studio']}")
    if item.get("contentRating"):
        meta_lines.append(f"[bold cyan]Content Rating:[/bold cyan] {item['contentRating']}")
    genres = ", ".join(g["tag"] for g in item.get("Genre", []))
    if genres:
        meta_lines.append(f"[bold cyan]Genres:[/bold cyan] {genres}")
    directors = ", ".join(d["tag"] for d in item.get("Director", []))
    if directors:
        meta_lines.append(f"[bold cyan]Director:[/bold cyan] {directors}")
    actors = ", ".join(a["tag"] for a in item.get("Role", [])[:5])
    if actors:
        meta_lines.append(f"[bold cyan]Cast:[/bold cyan] {actors}")
    for media in item.get("Media", []):
        for part in media.get("Part", []):
            meta_lines.append(f"[bold cyan]File:[/bold cyan] [dim]{part.get('file', '—')}[/dim]")
            meta_lines.append(f"[bold cyan]Size:[/bold cyan] {format_size(part.get('size'))}")
        meta_lines.append(
            f"[bold cyan]Video:[/bold cyan] {media.get('videoCodec','?').upper()} "
            f"{media.get('videoResolution','?')}p  "
            f"[bold cyan]Audio:[/bold cyan] {media.get('audioCodec','?').upper()} "
            f"{media.get('audioChannels','?')}ch"
        )
    console.print(Panel(
        "\n".join(meta_lines) + f"\n\n[italic dim]{synopsis}[/italic dim]",
        title=f"[bold white]{title}[/bold white]",
        border_style="cyan",
    ))

# ── CLI ───────────────────────────────────────────────────────────────────────

HELP_TEXT = """
[bold cyan]Basic commands:[/bold cyan]
  [yellow]status[/yellow]                    Server info and version
  [yellow]libraries[/yellow]                 List all media libraries
  [yellow]browse[/yellow] [dim]<id>[/dim]             Browse a library by ID
  [yellow]search[/yellow] [dim][query] [--actor name] [--director name] [--genre name] [--studio name] [--year YYYY] [--library id] [--type movie|show|episode][/dim]
  [yellow]info[/yellow] [dim]<key>[/dim]              Show detailed info for an item
  [yellow]sessions[/yellow]                  Active playback sessions
  [yellow]recent[/yellow] [dim][count][/dim]          Recently added content
  [yellow]ondeck[/yellow]                    Continue watching (on deck)
  [yellow]children[/yellow] [dim]<key>[/dim]          List seasons/episodes for a show
  [yellow]token[/yellow] [dim]<token>[/dim]           Set or update your Plex token
  [yellow]url[/yellow] [dim]<key>[/dim]               Print stream URL for an item

[bold cyan]Library health:[/bold cyan]
  [yellow]dupes[/yellow]                     Find duplicate titles across libraries
  [yellow]missing[/yellow]                   Items with incomplete metadata
  [yellow]quality[/yellow]                   Resolution breakdown per library
  [yellow]orphans[/yellow]                   Items with no associated media files

[bold cyan]Watch statistics:[/bold cyan]
  [yellow]stats[/yellow]                     Library totals and watch history summary
  [yellow]history[/yellow] [dim][user] [count][/dim]  Recent watch history
  [yellow]unwatched[/yellow] [dim][library_id][/dim]  Content never played
  [yellow]toprated[/yellow] [dim][library_id][/dim]   Highest-rated items
  [yellow]recently_played[/yellow] [dim][count][/dim] Most recently watched

[bold cyan]Storage analysis:[/bold cyan]
  [yellow]storage[/yellow]                   Disk usage breakdown by library
  [yellow]codecs[/yellow]                    Video/audio codec distribution
  [yellow]transcode[/yellow]                 Items likely to require transcoding

[bold cyan]Collection tools:[/bold cyan]
  [yellow]export[/yellow] [dim]<library_id> [file][/dim]  Export library to CSV or JSON
  [yellow]stale[/yellow] [dim][months][/dim]          Shows with no updates in N months

[bold cyan]Monitoring:[/bold cyan]
  [yellow]watch[/yellow] [dim][seconds][/dim]          Live-refresh sessions (Ctrl+C to stop)
  [yellow]alert[/yellow] [dim][seconds][/dim]          Alert when a transcode session starts

  [yellow]help[/yellow]  [yellow]quit[/yellow] / [yellow]exit[/yellow]
"""

class PlexShell(cmd.Cmd):
    intro = ""
    prompt = "[plex]> "
    ruler = ""

    def __init__(self, client: PlexClient):
        super().__init__()
        self.client = client

    def emptyline(self):
        pass

    def default(self, line: str):
        console.print(f"[red]Unknown command:[/red] {line}  (type [yellow]help[/yellow])")

    def do_help(self, _):
        console.print(HELP_TEXT)

    def do_quit(self, _):
        console.print("[dim]Goodbye.[/dim]")
        return True

    def do_exit(self, arg):
        return self.do_quit(arg)

    def do_EOF(self, _):
        console.print()
        return self.do_quit(_)

    # ── Basic commands ────────────────────────────────────────────────────────

    def do_status(self, _):
        info = self.client.server_info()
        if not info:
            return
        console.print(Panel(
            f"[bold cyan]Server:[/bold cyan] {info.get('friendlyName', 'Unknown')}\n"
            f"[bold cyan]Version:[/bold cyan] {info.get('version', '—')}\n"
            f"[bold cyan]Platform:[/bold cyan] {info.get('platform', '—')} {info.get('platformVersion', '')}\n"
            f"[bold cyan]My Plex:[/bold cyan] {'✓' if info.get('myPlex') else '✗'}\n"
            f"[bold cyan]URL:[/bold cyan] {BASE_URL}",
            title="[bold white]Plex Media Server[/bold white]",
            border_style="green",
        ))

    def do_libraries(self, _):
        libs = self.client.libraries()
        if libs:
            print_libraries(libs)

    def do_browse(self, arg: str):
        """browse <id>"""
        if not arg.strip():
            libs = self.client.libraries()
            if libs:
                print_libraries(libs)
                console.print("[dim]Usage: browse <id>[/dim]")
            return
        items = self.client.library_contents(arg.strip())
        print_media_table(items, f"Library {arg.strip()}")

    def do_search(self, arg: str):
        """search [query] [--actor name] [--director name] [--genre name] [--studio name] [--year YYYY] [--library id] [--type movie|show|episode]"""
        if not arg.strip():
            console.print(
                "[yellow]Usage:[/yellow] search [dim][query][/dim] "
                "[dim][--actor name] [--director name] [--genre name] "
                "[--studio name] [--year YYYY] [--library id] [--type movie|show|episode][/dim]"
            )
            return

        query, filters = parse_search_args(arg.strip())
        section_id = filters.pop("library", None)
        type_filter = filters.pop("type", None)

        label_parts = [f'"{query}"'] if query else []
        label_parts += [f"--{k} {v}" for k, v in filters.items()]
        if type_filter:
            label_parts.append(f"--type {type_filter}")
        label = "Search: " + " ".join(label_parts)

        tag_filters = {"actor", "director", "genre"}
        if not query and filters.keys() & tag_filters:
            console.print(
                f"[yellow]--actor, --director, and --genre require a title query to work "
                f"(e.g. search breaking --actor 'Bryan Cranston')[/yellow]"
            )
            return

        if not filters and not section_id:
            # Simple global title search
            if not query:
                console.print("[yellow]Provide a query or at least one filter flag.[/yellow]")
                return
            with console.status(f"Searching [cyan]{query}[/cyan]..."):
                results = self.client.search(query)
        elif section_id:
            with console.status(f"Searching library {section_id}..."):
                results = self.client.section_search(section_id, query, **filters)
        else:
            # Filter search across all libraries
            with console.status("Searching all libraries..."):
                results = []
                for lib in self.client.libraries():
                    results.extend(self.client.section_search(lib.get("key", ""), query, **filters))

        if type_filter:
            results = [r for r in results if r.get("type", "").lower() == type_filter.lower()]

        print_media_table(results, label)

    def do_info(self, arg: str):
        """info <key>"""
        if not arg.strip():
            console.print("[yellow]Usage: info <key>[/yellow]")
            return
        with console.status("Fetching..."):
            item = self.client.metadata(arg.strip())
        print_item_detail(item)

    def do_sessions(self, _):
        print_sessions(self.client.sessions())

    def do_recent(self, arg: str):
        """recent [count]"""
        count = int(arg.strip()) if arg.strip().isdigit() else 20
        with console.status("Fetching recently added..."):
            items = self.client.recent(count)
        print_media_table(items, f"Recently Added (last {count})")

    def do_ondeck(self, _):
        with console.status("Fetching on deck..."):
            items = self.client.on_deck()
        print_media_table(items, "On Deck")

    def do_children(self, arg: str):
        """children <key>"""
        if not arg.strip():
            console.print("[yellow]Usage: children <key>[/yellow]")
            return
        with console.status("Fetching..."):
            items = self.client.children(arg.strip())
        print_media_table(items, f"Children of {arg.strip()}")

    def do_url(self, arg: str):
        """url <key>"""
        if not arg.strip():
            console.print("[yellow]Usage: url <key>[/yellow]")
            return
        console.print(f"[cyan]{BASE_URL}/library/metadata/{arg.strip()}/stream?X-Plex-Token={self.client.token}[/cyan]")

    def do_token(self, arg: str):
        """token <token>"""
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

    def do_dupes(self, _):
        """Find duplicate titles in each library."""
        libs = self.client.libraries()
        if not libs:
            return
        found_any = False
        with console.status("Scanning for duplicates..."):
            for lib in libs:
                lid = lib.get("key", "")
                lib_title = lib.get("title", lid)
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
                    total_size = sum(
                        p.get("size", 0)
                        for m in item.get("Media", [])
                        for p in m.get("Part", [])
                    )
                    file_count = sum(len(m.get("Part", [])) for m in item.get("Media", []))
                    t.add_row(
                        item.get("ratingKey", ""),
                        item.get("title", ""),
                        year(item),
                        str(file_count),
                        format_size(total_size),
                    )
                console.print(t)
        if not found_any:
            console.print("[green]No duplicates found.[/green]")

    def do_missing(self, _):
        """Find items with incomplete metadata across all libraries."""
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
                if not item.get("summary", "").strip():
                    missing.append("summary")
                if not item.get("thumb") and not item.get("art"):
                    missing.append("poster")
                if not item.get("rating") and not item.get("audienceRating"):
                    missing.append("rating")
                if not item.get("Genre"):
                    missing.append("genres")
                if missing:
                    t.add_row(lib_title, item.get("ratingKey", ""), item.get("title", ""), ", ".join(missing))
                    count += 1

        if count == 0:
            console.print("[green]All items have complete metadata.[/green]")
        else:
            console.print(t)
            console.print(f"[yellow]{count} items with incomplete metadata.[/yellow]")

    def do_quality(self, _):
        """Resolution breakdown across all libraries."""
        with console.status("Scanning all libraries..."):
            rows = self.client.all_media_rows()

        # Per-library resolution counts
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
            total = sum(counts.values())
            t.add_row(
                lib_title,
                str(counts.get("4K", 0)) if counts.get("4K") else "[dim]—[/dim]",
                str(counts.get("1080p", 0)) if counts.get("1080p") else "[dim]—[/dim]",
                str(counts.get("720p", 0)) if counts.get("720p") else "[dim]—[/dim]",
                str(counts.get("SD", 0)) if counts.get("SD") else "[dim]—[/dim]",
                str(counts.get("Unknown", 0)) if counts.get("Unknown") else "[dim]—[/dim]",
                str(total),
            )

        grand_total = sum(total_res.values())
        t.add_section()
        t.add_row(
            "[bold]TOTAL[/bold]",
            str(total_res.get("4K", 0)),
            str(total_res.get("1080p", 0)),
            str(total_res.get("720p", 0)),
            str(total_res.get("SD", 0)),
            str(total_res.get("Unknown", 0)),
            f"[bold]{grand_total}[/bold]",
        )
        console.print(t)

    def do_orphans(self, _):
        """Find items with no associated media files."""
        with console.status("Scanning all libraries..."):
            data = self.client.all_items_by_library()

        t = Table(title="Orphaned Items (no media files)", box=box.ROUNDED)
        t.add_column("Library", style="cyan", width=16)
        t.add_column("Key", style="dim", width=7)
        t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Type", style="yellow", width=10)

        count = 0
        for lib_title, d in data.items():
            for item in d["items"]:
                has_media = any(
                    m.get("Part") for m in item.get("Media", [])
                )
                if not has_media:
                    t.add_row(lib_title, item.get("ratingKey", ""), item.get("title", ""), item.get("type", ""))
                    count += 1

        if count == 0:
            console.print("[green]No orphaned items found.[/green]")
        else:
            console.print(t)
            console.print(f"[yellow]{count} orphaned items.[/yellow]")

    # ── Watch statistics ──────────────────────────────────────────────────────

    def do_stats(self, _):
        """Library totals and watch history summary."""
        with console.status("Gathering stats..."):
            data = self.client.all_items_by_library()
            hist = self.client.history(count=500)

        # Library totals table
        t = Table(title="Library Summary", box=box.ROUNDED)
        t.add_column("Library", style="cyan")
        t.add_column("Type", style="yellow", width=8)
        t.add_column("Items", justify="right", width=7)
        t.add_column("Total Duration", justify="right", width=14)
        t.add_column("Total Size", justify="right", width=12)

        grand_items = 0
        grand_ms = 0
        grand_bytes = 0
        for lib_title, d in data.items():
            items = d["items"]
            lib_type = d["info"].get("type", "")
            total_ms = sum(i.get("duration", 0) or 0 for i in items)
            total_bytes = sum(
                p.get("size", 0) or 0
                for i in items
                for m in i.get("Media", [])
                for p in m.get("Part", [])
            )
            grand_items += len(items)
            grand_ms += total_ms
            grand_bytes += total_bytes
            t.add_row(lib_title, lib_type, str(len(items)), format_duration(total_ms), format_size(total_bytes))

        t.add_section()
        t.add_row("[bold]TOTAL[/bold]", "", f"[bold]{grand_items}[/bold]",
                  format_duration(grand_ms), format_size(grand_bytes))
        console.print(t)

        # Watch history summary
        if hist:
            total_plays = len(hist)
            user_counts: Counter = Counter()
            title_counts: Counter = Counter()
            for h in hist:
                user_counts[h.get("User", {}).get("title", "Unknown")] += 1
                title_counts[h.get("title", "?")] += 1

            console.print(Panel(
                f"[bold cyan]Total plays in history:[/bold cyan] {total_plays}\n\n"
                "[bold cyan]Most active users:[/bold cyan]\n" +
                "\n".join(f"  {u}: {c}" for u, c in user_counts.most_common(5)) +
                "\n\n[bold cyan]Most played titles:[/bold cyan]\n" +
                "\n".join(f"  {t_}: {c}" for t_, c in title_counts.most_common(5)),
                title="[bold white]Watch History[/bold white]",
                border_style="magenta",
            ))
        else:
            console.print("[dim]Watch history unavailable (may require Plex Pass).[/dim]")

    def do_history(self, arg: str):
        """history [username] [count]"""
        parts = arg.strip().split()
        count = 50
        username_filter = None

        for p in parts:
            if p.isdigit():
                count = int(p)
            else:
                username_filter = p.lower()

        with console.status("Fetching history..."):
            records = self.client.history(count=count)

        if not records:
            console.print("[yellow]No history available (may require Plex Pass).[/yellow]")
            return

        if username_filter:
            records = [r for r in records if username_filter in r.get("User", {}).get("title", "").lower()]

        t = Table(title="Watch History", box=box.ROUNDED)
        t.add_column("When", style="dim", width=17)
        t.add_column("User", style="cyan", width=14)
        t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Type", style="yellow", width=8)

        for r in records:
            parent = r.get("grandparentTitle", "")
            title = r.get("title", "")
            full = f"{parent} — {title}" if parent else title
            t.add_row(
                format_ts(r.get("viewedAt")),
                r.get("User", {}).get("title", "—"),
                full,
                r.get("type", ""),
            )
        console.print(t)

    def do_unwatched(self, arg: str):
        """unwatched [library_id]  — items never played"""
        if arg.strip():
            libs_to_check = [{"key": arg.strip(), "title": f"Library {arg.strip()}"}]
        else:
            with console.status("Fetching libraries..."):
                libs_to_check = self.client.libraries()

        t = Table(title="Unwatched Content", box=box.ROUNDED)
        t.add_column("Library", style="cyan", width=16)
        t.add_column("Key", style="dim", width=7)
        t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Year", width=6, justify="right")
        t.add_column("Added", width=17, style="dim")

        count = 0
        with console.status("Scanning..."):
            for lib in libs_to_check:
                items = self.client.library_contents(lib.get("key", ""))
                for item in items:
                    if not item.get("viewCount"):
                        t.add_row(
                            lib.get("title", ""),
                            item.get("ratingKey", ""),
                            item.get("title", ""),
                            year(item),
                            format_ts(item.get("addedAt")),
                        )
                        count += 1

        if count == 0:
            console.print("[green]Everything has been watched![/green]")
        else:
            console.print(t)
            console.print(f"[yellow]{count} unwatched items.[/yellow]")

    def do_toprated(self, arg: str):
        """toprated [library_id]  — highest-rated items"""
        if arg.strip():
            libs_to_check = [{"key": arg.strip(), "title": f"Library {arg.strip()}"}]
        else:
            with console.status("Fetching libraries..."):
                libs_to_check = self.client.libraries()

        all_items = []
        with console.status("Fetching ratings..."):
            for lib in libs_to_check:
                for item in self.client.library_contents(lib.get("key", ""), sort="rating:desc"):
                    r = item.get("rating") or item.get("audienceRating")
                    if r:
                        all_items.append((float(r), lib.get("title", ""), item))

        all_items.sort(key=lambda x: x[0], reverse=True)
        t = Table(title="Top Rated", box=box.ROUNDED)
        t.add_column("#", style="dim", width=4)
        t.add_column("Rating", width=7, justify="right", style="bold green")
        t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Year", width=6, justify="right")
        t.add_column("Library", style="cyan", width=16)

        for i, (r, lib_title, item) in enumerate(all_items[:50], 1):
            t.add_row(str(i), f"{r:.1f}", item.get("title", ""), year(item), lib_title)
        console.print(t)

    def do_recently_played(self, arg: str):
        """recently_played [count]"""
        count = int(arg.strip()) if arg.strip().isdigit() else 20
        with console.status("Fetching history..."):
            records = self.client.history(count=count)
        if not records:
            console.print("[yellow]No history available (may require Plex Pass).[/yellow]")
            return

        t = Table(title=f"Recently Played (last {count})", box=box.ROUNDED)
        t.add_column("When", style="dim", width=17)
        t.add_column("User", style="cyan", width=14)
        t.add_column("Title", style="bold white", min_width=28)
        t.add_column("Type", style="yellow", width=8)
        for r in records:
            parent = r.get("grandparentTitle", "")
            title = r.get("title", "")
            t.add_row(
                format_ts(r.get("viewedAt")),
                r.get("User", {}).get("title", "—"),
                f"{parent} — {title}" if parent else title,
                r.get("type", ""),
            )
        console.print(t)

    # ── Storage analysis ──────────────────────────────────────────────────────

    def do_storage(self, _):
        """Disk usage breakdown by library."""
        with console.status("Calculating storage..."):
            rows = self.client.all_media_rows()

        lib_sizes: dict = defaultdict(int)
        lib_counts: dict = defaultdict(int)
        for row in rows:
            lib_sizes[row["library"]] += row.get("size") or 0
            lib_counts[row["library"]] += 1

        t = Table(title="Storage by Library", box=box.ROUNDED)
        t.add_column("Library", style="cyan")
        t.add_column("Files", justify="right", width=7)
        t.add_column("Total Size", justify="right", width=12, style="bold")
        t.add_column("Avg Size", justify="right", width=12)

        total_bytes = 0
        total_files = 0
        for lib_title in sorted(lib_sizes):
            sz = lib_sizes[lib_title]
            cnt = lib_counts[lib_title]
            avg = sz // cnt if cnt else 0
            total_bytes += sz
            total_files += cnt
            t.add_row(lib_title, str(cnt), format_size(sz), format_size(avg))

        t.add_section()
        t.add_row("[bold]TOTAL[/bold]", f"[bold]{total_files}[/bold]",
                  f"[bold]{format_size(total_bytes)}[/bold]",
                  format_size(total_bytes // total_files) if total_files else "—")
        console.print(t)

        # Top 10 largest files
        top = sorted(rows, key=lambda r: r.get("size") or 0, reverse=True)[:10]
        if top:
            t2 = Table(title="Top 10 Largest Files", box=box.ROUNDED)
            t2.add_column("Size", justify="right", width=12, style="bold yellow")
            t2.add_column("Title", style="bold white", min_width=28)
            t2.add_column("Library", style="cyan", width=16)
            t2.add_column("Codec", width=8)
            for row in top:
                t2.add_row(
                    format_size(row.get("size")),
                    row["title"],
                    row["library"],
                    row["videoCodec"].upper(),
                )
            console.print(t2)

    def do_codecs(self, _):
        """Video and audio codec distribution across all media."""
        with console.status("Scanning codecs..."):
            rows = self.client.all_media_rows()

        video_counts: Counter = Counter(r["videoCodec"] or "unknown" for r in rows)
        audio_counts: Counter = Counter(r["audioCodec"] or "unknown" for r in rows)
        container_counts: Counter = Counter(r["container"] or "unknown" for r in rows)
        total = len(rows)

        def make_table(title: str, counts: Counter) -> Table:
            t = Table(title=title, box=box.ROUNDED)
            t.add_column("Codec", style="bold cyan")
            t.add_column("Count", justify="right", width=8)
            t.add_column("Share", justify="right", width=8)
            for codec, cnt in counts.most_common():
                pct = cnt / total * 100 if total else 0
                t.add_row(codec.upper(), str(cnt), f"{pct:.1f}%")
            return t

        console.print(make_table("Video Codecs", video_counts))
        console.print(make_table("Audio Codecs", audio_counts))
        console.print(make_table("Containers", container_counts))

    def do_transcode(self, _):
        """Items likely to require transcoding on most clients."""
        # H.264 video + AAC/AC3/EAC3/MP3 audio in MKV/MP4 = widely direct-playable
        SAFE_VIDEO = {"h264", "hevc", "av1"}
        SAFE_AUDIO = {"aac", "ac3", "eac3", "mp3", "opus", "vorbis"}
        SAFE_CONTAINERS = {"mkv", "mp4", "m4v", "mov"}

        with console.status("Analysing codec compatibility..."):
            rows = self.client.all_media_rows()

        t = Table(title="Likely Transcode Required", box=box.ROUNDED)
        t.add_column("Library", style="cyan", width=14)
        t.add_column("Title", style="bold white", min_width=26)
        t.add_column("Video", width=10)
        t.add_column("Audio", width=10)
        t.add_column("Container", width=10)
        t.add_column("Reason", style="yellow")

        count = 0
        for row in rows:
            reasons = []
            vc = row["videoCodec"]
            ac = row["audioCodec"]
            ct = row["container"]
            if vc and vc not in SAFE_VIDEO:
                reasons.append(f"video:{vc.upper()}")
            if ac and ac not in SAFE_AUDIO:
                reasons.append(f"audio:{ac.upper()}")
            if ct and ct not in SAFE_CONTAINERS:
                reasons.append(f"container:{ct.upper()}")
            if reasons:
                t.add_row(
                    row["library"],
                    row["title"],
                    vc.upper() if vc else "?",
                    ac.upper() if ac else "?",
                    ct.upper() if ct else "?",
                    ", ".join(reasons),
                )
                count += 1

        if count == 0:
            console.print("[green]All items should direct-play on most clients.[/green]")
        else:
            console.print(t)
            console.print(f"[yellow]{count} items may require transcoding.[/yellow]")

    # ── Collection tools ──────────────────────────────────────────────────────

    def do_export(self, arg: str):
        """export <library_id> [filename]  — export to CSV or JSON"""
        parts = arg.strip().split()
        if not parts:
            console.print("[yellow]Usage: export <library_id> [filename][/yellow]")
            return

        section_id = parts[0]
        filename = parts[1] if len(parts) > 1 else None

        with console.status(f"Fetching library {section_id}..."):
            items = self.client.library_contents(section_id)

        if not items:
            console.print("[yellow]No items found.[/yellow]")
            return

        date_str = datetime.now().strftime("%Y%m%d")
        use_json = filename and filename.endswith(".json")

        if not filename:
            ext = "csv"
            filename = f"plex_export_{section_id}_{date_str}.{ext}"

        fields = ["ratingKey", "title", "year", "type", "rating", "audienceRating",
                  "duration", "addedAt", "summary", "studio", "contentRating"]

        if use_json:
            Path(filename).write_text(json.dumps(items, indent=2))
        else:
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                writer.writeheader()
                for item in items:
                    row = {k: item.get(k, "") for k in fields}
                    row["duration"] = format_duration(item.get("duration"))
                    row["addedAt"] = format_ts(item.get("addedAt"))
                    writer.writerow(row)

        console.print(f"[green]Exported {len(items)} items to[/green] [bold]{filename}[/bold]")

    def do_stale(self, arg: str):
        """stale [months]  — TV shows not updated in N months (default 6)"""
        months = int(arg.strip()) if arg.strip().isdigit() else 6
        cutoff_ts = int(time.time()) - (months * 30 * 86400)

        with console.status("Fetching TV libraries..."):
            libs = [l for l in self.client.libraries() if l.get("type") == "show"]

        if not libs:
            console.print("[yellow]No TV show libraries found.[/yellow]")
            return

        t = Table(title=f"Shows Not Updated in {months}+ Months", box=box.ROUNDED)
        t.add_column("Library", style="cyan", width=16)
        t.add_column("Key", style="dim", width=7)
        t.add_column("Show", style="bold white", min_width=28)
        t.add_column("Last Updated", width=14, style="yellow")
        t.add_column("Age", width=14, style="dim")

        count = 0
        with console.status("Scanning shows..."):
            for lib in libs:
                items = self.client.library_contents(lib.get("key", ""))
                stale = [i for i in items if (i.get("updatedAt") or 0) < cutoff_ts]
                stale.sort(key=lambda i: i.get("updatedAt") or 0)
                for item in stale:
                    t.add_row(
                        lib.get("title", ""),
                        item.get("ratingKey", ""),
                        item.get("title", ""),
                        format_ts(item.get("updatedAt")),
                        months_ago(item.get("updatedAt")),
                    )
                    count += 1

        if count == 0:
            console.print(f"[green]All shows updated within the last {months} months.[/green]")
        else:
            console.print(t)
            console.print(f"[yellow]{count} stale shows.[/yellow]")

    # ── Monitoring ────────────────────────────────────────────────────────────

    def do_watch(self, arg: str):
        """watch [seconds]  — live-refresh sessions (Ctrl+C to stop)"""
        interval = int(arg.strip()) if arg.strip().isdigit() else 5
        console.print(f"[dim]Refreshing every {interval}s — Ctrl+C to stop[/dim]")
        try:
            with Live(console=console, refresh_per_second=2, screen=False) as live:
                while True:
                    sessions = self.client.sessions()
                    if sessions:
                        live.update(build_sessions_table(sessions))
                    else:
                        live.update("[yellow]No active sessions.[/yellow]")
                    time.sleep(interval)
        except KeyboardInterrupt:
            console.print("\n[dim]Watch stopped.[/dim]")

    def do_alert(self, arg: str):
        """alert [seconds]  — notify when a transcode session starts (Ctrl+C to stop)"""
        interval = int(arg.strip()) if arg.strip().isdigit() else 10
        console.print(f"[dim]Monitoring for transcodes every {interval}s — Ctrl+C to stop[/dim]")
        known_transcodes: set = set()
        try:
            while True:
                sessions = self.client.sessions()
                current_keys = set()
                for s in sessions:
                    key = s.get("sessionKey", "")
                    is_transcode = bool(s.get("TranscodeSession"))
                    if not is_transcode:
                        # Check stream decision on media parts
                        for m in s.get("Media", []):
                            for p in m.get("Part", []):
                                for stream in p.get("Stream", []):
                                    if stream.get("decision") == "transcode":
                                        is_transcode = True
                    if is_transcode:
                        current_keys.add(key)
                        if key not in known_transcodes:
                            user = s.get("User", {}).get("title", "?")
                            title = s.get("title", "?")
                            parent = s.get("grandparentTitle", "")
                            full = f"{parent} — {title}" if parent else title
                            player = s.get("Player", {}).get("title", "?")
                            ts_info = s.get("TranscodeSession", {})
                            console.print(Panel(
                                f"[bold red]TRANSCODE STARTED[/bold red]\n"
                                f"User:   [cyan]{user}[/cyan]\n"
                                f"Title:  [white]{full}[/white]\n"
                                f"Player: [cyan]{player}[/cyan]\n"
                                f"Speed:  {ts_info.get('speed', '?')}x  "
                                f"Progress: {int((ts_info.get('progress') or 0))}%",
                                title=f"[bold red]Alert — {format_ts(int(time.time()))}[/bold red]",
                                border_style="red",
                            ))
                # Clear sessions that ended
                known_transcodes = current_keys
                time.sleep(interval)
        except KeyboardInterrupt:
            console.print("\n[dim]Alert stopped.[/dim]")

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
        title="[yellow]Setup Required[/yellow]",
        border_style="yellow",
    ))
    token = Prompt.ask("[yellow]Enter your Plex token[/yellow]")
    if token:
        cfg["token"] = token
        save_config(cfg)
        console.print(f"[green]Token saved to {CONFIG_FILE}[/green]")
    return token

def main():
    console.print(Panel(
        "[bold white]Plex Media Server CLI[/bold white]\n"
        f"[dim]Connecting to {BASE_URL}[/dim]",
        border_style="cyan",
        expand=False,
    ))

    token = get_token()
    if not token:
        console.print("[red]No token provided. Exiting.[/red]")
        sys.exit(1)

    client = PlexClient(token)

    with console.status("Connecting..."):
        info = client.server_info()
    if info:
        name = info.get("friendlyName", "Plex Server")
        ver = info.get("version", "")
        console.print(f"[green]Connected to[/green] [bold]{name}[/bold] [dim]v{ver}[/dim]")
    else:
        console.print(f"[yellow]Could not reach {BASE_URL} — commands may fail.[/yellow]")

    console.print("[dim]Type [bold]help[/bold] for available commands.[/dim]\n")

    shell = PlexShell(client)
    try:
        shell.cmdloop()
    except KeyboardInterrupt:
        console.print("\n[dim]Interrupted. Goodbye.[/dim]")

if __name__ == "__main__":
    main()
