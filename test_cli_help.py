"""Offline regression checks for command discovery and alias dispatch."""
import io
import unittest
from unittest.mock import Mock, patch

from rich.console import Console

import plex_cli as cli


class HelpTests(unittest.TestCase):
    def setUp(self):
        self.output = io.StringIO()
        self.console = Console(file=self.output, width=160, color_system=None)
        self.console_patch = patch.object(cli, "console", self.console)
        self.console_patch.start()
        self.addCleanup(self.console_patch.stop)
        self.shell = cli.PlexShell(None)

    def run_help(self, command):
        self.output.seek(0)
        self.output.truncate()
        self.shell.onecmd(command)
        return self.output.getvalue()

    def test_focused_help_and_search(self):
        self.assertIn("Command groups", self.run_help("help"))
        self.assertIn("Usage: watch_next", self.run_help("help watch_next"))
        self.assertIn("Usage: watch_next", self.run_help("help watch next"))
        self.assertIn("largest", self.run_help("help storage"))
        self.assertNotIn("radarr status", self.output.getvalue())
        self.assertIn("Usage: storage", self.run_help("help library storage"))
        self.assertIn("foreign_subtitle_audit", self.run_help("help --search subtitles"))
        self.assertIn("watch monitor", self.run_help("help watch"))
        self.assertIn("Usage: watch", self.run_help("help watch monitor"))
        self.assertIn("Playback control", self.run_help("help --all"))

    def test_suggestions_do_not_execute(self):
        self.shell.do_radarr_status = Mock()
        self.assertIn("radarr_status", self.run_help("radar_status"))
        self.assertIn("status", self.run_help("radarr statsu"))
        self.assertIn("watch_next", self.run_help("help watch_nex"))
        self.shell.do_radarr_status.assert_not_called()

    def test_aliases_preserve_arguments_and_old_names(self):
        self.shell.do_search = Mock(return_value=False)
        args = '--director "Ridley Scott" --year 1979'
        self.shell.onecmd("library search " + args)
        self.shell.do_search.assert_called_once_with(args)
        self.shell.onecmd("search " + args)
        self.assertEqual(self.shell.do_search.call_count, 2)
        self.shell.do_watch = Mock(return_value=False)
        for command, expected in (("watch", ""), ("watch 5", "5"), ("watch monitor 10", "10")):
            self.shell.onecmd(command)
            self.shell.do_watch.assert_called_with(expected)

    def test_alias_uses_target_paging_behavior(self):
        handler = Mock(return_value=False)
        handler.interactive = True
        self.shell.do_sonarr_missing = handler
        with patch.object(cli, "console") as console:
            console.is_terminal = True
            self.shell.onecmd("sonarr missing")
            console.pager.assert_not_called()
        handler.assert_called_once_with("")

    def test_completion(self):
        self.assertIn("library", self.shell.completenames("lib"))
        self.assertIn("next", self.shell.complete_watch("ne", "watch ne", 6, 8))
        self.assertIn("status", self.shell.complete_help("st", "help radarr st", 12, 14))
        self.shell.complete_search = Mock(return_value=["--director"])
        line = "library search --dir"
        self.assertEqual(self.shell.complete_library("--dir", line, 15, 20), ["--director"])
        self.shell.complete_search.assert_called_once_with("--dir", "search --dir", 7, 12)

    def test_help_never_loads_config_or_connects(self):
        cases = (("help",), ("--help",), ("help", "storage"), ("radarr",),
                 ("watch_next", "--help"), ("radarr", "status", "--help"),
                 ("watch", "--help"), ("sonarr", "help", "missing"))
        with patch.object(cli, "load_config", side_effect=AssertionError("config read")), \
             patch.object(cli, "PlexClient", side_effect=AssertionError("client created")):
            for args in cases:
                with self.subTest(args=args), patch.object(cli.sys, "argv", ["plex_cli.py", *args]):
                    cli.main()

    def test_all_aliases_have_documented_handlers(self):
        documented = {name for _, commands in cli._HELP_SECTIONS for name, _, _ in commands}
        for aliases in cli._COMMAND_GROUPS.values():
            for name in aliases.values():
                self.assertIn(name, documented)
                self.assertTrue(callable(getattr(self.shell, "do_" + name)))


if __name__ == "__main__":
    unittest.main()
