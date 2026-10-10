# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Per-OS local storage layout (controller/paths.py)."""

from __future__ import annotations

import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from controller.paths import APP_SLUG, MACOS_APP_NAME, AppPaths, resolve_app_paths

HOME = Path("/home/example")
MAC_HOME = Path("/Users/example")


class MacOSLayoutTests(unittest.TestCase):
    def test_library_layout(self):
        paths = resolve_app_paths(env={}, platform="darwin", home=MAC_HOME)
        support = MAC_HOME / "Library" / "Application Support" / MACOS_APP_NAME
        self.assertEqual(paths.config_dir, support / "config")
        self.assertEqual(paths.data_dir, support / "data")
        self.assertEqual(paths.log_dir, MAC_HOME / "Library" / "Logs" / MACOS_APP_NAME)

    def test_xdg_variables_do_not_apply_on_macos(self):
        env = {
            "XDG_CONFIG_HOME": "/xdg/config",
            "XDG_DATA_HOME": "/xdg/data",
            "XDG_STATE_HOME": "/xdg/state",
        }
        self.assertEqual(
            resolve_app_paths(env=env, platform="darwin", home=MAC_HOME),
            resolve_app_paths(env={}, platform="darwin", home=MAC_HOME),
        )


class LinuxLayoutTests(unittest.TestCase):
    def test_xdg_defaults(self):
        paths = resolve_app_paths(env={}, platform="linux", home=HOME)
        self.assertEqual(paths.config_dir, HOME / ".config" / APP_SLUG)
        self.assertEqual(paths.data_dir, HOME / ".local" / "share" / APP_SLUG)
        self.assertEqual(paths.log_dir, HOME / ".local" / "state" / APP_SLUG / "logs")

    def test_absolute_xdg_overrides(self):
        env = {
            "XDG_CONFIG_HOME": "/xdg/config",
            "XDG_DATA_HOME": "/xdg/data",
            "XDG_STATE_HOME": "/xdg/state",
        }
        paths = resolve_app_paths(env=env, platform="linux", home=HOME)
        self.assertEqual(paths.config_dir, Path("/xdg/config") / APP_SLUG)
        self.assertEqual(paths.data_dir, Path("/xdg/data") / APP_SLUG)
        self.assertEqual(paths.log_dir, Path("/xdg/state") / APP_SLUG / "logs")

    def test_relative_and_blank_xdg_values_are_ignored(self):
        defaults = resolve_app_paths(env={}, platform="linux", home=HOME)
        for value in ("relative/config", "./config", "", "   "):
            with self.subTest(value=value):
                env = {
                    "XDG_CONFIG_HOME": value,
                    "XDG_DATA_HOME": value,
                    "XDG_STATE_HOME": value,
                }
                self.assertEqual(
                    resolve_app_paths(env=env, platform="linux", home=HOME), defaults
                )

    def test_each_xdg_variable_is_independent(self):
        paths = resolve_app_paths(
            env={"XDG_DATA_HOME": "/srv/data"}, platform="linux", home=HOME
        )
        self.assertEqual(paths.config_dir, HOME / ".config" / APP_SLUG)
        self.assertEqual(paths.data_dir, Path("/srv/data") / APP_SLUG)
        self.assertEqual(paths.log_dir, HOME / ".local" / "state" / APP_SLUG / "logs")


class ControllerHomeOverrideTests(unittest.TestCase):
    def test_override_collapses_layout_on_every_platform(self):
        for platform in ("darwin", "linux"):
            with self.subTest(platform=platform):
                env = {
                    "DGX_CONTROLLER_HOME": "/opt/dgx-home",
                    "XDG_CONFIG_HOME": "/xdg/config",
                }
                paths = resolve_app_paths(env=env, platform=platform, home=HOME)
                self.assertEqual(
                    paths,
                    AppPaths(
                        config_dir=Path("/opt/dgx-home/config"),
                        data_dir=Path("/opt/dgx-home/data"),
                        log_dir=Path("/opt/dgx-home/logs"),
                    ),
                )

    def test_blank_override_is_ignored(self):
        self.assertEqual(
            resolve_app_paths(
                env={"DGX_CONTROLLER_HOME": "  "}, platform="linux", home=HOME
            ),
            resolve_app_paths(env={}, platform="linux", home=HOME),
        )

    def test_override_expands_user(self):
        with mock.patch.dict(os.environ, {"HOME": "/home/tilde"}):
            paths = resolve_app_paths(
                env={"DGX_CONTROLLER_HOME": "~/dgx"}, platform="linux", home=HOME
            )
        self.assertEqual(paths.config_dir, Path("/home/tilde/dgx/config"))

    def test_defaults_read_process_environment(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.dict(os.environ, {"DGX_CONTROLLER_HOME": directory}),
        ):
            self.assertEqual(resolve_app_paths().data_dir, Path(directory) / "data")


class WellKnownFileTests(unittest.TestCase):
    def test_files_live_in_the_expected_directories(self):
        paths = resolve_app_paths(
            env={"DGX_CONTROLLER_HOME": "/r"}, platform="linux", home=HOME
        )
        self.assertEqual(paths.nodes_file, Path("/r/config/nodes.json"))
        self.assertEqual(paths.profiles_dir, Path("/r/config/profiles"))
        self.assertEqual(paths.known_hosts_file, Path("/r/config/known_hosts"))
        self.assertEqual(paths.ssh_dir, Path("/r/config/ssh"))
        self.assertEqual(paths.generated_profiles_dir, Path("/r/data/profiles"))
        self.assertEqual(paths.recipe_drafts_dir, Path("/r/data/recipes"))
        self.assertEqual(paths.database_path, Path("/r/data/controller.sqlite3"))
        self.assertEqual(paths.app_log_file, Path("/r/logs/controller-app.log"))


@unittest.skipIf(os.name != "posix", "POSIX permission bits")
class EnsureTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.home = Path(self._tmp.name)
        previous = os.umask(0o022)
        self.addCleanup(os.umask, previous)

    def _created(self, paths: AppPaths) -> list[Path]:
        return [
            paths.config_dir,
            paths.data_dir,
            paths.log_dir,
            paths.profiles_dir,
            paths.generated_profiles_dir,
            paths.recipe_drafts_dir,
        ]

    def test_ensure_creates_private_directories_under_temp_home(self):
        for platform in ("linux", "darwin"):
            with self.subTest(platform=platform):
                paths = resolve_app_paths(
                    env={}, platform=platform, home=self.home / platform
                )
                self.assertIs(paths.ensure(), paths)
                for directory in self._created(paths):
                    self.assertTrue(directory.is_dir(), directory)
                    self.assertEqual(
                        stat.S_IMODE(directory.stat().st_mode), 0o700, directory
                    )
                    self.assertTrue(directory.is_relative_to(self.home / platform))

    def test_ensure_is_idempotent_and_leaves_existing_files_untouched(self):
        paths = resolve_app_paths(
            env={"DGX_CONTROLLER_HOME": str(self.home / "dgx")}, platform="linux"
        )
        paths.ensure()
        paths.nodes_file.write_text('{"schema_version": 1}', encoding="utf-8")
        os.chmod(paths.nodes_file, 0o600)
        paths.ensure()
        self.assertEqual(
            paths.nodes_file.read_text(encoding="utf-8"), '{"schema_version": 1}'
        )
        self.assertEqual(stat.S_IMODE(paths.nodes_file.stat().st_mode), 0o600)
        self.assertFalse(paths.database_path.exists())
        self.assertFalse(paths.app_log_file.exists())


if __name__ == "__main__":
    unittest.main()
