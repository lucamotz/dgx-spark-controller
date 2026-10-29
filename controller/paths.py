# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Platform-aware local storage locations for the controller.

The controller keeps three kinds of local state: reviewed configuration (nodes,
hand-written profiles, pinned SSH host keys), durable data (the operation
journal and generated recipe profiles), and logs. On Linux they follow the XDG
base-directory specification; on macOS they live under the user's Library.
``DGX_CONTROLLER_HOME`` collapses everything below one directory, which is how
tests, containers, and a repository-local development checkout stay isolated.
``write_private_file`` replaces the controller's private configuration and
state files.

Nothing in this module connects to a node or reads a secret.
"""

from __future__ import annotations

import contextlib
import os
import sys
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

APP_SLUG = "dgx-spark-controller"
MACOS_APP_NAME = "DGXSparkController"


@dataclass(frozen=True)
class AppPaths:
    """Resolved local directories and the well-known files inside them."""

    config_dir: Path
    data_dir: Path
    log_dir: Path

    @property
    def nodes_file(self) -> Path:
        return self.config_dir / "nodes.json"

    @property
    def profiles_dir(self) -> Path:
        """Hand-reviewed profiles an operator places here explicitly."""
        return self.config_dir / "profiles"

    @property
    def generated_profiles_dir(self) -> Path:
        """Profiles produced by the recipe builder after a verified prepare."""
        return self.data_dir / "profiles"

    @property
    def recipe_drafts_dir(self) -> Path:
        return self.data_dir / "recipes"

    @property
    def database_path(self) -> Path:
        return self.data_dir / "controller.sqlite3"

    @property
    def known_hosts_file(self) -> Path:
        """Host keys the operator explicitly trusted during onboarding."""
        return self.config_dir / "known_hosts"

    @property
    def ssh_dir(self) -> Path:
        """Location for a controller-generated SSH key pair, if one is requested."""
        return self.config_dir / "ssh"

    @property
    def app_log_file(self) -> Path:
        return self.log_dir / "controller-app.log"

    def ensure(self) -> AppPaths:
        """Create private directories (mode 0700) without touching existing files."""
        for directory in (
            self.config_dir,
            self.data_dir,
            self.log_dir,
            self.profiles_dir,
            self.generated_profiles_dir,
            self.recipe_drafts_dir,
        ):
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        return self


def write_private_file(path: Path, payload: str, *, prefix: str) -> None:
    """Atomically replace ``path`` with ``payload``, readable only by this user.

    The parent directory is created with mode 0700 when it is missing, the data
    reaches the disk before the rename, and a failed write leaves neither a
    partial file nor a temporary file behind.
    """
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(
        prefix=prefix, suffix=path.suffix, dir=path.parent
    )
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)
        raise


def xdg_base_dir(environment: Mapping[str, str], name: str) -> Path | None:
    """Return the ``$XDG_*`` base directory ``name``, or None when unusable.

    The XDG base-directory specification ignores empty and relative values.
    """
    raw = environment.get(name, "").strip()
    return Path(raw) if raw and Path(raw).is_absolute() else None


def resolve_app_paths(
    *,
    env: Mapping[str, str] | None = None,
    platform: str | None = None,
    home: Path | None = None,
) -> AppPaths:
    """Return the local storage layout for this user and operating system."""
    environment = os.environ if env is None else env
    system = sys.platform if platform is None else platform
    user_home = Path.home() if home is None else home

    override = environment.get("DGX_CONTROLLER_HOME", "").strip()
    if override:
        root = Path(override).expanduser()
        return AppPaths(
            config_dir=root / "config", data_dir=root / "data", log_dir=root / "logs"
        )

    if system == "darwin":
        support = user_home / "Library" / "Application Support" / MACOS_APP_NAME
        return AppPaths(
            config_dir=support / "config",
            data_dir=support / "data",
            log_dir=user_home / "Library" / "Logs" / MACOS_APP_NAME,
        )

    def xdg(name: str, default: Path) -> Path:
        base = xdg_base_dir(environment, name)
        return default if base is None else base

    config_home = xdg("XDG_CONFIG_HOME", user_home / ".config")
    data_home = xdg("XDG_DATA_HOME", user_home / ".local" / "share")
    state_home = xdg("XDG_STATE_HOME", user_home / ".local" / "state")
    return AppPaths(
        config_dir=config_home / APP_SLUG,
        data_dir=data_home / APP_SLUG,
        log_dir=state_home / APP_SLUG / "logs",
    )
