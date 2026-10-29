# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Locate, validate, or generate the SSH client key used to reach a node.

Safety boundary: this module never opens, reads, copies, or returns private key
contents. It inspects private keys only through ``stat`` (type and mode) and
reads the sibling ``.pub`` file, which is public by definition. Key generation
delegates to ``ssh-keygen`` with an argv
list, never overwrites an existing file, and verifies mode 0600. Passwords stay
with the human: authorizing a key on a node is done by the operator running the
``ssh-copy-id`` command from ``copy_id_command`` in their own terminal.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import re
import shlex
import stat
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from controller.paths import xdg_base_dir

from .commands import Runner, run_captured
from .hostkeys import decode_public_key_blob, validate_host, validate_port
from .network import DiscoveryError

NVIDIA_SYNC_KEY_NAME = "nvsync.key"
CONTROLLER_KEY_NAME = "id_ed25519"
SOURCE_NVIDIA_SYNC = "nvidia-sync"
SOURCE_SSH_DIR = "ssh-dir"
SOURCE_CONTROLLER = "controller"
MAX_PUBLIC_KEY_BYTES = 16 * 1024
KEYGEN_TIMEOUT_SECONDS = 30

_USERNAME_RE = re.compile(r"[a-z_][a-z0-9_-]{0,31}")
_KEY_TYPE_RE = re.compile(r"[a-z0-9][a-z0-9@.+-]{0,63}")
_PUBLIC_KEY_LINE_RE = re.compile(
    r"(?P<type>[a-z0-9][a-z0-9@.+-]{0,63}) (?P<blob>[A-Za-z0-9+/]+={0,2})"
    r"(?: (?P<comment>[^\x00-\x1f\x7f]*))?"
)
_SKIPPED_SSH_PREFIXES = ("known_hosts", "authorized_keys")
_SKIPPED_SSH_NAMES = frozenset({"config", "environment", "rc"})


@dataclass(frozen=True)
class KeyCandidate:
    """A private key file the operator may select; never holds key material."""

    id: str
    path: str
    display_path: str
    source: str
    has_public_key: bool
    key_type: str | None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-ready mapping for the onboarding API."""
        return {
            "id": self.id,
            "path": self.path,
            "display_path": self.display_path,
            "source": self.source,
            "has_public_key": self.has_public_key,
            "key_type": self.key_type,
        }


def _home(home: Path | None) -> Path:
    return Path.home() if home is None else Path(home)


def _display_path(path: Path, home: Path) -> str:
    """Return ``~/...`` for paths inside ``home``, else the absolute path."""
    pairs = [(path, home)]
    with contextlib.suppress(OSError, RuntimeError):
        pairs.append((path.resolve(), home.resolve()))
    for candidate, base in pairs:
        if candidate == base:
            return "~"
        if candidate.is_relative_to(base):
            return "~/" + candidate.relative_to(base).as_posix()
    return str(path)


def _public_key_path(private_key_path: Path) -> Path:
    return Path(f"{private_key_path}.pub")


def _read_public_key_type(public_path: Path) -> str | None:
    try:
        with public_path.open("rb") as handle:
            data = handle.read(MAX_PUBLIC_KEY_BYTES)
    except OSError:
        return None
    fields = data.decode("utf-8", errors="replace").split(maxsplit=1)
    if not fields or not _KEY_TYPE_RE.fullmatch(fields[0]):
        return None
    return fields[0]


def _key_candidate(path: Path, source: str, home: Path) -> KeyCandidate | None:
    """Describe a private key file using only ``stat`` and its ``.pub`` sibling.

    ``path`` keeps the location as found (as ``ssh -i`` would use it); the id
    and deduplication use the fully resolved path.
    """
    absolute = Path(os.path.abspath(path))
    try:
        resolved = absolute.resolve()
        status = resolved.stat()
    except (OSError, RuntimeError):
        return None
    if not stat.S_ISREG(status.st_mode):
        return None
    public_path = _public_key_path(absolute)
    has_public_key = public_path.is_file()
    return KeyCandidate(
        id=hashlib.sha256(str(resolved).encode("utf-8")).hexdigest()[:12],
        path=str(absolute),
        display_path=_display_path(absolute, home),
        source=source,
        has_public_key=has_public_key,
        key_type=_read_public_key_type(public_path) if has_public_key else None,
    )


def nvidia_sync_key_paths(
    *,
    home: Path | None = None,
    platform: str | None = None,
    env: Mapping[str, str] | None = None,
) -> list[Path]:
    """Return every plausible NVIDIA Sync ``nvsync.key`` location.

    NVIDIA Sync generates a key pair named ``nvsync.key`` when it pairs with a
    DGX Spark. The caller checks which of these paths exist.

    Args:
        home: Home directory; defaults to ``Path.home()``.
        platform: ``sys.platform`` value; defaults to the running platform.
        env: Environment used for ``XDG_CONFIG_HOME``/``XDG_DATA_HOME``.

    Returns:
        Candidate paths in preference order, without duplicates.

    """
    user_home = _home(home)
    system = sys.platform if platform is None else platform
    environment = os.environ if env is None else env
    suffix = Path("NVIDIA", "Sync", "config", NVIDIA_SYNC_KEY_NAME)
    if system == "darwin":
        return [user_home / "Library" / "Application Support" / suffix]

    candidates = [
        xdg_base_dir(environment, "XDG_CONFIG_HOME"),
        user_home / ".config",
        xdg_base_dir(environment, "XDG_DATA_HOME"),
        user_home / ".local" / "share",
    ]
    paths: list[Path] = []
    for base in candidates:
        if base is not None and base / suffix not in paths:
            paths.append(base / suffix)
    return paths


def _ssh_dir_keys(ssh_dir: Path, home_resolved: Path) -> list[Path]:
    try:
        names = sorted(os.listdir(ssh_dir))
    except OSError:
        return []
    available = set(names)
    keys: list[Path] = []
    for name in names:
        if (
            name.endswith(".pub")
            or name.startswith(_SKIPPED_SSH_PREFIXES)
            or name in _SKIPPED_SSH_NAMES
            or f"{name}.pub" not in available
        ):
            continue
        path = ssh_dir / name
        try:
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                if not path.resolve().is_relative_to(home_resolved):
                    continue
            elif not stat.S_ISREG(mode):
                continue
        except (OSError, RuntimeError):
            continue
        keys.append(path)
    return keys


def find_key_candidates(
    *,
    home: Path | None = None,
    platform: str | None = None,
    env: Mapping[str, str] | None = None,
    controller_key_dir: Path | None = None,
) -> list[KeyCandidate]:
    """List selectable private keys without reading any private key contents.

    Order: existing NVIDIA Sync keys, then ``~/.ssh`` files that have a sibling
    ``<name>.pub`` (sorted by name; ``known_hosts*``, ``authorized_keys*``,
    ``config``, ``*.pub``, non-regular files, and symlinks leaving the home
    directory are skipped), then ``controller_key_dir/id_ed25519``.

    Args:
        home: Home directory; defaults to ``Path.home()``.
        platform: ``sys.platform`` value; defaults to the running platform.
        env: Environment used for XDG lookups.
        controller_key_dir: Directory holding a controller-generated key.

    Returns:
        Candidates deduplicated by resolved path; ``path`` is the absolute
        location as found, so a symlinked key keeps its own ``.pub`` sibling.

    """
    user_home = _home(home)
    try:
        home_resolved = user_home.resolve()
    except (OSError, RuntimeError):
        return []
    sources: list[tuple[Path, str]] = [
        (path, SOURCE_NVIDIA_SYNC)
        for path in nvidia_sync_key_paths(home=user_home, platform=platform, env=env)
    ]
    sources += [
        (path, SOURCE_SSH_DIR)
        for path in _ssh_dir_keys(user_home / ".ssh", home_resolved)
    ]
    if controller_key_dir is not None:
        sources.append(
            (Path(controller_key_dir) / CONTROLLER_KEY_NAME, SOURCE_CONTROLLER)
        )
    candidates: list[KeyCandidate] = []
    seen: set[Path] = set()
    for path, source in sources:
        candidate = _key_candidate(path, source, user_home)
        if candidate is None:
            continue
        resolved = Path(candidate.path).resolve()
        if resolved not in seen:
            seen.add(resolved)
            candidates.append(candidate)
    return candidates


def _run_keygen(runner: Runner, private_path: Path) -> None:
    argv = ["ssh-keygen", "-q", "-t", "ed25519", "-N", ""]
    argv += ["-C", "dgx-spark-controller", "-f", str(private_path)]
    try:
        # With stdin at EOF, ssh-keygen answers "no" to any overwrite prompt.
        completed = run_captured(runner, argv, timeout=KEYGEN_TIMEOUT_SECONDS)
    except FileNotFoundError as exc:
        raise DiscoveryError(
            "ssh_key_generation_failed",
            "The ssh-keygen tool was not found on this computer.",
            "Install the OpenSSH client tools and try again.",
        ) from exc
    except (OSError, subprocess.SubprocessError) as exc:
        raise DiscoveryError(
            "ssh_key_generation_failed", "The ssh-keygen tool did not complete."
        ) from exc
    if completed.returncode != 0:
        raise DiscoveryError(
            "ssh_key_generation_failed",
            f"The ssh-keygen tool exited with status {completed.returncode}.",
        )


def generate_key(
    directory: Path,
    *,
    runner: Runner = subprocess.run,
    home: Path | None = None,
) -> KeyCandidate:
    """Create a new passphrase-less ed25519 key pair with ``ssh-keygen``.

    The private key is ``CONTROLLER_KEY_NAME`` in ``directory`` and the public
    key sits beside it with a ``.pub`` suffix.

    Args:
        directory: Absolute directory for the key; created with mode 0700.
        runner: ``subprocess.run`` compatible callable, injectable for tests.
        home: Home directory used only for ``display_path``.

    Returns:
        The new key as a ``controller`` candidate.

    Raises:
        DiscoveryError: ``ssh_key_exists`` if either file already exists,
            ``ssh_key_generation_failed`` if ``ssh-keygen`` fails, or
            ``ssh_key_permissions`` if the private key is not mode 0600.

    """
    target_dir = Path(directory)
    if not target_dir.is_absolute():
        raise DiscoveryError(
            "invalid_ssh_key_path", "The key directory must be absolute."
        )
    private_path = target_dir / CONTROLLER_KEY_NAME
    public_path = _public_key_path(private_path)
    if os.path.lexists(private_path) or os.path.lexists(public_path):
        raise DiscoveryError(
            "ssh_key_exists",
            f"{private_path} already exists and will not be overwritten.",
            "Select the existing key instead.",
        )
    if not target_dir.exists():
        target_dir.mkdir(mode=0o700, parents=True)
        os.chmod(target_dir, 0o700)
    elif not target_dir.is_dir():
        raise DiscoveryError(
            "invalid_ssh_key_path", f"{target_dir} is not a directory."
        )
    _run_keygen(runner, private_path)
    try:
        status = os.lstat(private_path)
    except OSError as exc:
        raise DiscoveryError(
            "ssh_key_generation_failed", "The ssh-keygen tool did not create the key."
        ) from exc
    if not stat.S_ISREG(status.st_mode) or not public_path.is_file():
        raise DiscoveryError(
            "ssh_key_generation_failed",
            "The ssh-keygen tool did not create the key pair.",
        )
    if stat.S_IMODE(status.st_mode) != 0o600:
        raise DiscoveryError(
            "ssh_key_permissions",
            f"{private_path} was created with unexpected permissions.",
            f"Run: chmod 600 {shlex.quote(str(private_path))}",
        )
    candidate = _key_candidate(private_path, SOURCE_CONTROLLER, _home(home))
    if candidate is None:
        raise DiscoveryError(
            "ssh_key_generation_failed", "The generated key cannot be inspected."
        )
    return candidate


def public_key_text(private_key_path: Path) -> str:
    """Return the validated one-line public key stored next to a private key.

    Only ``<private_key_path>.pub`` is read.

    Args:
        private_key_path: Path of the private key (not the ``.pub`` file).

    Returns:
        ``"<type> <base64> [comment]"`` without surrounding whitespace.

    Raises:
        DiscoveryError: ``public_key_missing`` if the file is absent or not a
            regular file; ``public_key_invalid`` if it is not a single valid key.

    """
    public_path = _public_key_path(Path(private_key_path))
    try:
        status = public_path.stat()
    except OSError as exc:
        raise DiscoveryError(
            "public_key_missing",
            f"{public_path.name} was not found next to the private key.",
            "Recreate it with: ssh-keygen -y -f <private key> > <private key>.pub",
        ) from exc
    if not stat.S_ISREG(status.st_mode):
        raise DiscoveryError(
            "public_key_missing", f"{public_path.name} is not a regular file."
        )
    invalid = DiscoveryError(
        "public_key_invalid", f"{public_path.name} is not a valid public key."
    )
    if status.st_size > MAX_PUBLIC_KEY_BYTES:
        raise invalid
    try:
        with public_path.open("rb") as handle:
            line = handle.read(MAX_PUBLIC_KEY_BYTES + 1).decode("utf-8").strip()
    except (OSError, UnicodeDecodeError) as exc:
        raise invalid from exc
    match = _PUBLIC_KEY_LINE_RE.fullmatch(line)
    if match is None:
        raise invalid
    try:
        decode_public_key_blob(match.group("blob"), match.group("type"))
    except DiscoveryError as exc:
        raise invalid from exc
    return line


def validate_username(value: str) -> str:
    """Validate a POSIX-style login name for the node account.

    Args:
        value: Candidate user name.

    Returns:
        The unchanged user name.

    Raises:
        DiscoveryError: ``invalid_ssh_user`` unless it matches
            ``[a-z_][a-z0-9_-]{0,31}``.

    """
    if not isinstance(value, str) or not _USERNAME_RE.fullmatch(value):
        raise DiscoveryError(
            "invalid_ssh_user",
            "User names use a-z, 0-9, '_' and '-', starting with a letter or '_'.",
        )
    return value


def copy_id_command(
    user: str, host: str, private_key_path: Path, *, port: int = 22
) -> str:
    """Build the ``ssh-copy-id`` command the operator runs in their own terminal.

    The controller never runs it: ``ssh-copy-id`` asks for the account
    password, and that interaction stays with the human.

    Args:
        user: Login name on the node.
        host: Node address or host name.
        private_key_path: Private key whose ``.pub`` file is installed.
        port: SSH port; ``-p`` is added only when it is not 22.

    Returns:
        A shell-quoted command line.

    """
    login = validate_username(user)
    target = validate_host(host)
    argv = ["ssh-copy-id", "-i", str(_public_key_path(Path(private_key_path)))]
    if validate_port(port) != 22:
        argv += ["-p", str(port)]
    argv.append(f"{login}@{target}")
    return shlex.join(argv)
