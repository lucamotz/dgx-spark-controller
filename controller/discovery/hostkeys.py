# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""SSH host-key discovery and the controller's trusted ``known_hosts`` store.

Safety boundary: the onboarding wizard shows the operator a node's host-key
fingerprints (``SHA256:...`` exactly as ``ssh-keygen -lf`` prints them) and only
an explicit approval calls ``KnownHostsStore.trust``. ``scan_host_keys`` runs
``ssh-keyscan`` as an argv list with a validated host and a bounded timeout; it
never authenticates and never edits ``~/.ssh``. Every later SSH invocation uses
``ssh_known_hosts_options``, so host keys are checked strictly against the
operator's ``~/.ssh/known_hosts`` plus the controller store, and an unknown or
changed key fails closed instead of being accepted on first use.

Host names are validated before they reach argv or a ``known_hosts`` line, so
neither option injection (a leading ``-``) nor line injection (whitespace,
newlines, or ``,``) is possible.

``user_known_host_keys`` only reads the operator's own ``~/.ssh/known_hosts``
with ``ssh-keygen -F`` (which also matches hashed host names) so the wizard can
say whether a scanned key matches what the operator's ssh already trusts; the
controller never writes that file.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import ipaddress
import re
import struct
import subprocess
import threading
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

from controller.paths import write_private_file

from .commands import Runner, run_captured
from .network import DiscoveryError

SUPPORTED_HOST_KEY_TYPES: tuple[str, ...] = (
    "ssh-ed25519",
    "ecdsa-sha2-nistp256",
    "ecdsa-sha2-nistp384",
    "ecdsa-sha2-nistp521",
    "ssh-rsa",
)
KEYSCAN_KEY_TYPES = "ed25519,ecdsa,rsa"
DEFAULT_SSH_PORT = 22
MAX_KEY_BLOB_BYTES = 16 * 1024
MAX_KNOWN_HOSTS_BYTES = 1024 * 1024
MAX_KEYSCAN_TIMEOUT_SECONDS = 60
MAX_KNOWN_HOSTS_LOOKUP_BYTES = 256 * 1024
USER_KNOWN_HOSTS_FILE = "~/.ssh/known_hosts"

_LABEL_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
# A final label such as "1" or "0x7f" makes inet_aton()/getaddrinfo() read the
# whole name as a numeric IPv4 address ("10.1" -> 10.0.0.1).
_NUMERIC_LABEL_RE = re.compile(r"[0-9]+|0x[0-9a-f]*")
_DOTTED_NUMERIC_RE = re.compile(r"[0-9.]+")
_BASE64_RE = re.compile(r"[A-Za-z0-9+/]+={0,2}")
_PLAIN_CONFIG_PATH_RE = re.compile(r"[A-Za-z0-9_./,:@+=-]+")
_STORE_LOCK = threading.Lock()
_HOST_HINT = "Enter an IPv4/IPv6 address or a host name such as spark-1a2b.local."


def _invalid_host(message: str) -> DiscoveryError:
    return DiscoveryError("invalid_host", message, _HOST_HINT)


def validate_host(value: str) -> str:
    """Validate and canonicalize an SSH host.

    Args:
        value: IPv4/IPv6 literal (IPv6 optionally in brackets) or an RFC 1123
            host name with an optional trailing dot.

    Returns:
        The compressed IP literal, or the lowercase name without trailing dot.

    Raises:
        DiscoveryError: ``invalid_host`` for empty values, whitespace, ``@``,
            ``:`` or ``_`` in names, a leading ``-``, labels outside 1-63
            letters/digits/inner hyphens, names over 253 characters, a numeric
            final label, or IPv6 zone identifiers.

    """
    if not isinstance(value, str) or not value:
        raise _invalid_host("Host must not be empty.")
    if len(value) > 255 or any(char.isspace() for char in value):
        raise _invalid_host("Host must not contain spaces.")
    if value.startswith("-"):
        raise _invalid_host("Host must not start with '-'.")
    bracketed = value.startswith("[") and value.endswith("]")
    literal = value[1:-1] if bracketed else value
    if ":" in literal or "%" in literal or _DOTTED_NUMERIC_RE.fullmatch(literal):
        try:
            address = ipaddress.ip_address(literal)
        except ValueError as exc:
            raise _invalid_host("Host is not a valid IP address.") from exc
        if isinstance(address, ipaddress.IPv6Address) and address.scope_id:
            raise _invalid_host("IPv6 zone identifiers are not supported.")
        return str(address)
    if bracketed:
        raise _invalid_host("Only IPv6 addresses may be written in brackets.")
    name = (value[:-1] if value.endswith(".") else value).lower()
    if not name or len(name) > 253:
        raise _invalid_host("Host names must be 1-253 characters.")
    labels = name.split(".")
    if any(not _LABEL_RE.fullmatch(label) for label in labels):
        raise _invalid_host("Host name labels allow letters, digits, inner hyphens.")
    if _NUMERIC_LABEL_RE.fullmatch(labels[-1]):
        raise _invalid_host("Host names must not end in a numeric label.")
    return name


def validate_port(port: int) -> int:
    """Return ``port`` unchanged if it is an integer from 1 to 65535.

    Raises:
        DiscoveryError: ``invalid_port`` otherwise.

    """
    if type(port) is not int or not 1 <= port <= 65535:
        raise DiscoveryError(
            "invalid_port", "The SSH port must be an integer from 1 to 65535."
        )
    return port


def known_hosts_host(host: str, port: int = DEFAULT_SSH_PORT) -> str:
    """Return the ``known_hosts`` host token OpenSSH uses for ``host:port``.

    Args:
        host: Host to canonicalize with ``validate_host``.
        port: SSH port.

    Returns:
        ``host`` for port 22, otherwise ``[host]:port``.

    """
    canonical = validate_host(host)
    if validate_port(port) == DEFAULT_SSH_PORT:
        return canonical
    return f"[{canonical}]:{port}"


def decode_public_key_blob(
    public_key_b64: str, expected_type: str | None = None
) -> bytes:
    """Strictly decode an OpenSSH public-key blob.

    Args:
        public_key_b64: Base64 blob as written in ``known_hosts`` or ``.pub``.
        expected_type: If given, the key type name embedded in the blob must
            equal it.

    Returns:
        The decoded blob.

    Raises:
        DiscoveryError: ``invalid_host_key`` for bad base64, a truncated blob,
            or a mismatched embedded type.

    """
    max_chars = (MAX_KEY_BLOB_BYTES * 4) // 3 + 4
    if (
        not isinstance(public_key_b64, str)
        or len(public_key_b64) > max_chars
        or not _BASE64_RE.fullmatch(public_key_b64)
    ):
        raise DiscoveryError("invalid_host_key", "Public key is not valid base64.")
    try:
        blob = base64.b64decode(public_key_b64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise DiscoveryError("invalid_host_key", "Public key is not base64.") from exc
    name_length = struct.unpack_from("!I", blob, 0)[0] if len(blob) >= 4 else 0
    if name_length == 0 or 4 + name_length > len(blob):
        raise DiscoveryError("invalid_host_key", "Public key blob is truncated.")
    embedded = blob[4 : 4 + name_length]
    if expected_type is not None and embedded != expected_type.encode("utf-8"):
        raise DiscoveryError("invalid_host_key", "Public key type does not match.")
    return blob


def fingerprint(public_key_b64: str) -> str:
    """Return ``SHA256:<unpadded base64>`` exactly as ``ssh-keygen -lf`` prints.

    Args:
        public_key_b64: Base64 public-key blob.

    Returns:
        The SHA-256 fingerprint string.

    Raises:
        DiscoveryError: ``invalid_host_key`` if the blob cannot be decoded.

    """
    digest = hashlib.sha256(decode_public_key_blob(public_key_b64)).digest()
    return "SHA256:" + base64.b64encode(digest).decode("ascii").rstrip("=")


@dataclass(frozen=True)
class HostKey:
    """One validated SSH host public key for ``host:port``.

    Attributes:
        host: Host canonicalized with ``validate_host``.
        port: SSH port.
        key_type: One of ``SUPPORTED_HOST_KEY_TYPES``; matches the blob.
        public_key: Base64 key blob.
        fingerprint_sha256: ``SHA256:...``; must match ``public_key``.

    """

    host: str
    port: int
    key_type: str
    public_key: str
    fingerprint_sha256: str

    def __post_init__(self) -> None:
        """Canonicalize the host and reject inconsistent key data."""
        object.__setattr__(self, "host", validate_host(self.host))
        validate_port(self.port)
        if self.key_type not in SUPPORTED_HOST_KEY_TYPES:
            raise DiscoveryError("invalid_host_key", "Unsupported host key type.")
        decode_public_key_blob(self.public_key, self.key_type)
        if self.fingerprint_sha256 != fingerprint(self.public_key):
            raise DiscoveryError("invalid_host_key", "Fingerprint does not match key.")

    @classmethod
    def create(cls, host: str, port: int, key_type: str, public_key: str) -> HostKey:
        """Build a key and compute its fingerprint."""
        return cls(host, port, key_type, public_key, fingerprint(public_key))

    def known_hosts_line(self) -> str:
        """Return ``host type blob`` or ``[host]:port type blob``."""
        token = known_hosts_host(self.host, self.port)
        return f"{token} {self.key_type} {self.public_key}"


def parse_keyscan_output(text: str, host: str, port: int) -> list[HostKey]:
    """Parse ``ssh-keyscan`` stdout into keys for ``host:port``.

    The host field printed by ssh-keyscan is ignored: keys are attributed to the
    validated ``host``/``port`` the caller asked about.

    Args:
        text: ssh-keyscan standard output.
        host: Host that was scanned.
        port: Port that was scanned.

    Returns:
        Deduplicated keys ordered ed25519, ECDSA, RSA. Comments, blank lines,
        unsupported types, and undecodable blobs are skipped.

    """
    canonical = validate_host(host)
    validate_port(port)
    keys: list[HostKey] = []
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 3 or fields[0].startswith("#"):
            continue
        if fields[1] not in SUPPORTED_HOST_KEY_TYPES:
            continue
        try:
            key = HostKey.create(canonical, port, fields[1], fields[2])
        except DiscoveryError:
            continue
        if key not in keys:
            keys.append(key)
    keys.sort(key=lambda key: SUPPORTED_HOST_KEY_TYPES.index(key.key_type))
    return keys


def keyscan_command(
    host: str, *, port: int = DEFAULT_SSH_PORT, timeout: int = 5
) -> list[str]:
    """Return the exact ``ssh-keyscan`` argv for a validated host.

    OpenSSH's ssh-keyscan parses options with ``getopt(3)``, which ends option
    parsing at ``--``: with OpenSSH_10.3p1, ``ssh-keyscan -T 1 --`` prints usage
    rather than scanning a host named ``--``. ``--`` is defense in depth on top
    of ``validate_host`` refusing a leading ``-``.

    Args:
        host: Host to scan.
        port: SSH port.
        timeout: ssh-keyscan ``-T`` seconds, 1-60.

    Returns:
        The argv list.

    Raises:
        DiscoveryError: ``invalid_host``, ``invalid_port``, or
            ``invalid_scan_options``.

    """
    canonical = validate_host(host)
    validate_port(port)
    if type(timeout) is not int or not 1 <= timeout <= MAX_KEYSCAN_TIMEOUT_SECONDS:
        raise DiscoveryError(
            "invalid_scan_options",
            f"Host-key scan timeout must be 1-{MAX_KEYSCAN_TIMEOUT_SECONDS} seconds.",
        )
    return [
        "ssh-keyscan",
        "-T",
        str(timeout),
        "-p",
        str(port),
        "-t",
        KEYSCAN_KEY_TYPES,
        "--",
        canonical,
    ]


def scan_host_keys(
    host: str,
    *,
    port: int = DEFAULT_SSH_PORT,
    timeout: int = 5,
    runner: Runner = subprocess.run,
) -> list[HostKey]:
    """Fetch a node's SSH host keys with ``ssh-keyscan``.

    Args:
        host: Host to scan; validated before anything runs.
        port: SSH port.
        timeout: ssh-keyscan ``-T`` seconds; the subprocess gets a larger cap
            because ssh-keyscan opens one connection per key type.
        runner: ``subprocess.run`` compatible callable, injectable for tests.

    Returns:
        At least one key, ordered ed25519, ECDSA, RSA.

    Raises:
        DiscoveryError: ``host_key_scan_failed`` for a missing binary, a
            timeout, or no supported key in the output (command output is not
            copied into the error); validation codes for bad arguments.

    """
    argv = keyscan_command(host, port=port, timeout=timeout)
    canonical = argv[-1]
    target = known_hosts_host(canonical, port)
    try:
        completed = run_captured(runner, argv, timeout=timeout * 4 + 5)
    except FileNotFoundError as exc:
        raise DiscoveryError(
            "host_key_scan_failed",
            "The ssh-keyscan tool was not found on this computer.",
            "Install the OpenSSH client tools and try again.",
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise DiscoveryError(
            "host_key_scan_failed",
            f"Timed out reading SSH host keys from {target}.",
            "Check that the node is powered on and reachable, then retry.",
        ) from exc
    except OSError as exc:
        raise DiscoveryError(
            "host_key_scan_failed",
            "The ssh-keyscan tool could not be started.",
            "Check the local OpenSSH installation and try again.",
        ) from exc
    stdout = completed.stdout if isinstance(completed.stdout, str) else ""
    keys = parse_keyscan_output(stdout, canonical, port)
    if not keys:
        raise DiscoveryError(
            "host_key_scan_failed",
            f"No supported SSH host keys were received from {target}.",
            f"Check that the node is reachable and sshd listens on port {port}.",
        )
    return keys


@dataclass(frozen=True)
class KnownHostEntry:
    """One key recorded for a host in the operator's own ``known_hosts``.

    Attributes:
        key_type: One of ``SUPPORTED_HOST_KEY_TYPES``.
        fingerprint_sha256: ``SHA256:...`` of the recorded key.
        revoked: The line carries the ``@revoked`` marker.

    """

    key_type: str
    fingerprint_sha256: str
    revoked: bool = False


def known_hosts_lookup_command(
    host: str, port: int, known_hosts_path: Path
) -> list[str]:
    """Return the read-only ``ssh-keygen -F`` argv for a validated host.

    The host token is the single argument of ``-F`` (``validate_host`` already
    refuses a leading ``-``), and nothing passes through a shell.

    Args:
        host: Host to look up.
        port: SSH port; non-22 ports use the ``[host]:port`` token.
        known_hosts_path: Absolute path of the file to search.

    Returns:
        The argv list.

    Raises:
        DiscoveryError: ``invalid_host``, ``invalid_port`` or
            ``invalid_known_hosts_path``.

    """
    path = Path(known_hosts_path)
    if not path.is_absolute():
        raise DiscoveryError(
            "invalid_known_hosts_path", "The known_hosts path must be absolute."
        )
    return ["ssh-keygen", "-F", known_hosts_host(host, port), "-f", str(path)]


def parse_known_hosts_lookup(text: str) -> list[KnownHostEntry]:
    """Parse ``ssh-keygen -F`` output into recorded keys.

    Comment lines, ``@cert-authority`` entries, unsupported types and
    undecodable blobs are skipped; the host field (possibly hashed) is ignored
    because ``ssh-keygen`` already matched it.
    """
    entries: list[KnownHostEntry] = []
    for line in text.splitlines():
        fields = line.split()
        revoked = bool(fields) and fields[0] == "@revoked"
        if revoked:
            fields = fields[1:]
        if len(fields) < 3 or fields[0].startswith(("#", "@")):
            continue
        if fields[1] not in SUPPORTED_HOST_KEY_TYPES:
            continue
        try:
            decode_public_key_blob(fields[2], fields[1])
        except DiscoveryError:
            continue
        entry = KnownHostEntry(fields[1], fingerprint(fields[2]), revoked)
        if entry not in entries:
            entries.append(entry)
    return entries


def user_known_host_keys(
    host: str,
    port: int = DEFAULT_SSH_PORT,
    *,
    known_hosts_path: Path,
    runner: Runner = subprocess.run,
    timeout: int = 5,
) -> list[KnownHostEntry] | None:
    """Read the keys the operator's ``known_hosts`` records for ``host:port``.

    Args:
        host: Host to look up; validated before anything runs.
        port: SSH port.
        known_hosts_path: The operator's ``known_hosts`` (normally
            ``~/.ssh/known_hosts``); it is only read.
        runner: ``subprocess.run`` compatible callable, injectable for tests.
        timeout: Seconds before the lookup is abandoned.

    Returns:
        The recorded keys (empty when the file or host is absent), or None when
        the lookup could not run, for example without ``ssh-keygen``.

    Raises:
        DiscoveryError: Validation codes for bad arguments.

    """
    argv = known_hosts_lookup_command(host, port, known_hosts_path)
    if not Path(known_hosts_path).is_file():
        return []
    try:
        completed = run_captured(runner, argv, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode == 1:
        return []
    if completed.returncode != 0 or not isinstance(completed.stdout, str):
        return None
    return parse_known_hosts_lookup(completed.stdout[:MAX_KNOWN_HOSTS_LOOKUP_BYTES])


def known_hosts_status(key: HostKey, recorded: Sequence[KnownHostEntry] | None) -> str:
    """Compare a scanned key with the operator's recorded keys for that host.

    Returns:
        ``match`` when the same key is recorded, ``mismatch`` when a different
        key of the same type is recorded, ``revoked`` when the key is marked
        ``@revoked``, ``absent`` when no key of that type is recorded, and
        ``unknown`` when the file could not be read.

    """
    if recorded is None:
        return "unknown"
    if any(
        entry.revoked and entry.fingerprint_sha256 == key.fingerprint_sha256
        for entry in recorded
    ):
        return "revoked"
    same_type = [
        entry
        for entry in recorded
        if entry.key_type == key.key_type and not entry.revoked
    ]
    if any(entry.fingerprint_sha256 == key.fingerprint_sha256 for entry in same_type):
        return "match"
    return "mismatch" if same_type else "absent"


def _entry_fields(line: str) -> tuple[list[str], str, str] | None:
    """Return ``(host patterns, key type, blob)`` for a plain entry."""
    fields = line.split()
    if len(fields) < 3 or fields[0].startswith(("#", "@", "|")):
        return None
    if fields[1] not in SUPPORTED_HOST_KEY_TYPES:
        return None
    return fields[0].split(","), fields[1], fields[2]


class KnownHostsStore:
    """A controller-owned OpenSSH ``known_hosts`` file of approved host keys.

    Writes are atomic (temporary file + ``os.replace``), produce mode 0600, and
    create a missing parent directory with mode 0700. Lines this class does not
    understand are preserved. A process-wide lock serializes read-modify-write.
    """

    def __init__(self, path: Path) -> None:
        """Bind the store to ``path``; nothing is read or created yet."""
        self.path = Path(path)

    def _read_lines(self) -> list[str]:
        try:
            with self.path.open("r", encoding="utf-8", errors="replace") as handle:
                data = handle.read(MAX_KNOWN_HOSTS_BYTES + 1)
        except FileNotFoundError:
            return []
        if len(data) > MAX_KNOWN_HOSTS_BYTES:
            raise DiscoveryError(
                "known_hosts_too_large",
                "The controller known_hosts file is unexpectedly large.",
                "Inspect the file and remove stale entries.",
            )
        return data.splitlines()

    def trust(self, keys: Iterable[HostKey]) -> None:
        """Append every key that is not already stored; never duplicates.

        Args:
            keys: Keys the operator explicitly approved.

        """
        pending = list(keys)
        if not pending:
            return
        with _STORE_LOCK:
            lines = self._read_lines()
            present: set[tuple[str, str, str]] = set()
            for line in lines:
                fields = _entry_fields(line)
                if fields is not None:
                    patterns, key_type, blob = fields
                    present.update((p.lower(), key_type, blob) for p in patterns)
            additions: list[str] = []
            for key in pending:
                token = known_hosts_host(key.host, key.port).lower()
                identity = (token, key.key_type, key.public_key)
                if identity not in present:
                    present.add(identity)
                    additions.append(key.known_hosts_line())
            if additions:
                write_private_file(
                    self.path,
                    "".join(f"{line}\n" for line in [*lines, *additions]),
                    prefix=".known_hosts.",
                )

    def trusted_keys(self, host: str, port: int = DEFAULT_SSH_PORT) -> list[HostKey]:
        """Return the stored keys whose host token exactly matches ``host:port``."""
        canonical = validate_host(host)
        token = known_hosts_host(canonical, port).lower()
        with _STORE_LOCK:
            lines = self._read_lines()
        keys: list[HostKey] = []
        for line in lines:
            fields = _entry_fields(line)
            if fields is None or token not in (p.lower() for p in fields[0]):
                continue
            try:
                key = HostKey.create(canonical, port, fields[1], fields[2])
            except DiscoveryError:
                continue
            if key not in keys:
                keys.append(key)
        return keys

    def is_trusted(self, key: HostKey) -> bool:
        """Return whether exactly this key is stored for its host and port."""
        return key in self.trusted_keys(key.host, key.port)


def ssh_config_path_value(path: str) -> str:
    r"""Encode one file path for a multi-path ``ssh_config`` value.

    Verified with OpenSSH_10.3p1 via ``ssh -G`` (which never connects):

    * ``UserKnownHostsFile`` splits on unquoted whitespace, so a path such as
      ``.../Application Support/...`` must be double-quoted. Twenty quoted
      ``"/a b/N"`` entries are accepted, while the same entries unquoted exceed
      the 32-file limit (``too many userknownhostsfile entries``).
    * Inside double quotes, ``\"`` and ``\\`` are escapes; ``#`` and ``'`` are
      literal.
    * ``%`` starts a TOKENS expansion and must be written ``%%``; an unknown
      ``%x`` aborts ssh.
    * ``${NAME}`` is always expanded from the environment and has no escape.

    Args:
        path: Absolute file path.

    Returns:
        The path, double-quoted and escaped when needed.

    Raises:
        DiscoveryError: ``invalid_known_hosts_path`` for paths containing
            ``${`` or control characters, which cannot be passed safely.

    """
    if not path or "${" in path or any(ord(c) < 0x20 or ord(c) == 0x7F for c in path):
        raise DiscoveryError(
            "invalid_known_hosts_path",
            "The known_hosts path cannot be passed safely to ssh.",
            "Use a path without control characters or '${'.",
        )
    escaped = path.replace("%", "%%")
    if _PLAIN_CONFIG_PATH_RE.fullmatch(path):
        return escaped
    return '"' + escaped.replace("\\", "\\\\").replace('"', '\\"') + '"'


def ssh_known_hosts_options(store_path: Path) -> tuple[str, ...]:
    """Return ssh ``-o`` argv pairs for strict checking against approved keys.

    On macOS the second item looks like ``UserKnownHostsFile=~/.ssh/known_hosts
    "/Users/me/Library/Application Support/.../known_hosts"``. With
    ``StrictHostKeyChecking=yes`` ssh never adds or accepts an unknown or
    changed key; overriding ``UserKnownHostsFile`` also disables OpenSSH's
    automatic ``UpdateHostKeys``.

    Args:
        store_path: Absolute path of the controller ``known_hosts`` store.

    Returns:
        ``("-o", "UserKnownHostsFile=...", "-o", "StrictHostKeyChecking=yes")``.

    Raises:
        DiscoveryError: ``invalid_known_hosts_path`` for a relative or
            unrepresentable path.

    """
    path = Path(store_path)
    if not path.is_absolute():
        raise DiscoveryError(
            "invalid_known_hosts_path", "The known_hosts store path must be absolute."
        )
    return (
        "-o",
        f"UserKnownHostsFile={USER_KNOWN_HOSTS_FILE} "
        + ssh_config_path_value(str(path)),
        "-o",
        "StrictHostKeyChecking=yes",
    )
