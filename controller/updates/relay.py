# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""The update relay: the only way nodes download system updates.

Nodes need no Internet access. While a check, download or apply runs, the
controller serves this loopback HTTP proxy and forwards each node's
``127.0.0.1:18877`` to it over SSH (see :mod:`controller.updates.nodeops`).
The node's update helper sends every apt and fwupd download here:

* ``GET http://<host>/<path>`` for a repository or LVFS file. In ``online``
  mode the relay fetches it from the host's upstream scheme (https for every
  host a node source names with https) and keeps a copy in the cache, laid
  out as ``<cache>/<host>/<path>``. Package and firmware files are served
  from the cache when present; index files are always fetched fresh. In
  ``directory`` mode the relay serves ``<directory>/<host>/<path>`` and never
  opens a network connection, so a cache copied from an online controller,
  or an apt-mirror tree, works as an offline source.
* ``CONNECT <host>:443`` in ``online`` mode only, for sources that need
  credentials (Ubuntu Pro). The relay only tunnels bytes; TLS and the
  credentials stay end to end between the node and the host.

Only hosts the nodes' own apt sources name, plus the LVFS hosts, are
allowed; redirects may move to another host of the same site. The relay
trusts nothing it serves: apt verifies signed indexes and package hashes,
and fwupd verifies signed metadata and firmware, on the node.
"""

from __future__ import annotations

import contextlib
import http.client
import ipaddress
import os
import re
import secrets
import select
import shutil
import socket
import ssl
import tempfile
import threading
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit

CONTROL_HOST = "dgx-spark-controller.invalid"
LVFS_HOSTS = {"cdn.fwupd.org": "https", "fwupd.org": "https"}
SOURCE_KINDS = frozenset({"online", "directory"})
# Content-addressed files that never change once published.
IMMUTABLE_SUFFIXES = (".deb", ".udeb", ".ddeb", ".cab")
MAX_REDIRECTS = 5
MAX_FILE_BYTES = 8 * 1024**3
CHUNK_BYTES = 1024 * 1024
_COMPONENT_RE = re.compile(r"[A-Za-z0-9._~+@:=,-]{1,255}\Z")
HOST_RE = re.compile(r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}\Z")


class RelayError(Exception):
    """A request the relay refuses, with the HTTP status to answer."""

    def __init__(self, status: int, reason: str) -> None:
        super().__init__(reason)
        self.status = status
        self.reason = reason


@dataclass(frozen=True)
class RelaySource:
    """Where the relay takes files from: the Internet or a local directory."""

    kind: str = "online"
    directory: Path | None = None

    def __post_init__(self) -> None:
        if self.kind not in SOURCE_KINDS:
            raise ValueError("relay source kind must be online or directory")
        if (self.kind == "directory") != (self.directory is not None):
            raise ValueError("a directory source needs exactly one directory")


@dataclass
class RelayStats:
    """What one relay session served, for operation evidence."""

    files: int = 0
    bytes: int = 0
    cache_hits: int = 0
    upstream_bytes: int = 0
    tunnels: int = 0
    missing: int = 0
    errors: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add(self, **counts: int) -> None:
        with self._lock:
            for name, value in counts.items():
                setattr(self, name, getattr(self, name) + value)

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "files": self.files,
                "bytes": self.bytes,
                "cache_hits": self.cache_hits,
                "upstream_bytes": self.upstream_bytes,
                "tunnels": self.tunnels,
                "missing": self.missing,
                "errors": self.errors,
            }


@dataclass(frozen=True)
class RelaySession:
    """A running relay: its loopback port, its proof token, its statistics.

    The token is served on a control path the helper never asks for, so the
    controller can prove a node's forwarded port reaches this relay and not
    some other listener on that port.
    """

    port: int
    token: str
    stats: RelayStats


def relative_path(path: str) -> tuple[str, ...]:
    """Validated components of a URL path; raises ``RelayError`` otherwise."""
    if not path.startswith("/") or len(path) > 2048:
        raise RelayError(400, "invalid path")
    components = tuple(unquote(path[1:]).split("/"))
    if not components or any(
        part in {".", ".."} or not _COMPONENT_RE.fullmatch(part) for part in components
    ):
        raise RelayError(400, "invalid path")
    return components


def same_site(host: str, allowed: Mapping[str, str]) -> bool:
    """Whether ``host`` is allowed or shares the last two labels of one."""
    if host in allowed:
        return True
    site = ".".join(host.split(".")[-2:])
    return any(".".join(name.split(".")[-2:]) == site for name in allowed)


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, relay: _Session) -> None:
        self.relay = relay
        super().__init__(("127.0.0.1", 0), _Handler)


@dataclass
class _Session:
    source: RelaySource
    hosts: Mapping[str, str]
    cache_dir: Path
    stats: RelayStats
    context: ssl.SSLContext
    token: str


class UpdateRelay:
    """Serve update downloads to the nodes for the duration of one session."""

    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir

    @contextlib.contextmanager
    def serving(
        self, source: RelaySource, hosts: Mapping[str, str]
    ) -> Iterator[RelaySession]:
        """Run the relay for the body's duration.

        Args:
            source: Internet (with the cache) or a local directory.
            hosts: Repository host -> upstream scheme from the nodes' sources.

        """
        allowed = {
            host: scheme
            for host, scheme in {**hosts, **LVFS_HOSTS}.items()
            if HOST_RE.fullmatch(host) and scheme in {"http", "https"}
        }
        if source.kind == "online":
            self.cache_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        stats = RelayStats()
        session = _Session(
            source,
            allowed,
            self.cache_dir,
            stats,
            ssl.create_default_context(),
            secrets.token_hex(16),
        )
        server = _Server(session)
        thread = threading.Thread(
            target=server.serve_forever,
            args=(0.05,),
            name="dgx-update-relay",
            daemon=True,
        )
        thread.start()
        try:
            yield RelaySession(server.server_address[1], session.token, stats)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


class _Handler(BaseHTTPRequestHandler):
    server: _Server
    protocol_version = "HTTP/1.1"
    timeout = 300

    def log_message(self, _format: str, *_args: object) -> None:
        """Never log request lines: they name hosts and files on the nodes."""

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler callback
        session = self.server.relay
        try:
            host, components = self._target()
            if host == CONTROL_HOST:
                control = {"mode": session.source.kind, "token": session.token}
                if len(components) != 1 or components[0] not in control:
                    raise RelayError(404, "unknown control path")
                self._send_bytes(control[components[0]].encode("ascii"))
                return
            if host not in session.hosts:
                raise RelayError(403, "host not allowed")
            if session.source.kind == "directory":
                assert session.source.directory is not None
                self._send_file(_inside(session.source.directory, host, components))
                return
            cached = _inside(session.cache_dir, host, components, must_exist=False)
            # Only a file that came over TLS is trusted to be what it names;
            # apt and fwupd still verify it, but a bad copy would stay forever.
            if (
                components[-1].endswith(IMMUTABLE_SUFFIXES)
                and session.hosts[host] == "https"
                and cached.is_file()
            ):
                session.stats.add(cache_hits=1)
                self._send_file(cached)
                return
            self._relay_upstream(session.hosts[host], host, components, cached)
        except RelayError as error:
            if error.status == 404:
                session.stats.add(missing=1)
            self._send_error(error.status, error.reason)
        except (OSError, ValueError, http.client.HTTPException):
            session.stats.add(errors=1)
            with contextlib.suppress(OSError):
                self._send_error(502, "upstream failed")

    def do_CONNECT(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler callback
        session = self.server.relay
        host, _, port = self.path.partition(":")
        host = host.lower()
        if (
            session.source.kind != "online"
            or port != "443"
            or host not in session.hosts
        ):
            self._send_error(403, "tunnel not allowed")
            return
        try:
            upstream = socket.create_connection((host, 443), timeout=30)
        except OSError:
            self._send_error(502, "upstream unreachable")
            return
        if not _public_peer(upstream):
            upstream.close()
            self._send_error(403, "upstream address not allowed")
            return
        session.stats.add(tunnels=1)
        self.send_response(200, "Connection Established")
        self.end_headers()
        self.close_connection = True
        _pump(self.connection, upstream)

    def do_HEAD(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler callback
        self._send_error(405, "method not allowed")

    def _target(self) -> tuple[str, tuple[str, ...]]:
        parts = urlsplit(self.path)
        host = (parts.hostname or "").lower()
        try:
            port = parts.port
        except ValueError as error:
            raise RelayError(400, "invalid port") from error
        if (
            parts.scheme != "http"
            or parts.username
            or parts.password
            or port not in {None, 80}
            or parts.query
            or parts.fragment
            or not host
        ):
            raise RelayError(400, "absolute http URL required")
        return host, relative_path(parts.path)

    def _relay_upstream(
        self, scheme: str, host: str, components: tuple[str, ...], cached: Path
    ) -> None:
        session = self.server.relay
        url = f"{scheme}://{host}/{'/'.join(components)}"
        for _ in range(MAX_REDIRECTS + 1):
            response, connection = _open(url, session.context)
            try:
                if response.status in {301, 302, 303, 307, 308}:
                    location = response.getheader("Location") or ""
                    url = urljoin(url, location)
                    target = urlsplit(url)
                    if (
                        target.scheme not in {"http", "https"}
                        or target.port is not None
                        or not same_site((target.hostname or "").lower(), session.hosts)
                    ):
                        raise RelayError(502, "redirect not allowed")
                    continue
                if response.status == 404:
                    raise RelayError(404, "not found upstream")
                if response.status != 200:
                    raise RelayError(502, f"upstream status {response.status}")
                # A directory listing is not a file; never cache one.
                self._stream(response, None if url.endswith("/") else cached)
                return
            finally:
                connection.close()
        raise RelayError(502, "too many redirects")

    def _stream(self, response: http.client.HTTPResponse, cached: Path | None) -> None:
        session = self.server.relay
        length_header = response.getheader("Content-Length")
        length = (
            int(length_header) if length_header and length_header.isdigit() else None
        )
        if length is not None and length > MAX_FILE_BYTES:
            raise RelayError(502, "file too large")
        handle = None
        if cached is not None:
            # The download is served even when it cannot be cached.
            with contextlib.suppress(OSError):
                cached.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                handle = tempfile.NamedTemporaryFile(  # noqa: SIM115 - renamed below
                    dir=cached.parent, prefix=".relay-", delete=False
                )
        complete = False
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            if length is None:
                self.send_header("Connection", "close")
                self.close_connection = True
            else:
                self.send_header("Content-Length", str(length))
            self.end_headers()
            received = 0
            while chunk := response.read(CHUNK_BYTES):
                received += len(chunk)
                if received > MAX_FILE_BYTES:
                    raise RelayError(502, "file too large")
                if handle is not None:
                    try:
                        handle.write(chunk)
                    except OSError:
                        # A full or broken cache never fails the download.
                        handle.close()
                        with contextlib.suppress(OSError):
                            os.unlink(handle.name)
                        handle = None
                self.wfile.write(chunk)
            # Without a length only a completed chunked body proves the
            # download whole; anything else is served but never cached.
            complete = received == length if length is not None else response.chunked
            session.stats.add(files=1, bytes=received, upstream_bytes=received)
        finally:
            if not complete:
                self.close_connection = True
            if handle is not None:
                handle.close()
                try:
                    if not complete or cached is None:
                        raise OSError("incomplete download")
                    os.replace(handle.name, cached)
                except OSError:
                    with contextlib.suppress(OSError):
                        os.unlink(handle.name)

    def _send_file(self, path: Path) -> None:
        if not path.is_file():
            raise RelayError(404, "not found")
        size = path.stat().st_size
        with path.open("rb") as handle:
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(size))
            self.end_headers()
            shutil.copyfileobj(handle, self.wfile, CHUNK_BYTES)
        self.server.relay.stats.add(files=1, bytes=size)

    def _send_bytes(self, body: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=ascii")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle_one_request(self) -> None:
        self._responded = False
        super().handle_one_request()

    def send_response(self, code: int, message: str | None = None) -> None:
        self._responded = True
        super().send_response(code, message)

    def _send_error(self, status: int, reason: str) -> None:
        if self._responded:
            # A body was already under way: the only honest signal left is
            # closing the connection, so the client sees a short download.
            self.close_connection = True
            return
        body = reason.encode("ascii", "replace")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=ascii")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _inside(
    root: Path, host: str, components: tuple[str, ...], *, must_exist: bool = True
) -> Path:
    """``root/host/components`` after proving it cannot leave ``root``."""
    base = root.resolve()
    path = base.joinpath(host, *components)
    resolved = path.resolve() if must_exist else path.parent.resolve() / path.name
    if not resolved.is_relative_to(base):
        raise RelayError(403, "outside the source")
    return resolved


def _open(
    url: str, context: ssl.SSLContext
) -> tuple[http.client.HTTPResponse, http.client.HTTPConnection]:
    target = urlsplit(url)
    host = target.hostname or ""
    connection: http.client.HTTPConnection
    if target.scheme == "https":
        connection = http.client.HTTPSConnection(
            host, target.port or 443, timeout=120, context=context
        )
    else:
        connection = http.client.HTTPConnection(host, target.port or 80, timeout=120)
    connection.connect()
    if connection.sock is None or not _public_peer(connection.sock):
        connection.close()
        raise RelayError(403, "upstream address not allowed")
    path = target.path or "/"
    if target.query:
        path = f"{path}?{target.query}"
    connection.request(
        "GET",
        path,
        headers={
            "User-Agent": "dgx-spark-controller-update-relay",
            "Accept-Encoding": "identity",
        },
    )
    return connection.getresponse(), connection


def _public_peer(sock: socket.socket) -> bool:
    """Whether an upstream connection left this host and its link.

    A repository name in a node's sources must not reach the controller host's
    own loopback or link-local services through the relay.
    """
    address = ipaddress.ip_address(sock.getpeername()[0])
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        address = address.ipv4_mapped
    return not (address.is_loopback or address.is_link_local or address.is_unspecified)


def _pump(client: socket.socket, upstream: socket.socket) -> None:
    """Copy bytes both ways until either side closes or stays idle."""
    sockets = [client, upstream]
    try:
        while True:
            readable, _, broken = select.select(sockets, [], sockets, 300)
            if broken or not readable:
                return
            for sock in readable:
                data = sock.recv(65536)
                if not data:
                    return
                (upstream if sock is client else client).sendall(data)
    except OSError:
        return
    finally:
        upstream.close()
