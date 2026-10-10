# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

r"""Bounded, read-only LAN probing that ranks likely DGX Spark nodes.

Safety boundary: every target is re-validated with
``network.validate_scan_target`` (private IPv4 only, at most a /22 each) and the
whole scan is capped at ``MAX_TOTAL_HOSTS`` addresses. Probes are plain TCP
connects, one SSH identification-banner read (no key exchange, no
authentication, nothing written), reverse DNS, and an unauthenticated
``GET /v1/models`` on the well-known inference ports of hosts that already
answered on port 22. Every probe has a hard deadline and a byte cap, HTTP
requests ignore proxy settings, and everything a remote host returns is treated
as untrusted text: it is stripped of control characters and truncated before it
appears in a ``Candidate``.

Classification heuristics (``classify``):

* Strong hostname evidence (case-insensitive, any reverse-DNS or mDNS name):
  ``^spark-[0-9a-f]{4}\b`` (DGX Spark factory hostname), ``^gx10-`` (ASUS
  Ascent GX10), ``gb10`` (the GB10 platform), ``ascent`` (ASUS), and
  ``promax`` (Dell Pro Max with GB10).
* Medium hostname evidence: a generic ``dgx`` or ``spark`` in the name.
* Weak evidence: an SSH banner containing ``Ubuntu`` (DGX OS is Ubuntu-based)
  and a model listed by ``/v1/models`` on a vLLM/llama.cpp/Ollama port.
* SSH reachability is required for any positive result.

``high`` = strong hostname + SSH; ``medium`` = generic hostname + SSH, or
Ubuntu banner + served model + SSH; ``low`` otherwise. ``likely_spark`` is true
exactly for ``high`` and ``medium``.
"""

from __future__ import annotations

import concurrent.futures
import ipaddress
import json
import re
import socket
import threading
import time
import unicodedata
from collections.abc import Callable, Collection, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from . import mdns as _mdns
from .network import MAX_SCAN_HOSTS, DiscoveryError, validate_scan_target

try:
    import resource as _resource
except ImportError:  # pragma: no cover - non-POSIX platforms
    _resource = None  # type: ignore[assignment]

SSH_PORT = 22
MODEL_PORTS = (8000, 8888, 11434)
MAX_TOTAL_HOSTS = MAX_SCAN_HOSTS * 4
MAX_WORKERS = 256
MAX_CONNECT_TIMEOUT_SECONDS = 5.0
BANNER_TIMEOUT_SECONDS = 1.0
BANNER_MAX_BYTES = 256
BANNER_MAX_CHARS = 255
REVERSE_DNS_TIMEOUT_SECONDS = 1.0
REVERSE_DNS_WORKERS = 16
HTTP_TIMEOUT_SECONDS = 1.5
HTTP_MAX_BODY_BYTES = 64 * 1024
HTTP_MAX_HEADER_BYTES = 8 * 1024
MDNS_WAIT_SECONDS = 3.0
MAX_REASON_TEXT = 120
MAX_MODEL_ID_CHARS = 200
MAX_MODELS_PER_PORT = 32
_RESERVED_DESCRIPTORS = 64

CONFIDENCE_HIGH = "high"
CONFIDENCE_MEDIUM = "medium"
CONFIDENCE_LOW = "low"

_STRONG = 2
_GENERIC = 1

_STRONG_HOSTNAME_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"^spark-[0-9a-f]{4}\b", re.IGNORECASE),
        "the DGX Spark factory hostname pattern 'spark-xxxx'",
    ),
    (
        re.compile(r"^gx10-", re.IGNORECASE),
        "the ASUS Ascent GX10 hostname pattern 'gx10-'",
    ),
    (re.compile(r"gb10", re.IGNORECASE), "the NVIDIA GB10 platform name"),
    (re.compile(r"ascent", re.IGNORECASE), "the ASUS Ascent product name"),
    (re.compile(r"promax", re.IGNORECASE), "the Dell Pro Max (GB10) product name"),
)
_GENERIC_HOSTNAME_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"dgx", re.IGNORECASE), "the generic name 'dgx'"),
    (re.compile(r"spark", re.IGNORECASE), "the generic name 'spark'"),
)
_STATUS_LINE_RE = re.compile(rb"HTTP/1\.[01] (\d{3})(?: [^\r\n]*)?")

ConnectFn = Callable[[str, int, float], bool]
BannerFn = Callable[[str, int, float], str | bytes | None]
ReverseDnsFn = Callable[[str], str | None]
HttpGetJsonFn = Callable[[str, float], Any]
MdnsBrowseFn = Callable[[], Iterable[_mdns.MdnsService]]
Executor = concurrent.futures.ThreadPoolExecutor


@dataclass(frozen=True)
class Candidate:
    """One host that answered on TCP port 22, with sanitized evidence.

    Attributes:
        address: IPv4 address.
        hostname: Reverse-DNS or mDNS name without the trailing dot.
        ssh_open: Whether TCP port 22 accepted a connection.
        ssh_banner: First SSH identification line, printable ASCII, <= 255 chars.
        open_ports: Open ports among 22 and ``MODEL_PORTS``.
        served_models: Model ids from ``/v1/models`` on open model ports.
        likely_spark: True for ``high`` and ``medium`` confidence.
        confidence: ``"high"``, ``"medium"``, or ``"low"``.
        reasons: Short human-readable evidence strings.

    """

    address: str
    hostname: str | None
    ssh_open: bool
    ssh_banner: str | None
    open_ports: tuple[int, ...]
    served_models: tuple[str, ...]
    likely_spark: bool
    confidence: str
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-ready mapping."""
        return {
            "address": self.address,
            "hostname": self.hostname,
            "ssh_open": self.ssh_open,
            "ssh_banner": self.ssh_banner,
            "open_ports": list(self.open_ports),
            "served_models": list(self.served_models),
            "likely_spark": self.likely_spark,
            "confidence": self.confidence,
            "reasons": list(self.reasons),
        }


def safe_text(value: str, limit: int = MAX_REASON_TEXT) -> str:
    """Strip control/format characters and truncate untrusted text.

    Args:
        value: Untrusted text from a remote host.
        limit: Maximum length of the result, including a ``...`` marker.

    Returns:
        Cleaned text of at most ``limit`` characters.

    """
    kept = (char for char in value if not unicodedata.category(char).startswith("C"))
    cleaned = "".join(kept).strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: max(0, limit - 3)] + "..."


def sanitize_banner(raw: str | bytes | None) -> str | None:
    """Return the SSH identification line from raw banner bytes.

    RFC 4253 lets a server send other lines before ``SSH-``; the first such line
    wins, otherwise the first non-empty line is used.

    Args:
        raw: Bytes or text read from port 22.

    Returns:
        Printable ASCII of at most ``BANNER_MAX_CHARS`` characters, or ``None``.

    """
    if raw is None:
        return None
    text = raw.decode("latin-1") if isinstance(raw, bytes | bytearray) else str(raw)
    lines = [line.strip("\r") for line in text[: BANNER_MAX_BYTES * 4].split("\n")]
    chosen = next((line for line in lines if line.startswith("SSH-")), None)
    if chosen is None:
        chosen = next((line for line in lines if line.strip()), "")
    printable = "".join(char for char in chosen if 0x20 <= ord(char) < 0x7F)
    return printable.strip()[:BANNER_MAX_CHARS] or None


def _clean_names(values: Iterable[object]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        cleaned = safe_text(value.strip().rstrip("."), 10_000)
        if cleaned and len(cleaned) <= 253 and cleaned.lower() not in seen:
            seen.add(cleaned.lower())
            names.append(cleaned)
    return names


def _clean_models(values: Iterable[object]) -> tuple[str, ...]:
    models: list[str] = []
    for value in values:
        cleaned = safe_text(value, MAX_MODEL_ID_CHARS) if isinstance(value, str) else ""
        if cleaned and cleaned not in models:
            models.append(cleaned)
    return tuple(models)


def _match_hostname(name: str) -> tuple[int, str] | None:
    for pattern, label in _STRONG_HOSTNAME_PATTERNS:
        if pattern.search(name):
            return _STRONG, label
    for pattern, label in _GENERIC_HOSTNAME_PATTERNS:
        if pattern.search(name):
            return _GENERIC, label
    return None


def classify(
    address: str,
    *,
    hostname: str | None,
    ssh_banner: str | bytes | None,
    open_ports: Iterable[int],
    served_models: Iterable[str],
    mdns_names: Iterable[str] = (),
    ssh_open: bool | None = None,
) -> Candidate:
    """Score one host from already-collected evidence; performs no I/O.

    Args:
        address: IPv4 address of the host.
        hostname: Reverse-DNS name, if any.
        ssh_banner: Raw SSH banner; sanitized here.
        open_ports: Ports that accepted a TCP connection.
        served_models: Model ids reported by ``/v1/models``.
        mdns_names: Host names learned from mDNS for this address.
        ssh_open: Override for SSH reachability; by default port 22 must be in
            ``open_ports`` or a banner must have been read.

    Returns:
        The candidate. ``hostname`` is the name with the strongest pattern
        match, else the reverse-DNS name, else the first mDNS name.

    Raises:
        ValueError: If ``address`` is not an IPv4 address.

    """
    ip = ipaddress.IPv4Address(str(address).strip())
    ports = tuple(sorted({p for p in open_ports if type(p) is int and 0 < p < 65536}))
    banner = sanitize_banner(ssh_banner)
    if ssh_open is None:
        ssh_open = SSH_PORT in ports or banner is not None
    models = _clean_models(served_models)
    names = _clean_names([hostname, *mdns_names])

    reasons: list[str] = []
    strength = 0
    best_name: str | None = None
    for name in names:
        match = _match_hostname(name)
        if match is None:
            continue
        reasons.append(f"hostname '{safe_text(name)}' matches {match[1]}")
        if match[0] > strength:
            strength, best_name = match[0], name
    if ssh_open:
        reasons.append(f"SSH port {SSH_PORT} is open")
    else:
        reasons.append(f"SSH port {SSH_PORT} did not answer; onboarding needs SSH")
    ubuntu = banner is not None and "ubuntu" in banner.lower()
    if ubuntu:
        reasons.append("SSH banner reports Ubuntu (DGX OS is Ubuntu-based)")
    if models:
        listed = safe_text(", ".join(models))
        reasons.append(f"serves model(s) {listed} on an inference port")

    if not ssh_open:
        confidence = CONFIDENCE_LOW
    elif strength == _STRONG:
        confidence = CONFIDENCE_HIGH
    elif strength == _GENERIC or (ubuntu and models):
        confidence = CONFIDENCE_MEDIUM
    else:
        confidence = CONFIDENCE_LOW
    if best_name is None and names:
        best_name = names[0]
    return Candidate(
        address=str(ip),
        hostname=best_name,
        ssh_open=bool(ssh_open),
        ssh_banner=banner,
        open_ports=ports,
        served_models=models,
        likely_spark=confidence in (CONFIDENCE_HIGH, CONFIDENCE_MEDIUM),
        confidence=confidence,
        reasons=tuple(reasons),
    )


def tcp_connect(address: str, port: int, timeout: float) -> bool:
    """Return whether a TCP handshake to ``address:port`` completes in time."""
    try:
        with socket.create_connection((address, port), timeout=timeout):
            return True
    except OSError:
        return False


def read_ssh_banner(address: str, port: int, timeout: float) -> str | None:
    """Read at most 256 bytes or one line of the SSH identification.

    Nothing is sent; the connection is closed before key exchange.

    Args:
        address: Host address.
        port: SSH port.
        timeout: Overall deadline in seconds.

    Returns:
        The raw text read (Latin-1 decoded), or ``None`` if nothing arrived.

    """
    deadline = time.monotonic() + timeout
    buffer = b""
    try:
        with socket.create_connection((address, port), timeout=timeout) as conn:
            while len(buffer) < BANNER_MAX_BYTES and b"\n" not in buffer:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                conn.settimeout(remaining)
                chunk = conn.recv(BANNER_MAX_BYTES - len(buffer))
                if not chunk:
                    break
                buffer += chunk
    except OSError:
        pass
    return buffer.decode("latin-1") if buffer else None


def reverse_dns_lookup(address: str) -> str | None:
    """Return the PTR name for ``address`` via the system resolver, or ``None``."""
    try:
        name = socket.gethostbyaddr(address)[0]
    except (OSError, UnicodeError):
        return None
    return name if name and name != address else None


def _header_value(head: bytes, wanted: bytes) -> bytes | None:
    for line in head.split(b"\r\n")[1:]:
        key, _, value = line.partition(b":")
        if key.strip().lower() == wanted:
            return value.strip()
    return None


def _response_complete(buffer: bytes) -> bool:
    head, separator, body = buffer.partition(b"\r\n\r\n")
    if not separator:
        return False
    length = _header_value(head, b"content-length")
    if length is not None:
        return length.isdigit() and len(body) >= int(length)
    encoding = _header_value(head, b"transfer-encoding") or b""
    return b"chunked" in encoding.lower() and body.endswith(b"0\r\n\r\n")


def _dechunk(body: bytes) -> bytes | None:
    result = bytearray()
    position = 0
    while True:
        line_end = body.find(b"\r\n", position)
        if line_end < 0:
            return None
        size_text = body[position:line_end].split(b";", 1)[0].strip()
        try:
            size = int(size_text, 16)
        except ValueError:
            return None
        position = line_end + 2
        if size == 0:
            return bytes(result)
        if size < 0 or position + size + 2 > len(body):
            return None
        result += body[position : position + size]
        position += size + 2
        if len(result) > HTTP_MAX_BODY_BYTES:
            return None


def _read_response(host: str, port: int, target: str, timeout: float) -> bytes:
    deadline = time.monotonic() + timeout
    request = (
        f"GET {target} HTTP/1.0\r\nHost: {host}:{port}\r\n"
        "Accept: application/json\r\nUser-Agent: dgx-spark-controller-discovery\r\n"
        "Connection: close\r\n\r\n"
    ).encode("ascii")
    limit = HTTP_MAX_BODY_BYTES + HTTP_MAX_HEADER_BYTES
    buffer = bytearray()
    with socket.create_connection((host, port), timeout=timeout) as conn:
        conn.settimeout(max(0.001, deadline - time.monotonic()))
        conn.sendall(request)
        while not _response_complete(bytes(buffer)):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("HTTP probe deadline exceeded")
            conn.settimeout(remaining)
            chunk = conn.recv(16 * 1024)
            if not chunk:
                break
            buffer += chunk
            if len(buffer) > limit:
                raise ValueError("HTTP response exceeds the size cap")
    return bytes(buffer)


def _http_get(host: str, port: int, target: str, timeout: float) -> bytes | None:
    """Return the body of a 200 response from a bounded HTTP/1.0 GET.

    A raw socket is used instead of ``urllib`` so proxy environment variables
    never route LAN probes elsewhere and a slow-drip server cannot hold a worker
    past one overall deadline.
    """
    try:
        raw = _read_response(host, port, target, timeout)
    except (OSError, ValueError):
        return None
    head, separator, body = raw.partition(b"\r\n\r\n")
    status = _STATUS_LINE_RE.fullmatch(head.split(b"\r\n", 1)[0])
    if not separator or len(head) > HTTP_MAX_HEADER_BYTES or status is None:
        return None
    if status.group(1) != b"200":
        return None
    encoding = _header_value(head, b"transfer-encoding") or b""
    length = _header_value(head, b"content-length")
    if b"chunked" in encoding.lower():
        decoded = _dechunk(body)
        if decoded is None:
            return None
        body = decoded
    elif length is not None:
        if not length.isdigit() or len(body) < int(length):
            return None
        body = body[: int(length)]
    return body if len(body) <= HTTP_MAX_BODY_BYTES else None


def fetch_json(url: str, timeout: float) -> Any:
    """GET ``http://<IP literal>:<port>/<path>`` and parse a JSON 200 response.

    Host names are refused so the probe never triggers a DNS lookup.

    Args:
        url: Absolute ``http`` URL with an IP literal and explicit port.
        timeout: Overall deadline in seconds.

    Returns:
        Parsed JSON, or ``None`` on any failure or non-200 status.

    """
    parts = urlsplit(url)
    try:
        port = parts.port
        ipaddress.ip_address(parts.hostname or "")
    except ValueError:
        return None
    if parts.scheme != "http" or parts.hostname is None or port is None:
        return None
    target = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
    if any(not 0x21 <= ord(char) <= 0x7E for char in target):
        return None
    body = _http_get(parts.hostname, port, target, timeout)
    if body is None:
        return None
    try:
        return json.loads(body.decode("utf-8"))
    except ValueError:  # includes UnicodeDecodeError and JSONDecodeError
        return None


def model_ids(document: Any) -> tuple[str, ...]:
    """Extract sanitized ``data[].id`` values from a ``/v1/models`` document."""
    if not isinstance(document, dict) or not isinstance(document.get("data"), list):
        return ()
    ids = [
        entry["id"]
        for entry in document["data"][:MAX_MODELS_PER_PORT]
        if isinstance(entry, dict) and isinstance(entry.get("id"), str)
    ]
    return _clean_models(ids)


def _host_addresses(network: ipaddress.IPv4Network) -> Iterable[ipaddress.IPv4Address]:
    return iter(network) if network.prefixlen >= 31 else network.hosts()


def expand_targets(
    targets: Sequence[ipaddress.IPv4Network | str], exclude: Collection[str] = ()
) -> list[str]:
    """Validate every target and list the unique host addresses to probe.

    Args:
        targets: Networks (or CIDR strings) to scan.
        exclude: Addresses to skip, e.g. this computer's own addresses.

    Returns:
        Host addresses in target order; network and broadcast addresses are
        skipped for prefixes shorter than /31.

    Raises:
        DiscoveryError: From ``validate_scan_target``, or
            ``scan_target_too_large`` above ``MAX_TOTAL_HOSTS`` addresses.

    """
    if isinstance(targets, str | ipaddress.IPv4Network):
        targets = [targets]
    excluded: set[ipaddress.IPv4Address] = set()
    for item in exclude:
        try:
            excluded.add(ipaddress.IPv4Address(str(item).strip()))
        except ValueError:
            continue  # e.g. a local IPv6 address
    seen: set[ipaddress.IPv4Address] = set()
    hosts: list[str] = []
    for target in targets:
        for ip in _host_addresses(validate_scan_target(str(target))):
            if ip in excluded or ip in seen:
                continue
            seen.add(ip)
            hosts.append(str(ip))
            if len(hosts) > MAX_TOTAL_HOSTS:
                raise DiscoveryError(
                    "scan_target_too_large",
                    f"The selected networks exceed {MAX_TOTAL_HOSTS} addresses.",
                    "Scan fewer networks, or enter the DGX Spark address directly.",
                )
    return hosts


def _worker_limit(requested: int) -> int:
    """Cap workers so concurrent sockets stay below the descriptor limit."""
    limit = max(1, min(requested, MAX_WORKERS))
    if _resource is None:
        return limit
    try:
        soft, _hard = _resource.getrlimit(_resource.RLIMIT_NOFILE)
    except (OSError, ValueError):
        return limit
    if soft == _resource.RLIM_INFINITY or soft <= 0:
        return limit
    return max(1, min(limit, (soft - _RESERVED_DESCRIPTORS) // 2))


def _safe_connect(connect: ConnectFn, address: str, port: int, timeout: float) -> bool:
    try:
        return bool(connect(address, port, timeout))
    except (OSError, ValueError):
        return False


def _safe_banner(read_banner: BannerFn, address: str) -> str | bytes | None:
    try:
        return read_banner(address, SSH_PORT, BANNER_TIMEOUT_SECONDS)
    except (OSError, ValueError):
        return None


def _safe_reverse_dns(reverse_dns: ReverseDnsFn, address: str) -> str | None:
    try:
        return reverse_dns(address)
    except (OSError, ValueError):
        return None


def _safe_models(http_get: HttpGetJsonFn, address: str, port: int) -> tuple[str, ...]:
    try:
        document = http_get(f"http://{address}:{port}/v1/models", HTTP_TIMEOUT_SECONDS)
    except (OSError, ValueError):
        return ()
    return model_ids(document)


def _safe_mdns(mdns_browse: MdnsBrowseFn) -> list[_mdns.MdnsService]:
    try:
        return list(mdns_browse())
    except (OSError, ValueError):
        return []


def _cancelled(cancel: threading.Event | None) -> bool:
    return cancel is not None and cancel.is_set()


@dataclass
class _Evidence:
    banner: str | bytes | None = None
    reverse_name: str | None = None
    model_ports: list[int] = field(default_factory=list)
    models: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _Probes:
    connect: ConnectFn
    read_banner: BannerFn
    reverse_dns: ReverseDnsFn
    http_get_json: HttpGetJsonFn
    timeout: float


def _probe_ssh(
    pool: Executor,
    hosts: Sequence[str],
    probes: _Probes,
    progress: Callable[[int, int], None] | None,
    cancel: threading.Event | None,
) -> list[str]:
    futures = {
        pool.submit(_safe_connect, probes.connect, host, SSH_PORT, probes.timeout): host
        for host in hosts
    }
    responders: list[str] = []
    completed = concurrent.futures.as_completed(futures)
    for done, future in enumerate(completed, start=1):
        if future.result():
            responders.append(futures[future])
        if progress is not None:
            progress(done, len(hosts))
        if _cancelled(cancel):
            break
    return sorted(responders, key=ipaddress.IPv4Address)


def _probe_details(
    pool: Executor,
    responders: Sequence[str],
    probes: _Probes,
    cancel: threading.Event | None,
) -> dict[str, _Evidence]:
    evidence = {host: _Evidence() for host in responders}
    rdns_pool = Executor(
        max_workers=min(REVERSE_DNS_WORKERS, len(responders)),
        thread_name_prefix="discovery-rdns",
    )
    try:
        rdns = {
            host: rdns_pool.submit(_safe_reverse_dns, probes.reverse_dns, host)
            for host in responders
        }
        banners = {
            host: pool.submit(_safe_banner, probes.read_banner, host)
            for host in responders
        }
        ports = {
            (host, port): pool.submit(
                _safe_connect, probes.connect, host, port, probes.timeout
            )
            for host in responders
            for port in MODEL_PORTS
        }
        for (host, port), port_open in ports.items():
            if port_open.result():
                evidence[host].model_ports.append(port)
        if not _cancelled(cancel):
            models = {
                (host, port): pool.submit(
                    _safe_models, probes.http_get_json, host, port
                )
                for host in responders
                for port in evidence[host].model_ports
            }
            for (host, _port), listed in models.items():
                found = evidence[host].models
                found.extend(m for m in listed.result() if m not in found)
        for host, banner in banners.items():
            evidence[host].banner = banner.result()
        deadline = time.monotonic() + REVERSE_DNS_TIMEOUT_SECONDS
        for host, name in rdns.items():
            remaining = max(0.0, deadline - time.monotonic())
            try:
                evidence[host].reverse_name = name.result(timeout=remaining)
            except (concurrent.futures.TimeoutError, concurrent.futures.CancelledError):
                evidence[host].reverse_name = None
    finally:
        # A stuck resolver call must never hold the scan open.
        rdns_pool.shutdown(wait=False, cancel_futures=True)
    return evidence


def _mdns_names(
    services: Iterable[_mdns.MdnsService], hosts: Collection[str]
) -> dict[str, list[str]]:
    names: dict[str, list[str]] = {}
    for service in services:
        host = service.host
        if not host or not host.strip(". "):
            continue
        for address in service.addresses:
            try:
                normalized = str(ipaddress.IPv4Address(address))
            except ValueError:
                continue
            bucket = names.setdefault(normalized, []) if normalized in hosts else None
            if bucket is not None and host not in bucket:
                bucket.append(host)
    return names


def _await_mdns(
    future: concurrent.futures.Future[list[_mdns.MdnsService]], started: float
) -> list[_mdns.MdnsService]:
    remaining = max(0.0, started + MDNS_WAIT_SECONDS - time.monotonic())
    try:
        return future.result(timeout=remaining)
    except (concurrent.futures.TimeoutError, concurrent.futures.CancelledError):
        return []


def scan(
    targets: Sequence[ipaddress.IPv4Network],
    *,
    exclude: Collection[str] = (),
    timeout: float = 0.4,
    workers: int = 128,
    connect: ConnectFn | None = None,
    read_banner: BannerFn | None = None,
    reverse_dns: ReverseDnsFn | None = None,
    http_get_json: HttpGetJsonFn | None = None,
    mdns_browse: MdnsBrowseFn | None = None,
    progress: Callable[[int, int], None] | None = None,
    cancel: threading.Event | None = None,
) -> list[Candidate]:
    """Probe private targets for SSH responders and return ranked candidates.

    Phase 0 browses mDNS concurrently. Phase 1 TCP-connects to port 22 on every
    host address and reports ``progress(done, total)`` from the calling thread.
    Phase 2, only for responders, reads the SSH banner, resolves reverse DNS in
    a separate pool with a bounded wait, probes ``MODEL_PORTS``, and fetches
    ``/v1/models`` from open ones. mDNS host names are merged by address;
    mDNS-only hosts are not returned. Per-host failures never raise.

    Args:
        targets: Private IPv4 networks; each is re-validated.
        exclude: Addresses never probed (e.g. this computer's own).
        timeout: TCP connect timeout in seconds, in ``(0, 5]``.
        workers: Maximum concurrent probes (also capped by the fd limit).
        connect: ``(address, port, timeout) -> bool``.
        read_banner: ``(address, port, timeout) -> str | bytes | None``.
        reverse_dns: ``(address) -> str | None``.
        http_get_json: ``(url, timeout) -> parsed JSON | None``.
        mdns_browse: ``() -> list[MdnsService]``.
        progress: Called with ``(done, total)`` after each port-22 probe.
        cancel: When set, probing stops promptly and the candidates learned so
            far (possibly none) are returned.

    Returns:
        Candidates sorted by ``(not likely_spark, address)``.

    Raises:
        DiscoveryError: For invalid targets or options, before any probe.

    """
    if type(timeout) not in (int, float) or not 0 < timeout <= 5:
        raise DiscoveryError("invalid_scan_options", "Timeout must be in (0, 5] s.")
    if type(workers) is not int or workers < 1:
        raise DiscoveryError("invalid_scan_options", "Workers must be positive.")
    hosts = expand_targets(targets, exclude)
    if progress is not None:
        progress(0, len(hosts))
    if not hosts or _cancelled(cancel):
        return []
    probes = _Probes(
        connect=tcp_connect if connect is None else connect,
        read_banner=read_ssh_banner if read_banner is None else read_banner,
        reverse_dns=reverse_dns_lookup if reverse_dns is None else reverse_dns,
        http_get_json=fetch_json if http_get_json is None else http_get_json,
        timeout=float(timeout),
    )
    started = time.monotonic()
    mdns_pool = Executor(max_workers=1, thread_name_prefix="discovery-mdns")
    mdns_future = mdns_pool.submit(
        _safe_mdns, _mdns.browse if mdns_browse is None else mdns_browse
    )
    mdns_pool.shutdown(wait=False)

    evidence: dict[str, _Evidence] = {}
    pool = Executor(_worker_limit(workers), thread_name_prefix="discovery-scan")
    try:
        responders = _probe_ssh(pool, hosts, probes, progress, cancel)
        evidence = {host: _Evidence() for host in responders}
        if responders and not _cancelled(cancel):
            evidence = _probe_details(pool, responders, probes, cancel)
    finally:
        pool.shutdown(wait=True, cancel_futures=True)

    services: list[_mdns.MdnsService] = []
    if evidence and not _cancelled(cancel):
        services = _await_mdns(mdns_future, started)
    else:
        mdns_future.cancel()
    names = _mdns_names(services, evidence.keys())
    candidates = [
        classify(
            host,
            hostname=found.reverse_name,
            ssh_banner=found.banner,
            open_ports=[SSH_PORT, *found.model_ports],
            served_models=found.models,
            mdns_names=names.get(host, ()),
            ssh_open=True,
        )
        for host, found in evidence.items()
    ]
    candidates.sort(
        key=lambda c: (not c.likely_spark, ipaddress.IPv4Address(c.address))
    )
    return candidates
