# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Minimal, best-effort multicast-DNS service browser (RFC 6762 / RFC 6763).

Safety boundary: ``browse`` sends one small PTR query (plus at most
``MAX_FOLLOWUP_QUERIES`` follow-ups for SRV/TXT/A records the first answers
omitted) to the link-local mDNS group 224.0.0.251:5353 from an ephemeral port.
Per RFC 6762 section 6.7 that is a "legacy unicast" query: responders answer by
unicast to this socket only and nothing is multicast back onto the LAN. The
browser never registers, announces, or probes names. Responses are untrusted:
the parser is bounds-checked, refuses compression loops, caps sizes, and
returns ``[]`` for a malformed packet instead of raising. Sandboxed or
multicast-less environments simply yield no services.
"""

from __future__ import annotations

import contextlib
import ipaddress
import secrets
import socket
import struct
import time
import types
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .network import DiscoveryError

MDNS_GROUP = "224.0.0.251"
MDNS_PORT = 5353

TYPE_A = 1
TYPE_PTR = 12
TYPE_TXT = 16
TYPE_AAAA = 28
TYPE_SRV = 33
CLASS_IN = 1
_UNICAST_RESPONSE_BIT = 0x8000
_CACHE_FLUSH_BIT = 0x8000
_FLAG_RESPONSE = 0x8000

DEFAULT_SERVICE_TYPES = (
    "_ssh._tcp.local.",
    "_workstation._tcp.local.",
    "_sftp-ssh._tcp.local.",
    "_device-info._tcp.local.",
)

MAX_PACKET_BYTES = 9000
MAX_RESPONSES = 256
MAX_TIMEOUT_SECONDS = 10.0
MAX_FOLLOWUP_QUERIES = 2
_MAX_QUESTIONS = 32
_MAX_NAME_BYTES = 255
_MAX_POINTER_JUMPS = 64
_MAX_TXT_ENTRIES = 64
_MAX_ADDRESSES_PER_HOST = 16

SocketFactory = Callable[[], Any]


class _Malformed(Exception):
    """A structurally invalid DNS message."""


@dataclass(frozen=True)
class DnsRecord:
    r"""One DNS resource record.

    Names use DNS presentation form with a trailing dot; a literal ``.`` or
    ``\`` inside a label is escaped as ``\.`` or ``\\``.

    Attributes:
        name: Owner name, e.g. ``_ssh._tcp.local.``.
        rtype: Record type number.
        rclass: Record class with the mDNS cache-flush bit removed.
        ttl: Time to live in seconds.
        data: Decoded payload: PTR ``str``; SRV ``(priority, weight, port,
            target)``; TXT ``dict[str, str]``; A/AAAA ``str``; other ``bytes``.
        cache_flush: Whether the mDNS cache-flush bit was set.

    """

    name: str
    rtype: int
    rclass: int
    ttl: int
    data: Any = field(hash=False)
    cache_flush: bool = False


@dataclass(frozen=True)
class MdnsService:
    """A service instance joined from PTR, SRV, TXT, and A/AAAA records.

    Attributes:
        instance: Raw instance name, e.g.
            ``spark-1a2b [aa:bb:cc:dd:ee:ff]._workstation._tcp.local.``.
        service: Service type, e.g. ``_workstation._tcp.local.``.
        host: SRV target such as ``spark-1a2b.local.``, if known.
        port: SRV port, if known.
        addresses: A/AAAA addresses of ``host`` from the same responses.
        txt: Read-only TXT key/value pairs (keys lowercased).

    """

    instance: str
    service: str
    host: str | None
    port: int | None
    addresses: tuple[str, ...]
    txt: Mapping[str, str] = field(hash=False)

    def __post_init__(self) -> None:
        """Freeze ``txt`` as a read-only copy."""
        object.__setattr__(self, "txt", types.MappingProxyType(dict(self.txt)))

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-ready mapping."""
        return {
            "instance": self.instance,
            "service": self.service,
            "host": self.host,
            "port": self.port,
            "addresses": list(self.addresses),
            "txt": dict(self.txt),
        }


def _split_name(name: str) -> list[bytes]:
    r"""Split a presentation-form name into wire labels, honoring ``\.``."""
    labels: list[bytes] = []
    current: list[str] = []
    index = 0
    while index < len(name):
        char = name[index]
        if char == "\\" and index + 1 < len(name):
            current.append(name[index + 1])
            index += 2
            continue
        if char == ".":
            labels.append("".join(current).encode("utf-8"))
            current = []
        else:
            current.append(char)
        index += 1
    if current:
        labels.append("".join(current).encode("utf-8"))
    return labels


def _encode_name(name: str) -> bytes:
    if not isinstance(name, str) or name in ("", "."):
        raise DiscoveryError("invalid_mdns_name", "mDNS names must not be empty.")
    encoded = bytearray()
    for label in _split_name(name):
        if not 1 <= len(label) <= 63:
            raise DiscoveryError("invalid_mdns_name", f"Invalid DNS label in {name!r}.")
        encoded.append(len(label))
        encoded += label
    encoded.append(0)
    if len(encoded) > _MAX_NAME_BYTES:
        raise DiscoveryError("invalid_mdns_name", "mDNS name exceeds 255 bytes.")
    return bytes(encoded)


def _encode_questions(
    questions: Sequence[tuple[str, int]], *, query_id: int, unicast_response: bool
) -> bytes:
    if not 0 <= query_id <= 0xFFFF:
        raise DiscoveryError("invalid_mdns_query", "DNS query ids are 16 bits.")
    if not 1 <= len(questions) <= _MAX_QUESTIONS:
        raise DiscoveryError(
            "invalid_mdns_query", f"A query needs 1-{_MAX_QUESTIONS} questions."
        )
    qclass = CLASS_IN | (_UNICAST_RESPONSE_BIT if unicast_response else 0)
    message = bytearray(struct.pack("!6H", query_id, 0, len(questions), 0, 0, 0))
    for name, qtype in questions:
        message += _encode_name(name)
        message += struct.pack("!HH", qtype, qclass)
    return bytes(message)


def encode_query(
    names: Sequence[str], *, query_id: int = 0, unicast_response: bool = True
) -> bytes:
    """Encode a DNS query message with one PTR question per name.

    Args:
        names: Names to query, e.g. ``["_ssh._tcp.local."]``.
        query_id: 16-bit DNS message id.
        unicast_response: Set the RFC 6762 "QU" bit on every question.

    Returns:
        The wire-format query.

    Raises:
        DiscoveryError: ``invalid_mdns_name`` or ``invalid_mdns_query``.

    """
    if isinstance(names, str):
        raise DiscoveryError("invalid_mdns_query", "Pass a sequence of names.")
    return _encode_questions(
        [(name, TYPE_PTR) for name in names],
        query_id=query_id,
        unicast_response=unicast_response,
    )


def _label_text(label: bytes) -> str:
    text = label.decode("utf-8", errors="replace")
    return text.replace("\\", "\\\\").replace(".", "\\.")


def _read_name(data: bytes, offset: int) -> tuple[str, int]:
    """Read a possibly compressed name.

    Every compression pointer must target an offset strictly lower than the
    lowest offset visited so far for this name, so decoding always terminates.

    Returns:
        The presentation-form name and the offset just past it.

    """
    labels: list[str] = []
    position = lowest = offset
    after: int | None = None
    total = 1
    jumps = 0
    while True:
        if position >= len(data):
            raise _Malformed("name runs past end of message")
        length = data[position]
        if length & 0xC0 == 0xC0:
            if position + 1 >= len(data):
                raise _Malformed("truncated compression pointer")
            pointer = ((length & 0x3F) << 8) | data[position + 1]
            if after is None:
                after = position + 2
            jumps += 1
            if pointer >= lowest or jumps > _MAX_POINTER_JUMPS:
                raise _Malformed("compression pointer loop")
            position = lowest = pointer
            continue
        if length & 0xC0:
            raise _Malformed("unsupported label type")
        position += 1
        if length == 0:
            break
        end = position + length
        total += length + 1
        if end > len(data) or total > _MAX_NAME_BYTES:
            raise _Malformed("label runs past end of message or name too long")
        labels.append(_label_text(data[position:end]))
        position = end
    name = ".".join(labels) + "." if labels else "."
    return name, position if after is None else after


def _parse_txt(rdata: bytes) -> dict[str, str]:
    result: dict[str, str] = {}
    position = 0
    entries = 0
    while position < len(rdata):
        end = position + 1 + rdata[position]
        if end > len(rdata):
            raise _Malformed("TXT string runs past rdata")
        entry = rdata[position + 1 : end]
        position = end
        entries += 1
        key, separator, value = entry.partition(b"=")
        if entries > _MAX_TXT_ENTRIES or not key:
            continue
        key_text = key.decode("utf-8", errors="replace").lower()
        # RFC 6763 section 6.4: only the first occurrence of a key counts.
        if key_text not in result:
            text = value.decode("utf-8", errors="replace") if separator else ""
            result[key_text] = text
    return result


def _parse_rdata(data: bytes, rtype: int, start: int, end: int) -> Any:
    rdata = data[start:end]
    if rtype == TYPE_A:
        if len(rdata) != 4:
            raise _Malformed("A record must be 4 bytes")
        return str(ipaddress.IPv4Address(rdata))
    if rtype == TYPE_AAAA:
        if len(rdata) != 16:
            raise _Malformed("AAAA record must be 16 bytes")
        return str(ipaddress.IPv6Address(rdata))
    if rtype == TYPE_PTR:
        name, after = _read_name(data, start)
        if after > end:
            raise _Malformed("PTR name exceeds rdata")
        return name
    if rtype == TYPE_SRV:
        if len(rdata) < 7:
            raise _Malformed("SRV record too short")
        priority, weight, port = struct.unpack_from("!3H", data, start)
        target, after = _read_name(data, start + 6)
        if after > end:
            raise _Malformed("SRV target exceeds rdata")
        return (priority, weight, port, target)
    if rtype == TYPE_TXT:
        return _parse_txt(rdata)
    return bytes(rdata)


def _parse_packet(data: bytes) -> tuple[int, int, list[DnsRecord]] | None:
    """Return ``(id, flags, records)``, or ``None`` for a malformed message."""
    try:
        message = bytes(data)
        if len(message) < 12:
            return None
        query_id, flags, qdcount, ancount, nscount, arcount = struct.unpack_from(
            "!6H", message, 0
        )
        position = 12
        for _ in range(qdcount):
            _, position = _read_name(message, position)
            position += 4
            if position > len(message):
                return None
        records: list[DnsRecord] = []
        for _ in range(ancount + nscount + arcount):
            name, position = _read_name(message, position)
            if position + 10 > len(message):
                return None
            rtype, rclass, ttl, rdlength = struct.unpack_from(
                "!HHIH", message, position
            )
            position += 10
            end = position + rdlength
            if end > len(message):
                return None
            records.append(
                DnsRecord(
                    name=name,
                    rtype=rtype,
                    rclass=rclass & ~_CACHE_FLUSH_BIT,
                    ttl=ttl,
                    data=_parse_rdata(message, rtype, position, end),
                    cache_flush=bool(rclass & _CACHE_FLUSH_BIT),
                )
            )
            position = end
        return query_id, flags, records
    except (_Malformed, ValueError, IndexError, struct.error):
        return None


def parse_message(data: bytes) -> list[DnsRecord]:
    """Decode the answer, authority, and additional records of a DNS message.

    Args:
        data: Untrusted wire-format message.

    Returns:
        All records in message order, or ``[]`` if the message is malformed
        (truncation, bad lengths, compression loops, oversized names). Never
        raises for bad input.

    """
    parsed = _parse_packet(data)
    return [] if parsed is None else parsed[2]


def _normalize_service_type(value: str) -> str:
    if not isinstance(value, str):
        raise DiscoveryError("invalid_mdns_service", "Service types must be text.")
    lowered = (value if value.endswith(".") else value + ".").lower()
    labels = lowered.split(".")
    if (
        len(labels) != 4
        or not labels[0].startswith("_")
        or labels[1] not in ("_tcp", "_udp")
        or labels[2] != "local"
    ):
        raise DiscoveryError(
            "invalid_mdns_service", f"Unsupported mDNS service type {value!r}."
        )
    _encode_name(lowered)
    return lowered


class _Collector:
    """Accumulate records from many responses and join them into services."""

    def __init__(self, service_types: Sequence[str]) -> None:
        self._types = tuple(service_types)
        self._instances: dict[str, dict[str, str]] = {t: {} for t in self._types}
        self._srv: dict[str, tuple[int, int, int, str]] = {}
        self._txt: dict[str, dict[str, str]] = {}
        self._addresses: dict[str, list[str]] = {}
        self._asked: set[tuple[str, int]] = set()

    def add(self, records: Iterable[DnsRecord]) -> None:
        for record in records:
            if record.rclass != CLASS_IN or record.ttl == 0:
                continue  # wrong class, or an RFC 6762 "goodbye" record
            owner = record.name.lower()
            if record.rtype == TYPE_PTR and owner in self._instances:
                self._instances[owner].setdefault(record.data.lower(), record.data)
            elif record.rtype == TYPE_SRV:
                self._srv.setdefault(owner, record.data)
            elif record.rtype == TYPE_TXT:
                self._txt.setdefault(owner, record.data)
            elif record.rtype in (TYPE_A, TYPE_AAAA):
                known = self._addresses.setdefault(owner, [])
                if record.data not in known and len(known) < _MAX_ADDRESSES_PER_HOST:
                    known.append(record.data)

    def pending_questions(self, limit: int = 16) -> list[tuple[str, int]]:
        """Return questions for SRV/TXT/A data the responders did not volunteer."""
        wanted: list[tuple[str, int]] = []
        for instances in self._instances.values():
            for key, instance in instances.items():
                if key not in self._srv:
                    wanted.append((instance, TYPE_SRV))
                if key not in self._txt:
                    wanted.append((instance, TYPE_TXT))
        for _priority, _weight, _port, target in self._srv.values():
            if target != "." and target.lower() not in self._addresses:
                wanted.append((target, TYPE_A))
        questions: list[tuple[str, int]] = []
        for name, qtype in wanted:
            if (name.lower(), qtype) in self._asked or len(questions) >= limit:
                continue
            try:
                _encode_name(name)
            except DiscoveryError:
                continue
            self._asked.add((name.lower(), qtype))
            questions.append((name, qtype))
        return questions

    def services(self) -> list[MdnsService]:
        result: list[MdnsService] = []
        for service in self._types:
            for key, instance in self._instances[service].items():
                srv = self._srv.get(key)
                host = srv[3] if srv is not None and srv[3] != "." else None
                addresses = self._addresses.get(host.lower(), []) if host else []
                result.append(
                    MdnsService(
                        instance=instance,
                        service=service,
                        host=host,
                        port=srv[2] if srv is not None else None,
                        addresses=tuple(addresses),
                        txt=self._txt.get(key, {}),
                    )
                )
        result.sort(key=lambda item: (item.service, item.instance.lower()))
        return result


def _default_socket() -> socket.socket:
    return socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)


def _collect(sock: Any, collector: _Collector, query_id: int, timeout: float) -> None:
    """Receive responses until the deadline; stop quietly on any socket error."""
    destination = (MDNS_GROUP, MDNS_PORT)
    deadline = time.monotonic() + timeout
    followups = 0
    for _ in range(MAX_RESPONSES):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        try:
            sock.settimeout(remaining)
            data, _source = sock.recvfrom(MAX_PACKET_BYTES)
        except OSError:  # includes socket.timeout
            return
        parsed = _parse_packet(data)
        if parsed is None:
            continue
        response_id, flags, records = parsed
        if not flags & _FLAG_RESPONSE or response_id != query_id:
            continue
        collector.add(records)
        questions = (
            collector.pending_questions() if followups < MAX_FOLLOWUP_QUERIES else []
        )
        if questions:
            followups += 1
            packet = _encode_questions(
                questions, query_id=query_id, unicast_response=True
            )
            try:
                sock.sendto(packet, destination)
            except OSError:
                followups = MAX_FOLLOWUP_QUERIES


def browse(
    service_types: Sequence[str] = DEFAULT_SERVICE_TYPES,
    *,
    timeout: float = 2.0,
    socket_factory: SocketFactory | None = None,
) -> list[MdnsService]:
    """Browse mDNS service types for ``timeout`` seconds (best effort).

    One legacy-unicast PTR query is sent from an ephemeral port; responders
    reply by unicast with the same query id, which filters unrelated traffic.
    When answers omit the SRV/TXT/A records needed to join an instance to an
    address, up to ``MAX_FOLLOWUP_QUERIES`` follow-ups ask for exactly those.

    Args:
        service_types: Service types such as ``_ssh._tcp.local.``.
        timeout: Collection time in seconds, in ``(0, MAX_TIMEOUT_SECONDS]``.
        socket_factory: Returns a UDP socket-like object; injectable for tests.

    Returns:
        Joined services sorted by service type and instance. ``[]`` if the
        socket cannot be created or the query cannot be sent (for example in a
        sandbox); a receive error ends collection early.

    Raises:
        DiscoveryError: For invalid service types or timeout (programming
            errors); never for network failures.

    """
    if isinstance(service_types, str):
        raise DiscoveryError("invalid_mdns_service", "Pass a sequence of types.")
    requested = list(dict.fromkeys(_normalize_service_type(t) for t in service_types))
    if not requested:
        return []
    if type(timeout) not in (int, float) or not 0 < timeout <= MAX_TIMEOUT_SECONDS:
        raise DiscoveryError(
            "invalid_mdns_timeout",
            f"mDNS timeout must be in (0, {MAX_TIMEOUT_SECONDS:g}] seconds.",
        )
    query_id = 1 + secrets.randbelow(0xFFFF)
    collector = _Collector(requested)
    factory = _default_socket if socket_factory is None else socket_factory
    try:
        sock = factory()
    except OSError:
        return []
    try:
        # Responders expect on-link queries to carry IP TTL 255.
        with contextlib.suppress(OSError):
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, bytes([255]))
        try:
            sock.sendto(
                encode_query(requested, query_id=query_id), (MDNS_GROUP, MDNS_PORT)
            )
        except OSError:
            return []
        _collect(sock, collector, query_id, float(timeout))
    finally:
        with contextlib.suppress(OSError):
            sock.close()
    return collector.services()
