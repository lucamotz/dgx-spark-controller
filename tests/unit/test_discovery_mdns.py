# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Tests for the minimal multicast-DNS encoder, parser, and browser."""

import socket
import struct
import unittest
from collections.abc import Callable

from controller.discovery import mdns
from controller.discovery.mdns import (
    DEFAULT_SERVICE_TYPES,
    TYPE_A,
    TYPE_AAAA,
    TYPE_PTR,
    TYPE_SRV,
    DnsRecord,
    MdnsService,
    browse,
    encode_query,
    parse_packet,
)
from controller.discovery.network import DiscoveryError

WORKSTATION = "_workstation._tcp.local."
SSH = "_ssh._tcp.local."
INSTANCE = "spark-1a2b [aa:bb:cc:dd:ee:ff]._workstation._tcp.local."


def wire_name(name: str) -> bytes:
    """Encode a dotted name without compression (labels must not contain dots)."""
    encoded = b""
    for label in name.rstrip(".").split("."):
        raw = label.encode("utf-8")
        encoded += bytes([len(raw)]) + raw
    return encoded + b"\x00"


def labels_name(*labels: bytes) -> bytes:
    """Encode raw labels, allowing dots inside a label."""
    return b"".join(bytes([len(label)]) + label for label in labels) + b"\x00"


def record(
    owner: bytes, rtype: int, rdata: bytes, *, ttl: int = 120, rclass: int = 1
) -> bytes:
    """Encode one resource record with an already-encoded owner name."""
    return owner + struct.pack("!HHIH", rtype, rclass, ttl, len(rdata)) + rdata


def srv(port: int, target: str) -> bytes:
    """Encode SRV rdata."""
    return struct.pack("!3H", 0, 0, port) + wire_name(target)


def message(
    query_id: int,
    answers: tuple[bytes, ...] = (),
    additionals: tuple[bytes, ...] = (),
    *,
    flags: int = 0x8400,
) -> bytes:
    """Build a DNS response without a question section."""
    header = struct.pack("!6H", query_id, flags, 0, len(answers), 0, len(additionals))
    return header + b"".join(answers) + b"".join(additionals)


def full_response(query_id: int) -> bytes:
    """A complete legacy-unicast answer for the workstation service."""
    return message(
        query_id,
        answers=(record(wire_name(WORKSTATION), TYPE_PTR, wire_name(INSTANCE)),),
        additionals=(
            record(
                wire_name(INSTANCE),
                TYPE_SRV,
                srv(9, "spark-1a2b.local."),
                rclass=0x8001,
            ),
            record(wire_name("spark-1a2b.local."), TYPE_A, bytes([192, 168, 1, 20])),
            record(
                wire_name("spark-1a2b.local."),
                TYPE_AAAA,
                bytes.fromhex("fe800000000000000000000000000001"),
            ),
        ),
    )


Responder = Callable[[int], bytes | BaseException]


def answer_with(*answers: bytes) -> Responder:
    """Return a responder that answers with the given records."""
    return lambda query_id: message(query_id, answers)


class FakeSocket:
    """A scripted UDP socket; each ``recvfrom`` pops the next responder."""

    def __init__(self, responders: list[Responder], *, fail_send: bool = False) -> None:
        """Script responses; ``fail_send`` makes ``sendto`` raise."""
        self.responders = list(responders)
        self.fail_send = fail_send
        self.sent: list[tuple[bytes, tuple[str, int]]] = []
        self.options: list[tuple[object, ...]] = []
        self.timeouts: list[float] = []
        self.closed = False

    def setsockopt(self, *args: object) -> None:
        """Record socket options."""
        self.options.append(args)

    def settimeout(self, value: float) -> None:
        """Record receive timeouts."""
        self.timeouts.append(value)

    def sendto(self, data: bytes, address: tuple[str, int]) -> int:
        """Record a sent datagram."""
        if self.fail_send:
            raise OSError("multicast blocked")
        self.sent.append((data, address))
        return len(data)

    def recvfrom(self, size: int) -> tuple[bytes, tuple[str, int]]:
        """Return the next scripted packet or time out."""
        if not self.responders:
            raise TimeoutError("timed out")
        query_id = struct.unpack("!H", self.sent[0][0][:2])[0]
        result = self.responders.pop(0)(query_id)
        if isinstance(result, BaseException):
            raise result
        return result[:size], ("192.168.1.20", 5353)

    def close(self) -> None:
        """Mark the socket closed."""
        self.closed = True


def questions_of(packet: bytes) -> list[tuple[str, int, int]]:
    """Decode the question section of a query we sent."""
    count = struct.unpack("!H", packet[4:6])[0]
    position = 12
    result = []
    for _ in range(count):
        labels = []
        while packet[position]:
            length = packet[position]
            labels.append(packet[position + 1 : position + 1 + length].decode())
            position += 1 + length
        position += 1
        qtype, qclass = struct.unpack("!HH", packet[position : position + 4])
        position += 4
        result.append((".".join(labels) + ".", qtype, qclass))
    return result


class EncodeQueryTests(unittest.TestCase):
    """Query encoding."""

    def test_header_and_questions(self) -> None:
        """One PTR question per name with the QU bit and the given id."""
        packet = encode_query([SSH, "_sftp-ssh._tcp.local"], query_id=0x1234)
        self.assertEqual(struct.unpack("!6H", packet[:12]), (0x1234, 0, 2, 0, 0, 0))
        self.assertEqual(
            questions_of(packet),
            [(SSH, TYPE_PTR, 0x8001), ("_sftp-ssh._tcp.local.", TYPE_PTR, 0x8001)],
        )
        self.assertEqual(packet[12:17], b"\x04_ssh")

    def test_invalid_inputs(self) -> None:
        """Bad names, ids, and counts are rejected."""
        bad_calls = (
            lambda: encode_query([]),
            lambda: encode_query([""]),
            lambda: encode_query(["a..local."]),
            lambda: encode_query(["x" * 64 + ".local."]),
            lambda: encode_query([".".join(["abcdefgh"] * 40)]),
            lambda: encode_query([SSH], query_id=0x10000),
            lambda: encode_query([SSH], query_id=-1),
            lambda: encode_query([SSH] * 33),
        )
        for index, call in enumerate(bad_calls):
            with self.subTest(index=index), self.assertRaises(DiscoveryError):
                call()


class ParseMessageTests(unittest.TestCase):
    """Response parsing, including hostile input."""

    def test_decodes_all_record_types(self) -> None:
        """PTR, SRV, A, AAAA, and unknown types decode; cache-flush is stripped."""
        query_id, flags, records = parse_packet(full_response(7))
        self.assertEqual((7, 0x8400), (query_id, flags))
        self.assertEqual(
            [(r.name, r.rtype) for r in records],
            [
                (WORKSTATION, TYPE_PTR),
                (INSTANCE, TYPE_SRV),
                ("spark-1a2b.local.", TYPE_A),
                ("spark-1a2b.local.", TYPE_AAAA),
            ],
        )
        self.assertEqual(records[0].data, INSTANCE)
        self.assertEqual(records[1].data, (0, 0, 9, "spark-1a2b.local."))
        self.assertEqual(records[1].rclass, 1)
        self.assertEqual(records[2].data, "192.168.1.20")
        self.assertEqual(records[3].data, "fe80::1")
        self.assertEqual(records[0].ttl, 120)
        unknown = parse_packet(
            message(1, (record(wire_name("x.local."), 99, b"\x01\x02"),))
        )
        self.assertEqual(
            unknown, (1, 0x8400, [DnsRecord("x.local.", 99, 1, 120, b"\x01\x02")])
        )

    def test_skips_question_section_and_follows_compression(self) -> None:
        """Questions are skipped and backward pointers resolve."""
        header = struct.pack("!6H", 3, 0x8400, 1, 1, 0, 0)
        question = wire_name(SSH) + struct.pack("!HH", TYPE_PTR, 1)
        rdata = b"\x05spark\xc0\x0c"  # "spark" + pointer to the question name
        answer = b"\xc0\x0c" + struct.pack("!HHIH", TYPE_PTR, 1, 60, len(rdata)) + rdata
        self.assertEqual(
            parse_packet(header + question + answer),
            (3, 0x8400, [DnsRecord(SSH, TYPE_PTR, 1, 60, "spark." + SSH)]),
        )

    def test_labels_containing_dots_are_escaped(self) -> None:
        """A literal dot or backslash in a label is escaped in presentation form."""
        owner = labels_name(b"My.Spark\\1", b"_ssh", b"_tcp", b"local")
        _, _, records = parse_packet(message(1, (record(owner, 99, b""),)))
        self.assertEqual(records[0].name, "My\\.Spark\\\\1._ssh._tcp.local.")

    def test_malformed_messages_return_empty(self) -> None:
        """Truncation, bad lengths, loops, and oversize names never raise."""
        good = full_response(9)
        header = struct.pack("!6H", 1, 0x8400, 0, 1, 0, 0)
        loop = header + b"\xc0\x0c" + struct.pack("!HHIH", 1, 1, 1, 4) + b"\x00" * 4
        forward = header + b"\xc0\x20" + b"\x00" * 40
        long_name = b"".join(b"\x3f" + b"a" * 63 for _ in range(5)) + b"\x00"
        too_long = header + long_name + struct.pack("!HHIH", 1, 1, 1, 4) + b"\x00" * 4
        bad_a = message(1, (record(wire_name("x.local."), TYPE_A, b"\x01\x02\x03"),))
        bad_aaaa = message(1, (record(wire_name("x.local."), TYPE_AAAA, b"\x01" * 4),))
        short_srv = message(1, (record(wire_name("x.local."), TYPE_SRV, b"\x00" * 6),))
        overrun = good[:-1]
        ptr_overrun = (
            header
            + wire_name("x.local.")
            + struct.pack("!HHIH", TYPE_PTR, 1, 1, 3)
            + b"\x05spark\x00"
        )
        label_type = header + b"\x40abc\x00" + struct.pack("!HHIH", 1, 1, 1, 0)
        cases = [
            b"",
            b"\x00" * 11,
            loop,
            forward,
            too_long,
            bad_a,
            bad_aaaa,
            short_srv,
            overrun,
            ptr_overrun,
            label_type,
        ]
        for index, data in enumerate(cases):
            with self.subTest(index=index):
                self.assertIsNone(parse_packet(data))

    def test_every_truncation_is_safe(self) -> None:
        """Parsing any prefix of a valid message never raises."""
        data = full_response(5)
        for size in range(len(data)):
            parse_packet(data[:size])
        self.assertEqual(len(parse_packet(data)[2]), 4)


class BrowseTests(unittest.TestCase):
    """The best-effort browser with an injected socket."""

    def run_browse(self, sock: FakeSocket, **kwargs: object) -> list[MdnsService]:
        """Browse with the fake socket."""
        return browse(socket_factory=lambda: sock, **kwargs)  # type: ignore[arg-type]

    def test_joins_ptr_srv_and_addresses(self) -> None:
        """A complete response yields one joined service; no follow-up is sent."""
        sock = FakeSocket([full_response])
        services = self.run_browse(sock)
        self.assertEqual(
            services,
            [
                MdnsService(
                    instance=INSTANCE,
                    service=WORKSTATION,
                    host="spark-1a2b.local.",
                    port=9,
                    addresses=("192.168.1.20", "fe80::1"),
                )
            ],
        )
        self.assertEqual(len(sock.sent), 1)
        packet, destination = sock.sent[0]
        self.assertEqual(destination, ("224.0.0.251", 5353))
        self.assertEqual(
            [name for name, _qtype, _qclass in questions_of(packet)],
            list(DEFAULT_SERVICE_TYPES),
        )
        self.assertIn(
            (socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, bytes([255])), sock.options
        )
        self.assertTrue(sock.closed)

    def test_ignores_foreign_queries_malformed_and_goodbye_packets(self) -> None:
        """Wrong ids, queries, garbage, wrong class, and TTL 0 are discarded."""
        ptr = record(wire_name(SSH), TYPE_PTR, wire_name("other." + SSH))
        sock = FakeSocket(
            [
                lambda qid: message(qid ^ 0xFFFF, (ptr,)),
                lambda qid: message(qid, (ptr,), flags=0),
                lambda qid: b"\xff" * 7,
                lambda qid: message(
                    qid,
                    (record(wire_name(SSH), TYPE_PTR, wire_name("bye." + SSH), ttl=0),),
                ),
                lambda qid: message(
                    qid,
                    (
                        record(
                            wire_name(SSH), TYPE_PTR, wire_name("ch." + SSH), rclass=3
                        ),
                    ),
                ),
            ]
        )
        self.assertEqual(self.run_browse(sock, service_types=[SSH]), [])
        self.assertEqual(len(sock.sent), 1)

    def test_follow_up_queries_fill_missing_records(self) -> None:
        """Missing SRV and then A records trigger bounded follow-ups."""
        ptr = record(wire_name(WORKSTATION), TYPE_PTR, wire_name(INSTANCE))
        srv_record = record(wire_name(INSTANCE), TYPE_SRV, srv(22, "spark-1a2b.local."))
        a_record = record(wire_name("spark-1a2b.local."), TYPE_A, bytes([10, 0, 0, 5]))
        sock = FakeSocket(
            [
                lambda qid: message(qid, (ptr,)),
                lambda qid: message(qid, (srv_record,)),
                lambda qid: message(qid, (a_record,)),
            ]
        )
        services = self.run_browse(sock, service_types=[WORKSTATION])
        self.assertEqual(services[0].addresses, ("10.0.0.5",))
        self.assertEqual(services[0].port, 22)
        self.assertEqual(len(sock.sent), 3)
        self.assertEqual(
            [(name, qtype) for name, qtype, _ in questions_of(sock.sent[1][0])],
            [(INSTANCE, TYPE_SRV)],
        )
        self.assertEqual(
            [(name, qtype) for name, qtype, _ in questions_of(sock.sent[2][0])],
            [("spark-1a2b.local.", TYPE_A)],
        )

    def test_follow_ups_are_bounded(self) -> None:
        """Responders that never complete cannot trigger unbounded queries."""
        responders: list[Responder] = []
        for index in range(10):
            instance = wire_name(f"n{index}." + SSH)
            ptr = record(wire_name(SSH), TYPE_PTR, instance)
            responders.append(answer_with(ptr))
        sock = FakeSocket(responders)
        services = self.run_browse(sock, service_types=[SSH])
        self.assertEqual(len(services), 10)
        self.assertTrue(all(s.host is None and s.port is None for s in services))
        self.assertEqual(len(sock.sent), 1 + mdns.MAX_FOLLOWUP_QUERIES)

    def test_socket_failures_return_empty_or_partial(self) -> None:
        """Creation/send errors give ``[]``; a receive error keeps prior results."""

        def broken_factory() -> FakeSocket:
            raise OSError("sandboxed")

        self.assertEqual(browse(socket_factory=broken_factory), [])
        blocked = FakeSocket([full_response], fail_send=True)
        self.assertEqual(self.run_browse(blocked), [])
        self.assertTrue(blocked.closed)
        partial = FakeSocket([full_response, lambda qid: OSError("network down")])
        self.assertEqual(len(self.run_browse(partial)), 1)

    def test_setsockopt_failure_is_tolerated(self) -> None:
        """A platform rejecting the TTL option still browses."""
        sock = FakeSocket([full_response])

        def refuse(*args: object) -> None:
            raise OSError("not supported")

        sock.setsockopt = refuse  # type: ignore[method-assign]
        self.assertEqual(len(self.run_browse(sock)), 1)

    def test_invalid_arguments_raise(self) -> None:
        """Bad service types and timeouts are programming errors."""
        sock = FakeSocket([])
        for kwargs in (
            {"service_types": ["ssh.local."]},
            {"service_types": ["_ssh._xyz.local."]},
            {"service_types": ["_ssh._tcp.example."]},
            {"service_types": [42]},
            {"timeout": 0},
            {"timeout": -1.0},
            {"timeout": 11},
            {"timeout": True},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(DiscoveryError):
                self.run_browse(sock, **kwargs)
        self.assertEqual(sock.sent, [])
        self.assertEqual(self.run_browse(sock, service_types=[]), [])

    def test_service_type_normalization(self) -> None:
        """Types without the trailing dot or in upper case are accepted."""
        sock = FakeSocket([])
        self.run_browse(sock, service_types=["_SSH._tcp.local", SSH])
        self.assertEqual([q[0] for q in questions_of(sock.sent[0][0])], [SSH])


if __name__ == "__main__":
    unittest.main()
