# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Onboarding wizard backend: scan, trust, identify, and save Spark nodes.

The wizard only ever reads from the network and the nodes: it scans private
subnets, fetches SSH host keys for the operator to confirm, and runs a fixed
read-only probe with a key the operator selected. The single write is the local
``nodes.json``, built exclusively from nodes identified in this session.
"""

from __future__ import annotations

import getpass
import ipaddress
import json
import os
import re
import secrets
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from controller.config import parse_nodes_document
from controller.discovery import hostkeys, network, scanner, sshkeys
from controller.discovery.identify import (
    NetworkInterface,
    NodeIdentity,
    SSHTarget,
    check_cluster_links,
    probe_identity,
)
from controller.discovery.network import DiscoveryError
from controller.domain import ID_RE as NODE_ID_RE
from controller.paths import AppPaths, write_private_file

MODEL_ROOT_RE = re.compile(r"/[A-Za-z0-9._/-]{1,200}\Z")
IDENTITY_TTL_SECONDS = 30 * 60
SAVE_NODE_KEYS = frozenset(
    {
        "host",
        "user",
        "key_id",
        "port",
        "node_id",
        "display_name",
        "model_root",
        "cluster_addresses",
    }
)


class DiscoveryBackend(Protocol):
    """Network-facing operations, swappable for the deterministic fake mode."""

    def local_networks(self) -> list[network.LocalNetwork]: ...

    def scan(
        self,
        targets: Sequence[ipaddress.IPv4Network],
        *,
        exclude: Sequence[str],
        progress: Callable[[int, int], None],
        cancel: threading.Event,
    ) -> list[scanner.Candidate]: ...

    def host_keys(self, host: str, port: int) -> list[hostkeys.HostKey]: ...

    def user_known_hosts(
        self, host: str, port: int
    ) -> list[hostkeys.KnownHostEntry] | None: ...

    def identify(
        self, target: SSHTarget, known_hosts_options: Sequence[str]
    ) -> NodeIdentity: ...

    def cluster_links(
        self,
        target: SSHTarget,
        known_hosts_options: Sequence[str],
        peers: Sequence[str],
    ) -> dict[str, bool]: ...


class RealDiscoveryBackend:
    """Discovery against the real local network and nodes."""

    def __init__(self, *, user_known_hosts_path: Path | None = None) -> None:
        self._user_known_hosts_path = user_known_hosts_path

    def local_networks(self) -> list[network.LocalNetwork]:
        return network.local_ipv4_networks()

    def scan(
        self,
        targets: Sequence[ipaddress.IPv4Network],
        *,
        exclude: Sequence[str],
        progress: Callable[[int, int], None],
        cancel: threading.Event,
    ) -> list[scanner.Candidate]:
        return scanner.scan(targets, exclude=exclude, progress=progress, cancel=cancel)

    def host_keys(self, host: str, port: int) -> list[hostkeys.HostKey]:
        return hostkeys.scan_host_keys(host, port=port)

    def user_known_hosts(
        self, host: str, port: int
    ) -> list[hostkeys.KnownHostEntry] | None:
        # Informational only: an unusual home directory never blocks the wizard.
        try:
            path = self._user_known_hosts_path or Path.home() / ".ssh" / "known_hosts"
            return hostkeys.user_known_host_keys(host, port, known_hosts_path=path)
        except (DiscoveryError, RuntimeError):
            return None

    def identify(
        self, target: SSHTarget, known_hosts_options: Sequence[str]
    ) -> NodeIdentity:
        return probe_identity(target, known_hosts_options=known_hosts_options)

    def cluster_links(
        self,
        target: SSHTarget,
        known_hosts_options: Sequence[str],
        peers: Sequence[str],
    ) -> dict[str, bool]:
        return check_cluster_links(
            target, peers, known_hosts_options=known_hosts_options
        )


@dataclass
class ScanJob:
    job_id: str
    targets: tuple[str, ...]
    state: str = "running"
    done: int = 0
    total: int = 0
    candidates: list[dict[str, Any]] = field(default_factory=list)
    error: dict[str, str] | None = None
    started_at: float = field(default_factory=time.time)
    cancel: threading.Event = field(default_factory=threading.Event)

    def resource(self) -> dict[str, Any]:
        return {
            "id": self.job_id,
            "state": self.state,
            "targets": list(self.targets),
            "progress": {"done": self.done, "total": self.total},
            "candidates": list(self.candidates),
            "error": self.error,
        }


@dataclass(frozen=True)
class _IdentifiedNode:
    target: SSHTarget
    key_id: str
    identity: NodeIdentity
    identified_at: float


class SetupService:
    """State and validation for the browser onboarding wizard."""

    def __init__(
        self,
        *,
        paths: AppPaths,
        backend: DiscoveryBackend,
        on_nodes_saved: Callable[[], None] = lambda: None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.paths = paths
        self.backend = backend
        self._on_nodes_saved = on_nodes_saved
        self._clock = clock
        self._mutex = threading.RLock()
        self._jobs: dict[str, ScanJob] = {}
        self._identified: dict[tuple[str, int], _IdentifiedNode] = {}

    def known_hosts_options(self) -> tuple[str, ...]:
        return hostkeys.ssh_known_hosts_options(self.paths.known_hosts_file)

    def overview(self) -> dict[str, Any]:
        """Local facts the wizard needs before the first scan."""
        try:
            networks = self.backend.local_networks()
        except OSError:
            networks = []
        return {
            "networks": [item.to_dict() for item in networks],
            "default_targets": [
                str(target) for target in network.default_scan_targets(networks)
            ],
            "ssh_keys": self.ssh_keys(),
            "nodes_file_exists": self.paths.nodes_file.exists(),
            "default_ssh_user": local_user_name(),
        }

    def ssh_keys(self) -> list[dict[str, Any]]:
        return [
            candidate.to_dict()
            for candidate in sshkeys.find_key_candidates(
                controller_key_dir=self.paths.ssh_dir
            )
        ]

    def generate_key(self) -> dict[str, Any]:
        candidate = sshkeys.generate_key(self.paths.ssh_dir)
        return {
            "key": candidate.to_dict(),
            "public_key": sshkeys.public_key_text(Path(candidate.path)),
        }

    def start_scan(self, body: Mapping[str, Any]) -> dict[str, Any]:
        _require_keys(body, {"targets"})
        raw_targets = body.get("targets")
        if raw_targets is None:
            targets = network.default_scan_targets(self.backend.local_networks())
        elif isinstance(raw_targets, list) and all(
            isinstance(item, str) for item in raw_targets
        ):
            targets = [network.validate_scan_target(item) for item in raw_targets[:8]]
        else:
            raise DiscoveryError(
                "invalid_scan_target", "targets must be a list of CIDR strings."
            )
        if not targets:
            raise DiscoveryError(
                "no_scan_target",
                "No private local network was found to scan.",
                "Enter the subnet your Sparks are on, for example 192.168.1.0/24.",
            )
        exclude = [item.address for item in self.backend.local_networks()]
        job = ScanJob(
            job_id="scan-" + secrets.token_hex(6),
            targets=tuple(str(target) for target in targets),
            total=sum(max(target.num_addresses - 2, 1) for target in targets),
        )
        with self._mutex:
            if any(item.state == "running" for item in self._jobs.values()):
                raise DiscoveryError(
                    "scan_in_progress",
                    "A network scan is already running.",
                    "Wait for it to finish or cancel it.",
                )
            self._jobs = {
                key: value
                for key, value in self._jobs.items()
                if self._clock() - value.started_at < 3600
            }
            self._jobs[job.job_id] = job
        thread = threading.Thread(
            target=self._run_scan,
            args=(job, targets, exclude),
            name="dgx-controller-scan",
            daemon=True,
        )
        thread.start()
        return job.resource()

    def _run_scan(
        self,
        job: ScanJob,
        targets: Sequence[ipaddress.IPv4Network],
        exclude: Sequence[str],
    ) -> None:
        def progress(done: int, total: int) -> None:
            job.done, job.total = done, total

        try:
            candidates = self.backend.scan(
                targets, exclude=exclude, progress=progress, cancel=job.cancel
            )
            job.candidates = [candidate.to_dict() for candidate in candidates]
            job.state = "cancelled" if job.cancel.is_set() else "succeeded"
        except DiscoveryError as error:
            job.error = {"code": error.code, "message": error.message}
            job.state = "failed"
        except Exception:
            job.error = {"code": "scan_failed", "message": "The network scan failed."}
            job.state = "failed"

    def get_scan(self, job_id: str) -> dict[str, Any]:
        with self._mutex:
            job = self._jobs.get(job_id)
        if job is None:
            raise DiscoveryError("scan_not_found", "The scan does not exist.")
        return job.resource()

    def cancel_scan(self, job_id: str) -> dict[str, Any]:
        with self._mutex:
            job = self._jobs.get(job_id)
        if job is None:
            raise DiscoveryError("scan_not_found", "The scan does not exist.")
        job.cancel.set()
        return job.resource()

    def host_keys(self, body: Mapping[str, Any]) -> dict[str, Any]:
        _require_keys(body, {"host", "port"})
        host, port = _host(body), _port(body)
        store = hostkeys.KnownHostsStore(self.paths.known_hosts_file)
        keys = self.backend.host_keys(host, port)
        recorded = self.backend.user_known_hosts(host, port)
        return {
            "host": host,
            "port": port,
            "keys": [
                {
                    "key_type": key.key_type,
                    "fingerprint_sha256": key.fingerprint_sha256,
                    "trusted": store.is_trusted(key),
                    "user_known_hosts": hostkeys.known_hosts_status(key, recorded),
                }
                for key in keys
            ],
        }

    def trust_host_key(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Pin host keys whose fingerprints the operator confirmed.

        The keys are fetched again and only those matching a confirmed
        fingerprint are stored, so a key that changed between display and
        confirmation is never trusted.
        """
        _require_keys(body, {"host", "port", "fingerprints"})
        host, port = _host(body), _port(body)
        fingerprints = body.get("fingerprints")
        if not isinstance(fingerprints, list) or not fingerprints:
            raise DiscoveryError(
                "invalid_fingerprints", "Confirm at least one host key fingerprint."
            )
        confirmed = {item for item in fingerprints if isinstance(item, str)}
        keys = [
            key
            for key in self.backend.host_keys(host, port)
            if key.fingerprint_sha256 in confirmed
        ]
        if not keys:
            raise DiscoveryError(
                "host_key_mismatch",
                "The node's current host key does not match the confirmed fingerprint.",
                "Rescan the host key and compare it on the node's console.",
            )
        hostkeys.KnownHostsStore(self.paths.known_hosts_file).trust(keys)
        return {"host": host, "port": port, "trusted": len(keys)}

    def identify(self, body: Mapping[str, Any]) -> dict[str, Any]:
        _require_keys(body, {"host", "user", "key_id", "port"})
        host = _host(body)
        user = sshkeys.validate_username(_text(body.get("user"), "user"))
        port = _port(body)
        key = self._key(_text(body.get("key_id"), "key_id"))
        target = SSHTarget(host=host, user=user, key_path=key.path, port=port)
        identity = self.backend.identify(target, self.known_hosts_options())
        result = identity.to_dict()
        if identity.ok:
            with self._mutex:
                self._identified[(host, port)] = _IdentifiedNode(
                    target, key.id, identity, self._clock()
                )
            result["suggested"] = self._suggested_node(identity, target, key)
        elif identity.error_code == "auth_failed":
            result["copy_id_command"] = sshkeys.copy_id_command(
                user, host, Path(key.path), port=port
            )
        return result

    def check_links(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Ping every other identified node's cluster addresses from one node."""
        _require_keys(body, {"host", "port"})
        host, port = _host(body), _port(body)
        with self._mutex:
            source = self._fresh_identity(host, port)
            peers = [
                address
                for (other_host, other_port), node in self._identified.items()
                if (other_host, other_port) != (host, port)
                for address in node.identity.cluster_addresses
            ]
        results = self.backend.cluster_links(
            source.target, self.known_hosts_options(), peers
        )
        return {"host": host, "links": results}

    def save_nodes(self, body: Mapping[str, Any]) -> dict[str, Any]:
        """Write ``nodes.json`` from nodes identified during this session."""
        if not isinstance(body, Mapping) or set(body) - {"nodes", "replace"}:
            raise DiscoveryError(
                "invalid_request", "Send only 'nodes' and optional 'replace'."
            )
        raw_nodes = body.get("nodes")
        if not isinstance(raw_nodes, list) or not 1 <= len(raw_nodes) <= 16:
            raise DiscoveryError("invalid_request", "Select between 1 and 16 nodes.")
        replace = body.get("replace", False)
        if not isinstance(replace, bool):
            raise DiscoveryError("invalid_request", "'replace' must be true or false.")
        new_entries = [self._node_entry(item) for item in raw_nodes]
        existing: list[dict[str, Any]] = []
        if not replace and self.paths.nodes_file.exists():
            try:
                document = json.loads(self.paths.nodes_file.read_text(encoding="utf-8"))
                existing = list(document.get("nodes", []))
            except (OSError, json.JSONDecodeError, AttributeError) as error:
                raise DiscoveryError(
                    "nodes_file_unreadable",
                    "The existing nodes.json cannot be merged.",
                    "Save with 'replace' to start a fresh configuration.",
                ) from error
        new_ids = {entry["id"] for entry in new_entries}
        new_addresses = {entry["management_address"] for entry in new_entries}
        merged = [
            entry
            for entry in existing
            if entry.get("id") not in new_ids
            and entry.get("management_address") not in new_addresses
        ] + new_entries
        if len({entry["id"] for entry in merged}) != len(merged):
            raise DiscoveryError("duplicate_node_id", "Node ids must be unique.")
        document = {"schema_version": 1, "nodes": merged}
        try:
            parse_nodes_document(document)
        except ValueError as error:
            raise DiscoveryError(
                "invalid_node_config",
                f"The resulting configuration is invalid: {error}",
            ) from error
        self._write_nodes(document)
        self._on_nodes_saved()
        return {"nodes": merged, "path": str(self.paths.nodes_file)}

    def _node_entry(self, raw: Any) -> dict[str, Any]:
        if not isinstance(raw, Mapping) or set(raw) - SAVE_NODE_KEYS:
            raise DiscoveryError(
                "invalid_request", "A node entry has unsupported fields."
            )
        host, port = _host(raw), _port(raw)
        with self._mutex:
            identified = self._fresh_identity(host, port)
        identity = identified.identity
        key = self._key(_text(raw.get("key_id"), "key_id"))
        suggested = self._suggested_node(identity, identified.target, key)
        node_id = raw.get("node_id") or suggested["id"]
        if not isinstance(node_id, str) or not NODE_ID_RE.fullmatch(node_id):
            raise DiscoveryError(
                "invalid_node_id",
                "Node ids use lowercase letters, digits, '.', '_' and '-'.",
            )
        display_name = raw.get("display_name") or suggested["display_name"]
        if not isinstance(display_name, str) or not 1 <= len(display_name) <= 80:
            raise DiscoveryError(
                "invalid_display_name", "Display names are 1-80 characters."
            )
        model_root = raw.get("model_root") or suggested["model_roots"][0]
        if not isinstance(model_root, str) or not MODEL_ROOT_RE.fullmatch(model_root):
            raise DiscoveryError(
                "invalid_model_root", "The model root must be an absolute plain path."
            )
        allowed_cluster = set(identity.cluster_addresses) | {
            address.split("/")[0]
            for interface in identity.interfaces
            for address in interface.addresses
        }
        cluster_addresses = raw.get("cluster_addresses", suggested["cluster_addresses"])
        if not isinstance(cluster_addresses, list) or not set(cluster_addresses) <= (
            allowed_cluster
        ):
            raise DiscoveryError(
                "invalid_cluster_address",
                "Cluster addresses must be addresses the node itself reported.",
            )
        entry = dict(suggested)
        entry.update(
            id=node_id,
            display_name=display_name.strip(),
            model_roots=[model_root],
            cluster_addresses=list(cluster_addresses),
        )
        entry["ssh"] = dict(suggested["ssh"], key_path=key.display_path)
        return entry

    def _suggested_node(
        self, identity: NodeIdentity, target: SSHTarget, key: sshkeys.KeyCandidate
    ) -> dict[str, Any]:
        hostname = identity.hostname or target.host
        node_id = re.sub(r"[^a-z0-9-]+", "-", hostname.lower()).strip("-") or "spark"
        capabilities = ["openai-compatible-http"]
        if identity.gpus:
            capabilities.append("cuda")
        if identity.docker_version:
            capabilities.append("docker")
            if identity.gpus:
                capabilities.append("vllm")
        if identity.rdma_devices:
            capabilities.append("roce")
        labels = ["spark", "gb10"] if identity.is_dgx_spark else ["linux-gpu"]
        home = identity.home_dir if identity.home_dir.startswith("/") else "/home"
        return {
            "id": node_id[:63],
            "display_name": hostname[:80],
            "ssh": {
                "address": target.host,
                "user": target.user,
                "key_path": key.display_path,
                "connect_timeout_s": 10,
            },
            "management_address": target.host,
            "cluster_addresses": list(identity.cluster_addresses),
            "labels": labels,
            "capabilities": sorted(capabilities),
            "model_roots": [f"{home.rstrip('/')}/models"],
            "endpoint_ports": {"start": 8000, "end": 8999},
        }

    def _fresh_identity(self, host: str, port: int) -> _IdentifiedNode:
        identified = self._identified.get((host, port))
        if identified is None or self._clock() - identified.identified_at > (
            IDENTITY_TTL_SECONDS
        ):
            raise DiscoveryError(
                "node_not_identified",
                f"{host} has not been identified in this session.",
                "Run the read-only identification step for this node again.",
            )
        return identified

    def _key(self, key_id: str) -> sshkeys.KeyCandidate:
        for candidate in sshkeys.find_key_candidates(
            controller_key_dir=self.paths.ssh_dir
        ):
            if candidate.id == key_id:
                return candidate
        raise DiscoveryError(
            "ssh_key_not_found",
            "The selected SSH key is no longer available.",
            "Refresh the key list and choose again.",
        )

    def _write_nodes(self, document: Mapping[str, Any]) -> None:
        target = self.paths.nodes_file
        if target.exists():
            backup = target.with_name(f"nodes.json.bak-{int(self._clock())}")
            backup.write_bytes(target.read_bytes())
            os.chmod(backup, 0o600)
        write_private_file(
            target, json.dumps(document, indent=2) + "\n", prefix=".nodes-"
        )


class FakeDiscoveryBackend:
    """Deterministic two-Spark lab used by fake mode and tests."""

    NODES = {
        "192.168.50.10": ("spark-1a2b", "10.10.10.1"),
        "192.168.50.11": ("spark-3c4d", "10.10.10.2"),
    }

    def __init__(
        self,
        *,
        user_known_hosts: Mapping[str, list[hostkeys.KnownHostEntry] | None]
        | None = None,
    ) -> None:
        # Never the operator's real ~/.ssh/known_hosts: tests inject entries.
        self._user_known_hosts = dict(user_known_hosts or {})

    def local_networks(self) -> list[network.LocalNetwork]:
        return [network.LocalNetwork("eth0", "192.168.50.2", "192.168.50.0/24")]

    def scan(
        self,
        targets: Sequence[ipaddress.IPv4Network],
        *,
        exclude: Sequence[str],
        progress: Callable[[int, int], None],
        cancel: threading.Event,
    ) -> list[scanner.Candidate]:
        progress(254, 254)
        results = []
        for address, (hostname, _cluster) in self.NODES.items():
            if not any(ipaddress.IPv4Address(address) in target for target in targets):
                continue
            results.append(
                scanner.classify(
                    address,
                    hostname=hostname,
                    ssh_banner="SSH-2.0-OpenSSH_9.6p1 Ubuntu-3ubuntu13.5",
                    open_ports=(22,),
                    served_models=(),
                )
            )
        return results

    def host_keys(self, host: str, port: int) -> list[hostkeys.HostKey]:
        blob = "AAAAC3NzaC1lZDI1NTE5AAAAI" + "A" * 43
        return [
            hostkeys.HostKey(
                host=host,
                port=port,
                key_type="ssh-ed25519",
                public_key=blob,
                fingerprint_sha256=hostkeys.fingerprint(blob),
            )
        ]

    def user_known_hosts(
        self, host: str, port: int
    ) -> list[hostkeys.KnownHostEntry] | None:
        return self._user_known_hosts.get(host, [])

    def identify(
        self, target: SSHTarget, known_hosts_options: Sequence[str]
    ) -> NodeIdentity:
        hostname, cluster = self.NODES.get(target.host, ("", ""))
        if not hostname:
            return NodeIdentity.failed(target, "unreachable", "No fake node here.")
        return NodeIdentity(
            host=target.host,
            user=target.user,
            ok=True,
            hostname=hostname,
            os_pretty="Ubuntu 24.04.3 LTS (DGX OS)",
            arch="aarch64",
            product_name="NVIDIA DGX Spark",
            gpus=("NVIDIA GB10",),
            gpu_driver="580.95.05",
            cuda_version="13.0",
            memory_total_bytes=128 * 1024**3,
            home_dir=f"/home/{target.user}",
            disk_free_bytes=3 * 1024**4,
            docker_version="28.3.3",
            nvidia_runtime=True,
            gpu_container_support="runtime",
            systemd_user_state="running",
            linger=True,
            interfaces=(
                NetworkInterface("enP7s7", "r8127", 10000, (f"{target.host}/24",)),
                NetworkInterface(
                    "enp1s0f1np1", "mlx5_core", 200000, (f"{cluster}/24",)
                ),
            ),
            rdma_devices=("rocep1s0f1",),
            is_dgx_spark=True,
            spark_evidence=("GPU reports NVIDIA GB10", "aarch64 (Grace CPU)"),
            cluster_addresses=(cluster,),
        )

    def cluster_links(
        self,
        target: SSHTarget,
        known_hosts_options: Sequence[str],
        peers: Sequence[str],
    ) -> dict[str, bool]:
        return {peer: True for peer in peers}


def local_user_name() -> str:
    """The controller host's login name, offered as the default SSH user.

    Returns an empty string when it is unknown or not a valid node user name.
    """
    try:
        name = getpass.getuser()
    except (OSError, KeyError):
        return ""
    try:
        return sshkeys.validate_username(name)
    except DiscoveryError:
        return ""


def _require_keys(body: Mapping[str, Any], allowed: set[str]) -> None:
    if not isinstance(body, Mapping):
        raise DiscoveryError(
            "invalid_request", "The request body must be a JSON object."
        )
    unknown = sorted(set(body) - allowed)
    if unknown:
        raise DiscoveryError("invalid_request", f"Unsupported field {unknown[0]!r}.")


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 300:
        raise DiscoveryError("invalid_request", f"'{name}' is required.")
    return value.strip()


def _host(body: Mapping[str, Any]) -> str:
    return hostkeys.validate_host(_text(body.get("host"), "host"))


def _port(body: Mapping[str, Any]) -> int:
    """The optional SSH ``port`` of a request body, 22 when absent."""
    return hostkeys.validate_port(body.get("port", 22))
