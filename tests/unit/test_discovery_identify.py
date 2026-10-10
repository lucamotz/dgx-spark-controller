# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Interpretation of recorded read-only identify probe outputs.

The fixtures mirror what the probe's ``emit`` prints on a DGX OS node: one
``KEY=<base64>`` line per fact. Docker 29 on DGX OS often exposes the GPU only
through CDI, without a registered ``nvidia`` runtime.
"""

from __future__ import annotations

import base64
import unittest

from controller.discovery.identify import (
    PROBE_SCRIPT,
    SSHTarget,
    interpret_probe,
    node_identity_from_mapping,
)

TARGET = SSHTarget(host="192.0.2.10", user="spark", key_path="/tmp/id_ed25519")

RUNC_ONLY = (
    '{"io.containerd.runc.v2":{"path":"runc","status":{"org.opencontainers.runtime-'
    'spec.features":"{}"}},"runc":{"path":"runc"}}'
)
WITH_NVIDIA = (
    '{"io.containerd.runc.v2":{"path":"runc"},"nvidia":{"path":'
    '"nvidia-container-runtime"},"runc":{"path":"runc"}}'
)
CDI_JSON = (
    '[{"Source":"cdi","ID":"nvidia.com/gpu=0"},'
    '{"Source":"cdi","ID":"nvidia.com/gpu=GPU-00000000-1111-2222-3333-444444444444"},'
    '{"Source":"cdi","ID":"nvidia.com/gpu=all"}]'
)
# ``docker info`` text on Docker versions whose template lacks DiscoveredDevices;
# the probe keeps only the ``cdi:`` lines.
CDI_TEXT = (
    "  cdi: nvidia.com/gpu=0\n"
    "  cdi: nvidia.com/gpu=GPU-00000000-1111-2222-3333-444444444444\n"
    "  cdi: nvidia.com/gpu=all"
)
GPU_WARNING = "neither the NVIDIA runtime nor an NVIDIA CDI device"


def probe_output(**overrides: str) -> str:
    """Encode facts exactly like the probe's ``emit`` shell function."""
    facts = {
        "HOSTNAME": "spark-1a2b",
        "OS_PRETTY": "Ubuntu 24.04.3 LTS",
        "ARCH": "aarch64",
        "PRODUCT_NAME": "NVIDIA DGX Spark",
        "GPU": "NVIDIA GB10, 580.95.05",
        "CUDA": "CUDA Version: 13.0",
        "MEM_TOTAL": str(128 * 1024**3),
        "HOME_DIR": "/home/spark",
        "DOCKER_VERSION": "29.1.3",
        "DOCKER_RUNTIMES": RUNC_ONLY,
        "DOCKER_CDI_JSON": "",
        "SYSTEMD_USER": "running",
        "LINGER": "yes",
        **overrides,
    }
    return "".join(
        f"{key}={base64.b64encode(value.encode()).decode()}\n"
        for key, value in facts.items()
    )


def gpu_warnings(identity) -> list[str]:
    return [warning for warning in identity.warnings if GPU_WARNING in warning]


class GPUContainerSupportTests(unittest.TestCase):
    def test_nvidia_runtime_only(self) -> None:
        identity = interpret_probe(
            TARGET,
            probe_output(DOCKER_RUNTIMES=WITH_NVIDIA, DOCKER_CDI_JSON="null"),
        )
        self.assertTrue(identity.nvidia_runtime)
        self.assertEqual("runtime", identity.gpu_container_support)
        self.assertEqual([], gpu_warnings(identity))

    def test_cdi_only_from_json(self) -> None:
        identity = interpret_probe(TARGET, probe_output(DOCKER_CDI_JSON=CDI_JSON))
        self.assertFalse(identity.nvidia_runtime)
        self.assertEqual("cdi", identity.gpu_container_support)
        self.assertEqual([], gpu_warnings(identity))

    def test_cdi_only_from_docker_info_text(self) -> None:
        identity = interpret_probe(TARGET, probe_output(DOCKER_CDI_TEXT=CDI_TEXT))
        self.assertEqual("cdi", identity.gpu_container_support)
        self.assertEqual([], gpu_warnings(identity))

    def test_neither_runtime_nor_cdi_warns(self) -> None:
        for cdi_json in ("", "null", "[]", '[{"Source":"cdi","ID":"vendor/x=0"}]'):
            with self.subTest(cdi_json=cdi_json):
                identity = interpret_probe(
                    TARGET, probe_output(DOCKER_CDI_JSON=cdi_json)
                )
                self.assertIsNone(identity.gpu_container_support)
                self.assertEqual(1, len(gpu_warnings(identity)))

    def test_json_list_wins_over_text_and_bad_values_are_ignored(self) -> None:
        identity = interpret_probe(
            TARGET,
            probe_output(
                DOCKER_CDI_JSON='[{"Source":"cdi","ID":"nvidia.com/gpu=all; rm"}]',
                DOCKER_CDI_TEXT=CDI_TEXT,
            ),
        )
        self.assertIsNone(identity.gpu_container_support)

    def test_no_docker_reports_only_the_docker_warning(self) -> None:
        identity = interpret_probe(
            TARGET, probe_output(DOCKER_VERSION="", DOCKER_RUNTIMES="")
        )
        self.assertIsNone(identity.gpu_container_support)
        self.assertEqual([], gpu_warnings(identity))
        self.assertTrue(any("Docker is not usable" in w for w in identity.warnings))

    def test_identity_round_trips_through_the_cached_mapping(self) -> None:
        identity = interpret_probe(TARGET, probe_output(DOCKER_CDI_JSON=CDI_JSON))
        rebuilt = node_identity_from_mapping(identity.to_dict())
        self.assertEqual("cdi", rebuilt.gpu_container_support)

    def test_probe_reads_cdi_json_first_then_falls_back_to_text(self) -> None:
        json_query = PROBE_SCRIPT.index("{{json .DiscoveredDevices}}")
        text_query = PROBE_SCRIPT.index("cdi: ")
        self.assertLess(json_query, text_query)
        self.assertIn("emit DOCKER_CDI_TEXT", PROBE_SCRIPT)


if __name__ == "__main__":
    unittest.main()
