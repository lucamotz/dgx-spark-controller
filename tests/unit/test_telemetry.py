# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Offline capacity collection, partial metrics and shared polling bounds."""

import os
import subprocess
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock

from controller.telemetry import NodeTelemetry, TelemetryCache
from controller.transport.ssh import SSHExecution, SSHExecutionError
from controller.transport.telemetry import (
    TELEMETRY_SCRIPT,
    parse_telemetry,
    sample_ssh_telemetry,
)

MEMORY = "MemTotal: 131072\nMemAvailable: 32768\nSwapTotal: 8192\nSwapFree: 4096\n"


class TelemetryTests(unittest.TestCase):
    def test_memory_is_total_less_available_and_zero_gpu_is_valid(self):
        sample = parse_telemetry("spark", MEMORY + "GPU\n0\n").to_dict()
        self.assertEqual("ok", sample["status"])
        self.assertEqual(75.0, sample["memory"]["used_percent"])
        self.assertEqual(96 * 1024 * 1024, sample["memory"]["used_bytes"])
        self.assertEqual(4 * 1024 * 1024, sample["memory"]["swap_used_bytes"])
        self.assertEqual(0.0, sample["gpu"]["utilization_percent"])
        self.assertIsNotNone(sample["sampled_at"])

    def test_unsupported_or_invalid_gpu_preserves_system_memory(self):
        for gpu in ("N/A", "[Not Supported]", "", "nan", "inf", "-1", "101", "22\n30"):
            with self.subTest(gpu=gpu):
                sample = parse_telemetry("spark", MEMORY + "GPU\n" + gpu)
                self.assertEqual("partial", sample.status)
                self.assertIsNone(sample.gpu_utilization_percent)
                self.assertIsNotNone(sample.memory)

    def test_bad_memory_is_unknown_instead_of_empty_or_full(self):
        for memory in (
            "",
            "MemTotal: 0\nMemAvailable: 0\n",
            "MemTotal: 1\nMemAvailable: 2\n",
        ):
            with self.subTest(memory=memory):
                sample = parse_telemetry("spark", memory + "GPU\n42\n")
                self.assertEqual("partial", sample.status)
                self.assertIsNone(sample.memory)
                self.assertEqual(42, sample.gpu_utilization_percent)
        self.assertEqual(
            "unavailable", parse_telemetry("spark", "invalid output").status
        )

    def test_missing_swap_does_not_claim_zero(self):
        sample = parse_telemetry("spark", "MemTotal: 1024\nMemAvailable: 256\nGPU\n12")
        self.assertIsNone(sample.memory.swap_used_bytes)

    def test_memory_pressure_skip_preserves_memory_and_explains_missing_gpu(self):
        sample = parse_telemetry("spark", MEMORY + "GPU\ndeferred\n")
        self.assertEqual("partial", sample.status)
        self.assertIsNotNone(sample.memory)
        self.assertIsNone(sample.gpu_utilization_percent)
        self.assertIn("below 1 GiB", sample.message)

    def test_remote_script_skips_gpu_process_under_pressure_and_bounds_normal_query(
        self,
    ):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            meminfo = root / "meminfo"
            command_log = root / "query"
            timeout = root / "timeout"
            timeout.write_text(
                '#!/bin/bash\nprintf "%s\\n" "$@" > "$QUERY_LOG"\nprintf "12\\n"\n'
            )
            timeout.chmod(0o755)
            environment = {
                **os.environ,
                "PATH": temporary + os.pathsep + os.environ["PATH"],
                "QUERY_LOG": str(command_log),
            }
            script = TELEMETRY_SCRIPT.replace("/proc/meminfo", str(meminfo))
            for available in (1048575, 1048576):
                meminfo.write_text(
                    f"MemTotal: 2097152 kB\nMemAvailable: {available} kB\n"
                    "SwapTotal: 0 kB\nSwapFree: 0 kB\n"
                )
                result = subprocess.run(
                    ["/bin/bash"],
                    input=script,
                    text=True,
                    capture_output=True,
                    env=environment,
                    timeout=2,
                    check=True,
                )
                sample = parse_telemetry("spark", result.stdout)
                if available < 1048576:
                    self.assertFalse(command_log.exists())
                    self.assertIsNone(sample.gpu_utilization_percent)
                else:
                    self.assertEqual(12, sample.gpu_utilization_percent)
                    self.assertEqual(
                        [
                            "--signal=KILL",
                            "3s",
                            "nvidia-smi",
                            "--id=0",
                            "--query-gpu=utilization.gpu",
                            "--format=csv,noheader,nounits",
                        ],
                        command_log.read_text().splitlines(),
                    )

    def test_fixed_ssh_query_is_bounded_and_failure_detail_does_not_escape(self):
        executor = Mock()
        executor.run_script.return_value = SSHExecution(0, MEMORY + "GPU\n75\n", "")
        self.assertEqual(
            75, sample_ssh_telemetry(executor, "spark").gpu_utilization_percent
        )
        executor.run_script.assert_called_once_with(
            "spark", TELEMETRY_SCRIPT, timeout_seconds=8, check=False
        )
        executor.run_script.return_value = SSHExecution(
            255, "token=private", "ssh private detail"
        )
        sample = sample_ssh_telemetry(executor, "spark")
        self.assertEqual("unavailable", sample.status)
        self.assertNotIn("private", str(sample.to_dict()))

    def test_cache_rate_limits_successes_and_timeouts_until_interval_expires(self):
        now = [0.0]
        collector = Mock(
            side_effect=[
                NodeTelemetry("spark", message="first"),
                SSHExecutionError("token=secret"),
            ]
        )
        cache = TelemetryCache(("spark",), collector, monotonic=lambda: now[0])
        first = cache.snapshot()
        now[0] = 9.99
        self.assertEqual(first, cache.snapshot())
        self.assertEqual(1, collector.call_count)
        now[0] = 10
        failed = cache.snapshot()
        self.assertEqual("unavailable", failed["items"][0]["status"])
        self.assertNotIn("secret", str(failed))
        now[0] = 19.99
        self.assertEqual(failed, cache.snapshot())
        self.assertEqual(2, collector.call_count)

    def test_concurrent_clients_share_one_inflight_sample(self):
        entered = threading.Event()
        release = threading.Event()
        calls = []

        def collect(node_id):
            calls.append(node_id)
            entered.set()
            self.assertTrue(release.wait(2))
            return NodeTelemetry(node_id)

        cache = TelemetryCache(("spark",), collect)
        with ThreadPoolExecutor(max_workers=5) as workers:
            futures = [workers.submit(cache.snapshot) for _ in range(5)]
            self.assertTrue(entered.wait(2))
            release.set()
            results = [future.result(timeout=2) for future in futures]
        self.assertEqual(["spark"], calls)
        self.assertTrue(all(result == results[0] for result in results))


if __name__ == "__main__":
    unittest.main()
