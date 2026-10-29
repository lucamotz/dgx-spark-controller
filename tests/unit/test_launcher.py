# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Unit tests for the cross-platform desktop launcher.

No GUI toolkit is started and port 18876 is never bound: collaborators are
fakes, and the few real sockets/processes use ephemeral ports or trivial
local Python children that the test terminates itself.
"""

from __future__ import annotations

import http.server
import importlib
import io
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import zlib
from collections.abc import Callable
from pathlib import Path
from unittest import mock

import controller.main
from controller import launcher
from controller.assets import ICON_PNG, ICON_PNG_256
from controller.assets.generate_icon import render_png
from controller.paths import resolve_app_paths

ROOT = Path(__file__).resolve().parents[2]
LAUNCHER_SOURCE = ROOT / "controller" / "launcher.py"


class FakeResponse:
    def __init__(
        self, status: int = 200, body: bytes | str = b'{"status": "ok"}'
    ) -> None:
        self.status = status
        self._body = body.encode() if isinstance(body, str) else body

    def read(self, _limit: int = -1) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_exc) -> None:
        return None


class RecordingOpener:
    """Opener fake that answers from a script and records every requested URL."""

    def __init__(self, *responses) -> None:
        self.responses = list(responses)
        self.urls: list[str] = []

    def __call__(self, url: str, timeout: float):
        self.urls.append(url)
        response = (
            self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
        )
        if isinstance(response, BaseException):
            raise response
        return response


class FakeProcess:
    """Popen-like fake with scripted behaviour for terminate/kill."""

    def __init__(
        self, *, exits_on_terminate: bool = True, exit_code: int | None = None
    ) -> None:
        self.returncode = exit_code
        self.exits_on_terminate = exits_on_terminate
        self.calls: list[str] = []

    def poll(self):
        return self.returncode

    def terminate(self) -> None:
        self.calls.append("terminate")
        if self.exits_on_terminate:
            self.returncode = -signal.SIGTERM

    def kill(self) -> None:
        self.calls.append("kill")
        self.returncode = -signal.SIGKILL

    def wait(self, timeout=None):
        self.calls.append(f"wait:{timeout}")
        if self.returncode is None:
            raise subprocess.TimeoutExpired("fake", timeout)
        return self.returncode


class CrashingProcess(FakeProcess):
    """Reports running for ``healthy_polls`` polls, then exits with ``exit_code``."""

    def __init__(self, *, healthy_polls: int, exit_code: int) -> None:
        super().__init__()
        self._remaining = healthy_polls
        self._crash_code = exit_code

    def poll(self):
        if self.returncode is None:
            if self._remaining <= 0:
                self.returncode = self._crash_code
            self._remaining -= 1
        return self.returncode


class FakePopen:
    def __init__(self, process: FakeProcess | None = None) -> None:
        self.process = process or FakeProcess()
        self.calls: list[tuple[list[str], dict]] = []

    def __call__(self, command, **kwargs):
        self.calls.append((list(command), kwargs))
        return self.process


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _kill_if_running(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.kill()
        process.wait()


class TempHomeTestCase(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.paths = resolve_app_paths(env={"DGX_CONTROLLER_HOME": self._tmp.name})


class CommandAndEnvironmentTests(unittest.TestCase):
    def test_service_command_is_exact_loopback_module_invocation(self):
        self.assertEqual(
            launcher.build_service_command("/venv/bin/python", 18999),
            [
                "/venv/bin/python",
                "-m",
                "controller",
                "--host",
                "127.0.0.1",
                "--port",
                "18999",
            ],
        )

    def test_service_command_rejects_out_of_range_ports(self):
        for port in (0, 65536, -1):
            with self.assertRaises(ValueError):
                launcher.build_service_command("python", port)

    def test_child_environment_opts_into_parent_watchdog_without_mutating_input(self):
        base = {"PATH": "/usr/bin", "DGX_CONTROLLER_EXIT_WITH_PARENT": "0"}
        environment = launcher.build_child_environment(base)
        self.assertEqual(environment["DGX_CONTROLLER_EXIT_WITH_PARENT"], "1")
        self.assertEqual(environment["PYTHONUNBUFFERED"], "1")
        self.assertEqual(environment["PATH"], "/usr/bin")
        self.assertEqual(base["DGX_CONTROLLER_EXIT_WITH_PARENT"], "0")

    def test_default_port_is_the_service_default(self):
        self.assertEqual(
            launcher.parse_args([]).port, controller.main.parse_args([]).port
        )

    def test_parse_args_flags_and_validation(self):
        args = launcher.parse_args(
            ["--port", "19000", "--browser", "--no-open", "--python", "/x/python"]
        )
        self.assertEqual(
            (args.port, args.browser, args.no_open, args.python),
            (19000, True, True, "/x/python"),
        )
        with mock.patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit):
            launcher.parse_args(["--port", "70000"])


class HealthCheckTests(unittest.TestCase):
    def test_healthy_only_for_200_with_status_ok_json(self):
        opener = RecordingOpener(FakeResponse(200, b'{"status": "ok", "version": "x"}'))
        self.assertTrue(launcher.check_health("http://127.0.0.1:1/", opener=opener))
        self.assertEqual(opener.urls, ["http://127.0.0.1:1/api/health"])

    def test_unhealthy_variants(self):
        cases = [
            FakeResponse(200, b'{"status": "degraded"}'),
            FakeResponse(200, b'["ok"]'),
            FakeResponse(200, b"not json"),
            FakeResponse(200, b"\xff\xfe"),
            FakeResponse(503, b'{"status": "ok"}'),
            urllib.error.URLError("refused"),
            ConnectionResetError("reset"),
            TimeoutError("slow"),
        ]
        for case in cases:
            with self.subTest(case=case):
                self.assertFalse(
                    launcher.check_health(
                        "http://127.0.0.1:1", opener=RecordingOpener(case)
                    )
                )

    def test_real_loopback_probe_ignores_proxy_environment(self):
        class Handler(http.server.BaseHTTPRequestHandler):
            seen: list[str] = []

            def do_GET(self):  # noqa: N802 - stdlib hook name
                Handler.seen.append(self.path)
                body = json.dumps({"status": "ok"}).encode()
                self.send_response(200 if self.path == "/api/health" else 404)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                return None

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        url = launcher.service_url(server.server_address[1])
        proxy = {
            "http_proxy": "http://127.0.0.1:9",
            "HTTP_PROXY": "http://127.0.0.1:9",
            "no_proxy": "",
            "NO_PROXY": "",
        }
        with mock.patch.dict(os.environ, proxy):
            self.assertTrue(launcher.check_health(url))
        self.assertEqual(Handler.seen, ["/api/health"])

    def test_port_probe_detects_listener_on_ephemeral_port(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            port = listener.getsockname()[1]
            self.assertTrue(launcher.port_accepts_connections("127.0.0.1", port))
        self.assertFalse(launcher.port_accepts_connections("127.0.0.1", _free_port()))


class UiChoiceTests(unittest.TestCase):
    @staticmethod
    def finder(*available: str):
        return lambda name: object() if name in available else None

    def test_flags_win(self):
        self.assertEqual(
            launcher.choose_ui(no_open=True, force_browser=True).mode, "none"
        )
        self.assertEqual(
            launcher.choose_ui(
                force_browser=True, find_spec=self.finder("webview")
            ).mode,
            "browser",
        )

    def test_missing_pywebview_falls_back_to_browser(self):
        choice = launcher.choose_ui(platform="darwin", env={}, find_spec=self.finder())
        self.assertEqual(choice.mode, "browser")
        self.assertIn("pywebview", choice.reason)

    def test_macos_with_pywebview_uses_window(self):
        self.assertEqual(
            launcher.choose_ui(
                platform="darwin", env={}, find_spec=self.finder("webview")
            ).mode,
            "window",
        )

    def test_linux_without_display_prints_url_instead_of_spawning_a_console_browser(
        self,
    ):
        choice = launcher.choose_ui(
            platform="linux",
            env={"DISPLAY": " "},
            find_spec=self.finder("webview", "gi"),
        )
        self.assertEqual(choice.mode, "url")
        self.assertIn("DISPLAY", choice.reason)

    def test_linux_needs_gtk_or_qt_bindings(self):
        x11 = {"DISPLAY": ":0"}
        choice = launcher.choose_ui(
            platform="linux", env=x11, find_spec=self.finder("webview")
        )
        self.assertEqual(choice.mode, "browser")
        self.assertIn("desktop-gtk", choice.reason)
        self.assertEqual(
            launcher.choose_ui(
                platform="linux", env=x11, find_spec=self.finder("webview", "gi")
            ).mode,
            "window",
        )
        wayland = {"WAYLAND_DISPLAY": "wayland-0"}
        self.assertEqual(
            launcher.choose_ui(
                platform="linux", env=wayland, find_spec=self.finder("webview", "qtpy")
            ).mode,
            "window",
        )

    def test_broken_find_spec_counts_as_unavailable(self):
        def broken(name):
            raise ValueError(name)

        self.assertEqual(
            launcher.choose_ui(platform="darwin", env={}, find_spec=broken).mode,
            "browser",
        )


class WaitForHealthTests(unittest.TestCase):
    def test_returns_when_healthy(self):
        opener = RecordingOpener(
            urllib.error.URLError("down"), urllib.error.URLError("down"), FakeResponse()
        )
        sleeps: list[float] = []
        self.assertTrue(
            launcher.wait_for_health(
                "http://x",
                FakeProcess(),
                opener=opener,
                sleep=sleeps.append,
                poll_interval=0.1,
            )
        )
        self.assertEqual(sleeps, [0.1, 0.1])

    def test_early_exit_is_a_clear_error(self):
        opener = RecordingOpener(urllib.error.URLError("down"))
        with self.assertRaisesRegex(launcher.LaunchError, "exited with code 3"):
            launcher.wait_for_health(
                "http://x",
                FakeProcess(exit_code=3),
                opener=opener,
                sleep=lambda _s: None,
            )

    def test_timeout_is_a_clear_error(self):
        ticks = iter([0.0, 10.0, 31.0])
        opener = RecordingOpener(urllib.error.URLError("down"))
        with self.assertRaisesRegex(launcher.LaunchError, "within 30 seconds"):
            launcher.wait_for_health(
                "http://x",
                FakeProcess(),
                opener=opener,
                timeout=30,
                clock=lambda: next(ticks),
                sleep=lambda _s: None,
            )

    def test_cancel_returns_false(self):
        cancel = threading.Event()
        cancel.set()
        self.assertFalse(
            launcher.wait_for_health(
                "http://x",
                FakeProcess(),
                opener=RecordingOpener(FakeResponse()),
                cancel=cancel,
            )
        )


class StopChildTests(unittest.TestCase):
    def test_none_and_exited_processes_are_left_alone(self):
        self.assertIsNone(launcher.stop_child(None))
        exited = FakeProcess(exit_code=0)
        self.assertEqual(launcher.stop_child(exited), 0)
        self.assertEqual(exited.calls, [])

    def test_terminate_then_wait(self):
        process = FakeProcess()
        self.assertEqual(
            launcher.stop_child(process, grace_seconds=10), -signal.SIGTERM
        )
        self.assertEqual(process.calls, ["terminate", "wait:10"])

    def test_kill_after_grace_period(self):
        process = FakeProcess(exits_on_terminate=False)
        self.assertEqual(
            launcher.stop_child(process, grace_seconds=10, kill_seconds=5),
            -signal.SIGKILL,
        )
        self.assertEqual(process.calls, ["terminate", "wait:10", "kill", "wait:5"])

    @unittest.skipIf(os.name != "posix", "POSIX signal semantics")
    def test_real_child_that_ignores_sigterm_is_killed(self):
        code = (
            "import signal, sys, time\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "sys.stdout.write('ready\\n'); sys.stdout.flush()\n"
            "time.sleep(60)\n"
        )
        process = subprocess.Popen(
            [sys.executable, "-c", code], stdout=subprocess.PIPE, text=True
        )
        self.addCleanup(_kill_if_running, process)
        assert process.stdout is not None
        self.assertEqual(process.stdout.readline().strip(), "ready")
        process.stdout.close()
        self.assertEqual(
            launcher.stop_child(process, grace_seconds=0.3, kill_seconds=5),
            -signal.SIGKILL,
        )

    @unittest.skipIf(os.name != "posix", "POSIX signal semantics")
    def test_real_child_terminates_gracefully(self):
        process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"]
        )
        self.addCleanup(_kill_if_running, process)
        self.assertEqual(launcher.stop_child(process, grace_seconds=5), -signal.SIGTERM)


class LocalServiceTests(TempHomeTestCase):
    def make_service(self, opener, popen=None, **kwargs):
        return launcher.LocalService(
            port=_free_port(),
            python="/venv/bin/python",
            paths=self.paths,
            cwd=Path("/repo"),
            environ={"PATH": "/usr/bin"},
            opener=opener,
            popen=popen or FakePopen(),
            probe_port=lambda _host, _port: False,
            poll_interval=0.01,
            **kwargs,
        )

    def test_existing_healthy_service_is_reused_and_never_stopped(self):
        popen = FakePopen()
        opener = RecordingOpener(FakeResponse())
        service = self.make_service(opener, popen)
        self.assertTrue(service.start())
        self.assertTrue(service.reused)
        self.assertFalse(service.owns_child)
        self.assertEqual(popen.calls, [])
        self.assertIsNone(service.stop())
        self.assertTrue(all(url.endswith("/api/health") for url in opener.urls))

    def test_starts_child_with_watchdog_env_and_log_then_stops_only_it(self):
        process = FakeProcess()
        popen = FakePopen(process)
        opener = RecordingOpener(
            urllib.error.URLError("down"), urllib.error.URLError("down"), FakeResponse()
        )
        service = self.make_service(opener, popen)
        self.assertTrue(service.start())
        self.assertTrue(service.owns_child)
        self.assertFalse(service.reused)
        command, kwargs = popen.calls[0]
        self.assertEqual(
            command,
            [
                "/venv/bin/python",
                "-m",
                "controller",
                "--host",
                "127.0.0.1",
                "--port",
                str(service.port),
            ],
        )
        self.assertEqual(kwargs["cwd"], "/repo")
        self.assertEqual(kwargs["env"]["DGX_CONTROLLER_EXIT_WITH_PARENT"], "1")
        self.assertEqual(kwargs["stderr"], subprocess.STDOUT)
        self.assertTrue(
            os.path.samestat(
                os.fstat(kwargs["stdout"].fileno()), self.paths.app_log_file.stat()
            )
        )
        if os.name == "posix":
            self.assertEqual(self.paths.app_log_file.stat().st_mode & 0o777, 0o600)
        self.assertEqual(service.stop(), -signal.SIGTERM)
        self.assertEqual(process.calls[0], "terminate")
        self.assertFalse(service.owns_child)
        self.assertIsNone(service.stop())  # idempotent
        self.assertIn(
            "launcher stopped the local controller", self.paths.app_log_file.read_text()
        )
        self.assertTrue(all(url.endswith("/api/health") for url in opener.urls))

    def test_early_child_exit_raises_with_log_location_and_tail(self):
        process = FakeProcess(exit_code=1)
        service = self.make_service(
            RecordingOpener(urllib.error.URLError("down")), FakePopen(process)
        )
        with self.assertRaises(launcher.LaunchError) as context:
            service.start()
        message = str(context.exception)
        self.assertIn("exited with code 1", message)
        self.assertIn(str(self.paths.app_log_file), message)
        self.assertIn("starting:", message)  # log tail is included
        self.assertFalse(service.owns_child)

    def test_foreign_program_on_port_is_refused_without_spawning(self):
        popen = FakePopen()
        service = launcher.LocalService(
            port=_free_port(),
            paths=self.paths,
            opener=RecordingOpener(urllib.error.URLError("down")),
            popen=popen,
            probe_port=lambda _host, _port: True,
        )
        with self.assertRaisesRegex(
            launcher.LaunchError, "not a healthy DGX Spark Controller"
        ):
            service.start()
        self.assertEqual(popen.calls, [])

    def test_lost_bind_race_reuses_the_winner_without_owning_it(self):
        process = FakeProcess(
            exit_code=1
        )  # our child lost the port to another launcher
        opener = RecordingOpener(urllib.error.URLError("down"), FakeResponse())
        service = self.make_service(opener, FakePopen(process))
        self.assertTrue(service.start())
        self.assertTrue(service.reused)
        self.assertFalse(service.owns_child)

    def test_cancel_before_start_spawns_nothing(self):
        popen = FakePopen()
        service = self.make_service(
            RecordingOpener(urllib.error.URLError("down")), popen
        )
        service.cancel()
        self.assertFalse(service.start())
        self.assertEqual(popen.calls, [])

    def test_cancel_during_startup_stops_child(self):
        process = FakeProcess()
        popen = FakePopen(process)
        service: launcher.LocalService

        def opener(_url: str, _timeout: float):
            if popen.calls:  # the first health poll after the spawn
                service.cancel()
            raise urllib.error.URLError("down")

        service = self.make_service(opener, popen)
        self.assertFalse(service.start())
        self.assertIn("terminate", process.calls)
        self.assertFalse(service.owns_child)


class BrowserModeTests(TempHomeTestCase):
    def test_reused_service_opens_browser_and_returns_without_waiting(self):
        opened: list[str] = []

        def record(url: str) -> bool:
            opened.append(url)
            return True

        service = launcher.LocalService(
            port=_free_port(),
            paths=self.paths,
            opener=RecordingOpener(FakeResponse()),
            popen=FakePopen(),
        )
        with mock.patch("sys.stderr", io.StringIO()) as stderr:
            code = launcher.run_browser(
                service,
                mode="browser",
                open_browser=record,
            )
        self.assertEqual(code, 0)
        self.assertEqual(opened, [service.url + "/"])
        self.assertIn("already running", stderr.getvalue())

    def test_launch_failure_exits_non_zero_with_message(self):
        service = launcher.LocalService(
            port=_free_port(),
            paths=self.paths,
            opener=RecordingOpener(urllib.error.URLError("down")),
            popen=FakePopen(FakeProcess(exit_code=2)),
            probe_port=lambda _host, _port: False,
            poll_interval=0.01,
        )
        with mock.patch("sys.stderr", io.StringIO()) as stderr:
            code = launcher.run_browser(
                service,
                mode="none",
                open_browser=lambda _url: self.fail("must not open"),
            )
        self.assertEqual(code, 1)
        self.assertIn("exited with code 2", stderr.getvalue())

    def test_child_crash_after_start_exits_non_zero(self):
        process = CrashingProcess(healthy_polls=3, exit_code=9)
        service = launcher.LocalService(
            port=_free_port(),
            paths=self.paths,
            opener=RecordingOpener(urllib.error.URLError("down"), FakeResponse()),
            popen=FakePopen(process),
            probe_port=lambda _host, _port: False,
            poll_interval=0.01,
        )
        with mock.patch("sys.stderr", io.StringIO()) as stderr:
            code = launcher.run_browser(service, mode="url", poll_interval=0.01)
        self.assertEqual(code, 1)
        self.assertIn("exit code 9", stderr.getvalue())


class FakeEvent:
    def __init__(self) -> None:
        self.handlers: list[Callable[[], object]] = []

    def __iadd__(self, handler):
        self.handlers.append(handler)
        return self

    def fire(self) -> None:
        for handler in self.handlers:
            handler()


class FakeWindow:
    def __init__(self) -> None:
        self.events = type("Events", (), {})()
        self.events.closed = FakeEvent()
        self.loaded: list[tuple[str, str]] = []
        self.destroyed = False

    def load_url(self, url: str) -> None:
        self.loaded.append(("url", url))

    def load_html(self, content: str) -> None:
        self.loaded.append(("html", content))

    def destroy(self) -> None:
        self.destroyed = True
        self.events.closed.fire()


class FakeWebview:
    """Stands in for pywebview; start() runs boot, then the user closes it."""

    def __init__(self, *, fail_start: Exception | None = None) -> None:
        self.window = FakeWindow()
        self.start_kwargs: dict = {}
        self.fail_start = fail_start

    def create_window(self, _title, **_kwargs):
        return self.window

    def start(self, func=None, icon=None):
        self.start_kwargs = {"icon": icon}
        if self.fail_start is not None:
            raise self.fail_start
        boot = threading.Thread(target=func)
        boot.start()
        for _ in range(500):
            if self.window.loaded:
                break
            threading.Event().wait(0.01)
        self.window.events.closed.fire()  # the user closes the window
        boot.join(timeout=5)


class WindowModeTests(TempHomeTestCase):
    def make_service(self, opener, process=None):
        return launcher.LocalService(
            port=_free_port(),
            paths=self.paths,
            opener=opener,
            popen=FakePopen(process or FakeProcess()),
            probe_port=lambda _host, _port: False,
            poll_interval=0.01,
        )

    def test_window_loads_the_ui_and_closing_stops_only_our_child(self):
        process = FakeProcess()
        service = self.make_service(
            RecordingOpener(urllib.error.URLError("down"), FakeResponse()), process
        )
        webview = FakeWebview()
        self.assertEqual(
            launcher.run_window(service, webview=webview, platform="linux"), 0
        )
        self.assertEqual(webview.window.loaded, [("url", service.url + "/")])
        self.assertEqual(webview.start_kwargs["icon"], str(ICON_PNG_256))
        self.assertEqual(process.calls[0], "terminate")
        self.assertFalse(service.owns_child)

    def test_closing_window_leaves_preexisting_service_running(self):
        popen_process = FakeProcess()
        service = self.make_service(RecordingOpener(FakeResponse()), popen_process)
        self.assertEqual(
            launcher.run_window(service, webview=FakeWebview(), platform="linux"), 0
        )
        self.assertTrue(service.reused)
        self.assertEqual(popen_process.calls, [])

    def test_startup_failure_is_shown_in_window_and_exits_non_zero(self):
        service = self.make_service(
            RecordingOpener(urllib.error.URLError("down")), FakeProcess(exit_code=4)
        )
        webview = FakeWebview()
        with mock.patch("sys.stderr", io.StringIO()):
            self.assertEqual(
                launcher.run_window(service, webview=webview, platform="linux"), 1
            )
        kind, content = webview.window.loaded[0]
        self.assertEqual(kind, "html")
        self.assertIn("exited with code 4", content)
        self.assertIn("Remote models were not touched", content)

    def test_gui_backend_failure_before_boot_allows_browser_fallback(self):
        popen = FakePopen()
        service = launcher.LocalService(
            port=_free_port(),
            paths=self.paths,
            opener=RecordingOpener(FakeResponse()),
            popen=popen,
        )
        with self.assertRaises(launcher.GuiUnavailable):
            launcher.run_window(
                service,
                webview=FakeWebview(fail_start=RuntimeError("no GTK")),
                platform="linux",
            )
        self.assertEqual(popen.calls, [])
        self.assertFalse(
            service.cancelled.is_set()
        )  # the browser fallback can still start


class SignalTests(unittest.TestCase):
    @unittest.skipIf(
        not hasattr(signal, "SIGTERM") or os.name != "posix", "POSIX signals"
    )
    def test_sigterm_reaches_callback_once_and_handlers_are_restored(self):
        before = signal.getsignal(signal.SIGTERM)
        calls: list[int] = []
        done = threading.Event()

        def callback():
            calls.append(1)
            done.set()

        signals = launcher.StopSignals(callback)
        self.assertTrue(signals.install())
        try:
            os.kill(os.getpid(), signal.SIGTERM)
            self.assertTrue(done.wait(5))
        finally:
            signals.restore()
        self.assertEqual(calls, [1])
        self.assertEqual(signals.received, signal.SIGTERM)
        self.assertIs(signal.getsignal(signal.SIGTERM), before)

    def test_install_outside_main_thread_is_a_no_op(self):
        result: list[bool] = []
        thread = threading.Thread(
            target=lambda: result.append(launcher.StopSignals(lambda: None).install())
        )
        thread.start()
        thread.join()
        self.assertEqual(result, [False])


class SafetyInvariantTests(unittest.TestCase):
    def test_launcher_source_never_calls_mutating_api_routes(self):
        source = LAUNCHER_SOURCE.read_text(encoding="utf-8")
        self.assertNotIn("/api/v1", source)
        self.assertNotIn("DELETE", source)
        self.assertNotIn('"POST"', source)

    def test_pages_escape_messages(self):
        page = launcher.error_page("<script>x</script>\n\ntail <b>")
        self.assertNotIn("<script>x", page)
        self.assertIn("&lt;script&gt;", page)
        self.assertIn("DGX Spark", launcher.loading_page())

    @unittest.skipUnless(sys.platform == "darwin", "macOS notification center")
    def test_macos_terminate_observer_calls_back(self):
        try:
            Foundation = importlib.import_module("Foundation")
        except ImportError:
            self.skipTest("PyObjC is not installed")
        calls: list[int] = []
        observer = launcher._macos_observer_class(Foundation).alloc().init()
        observer.callback = lambda: calls.append(1)
        center = Foundation.NSNotificationCenter.defaultCenter()
        name = "DGXControllerLauncherTestNotification"
        center.addObserver_selector_name_object_(
            observer, "applicationWillTerminate:", name, None
        )
        try:
            center.postNotificationName_object_(name, None)
        finally:
            center.removeObserver_(observer)
        self.assertEqual(calls, [1])


class IconAssetTests(unittest.TestCase):
    @staticmethod
    def png_size(data: bytes) -> tuple[int, int]:
        assert data[:8] == b"\x89PNG\r\n\x1a\n"
        width, height = struct.unpack(">II", data[16:24])
        return width, height

    def test_committed_icons_are_valid_pngs_of_expected_size(self):
        self.assertEqual(self.png_size(ICON_PNG.read_bytes()), (1024, 1024))
        self.assertEqual(self.png_size(ICON_PNG_256.read_bytes()), (256, 256))

    def test_generator_produces_decodable_rgba_png(self):
        data = render_png(32)
        self.assertEqual(self.png_size(data), (32, 32))
        idat = data[data.index(b"IDAT") + 4 : data.index(b"IEND") - 8]
        raw = zlib.decompress(idat)
        self.assertEqual(len(raw), 32 * (1 + 32 * 4))
        corner_alpha = raw[1 + 3]
        centre_row = raw[16 * (1 + 32 * 4) : 17 * (1 + 32 * 4)]
        self.assertEqual(corner_alpha, 0)  # transparent outside the tile
        self.assertEqual(centre_row[1 + 16 * 4 + 3], 255)  # opaque inside


if __name__ == "__main__":
    unittest.main()
