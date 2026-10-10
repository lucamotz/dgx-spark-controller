# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Cross-platform desktop launcher for the local DGX Spark Controller.

``dgx-spark-controller-app`` (or ``python -m controller.launcher``) makes the
loopback controller available and shows its web UI, preferably in a native
pywebview window (Cocoa/WebKit on macOS, GTK or Qt on Linux) and otherwise in
the system browser.

Ownership rules (a core project invariant):

* The launcher owns at most one thing: the local controller child process it
  started itself. Closing the window, Cmd-Q, Ctrl-C, SIGTERM or SIGHUP stop
  only that child (terminate, wait up to 10 s, then kill).
* App exit never unloads a remote model. The launcher sends no request to the
  controller other than the read-only ``GET /api/health`` probe, so it cannot
  stop, unload or switch a deployment. Remote inference keeps serving until an
  operator explicitly unloads it from the UI.
* If a healthy controller already listens on the port (for example the systemd
  user service or another window), the launcher only opens the UI and leaves
  that service running when the window closes.
* The child runs with ``DGX_CONTROLLER_EXIT_WITH_PARENT=1``, so even a hard
  kill of the launcher ends the local service through its parent watchdog.

GUI code is confined to :func:`run_window`; everything else is pure or takes
injectable collaborators so it can be tested without a display.
"""

from __future__ import annotations

import argparse
import contextlib
import html
import http.client
import importlib
import importlib.util
import inspect
import json
import os
import shlex
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import webbrowser
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, Protocol

from controller.assets import ICON_PNG, ICON_PNG_256
from controller.paths import AppPaths, resolve_app_paths

APP_TITLE = "DGX Spark Controller"
LOOPBACK_HOST = "127.0.0.1"
DEFAULT_PORT = 18876
HEALTH_PATH = "/api/health"
STARTUP_TIMEOUT_SECONDS = 30.0
STOP_GRACE_SECONDS = 10.0
KILL_GRACE_SECONDS = 5.0
WINDOW_SIZE = (1360, 900)
WINDOW_MIN_SIZE = (980, 640)
WINDOW_BACKGROUND = "#F5F7F8"
PACKAGE_PARENT = Path(__file__).resolve().parents[1]
"""Directory containing the ``controller`` package; the child's working dir."""

Opener = Callable[[str, float], Any]


class ChildProcess(Protocol):
    """The subset of :class:`subprocess.Popen` the launcher relies on."""

    @property
    def returncode(self) -> int | None: ...

    def poll(self) -> int | None: ...

    def wait(self, timeout: float | None = None) -> int: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...


class LaunchError(RuntimeError):
    """The local service could not be made available (user-facing message)."""


class GuiUnavailable(RuntimeError):
    """The native window backend failed before the service was started."""


def service_url(port: int) -> str:
    """Return the loopback base URL for ``port``."""
    return f"http://{LOOPBACK_HOST}:{port}"


def build_service_command(python: str, port: int) -> list[str]:
    """Build the exact child command line.

    Args:
        python: Interpreter that runs ``-m controller``.
        port: Loopback TCP port, the only variable part of the command.

    Returns:
        The argv list for :class:`subprocess.Popen`.

    Raises:
        ValueError: If ``port`` is outside 1-65535.

    """
    if not 0 < int(port) < 65536:
        raise ValueError("port must be between 1 and 65535")
    return [
        python,
        "-m",
        "controller",
        "--host",
        LOOPBACK_HOST,
        "--port",
        str(int(port)),
    ]


def build_child_environment(base: Mapping[str, str]) -> dict[str, str]:
    """Copy ``base`` and opt the child into the parent-exit watchdog."""
    environment = dict(base)
    environment["DGX_CONTROLLER_EXIT_WITH_PARENT"] = "1"
    environment["PYTHONUNBUFFERED"] = "1"
    return environment


# Loopback probes must never go through an HTTP proxy from the environment.
_DIRECT_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _direct_open(url: str, timeout: float) -> Any:
    return _DIRECT_OPENER.open(url, timeout=timeout)


def check_health(
    base_url: str, *, opener: Opener | None = None, timeout: float = 1.5
) -> bool:
    """Probe ``GET /api/health``.

    Args:
        base_url: Controller base URL.
        opener: Callable ``(url, timeout)`` returning a response context
            manager; defaults to a proxy-free urllib opener.
        timeout: Socket timeout in seconds.

    Returns:
        True only for HTTP 200 with a JSON object whose ``status`` is ``ok``.

    """
    open_url = opener or _direct_open
    try:
        with open_url(base_url.rstrip("/") + HEALTH_PATH, timeout) as response:
            status = getattr(response, "status", None)
            if status is None:
                status = response.getcode()
            if status != 200:
                return False
            payload = json.loads(response.read(65536).decode("utf-8"))
    except (OSError, ValueError, http.client.HTTPException):
        return False
    return isinstance(payload, dict) and payload.get("status") == "ok"


def port_accepts_connections(host: str, port: int, *, timeout: float = 0.5) -> bool:
    """Return whether anything accepts TCP connections on ``host:port``."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@dataclass(frozen=True)
class UiChoice:
    """How to present the UI.

    Attributes:
        mode: ``window``, ``browser``, ``url`` (print the URL only) or ``none``.
        reason: Why a native window is not used, if that was not requested.

    """

    mode: str
    reason: str = ""


def _module_available(name: str, find_spec: Callable[[str], Any]) -> bool:
    try:
        return find_spec(name) is not None
    except (ImportError, ValueError):
        return False


def choose_ui(
    *,
    force_browser: bool = False,
    no_open: bool = False,
    platform: str | None = None,
    env: Mapping[str, str] | None = None,
    find_spec: Callable[[str], Any] = importlib.util.find_spec,
) -> UiChoice:
    """Pick the UI backend without importing or starting a GUI toolkit.

    Args:
        force_browser: The user asked for the system browser.
        no_open: The user asked for no UI at all.
        platform: ``sys.platform`` override.
        env: Environment override.
        find_spec: Module lookup override.

    Returns:
        The chosen :class:`UiChoice`.

    """
    system = sys.platform if platform is None else platform
    environment = os.environ if env is None else env
    if no_open:
        return UiChoice("none")
    if force_browser:
        return UiChoice("browser")
    unix_desktop = system.startswith("linux") or "bsd" in system
    has_display = any(
        environment.get(name, "").strip() for name in ("DISPLAY", "WAYLAND_DISPLAY")
    )
    if unix_desktop and not has_display:
        # webbrowser could pick a console browser that takes over the terminal.
        return UiChoice(
            "url", "no graphical display (DISPLAY and WAYLAND_DISPLAY are unset)"
        )
    if not _module_available("webview", find_spec):
        return UiChoice(
            "browser",
            "pywebview is not installed (install the 'desktop' extra)",
        )
    if unix_desktop and not (
        _module_available("gi", find_spec) or _module_available("qtpy", find_spec)
    ):
        return UiChoice(
            "browser",
            "pywebview needs GTK (PyGObject) or Qt bindings; install the "
            "'desktop-gtk' or 'desktop-qt' extra",
        )
    return UiChoice("window")


def wait_for_health(
    base_url: str,
    process: ChildProcess,
    *,
    timeout: float = STARTUP_TIMEOUT_SECONDS,
    opener: Opener | None = None,
    cancel: threading.Event | None = None,
    poll_interval: float = 0.25,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Any] | None = None,
) -> bool:
    """Poll the health route until the child is ready.

    Args:
        base_url: Controller base URL.
        process: The child that should become healthy.
        timeout: Seconds before giving up.
        opener: Health-probe opener override.
        cancel: Event that aborts the wait when set.
        poll_interval: Seconds between probes.
        clock: Monotonic clock override.
        sleep: Sleep override; defaults to waiting on ``cancel``.

    Returns:
        True when healthy, False when ``cancel`` was set.

    Raises:
        LaunchError: If the child exits first or ``timeout`` elapses.

    """
    deadline = clock() + timeout
    if sleep is None:
        sleep = cancel.wait if cancel is not None else time.sleep
    while True:
        if cancel is not None and cancel.is_set():
            return False
        if check_health(base_url, opener=opener):
            return True
        exit_code = process.poll()
        if exit_code is not None:
            raise LaunchError(
                f"The local controller exited with code {exit_code} "
                f"before {HEALTH_PATH} became ready."
            )
        if clock() >= deadline:
            raise LaunchError(
                "The local controller did not become healthy "
                f"within {timeout:g} seconds."
            )
        sleep(poll_interval)


def stop_child(
    process: ChildProcess | None,
    *,
    grace_seconds: float = STOP_GRACE_SECONDS,
    kill_seconds: float = KILL_GRACE_SECONDS,
) -> int | None:
    """Terminate one local child, escalating to kill after a grace period.

    Args:
        process: The child to stop; None is a no-op.
        grace_seconds: Wait after SIGTERM before killing.
        kill_seconds: Wait after SIGKILL.

    Returns:
        The exit code, or None if the child could not be reaped.

    """
    if process is None:
        return None
    if process.poll() is not None:
        return process.returncode
    with contextlib.suppress(OSError):
        process.terminate()
    try:
        return process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        pass
    with contextlib.suppress(OSError):
        process.kill()
    try:
        return process.wait(timeout=kill_seconds)
    except subprocess.TimeoutExpired:
        return None


def read_log_tail(path: Path, lines: int = 15) -> str:
    """Return the last ``lines`` lines of ``path``, or "" if unreadable."""
    try:
        with path.open("r", encoding="utf-8", errors="replace") as stream:
            return "".join(deque(stream, maxlen=lines)).rstrip()
    except OSError:
        return ""


def _open_private_log(path: Path) -> IO[str]:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    return os.fdopen(descriptor, "a", buffering=1, encoding="utf-8")


class LocalService:
    """Provide a healthy loopback controller, owning only a child it started.

    A service object is single-use: once :meth:`stop` ran it stays cancelled.
    """

    def __init__(
        self,
        *,
        port: int = DEFAULT_PORT,
        python: str | None = None,
        paths: AppPaths | None = None,
        cwd: Path = PACKAGE_PARENT,
        environ: Mapping[str, str] | None = None,
        opener: Opener | None = None,
        popen: Callable[..., ChildProcess] = subprocess.Popen,
        probe_port: Callable[[str, int], bool] = port_accepts_connections,
        startup_timeout: float = STARTUP_TIMEOUT_SECONDS,
        grace_seconds: float = STOP_GRACE_SECONDS,
        kill_seconds: float = KILL_GRACE_SECONDS,
        poll_interval: float = 0.25,
    ) -> None:
        """Configure the service.

        Args:
            port: Loopback port.
            python: Interpreter for the child; defaults to ``sys.executable``.
            paths: Local storage layout; resolved lazily when omitted.
            cwd: Child working directory.
            environ: Base environment for the child.
            opener: Health-probe opener override.
            popen: Process factory override.
            probe_port: TCP probe override.
            startup_timeout: Seconds to wait for health.
            grace_seconds: SIGTERM grace period on stop.
            kill_seconds: Wait after SIGKILL on stop.
            poll_interval: Seconds between health probes.

        """
        self.port = int(port)
        self.url = service_url(self.port)
        self.command = build_service_command(python or sys.executable, self.port)
        self.cancelled = threading.Event()
        self.reused = False
        self._paths = paths
        self._cwd = Path(cwd)
        self._environ = os.environ if environ is None else environ
        self._opener = opener
        self._popen = popen
        self._probe_port = probe_port
        self._startup_timeout = startup_timeout
        self._grace_seconds = grace_seconds
        self._kill_seconds = kill_seconds
        self._poll_interval = poll_interval
        self._state_lock = threading.Lock()
        self._stop_lock = threading.Lock()
        self._process: ChildProcess | None = None
        self._log_stream: IO[str] | None = None

    @property
    def paths(self) -> AppPaths:
        """The local storage layout used for the log file."""
        if self._paths is None:
            self._paths = resolve_app_paths()
        return self._paths

    @property
    def log_path(self) -> Path:
        """Log file that receives the child's stdout and stderr."""
        return self.paths.app_log_file

    @property
    def owns_child(self) -> bool:
        """Whether a child started by this object is being supervised."""
        return self._process is not None

    def child_exit_code(self) -> int | None:
        """Return the owned child's exit code, or None while running/not owned."""
        process = self._process
        return None if process is None else process.poll()

    def cancel(self) -> None:
        """Abort a pending :meth:`start` without blocking; safe from any thread."""
        self.cancelled.set()

    def log_hint(self) -> str:
        """Describe where to look after a failure, with the latest log lines."""
        tail = read_log_tail(self.log_path)
        hint = f"See the log at {self.log_path}."
        return f"{hint}\n\nLast log lines:\n{tail}" if tail else hint

    def start(self) -> bool:
        """Make a healthy controller reachable at :attr:`url`.

        Returns:
            True when a controller is healthy (ours or a pre-existing one),
            False when cancelled.

        Raises:
            LaunchError: If the service cannot be started.

        """
        if self.cancelled.is_set():
            return False
        if check_health(self.url, opener=self._opener):
            self.reused = True
            return True
        if self._probe_port(LOOPBACK_HOST, self.port):
            raise LaunchError(
                f"Port {self.port} on {LOOPBACK_HOST} is used by a program that "
                "is not a healthy DGX Spark Controller. Stop that program or "
                "choose another port with --port."
            )
        process = self._spawn()
        if process is None:
            return False
        try:
            ready = wait_for_health(
                self.url,
                process,
                timeout=self._startup_timeout,
                opener=self._opener,
                cancel=self.cancelled,
                poll_interval=self._poll_interval,
            )
        except LaunchError as error:
            cancelled = self.cancelled.is_set()
            self.stop()
            if cancelled:
                return False
            raise LaunchError(f"{error} {self.log_hint()}") from None
        if not ready:
            self.stop()
            return False
        if process.poll() is not None:
            # Another launcher bound the port first: use it, never stop it.
            self._release_child()
            self.reused = True
        return True

    def _spawn(self) -> ChildProcess | None:
        with self._state_lock:
            if self.cancelled.is_set():
                return None
            try:
                log = _open_private_log(self.paths.ensure().app_log_file)
            except OSError as error:
                raise LaunchError(
                    f"Could not prepare the local log directory: {error}"
                ) from error
            stamp = time.strftime("%Y-%m-%dT%H:%M:%S%z")
            log.write(
                f"\n--- {stamp} launcher pid {os.getpid()} starting: "
                f"{shlex.join(self.command)}\n"
            )
            try:
                process = self._popen(
                    self.command,
                    cwd=str(self._cwd),
                    env=build_child_environment(self._environ),
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    close_fds=True,
                )
            except OSError as error:
                log.close()
                raise LaunchError(
                    f"Could not start the local controller: {error}. {self.log_hint()}"
                ) from error
            self._process = process
            self._log_stream = log
            return process

    def _release_child(self) -> None:
        with self._state_lock:
            self._process = None
            log, self._log_stream = self._log_stream, None
        if log is not None:
            log.close()

    def stop(self) -> int | None:
        """Stop only the child this object started; idempotent and thread-safe.

        Returns:
            The child's exit code, or None if there was no child.

        """
        self.cancelled.set()
        with self._stop_lock:
            with self._state_lock:
                process, self._process = self._process, None
                log, self._log_stream = self._log_stream, None
            exit_code = stop_child(
                process,
                grace_seconds=self._grace_seconds,
                kill_seconds=self._kill_seconds,
            )
            if log is not None:
                try:
                    log.write(
                        "--- launcher stopped the local controller "
                        f"(exit code {exit_code})\n"
                    )
                    log.close()
                except (OSError, ValueError):
                    pass
            return exit_code


class StopSignals:
    """Route SIGINT, SIGTERM and SIGHUP to a callback once.

    A Python signal handler only runs when the main thread executes bytecode,
    which a Cocoa or Qt event loop may not do for a long time. A watcher
    thread on :func:`signal.set_wakeup_fd` therefore delivers the request too,
    so the callback must be idempotent and thread-safe.
    """

    NAMES = ("SIGINT", "SIGTERM", "SIGHUP")

    def __init__(self, callback: Callable[[], object]) -> None:
        """Create an uninstalled router for ``callback``."""
        self.received: int | None = None
        self._callback = callback
        self._numbers = {
            int(getattr(signal, name)) for name in self.NAMES if hasattr(signal, name)
        }
        self._previous: dict[int, Any] = {}
        self._previous_wakeup = -1
        self._sockets: tuple[socket.socket, socket.socket] | None = None
        self._watcher: threading.Thread | None = None
        self._lock = threading.Lock()

    def install(self) -> bool:
        """Install handlers; returns False outside the main thread."""
        if threading.current_thread() is not threading.main_thread():
            return False
        for number in self._numbers:
            self._previous[number] = signal.signal(number, self._handle)
        try:
            reader, writer = socket.socketpair()
        except OSError:
            return True
        try:
            writer.setblocking(False)
            self._previous_wakeup = signal.set_wakeup_fd(
                writer.fileno(), warn_on_full_buffer=False
            )
        except (OSError, ValueError):
            reader.close()
            writer.close()
            return True
        self._sockets = (reader, writer)
        self._watcher = threading.Thread(
            target=self._watch, args=(reader,), name="dgx-launcher-signals", daemon=True
        )
        self._watcher.start()
        return True

    def restore(self) -> None:
        """Restore the previous handlers and wakeup fd."""
        if threading.current_thread() is not threading.main_thread():
            return
        for number, handler in self._previous.items():
            signal.signal(number, handler)
        self._previous.clear()
        if self._sockets is None:
            return
        reader, writer = self._sockets
        with contextlib.suppress(OSError, ValueError):
            signal.set_wakeup_fd(self._previous_wakeup)
        with contextlib.suppress(OSError):
            writer.shutdown(socket.SHUT_RDWR)
        if self._watcher is not None:
            self._watcher.join(timeout=1.0)
        reader.close()
        writer.close()
        self._sockets = None
        self._watcher = None

    def _handle(self, signum: int, _frame: object) -> None:
        self._fire(signum)

    def _watch(self, reader: socket.socket) -> None:
        while True:
            try:
                data = reader.recv(64)
            except OSError:
                return
            if not data:
                return
            for number in data:
                if number in self._numbers:
                    self._fire(number)

    def _fire(self, signum: int) -> None:
        with self._lock:
            if self.received is not None:
                return
            self.received = signum
        self._callback()


_PAGE_STYLE = """
html,body{height:100%;margin:0}
body{display:flex;align-items:center;justify-content:center;background:#f5f7f8;
color:#50616b;font:15px -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,
Arial,sans-serif}
.wrap{max-width:640px;padding:24px;text-align:center}
.brand{display:inline-flex;align-items:center;gap:10px;color:#29483f;
font-size:28px;font-weight:700;letter-spacing:-.03em}
.mark{display:grid;width:36px;height:36px;place-items:center;border-radius:50%;
border:1px solid rgba(61,100,86,.23);background:#e2eee7;color:#3d6456;
font-size:17px}
.accent{color:#53717c}.status{margin-top:14px}
.dot{display:inline-block;width:7px;height:7px;border-radius:50%;margin:0 2px;
background:#3d6456;animation:p 1s infinite}
.dot:nth-child(2){animation-delay:.15s}.dot:nth-child(3){animation-delay:.3s}
@keyframes p{0%,100%{opacity:.3}50%{opacity:1}}
@media (prefers-reduced-motion:reduce){.dot{animation:none}}
.error{margin-top:16px;color:#b4473a;font-weight:600}
pre{margin:14px 0 0;padding:12px;max-height:45vh;overflow:auto;text-align:left;
border:1px solid #dfe6e9;border-radius:8px;background:#fff;color:#19282f;
white-space:pre-wrap;font:12px ui-monospace,SFMono-Regular,Menlo,monospace}
.note{margin-top:12px;color:#657782;font-size:13px}
"""

_BRAND = (
    '<div class="brand"><span class="mark" aria-hidden="true">&#9638;</span>'
    '<span>DGX Spark <span class="accent">Controller</span></span></div>'
)


def _page(body: str) -> str:
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        f"<title>{APP_TITLE}</title><style>{_PAGE_STYLE}</style></head>"
        f'<body><div class="wrap">{_BRAND}{body}</div></body></html>'
    )


def loading_page() -> str:
    """Return the HTML shown while the local service starts."""
    dots = '<span class="dot"></span>' * 3
    return _page(
        f'<div class="status" role="status">Starting the local controller {dots}</div>'
    )


def error_page(message: str) -> str:
    """Return escaped HTML for a launch failure.

    Args:
        message: Summary, optionally followed by a blank line and details.

    """
    summary, _, detail = message.partition("\n\n")
    body = f'<div class="error" role="alert">{html.escape(summary)}</div>'
    if detail:
        body += f"<pre>{html.escape(detail)}</pre>"
    body += (
        '<div class="note">Close this window, fix the problem, and reopen the '
        "app. Remote models were not touched.</div>"
    )
    return _page(body)


def _print(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def run_browser(
    service: LocalService,
    *,
    mode: str = "browser",
    open_browser: Callable[[str], bool] = webbrowser.open,
    poll_interval: float = 0.5,
) -> int:
    """Run the service in the foreground and show it in the system browser.

    Args:
        service: The local service to start and supervise.
        mode: ``browser`` opens the URL, ``url`` prints it, ``none`` does
            neither.
        open_browser: Browser opener override.
        poll_interval: Seconds between child liveness checks.

    Returns:
        The process exit code.

    """
    stop_requested = threading.Event()

    def request_stop() -> None:
        service.cancel()
        stop_requested.set()

    signals = StopSignals(request_stop)
    signals.install()
    try:
        try:
            ready = service.start()
        except LaunchError as error:
            _print(f"error: {error}")
            return 1
        if not ready:
            return 0
        url = service.url + "/"
        if mode == "browser" and not open_browser(url):
            _print(f"Could not open a browser automatically; open {url}")
        elif mode == "url":
            _print(f"Open {url} in a browser on this machine or via SSH forwarding.")
        if not service.owns_child:
            _print(f"Using the DGX Spark Controller already running at {url}.")
            return 0
        _print(
            f"DGX Spark Controller is running at {url} (log: {service.log_path}).\n"
            "Press Ctrl-C to stop the local service. Remote models keep running."
        )
        while not stop_requested.wait(poll_interval):
            exit_code = service.child_exit_code()
            if exit_code is not None:
                _print(
                    "error: the local controller stopped unexpectedly "
                    f"(exit code {exit_code}). {service.log_hint()}"
                )
                return 1
        return 0
    finally:
        signals.restore()
        service.stop()


_MACOS_OBSERVER_CLASS: list[Any] = []
_MACOS_OBSERVERS: list[Any] = []


def _macos_observer_class(foundation: Any) -> Any:
    """Return (once) an NSObject subclass that forwards app termination."""
    if not _MACOS_OBSERVER_CLASS:

        def will_terminate(self: Any, _notification: object) -> None:
            callback = getattr(self, "callback", None)
            if callback is not None:
                callback()

        observer_class = type(
            "DGXControllerTerminateObserver",
            (foundation.NSObject,),
            {"applicationWillTerminate_": will_terminate},
        )
        _MACOS_OBSERVER_CLASS.append(observer_class)
    return _MACOS_OBSERVER_CLASS[0]


def _prepare_macos_app(on_terminate: Callable[[], object]) -> None:
    """Name the app in the menu bar and stop the child on Cmd-Q.

    ``[NSApp terminate:]`` exits without unwinding Python, so the child is
    stopped from an ``NSApplicationWillTerminateNotification`` observer. This
    is best-effort; the child's parent watchdog remains the fallback.
    """
    try:
        appkit = importlib.import_module("AppKit")
        foundation = importlib.import_module("Foundation")
    except ImportError:
        return
    try:
        foundation.NSProcessInfo.processInfo().setProcessName_(APP_TITLE)
        info = foundation.NSBundle.mainBundle().infoDictionary()
        if info is not None:
            info["CFBundleName"] = APP_TITLE
        observer = _macos_observer_class(foundation).alloc().init()
        observer.callback = on_terminate
        center = foundation.NSNotificationCenter.defaultCenter()
        center.addObserver_selector_name_object_(
            observer,
            "applicationWillTerminate:",
            appkit.NSApplicationWillTerminateNotification,
            None,
        )
        _MACOS_OBSERVERS.append(observer)
    except Exception:
        pass


def run_window(
    service: LocalService, *, webview: Any, platform: str | None = None
) -> int:
    """Show the UI in a pywebview window.

    Args:
        service: The local service to start and supervise.
        webview: The imported ``webview`` module (or a test double).
        platform: ``sys.platform`` override.

    Returns:
        The process exit code.

    Raises:
        GuiUnavailable: If the GUI failed before any child was started, so
            the caller can fall back to the system browser.

    """
    system = sys.platform if platform is None else platform
    state: dict[str, Any] = {"booted": False, "error": None}
    try:
        window = webview.create_window(
            APP_TITLE,
            html=loading_page(),
            width=WINDOW_SIZE[0],
            height=WINDOW_SIZE[1],
            min_size=WINDOW_MIN_SIZE,
            background_color=WINDOW_BACKGROUND,
            text_select=True,
            zoomable=True,
        )
    except Exception as error:
        raise GuiUnavailable(str(error) or type(error).__name__) from error

    def request_stop() -> None:
        service.cancel()
        with contextlib.suppress(Exception):
            window.destroy()

    def on_closed() -> None:
        # Never block the GUI thread; the main thread stops the child.
        service.cancel()

    window.events.closed += on_closed

    def show_error(message: str) -> None:
        state["error"] = message
        _print(f"error: {message}")
        with contextlib.suppress(Exception):
            window.load_html(error_page(message))

    def boot() -> None:
        state["booted"] = True
        try:
            ready = service.start()
        except LaunchError as error:
            show_error(str(error))
            return
        if not ready:
            request_stop()
            return
        try:
            window.load_url(service.url + "/")
        except Exception:  # closed while the service was starting
            return
        while not service.cancelled.wait(1.0):
            exit_code = service.child_exit_code()
            if exit_code is not None:
                show_error(
                    "The local controller stopped unexpectedly "
                    f"(exit code {exit_code}). {service.log_hint()}"
                )
                return

    start_options: dict[str, Any] = {}
    parameters: Mapping[str, Any]
    try:
        parameters = inspect.signature(webview.start).parameters
    except (TypeError, ValueError):
        parameters = {}
    icon = ICON_PNG if system == "darwin" else ICON_PNG_256
    if "icon" in parameters and icon.is_file():
        start_options["icon"] = str(icon)
    if system == "darwin":
        _prepare_macos_app(service.stop)

    signals = StopSignals(request_stop)
    signals.install()
    gui_error: Exception | None = None
    try:
        webview.start(boot, **start_options)
    except Exception as error:
        if state["booted"]:
            state["error"] = state["error"] or f"The native window failed: {error}"
            _print(f"error: {state['error']}")
        else:
            gui_error = error
    finally:
        signals.restore()
        if state["booted"]:
            service.stop()
    if gui_error is not None:
        message = str(gui_error) or type(gui_error).__name__
        raise GuiUnavailable(message) from gui_error
    return 1 if state["error"] else 0


def _port(value: str) -> int:
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("port must be an integer") from None
    if not 0 < number < 65536:
        raise argparse.ArgumentTypeError("port must be between 1 and 65535")
    return number


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse launcher command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="dgx-spark-controller-app",
        description=(
            "Start the local DGX Spark Controller service and open its UI in a "
            "native window (or the system browser). Closing it stops only the "
            "local service; it never unloads a remote model."
        ),
    )
    parser.add_argument(
        "--port",
        type=_port,
        default=DEFAULT_PORT,
        help=f"loopback port (default: {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--browser",
        action="store_true",
        help="use the system browser instead of a native window",
    )
    parser.add_argument(
        "--no-open",
        action="store_true",
        help="run the local service without opening any UI",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="interpreter for the local service child (default: this one)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for ``dgx-spark-controller-app``.

    Args:
        argv: Arguments without the program name; defaults to ``sys.argv``.

    Returns:
        The process exit code.

    """
    args = parse_args(argv)
    service = LocalService(port=args.port, python=args.python)
    choice = choose_ui(force_browser=args.browser, no_open=args.no_open)
    if choice.mode == "window":
        try:
            webview = importlib.import_module("webview")
        except Exception as error:
            choice = UiChoice("browser", f"pywebview failed to import ({error})")
        else:
            try:
                return run_window(service, webview=webview)
            except GuiUnavailable as error:
                choice = UiChoice("browser", f"the window failed to start ({error})")
    if choice.reason:
        suffix = "; using the system browser" if choice.mode == "browser" else ""
        _print(f"Native window unavailable: {choice.reason}{suffix}.")
    return run_browser(service, mode=choice.mode)


if __name__ == "__main__":
    raise SystemExit(main())
