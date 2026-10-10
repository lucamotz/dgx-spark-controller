# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Bounded, allowlisted HTTPS JSON fetches for the read-only catalog.

Safety boundary: this is the only catalog module that opens a network
connection, and it only performs ``GET`` requests that return JSON.

* The URL must be ``https`` on a host in ``ALLOWED_HOSTS`` with no userinfo
  and no explicit port. The same check runs on every redirect target before
  it is followed (at most ``MAX_REDIRECTS`` hops) and on the final URL.
* Redirect decision: Hugging Face answers ``/{repo}/resolve/{sha}/config.json``
  with a *relative* 307 to ``/api/resolve-cache/...`` on ``huggingface.co``
  (verified 2026-10-10), and a model API request with non-canonical repo case
  307s to the canonical path on the same host. Those same-host redirects are
  followed. CDN hosts (``cdn-lfs*.hf.co``, ``*.hf.co``,
  ``cas-bridge.xethub.hf.co``) are deliberately not allowlisted: they serve
  LFS/Xet blobs such as weight shards, which the catalog never downloads. A
  config file stored in LFS therefore fails closed with ``host_not_allowed``
  instead of widening the allowlist.
* ``Authorization`` is forwarded on a redirect only when the target host equals
  the current host, so a Hugging Face token never reaches GitHub or Docker Hub.
* At most ``max_bytes`` of body are read; more is ``response_too_large``.
  ``Accept-Encoding: identity`` is sent so no compressed body is expanded.
* Header values (tokens) never appear in an error message or exception chain.

Status mapping: 401/403 -> ``upstream_unauthorized`` (callers may reinterpret
it as gated or private), 404 -> ``not_found``, 429 or a GitHub 403 with an
exhausted rate limit -> ``upstream_rate_limited``, any other failure ->
``upstream_unavailable``.
"""

from __future__ import annotations

import contextlib
import http.client
import json
import re
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict
from collections.abc import Callable, Hashable, Mapping
from http.client import HTTPMessage
from typing import Any, TypeGuard, TypeVar

USER_AGENT = "dgx-spark-controller/0.2"
MAX_JSON_DEPTH = 64
ALLOWED_HOSTS = frozenset({"huggingface.co", "api.github.com", "hub.docker.com"})
DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_MAX_BYTES = 8 * 1024 * 1024
MAX_REDIRECTS = 5
MAX_URL_LENGTH = 2048

_HEADER_NAME_RE = re.compile(r"^[A-Za-z0-9-]{1,64}$")
_UPSTREAM_CODE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
_MAX_HEADER_VALUE_LENGTH = 8192
_TOO_LARGE_REMEDIATION = (
    "The upstream document is unexpectedly large; inspect it manually."
)
_V = TypeVar("_V")


class CatalogError(Exception):
    """A safe, user-facing catalog failure with a stable machine code.

    Attributes:
        code: Stable machine-readable error code, e.g. ``model_gated``.
        message: Operator-facing description; never contains header values.
        remediation: Operator-facing next step, possibly empty.
        status: Upstream HTTP status when one was received.
        upstream_code: Sanitized upstream error token such as Hugging Face's
            ``X-Error-Code`` (``GatedRepo``, ``RepoNotFound``).

    """

    def __init__(
        self,
        code: str,
        message: str,
        remediation: str = "",
        *,
        status: int | None = None,
        upstream_code: str | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.remediation = remediation
        self.status = status
        self.upstream_code = upstream_code
        super().__init__(f"{message} ({code})")

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation for API responses."""
        return {
            "code": self.code,
            "message": self.message,
            "remediation": self.remediation,
            "status": self.status,
            "upstream_code": self.upstream_code,
        }


def _host_not_allowed(message: str) -> CatalogError:
    return CatalogError(
        "host_not_allowed",
        message,
        "The catalog only contacts huggingface.co, api.github.com and "
        "hub.docker.com over HTTPS.",
    )


def check_url(url: Any) -> urllib.parse.SplitResult:
    """Validate that ``url`` is an allowlisted HTTPS URL.

    Args:
        url: Candidate URL; any type is accepted and checked.

    Returns:
        The split URL.

    Raises:
        CatalogError: ``host_not_allowed`` for a malformed URL, a non-https
            scheme, credentials, an explicit port, or a host outside
            ``ALLOWED_HOSTS``.

    """
    if not isinstance(url, str) or not url or len(url) > MAX_URL_LENGTH:
        raise _host_not_allowed("Catalog URL is missing or too long.")
    if any(ord(char) <= 0x20 or ord(char) == 0x7F for char in url) or "\\" in url:
        raise _host_not_allowed(
            "Catalog URL contains whitespace, control characters or backslashes."
        )
    parts = urllib.parse.urlsplit(url)
    if parts.scheme != "https":
        raise _host_not_allowed("Catalog URLs must use https.")
    if "@" in parts.netloc or ":" in parts.netloc:
        raise _host_not_allowed(
            "Catalog URLs must not contain credentials or an explicit port."
        )
    host = parts.hostname
    if host is None or parts.netloc.lower() != host or host not in ALLOWED_HOSTS:
        raise _host_not_allowed("Catalog URL host is not allowlisted.")
    return parts


class AllowlistRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Follow redirects only to allowlisted HTTPS hosts; drop auth across hosts."""

    max_redirections = MAX_REDIRECTS
    max_repeats = 2

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> urllib.request.Request | None:
        """Return the follow-up request, refusing non-allowlisted targets.

        Raises:
            CatalogError: ``host_not_allowed`` before the target is contacted.

        """
        try:
            target = check_url(newurl)
        except CatalogError:
            fp.close()
            raise
        new_request = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new_request is not None:
            source_host = urllib.parse.urlsplit(req.full_url).hostname
            if target.hostname != source_host:
                new_request.remove_header("Authorization")
        return new_request


def build_opener(
    https_handler: urllib.request.BaseHandler | None = None,
) -> urllib.request.OpenerDirector:
    """Build an HTTPS-only opener with the allowlisting redirect handler.

    Args:
        https_handler: Replacement for the TLS socket layer, used by offline
            tests that need the real redirect and error processing.

    Returns:
        An opener without plain-HTTP, FTP, file or data handlers.

    """
    https = https_handler or urllib.request.HTTPSHandler(
        context=ssl.create_default_context()
    )
    opener = urllib.request.OpenerDirector()
    handlers: tuple[urllib.request.BaseHandler, ...] = (
        urllib.request.ProxyHandler(),
        https,
        urllib.request.HTTPDefaultErrorHandler(),
        AllowlistRedirectHandler(),
        urllib.request.HTTPErrorProcessor(),
    )
    for handler in handlers:
        opener.add_handler(handler)
    return opener


def _header(headers: Any, name: str) -> str | None:
    try:
        value = headers.get(name) if headers is not None else None
    except Exception:
        return None
    return value.strip() if isinstance(value, str) else None


def _http_error(exc: urllib.error.HTTPError, host: str) -> CatalogError:
    status = exc.code if isinstance(exc.code, int) else None
    headers = exc.headers
    raw_code = _header(headers, "X-Error-Code")
    upstream_code = (
        raw_code if raw_code and _UPSTREAM_CODE_RE.fullmatch(raw_code) else None
    )
    with contextlib.suppress(Exception):
        exc.close()
    rate_limited = status == 429 or (
        status == 403 and _header(headers, "X-RateLimit-Remaining") == "0"
    )
    if rate_limited:
        return CatalogError(
            "upstream_rate_limited",
            f"{host} rate-limited the catalog request.",
            "Wait a few minutes and retry, or configure an access token.",
            status=status,
            upstream_code=upstream_code,
        )
    if status in (401, 403):
        return CatalogError(
            "upstream_unauthorized",
            f"{host} refused the request without valid access (HTTP {status}).",
            "Check that the resource exists and that a permitted access token "
            "is configured.",
            status=status,
            upstream_code=upstream_code,
        )
    if status == 404:
        return CatalogError(
            "not_found",
            f"{host} reported that the resource does not exist (HTTP 404).",
            "Check the name, tag or revision.",
            status=status,
            upstream_code=upstream_code,
        )
    return CatalogError(
        "upstream_unavailable",
        f"{host} returned HTTP {status if status is not None else 'error'}.",
        "Retry later; the upstream service may be degraded.",
        status=status,
        upstream_code=upstream_code,
    )


def _unavailable(host: str, detail: str) -> CatalogError:
    return CatalogError(
        "upstream_unavailable",
        f"{host} {detail}",
        "Check this Mac's network connection and retry later.",
    )


def _too_large(host: str, max_bytes: int) -> CatalogError:
    return CatalogError(
        "response_too_large",
        f"{host} response exceeds the {max_bytes}-byte catalog limit.",
        _TOO_LARGE_REMEDIATION,
    )


def is_int(value: Any) -> TypeGuard[int]:
    """Whether ``value`` is an ``int`` and not a ``bool``, which subclasses it."""
    return isinstance(value, int) and not isinstance(value, bool)


def is_number(value: Any) -> TypeGuard[int | float]:
    """Whether ``value`` is an ``int`` or ``float`` and not a ``bool``."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def bearer_header(token: Any, *, service: str, env_name: str) -> dict[str, str]:
    """Build an ``Authorization`` header for a well-formed single-line token.

    Args:
        token: The access token; never echoed in errors.
        service: Human-readable service name for messages.
        env_name: Environment variable the operator should fix.

    Returns:
        A one-entry header mapping.

    Raises:
        CatalogError: ``invalid_token`` for a non-string, empty, overlong or
            whitespace/control/non-ASCII-containing token.

    """
    if (
        not isinstance(token, str)
        or not 1 <= len(token) <= 512
        or any(ord(char) <= 0x20 or ord(char) >= 0x7F for char in token)
    ):
        raise CatalogError(
            "invalid_token",
            f"The configured {service} token is malformed.",
            f"Set {env_name} to a single-line {service} access token.",
        )
    return {"Authorization": f"Bearer {token}"}


def _reject_constant(name: str) -> Any:
    raise ValueError(f"non-standard JSON constant {name}")


def _request(url: str, headers: Mapping[str, str] | None) -> urllib.request.Request:
    request = urllib.request.Request(url, method="GET")
    request.add_header("User-Agent", USER_AGENT)
    request.add_header("Accept", "application/json")
    request.add_header("Accept-Encoding", "identity")
    if headers is None:
        return request
    if not isinstance(headers, Mapping):
        raise CatalogError("invalid_request", "Request headers must be a mapping.")
    for name, value in headers.items():
        if not isinstance(name, str) or not _HEADER_NAME_RE.fullmatch(name):
            raise CatalogError("invalid_request", "A request header name is invalid.")
        if (
            not isinstance(value, str)
            or len(value) > _MAX_HEADER_VALUE_LENGTH
            or any(char in value for char in "\r\n\0")
        ):
            raise CatalogError(
                "invalid_request", f"Request header {name!r} has an invalid value."
            )
        request.add_header(name, value)
    return request


def _open(
    active: Any, request: urllib.request.Request, host: str, timeout: float
) -> Any:
    try:
        return active.open(request, timeout=timeout)
    except CatalogError:
        raise
    except urllib.error.HTTPError as exc:
        raise _http_error(exc, host) from None
    except TimeoutError:
        raise _unavailable(host, "did not respond in time.") from None
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, TimeoutError):
            raise _unavailable(host, "did not respond in time.") from None
        raise _unavailable(host, "could not be reached.") from None
    except (OSError, http.client.HTTPException, ValueError):
        raise _unavailable(host, "could not be reached.") from None


def _read_body(response: Any, url: str, host: str, max_bytes: int) -> Any:
    try:
        status = getattr(response, "status", None)
        if status != 200:
            raise _unavailable(host, f"returned unexpected HTTP status {status}.")
        check_url(response.geturl() if hasattr(response, "geturl") else url)
        declared = _header(getattr(response, "headers", None), "Content-Length")
        if declared and declared.isdigit() and int(declared) > max_bytes:
            raise _too_large(host, max_bytes)
        return response.read(max_bytes + 1)
    except CatalogError:
        raise
    except TimeoutError:
        raise _unavailable(host, "did not finish the response in time.") from None
    except (OSError, http.client.HTTPException, ValueError, AttributeError):
        raise _unavailable(host, "sent an incomplete response.") from None
    finally:
        with contextlib.suppress(Exception):
            response.close()


def get_json(
    url: str,
    *,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    max_bytes: int = DEFAULT_MAX_BYTES,
    headers: Mapping[str, str] | None = None,
    opener: Any = None,
) -> Any:
    """GET an allowlisted HTTPS URL and decode its JSON body.

    Args:
        url: Absolute ``https`` URL on a host in ``ALLOWED_HOSTS``.
        timeout: Socket timeout in seconds (0 < timeout <= 300).
        max_bytes: Largest accepted body; one extra byte is read to detect
            overflow.
        headers: Extra request headers, e.g. ``Authorization``. Values are
            never included in errors.
        opener: Object with ``open(request, timeout=...)``, injected by tests.
            Defaults to ``build_opener()``.

    Returns:
        The decoded JSON document (``NaN``/``Infinity`` are rejected).

    Raises:
        CatalogError: ``host_not_allowed``, ``invalid_request``,
            ``upstream_unauthorized``, ``not_found``,
            ``upstream_rate_limited``, ``response_too_large`` or
            ``upstream_unavailable``.
        ValueError: If ``timeout`` or ``max_bytes`` is out of range.

    """
    if not is_number(timeout) or not 0 < timeout <= 300:
        raise ValueError("timeout must be between 0 and 300 seconds")
    if not is_int(max_bytes) or max_bytes < 1:
        raise ValueError("max_bytes must be a positive integer")
    host = check_url(url).hostname or "upstream"
    request = _request(url, headers)
    active = opener if opener is not None else build_opener()
    response = _open(active, request, host, timeout)
    body = _read_body(response, url, host, max_bytes)
    if not isinstance(body, (bytes, bytearray)):
        raise _unavailable(host, "sent a non-binary response body.")
    if len(body) > max_bytes:
        raise _too_large(host, max_bytes)
    try:
        value = json.loads(bytes(body), parse_constant=_reject_constant)
    except (ValueError, RecursionError):
        raise _unavailable(host, "returned a response that is not JSON.") from None
    # Whether json raises RecursionError depends on the interpreter's C stack,
    # so bound nesting explicitly for identical behaviour on every platform.
    if _nesting_depth(value) > MAX_JSON_DEPTH:
        raise _unavailable(host, "returned JSON nested too deeply.")
    return value


def _nesting_depth(value: Any) -> int:
    deepest = 0
    stack: list[tuple[Any, int]] = [(value, 1)]
    while stack:
        item, depth = stack.pop()
        if isinstance(item, dict):
            children: Any = item.values()
        elif isinstance(item, list):
            children = item
        else:
            continue
        deepest = max(deepest, depth)
        if deepest > MAX_JSON_DEPTH:
            return deepest
        stack.extend((child, depth + 1) for child in children)
    return deepest


class TTLCache:
    """A small thread-safe time-to-live cache with a bounded entry count.

    Entries expire ``ttl_seconds`` after they were stored, measured with
    ``clock``. When full, the least recently used entry is evicted.
    ``get_or_compute`` runs the computation outside the lock, so concurrent
    misses may compute twice but a slow upstream never blocks other keys.
    Exceptions are never cached. Never use a secret such as a token in a key.
    """

    _MISSING: Any = object()

    def __init__(
        self,
        ttl_seconds: float,
        *,
        max_entries: int = 128,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create an empty cache.

        Args:
            ttl_seconds: Positive lifetime of each entry.
            max_entries: Positive upper bound on stored entries.
            clock: Monotonic time source, injectable for tests.

        Raises:
            ValueError: If ``ttl_seconds`` or ``max_entries`` is not positive.

        """
        if not is_number(ttl_seconds) or ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        if not is_int(max_entries) or max_entries < 1:
            raise ValueError("max_entries must be a positive integer")
        self.ttl_seconds = float(ttl_seconds)
        self.max_entries = max_entries
        self._clock = clock
        self._lock = threading.Lock()
        self._entries: OrderedDict[Hashable, tuple[float, Any]] = OrderedDict()

    def _lookup(self, key: Hashable, now: float) -> Any:
        entry = self._entries.get(key)
        if entry is None:
            return self._MISSING
        expires_at, value = entry
        if now >= expires_at:
            del self._entries[key]
            return self._MISSING
        self._entries.move_to_end(key)
        return value

    def get(self, key: Hashable, default: Any = None) -> Any:
        """Return the live value for ``key``, or ``default``."""
        with self._lock:
            value = self._lookup(key, self._clock())
        return default if value is self._MISSING else value

    def set(self, key: Hashable, value: Any) -> None:
        """Store ``value`` for ``key``, evicting expired and excess entries."""
        with self._lock:
            now = self._clock()
            self._entries.pop(key, None)
            expired = [
                item
                for item, (expires_at, _) in self._entries.items()
                if now >= expires_at
            ]
            for item in expired:
                del self._entries[item]
            self._entries[key] = (now + self.ttl_seconds, value)
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)

    def get_or_compute(self, key: Hashable, compute: Callable[[], _V]) -> _V:
        """Return the cached value for ``key`` or compute and store it.

        Args:
            key: Hashable cache key.
            compute: Zero-argument function called on a miss; its exceptions
                propagate and nothing is stored.

        Returns:
            The cached or newly computed value.

        """
        with self._lock:
            cached = self._lookup(key, self._clock())
        if cached is not self._MISSING:
            return cached
        value = compute()
        self.set(key, value)
        return value

    def clear(self) -> None:
        """Remove every entry."""
        with self._lock:
            self._entries.clear()

    def __len__(self) -> int:
        """Return the number of unexpired entries."""
        with self._lock:
            now = self._clock()
            return sum(1 for expires, _ in self._entries.values() if now < expires)
