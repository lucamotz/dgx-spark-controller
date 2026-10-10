# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Local-only stdlib HTTP server for the browser controller shell."""

from __future__ import annotations

import ipaddress
import json
import socket
from collections.abc import Mapping
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from controller.app import VERSION, ApplicationError, ControllerApplication
from controller.catalog.http import CatalogError
from controller.discovery.network import DiscoveryError
from controller.host import ServiceHost
from controller.recipes.spec import RecipeError

MAX_JSON_BYTES = 64 * 1024
STATIC_FILES = {
    "/": "index.html",
    "/index.html": "index.html",
    "/styles.css": "styles.css",
    "/app.js": "app.js",
    "/ui-core.js": "ui-core.js",
    "/ui-dashboard.js": "ui-dashboard.js",
    "/ui-models.js": "ui-models.js",
    "/ui-operations.js": "ui-operations.js",
    "/ui-setup.js": "ui-setup.js",
    "/ui-recipes.js": "ui-recipes.js",
}
NOT_FOUND_CODES = frozenset(
    {
        "recipe_not_found",
        "scan_not_found",
        "not_found",
        "model_not_found",
        "operation_not_found",
    }
)
CONFLICT_CODES = frozenset(
    {
        "idempotency_conflict",
        "recipe_busy",
        "scan_in_progress",
        "setup_required",
        "host_key_mismatch",
        "node_not_identified",
    }
)
CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
}
CONTENT_SECURITY_POLICY = (
    "default-src 'self'; base-uri 'none'; object-src 'none'; frame-ancestors 'none'; "
    "script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:"
)


def is_local_bind_host(host: str) -> bool:
    """Return whether ``host`` is an explicit loopback-only server bind."""
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class ControllerHTTPServer(ThreadingHTTPServer):
    """Threaded local server carrying one application service instance."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self, address: tuple[str, int], host: ServiceHost, web_root: Path
    ) -> None:
        bind_host, _port = address
        if not is_local_bind_host(bind_host):
            raise ValueError("controller server may bind only to a loopback address")
        if ":" in bind_host:
            self.address_family = socket.AF_INET6
        self.host = host
        self.web_root = web_root
        super().__init__(address, ControllerRequestHandler)


class ControllerRequestHandler(BaseHTTPRequestHandler):
    """Small, explicit route dispatcher; no filesystem path is request-controlled."""

    server: ControllerHTTPServer
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler callback
        self._dispatch(include_body=True)

    def do_HEAD(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler callback
        self._dispatch(include_body=False)

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler callback
        self._dispatch(include_body=True)

    def do_DELETE(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler callback
        self._dispatch(include_body=True)

    def do_OPTIONS(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler callback
        self._error(
            ApplicationError(
                "method_not_allowed",
                "This local API does not support cross-origin requests.",
                "Use the controller UI from the same local origin.",
                status=405,
            )
        )

    def _dispatch(self, *, include_body: bool) -> None:
        try:
            split = urlsplit(self.path)
            path = split.path
            if path in STATIC_FILES and self.command in {"GET", "HEAD"}:
                self._static(STATIC_FILES[path], include_body=include_body)
                return
            if path == "/api/health" and self.command in {"GET", "HEAD"}:
                self._json(HTTPStatus.OK, self._health(), include_body=include_body)
                return
            if not path.startswith("/api/v1/"):
                raise ApplicationError(
                    "route_not_found",
                    "The requested local route does not exist.",
                    "Open the controller home page or use a documented API route.",
                    status=404,
                )
            if self.command not in {"GET", "HEAD", "POST", "DELETE"}:
                raise ApplicationError(
                    "method_not_allowed",
                    "This API method is not supported for the requested route.",
                    "Use the documented HTTP method for this resource.",
                    status=405,
                )
            payload, status = self._api(
                path,
                parse_qs(split.query, keep_blank_values=True),
                include_body=include_body,
            )
            self._json(status, payload, include_body=include_body)
        except ApplicationError as error:
            self._error(error, include_body=include_body)
        except (DiscoveryError, RecipeError, CatalogError) as error:
            self._error(self._service_error(error), include_body=include_body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._error(
                ApplicationError(
                    "invalid_json",
                    "The request body must be valid UTF-8 JSON.",
                    "Send a JSON object using application/json.",
                ),
                include_body=include_body,
            )
        except (BrokenPipeError, ConnectionResetError):
            return
        except Exception:
            self._error(
                ApplicationError(
                    "internal_error",
                    "The local controller could not complete this request.",
                    "Retry the request. If it continues, inspect the local "
                    "controller log.",
                    status=500,
                    retryable=True,
                ),
                include_body=include_body,
            )

    def _api(
        self, path: str, query: Mapping[str, list[str]], *, include_body: bool
    ) -> tuple[dict[str, Any], HTTPStatus]:
        del include_body
        host = self.server.host
        parts = [part for part in path.split("/") if part]
        # parts are always ["api", "v1", ...] after _dispatch's prefix check.
        tail = parts[2:]
        if tail and tail[0] in {"setup", "catalog", "recipes"}:
            return self._service_api(host, tail, query)
        if (
            tail
            and tail[0] == "operations"
            and len(tail) == 3
            and tail[2] == "cancel"
            and self.command == "POST"
            and host.recipes.cancel_prepare(tail[1])
        ):
            self._require_empty_body()
            return {"operation": {"id": tail[1], "cancel_requested": True}}, (
                HTTPStatus.ACCEPTED
            )
        app = host.app
        if app is None:
            if self.command in {"GET", "HEAD"} and tail == ["bootstrap"]:
                return self._setup_bootstrap(host), HTTPStatus.OK
            raise ApplicationError(
                "setup_required",
                "No nodes are configured yet.",
                "Open Setup to discover and connect your DGX Spark nodes.",
                status=409,
            )
        if self.command in {"GET", "HEAD"}:
            if tail == ["bootstrap"]:
                payload = app.bootstrap()
                payload["setup"] = host.status()
                payload["health"] = self._health()
                return payload, HTTPStatus.OK
            if tail == ["nodes"]:
                return {"items": app.list_nodes()}, HTTPStatus.OK
            if tail == ["telemetry"]:
                return app.list_telemetry(), HTTPStatus.OK
            if len(tail) == 2 and tail[0] == "nodes":
                return app.get_node(tail[1]), HTTPStatus.OK
            if tail == ["models"]:
                return {"items": app.list_models()}, HTTPStatus.OK
            if tail == ["profiles"]:
                return {"items": app.list_profiles()}, HTTPStatus.OK
            if tail == ["deployments"]:
                return {"items": app.list_deployments()}, HTTPStatus.OK
            if tail == ["operations"]:
                return {
                    "items": app.list_operations(limit=self._limit(query))
                }, HTTPStatus.OK
            if len(tail) == 2 and tail[0] == "operations":
                return app.get_operation(tail[1]), HTTPStatus.OK
            if len(tail) == 3 and tail[0] == "operations" and tail[2] == "logs":
                return app.get_logs(
                    tail[1],
                    node_id=self._optional_query(query, "node"),
                    cursor=self._optional_query(query, "cursor"),
                ), HTTPStatus.OK
        if self.command == "POST":
            if len(tail) == 3 and tail[0] == "nodes" and tail[2] == "refresh":
                self._require_empty_body()
                return app.refresh_node(tail[1]), HTTPStatus.OK
            if tail == ["deployments"]:
                return {
                    "operation": app.submit_load(
                        self._json_body(), idempotency_key=self._idempotency_key()
                    )
                }, HTTPStatus.ACCEPTED
            if len(tail) == 3 and tail[0] == "operations" and tail[2] == "cancel":
                self._require_empty_body()
                return {
                    "operation": app.cancel_operation(
                        tail[1], idempotency_key=self._idempotency_key()
                    )
                }, HTTPStatus.ACCEPTED
            if tail == ["reconcile"]:
                self._require_empty_body()
                return {
                    "operation": app.submit_reconcile(
                        idempotency_key=self._idempotency_key()
                    )
                }, HTTPStatus.ACCEPTED
            if tail == ["recover"]:
                return {
                    "operation": app.submit_recover(
                        self._json_body(), idempotency_key=self._idempotency_key()
                    )
                }, HTTPStatus.ACCEPTED
        if self.command == "DELETE" and len(tail) == 2 and tail[0] == "deployments":
            self._require_empty_body()
            return {
                "operation": app.submit_unload(
                    tail[1], idempotency_key=self._idempotency_key()
                )
            }, HTTPStatus.ACCEPTED
        raise ApplicationError(
            "route_not_found",
            "The requested API route does not exist.",
            "Use a documented versioned controller route.",
            status=404,
        )

    def _health(self) -> dict[str, Any]:
        status = self.server.host.status()
        return {
            "status": "ok",
            "version": VERSION,
            "schema_version": 1,
            "mode": status["mode"],
            "setup_required": status["setup_required"],
        }

    def _setup_bootstrap(self, host: ServiceHost) -> dict[str, Any]:
        return {
            "nodes": [],
            "models": [],
            "profiles": [],
            "deployments": [],
            "operations": [],
            "warnings": [],
            "health": self._health(),
            "setup": host.status(),
        }

    def _service_api(
        self, host: ServiceHost, tail: list[str], query: Mapping[str, list[str]]
    ) -> tuple[dict[str, Any], HTTPStatus]:
        """Routes for onboarding, catalog lookups, and the recipe builder."""
        setup, recipes = host.setup, host.recipes
        method = "GET" if self.command == "HEAD" else self.command
        route = (method, *tail)
        if route == ("GET", "setup"):
            return {**host.status(), **setup.overview()}, HTTPStatus.OK
        if route == ("POST", "setup", "scans"):
            return {"scan": setup.start_scan(self._json_body())}, HTTPStatus.ACCEPTED
        if method == "GET" and len(tail) == 3 and tail[:2] == ["setup", "scans"]:
            return {"scan": setup.get_scan(tail[2])}, HTTPStatus.OK
        if (
            method == "POST"
            and len(tail) == 4
            and tail[:2] == ["setup", "scans"]
            and tail[3] == "cancel"
        ):
            self._require_empty_body()
            return {"scan": setup.cancel_scan(tail[2])}, HTTPStatus.ACCEPTED
        if route == ("GET", "setup", "ssh-keys"):
            return {"items": setup.ssh_keys()}, HTTPStatus.OK
        if route == ("POST", "setup", "ssh-keys"):
            self._require_empty_body()
            return setup.generate_key(), HTTPStatus.CREATED
        if route == ("POST", "setup", "host-keys"):
            return setup.host_keys(self._json_body()), HTTPStatus.OK
        if route == ("POST", "setup", "host-keys", "trust"):
            return setup.trust_host_key(self._json_body()), HTTPStatus.OK
        if route == ("POST", "setup", "identify"):
            return {"identity": setup.identify(self._json_body())}, HTTPStatus.OK
        if route == ("POST", "setup", "links"):
            return setup.check_links(self._json_body()), HTTPStatus.OK
        if route == ("POST", "setup", "nodes"):
            return setup.save_nodes(self._json_body()), HTTPStatus.OK
        if route == ("POST", "catalog", "models", "inspect"):
            body = self._json_body()
            if set(body) - {"link"}:
                raise RecipeError("invalid_request", "Send only the model 'link'.")
            return recipes.inspect_model(body.get("link")), HTTPStatus.OK
        if route == ("GET", "catalog", "vllm-releases"):
            include = self._optional_query(query, "prereleases") == "1"
            return recipes.list_releases(include_prereleases=include), HTTPStatus.OK
        if route == ("GET", "recipes"):
            return {"items": recipes.list_drafts()}, HTTPStatus.OK
        if route == ("POST", "recipes"):
            return {"recipe": recipes.create_draft(self._json_body())}, (
                HTTPStatus.CREATED
            )
        if len(tail) == 2 and tail[0] == "recipes":
            if method == "GET":
                return {"recipe": recipes.get_draft(tail[1])}, HTTPStatus.OK
            if method == "POST":
                return {"recipe": recipes.update_draft(tail[1], self._json_body())}, (
                    HTTPStatus.OK
                )
            if method == "DELETE":
                self._require_empty_body()
                recipes.delete_draft(tail[1])
                return {"deleted": tail[1]}, HTTPStatus.OK
        if method == "POST" and len(tail) == 3 and tail[::2] == ["recipes", "prepare"]:
            self._require_empty_body()
            idempotency_key = self._idempotency_key()
            ControllerApplication._require_idempotency_key(idempotency_key)
            operation = recipes.submit_prepare(tail[1], idempotency_key=idempotency_key)
            app = host.app
            resource = (
                app.get_operation(operation.id)
                if app is not None
                else {"id": operation.id, "state": operation.state.value}
            )
            return {"operation": resource}, HTTPStatus.ACCEPTED
        raise ApplicationError(
            "route_not_found",
            "The requested API route does not exist.",
            "Use a documented versioned controller route.",
            status=404,
        )

    @staticmethod
    def _service_error(
        error: DiscoveryError | RecipeError | CatalogError,
    ) -> ApplicationError:
        code = error.code
        status = 400
        if code in NOT_FOUND_CODES:
            status = 404
        elif code in CONFLICT_CODES:
            status = 409
        elif isinstance(error, CatalogError) and code.startswith("upstream"):
            status = 502
        return ApplicationError(
            code,
            error.message,
            error.remediation or "Correct the request and try again.",
            status=status,
            retryable=status in {409, 502},
        )

    def _static(self, filename: str, *, include_body: bool) -> None:
        path = self.server.web_root / filename
        try:
            body = path.read_bytes()
        except OSError:
            raise ApplicationError(
                "static_asset_missing",
                "A required local UI asset is unavailable.",
                "Restore the controller web UI files and restart the service.",
                status=500,
            ) from None
        self._send(
            HTTPStatus.OK,
            body,
            CONTENT_TYPES.get(path.suffix, "application/octet-stream"),
            include_body=include_body,
            cache_control="no-cache",
        )

    def _json_body(self) -> Mapping[str, Any]:
        content_type = self.headers.get("Content-Type", "")
        if not content_type.lower().startswith("application/json"):
            raise ApplicationError(
                "invalid_content_type",
                "JSON requests must use Content-Type application/json.",
                "Send a JSON object using application/json.",
                status=415,
            )
        content_length = self._content_length()
        if content_length == 0:
            raise ApplicationError(
                "invalid_json",
                "A JSON object request body is required.",
                "Send the documented JSON request object.",
            )
        value = json.loads(self.rfile.read(content_length).decode("utf-8"))
        if not isinstance(value, dict):
            raise ApplicationError(
                "invalid_json",
                "The JSON request body must be an object.",
                "Send the documented JSON request object.",
            )
        return value

    def _require_empty_body(self) -> None:
        content_length = self._content_length(required=False)
        if content_length == 0:
            return
        content_type = self.headers.get("Content-Type", "")
        if not content_type.lower().startswith("application/json"):
            raise ApplicationError(
                "invalid_content_type",
                "JSON requests must use Content-Type application/json.",
                "Send an empty JSON object using application/json.",
                status=415,
            )
        value = json.loads(self.rfile.read(content_length).decode("utf-8"))
        if value != {}:
            raise ApplicationError(
                "invalid_request",
                "This action does not accept request fields.",
                "Remove request fields and retry the action.",
            )

    def _content_length(self, *, required: bool = True) -> int:
        raw = self.headers.get("Content-Length")
        if raw is None:
            if required:
                raise ApplicationError(
                    "content_length_required",
                    "A bounded request body is required.",
                    "Send Content-Length with the JSON request.",
                )
            return 0
        try:
            value = int(raw)
        except ValueError as error:
            raise ApplicationError(
                "invalid_content_length",
                "The request Content-Length is invalid.",
                "Send a bounded JSON request.",
            ) from error
        if value < 0 or value > MAX_JSON_BYTES:
            raise ApplicationError(
                "request_too_large",
                "The request body exceeds the local controller limit.",
                "Send only the documented small JSON request.",
                status=413,
            )
        return value

    def _idempotency_key(self) -> str:
        return self.headers.get("Idempotency-Key", "")

    @staticmethod
    def _optional_query(query: Mapping[str, list[str]], name: str) -> str | None:
        values = query.get(name, [])
        if len(values) > 1:
            raise ApplicationError(
                "invalid_query",
                "A query parameter may be supplied only once.",
                "Use one bounded query value.",
            )
        return values[0] if values and values[0] else None

    @classmethod
    def _limit(cls, query: Mapping[str, list[str]]) -> int:
        raw = cls._optional_query(query, "limit")
        if raw is None:
            return 50
        if not raw.isdecimal():
            raise ApplicationError(
                "invalid_limit",
                "The operation limit must be a positive integer.",
                "Use a limit between 1 and 100.",
            )
        value = int(raw)
        if not 1 <= value <= 100:
            raise ApplicationError(
                "invalid_limit",
                "The operation limit must be between 1 and 100.",
                "Use a bounded operation limit.",
            )
        return value

    def _json(
        self, status: HTTPStatus, value: Mapping[str, Any], *, include_body: bool
    ) -> None:
        body = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )
        self._send(
            status,
            body,
            "application/json; charset=utf-8",
            include_body=include_body,
            no_store=True,
        )

    def _error(self, error: ApplicationError, *, include_body: bool = True) -> None:
        self._json(
            HTTPStatus(error.status), error.envelope(), include_body=include_body
        )

    def _send(
        self,
        status: HTTPStatus,
        body: bytes,
        content_type: str,
        *,
        include_body: bool,
        no_store: bool = False,
        cache_control: str | None = None,
    ) -> None:
        self.send_response(status.value)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", CONTENT_SECURITY_POLICY)
        if no_store:
            self.send_header("Cache-Control", "no-store")
        elif cache_control:
            self.send_header("Cache-Control", cache_control)
        self.end_headers()
        if include_body and self.command != "HEAD":
            self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        """Avoid writing request paths, headers, or remote details into local logs."""


def create_server(
    service: ServiceHost,
    *,
    host: str = "127.0.0.1",
    port: int = 18876,
    web_root: str | Path | None = None,
) -> ControllerHTTPServer:
    """Create, but do not start, a loopback-only HTTP server."""
    if not 0 <= int(port) <= 65535:
        raise ValueError("controller port must be between 0 and 65535")
    root = Path(web_root) if web_root is not None else Path(__file__).with_name("webui")
    if not root.is_dir():
        raise ValueError("controller web UI directory does not exist")
    return ControllerHTTPServer((host, int(port)), service, root)
