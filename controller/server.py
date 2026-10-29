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

from controller.app import (
    SCHEMA_VERSION,
    VERSION,
    ApplicationError,
    ControllerApplication,
    require_idempotency_key,
)
from controller.catalog.http import CatalogError
from controller.discovery.network import DiscoveryError
from controller.host import ServiceHost
from controller.operations import OperationRecord
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
    "/ui-updates.js": "ui-updates.js",
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
        "recipe_prepared",
        "profile_in_use",
        "update_in_progress",
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
            self._require_loopback_host()
            if self.command not in {"GET", "HEAD", "POST", "DELETE"}:
                raise ApplicationError(
                    "method_not_allowed",
                    "This API method is not supported for the requested route.",
                    "Use the documented HTTP method for this resource.",
                    status=405,
                )
            payload, status = self._api(
                path, parse_qs(split.query, keep_blank_values=True)
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
        self, path: str, query: Mapping[str, list[str]]
    ) -> tuple[dict[str, Any], HTTPStatus]:
        host = self.server.host
        # ``_dispatch`` already checked the ``/api/v1/`` prefix.
        tail = [part for part in path.split("/") if part][2:]
        method = "GET" if self.command == "HEAD" else self.command
        route = (method, *tail)
        if tail and tail[0] in {"setup", "catalog", "recipes"}:
            return self._service_api(host, route, query)
        match route:
            # Recipe operations are cancelled by the recipe service, which runs
            # with or without a lifecycle application.
            case ("POST", "operations", operation_id, "cancel") if (
                host.recipes.cancel_operation(operation_id)
            ):
                self._require_empty_body()
                return _accepted({"id": operation_id, "cancel_requested": True})
        app = host.app
        if app is None:
            if route == ("GET", "bootstrap"):
                return self._setup_bootstrap(host), HTTPStatus.OK
            raise ApplicationError(
                "setup_required",
                "No nodes are configured yet.",
                "Open Setup to discover and connect your DGX Spark nodes.",
                status=409,
            )
        return self._lifecycle_api(host, app, route, query)

    def _lifecycle_api(
        self,
        host: ServiceHost,
        app: ControllerApplication,
        route: tuple[str, ...],
        query: Mapping[str, list[str]],
    ) -> tuple[dict[str, Any], HTTPStatus]:
        """Routes served by the configured lifecycle application."""
        match route:
            case ("GET", "bootstrap"):
                payload = app.bootstrap()
                payload["setup"] = host.status()
                payload["health"] = self._health()
                return payload, HTTPStatus.OK
            case ("GET", "nodes"):
                return {"items": app.list_nodes()}, HTTPStatus.OK
            case ("GET", "nodes", node_id):
                return app.get_node(node_id), HTTPStatus.OK
            case ("GET", "telemetry"):
                return app.list_telemetry(), HTTPStatus.OK
            case ("GET", "models"):
                return {"items": app.list_models()}, HTTPStatus.OK
            case ("GET", "profiles"):
                return {"items": app.list_profiles()}, HTTPStatus.OK
            case ("GET", "deployments"):
                return {"items": app.list_deployments()}, HTTPStatus.OK
            case ("GET", "operations"):
                limit = self._limit(query)
                return {"items": app.list_operations(limit=limit)}, HTTPStatus.OK
            case ("GET", "operations", operation_id):
                return app.get_operation(operation_id), HTTPStatus.OK
            case ("GET", "operations", operation_id, "logs"):
                logs = app.get_logs(
                    operation_id,
                    node_id=self._optional_query(query, "node"),
                    cursor=self._optional_query(query, "cursor"),
                )
                return logs, HTTPStatus.OK
            case ("POST", "nodes", node_id, "refresh"):
                self._require_empty_body()
                return app.refresh_node(node_id), HTTPStatus.OK
            case ("POST", "deployments"):
                return _accepted(
                    app.submit_load(
                        self._json_body(), idempotency_key=self._idempotency_key()
                    )
                )
            case ("POST", "operations", operation_id, "cancel"):
                self._require_empty_body()
                return _accepted(
                    app.cancel_operation(
                        operation_id, idempotency_key=self._idempotency_key()
                    )
                )
            case ("POST", "reconcile"):
                self._require_empty_body()
                return _accepted(
                    app.submit_reconcile(idempotency_key=self._idempotency_key())
                )
            case ("POST", "recover"):
                return _accepted(
                    app.submit_recover(
                        self._json_body(), idempotency_key=self._idempotency_key()
                    )
                )
            case ("GET", "updates"):
                return host.updates.overview(), HTTPStatus.OK
            case ("POST", "updates", "settings"):
                return host.updates.save_settings(self._json_body()), HTTPStatus.OK
            case ("POST", "updates", "check"):
                # A JSON body forces a CORS preflight, so no other site can
                # start a check.
                if self._json_body():
                    raise ApplicationError(
                        "invalid_request",
                        "This action does not accept request fields.",
                        "Send an empty JSON object.",
                    )
                return host.updates.start_check(), HTTPStatus.ACCEPTED
            case ("POST", "updates", "helper"):
                return host.updates.install_helper(self._json_body()), HTTPStatus.OK
            case ("POST", "updates", "run"):
                return _accepted(
                    host.updates.submit_update(
                        self._json_body(), idempotency_key=self._idempotency_key()
                    )
                )
            case ("DELETE", "deployments", deployment_id):
                self._require_empty_body()
                return _accepted(
                    app.submit_unload(
                        deployment_id, idempotency_key=self._idempotency_key()
                    )
                )
        raise _route_not_found()

    def _require_loopback_host(self) -> None:
        """Refuse DNS-rebound requests that address the API by another name.

        Any port is accepted, so an SSH tunnel to another local port works.
        """
        name = urlsplit(f"//{self.headers.get('Host') or ''}").hostname or ""
        try:
            loopback = name == "localhost" or ipaddress.ip_address(name).is_loopback
        except ValueError:
            loopback = False
        if not loopback:
            raise ApplicationError(
                "invalid_host_header",
                "The API answers only requests addressed to a loopback name.",
                "Open the controller at http://127.0.0.1:<port>/.",
                status=403,
            )

    def _health(self) -> dict[str, Any]:
        status = self.server.host.status()
        return {
            "status": "ok",
            "version": VERSION,
            "schema_version": SCHEMA_VERSION,
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
        self,
        host: ServiceHost,
        route: tuple[str, ...],
        query: Mapping[str, list[str]],
    ) -> tuple[dict[str, Any], HTTPStatus]:
        """Routes for onboarding, catalog lookups, and the recipe builder."""
        setup, recipes = host.setup, host.recipes
        match route:
            case ("GET", "setup"):
                return {**host.status(), **setup.overview()}, HTTPStatus.OK
            case ("POST", "setup", "scans"):
                return {"scan": setup.start_scan(self._json_body())}, (
                    HTTPStatus.ACCEPTED
                )
            case ("GET", "setup", "scans", scan_id):
                return {"scan": setup.get_scan(scan_id)}, HTTPStatus.OK
            case ("POST", "setup", "scans", scan_id, "cancel"):
                self._require_empty_body()
                return {"scan": setup.cancel_scan(scan_id)}, HTTPStatus.ACCEPTED
            case ("GET", "setup", "ssh-keys"):
                return {"items": setup.ssh_keys()}, HTTPStatus.OK
            case ("POST", "setup", "ssh-keys"):
                self._require_empty_body()
                return setup.generate_key(), HTTPStatus.CREATED
            case ("POST", "setup", "host-keys"):
                return setup.host_keys(self._json_body()), HTTPStatus.OK
            case ("POST", "setup", "host-keys", "trust"):
                return setup.trust_host_key(self._json_body()), HTTPStatus.OK
            case ("POST", "setup", "identify"):
                return {"identity": setup.identify(self._json_body())}, HTTPStatus.OK
            case ("POST", "setup", "links"):
                return setup.check_links(self._json_body()), HTTPStatus.OK
            case ("POST", "setup", "nodes"):
                return setup.save_nodes(self._json_body()), HTTPStatus.OK
            case ("POST", "catalog", "models", "inspect"):
                body = self._json_body()
                if set(body) - {"link"}:
                    raise RecipeError("invalid_request", "Send only the model 'link'.")
                return recipes.inspect_model(body.get("link")), HTTPStatus.OK
            case ("GET", "catalog", "vllm-releases"):
                include = self._optional_query(query, "prereleases") == "1"
                return recipes.list_releases(include_prereleases=include), (
                    HTTPStatus.OK
                )
            case ("GET", "recipes"):
                return {"items": recipes.list_drafts()}, HTTPStatus.OK
            case ("POST", "recipes"):
                return {"recipe": recipes.create_draft(self._json_body())}, (
                    HTTPStatus.CREATED
                )
            case ("GET", "recipes", draft_id):
                return {"recipe": recipes.get_draft(draft_id)}, HTTPStatus.OK
            case ("POST", "recipes", draft_id):
                return {"recipe": recipes.update_draft(draft_id, self._json_body())}, (
                    HTTPStatus.OK
                )
            case ("DELETE", "recipes", draft_id):
                self._require_empty_body()
                recipes.delete_draft(draft_id)
                return {"deleted": draft_id}, HTTPStatus.OK
            # Both are journaled recipe operations: an identifier, an optional
            # bounded flag for delete, and a required idempotency key.
            case ("POST", "recipes", draft_id, "prepare"):
                self._require_empty_body()
                operation = recipes.submit_prepare(
                    draft_id, idempotency_key=self._required_idempotency_key()
                )
                return self._recipe_operation(host, operation)
            case ("POST", "recipes", draft_id, "delete"):
                body = self._optional_json_body()
                operation = recipes.submit_delete(
                    draft_id, body, idempotency_key=self._required_idempotency_key()
                )
                return self._recipe_operation(host, operation)
        raise _route_not_found()

    @staticmethod
    def _recipe_operation(
        host: ServiceHost, operation: OperationRecord
    ) -> tuple[dict[str, Any], HTTPStatus]:
        app = host.app
        return _accepted(
            app.get_operation(operation.id)
            if app is not None
            else {"id": operation.id, "state": operation.state.value}
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
        self._require_json_content_type("Send a JSON object using application/json.")
        content_length = self._content_length()
        if content_length == 0:
            raise ApplicationError(
                "invalid_json",
                "A JSON object request body is required.",
                "Send the documented JSON request object.",
            )
        value = self._read_json(content_length)
        if not isinstance(value, dict):
            raise ApplicationError(
                "invalid_json",
                "The JSON request body must be an object.",
                "Send the documented JSON request object.",
            )
        return value

    def _optional_json_body(self) -> Mapping[str, Any]:
        if self._content_length(required=False) == 0:
            return {}
        return self._json_body()

    def _require_empty_body(self) -> None:
        content_length = self._content_length(required=False)
        if content_length == 0:
            return
        self._require_json_content_type(
            "Send an empty JSON object using application/json."
        )
        if self._read_json(content_length) != {}:
            raise ApplicationError(
                "invalid_request",
                "This action does not accept request fields.",
                "Remove request fields and retry the action.",
            )

    def _require_json_content_type(self, remediation: str) -> None:
        content_type = self.headers.get("Content-Type", "")
        if not content_type.lower().startswith("application/json"):
            raise ApplicationError(
                "invalid_content_type",
                "JSON requests must use Content-Type application/json.",
                remediation,
                status=415,
            )

    def _read_json(self, content_length: int) -> Any:
        return json.loads(self.rfile.read(content_length).decode("utf-8"))

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

    def _required_idempotency_key(self) -> str:
        idempotency_key = self._idempotency_key()
        require_idempotency_key(idempotency_key)
        return idempotency_key

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


def _accepted(operation: Mapping[str, Any]) -> tuple[dict[str, Any], HTTPStatus]:
    return {"operation": operation}, HTTPStatus.ACCEPTED


def _route_not_found() -> ApplicationError:
    return ApplicationError(
        "route_not_found",
        "The requested API route does not exist.",
        "Use a documented versioned controller route.",
        status=404,
    )


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
