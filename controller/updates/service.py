# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Host-level update service behind the Updates page.

It owns the saved settings, the latest node update status, the one-time
helper install, and the scheduler thread that starts an ``update`` operation
when a slot is due. The operation itself runs on the lifecycle worker of the
current :class:`~controller.app.ControllerApplication`.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime
from typing import Any

from controller.app import ApplicationError, ControllerApplication
from controller.operations import OperationAction, OperationState, utc_now
from controller.resources import safe_text
from controller.updates.nodeops import (
    NodeFacts,
    UpdateCheck,
    UpdateNodeError,
    UpdateNodeOps,
    helper_sha256,
    relay_hosts,
    relay_tunnels,
    require_sudo_password,
)
from controller.updates.relay import UpdateRelay
from controller.updates.settings import (
    SettingsError,
    SettingsStore,
    UpdateSettings,
    parse_request,
    slot_key,
)

logger = logging.getLogger(__name__)


def _local_now() -> datetime:
    return datetime.now().astimezone()


class UpdateService:
    """Settings, node update status, helper installs and the schedule."""

    def __init__(
        self,
        *,
        store: SettingsStore,
        relay: UpdateRelay,
        app: Callable[[], ControllerApplication | None],
        node_ops: Callable[[], UpdateNodeOps | None],
        tick_seconds: float = 30.0,
        now: Callable[[], datetime] = _local_now,
    ) -> None:
        self.store = store
        self.relay = relay
        self._app = app
        self._node_ops = node_ops
        self._tick_seconds = tick_seconds
        self._now = now
        self._mutex = threading.Lock()
        self._facts: dict[str, NodeFacts] = {}
        self._checks: dict[str, UpdateCheck] = {}
        self._checked_at: str | None = None
        self._checking = False
        self._check_error: str | None = None
        self._schedule_error: str | None = None
        # The newest finished update whose effects the cached facts reflect.
        self._settled_update: str | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        try:
            self._settings = store.load()
            self._settings_error: dict[str, str] | None = None
        except SettingsError as error:
            self._settings = UpdateSettings()
            self._settings_error = {"code": error.code, "message": error.message}

    def settings(self) -> UpdateSettings:
        with self._mutex:
            return self._settings

    def start(self) -> None:
        """Start the scheduler and a first read-only probe of the nodes."""
        self._thread = threading.Thread(
            target=self._schedule_loop, name="dgx-update-schedule", daemon=True
        )
        self._thread.start()
        threading.Thread(
            target=self._probe, name="dgx-update-probe", daemon=True
        ).start()

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def overview(self) -> dict[str, Any]:
        app = self._app()
        settings = self.settings()
        next_run = settings.next_run(self._now())
        active, last = self._operations(app)
        if app and last and active is None and last != self._settled_update:
            self._settled_update = last
            record = app.journal.get_operation(last)
            # Only a failed update left the nodes untouched; after any other the
            # earlier counts and versions are stale.
            if record is not None and record.state is not OperationState.FAILED:
                with self._mutex:
                    self._checks, self._checked_at = {}, None
                threading.Thread(
                    target=self._probe, name="dgx-update-probe", daemon=True
                ).start()
        with self._mutex:
            nodes = [
                self._node_row(node_id, node.display_name)
                for node_id, node in (app.nodes if app else {}).items()
            ]
            return {
                "settings": {
                    key: value
                    for key, value in settings.to_dict().items()
                    if key != "schema_version"
                },
                "settings_error": self._settings_error,
                "schedule_error": self._schedule_error,
                "timezone": self._now().tzname(),
                "next_run_at": next_run.isoformat() if next_run else None,
                "helper": {"sha256_prefix": helper_sha256()[:12]},
                "checking": self._checking,
                "check_error": self._check_error,
                "checked_at": self._checked_at,
                "active_operation_id": active,
                "last_operation": app.get_operation(last) if app and last else None,
                "nodes": nodes,
            }

    def save_settings(self, body: Mapping[str, Any]) -> dict[str, Any]:
        try:
            settings = parse_request(body, self.settings())
            self.store.save(settings)
        except SettingsError as error:
            raise ApplicationError(
                error.code, error.message, "Correct the setting and save again."
            ) from error
        with self._mutex:
            self._settings, self._settings_error = settings, None
        return self.overview()

    def submit_update(
        self,
        body: Mapping[str, Any],
        *,
        idempotency_key: str,
        requester: str | None = None,
    ) -> dict[str, Any]:
        """Queue an update unless a check holds the nodes' relay port."""
        app = self._app()
        if app is None:
            raise ApplicationError(
                "setup_required",
                "No nodes are configured yet.",
                "Open Setup to connect your DGX Spark nodes.",
                status=409,
            )
        if self._checking:
            raise ApplicationError(
                "update_check_running",
                "An update check is running.",
                "Wait for the check to finish, then start the update.",
                status=409,
            )
        return app.submit_update(
            body, idempotency_key=idempotency_key, requester=requester
        )

    def start_check(self) -> dict[str, Any]:
        app, ops = self._require_ready()
        with self._mutex:
            if not self._checking:
                self._checking = True
                threading.Thread(
                    target=self._check,
                    args=(ops, tuple(app.nodes)),
                    name="dgx-update-check",
                    daemon=True,
                ).start()
        return self.overview()

    def install_helper(self, body: Mapping[str, Any]) -> dict[str, Any]:
        app, ops = self._require_ready()
        if set(body) - {"password", "node_ids"}:
            raise ApplicationError(
                "invalid_request",
                "The request contains unsupported fields.",
                "Send only the sudo password and node ids.",
            )
        try:
            password = require_sudo_password(body.get("password"))
        except ValueError as error:
            raise ApplicationError(
                "invalid_sudo_password",
                "Enter the node user's sudo password.",
                "The password is used once and never stored.",
            ) from error
        node_ids = body.get("node_ids")
        if node_ids is None:
            with self._mutex:
                node_ids = [
                    node_id
                    for node_id in app.nodes
                    if node_id not in self._facts
                    or self._facts[node_id].helper != "ready"
                ]
        if not isinstance(node_ids, list) or not all(
            isinstance(node_id, str) and node_id in app.nodes for node_id in node_ids
        ):
            raise ApplicationError(
                "node_not_found",
                "Choose configured nodes.",
                "Refresh the Updates page and retry.",
                status=404,
            )
        results = []
        for node_id in node_ids:
            try:
                ops.install_helper(node_id, password)
                results.append({"node_id": node_id, "state": "installed"})
            except UpdateNodeError as error:
                results.append(
                    {
                        "node_id": node_id,
                        "state": "failed",
                        "code": error.code,
                        "message": safe_text(error.message),
                    }
                )
        del password
        self._probe(ops, node_ids)
        return {"results": results, "updates": self.overview()}

    def _require_ready(self) -> tuple[ControllerApplication, UpdateNodeOps]:
        app, ops = self._app(), self._node_ops()
        if app is None or ops is None:
            raise ApplicationError(
                "setup_required",
                "No nodes are configured yet.",
                "Open Setup to connect your DGX Spark nodes.",
                status=409,
            )
        active, _last = self._operations(app)
        if active:
            raise ApplicationError(
                "update_in_progress",
                "A cluster update is running.",
                "Wait for it to finish.",
                status=409,
                operation_id=active,
            )
        return app, ops

    @staticmethod
    def _operations(
        app: ControllerApplication | None,
    ) -> tuple[str | None, str | None]:
        """The active update operation and the newest update operation."""
        if app is None:
            return None, None
        updates = [
            record
            for record in app.journal.list_operations()
            if record.action is OperationAction.UPDATE
        ]
        active = next(
            (
                record.id
                for record in updates
                if record.state in {OperationState.QUEUED, OperationState.RUNNING}
            ),
            None,
        )
        return active, updates[0].id if updates else None

    def _node_row(self, node_id: str, display_name: str) -> dict[str, Any]:
        facts = self._facts.get(node_id)
        check = self._checks.get(node_id)
        run = facts.run if facts else None
        return {
            "node_id": node_id,
            "display_name": display_name,
            "reachable": bool(facts and facts.reachable),
            "helper": facts.helper if facts else "unreachable",
            "message": safe_text(facts.message, fallback="") if facts else "",
            "os_version": facts.os_version if facts else "",
            "dgx_version": facts.dgx_version if facts else "",
            "kernel": facts.kernel if facts else "",
            "driver": facts.driver if facts else "",
            "packages": check.packages if check else None,
            "firmware": check.firmware if check else None,
            "reboot_required": check.reboot_required if check else None,
            "checked": check is not None,
            "run": {
                "state": run.state,
                "mode": run.mode,
                "step": run.step,
                "failed_step": run.failed_step,
            }
            if run
            else None,
        }

    def _probe(
        self,
        ops: UpdateNodeOps | None = None,
        node_ids: Iterable[str] | None = None,
    ) -> dict[str, NodeFacts]:
        ops = ops or self._node_ops()
        app = self._app()
        if ops is None or app is None:
            return {}
        facts = {node_id: ops.facts(node_id) for node_id in node_ids or app.nodes}
        with self._mutex:
            self._facts.update(facts)
        return facts

    def _check(self, ops: UpdateNodeOps, node_ids: tuple[str, ...]) -> None:
        checks: dict[str, UpdateCheck] = {}
        error: str | None = "The check stopped unexpectedly."
        try:
            facts = self._probe(ops, node_ids)
            ready = [
                node_id for node_id in node_ids if facts[node_id].helper == "ready"
            ]
            if ready:
                with (
                    self.relay.serving(
                        self.settings().relay_source(), relay_hosts(facts.values())
                    ) as relay,
                    relay_tunnels(ops, ready, relay),
                ):
                    for node_id in ready:
                        checks[node_id] = ops.check(node_id)
            error = None
        except (UpdateNodeError, OSError) as failure:
            error = safe_text(getattr(failure, "message", None) or str(failure))
        finally:
            with self._mutex:
                self._checks = checks
                self._checked_at = utc_now()
                self._check_error = error
                self._checking = False

    def _schedule_loop(self) -> None:
        while not self._stop.wait(self._tick_seconds):
            try:
                self.tick()
            except Exception:
                # The scheduler must outlive one bad tick (e.g. a reload).
                logger.exception("update scheduler tick failed")

    def tick(self) -> None:
        """Start the due slot's update; a slot's key makes this idempotent."""
        slot = self.settings().due_slot(self._now())
        app = self._app()
        if slot is None or app is None or self._checking:
            # A running check holds the nodes' relay port; retry next tick.
            return
        try:
            app.submit_update({}, idempotency_key=slot_key(slot), requester="schedule")
            error = None
        except ApplicationError as failure:
            error = failure.message
            logger.warning("scheduled update not started: %s", failure.code)
        with self._mutex:
            self._schedule_error = error
