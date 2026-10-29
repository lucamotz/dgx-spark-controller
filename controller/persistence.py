# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Durable SQLite operation journal, deployment view, and cross-worker leases.

This module is the only writer for operation history.  The operation-events table
has database triggers that reject UPDATE and DELETE statements so an audit trail
cannot accidentally be rewritten by a future UI feature.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any

from controller.operations import (
    DeploymentRecord,
    LockConflict,
    OperationAction,
    OperationEvent,
    OperationPhase,
    OperationRecord,
    OperationState,
    ensure_phase_transition,
    ensure_state_transition,
    utc_now,
)

SCHEMA_VERSION = 1
_SENSITIVE_KEYS = frozenset(
    {"token", "secret", "password", "authorization", "ssh_key", "private_key"}
)


def _json(value: Any) -> str:
    return json.dumps(
        _redact(value), sort_keys=True, separators=(",", ":"), default=str
    )


def _decode(value: str | None, default: Any) -> Any:
    if value is None:
        return default
    return json.loads(value)


def _redact(value: Any) -> Any:
    """Recursively redact conventional secret-bearing fields before persistence."""
    if isinstance(value, Mapping):
        return {
            str(key): "[redacted]"
            if str(key).lower() in _SENSITIVE_KEYS
            else _redact(item)
            for key, item in value.items()
        }
    if isinstance(value, (tuple, list)):
        return [_redact(item) for item in value]
    return value


class OperationJournal:
    """SQLite-backed journal safe for multiple local worker threads/processes.

    SQLite's ``BEGIN IMMEDIATE`` serialises lease acquisition and event sequencing.
    The connection is created with ``check_same_thread=False`` because a local API
    commonly journals work in a worker thread; an in-process re-entrant lock still
    protects the connection object itself.
    """

    def __init__(self, path: str = ":memory:") -> None:
        if path != ":memory:":
            directory = os.path.dirname(os.path.abspath(path))
            if directory:
                os.makedirs(directory, exist_ok=True)
        self.path = path
        self._connection = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._connection.row_factory = sqlite3.Row
        self._mutex = threading.RLock()
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute("PRAGMA busy_timeout = 5000")
        if path != ":memory:":
            self._connection.execute("PRAGMA journal_mode = WAL")
        self._migrate()

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        with self._mutex:
            self._connection.close()

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        with self._mutex:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
            except BaseException:
                self._connection.rollback()
                raise
            else:
                self._connection.commit()

    def _migrate(self) -> None:
        with self._mutex:
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS operations (
                    id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    action TEXT NOT NULL,
                    state TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    requester TEXT,
                    node_ids_json TEXT NOT NULL,
                    previous_placement_json TEXT,
                    result_json TEXT,
                    error_code TEXT,
                    error_message TEXT,
                    remediation TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS operation_events (
                    operation_id TEXT NOT NULL REFERENCES operations(id),
                    sequence INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    code TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    message TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    PRIMARY KEY (operation_id, sequence)
                );
                CREATE TRIGGER IF NOT EXISTS operation_events_append_only_update
                BEFORE UPDATE ON operation_events
                BEGIN SELECT RAISE(ABORT, 'operation events are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS operation_events_append_only_delete
                BEFORE DELETE ON operation_events
                BEGIN SELECT RAISE(ABORT, 'operation events are append-only'); END;
                CREATE TABLE IF NOT EXISTS operation_locks (
                    resource TEXT PRIMARY KEY,
                    operation_id TEXT NOT NULL REFERENCES operations(id),
                    expires_at_epoch REAL NOT NULL,
                    acquired_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS operation_locks_expiry
                    ON operation_locks(expires_at_epoch);
                CREATE TABLE IF NOT EXISTS deployments (
                    id TEXT PRIMARY KEY,
                    profile_id TEXT NOT NULL,
                    expected_alias TEXT NOT NULL,
                    node_ids_json TEXT NOT NULL,
                    desired_state TEXT NOT NULL,
                    observed_state TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    endpoint TEXT,
                    owning_operation_id TEXT REFERENCES operations(id),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS deployments_active_nodes
                    ON deployments(observed_state, desired_state);
                """
            )
            row = self._connection.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()
            if row is None:
                self._connection.execute(
                    "INSERT INTO schema_meta(key, value) VALUES ('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
            elif int(row["value"]) != SCHEMA_VERSION:
                raise RuntimeError(f"Unsupported journal schema version {row['value']}")

    def create_or_get_operation(
        self,
        *,
        action: OperationAction,
        idempotency_key: str,
        request: Mapping[str, Any],
        node_ids: Sequence[str],
        requester: str | None = None,
    ) -> tuple[OperationRecord, bool]:
        """Atomically create an operation or return the one for the same key.

        Idempotency keys are global across mutation types.  Reusing a key for a
        different action is rejected, preventing a retry of an old HTTP request
        from becoming a different destructive action.
        """
        if not idempotency_key.strip():
            raise ValueError("idempotency_key is required")
        if not node_ids:
            raise ValueError("node_ids is required")
        now = utc_now()
        operation_id = str(uuid.uuid4())
        with self._write() as connection:
            existing = connection.execute(
                "SELECT * FROM operations WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            if existing is not None:
                if existing["action"] != action.value:
                    raise ValueError(
                        "idempotency_key was already used for another action"
                    )
                return self._operation_from_row(existing), False
            connection.execute(
                """
                INSERT INTO operations(
                    id, idempotency_key, action, state, phase, request_json, requester,
                    node_ids_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    operation_id,
                    idempotency_key,
                    action.value,
                    OperationState.QUEUED.value,
                    OperationPhase.QUEUED.value,
                    _json(request),
                    requester,
                    _json(list(node_ids)),
                    now,
                    now,
                ),
            )
            self._append_event_in_transaction(
                connection,
                operation_id,
                code="operation_queued",
                phase=OperationPhase.QUEUED,
                message="Operation accepted and queued.",
                details={"action": action.value},
            )
            row = connection.execute(
                "SELECT * FROM operations WHERE id = ?", (operation_id,)
            ).fetchone()
            assert row is not None
            return self._operation_from_row(row), True

    def get_operation(self, operation_id: str) -> OperationRecord | None:
        """Return the materialised operation record, if it exists."""
        with self._mutex:
            row = self._connection.execute(
                "SELECT * FROM operations WHERE id = ?", (operation_id,)
            ).fetchone()
        return self._operation_from_row(row) if row is not None else None

    def claim_operation(self, operation_id: str) -> tuple[OperationRecord, bool]:
        """Atomically move a queued operation into ``running`` exactly once.

        A second local worker observing an already-running record receives
        ``claimed=False`` and must leave execution to the first worker or restart
        reconciliation.  This is the execution half of idempotency.
        """
        with self._write() as connection:
            current = self._require_operation(connection, operation_id)
            if current.state != OperationState.QUEUED:
                return current, False
            ensure_state_transition(current.state, OperationState.RUNNING)
            ensure_phase_transition(current.phase, OperationPhase.VALIDATING)
            now = utc_now()
            connection.execute(
                "UPDATE operations SET state = ?, phase = ?, updated_at = ? "
                "WHERE id = ?",
                (
                    OperationState.RUNNING.value,
                    OperationPhase.VALIDATING.value,
                    now,
                    operation_id,
                ),
            )
            self._append_event_in_transaction(
                connection,
                operation_id,
                code="operation_started",
                phase=OperationPhase.VALIDATING,
                message="Operation worker claimed queued work.",
                details={},
            )
            return self._require_operation(connection, operation_id), True

    def list_operations(
        self, states: Sequence[OperationState] | None = None
    ) -> list[OperationRecord]:
        """List newest operations first, optionally filtered by their current state."""
        with self._mutex:
            if states:
                marks = ",".join("?" for _ in states)
                rows = self._connection.execute(
                    f"SELECT * FROM operations WHERE state IN ({marks}) "
                    "ORDER BY created_at DESC",
                    tuple(state.value for state in states),
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT * FROM operations ORDER BY created_at DESC"
                ).fetchall()
        return [self._operation_from_row(row) for row in rows]

    def events(self, operation_id: str) -> list[OperationEvent]:
        """Read ordered immutable events for one operation."""
        with self._mutex:
            rows = self._connection.execute(
                "SELECT * FROM operation_events WHERE operation_id = ? "
                "ORDER BY sequence",
                (operation_id,),
            ).fetchall()
        return [
            OperationEvent(
                operation_id=row["operation_id"],
                sequence=row["sequence"],
                created_at=row["created_at"],
                code=row["code"],
                phase=OperationPhase(row["phase"]),
                message=row["message"],
                details=_decode(row["details_json"], {}),
            )
            for row in rows
        ]

    def advance(
        self,
        operation_id: str,
        *,
        phase: OperationPhase | None = None,
        state: OperationState | None = None,
        code: str,
        message: str,
        details: Mapping[str, Any] | None = None,
    ) -> OperationRecord:
        """Append an event and atomically materialise its phase/state change."""
        with self._write() as connection:
            current = self._require_operation(connection, operation_id)
            next_phase = phase or current.phase
            next_state = state or current.state
            ensure_phase_transition(current.phase, next_phase)
            ensure_state_transition(current.state, next_state)
            now = utc_now()
            connection.execute(
                "UPDATE operations SET phase = ?, state = ?, updated_at = ? "
                "WHERE id = ?",
                (next_phase.value, next_state.value, now, operation_id),
            )
            self._append_event_in_transaction(
                connection,
                operation_id,
                code=code,
                phase=next_phase,
                message=message,
                details=details or {},
            )
            return self._require_operation(connection, operation_id)

    def record_previous_placement(
        self, operation_id: str, placement: Mapping[str, Any]
    ) -> OperationRecord:
        """Store an immutable-in-events rollback snapshot before a destructive stop."""
        with self._write() as connection:
            current = self._require_operation(connection, operation_id)
            if current.previous_placement is not None:
                raise RuntimeError("previous placement was already recorded")
            now = utc_now()
            connection.execute(
                "UPDATE operations SET previous_placement_json = ?, updated_at = ? "
                "WHERE id = ?",
                (_json(placement), now, operation_id),
            )
            self._append_event_in_transaction(
                connection,
                operation_id,
                code="previous_placement_recorded",
                phase=current.phase,
                message="Prior deployment placement recorded for rollback.",
                details=placement,
            )
            return self._require_operation(connection, operation_id)

    def finish(
        self,
        operation_id: str,
        *,
        state: OperationState,
        code: str,
        message: str,
        result: Mapping[str, Any] | None = None,
        error_code: str | None = None,
        remediation: str | None = None,
    ) -> OperationRecord:
        """Terminally complete an operation with a final append-only event."""
        if not state.terminal:
            raise ValueError("finish requires a terminal operation state")
        with self._write() as connection:
            current = self._require_operation(connection, operation_id)
            ensure_state_transition(current.state, state)
            ensure_phase_transition(current.phase, OperationPhase.COMPLETE)
            now = utc_now()
            connection.execute(
                """
                UPDATE operations
                SET state = ?, phase = ?, result_json = ?, error_code = ?,
                    error_message = ?, remediation = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    state.value,
                    OperationPhase.COMPLETE.value,
                    _json(result) if result is not None else None,
                    error_code,
                    message if error_code else None,
                    remediation,
                    now,
                    operation_id,
                ),
            )
            self._append_event_in_transaction(
                connection,
                operation_id,
                code=code,
                phase=OperationPhase.COMPLETE,
                message=message,
                details=result or {},
            )
            return self._require_operation(connection, operation_id)

    def acquire_locks(
        self,
        operation_id: str,
        node_ids: Sequence[str],
        *,
        lease_seconds: float = 60.0,
        cluster: bool = True,
    ) -> None:
        """Acquire/renew the cluster lease and per-node leases atomically.

        Lifecycle operations hold both; a recipe delete takes node leases only
        (``cluster=False``). Expired leases are cleared in the same transaction
        before conflict detection.
        """
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        resources = [f"node:{node_id}" for node_id in sorted(set(node_ids))]
        if cluster:
            resources.insert(0, "cluster:default")
        now_epoch = time.time()
        expires = now_epoch + lease_seconds
        acquired_at = utc_now()
        with self._write() as connection:
            self._require_operation(connection, operation_id)
            connection.execute(
                "DELETE FROM operation_locks WHERE expires_at_epoch <= ?", (now_epoch,)
            )
            for resource in resources:
                existing = connection.execute(
                    "SELECT operation_id FROM operation_locks WHERE resource = ?",
                    (resource,),
                ).fetchone()
                if existing is not None and existing["operation_id"] != operation_id:
                    raise LockConflict(resource, existing["operation_id"])
            for resource in resources:
                connection.execute(
                    """
                    INSERT INTO operation_locks(
                        resource, operation_id, expires_at_epoch, acquired_at
                    )
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(resource) DO UPDATE SET
                        operation_id = excluded.operation_id,
                        expires_at_epoch = excluded.expires_at_epoch,
                        acquired_at = excluded.acquired_at
                    """,
                    (resource, operation_id, expires, acquired_at),
                )

    def release_locks(self, operation_id: str) -> None:
        """Release all leases held by an operation; terminal journal state remains."""
        with self._write() as connection:
            connection.execute(
                "DELETE FROM operation_locks WHERE operation_id = ?", (operation_id,)
            )

    def active_locks(self) -> dict[str, str]:
        """Return non-expired resource-to-operation lease ownership."""
        now_epoch = time.time()
        with self._write() as connection:
            connection.execute(
                "DELETE FROM operation_locks WHERE expires_at_epoch <= ?", (now_epoch,)
            )
            rows = connection.execute(
                "SELECT resource, operation_id FROM operation_locks ORDER BY resource"
            ).fetchall()
        return {row["resource"]: row["operation_id"] for row in rows}

    def save_deployment(self, record: DeploymentRecord) -> DeploymentRecord:
        """Upsert the current desired/observed view.

        Historical operation events remain untouched.
        """
        with self._write() as connection:
            existing = connection.execute(
                "SELECT created_at FROM deployments WHERE id = ?", (record.id,)
            ).fetchone()
            created_at = (
                existing["created_at"] if existing is not None else record.created_at
            )
            connection.execute(
                """
                INSERT INTO deployments(
                    id, profile_id, expected_alias, node_ids_json, desired_state,
                    observed_state, spec_json, endpoint, owning_operation_id,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    profile_id = excluded.profile_id,
                    expected_alias = excluded.expected_alias,
                    node_ids_json = excluded.node_ids_json,
                    desired_state = excluded.desired_state,
                    observed_state = excluded.observed_state,
                    spec_json = excluded.spec_json,
                    endpoint = excluded.endpoint,
                    owning_operation_id = excluded.owning_operation_id,
                    updated_at = excluded.updated_at
                """,
                (
                    record.id,
                    record.profile_id,
                    record.expected_alias,
                    _json(list(record.node_ids)),
                    record.desired_state,
                    record.observed_state,
                    _json(record.spec),
                    record.endpoint,
                    record.owning_operation_id,
                    created_at,
                    record.updated_at,
                ),
            )
            row = connection.execute(
                "SELECT * FROM deployments WHERE id = ?", (record.id,)
            ).fetchone()
            assert row is not None
            return self._deployment_from_row(row)

    def get_deployment(self, deployment_id: str) -> DeploymentRecord | None:
        """Return one known deployment, including its serialised rollback spec."""
        with self._mutex:
            row = self._connection.execute(
                "SELECT * FROM deployments WHERE id = ?", (deployment_id,)
            ).fetchone()
        return self._deployment_from_row(row) if row is not None else None

    def list_deployments(
        self, *, include_stopped: bool = True
    ) -> list[DeploymentRecord]:
        """Return deployments ordered by latest observation."""
        with self._mutex:
            if include_stopped:
                rows = self._connection.execute(
                    "SELECT * FROM deployments ORDER BY updated_at DESC"
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT * FROM deployments WHERE observed_state != 'stopped' "
                    "ORDER BY updated_at DESC"
                ).fetchall()
        return [self._deployment_from_row(row) for row in rows]

    def _append_event_in_transaction(
        self,
        connection: sqlite3.Connection,
        operation_id: str,
        *,
        code: str,
        phase: OperationPhase,
        message: str,
        details: Mapping[str, Any],
    ) -> None:
        row = connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence "
            "FROM operation_events WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        connection.execute(
            """
            INSERT INTO operation_events(
                operation_id, sequence, created_at, code, phase, message, details_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                operation_id,
                row["next_sequence"],
                utc_now(),
                code,
                phase.value,
                message,
                _json(details),
            ),
        )

    def _require_operation(
        self, connection: sqlite3.Connection, operation_id: str
    ) -> OperationRecord:
        row = connection.execute(
            "SELECT * FROM operations WHERE id = ?", (operation_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown operation {operation_id}")
        return self._operation_from_row(row)

    @staticmethod
    def _operation_from_row(row: sqlite3.Row) -> OperationRecord:
        return OperationRecord(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            action=OperationAction(row["action"]),
            state=OperationState(row["state"]),
            phase=OperationPhase(row["phase"]),
            request=_decode(row["request_json"], {}),
            requester=row["requester"],
            node_ids=tuple(_decode(row["node_ids_json"], [])),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            previous_placement=_decode(row["previous_placement_json"], None),
            result=_decode(row["result_json"], None),
            error_code=row["error_code"],
            error_message=row["error_message"],
            remediation=row["remediation"],
        )

    @staticmethod
    def _deployment_from_row(row: sqlite3.Row) -> DeploymentRecord:
        return DeploymentRecord(
            id=row["id"],
            profile_id=row["profile_id"],
            expected_alias=row["expected_alias"],
            node_ids=tuple(_decode(row["node_ids_json"], [])),
            desired_state=row["desired_state"],
            observed_state=row["observed_state"],
            spec=_decode(row["spec_json"], {}),
            endpoint=row["endpoint"],
            owning_operation_id=row["owning_operation_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
