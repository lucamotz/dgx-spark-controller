# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Unit coverage for the durable journal's safety properties."""

from __future__ import annotations

import sqlite3
import unittest

from controller.operations import OperationAction, OperationPhase, OperationState
from controller.persistence import OperationJournal


class OperationJournalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.journal = OperationJournal()

    def tearDown(self) -> None:
        self.journal.close()

    def _operation(self, key: str, action: OperationAction = OperationAction.LOAD):
        operation, created = self.journal.create_or_get_operation(
            action=action,
            idempotency_key=key,
            request={"token": "must-not-persist"},
            node_ids=["node-a"],
        )
        self.assertTrue(created)
        return operation

    def test_idempotency_returns_one_operation_and_redacts_event_details(self) -> None:
        first = self._operation("same-request")
        second, created = self.journal.create_or_get_operation(
            action=OperationAction.LOAD,
            idempotency_key="same-request",
            request={"different": True},
            node_ids=["node-b"],
        )

        self.assertFalse(created)
        self.assertEqual(first.id, second.id)
        self.assertEqual("[redacted]", first.request["token"])
        self.assertEqual(1, len(self.journal.events(first.id)))
        with self.assertRaises(ValueError):
            self.journal.create_or_get_operation(
                action=OperationAction.UNLOAD,
                idempotency_key="same-request",
                request={},
                node_ids=["node-a"],
            )

    def test_events_are_ordered_and_database_append_only(self) -> None:
        operation = self._operation("event-order")
        self.journal.claim_operation(operation.id)
        self.journal.advance(
            operation.id,
            phase=OperationPhase.LOCKING,
            code="locks",
            message="Locks acquired.",
        )

        events = self.journal.events(operation.id)
        self.assertEqual([1, 2, 3], [event.sequence for event in events])
        with self.assertRaises(sqlite3.DatabaseError):
            self.journal._connection.execute(  # deliberate schema-level integrity check
                "UPDATE operation_events SET message = 'rewritten' "
                "WHERE operation_id = ?",
                (operation.id,),
            )

    def test_illegal_phase_is_rejected_and_claim_is_single_worker(self) -> None:
        operation = self._operation("phase-graph")
        claimed, did_claim = self.journal.claim_operation(operation.id)
        self.assertTrue(did_claim)
        repeated, did_claim_again = self.journal.claim_operation(operation.id)
        self.assertFalse(did_claim_again)
        self.assertEqual(claimed.id, repeated.id)
        with self.assertRaisesRegex(Exception, "cannot move"):
            self.journal.advance(
                operation.id,
                phase=OperationPhase.STARTING_HEAD,
                code="skip",
                message="Skipped mandatory phases.",
            )

    def test_cluster_and_node_locks_conflict_then_release(self) -> None:
        first = self._operation("lock-one")
        second = self._operation("lock-two")
        self.journal.acquire_locks(first.id, ["node-a"], lease_seconds=30)
        with self.assertRaisesRegex(Exception, "locked"):
            self.journal.acquire_locks(second.id, ["node-a"], lease_seconds=30)
        self.journal.release_locks(first.id)
        self.journal.acquire_locks(second.id, ["node-a"], lease_seconds=30)
        self.assertEqual(second.id, self.journal.active_locks()["node:node-a"])

    def test_terminal_state_cannot_be_reopened(self) -> None:
        operation = self._operation("terminal")
        self.journal.claim_operation(operation.id)
        final = self.journal.finish(
            operation.id,
            state=OperationState.FAILED,
            code="failed",
            message="Expected failure.",
            error_code="expected",
        )
        self.assertEqual(OperationState.FAILED, final.state)
        with self.assertRaisesRegex(Exception, "cannot move"):
            self.journal.advance(
                operation.id,
                phase=OperationPhase.LOCKING,
                code="reopen",
                message="Not permitted.",
            )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
