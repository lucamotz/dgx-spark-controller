# ADR-0003: durable operation journal

- Status: accepted
- Date: 2026-07-31

## Context

Model loads take minutes, may be interrupted, and must remain understandable
after a browser or controller restart. A one-shot request and response cannot
represent rollback or partial state.

## Decision

Mutations are durable operations with idempotency keys, phases, append-only
redacted events, leases, the previous placement, and an explicit terminal
state, stored in SQLite. Clients poll the operation.

## Alternatives

- In-memory background tasks: lose progress and ownership on restart.
- WebSocket-only progress: does not solve durable recovery.
- Fire-and-forget shell commands: cannot prove readiness or rollback.

## Consequences

Persistence and reconciliation are required, and operation events become a
first-class support artifact.

## Later updates

- Recipe preparation is journaled as a `prepare` operation with the phase
  `preparing`. It runs on its own worker without leases, and the reconciler
  skips prepare operations that are still alive in the process (see
  [ADR-0005](0005-generated-recipes.md)).
