# ADR-0004: one lifecycle owner

- Status: accepted
- Date: 2026-07-31

## Context

Before the controller existed, client applications started and stopped models
on the Spark themselves. Two components starting or stopping the same large
model can race, leak memory, or interrupt users.

## Decision

The controller is the only component permitted to change model state on the
nodes it manages. Clients consume the OpenAI-compatible endpoint. Taking over
an existing deployment discovers and records it; the controller never silently
kills a process it cannot match to a profile.

## Alternatives

- Keep per-application lifecycle managers: ownership stays ambiguous.
- Proxy every inference request through the controller: unnecessary coupling
  and latency.
- Let a system service own everything without controller state: loses the
  desired/observed split and operation visibility.

## Consequences

Handing over an existing deployment needs a deliberate maintenance window. The
controller supports adoption through reconciliation and reports unmanaged
processes clearly. A process whose identity differs from its profile in any
pinned value, including environment and argument values, stays unmanaged even
if its endpoint is healthy.
