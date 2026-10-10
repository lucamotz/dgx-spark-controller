# ADR-0002: SSH behind a transport interface

- Status: accepted
- Date: 2026-07-31

## Context

SSH is already available on DGX Spark nodes and is appropriate for a small
cluster, but orchestration must not construct raw SSH commands or become
coupled to a specific node agent.

## Decision

Define a `NodeTransport` protocol and implement it over SSH. Commands use
explicit argv, fixed scripts on stdin, key path references and redacted
output. Tests use a deterministic fake transport.

## Alternatives

- SSH directly in the orchestrator: fast to start, hard to test, easy to
  broaden unsafely.
- A remote HTTP agent first: a stronger long-term contract, but extra install
  and authentication work up front.
- A general-purpose command string: rejected as injection-prone.

## Consequences

SSH stays a replaceable adapter. A later authenticated node agent can
implement the same typed facts and commands without changing the domain,
configuration or operation contracts.

## Later updates

- Host keys are pinned into a controller-owned `known_hosts` file only after
  the operator confirms the fingerprint, and every connection uses
  `StrictHostKeyChecking=yes` (`controller/discovery/hostkeys.py`).
- Authentication is key-only (`BatchMode=yes`); authorizing a key with
  `ssh-copy-id` stays with the operator.
- Recipe preparation uses the same executor through
  `controller/recipes/nodeops.py`, with its own fixed scripts.
