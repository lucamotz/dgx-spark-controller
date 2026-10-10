# ADR-0006: a switch replaces whole deployments

- Status: accepted
- Date: 2026-10-10

## Context

A load stops the managed deployment that occupies its selected nodes before it
starts anything. Until now it refused (`partial_cluster_conflict`) when that
deployment also used nodes outside the selection, for example a two-node
tensor-parallel deployment on spark-a and spark-b followed by a one-node
profile on spark-a. The operator had to unload first, which left no rollback
if the new model then failed to start.

A shared deployment cannot run on part of its placement: once one rank stops,
the others hold memory but serve nothing.

## Decision

1. A load replaces every managed deployment observed on a selected node on
   all of the nodes that deployment uses.
2. Before any decision, the operation extends its leases to those extra nodes
   in one all-or-nothing, deterministic step and refreshes them. The lease
   keeper renews the whole set. A live lease of another operation fails the
   load with `operation_locked` before any mutation.
3. Extra nodes must hold nothing but a replaced deployment. An unmanaged
   process there is refused (`unmanaged_conflict`) and never stopped. A
   different managed deployment, or a node that is no longer configured,
   refuses the switch with `partial_cluster_conflict`.
4. Every rank is drained and stopped in the existing order (serving roles
   before workers). The new placement must pass the new profile's release
   checks; the extra nodes must pass the replaced deployment's own unload
   checks. Only then is the replaced deployment recorded `stopped` and the new
   one started.
5. The previous placement records the extra nodes, and a rollback restores
   the replaced deployment on all of its nodes.
6. The UI confirmation names every node the switch will stop.

## Alternatives

- **Keep refusing and require an unload.** Safe but loses the transactional
  rollback, and invites a two-step manual procedure.
- **Stop only the ranks on the selected nodes.** Rejected: it leaves orphaned
  ranks holding memory on nodes the operator did not select.
- **Follow ownership transitively across further deployments.** Rejected: an
  extra node owned by another deployment indicates journal drift; the switch
  refuses and asks for reconciliation instead of widening its scope.

## Consequences

- A load can stop a model on a node the operator did not select. The
  confirmation dialog and the `extending_switch_scope` event make that
  explicit.
- Lease scope is known only after the first observation, so an operation's
  `node_ids` remain the requested placement; the full set is in its previous
  placement (`observed_node_ids`, `extra_node_ids`).
