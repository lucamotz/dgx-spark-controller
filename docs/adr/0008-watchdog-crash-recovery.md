# ADR-0008: a watchdog recovers crashed deployments through the lifecycle

- Status: accepted; amended by [ADR-0009](0009-output-quality-watch.md)
- Date: 2026-11-06

## Context

A deployment is `ready` once a load proved it, and nothing looked again until
an operator refreshed or reconciled. When the head rank of a two-node vLLM
deployment exited under load, the controller kept the deployment `ready`
while the endpoint refused connections and the worker rank still held its
memory. Recovery took an operator to notice, reconcile, recover baseline and
load again, and the container's exit code and logs were lost to the stop.

## Decision

1. A watchdog thread in the controller service probes every deployment the
   controller last left serving (desired `ready`, owned by a load or update
   that succeeded, or that failed but left it `ready`) every 30 seconds with
   the full readiness check. Three
   failed probes in a row, at least 60 seconds from first to last and with
   every node reachable, confirm a crash.
2. The watchdog never runs a remote command that changes anything. It queues
   journaled operations on the lifecycle worker, with requester `watchdog`
   and idempotency keys derived from the operation they follow.
3. A new `crash-report` operation takes the deployment's leases, confirms the
   crash, writes a support bundle of every rank's state, exit code and last
   log lines before anything is stopped, records the deployment `failed`
   (desired stays `ready`) and decides whether a recovery may follow.
4. Recovery is opt-in per profile (`auto_recover`) and limited to
   `shared-vllm` profiles. It needs positive evidence of a crash: a rank
   where nothing of the deployment runs any more, and nothing but the
   deployment's own process on the others. A foreign or unidentified
   workload, or an endpoint that fails while every rank runs, is reported
   and left alone. Recovery is the existing baseline recovery followed by an
   ordinary load of the same profile revision on the same nodes, with
   preflight, readiness and the completion canary. The worker checks the
   report's ownership, the pause switch and the crash evidence again when
   the attempt runs.
5. A profile's budget (`max_attempts` within `window_s`) bounds the
   recoveries; the second waits `backoff_s` and each further one twice as
   long. A spent budget, a changed profile revision, or an attempt that ends
   `needs-attention` stops the watchdog for that deployment until an
   operator loads it again.
6. Nothing is queued while the watchdog is paused (a switch that survives
   restarts), while any lifecycle operation is queued or running, or while
   any lease is held. Once an operator's operation uses the nodes after a
   crash report, the watchdog leaves the deployment to the operator.

## Alternatives

- **Detect only, recover by hand.** Safe, but leaves a shared endpoint down
  until someone notices; the incident that prompted this ran unattended work.
- **Restart the crashed container in place.** Rejected: it bypasses the
  identity checks, the release proof and the completion canary, and a TP
  placement with one surviving rank cannot be restarted rank by rank.
- **One combined restart operation.** Rejected: separate `recover` and `load`
  operations reuse the verified code paths unchanged and look in the journal
  exactly like the manual procedure.
- **Retry without a budget.** Rejected: a load pattern that crashes the model
  would make it crash-loop forever.

## Consequences

- The controller may now stop and start a model without an operator request,
  but only a model it proved crashed, only for opted-in profiles, and only
  through operations an operator could have queued.
- A model stopped outside the controller looks like a crash; operators pause
  the watchdog before working on the nodes by hand.
- `failed` is now also written for a crashed deployment that is still desired
  `ready`, and operation resources expose `requester`.
