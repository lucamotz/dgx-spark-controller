# ADR-0009: the watchdog recovers deployments whose output degrades silently

- Status: accepted
- Date: 2026-11-06

## Context

The watchdog of [ADR-0008](0008-watchdog-crash-recovery.md) detects a
deployment that stops serving. A two-node vLLM deployment with speculative
decoding once kept answering `/health`, `/v1/models` and the completion
canary for hours while its output turned into garbage: replies that looped
until the output limit, foreign-script tokens spliced into words and shell
commands, characters dropped from identifiers. Nothing reacted until a
client noticed. In that run the draft acceptance (accepted over drafted
tokens) fell from its usual range to a few percent within minutes of the
fault and stayed there, and a fixed floor separated every healthy
five-minute window from every degraded one. When it happened again, the
acceptance collapsed as well, but the clients' output was already wrong a
few minutes before the first low window, and a temperature-0 request to copy
a few synthetic code lines came back altered in every run while that
request's own draft acceptance stayed high.

## Decision

1. A profile may opt in with `quality_watch`, a profile policy like
   `auto_recover` and not part of the deployment contract. Only vLLM
   profiles may enable it, and it needs an explicit floor.
2. On every probe that finds the deployment ready, the watchdog reads the
   serving rank's `/metrics` once per `window_s` and computes the window's
   draft acceptance from the two cumulative counters. A window with fewer
   than `min_draft_tokens` drafted tokens is not counted, and a restarted
   server (a counter that went down or a new process start time) starts the
   watch over. `consecutive_windows` counted windows in a row below
   `min_acceptance_percent` queue a `quality-report`.
3. The `quality-report` is a journaled operation like the `crash-report`:
   it takes the deployment's leases, writes the same support bundle before
   anything is stopped, and records the deployment `failed`, still desired
   `ready`. The server keeps running.
4. A recovery after a degradation is opt-in on its own
   (`quality_watch.recover`) and needs `auto_recover.enabled`, whose attempt
   budget and backoff it shares. While every rank still runs it starts
   `hold_s` after the report at the earliest, so the degraded server can be
   inspected live. It is the
   existing baseline recovery and load. Because every rank still runs, the
   recovery needs, instead of a gone rank, every node to answer and to run
   nothing but the deployment's own processes; the worker checks that, the
   report's ownership and the pause switch again when the attempt runs.
5. The crash and quality reports also save each rank's GPU faults from the
   kernel log (read with `journalctl -k -b`, which members of `adm` or
   `systemd-journal` can read without root, unlike `dmesg`) and the serving
   rank's metrics.
6. A profile may add an active `copy_probe`: every `interval_s` one
   temperature-0 completion asks the model to copy the profile's synthetic
   lines exactly, and `consecutive_failures` replies that miss a line trip
   the same quality report. A probe without a reply or content is
   inconclusive. The reply is judged and discarded. The probe runs on its own
   worker thread, because it can wait behind other requests for minutes,
   and the readiness probes must not.

## Alternatives

- **Judge free-form probe completions.** Rejected: judging free text needs
  a model or a heuristic per workload, and a short canary that only checks
  that a completion finishes still passed on the degraded server. A verbatim
  copy of synthetic lines needs neither, so the copy probe compares exact
  lines; it still costs a request slot on a shared endpoint, so it is
  optional and infrequent.
- **Watch finish reasons or output length.** Rejected: runaway replies show
  up only after the output limit, and long replies are normal for agents.
- **Recover at the first low window.** Rejected: one window can be an
  outlier; consecutive windows and a drafted-token minimum keep idle and
  prefill-only periods from counting.
- **Recover without a hold.** Rejected as the only option: a degraded server
  is evidence that a restart destroys, so the hold is configurable and
  pausing the watchdog keeps the server for as long as needed.

## Consequences

- The controller may now stop a model that still passes readiness, but only
  for profiles that enable both `quality_watch.recover` and `auto_recover`,
  only after the hold, within the shared budget, and only when no rank runs
  anything it cannot identify as the deployment's own.
- Copy-probe failures stop a model the same way, under the same
  conditions. Each probe takes one request slot of the shared endpoint
  every `interval_s`.
- The floor is model- and drafter-specific; a wrong floor reports or
  restarts a healthy server, which the budget bounds.
- The signal does not catch every kind of bad output: a degenerate
  repetition loop is easy to draft and can raise the acceptance.
- A report without `recover` ends `needs-attention` and owns the deployment,
  so, as after a crash report that needs attention, the watchdog stops
  watching it, crashes included, until an operator loads it again.
- ADR-0008's "only a model it proved crashed" now also admits a model
  whose own metrics show degraded output.
