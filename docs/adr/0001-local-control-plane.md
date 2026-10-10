# ADR-0001: local control plane

- Status: accepted
- Date: 2026-07-31

## Context

The first proof of concept controlled one DGX Spark on a local network from a
single workstation. Internet exposure, multi-user authentication and a general
scheduler are non-goals.

## Decision

Run the controller locally and expose a loopback-only API. Reach nodes through
explicit transport interfaces. Keep profile and configuration records
versioned and reviewable.

## Alternatives

- Kubernetes or Slurm: adds operational surface and does not fit a small
  cluster of Sparks.
- Remote web service: widens the trust boundary and secret exposure.
- Client applications owning model startup: preserves lifecycle races.

## Consequences

The controller is easy to inspect and survives UI restarts, but local
availability, backups and any future authentication remain the operator's
responsibility. Model endpoints keep running when the local window closes.

## Later updates

- The controller runs on Linux and macOS from one codebase, with a
  cross-platform launcher (`controller/launcher.py`) and per-user directories
  (`controller/paths.py`).
- The setup wizard and recipe builder kept the loopback-only boundary: they
  add routes to the same local API and never accept commands from the browser.
