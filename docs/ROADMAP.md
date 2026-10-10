# Scope and roadmap

## Version 0.2.0: what changed

Version 0.2.0 is the first public release. It started from a private,
macOS-only controller that managed one hand-qualified model on a two-node
Spark cluster.

| Area | Change |
| --- | --- |
| Platforms | One codebase for Linux and macOS: per-OS config/data/log locations (`controller/paths.py`), a cross-platform launcher (pywebview window or browser), a macOS app-bundle builder, and a Linux desktop entry plus `systemd --user` service installer. CI runs the suite on Ubuntu and macOS with Python 3.12 to 3.14. |
| Setup mode | The service starts with no configuration and hot-reloads when nodes or profiles appear (`controller/host.py`). |
| Discovery and onboarding | The wizard scans private subnets (TCP 22, SSH banners, reverse DNS, mDNS, model endpoints) and classifies likely Sparks. You confirm SSH host-key fingerprints, which are pinned in a controller `known_hosts`. Nodes are identified with a fixed read-only probe and the direct ConnectX links are checked; then `nodes.json` is written. |
| Recipe builder | Hugging Face link parsing and pinned revision metadata, a memory-fit estimate, vLLM GitHub releases, and arm64 image digests from Docker Hub. Drafts hold bounded settings. A journaled *prepare* operation pulls the image and downloads the weights on every node, verifies them, installs one hash-pinned template, and registers a generated profile. |
| Lifecycle | `per-node` launch for shared vLLM recipes: the controller starts each rank over its own SSH session, workers first, so nodes need no SSH access to each other. Single-node vLLM recipes are supported. The reconciler leaves live prepare operations alone. |
| Quality | vLLM-style tooling: ruff, mypy, typos, markdownlint, shellcheck, actionlint, SPDX headers, pytest, DCO sign-off, and pre-commit in CI. |
| Removed | Private evidence, qualification artifacts, host topology, deployment recipes, the ecosystem-specific front-line probe, and the PyObjC-only desktop shell. |

## Verified

- Unit and integration tests, including fake-mode end-to-end flows: setup mode
  → scan → trust → identify → save → reload → recipe → prepare → load → ready.
- Contract tests that run the real template scripts with a stub `docker`. They
  check that the generated identity fragments match the launched command
  exactly, that injected values are rejected rather than executed, and that
  `stop.sh` refuses containers it does not own.
- The catalog against the live Hugging Face, GitHub, and Docker Hub APIs
  (read-only): Qwen3-8B, a gated Llama model, and gpt-oss-120b resolve, and
  `v0.31.0` resolves to a pinned arm64 digest.

### Live end-to-end test (2026-10-10)

Two-node GB10 cluster: DGX OS, Docker 29.6, driver 580, direct ConnectX-7
links. The existing production deployment was unloaded for the maintenance
window and restored afterwards; the controller ran with a separate
`DGX_CONTROLLER_HOME`.

- Discovery on a /24 found both Sparks (reverse DNS, mDNS, SSH banner, model
  endpoint). Host-key fingerprints matched the operator's existing
  `known_hosts`; identification and both direct links passed; `nodes.json` was
  written and the controller reloaded without a restart.
- Recipe: Qwen3-8B at a pinned commit with the official
  `vllm/vllm-openai` v0.31.0 arm64 image. Prepare pulled the image and
  downloaded 15.3 GiB per node in about 3.5 minutes; re-prepare with the files
  present takes seconds. Prepare also ran while a model was serving.
- Tensor parallel across both nodes (`--nnodes 2`, per-node launch) reached
  readiness in about 2 minutes 40 seconds. Chat, the `qwen3` reasoning parser,
  and `hermes` tool calls worked. Decode was about 22 tokens/s on two nodes and
  14 tokens/s on one.
- A switch between two-node recipes stopped the old ranks, verified memory
  release, and started the new ones. Unload, a single-node load, and a
  controller restart with a model running all behaved as specified.
- Every test artifact (image, cache volume, weights, recipe directory) was
  removed from the nodes afterwards.

Defects the live test found and this release fixes:

- With no profile loaded, a node running a GPU container the controller did
  not start was shown as `idle`. Observation now reports foreign GPU
  containers, host GPU processes, and model endpoints in the node's port range,
  and the node is `unmanaged`.
- vLLM v0.31.0's `vllm --version` needs a GPU, so prepare failed. Prepare now
  reads package metadata in a GPU-less, memory-capped container.
- Nodes can use different Docker image stores, which report different image
  IDs for the same pinned pull. Head and worker image IDs are now pinned
  separately.
- A user `systemd` manager that is `degraded` (an unrelated unit failed) failed
  preflight on worker ranks; it is now accepted.
- After a switch, the replaced deployment stayed `ready` in the journal until
  the next reconcile. It is now recorded as stopped once its memory release is
  verified.
- The UI showed one recipe's context length and placement for every recipe
  sharing the same weights.

### Since the live test

These close the follow-ups the live test raised. They are covered by fake-node,
stub-script, and frontend tests, but have not yet run against the live cluster.

- **Switch across placements.** A load replaces every managed deployment it
  finds on a selected node, on all of that deployment's nodes, so a two-node
  model can be replaced by a one-node recipe in one operation. The extra nodes
  are leased and observed before any mutation; a node owned by any other
  deployment still refuses the switch. See
  [ADR-0006](adr/0006-whole-deployment-switch.md).
- **Completion canary.** After readiness, one bounded chat completion must
  finish with generated tokens, or the load rolls back. Evidence holds only
  status, latency, token count, and finish reason.
- **Recipe cleanup and delete.** A failed or cancelled prepare removes its
  working directory. A journaled delete removes a recipe's generated profiles
  and node files, and optionally weights, image, and cache volume when nothing
  else uses them.
- **Prepare across restarts.** Quitting the controller no longer cancels a
  running prepare; on startup it reattaches. Prepare requires lingering on
  every node and shows a progress bar per node.
- **Setup and catalog.** CDI-only GPU nodes are recognised; scanned host keys
  are compared with the operator's `~/.ssh/known_hosts`; the SSH user defaults
  to the local login; model cards show the memory a recipe reserves.
- **Tooling.** ESLint covers the web UI and its tests; the operation progress
  bar no longer depends on an inline style the CSP blocks.

## Not yet verified

- The Linux desktop window (GTK/Qt) on a Linux workstation; CI covers the
  service, not the GUI.
- Hosts with more than two nodes, or gated models with a node-side token.
- The changes listed under *Since the live test* on real nodes, in
  particular a two-node to one-node switch, recipe delete with artifact
  removal, and reattaching to a prepare after a controller restart.

## Next steps

1. **Second live test.** Re-run the end-to-end test in a maintenance window to
   cover the items above, then remove every test artifact with the new delete
   flow.
2. **One codebase for the private deployment.** Point the private controller at
   this package and keep only its reviewed profiles, node config, and evidence
   in a private overlay directory (`DGX_CONTROLLER_HOME` or `--profiles`).
3. **Single-profile delete.** Profiles whose draft was deleted before recipe
   delete existed have no card; add a route that deletes one generated
   profile.
4. **Split the recipe service.** `controller/recipes/service.py` holds
   prepare, delete, and reattach; move delete into its own module.
