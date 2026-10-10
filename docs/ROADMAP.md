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

## Not yet verified

- The Linux desktop window (GTK/Qt) on a Linux workstation; CI covers the
  service, not the GUI.
- Hosts with more than two nodes, or gated models with a node-side token.

## Next steps

1. **Switch across placements.** Switching from a two-node deployment to a
   one-node recipe is refused (`partial_cluster_conflict`); today the operator
   unloads first. Take leases on the union of the new placement and the
   replaced deployment's nodes, and name the extra stop in the confirmation.
2. **Clean up after prepare.** A failed prepare leaves its working directory on
   the node, and nothing removes old images, weights, or generated profiles.
   Add an explicit delete flow that removes all three.
3. **Lingering.** Prepare and launcher units live in the user's `systemd`
   manager. Without lingering they survive only while some session of that
   user is open. The wizard warns; prepare could also run its download as a
   detached container so it never depends on a session.
4. **CDI-only GPU nodes.** The identify probe warns "no NVIDIA runtime" on
   nodes that expose the GPU through CDI only, although `--gpus all` works
   there. Treat registered `nvidia.com/gpu` CDI devices as GPU-capable.
5. **One codebase for the private deployment.** Point the private controller at
   this package and keep only its reviewed profiles, node config, and evidence
   in a private overlay directory (`DGX_CONTROLLER_HOME` or `--profiles`).
6. **Prepare robustness.** Reattach to a prepare unit that is still running
   after a controller restart instead of marking it interrupted, and show
   per-node download progress bars.
7. **Completion canary.** The profile contract already has a
   `completion_canary`; execute it after readiness and record the result as
   qualification evidence for generated profiles.
8. **Web UI polish.** Default the SSH user to the local user name, show the
   memory a recipe reserves rather than its weight size, keep long recipe
   names clear of the origin badge, and extend linting to the JavaScript
   modules with browser-level tests for the setup and recipe pages.
