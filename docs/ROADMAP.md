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

## Not yet verified

- Discovery and identification against real Sparks on a real LAN.
- Prepare and load of a generated recipe on GB10 hardware: whether the official
  `vllm/vllm-openai` arm64 release images run on sm_121, and the multi-node
  `--nnodes` path over the ConnectX-7 link.
- The Linux desktop window (GTK/Qt) on a Linux workstation; CI covers the
  service, not the GUI.

## Next steps

1. **Live end-to-end test on the two-node cluster** (after the current
   model-change work on that cluster finishes, and inside an approved
   maintenance window, because loading a generated recipe stops the model that
   is serving):
    1. Run the controller with a separate `DGX_CONTROLLER_HOME` so the existing
       private controller configuration is untouched.
    2. Scan and identify both nodes, confirm host keys, check the direct link,
       and save. All of this is read-only on the nodes.
    3. Prepare a small model (for example Qwen3-8B with vLLM v0.31.0) on one
       node, then on two nodes. Prepare does not interrupt serving.
    4. Load, verify readiness and a chat completion, unload, and restore the
       production model through its reviewed profile.
2. **GB10 runtime qualification.** If the official release images do not run
   on sm_121, add a second runtime source: build a thin image on the node from
   the GitHub release wheel (the release assets carry SHA-256 digests), or
   allowlist specific digest-pinned GB10 images such as NVIDIA NGC vLLM.
3. **One codebase for the private deployment.** Point the private controller at
   this package and keep only its reviewed profiles, node config, and evidence
   in a private overlay directory (`DGX_CONTROLLER_HOME` or `--profiles`).
4. **Prepare robustness.** Reattach to a prepare unit that is still running
   after a controller restart instead of marking it interrupted, show per-node
   download progress bars, and garbage-collect old generated profiles, images,
   and weights with an explicit delete flow.
5. **Completion canary.** The profile contract already has a
   `completion_canary`; execute it after readiness and record the result as
   qualification evidence for generated profiles.
6. **Quality bar for the web UI.** Extend linting to the JavaScript modules
   (eslint/prettier) and add browser-level tests for the setup and recipe pages.
