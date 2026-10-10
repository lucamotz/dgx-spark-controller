# ADR-0005: generated recipes

- Status: accepted
- Date: 2026-10-10
- Amended: 2026-10-10 (cleanup, delete, restart reattach, lingering)

## Context

Hand-reviewed profiles are safe but slow to produce: each one pins scripts,
images and identity for one model on one cluster. Operators want to try a new
model or vLLM release from a Hugging Face link without writing scripts, and
without giving the browser a way to run arbitrary commands on the nodes. The
controller's existing safety contract (pinned script hashes, exact process
identity, readiness, memory recovery, journaled operations) must apply
unchanged to whatever the builder produces.

## Decision

1. **Release-only runtimes pinned by digest.** The builder offers only
   published vLLM releases from GitHub and the official `vllm/vllm-openai`
   image for that tag. The tag is resolved on Docker Hub to its `linux/arm64`
   manifest digest, and only the immutable reference
   `vllm/vllm-openai@sha256:<digest>` is stored. Every node pulls
   byte-identical content and reports the same image ID.
2. **Pinned model revisions.** A model link is resolved to the 40-character
   commit the Hub reports. Weights are downloaded at that commit and verified
   by file size; the weight index hash is pinned in the profile.
3. **One fixed template with a data contract.** `prepare.sh`, `start.sh` and
   `stop.sh` in `controller/recipes/templates/vllm-release/` are packaged with
   the controller, installed unchanged, and pinned by SHA-256. Their only input
   is a `KEY=value` environment file. The scripts parse it as data against an
   explicit key allowlist, and every value must match `[A-Za-z0-9._:/@+-]`. The
   browser can choose only bounded settings; no flag, path, environment
   variable or command is free-form.
4. **Per-node launch.** Generated profiles use `launch_mode: per-node`: the
   controller starts the same pinned `start.sh` on each node over that node's
   own SSH session, with a per-node overlay for rank, address and model root.
   Nodes never need SSH access to one another, and a single node is a valid
   placement.
5. **Prepare is separate from load.** A journaled `prepare` operation pulls the
   image and downloads the weights on its own worker without leases. It never
   starts, stops or adopts a model process. Loading the result uses the
   ordinary orchestrated lifecycle.
6. **A new profile per prepare.** Every successful prepare writes a new
   profile `g-<alias>-<6 hex>` with `origin: generated` to the data
   directory. A running deployment keeps the exact contract it was started
   with; switching to the new profile goes through stop, verify and start.
7. **Unqualified by default.** Official images are not qualified on GB10
   (`sm_121`), so generated profiles are always reported as
   `qualified: false` and carry `generated-recipe-unqualified`.
8. **Prepare outlives the controller.** Prepare requires lingering for the
   node user, so its `systemd --user` unit survives the SSH session; it fails
   with `linger_required` before any node change otherwise. Quitting the
   controller leaves the unit running, and the next start reattaches and
   finishes it. Only an unreadable node state ends it `needs-attention`.
   Recipe operations are therefore resolved by the recipe service, never by
   lifecycle reconciliation.
9. **Cleanup is bounded and explicit.** A failed or cancelled prepare removes
   only its own recipe directory and keeps resumable, shareable downloads.
   Deleting a prepared recipe is a journaled `delete-recipe` operation on the
   recipe worker: refused while a profile is in use, holding node leases,
   removing node directories before profile files, and removing weights,
   image and cache volume only on request and only when nothing else on that
   node references them. Remote removal accepts only paths and references of
   the exact shapes the builder produces (see [Security](../SECURITY.md)).

## Alternatives

- **Let the browser edit launch flags or scripts.** Rejected: it would turn the
  UI into a remote shell and break the "allowlisted script is the source of
  truth" contract.
- **Mutable image tags or nightly builds.** Rejected: nodes could run
  different bytes, and identity checks could not be pinned.
- **Render a new script per recipe.** Rejected: every script would need review.
  One template with data-only input is reviewed once.
- **Head-script launch for generated recipes.** Rejected: it requires SSH from
  the head to the workers and an operator-maintained launcher.
- **Update the profile in place on re-prepare.** Rejected: a serving deployment
  would stop matching its own identity and become unmanaged.
- **Download weights on the controller host.** Rejected: the host may be a
  laptop, and gated-model tokens belong on the nodes.
- **Cancel prepares on controller exit, or mark them interrupted on restart.**
  Rejected: a multi-hour download would be interrupted whenever the desktop
  app quits, and the operator would have to prepare again by hand.
- **Delete weights and images by default.** Rejected: they are shared by
  every recipe of the same model revision and release, and re-downloading
  them is the most expensive step.

## Consequences

- Operators can try supported models and releases quickly, and every run is
  traceable to a commit, an image digest and a template hash.
- Custom builds, patches, GGUF weights and non-release images still need a
  hand-reviewed profile.
- Generated profiles, images and weights accumulate until the operator
  deletes the recipe; a failed prepare cleans up only its own directory.
- A delete waits behind a running prepare on the shared recipe worker.
- Changing a template changes its hash; existing generated profiles keep the
  copies installed on the nodes when they were prepared.
- Multi-node recipes depend on the release's `vllm serve --nnodes` support,
  which prepare checks.
