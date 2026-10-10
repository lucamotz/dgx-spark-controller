# Model profiles

A profile is the only thing the controller can load. It is a versioned
contract: which nodes may run the model, which pinned scripts start and stop
it, how its process is identified, how readiness is proven, and which memory
and safety gates apply. A model file or a recipe draft is never runnable by
itself. The JSON format is in [Configuration](CONFIGURATION.md#profile-document).

## Reviewed and generated profiles

| | Reviewed | Generated |
| --- | --- | --- |
| Written by | an operator, by hand | the recipe builder after a successful prepare |
| Directory | `<config dir>/profiles/` | `<data dir>/profiles/` |
| `origin` | `reviewed` (default) | `generated` |
| Launch mode | `head-script` or `per-node` (vLLM), or a single-node llama.cpp script | always `per-node` vLLM |
| Runtime | anything you pin: custom images, patched builds, your own scripts | official `vllm/vllm-openai` release images only |
| API `qualified` | `true` | `false` |
| Changes | edit the file and increment `revision` | never edited; preparing again creates a new profile |

Both kinds pass the same parser and validation and are loaded through the same
lifecycle. Profile IDs must be unique across both directories.

## Qualification

`qualified` records who vouches for a profile, not whether a load succeeded.

- A **reviewed** profile is qualified because a person wrote it, pinned its
  scripts, image and identity, and is responsible for having tested it on
  their hardware. The controller cannot verify that claim.
- A **generated** profile is always unqualified. Official vLLM images are
  built and tested upstream for other targets and are not yet qualified on
  GB10 (`sm_121`). Generated profiles carry the invariant
  `generated-recipe-unqualified`, and the UI says so. A successful load proves
  process identity, health, alias and one completion for that load only; the
  operation's `result.completion_canary` records that evidence, and it does
  not change the flag.

The controller has no promotion workflow. To run a generated configuration as
a reviewed profile, write your own profile in `<config dir>/profiles/` after
your own testing.

## How a generated profile is built

After every node reports a successful prepare, `controller/recipes/render.py`
builds the profile from the draft and the facts the nodes reported:

- **Identity.** `profile_id` is `g-<alias>-<6 hex>` (unique per prepare),
  `revision` 1, display name `<recipe name> (vLLM <release>)`, and
  `provenance` with the draft and prepare IDs, Hugging Face repository and
  commit, vLLM release, image reference, the version reported by the image,
  and the template name.
- **Artifact.** `safetensors` at `<model root>/<org>--<name>-<commit[:8]>`,
  with `config.json` and the weight index as required files, the total weight
  size, and the index's SHA-256 as `manifest_sha256`.
- **Placement.** `shared-vllm` with `node_count` equal to the selected nodes,
  `allowed_node_ids` exactly those nodes, and the head as `control_node_id`.
- **Runner.** `start.sh` and `stop.sh` in
  `~/.local/share/dgx-spark-controller/recipes/<profile-id>/`, pinned to the
  SHA-256 of the packaged templates; `recipe.env` as the environment file;
  `required_environment` with every profile-wide value (image, image ID,
  model directory, alias, port, tensor-parallel size, master address and
  port, and the settings); one `node_environment` overlay per node
  (`NODE_RANK`, `VLLM_HOST_IP`, `MODEL_ROOT`); container name
  `dgxc-<profile-id>`; `image_digest` set to the digest-pinned image; and an
  `expected_recipe_commit` derived from the script hashes and environments.
- **Readiness.** Executable `/usr/local/bin/vllm`, the exact `vllm serve`
  arguments as required command fragments, the served model name as the
  exact alias, the recipe's startup timeout, a 10-second poll interval, and
  an enabled completion canary: the prompt `Reply with OK.`, at most 16
  tokens, and a 120-second timeout that tolerates first-request warmup.
- **Safety.** Script hashes required, a free-memory gate before start and
  after stop of `gpu_memory_utilization` × the smallest node's memory, at most
  8 GiB swap, a 180-second recovery timeout, and the invariants
  `generated-recipe-unqualified`, `image-digest-pinned`,
  `model-revision-pinned`, `start-and-stop-script-sha256-pinned`,
  `per-rank-image-id-pinned`, `served-alias-exact-match`,
  `health-http-200-required` and `memory-recovery-required`.

The rendered profile is validated before anything is installed; a profile the
parser would reject fails the prepare with `generated_profile_invalid`.

### Why every prepare creates a new profile

The deployment ID (`dep-<profile-id>`), the container name, its ownership
label and the ownership marker all derive from the profile ID, and readiness
compares the running container against the profile's image ID, arguments and
environment. If preparing again rewrote an existing profile, a deployment that
is still serving would stop matching its own contract: it would be reported as
unmanaged, or a stop could be checked against the wrong identity. A new
profile ID leaves the running deployment untouched. Loading the new profile
then replaces it through the normal stop, verify and start sequence. See
[ADR-0005](adr/0005-generated-recipes.md).

Old generated profiles are not deleted automatically. Remove a file from
`<data dir>/profiles/` only when no deployment uses it, then restart the
controller.

## Example reviewed profile

`config/examples/profiles/mimo-v26-flash-rl-bf16-884736-c2-shared-vllm.json`
is a hand-reviewed `head-script` shared-vLLM profile for a two-node cluster. It
shows a fully pinned reviewed contract: paired start and stop script hashes, a
recipe source revision, per-rank image IDs, a model index hash, exact
environment values, and a long list of invariants. Its scripts, image and
paths exist only on the nodes it was written for, so it is a reference for the
format, not a profile you can load elsewhere. Fake mode can load it with the
cluster node example.

## Readiness contract

Every profile requires all of the following; no check substitutes for
another:

1. Process or container identity: the pinned image (and, when pinned, the
   per-rank image ID), the ownership marker or label, and every required
   command fragment in the observed command.
2. `GET /health` returns HTTP 200.
3. `GET /v1/models` lists exactly the profile's alias.

A profile may add a fourth gate, `readiness.completion_canary`: one bounded
chat completion against the exact alias, which must return HTTP 200, a
`stop` or `length` finish reason and at least one completion token. Generated
profiles enable it; reviewed profiles keep their configured value, which is
disabled unless set. Enabling or changing it is a readiness change, so revise
the profile as below. The canary runs on every load of the profile that
starts it, never on a restore during rollback or for a placement that is
already ready. See [Operations](OPERATIONS.md#completion-canary).

After an unload the inverse is required: the process or container is gone,
the endpoint is closed, the alias has disappeared, free memory reaches the
recovery threshold before another model starts, and swap stays within its
bound.

## Revising a reviewed profile

A change to a script, image, build, argument, readiness or safety value is
never a silent edit:

1. Record the new script hashes, image identity and any changed evidence.
2. Check whether the alias, artifact files, memory thresholds or readiness
   contract changed.
3. Increment `revision` and update tests and documentation in the same
   change. Keep the previous file available for rollback.
4. Run the offline checks, then test the new revision on your nodes at a time
   when an interruption is acceptable.
