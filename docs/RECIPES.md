# Build and load a recipe

The **Recipes** page turns a Hugging Face model link and a released vLLM
version into a runnable profile. The flow has three separate steps, plus a
delete that cleans up after it:

1. **Inspect and save** a draft. Only the controller host talks to Hugging
   Face, GitHub and Docker Hub. Nothing happens on a node.
2. **Prepare** the draft. Every selected node pulls the pinned runtime image
   and downloads the pinned weights. The model that is currently serving is
   not touched.
3. **Load** the generated profile through the normal lifecycle: preflight,
   stop the conflicting deployment, verify memory recovery, start, verify
   readiness.
4. **Delete** a recipe you no longer need. A prepared recipe is deleted by a
   journaled operation that removes its generated profiles and node files,
   and optionally the weights and image that nothing else uses.

Finish [Setup](SETUP.md) first; a draft can only name configured nodes.

## 1. Inspect a model

Paste a model link and press **Inspect**. Accepted forms:

- `https://huggingface.co/<org>/<name>` (also `hf.co`, without scheme, or with
  `/tree/<rev>`, `/blob/<rev>/...`, `/resolve/<rev>/...` or `/commit/<sha>`)
- `<org>/<name>` or `<org>/<name>@<revision>`

The controller resolves the revision to its 40-character commit and pins the
draft to it. It reads the model API document and `config.json` at that commit
and shows the weight size and format, quantization, parameter count, maximum
context, architecture, license, and whether the repository is gated. It also
estimates the fit for one node up to the number of configured nodes (at most
four): weights split evenly across nodes must leave at least max(4 GiB, 5%)
of 85% of 128 GiB per node for KV cache and activations. The estimate is
arithmetic, not a qualification.

For gated or private repositories, set `HF_TOKEN` in the controller's
environment so it can read `config.json`, and run `hf auth login` on every
node for the download. See [Configuration](CONFIGURATION.md#environment-variables).

## 2. Choose a runtime, placement and settings

- **vLLM release.** The list shows published GitHub releases of
  `vllm-project/vllm` (no nightlies). The controller resolves the official
  `vllm/vllm-openai` image tag on Docker Hub to its `linux/arm64` manifest
  digest and stores the immutable reference
  `vllm/vllm-openai@sha256:<digest>`. A release without an arm64 image fails
  with `runtime_image_unavailable`.
- **CUDA build.** `default` or `cu129` in the UI; the API also accepts
  `cu130`. These map to the image tags `vX.Y.Z`, `vX.Y.Z-cu129` and
  `vX.Y.Z-cu130`.
- **Nodes.** One to eight configured nodes, used as one tensor-parallel group.
  The head node (rank 0) serves the endpoint. All selected nodes must report
  the same home directory and have the same first model root.
- **Served model name.** The exact alias clients use and readiness checks,
  `[a-z0-9][a-z0-9._-]{0,63}`. The default is derived from the model name.

Settings are bounded; unknown keys are rejected:

| Setting | Default | Allowed values |
| --- | --- | --- |
| `max_model_len` | model maximum, capped at 32,768 | 512–4,194,304 and at most the model's `max_position_embeddings` |
| `max_num_seqs` | 8 | 1–1,024 |
| `max_num_batched_tokens` | 8,192 in the UI; the API uses max(8,192, min(default context, 16,384)) | 256–1,048,576 and at least min(context, 2,048) |
| `gpu_memory_utilization` | 0.85 | 0.30–0.95, two decimals |
| `kv_cache_dtype` | `auto` | `auto`, `fp8`, `fp8_e4m3`, `fp8_e5m2` |
| `enable_prefix_caching` | `true` | boolean |
| `enforce_eager` | `false` | boolean |
| `trust_remote_code` | `true` only if the repository ships custom code | boolean; required for such repositories |
| `tool_call_parser` | none | a vLLM parser name, `[a-z0-9_]{1,40}`; also enables auto tool choice |
| `reasoning_parser` | none | a vLLM parser name, `[a-z0-9_]{1,40}` |
| `port` | 8000 in the UI; the API picks 8000, 8888 or 8080, whichever every node allows | 1,024–65,535, inside every selected node's `endpoint_ports` |
| `startup_timeout_s` | 2,400 | 120–14,400 (API only) |

Enable `trust_remote_code` only after reviewing the repository's code: vLLM
then runs that code on every node.

**Save recipe** stores the draft as `<data dir>/recipes/r-<id>.json`. Editing a
prepared draft resets it to `draft`; **Edit as new** copies it. Deleting a
draft that never produced a profile removes only that file; see
[Delete](#5-delete) for prepared recipes.

## 3. Prepare

**Prepare** starts a durable `prepare` operation; follow it on the Operations
page. It runs on its own worker, so a long download never blocks loading or
unloading the current model, and it never starts, stops or adopts a model
process.

Before it changes anything on a node, the controller checks that lingering
is enabled for the node user on every selected node
(`loginctl show-user "$(id -un)" -p Linger --value` must print `yes`). Without
lingering, the user's `systemd` manager, and with it the prepare unit, stops
when the controller's SSH session ends. A node without lingering fails the
prepare with `linger_required`; the remediation names the node and the exact
command, for example `sudo loginctl enable-linger spark`. The setup wizard
already warns about this.

On each selected node the controller then:

1. Creates `~/.local/share/dgx-spark-controller/recipes/<profile-id>/` (mode
   `0700`) and installs `prepare.sh`, a `prepare-<id>.env` data file and the
   expected weight-file list, verifying each file's SHA-256 after transfer.
2. Starts `prepare.sh` in a transient `systemd --user` unit
   `dgx-prepare-<id>.service`, so it survives the SSH session.

`prepare.sh` then:

1. Parses its environment file as data (allowlisted keys and characters).
2. Skips the download if every expected weight file already has the expected
   size. Otherwise it requires free space of the weight size plus 5% plus
   2 GiB in the model root.
3. Runs `docker pull` on the digest-pinned image and records its local image
   ID. It reads the vLLM version from the installed package metadata in a
   short container without GPU access, network, or more than 1 GiB of memory,
   and checks that `/usr/local/bin/vllm` exists. The `vllm` CLI itself is not
   run: recent releases detect the GPU while building their argument parser,
   and prepare never claims the GPU, so it cannot disturb a model that is
   still serving.
4. For more than one node, checks that the release's engine arguments include
   `--nnodes`, and fails otherwise.
5. Downloads the pinned revision with `huggingface_hub.snapshot_download`
   inside the pinned image (capped at 4 GiB of memory), as the node user, into
   `<model root>/<org>--<name>-<commit[:8]>`. The node's token file is mounted
   read-only when present.
6. Verifies every weight file's size and records the SHA-256 of
   `model.safetensors.index.json` (or `config.json`).

The controller polls every node about every 5 seconds and journals progress
when a node's step changes and at least every 15 seconds. Each progress event
carries `progress_by_node`: the step (`checking`, `pulling`, `inspecting`,
`downloading`, `verifying`, `done`), the state, the bytes in the model
directory and the expected weight size. The recipe card shows one progress bar
per node while the recipe is preparing, and the operation resource exposes the
latest values (see [API](API.md#recipe-routes)). It requires all nodes to
report the same vLLM version and manifest hash. Local image IDs may differ between the head and the
workers: the containerd image store reports the manifest digest and the classic
`overlay2` store the config digest for the same pinned pull, so the head and
the workers are pinned separately. All workers must agree. It then installs the pinned `start.sh`,
`stop.sh`, the rendered `recipe.env` and a recipe-revision file next to the
prepare files, writes the generated profile to `<data dir>/profiles/`, and
marks the draft `prepared`. The controller reloads its profile registry as soon
as no lifecycle operation is running.

Each successful prepare registers a new profile ID,
`g-<alias>-<6 hex characters>`, so a deployment that is still serving keeps
the exact contract it was started with. See
[ADR-0005](adr/0005-generated-recipes.md).

**Cancel** a running prepare from the Operations page
(`POST /api/v1/operations/{id}/cancel`). The controller stops the prepare
units and any download container on every node. A prepare fails after 6
hours, or when a node stops reporting progress for six consecutive polls.

After a failed or cancelled prepare, once its units have stopped and no
profile was registered from it, the controller removes exactly that prepare's
directory, `~/.local/share/dgx-spark-controller/recipes/<profile-id>/`, on
every node it installed files on (event `prepare_directory_removed`). If a
node is unreachable or the unit still runs, the directory is kept and the
event `prepare_cleanup_incomplete` names the node. Downloaded weights and the
pulled image are kept on purpose: the next prepare resumes the download, and
recipes of the same model revision and release share them. To free that
space, delete the recipe with **Also remove weights and image**
(see [Delete](#5-delete)).

### Controller restarts

Quitting or restarting the controller never stops a prepare: the units keep
downloading on the nodes and the operation stays `running`. On the next
start the controller reattaches:

- A **running** prepare is probed on every node. If every unit is still
  running or has already succeeded, the controller resumes polling and
  finishes normally, registering the profile on success (event
  `prepare_reattached`). If a node reports failure, or its unit is gone
  without a result, the prepare fails as usual and its directory is removed
  (`prepare_interrupted` when the unit vanished). Only when a node's state
  cannot be read (SSH fails, the node is no longer configured) does the
  operation end `needs-attention` with `prepare_interrupted`; its files and
  any unit are then left untouched, and preparing again resumes the download.
- A **queued** prepare never reached a node. It is cancelled
  (`queued_operation_interrupted`), like queued lifecycle operations, and the
  draft returns to `draft`. Prepare it again when you want it.

## 4. Load

A prepared draft shows **Load…**, which opens the load dialog for its profile.
Loading is the ordinary lifecycle operation described in
[Operations](OPERATIONS.md): if another deployment owns the selected nodes, you
confirm that it will be stopped first.

Each rank runs as a Docker container named `dgxc-<profile-id>` with host
networking, `--ipc host`, `--gpus all`, the model root mounted read-only at
`/models`, and the Docker volume `dgx-spark-controller-cache` at
`/root/.cache`. The command is `vllm serve /models/<model dir>` with the saved
settings. With several nodes, the ranks use vLLM's multiprocessing backend
(`--nnodes`, `--node-rank`, `--master-addr`, master port 29501, workers
`--headless`). Collective traffic is bound to the interface that owns the
rank's cluster address. The controller prefers a /24 shared by a head cluster
address and a cluster address on every worker, and otherwise uses management
addresses. `/dev/infiniband` is passed through when present.

The endpoint is `http://<head management address>:<port>` with the
OpenAI-compatible routes under `/v1`. `/v1/models` lists exactly the served
model name.

## 5. Delete

**Delete** on a recipe card asks for confirmation and says what it removes.

- A draft that never produced a profile is deleted locally
  (`DELETE /api/v1/recipes/{id}`); nothing on the nodes changes.
- A prepared recipe, or any recipe with **Also remove weights and image**
  checked, is deleted by a journaled `delete-recipe` operation
  (`POST /api/v1/recipes/{id}/delete`). The plain `DELETE` route refuses a
  recipe that has generated profiles with `409 recipe_prepared`, so a profile
  can never be orphaned.

The request is refused with `409 profile_in_use` while any deployment of one
of the recipe's profiles is not `stopped`, or while a load, unload or
recovery for one of them is queued or running. Unload first. The operation
runs on the recipe worker, after any prepare that is already running, and
never starts or stops a model. It holds the node leases of the affected nodes
while it removes files, so a lifecycle operation cannot start a profile that
is being deleted. Every removal step is idempotent; a failed or interrupted
delete can simply be run again.

It always removes, for every generated profile of the recipe (each prepare
registers one):

1. The profile's recipe directory on each of its nodes. A node refuses while
   the profile's container (`dgxc-<profile-id>`, running or stopped) or its
   prepare unit exists. A node that is no longer configured is skipped.
2. The generated profile file in `<data dir>/profiles/`, only after every
   directory is gone, followed by a profile registry reload. If a directory
   cannot be removed, the profile stays registered, the operation fails with
   `recipe_delete_incomplete`, and the recipe returns to its previous state.
3. The draft file, once every profile is gone.

With **Also remove weights and image** (`{"remove_artifacts": true}`), it also
removes, on each node:

- The model directory `<model root>/<org>--<name>-<commit[:8]>` of the
  recipe's profiles and of the draft itself (a failed prepare may have
  downloaded part of it), unless another profile, or another prepared or
  preparing recipe, on that node references the same directory. The node
  refuses while any prepare unit or download container runs, or while a
  running container's command names the directory.
- The pinned image `vllm/vllm-openai@sha256:<digest>`, unless another profile
  or prepared or preparing recipe on that node uses the same reference, or any
  container (running or stopped) was created from it.
- The `dgx-spark-controller-cache` Docker volume, when this delete removed the
  last generated profile on that node and no container uses the volume.

The operation's `result.artifacts` lists each node's `weights`, `image` and
`cache-volume` outcome: `removed`, `absent`, `in-use`, `refused` or `failed`.
The exact boundaries of what a node may remove are in
[Security](SECURITY.md#deleting-recipes).

## Limits

- **Release images only.** Nightly, source-built or patched images cannot be
  generated; use a hand-reviewed profile for those
  (see [Model profiles](MODEL_PROFILES.md)).
- **Safetensors only.** GGUF and `pytorch_model*.bin` repositories are rejected
  with `unsupported_weight_format`. A `config.json` stored in Git LFS cannot be
  read (`host_not_allowed`), because the catalog never contacts the LFS CDN.
- **Not qualified on GB10.** Official vLLM images are built and tested upstream
  for other targets and have not been qualified on GB10 (`sm_121`). Generated
  profiles are therefore marked `origin: generated` and `qualified: false` and
  carry the `generated-recipe-unqualified` invariant. A successful load proves
  readiness for that load only.
- **Multi-node needs `--nnodes`.** Releases whose `vllm serve` lacks
  `--nnodes` fail during prepare for placements with more than one node.
- **Same user and model root.** Multi-node placements need the same home
  directory and first model root on every node.
- **Cleanup is explicit.** A failed prepare removes only its own directory.
  Weights, images and generated profiles stay until you delete the recipe.
  Profiles whose draft was deleted before this flow existed have no recipe
  card; remove such a file from `<data dir>/profiles/` only when no
  deployment uses it.
