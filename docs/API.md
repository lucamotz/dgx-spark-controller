# Local API

The API is implemented by `controller.server`, `controller.host` and
`controller.app`. It listens on a loopback address only and serves the UI from
the same origin. There is no CORS support (`OPTIONS` answers `405`) and no
remote administration surface. No route accepts a shell command, script path,
flag, environment variable, SSH option or arbitrary URL.

## Run a fake service

Use fake mode to explore the API. It never opens an SSH connection:

```bash
DGX_CONTROLLER_HOME=$(mktemp -d) .venv/bin/python -m controller --mode fake --port 18876
```

This starts in setup mode with two simulated nodes to discover. To start with
nodes and the example profile already configured instead:

```bash
DGX_CONTROLLER_HOME=$(mktemp -d) .venv/bin/python -m controller --mode fake --port 18876 \
  --nodes config/examples/nodes.cluster.example.json \
  --profiles config/examples/profiles
```

Recipe catalog routes contact Hugging Face, GitHub and Docker Hub even in fake
mode. Stopping the service with `Ctrl-C` never unloads a remote model.

## Routes

GET routes also answer `HEAD`.

### Always available

| Method | Route | Status | Response |
| --- | --- | --- | --- |
| GET | `/api/health` | `200` | health object |
| GET | `/api/v1/bootstrap` | `200` | combined snapshot with `health` and `setup` |

### Lifecycle

These routes answer `409 setup_required` while no node is configured.

| Method | Route | Status | Response |
| --- | --- | --- | --- |
| GET | `/api/v1/nodes` | `200` | `{"items": [...]}` |
| GET | `/api/v1/nodes/{id}` | `200` | one node |
| POST | `/api/v1/nodes/{id}/refresh` | `200` | refreshed node (synchronous) |
| GET | `/api/v1/telemetry` | `200` | cached capacity samples and `interval_seconds` |
| GET | `/api/v1/models` | `200` | `{"items": [...]}` |
| GET | `/api/v1/profiles` | `200` | `{"items": [...]}` |
| GET | `/api/v1/deployments` | `200` | `{"items": [...]}` |
| POST | `/api/v1/deployments` | `202` | `{"operation": {...}}` load or switch |
| DELETE | `/api/v1/deployments/{id}` | `202` | `{"operation": {...}}` unload |
| GET | `/api/v1/operations?limit=50` | `200` | `{"items": [...]}`, newest first |
| GET | `/api/v1/operations/{id}` | `200` | one operation |
| GET | `/api/v1/operations/{id}/logs?node={node}&cursor={cursor}` | `200` | bounded log chunk |
| POST | `/api/v1/operations/{id}/cancel` | `202` | `{"operation": {...}}` |
| POST | `/api/v1/reconcile` | `202` | `{"operation": {...}}` read-only reconciliation |
| POST | `/api/v1/recover` | `202` | `{"operation": {...}}` baseline recovery |

### Setup wizard

| Method | Route | Status | Response |
| --- | --- | --- | --- |
| GET | `/api/v1/setup` | `200` | host status plus local networks, default targets and SSH keys |
| POST | `/api/v1/setup/scans` | `202` | `{"scan": {...}}` |
| GET | `/api/v1/setup/scans/{id}` | `200` | `{"scan": {...}}` |
| POST | `/api/v1/setup/scans/{id}/cancel` | `202` | `{"scan": {...}}` |
| GET | `/api/v1/setup/ssh-keys` | `200` | `{"items": [...]}` |
| POST | `/api/v1/setup/ssh-keys` | `201` | `{"key": {...}, "public_key": "..."}` |
| POST | `/api/v1/setup/host-keys` | `200` | `{"host", "port", "keys": [...]}` |
| POST | `/api/v1/setup/host-keys/trust` | `200` | `{"host", "port", "trusted": n}` |
| POST | `/api/v1/setup/identify` | `200` | `{"identity": {...}}` |
| POST | `/api/v1/setup/links` | `200` | `{"host", "links": {...}}` |
| POST | `/api/v1/setup/nodes` | `200` | `{"nodes": [...], "path": "..."}` |

### Catalog and recipes

| Method | Route | Status | Response |
| --- | --- | --- | --- |
| POST | `/api/v1/catalog/models/inspect` | `200` | `{"model", "fit", "suggested"}` |
| GET | `/api/v1/catalog/vllm-releases?prereleases=1` | `200` | `{"items": [...]}` |
| GET | `/api/v1/recipes` | `200` | `{"items": [...]}` |
| POST | `/api/v1/recipes` | `201` | `{"recipe": {...}}` |
| GET | `/api/v1/recipes/{id}` | `200` | `{"recipe": {...}}` |
| POST | `/api/v1/recipes/{id}` | `200` | `{"recipe": {...}}` partial update |
| DELETE | `/api/v1/recipes/{id}` | `200` | `{"deleted": "<id>"}` |
| POST | `/api/v1/recipes/{id}/prepare` | `202` | `{"operation": {...}}` |

## Request rules

- Bodies are JSON objects sent with `Content-Type: application/json` (`415
  invalid_content_type` otherwise) and a `Content-Length` of at most 64 KiB
  (`413 request_too_large`). Routes without a body accept no body or `{}`.
- Unknown body keys are rejected. Query parameters may appear only once.
  `limit` is an integer from 1 to 100.
- Every lifecycle mutation and `prepare` needs an `Idempotency-Key` header
  matching `[A-Za-z0-9][A-Za-z0-9._:-]{0,199}`. Reusing a key for the same
  action returns the existing operation; reusing it for a different lifecycle
  action is `409 idempotency_conflict`. Setup and recipe-draft routes do not
  use keys.
- Unknown routes answer `404 route_not_found`. Methods other than GET, HEAD,
  POST and DELETE answer `405 method_not_allowed`.

## Health, bootstrap and setup mode

`GET /api/health` describes only the local process:

```json
{"status": "ok", "version": "0.2.0", "schema_version": 1, "mode": "live", "setup_required": false}
```

`mode` is `live` or `fake`. `setup_required` is true while no valid
`nodes.json` is loaded.

`GET /api/v1/bootstrap` returns `nodes`, `models`, `profiles`, `deployments`,
`operations` (latest 50), `warnings`, `health` and `setup`. In setup mode the
collections are empty. `setup` is the host status:

```json
{
  "mode": "live",
  "configured": true,
  "setup_required": false,
  "reload_pending": false,
  "config_error": null,
  "nodes_path": "/home/user/.config/dgx-spark-controller/nodes.json"
}
```

`reload_pending` is true while a configuration change waits for a lifecycle
operation to finish. `config_error` holds `{"code", "message"}` when
`nodes.json` or a profile file fails validation.

## Read resources

Node state is observed state; it is never inferred from a request. The nested
`deployment` is null unless the observation identifies that exact journaled
deployment on this node.

```json
{
  "id": "spark-a",
  "display_name": "DGX Spark 1",
  "management_address": "192.0.2.10",
  "state": "idle",
  "last_refresh_at": "2026-10-10T14:02:00+00:00",
  "endpoint": "http://192.0.2.10:8000",
  "active_model_alias": null,
  "runtime_version": null,
  "memory": {"total_bytes": 137438953472, "used_bytes": 0, "used_percent": 0.0, "free_bytes": 137438953472, "swap_used_bytes": null},
  "gpu": {"name": "GB10", "utilization_percent": null},
  "warnings": [],
  "deployment": null
}
```

`GET /api/v1/models` lists one artifact record per profile; presence on a node
is `unverified` until proven. `GET /api/v1/profiles` exposes safe metadata
only, never a command:

```json
{
  "id": "g-qwen3-8b-1a2b3c",
  "display_name": "Qwen3-8B (vLLM v0.31.0)",
  "model_id": "hf-qwen-qwen3-8b-b968826d",
  "api_alias": "qwen3-8b",
  "backend": "vllm",
  "deployment_mode": "shared-vllm",
  "node_count": 2,
  "startup_timeout_seconds": 2400,
  "expected_memory_bytes": 16381516776,
  "weights_bytes": 16381516776,
  "gpu_memory_utilization": 0.85,
  "reserved_memory_bytes": 116822900736,
  "validation_state": "generated",
  "origin": "generated",
  "qualified": false,
  "provenance": {"hf_repo": "Qwen/Qwen3-8B", "hf_revision": "b968826d9c46dd6066d109eabc6255188de91218", "vllm_release": "v0.31.0", "vllm_version": "0.31.0", "draft_id": "r-0123456789ab"},
  "node_ids": ["spark-a", "spark-b"],
  "endpoint_port": 8000,
  "invariants": ["generated-recipe-unqualified", "served-alias-exact-match"]
}
```

`origin` is `reviewed` or `generated`; `qualified` is true only for reviewed
profiles. `weights_bytes` is the size of the model weights
(`expected_memory_bytes` carries the same value under its older name; it is
not a memory estimate). Several generated recipes can share one set of
weights, so for generated profiles `gpu_memory_utilization` is the recipe's
vLLM setting and `reserved_memory_bytes` the unified memory the recipe
reserves on each node: the utilization times the smallest node memory of its
placement, as pinned in the profile's start gate. Both are `null` for
reviewed profiles. Deployments:

```json
{
  "id": "dep-g-qwen3-8b-1a2b3c",
  "profile_id": "g-qwen3-8b-1a2b3c",
  "desired_state": "ready",
  "state": "ready",
  "node_ids": ["spark-a", "spark-b"],
  "endpoint": "http://192.0.2.10:8000",
  "alias": "qwen3-8b",
  "owning_operation_id": "op-example",
  "created_at": "2026-10-10T14:00:00+00:00",
  "updated_at": "2026-10-10T14:05:00+00:00"
}
```

## Live capacity monitoring

`GET /api/v1/telemetry` reads memory and GPU counters independently of
lifecycle observations. It accepts no parameters, creates no operation, and
cannot establish readiness.

```json
{
  "interval_seconds": 10,
  "items": [{
    "node_id": "spark-a",
    "sampled_at": "2026-10-10T12:00:00+00:00",
    "status": "ok",
    "simulated": false,
    "memory": {"total_bytes": 137438953472, "available_bytes": 34359738368, "used_bytes": 103079215104, "used_percent": 75.0, "swap_used_bytes": 0},
    "gpu": {"utilization_percent": 42.0},
    "message": ""
  }]
}
```

- Memory is unified system RAM; used bytes are `MemTotal - MemAvailable`. GPU
  utilization is the driver-reported busy percentage of GPU 0, not memory
  occupancy.
- `status` is `ok`, `partial` (one of memory or GPU missing) or `unavailable`.
  Unknown values are `null`, never zero. Fake mode sets `simulated: true`.
- Collection is serialized and cached across browser tabs: each node is
  sampled at most once per ten seconds, failures included. An uncached request
  can take up to eight seconds per node. There is no background collector.
- The remote script reads `/proc/meminfo` and runs one `nvidia-smi` query
  under a three-second hard timeout. Below 1 GiB `MemAvailable` the GPU query
  is skipped and `message` says why.

## Lifecycle mutations

### Load or switch

```bash
curl -fsS -X POST http://127.0.0.1:18876/api/v1/deployments \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: load-001' \
  --data '{"profile_id":"g-qwen3-8b-1a2b3c","node_ids":["spark-a","spark-b"]}'
```

The body accepts `profile_id`, `node_ids` and an optional `confirmation`
(UI metadata, not an authorization bypass). The `202` response wraps a
queued operation:

```json
{
  "operation": {
    "id": "op-example",
    "action": "load",
    "state": "queued",
    "phase": "queued",
    "progress_percent": 0,
    "message": "Operation accepted and queued.",
    "profile_id": "g-qwen3-8b-1a2b3c",
    "deployment_id": "dep-g-qwen3-8b-1a2b3c",
    "node_ids": ["spark-a", "spark-b"],
    "requested_at": "2026-10-10T14:00:00+00:00",
    "started_at": null,
    "updated_at": "2026-10-10T14:00:00+00:00",
    "events": [{"sequence": 1, "timestamp": "2026-10-10T14:00:00+00:00", "phase": "queued", "code": "operation_queued", "message": "Operation accepted and queued."}]
  }
}
```

Poll `GET /api/v1/operations/{id}` until `state` is `succeeded`, `failed`,
`cancelled` or `needs-attention`. A finished operation may carry `result`
(for example `{"deployment_id": ..., "expected_alias": ...}`) or `error` with
`code`, `message` and `remediation`.

### Unload, refresh, reconcile, recover, logs

```bash
curl -fsS -X DELETE http://127.0.0.1:18876/api/v1/deployments/dep-g-qwen3-8b-1a2b3c \
  -H 'Idempotency-Key: unload-001'
curl -fsS -X POST http://127.0.0.1:18876/api/v1/nodes/spark-a/refresh
curl -fsS -X POST http://127.0.0.1:18876/api/v1/reconcile -H 'Idempotency-Key: reconcile-001'
curl -fsS -X POST http://127.0.0.1:18876/api/v1/recover \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: recover-001' \
  --data '{"profile_id":"g-qwen3-8b-1a2b3c","confirmation":true}'
curl -fsS 'http://127.0.0.1:18876/api/v1/operations/op-example/logs?node=spark-a'
```

- Unload stops only the proven deployment, then verifies process, endpoint and
  alias removal and memory recovery. Closing the browser or app never calls it.
- Refresh is a synchronous read. Reconcile is a durable, read-only operation.
- Recover accepts only a registered `shared-vllm` profile ID (`409
  recovery_not_supported` otherwise). It proves the profile's exact runtime
  identity on every present rank, runs the paired stop script, and verifies
  the post-stop baseline.

### Cancel

`POST /api/v1/operations/{id}/cancel` takes no body:

- A **live prepare** is signalled to stop and the response is
  `202 {"operation": {"id": "<id>", "cancel_requested": true}}`. The prepare
  stops its node units and download containers at its next poll and finishes
  `cancelled` (event `prepare_cancelled`); downloaded files are kept.
- Any other operation needs an `Idempotency-Key`. A **queued** lifecycle
  operation is cancelled before any remote work. A **running** one answers
  `409 operation_not_cancellable`. A finished operation is returned
  unchanged.

## Setup wizard routes

All setup routes work in setup mode. None of them changes anything on a node.

- `GET /api/v1/setup` returns the host status fields above plus `networks`
  (`interface`, `address`, `network`), `default_targets`, `ssh_keys`,
  `nodes_file_exists` and `default_ssh_user` (the controller host's login
  name, or `""` when it is not a valid node user name).
- `POST /api/v1/setup/scans` with `{"targets": ["192.0.2.0/24"]}` (optional;
  up to eight private IPv4 networks of at most /22 each) starts a background
  scan. Only one scan runs at a time (`409 scan_in_progress`). A scan has
  `id`, `state` (`running`, `succeeded`, `failed`, `cancelled`), `targets`,
  `progress` (`done`, `total`), `candidates` and `error`. Each candidate has
  `address`, `hostname`, `ssh_open`, `ssh_banner`, `open_ports`,
  `served_models`, `likely_spark`, `confidence` (`high`, `medium`, `low`) and
  `reasons`.
- `GET /api/v1/setup/ssh-keys` lists selectable keys: `id`, `path`,
  `display_path`, `source` (`nvidia-sync`, `ssh-dir`, `controller`),
  `has_public_key`, `key_type`. `POST` generates the controller key and
  returns it with its public key; it never overwrites an existing key
  (`400 ssh_key_exists`).
- `POST /api/v1/setup/host-keys` with `{"host", "port"}` returns
  `keys: [{"key_type", "fingerprint_sha256", "trusted", "user_known_hosts"}]`.
  `trusted` refers to the controller `known_hosts`. `user_known_hosts`
  compares the key with the operator's own `~/.ssh/known_hosts`, read with
  `ssh-keygen -F` and never written: `match`, `mismatch` (a different key of
  the same type is recorded), `revoked` (marked `@revoked`), `absent` (no key
  of that type is recorded) or `unknown` (the file could not be read).
  `POST /api/v1/setup/host-keys/trust` with `{"host", "port",
  "fingerprints": [...]}` re-fetches the keys and pins only matching ones
  (`409 host_key_mismatch` if none match).
- `POST /api/v1/setup/identify` with `{"host", "port", "user", "key_id"}`
  returns `{"identity": {...}}`. Connection problems are not HTTP errors:
  `identity.ok` is false and `identity.error_code` is one of `auth_failed`
  (with `copy_id_command`), `host_key_untrusted`, `host_key_changed`,
  `unreachable`, `ssh_refused`, `host_unresolvable`, `ssh_key_not_found`,
  `ssh_missing`, `ssh_failed` or `probe_failed`. A successful identity carries
  hardware, Docker, systemd and network facts, `is_dgx_spark`, `warnings`,
  `cluster_addresses` and a `suggested` node entry. `gpu_container_support`
  says how Docker exposes the GPU: `runtime` (the `nvidia` runtime is
  registered), `cdi` (an `nvidia.com/gpu` CDI device is discovered, Docker 28
  and later) or `null` (neither, which adds a warning); `nvidia_runtime`
  remains as a boolean.
- `POST /api/v1/setup/links` with `{"host", "port"}` pings the cluster
  addresses of every other identified node from that node and returns
  `{"links": {"198.51.100.2": true}}`.
- `POST /api/v1/setup/nodes` with `{"nodes": [...], "replace": false}` writes
  `nodes.json` from nodes identified in the last 30 minutes (`409
  node_not_identified` otherwise). Each entry needs `host`, `port` and
  `key_id`, and may override `node_id`, `display_name`, `model_root` and
  `cluster_addresses` (only addresses the node reported). The response is the
  merged node list and the file path. The controller then reloads.

## Catalog routes

- `POST /api/v1/catalog/models/inspect` with `{"link": "<model link>"}`
  returns `model` (repository, resolved 40-character `revision`, `gated`,
  `license`, `architectures`, `max_position_embeddings`, `quantization`,
  `parameter_count`, `weight_format`, `weight_files`, `weight_bytes`,
  `warnings`, `requires_remote_code`), `fit` (one estimate per node count up to
  `min(configured nodes, 4)`) and `suggested` (`served_alias`, `display_name`,
  `max_model_len`, `trust_remote_code`).
- `GET /api/v1/catalog/vllm-releases` lists recent stable vLLM releases
  (`tag`, `version`, `published_at`, `prerelease`, `html_url`,
  `aarch64_wheels`). `?prereleases=1` includes release candidates.

Catalog results are cached for ten minutes. Upstream failures map to `502`
(`upstream_unavailable`, `upstream_rate_limited`, `upstream_unauthorized`).
A missing model is `404 model_not_found`; a gated model without access is
`400 model_gated`.

## Recipe routes

`POST /api/v1/recipes` requires `model_link`, `release_tag` and `node_ids`, and
accepts `cuda_variant`, `head_node_id`, `served_alias`, `display_name` and
`settings`. `POST /api/v1/recipes/{id}` accepts the same keys, all optional,
and resets the draft to `draft`. Bounds are listed in
[Recipes](RECIPES.md#2-choose-a-runtime-placement-and-settings). A draft
resource:

```json
{
  "draft_id": "r-0123456789ab",
  "display_name": "Qwen3-8B",
  "served_alias": "qwen3-8b",
  "model": {"repo_id": "Qwen/Qwen3-8B", "revision": "b968826d9c46dd6066d109eabc6255188de91218", "weight_bytes": 16381516776, "weight_files": [["model-00001-of-00005.safetensors", 3996250744]], "max_position_embeddings": 40960, "architectures": ["Qwen3ForCausalLM"], "quantization": null, "gated": false, "needs_remote_code": false},
  "runtime": {"release_tag": "v0.31.0", "reference": "vllm/vllm-openai@sha256:<64 hex>", "arm64_digest": "sha256:<64 hex>", "cuda_variant": "default"},
  "placement": {"node_ids": ["spark-a", "spark-b"], "head_node_id": "spark-a"},
  "settings": {"max_model_len": 32768, "max_num_seqs": 8, "max_num_batched_tokens": 16384, "gpu_memory_utilization": 0.85, "kv_cache_dtype": "auto", "enable_prefix_caching": true, "trust_remote_code": false, "enforce_eager": false, "tool_call_parser": null, "reasoning_parser": null, "port": 8000, "startup_timeout_s": 2400},
  "status": "draft",
  "profile_id": null,
  "last_operation_id": null,
  "message": "",
  "created_at": "2026-10-10T09:00:00+00:00",
  "updated_at": "2026-10-10T09:00:00+00:00",
  "schema_version": 1,
  "node_count": 2,
  "model_dir": "Qwen--Qwen3-8B-b968826d",
  "qualified": false
}
```

`status` is `draft`, `preparing`, `prepared` or `failed`. Updating or deleting
a draft that is `preparing` answers `409 recipe_busy`.

`POST /api/v1/recipes/{id}/prepare` takes no body, needs an `Idempotency-Key`,
and returns a `prepare` operation (`409 setup_required` without nodes). Its
phases are `validating`, `preparing`, `committing` and `complete`; progress
events use the codes `prepare_started`, `prepare_progress`,
`profile_generated` and `prepare_succeeded`. On success, `result` is
`{"profile_id": "...", "draft_id": "..."}` and the operation's `profile_id`
names the generated profile.

## Errors

Every error uses one envelope; mutation errors may add `operation_id`:

```json
{
  "error": {
    "code": "operation_not_found",
    "message": "The requested operation does not exist.",
    "retryable": false,
    "remediation": "Refresh operation history and choose an existing operation."
  }
}
```

Status codes:

| Status | Codes |
| --- | --- |
| `400` | `invalid_json`, `invalid_request`, `invalid_idempotency_key`, `invalid_limit`, `invalid_query`, `content_length_required`, and every setup, catalog or recipe validation code not listed below |
| `404` | `route_not_found`, `node_not_found`, `profile_not_found`, `deployment_not_found`, `operation_not_found`, `scan_not_found`, `recipe_not_found`, `model_not_found`, `not_found` |
| `405` | `method_not_allowed` |
| `409` | `setup_required`, `idempotency_conflict`, `operation_not_cancellable`, `recovery_not_supported`, `scan_in_progress`, `host_key_mismatch`, `node_not_identified`, `recipe_busy` |
| `413` | `request_too_large` |
| `415` | `invalid_content_type` |
| `500` | `internal_error`, `static_asset_missing` |
| `502` | `upstream_unavailable`, `upstream_rate_limited`, `upstream_unauthorized` |
| `503` | `controller_shutting_down` (lifecycle) |

Setup, catalog and recipe errors set `retryable` to true for `409` and `502`.
Codes are listed in the [state and error catalog](STATE_AND_ERROR_CATALOG.md).
Messages are bounded and redacted before they leave the service: absolute
paths, secret-looking values and command fragments are replaced, and tokens
and private key contents never appear.
