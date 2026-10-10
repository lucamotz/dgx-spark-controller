# State and error catalog

This is the canonical vocabulary. Code, API responses, operation events, tests
and documentation use these values exactly.

Two spellings coexist on purpose. Domain and configuration codes are
hyphenated (`config-unknown-key`). Operation, transport, API, setup, catalog
and recipe codes are snake_case (`operation_interrupted`). Do not convert one
into the other without updating clients and tests.

## Node states

| State | Meaning | Safe next action |
| --- | --- | --- |
| `unknown` | no trusted observation yet | refresh; do not mutate from stale data |
| `offline` | transport unavailable | check network and SSH; do not stop anything remotely |
| `idle` | reachable, no managed workload | preflight a profile |
| `ready` | API only: a known managed deployment serves its alias | load another profile or unload |
| `busy` | a known workload is active | inspect ownership; drain before a switch |
| `degraded` | reachable, but a fact or invariant is unhealthy | read-only diagnostics |
| `maintenance` | domain only: fenced by the operator | do not enqueue mutations |

## Deployment states

Load path: `planned` → `preflighting` → `stopping-conflicts` →
`starting-workers` / `starting-head` → `loading` → `ready`. Unload path:
`ready` → `draining` → `stopping` → `stopped`. `failed` and `unknown` need a
new preflight before reuse. The legal edges are in
`controller.domain.can_transition_deployment`.

## Operations

States: `queued` → `running` → one of `succeeded`, `failed`, `cancelled` or
`needs-attention`. There is no automatic retry after a destructive partial
failure; a new operation must revalidate observations.

Actions: `load`, `unload`, `recover`, `reconcile` and `prepare`. `refresh` is
a synchronous read and not journaled.

Phases, in order of progress: `queued`, `validating`, `locking`,
`refreshing`, `preflighting`, `snapshotting`, `draining`,
`stopping-conflicts`, `verifying-stopped`, `starting-workers`,
`starting-head`, `loading`, `verifying-ready`, `committing`, `rolling-back`,
`reconciling`, `preparing`, `complete`. A phase never moves backwards; the
legal edges are in `controller.operations.LEGAL_PHASE_TRANSITIONS`. A
`prepare` moves `queued` → `validating` → `preparing` → `committing` →
`complete`. A load whose placement is already ready may go from
`preflighting` straight to `committing` without remote mutation.

## Profiles and recipes

| Field | Values |
| --- | --- |
| profile `origin` | `reviewed` (hand-written; `qualified: true` in the API), `generated` (recipe builder; `qualified: false`) |
| runner `launch_mode` | `head-script`, `per-node` |
| `deployment_mode` | `independent`, `shared-vllm` (`shared-llama-rpc` is reserved) |
| recipe draft `status` | `draft`, `preparing`, `prepared`, `failed` |
| scan `state` | `running`, `succeeded`, `failed`, `cancelled` |
| scan candidate `confidence` | `high`, `medium`, `low` |

## Configuration and domain error codes

| Code | Meaning | Recovery |
| --- | --- | --- |
| `config-file-not-found` | JSON file or directory unavailable | restore the file; never guess defaults |
| `config-invalid-json` | JSON cannot be decoded | fix the syntax |
| `config-schema-unsupported` | unsupported `schema_version` | migrate explicitly |
| `config-unknown-key` | fail-closed key rejection | remove the key, or implement it with tests and docs |
| `config-missing-key` | required key absent | add the field |
| `config-invalid-value` | wrong type, range, path or enum | correct the value |
| `duplicate-id` | node or profile ID collision | assign a unique ID |
| `node-unreachable` | SSH failed | check address, key and network |
| `script-not-allowlisted` | runner is not a pinned script | use a profile |
| `script-not-readable` | remote script cannot be read | fix the path or permissions |
| `script-syntax-invalid` | `bash -n` failed | repair the script and revise the profile |
| `script-hash-mismatch` | bytes differ from the pinned hash | restore the bytes or revise the profile |
| `runtime-mismatch` | build or feature differs | install the pinned build |
| `model-missing` | required file absent | restore all files; never load a partial set |
| `port-unavailable` | port held by another owner | identify it; never kill an unmanaged process |
| `memory-insufficient` | memory guard failed | unload safely and preflight again |
| `conflict-not-stopped` | old managed unit still active | wait or escalate the targeted stop |
| `process-identity-mismatch` | executable or argv differs | mark unmanaged or failed |
| `health-check-failed` | `/health` is not HTTP 200 | inspect logs within the timeout |
| `model-alias-mismatch` | endpoint serves another alias | fail readiness |
| `memory-not-recovered` | post-stop memory guard failed | diagnose; do not start another large model |
| `unmanaged-process` | non-matching process owns a resource | investigate manually |
| `operation-conflict` | lease or idempotency conflict | inspect the existing operation |
| `rollback-failed` | previous placement not restored | leave `needs-attention`; recover deliberately |

## Lifecycle engine codes

Operations and transport results use these snake_case codes:
`preflight_failed`, `readiness_timeout`, `memory_release_unverified`,
`node_unreachable`, `unmanaged_conflict`, `unknown_managed_conflict`,
`partial_cluster_conflict`, `ownership_mismatch`, `unmanaged_deployment`,
`unknown_deployment`, `rollback_incomplete`, `operation_interrupted`,
`application_worker_failed`, `reconcile_failed`, `recovery_not_supported` and
`recovery_identity_mismatch`. Shared vLLM transport failures use
`cluster_recipe_missing`, `cluster_environment_failed`,
`cluster_start_failed`, `cluster_ownership_marker_failed`,
`cluster_stop_failed`, `cluster_launcher_failed` and
`cluster_container_exited`. The last is reported only when the exact owned
container exited after its launcher succeeded; the load then fails promptly
and rolls back instead of waiting for the startup timeout.

`unmanaged_deployment` also covers an unload with no observed deployment ID
but a remaining process, endpoint or alias. A missing ownership marker is not
evidence of a stopped model. `already_stopped` is returned only after the
normal release checks pass.

Common event codes: `operation_queued`, `operation_started`,
`operation_succeeded`, `operation_cancelled`, `queued_operation_interrupted`,
`interrupted_operation_proven_succeeded`, `already_stopped`,
`recovery_succeeded`, and `reconcile_started` / `reconcile_succeeded`.

## API boundary codes

`invalid_json`, `invalid_content_type`, `content_length_required`,
`invalid_content_length`, `request_too_large`, `invalid_query`,
`invalid_limit`, `invalid_idempotency_key`, `invalid_request`,
`idempotency_conflict`, `route_not_found`, `method_not_allowed`,
`setup_required`, `node_not_found`, `profile_not_found`,
`deployment_not_found`, `operation_not_found`, `node_not_in_operation`,
`invalid_log_cursor`, `operation_not_cancellable`,
`controller_shutting_down`, `internal_error` and `static_asset_missing`.
HTTP status mapping is in [API.md](API.md#errors).

## Setup wizard codes

| Code | Meaning |
| --- | --- |
| `invalid_scan_target`, `no_scan_target` | a target is not an IPv4 network or address; no private local network was found |
| `scan_target_not_private`, `scan_target_too_large` | outside the private ranges; more than 1,024 addresses |
| `scan_in_progress`, `scan_not_found`, `scan_failed` | one scan at a time; unknown scan ID; unexpected scan failure |
| `invalid_host`, `invalid_port` | host or port rejected before anything runs |
| `host_key_scan_failed` | `ssh-keyscan` missing, timed out, or returned no supported key |
| `invalid_fingerprints`, `host_key_mismatch` | nothing confirmed; the node's current key does not match the confirmation |
| `known_hosts_too_large`, `invalid_known_hosts_path` | the controller `known_hosts` file cannot be used safely |
| `invalid_ssh_user`, `invalid_ssh_key_path`, `ssh_key_outside_home`, `ssh_key_not_found`, `ssh_key_permissions` | key or user rejected |
| `ssh_key_exists`, `ssh_key_generation_failed`, `public_key_missing`, `public_key_invalid` | controller key generation or public key problems |
| `node_not_identified` | save attempted for a node not identified in the last 30 minutes |
| `invalid_node_id`, `invalid_display_name`, `invalid_model_root`, `invalid_cluster_address`, `duplicate_node_id`, `invalid_node_config` | save rejected |
| `nodes_file_unreadable` | the existing `nodes.json` cannot be merged; save with replace |

Identification failures are returned in `identity.error_code`, not as HTTP
errors: `auth_failed`, `host_key_untrusted`, `host_key_changed`,
`unreachable`, `ssh_refused`, `host_unresolvable`, `ssh_key_not_found`,
`ssh_missing`, `ssh_failed`, `probe_failed`.

## Catalog codes

| Code | Meaning |
| --- | --- |
| `invalid_model_link` | not a Hugging Face model link or `org/name` id |
| `model_not_found` | repository or revision does not exist, or is private without access |
| `model_gated` | access denied; accept the terms and configure tokens |
| `host_not_allowed` | a URL or redirect left the HTTPS allowlist (for example an LFS-stored `config.json`) |
| `response_too_large` | upstream document exceeded its size cap |
| `invalid_token` | `HF_TOKEN` or `GITHUB_TOKEN` is malformed |
| `upstream_unauthorized`, `upstream_rate_limited`, `upstream_unavailable` | upstream rejected, throttled or failed the request |
| `invalid_release_tag`, `invalid_cuda_variant`, `runtime_image_unavailable` | release or image cannot be pinned |
| `invalid_limit`, `invalid_fit_parameters`, `not_found` | invalid internal parameters; upstream 404 |

## Recipe codes

| Code | Meaning |
| --- | --- |
| `unsupported_weight_format` | the revision has no safetensors weights for vLLM |
| `invalid_model`, `model_revision_unpinned`, `model_has_no_weights` | model facts unusable |
| `invalid_release`, `runtime_unpinned` | release or image digest unusable |
| `invalid_setting`, `context_exceeds_model`, `remote_code_required` | settings out of bounds, context above the model maximum, custom code not opted in |
| `invalid_alias`, `invalid_recipe`, `invalid_recipe_id`, `unsupported_recipe` | draft fields or stored file invalid |
| `invalid_placement`, `node_not_configured`, `port_not_allowed`, `model_root_mismatch` | placement does not fit the configured nodes |
| `recipe_not_found`, `recipe_busy` | unknown draft; draft is being prepared |
| `unsafe_recipe_value` | a value outside `[A-Za-z0-9._:/@+-]` reached rendering |
| `generated_profile_invalid` | the rendered profile failed validation |

A failed `prepare` operation finishes with event code `prepare_failed` and one
of these `error.code` values: `prepare_failed` (a node reported failure, for
example disk space, image pull, download or `--nnodes` support),
`prepare_timeout`, `prepare_unobservable`, `prepare_incomplete`,
`prepare_mismatch` (nodes disagree on image, version or weights),
`home_directory_mismatch`, `model_root_mismatch`, `port_not_allowed`,
`recipe_install_failed`, `prepare_start_failed`, `unsafe_recipe_value`,
`generated_profile_invalid` or `node_not_configured`. Prepare events use
`prepare_started`, `prepare_progress`, `profile_generated`,
`prepare_succeeded`, `prepare_cancelled` and `prepare_already_running`.
