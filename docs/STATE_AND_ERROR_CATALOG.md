# State and error catalog

This is the canonical vocabulary. Code, API responses, operation events, tests
and documentation use these values exactly.

Two spellings coexist on purpose. Configuration codes are hyphenated
(`config-unknown-key`). Operation, transport, API, setup, catalog
and recipe codes are snake_case (`operation_interrupted`). Do not convert one
into the other without updating clients and tests.

## Node states

| State | Meaning | Safe next action |
| --- | --- | --- |
| `unknown` | no trusted observation yet | refresh; do not mutate from stale data |
| `offline` | transport unavailable | check network and SSH; do not stop anything remotely |
| `idle` | reachable, no managed workload | preflight a profile |
| `ready` | a known managed deployment serves its alias | load another profile or unload |
| `busy` | a known workload is active | inspect ownership; drain before a switch |
| `unmanaged` | a workload the controller cannot prove it started occupies the node (for example a GPU container started by hand) | stop it with the tool that started it; the controller never adopts or stops it |
| `degraded` | reachable, but a fact or invariant is unhealthy | read-only diagnostics |

## Deployment states

A deployment record has a desired state, `ready` or `stopped`, and the last
observed state the controller proved. Operation phases carry the detailed
progress; there is no separate deployment transition table.

| Observed state | Written when |
| --- | --- |
| `starting` | a load or a maintenance restore is about to start the ranks |
| `ready` | readiness is proven: a load commits or finds the placement already ready, a restore or rollback restarts it, or reconciliation observes it |
| `stopped` | release is proven on every node: unload, recovery, a switch that replaces it, a maintenance stop, or a failed restore whose ranks were stopped; reconciliation also records an absent deployment |
| `failed` | a load was rolled back |
| `unknown` | a stop, recovery or restore cleanup could not be proven, or reconciliation cannot classify the observations |

`failed` and `unknown` need a new preflight before reuse.

## Operations

States: `queued` → `running` → one of `succeeded`, `failed`, `cancelled` or
`needs-attention`. There is no automatic retry after a destructive partial
failure; a new operation must revalidate observations.

Actions: `load`, `unload`, `recover`, `reconcile`, `update`, `prepare` and
`delete-recipe`. `refresh` is a synchronous read and not journaled. `prepare`
and `delete-recipe` are recipe actions: they run on the recipe worker, and
after a restart the recipe service, not reconciliation, resolves them.

Phases, in order of progress: `queued`, `validating`, `locking`,
`refreshing`, `preflighting`, `snapshotting`, `draining`,
`stopping-conflicts`, `verifying-stopped`, `updating`, `rebooting`,
`starting-workers`, `starting-head`, `loading`, `verifying-ready`,
`committing`, `rolling-back`, `reconciling`, `preparing`, `deleting`,
`complete`. A phase never moves
backwards; the legal edges are in
`controller.operations.LEGAL_PHASE_TRANSITIONS`. A `prepare` moves `queued` →
`validating` → `preparing` → `committing` → `complete`; a `delete-recipe`
moves `queued` → `validating` → `deleting` → `complete`. A load whose placement is already ready may go from
`preflighting` straight to `committing` without remote mutation. An `update`
moves through `locking`, `refreshing`, `preflighting` (helper probe, check
and downloads, while the model serves), `snapshotting` (the placement to
restore), then `draining` → `stopping-conflicts` → `verifying-stopped` when
something serves, `updating`, `rebooting` when a node is rebooted, and the
start phases to `committing`; an up-to-date cluster goes from
`preflighting` to `committing`.

## Profiles and recipes

| Field | Values |
| --- | --- |
| profile `origin` | `reviewed` (hand-written; `qualified: true` in the API), `generated` (recipe builder; `qualified: false`) |
| runner `launch_mode` | `head-script`, `per-node` |
| `deployment_mode` | `independent`, `shared-vllm` (`shared-llama-rpc` is reserved) |
| recipe draft `status` | `draft`, `preparing`, `prepared`, `failed`, `deleting` |
| prepare `progress_by_node.*.state` | `running`, `succeeded`, `failed`, `absent`, `unknown` |
| delete `result.artifacts[].kind` | `weights`, `image`, `cache-volume` |
| delete `result.artifacts[].state` | `removed`, `absent`, `in-use`, `refused`, `failed` |
| scan `state` | `running`, `succeeded`, `failed`, `cancelled` |
| scan candidate `confidence` | `high`, `medium`, `low` |

## Configuration error codes

| Code | Meaning | Recovery |
| --- | --- | --- |
| `config-file-not-found` | JSON file or directory unavailable | restore the file; never guess defaults |
| `config-invalid-json` | JSON cannot be decoded | fix the syntax |
| `config-schema-unsupported` | unsupported `schema_version` | migrate explicitly |
| `config-unknown-key` | fail-closed key rejection | remove the key, or implement it with tests and docs |
| `config-missing-key` | required key absent | add the field |
| `config-invalid-value` | wrong type, range, path or enum | correct the value |
| `duplicate-id` | node or profile ID collision | assign a unique ID |

## Lifecycle engine codes

Operations and transport results use these snake_case codes:
`preflight_failed`, `readiness_timeout`, `completion_canary_failed`,
`memory_release_unverified`, `node_unreachable`, `operation_locked`,
`unmanaged_conflict`, `unknown_managed_conflict`,
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

A load replaces a managed deployment on every node it uses, including nodes
outside the requested placement. `operation_locked` means a live lease of
another operation covers a requested node or one of those extra nodes; the
load fails before any mutation. `partial_cluster_conflict` means the replaced
deployment cannot be stopped everywhere: a node outside the placement is owned
by a different managed deployment, or is no longer configured.
`unmanaged_conflict` applies to those extra nodes as well.

`completion_canary_failed` means the endpoint passed identity, health and
alias checks but its one bounded completion request did not return HTTP 200,
a `stop` or `length` finish reason, and at least one completion token (or the
request could not be sent). It is a readiness failure: the new deployment is
rolled back.

Common event codes: `operation_queued`, `operation_started`,
`operation_succeeded`, `operation_cancelled`, `queued_operation_interrupted`,
`interrupted_operation_proven_succeeded`, `already_stopped`,
`recovery_succeeded`, and `reconcile_started` / `reconcile_succeeded`. A load
that replaces a deployment outside its placement records
`extending_switch_scope`; a load with a completion canary records
`completion_canary_started` and then `completion_canary_passed` or
`completion_canary_failed`.

## API boundary codes

`invalid_json`, `invalid_content_type`, `content_length_required`,
`invalid_content_length`, `request_too_large`, `invalid_query`,
`invalid_limit`, `invalid_idempotency_key`, `invalid_request`,
`idempotency_conflict`, `route_not_found`, `method_not_allowed`,
`setup_required`, `node_not_found`, `profile_not_found`,
`deployment_not_found`, `operation_not_found`, `node_not_in_operation`,
`invalid_log_cursor`, `operation_not_cancellable`,
`controller_shutting_down`, `invalid_host_header` (a non-loopback `Host`
header), `internal_error` and
`static_asset_missing`. HTTP status mapping is in [API.md](API.md#errors).
An operation created while the controller shuts down is cancelled with event
`queued_operation_interrupted`.

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
| `invalid_ssh_user`, `invalid_ssh_key_path`, `ssh_key_not_found`, `ssh_key_permissions` | key or user rejected |
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
| `invalid_fit_parameters`, `not_found` | invalid internal parameters; upstream 404 |

## Recipe codes

| Code | Meaning |
| --- | --- |
| `unsupported_weight_format` | the revision has no safetensors weights for vLLM |
| `invalid_model`, `model_revision_unpinned`, `model_has_no_weights` | model facts unusable |
| `invalid_release`, `runtime_unpinned` | release or image digest unusable |
| `invalid_setting`, `context_exceeds_model`, `remote_code_required` | settings out of bounds, context above the model maximum, custom code not opted in |
| `invalid_alias`, `invalid_recipe`, `invalid_recipe_id`, `unsupported_recipe` | draft fields or stored file invalid |
| `invalid_placement`, `node_not_configured`, `port_not_allowed`, `model_root_mismatch` | placement does not fit the configured nodes |
| `recipe_not_found`, `recipe_busy` | unknown draft; draft is being prepared or deleted |
| `recipe_prepared` | `DELETE /api/v1/recipes/{id}` on a draft with generated profiles or `node_files_left`; use the journaled delete |
| `profile_in_use` | a deployment of one of the recipe's profiles is not stopped, or a lifecycle operation for one of them is queued or running (also raised inside a delete when a lifecycle operation holds a node lease) |
| `update_in_progress` | a prepare was requested while a cluster update is queued or running |
| `unsafe_recipe_value` | a value outside `[A-Za-z0-9._:/@+-]` reached rendering |
| `generated_profile_invalid` | the rendered profile failed validation |

A failed `prepare` operation finishes with event code `prepare_failed` and one
of these `error.code` values: `prepare_failed` (a node reported failure, for
example disk space, image pull, download or `--nnodes` support),
`prepare_timeout`, `prepare_unobservable`, `prepare_incomplete`,
`prepare_mismatch` (nodes disagree on image, version or weights),
`home_directory_mismatch`, `model_root_mismatch`, `port_not_allowed`,
`recipe_install_failed`, `prepare_start_failed`, `unsafe_recipe_value`,
`generated_profile_invalid`, `node_not_configured`, `linger_required` (a
node user does not linger; checked before any node change) or
`prepare_interrupted` (after a restart, a node's unit was gone without a
result). Prepare events use `prepare_started`, `prepare_progress`,
`profile_generated`, `prepare_succeeded`, `prepare_cancelled`,
`prepare_already_running`, `prepare_reattached`, `prepare_directory_removed`
and `prepare_cleanup_incomplete`.

After a controller restart, a queued recipe operation is cancelled with event
`queued_operation_interrupted`. A running prepare whose node state cannot be
read ends `needs-attention` with event and `error.code` `prepare_interrupted`;
one that the controller restarted before it reached a node ends `cancelled`
with event `prepare_interrupted`. A running delete ends `needs-attention` with
`delete_interrupted`.

A failed `delete-recipe` finishes with event `delete_failed` and `error.code`
`recipe_delete_incomplete` (a profile's node directory could not be removed;
the profile is kept), `profile_in_use`, `recipe_not_found` or
`setup_required`. Delete events use `delete_started`, `profile_deleted`,
`profile_kept`, `delete_node_skipped`, `artifact_removed`, `artifact_absent`,
`artifact_in_use`, `artifact_refused`, `artifact_failed`, `delete_succeeded`
and `operation_cancelled` (a queued delete was cancelled).

Node-side removals report one reason when they refuse or fail: `symlink`,
`not-a-directory`, `home`, `home-mismatch`, `prepare-active`,
`container-exists`, `in-use`, `docker-unavailable`, `no-docker`,
`outside-model-root`, `working-directory-mismatch`,
`unreachable`, `script-failed`, `no-result`, `rm`, `image-rm` or `volume-rm`.
They appear in event messages only.

## Update codes

| Field | Values |
| --- | --- |
| node `helper` (`GET /api/v1/updates`) | `ready`, `missing`, `outdated` (hash or ownership differs), `no-sudo` (sudoers rule missing), `unreachable` |
| helper run `state` | `idle`, `running`, `succeeded`, `failed`, `interrupted` (its unit is gone without a result) |
| helper run `step` | `starting`, `apt-update`, `firmware-refresh`, `apt-download`, `firmware-download`, `dpkg-configure`, `apt-upgrade`, `firmware-update`, `complete`; `failed_step` may also be `relay` |
| update settings `source` | `online`, `directory` |
| update `progress_by_node.*.state` | `running`, `succeeded`, `failed`, `unknown` |

Updates page requests fail with `invalid_schedule` (weekdays or time),
`invalid_update_source` (source or directory), `invalid_sudo_password`,
`update_in_progress` (an update is queued or running), `update_check_running`
(an update was requested during a check) or `updates_unavailable`.
`invalid_update_settings` appears only in `settings_error`, for a settings
file that cannot be read. A helper install reports per node
`sudo_authentication_failed` (wrong password, or the user may not use
`sudo`), `helper_stage_failed`, `helper_install_failed`, `helper_mismatch`
(the installed helper is not the packaged one), `node_unreachable`, or a code
the helper reported: `invalid_sudo_user`, `unknown_user`, `update_running`,
`source_missing` or `sudoers_invalid`.

A failed `update` operation carries one of these `error.code` values, or a
shared lifecycle code (`operation_locked`, `node_unreachable`,
`unmanaged_conflict`, `preflight_failed`) checked on every node:

| Code | Meaning |
| --- | --- |
| `update_blocked` | a recipe prepare or delete is running |
| `deployment_not_ready` | a managed deployment is not ready on all of its nodes |
| `update_helper_not_ready` | a node's helper is not `ready` |
| `update_check_failed` | a node could not check through the relay; a cause the helper reports replaces it: `relay_unreachable`, `apt_update_failed`, `firmware_refresh_failed`, `update_running`, `helper_mismatch` |
| `relay_tunnel_failed` | a node's tunnel did not reach this relay session, for example because another program holds port 18877 there |
| `relay_failed` | the relay failed on the controller host, for example it could not create its cache directory |
| `update_timeout` | downloads did not finish within 4 hours or installs within 2 hours; the helper keeps running |
| `update_download_failed` | a download failed or could not start on a node, also when an earlier helper run still holds it; nothing was stopped |
| `update_install_failed` | an install failed or could not start on a node; that node was not rebooted, the others were, and the deployments were restored; a restore failure is appended to the message |
| `node_reboot_refused` | a node did not accept its reboot (helper `reboot_failed` or `reboot_schedule_failed`); the others were rebooted and the deployments restored |
| `node_reboot_timeout` | a rebooted node was not back with a new boot ID, a finished boot, its user systemd manager, Docker, the GPU and its cluster links within 45 minutes |
| `update_restore_failed` | a stopped deployment could not be restored; the ranks it started were stopped |

Update events: `refreshing_nodes`, `update_preflight_started`,
`updates_found`, `snapshotting_prior_placement`, `update_progress` (with
`progress_by_node`), `update_download_failed` and `update_apply_failed` (a
run could not start, or failed with the node's last log lines),
`draining_for_maintenance`,
`stopping_for_maintenance`, `verifying_maintenance_stop`,
`update_install_started`, `rebooting_nodes`, `node_back_online`,
`restore_preflight_started`, `restoring_workers`, `restoring_serving_roles`,
`waiting_for_runtime`, `verifying_readiness`, the completion canary events,
`restore_cleanup_started` and `committing_update`. An up-to-date cluster
finishes with `cluster_up_to_date`, an updated one with
`operation_succeeded`.
