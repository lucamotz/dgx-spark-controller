# Configuration

Configuration is JSON, parsed by `controller/config.py` into immutable records
from `controller/domain.py`. Every document has `schema_version` (currently
`1`). Unknown keys, wrong types and unsupported versions fail closed, and
optional defaults live only in the parser.

## File locations

The controller keeps per-user state in three directories, resolved by
`controller/paths.py`:

| | macOS | Linux |
| --- | --- | --- |
| Config | `~/Library/Application Support/DGXSparkController/config` | `$XDG_CONFIG_HOME/dgx-spark-controller` (default `~/.config/dgx-spark-controller`) |
| Data | `~/Library/Application Support/DGXSparkController/data` | `$XDG_DATA_HOME/dgx-spark-controller` (default `~/.local/share/dgx-spark-controller`) |
| Logs | `~/Library/Logs/DGXSparkController` | `$XDG_STATE_HOME/dgx-spark-controller/logs` (default `~/.local/state/dgx-spark-controller/logs`) |

With `DGX_CONTROLLER_HOME=/some/dir` everything lives in
`/some/dir/{config,data,logs}` instead, which is how tests and throwaway fake
runs stay isolated. Relative XDG values are ignored. Directories are created
with mode `0700`.

| File | Directory | Written by |
| --- | --- | --- |
| `nodes.json` (and `nodes.json.bak-<epoch>`) | config | the setup wizard, or you |
| `profiles/*.json` (hand-reviewed profiles) | config | you |
| `known_hosts` (pinned host keys) | config | the setup wizard |
| `ssh/id_ed25519` and `.pub` (optional controller key) | config | the setup wizard |
| `controller.sqlite3` (operation journal) | data | the service |
| `profiles/*.json` (generated profiles) | data | recipe prepare |
| `recipes/r-*.json` (recipe drafts) | data | the Recipes page |
| `update-settings.json` (update schedule and source) | data | the Updates page |
| `update-cache/<host>/<path>` (relayed update downloads) | data | the update relay, in online mode |
| `controller-app.log` | logs | the desktop launcher |

The service reads profiles from both profile directories; a profile ID must be
unique across them. Command-line options override the defaults:
`--nodes PATH`, `--profiles DIR` (repeatable; replaces both default
directories), and `--database PATH`.

The repository's `config/examples/` holds a single-node and a two-node
`nodes` example and, in `config/examples/profiles/`, one hand-reviewed profile.
Examples are never loaded unless you pass them explicitly.

## Environment variables

| Variable | Where | Purpose |
| --- | --- | --- |
| `DGX_CONTROLLER_HOME` | controller host | put config, data and logs below one directory |
| `XDG_CONFIG_HOME`, `XDG_DATA_HOME`, `XDG_STATE_HOME` | controller host (Linux) | standard base directories |
| `HF_TOKEN` | controller host | optional; lets the catalog read `config.json` of gated or private models |
| `GITHUB_TOKEN` | controller host | optional; raises the GitHub API rate limit for the release list |
| `DGX_CONTROLLER_EXIT_WITH_PARENT` | set by the launcher | the service exits when its launcher process disappears |

Tokens are sent only as `Authorization` headers to their own host
(`huggingface.co` or `api.github.com`). They are never stored, logged, sent to
a node, or returned to the browser. Weight downloads on the nodes use the
token each node stores after `hf auth login`, never `HF_TOKEN` from the
controller host. Set the variables in the environment that starts the service,
for example in the systemd user unit or before launching the app.

## Nodes document

The setup wizard writes `nodes.json` (see [Setup](SETUP.md)). You can also
write it by hand; `config/examples/nodes.cluster.example.json` shows a
two-node cluster:

```json
{
  "schema_version": 1,
  "nodes": [
    {
      "id": "spark-a",
      "display_name": "DGX Spark 1",
      "ssh": {"address": "192.0.2.10", "user": "spark", "key_path": "~/.ssh/dgx_spark_ed25519", "connect_timeout_s": 10},
      "management_address": "192.0.2.10",
      "cluster_addresses": ["198.51.100.2"],
      "labels": ["spark", "gb10"],
      "capabilities": ["cuda", "docker", "openai-compatible-http", "roce", "vllm"],
      "model_roots": ["/home/spark/models"],
      "endpoint_ports": {"start": 8000, "end": 8999}
    }
  ]
}
```

| Key | Type | Default | Rule |
| --- | --- | --- | --- |
| `schema_version` | integer | required | `1` |
| `nodes` | array | required | at least one node; IDs unique |
| `nodes[].id` | ID | required | stable controller identity; need not equal the host name |
| `nodes[].display_name` | string | required | label in the UI |
| `nodes[].ssh.address` | string | required | address or host name used for SSH |
| `nodes[].ssh.user` | string | required | remote account |
| `nodes[].ssh.key_path` | string | required | path to the private key; a reference, never key contents |
| `nodes[].ssh.connect_timeout_s` | integer | `10` | positive seconds |
| `nodes[].management_address` | string | required | host for endpoint URLs |
| `nodes[].cluster_addresses` | string array | `[]` | addresses on the direct node-to-node link |
| `nodes[].labels` | string array | `[]` | matched by profile `node_selector.required_labels` |
| `nodes[].capabilities` | string array | `[]` | matched by `node_selector.required_capabilities` |
| `nodes[].model_roots` | absolute path array | required | at least one; recipes use the first |
| `nodes[].endpoint_ports` | `{start, end}` | `11434`–`11434` | inclusive range a profile port must fall into |

The wizard sets `connect_timeout_s` to 10 and `endpoint_ports` to 8000–8999,
and derives labels and capabilities from the identity probe. IDs are 1–128
characters matching `[a-z0-9][a-z0-9._-]*`.

## Profile document

A profile is the only loadable object: a versioned contract for how a model
runs, how readiness is proven, and which safety gates apply. See
[Model profiles](MODEL_PROFILES.md) for reviewed versus generated profiles.

| Key | Type | Default | Rule |
| --- | --- | --- | --- |
| `schema_version` | integer | required | `1` |
| `profile_id` | ID | required | unique across all profile directories |
| `revision` | integer | `1` | increment for any contract change |
| `display_name` | string | required | label in the UI |
| `origin` | `reviewed` or `generated` | `reviewed` | `generated` requires `runner.launch_mode: per-node` |
| `provenance` | string map | `{}` | source facts, for example `hf_repo`, `hf_revision`, `vllm_release`, `vllm_image`, `vllm_version`, `draft_id`, `prepare_id`, `template` |
| `artifact` | object | required | model identity and required files; not a command builder |
| `api_alias` | string | required | exact served model name |
| `backend` | `llama.cpp` or `vllm` | required | |
| `deployment_mode` | `independent` or `shared-vllm` | required | llama.cpp: `independent` with one node; vLLM: `shared-vllm` |
| `node_count` | integer | required | vLLM: at least 2 for `head-script`, at least 1 for `per-node` |
| `node_selector` | object | `{}` | `allowed_node_ids`, `required_labels`, `required_capabilities` |
| `runner` | object | required | allowlisted script reference |
| `endpoint` | object | required | `port`; `health_path` `/health`, `models_path` `/v1/models`, `completion_path` `/v1/chat/completions`, `host_source` `management-address`, `scheme` `http` |
| `readiness` | object | required | see below |
| `runtime` | object | required | runtime name, executable, build facts |
| `safety` | object | required | see below |

`artifact` requires `artifact_id`, `display_name`, `format` (`gguf`,
`safetensors` or `runtime-packaged`), an absolute `primary_path` and absolute `required_files`, and
accepts `quantization`, `architecture`, `size_bytes`, `primary_size_bytes` and
`manifest_sha256`.

### Runner

| Key | Default | Notes |
| --- | --- | --- |
| `kind` | required | `allowlisted-script` |
| `script_id`, `script_path` | required | absolute start script |
| `interpreter` | `bash` | |
| `syntax_check` | `true` | `bash -n` before every run |
| `expected_sha256`, `require_sha256` | none, `false` | live single-node profiles must pin the hash |
| `stop_script_path`, `expected_stop_sha256` | none | required for vLLM |
| `working_directory`, `environment_path` | none | required for vLLM |
| `required_environment` | `{}` | exact `KEY=value` pairs the launch environment must contain; required for vLLM |
| `control_node_id` | none | required for vLLM; must be in `allowed_node_ids` |
| `project_name`, `container_name`, `expected_recipe_commit`, `image_digest` | none | required for vLLM |
| `launch_mode` | `head-script` | `head-script` or `per-node` |
| `node_environment` | `{}` | per-node overlays; only with `per-node`, exactly one per allowed node, and no key may repeat a `required_environment` key |

With `head-script`, `environment_path` is an operator-owned file on the control
node; the controller writes a mode-`0600` overlay next to it that changes only
the `required_environment` keys and passes it to the scripts as `ENV_FILE`. It
never rewrites the source file. With `per-node`, the same happens on every node
with that node's overlay added. See
[Architecture](ARCHITECTURE.md#per-node-launch-contract).

### Readiness and safety

| Key | Default |
| --- | --- |
| `readiness.process_identity.executable_path` | required, absolute |
| `readiness.process_identity.required_command_fragments` | required; literal argv elements |
| `readiness.expected_alias` | `api_alias` (must equal it) |
| `readiness.expected_health_status` | `200` |
| `readiness.exact_alias_set` | `true` |
| `readiness.startup_timeout_s` | `420` |
| `readiness.poll_interval_s` | `2` |
| `readiness.completion_canary` | disabled; `timeout_s` 30, `max_tokens` 32, `prompt` `Say OK.`. When `enabled`, a load sends one bounded request to `endpoint.completion_path` after readiness passes and fails with `completion_canary_failed` unless it succeeds; see [Operations](OPERATIONS.md#completion-canary). Generated profiles enable it (`Reply with OK.`, 16 tokens, 120 s) |
| `safety.require_allowlisted_script`, `require_script_readable`, `require_script_syntax_check` | `true` |
| `safety.require_script_hash` | `false` |
| `safety.stop_conflicts_before_start` and the four `verify_*_after_stop` checks | `true` |
| `safety.minimum_free_memory_mib_before_start` / `_after_stop` | `16384` |
| `safety.maximum_swap_used_mib` | `1024` |
| `safety.recovery_timeout_s` | `120` |
| `safety.required_invariant_ids` | `[]` |

Generated profiles set their own values: the startup timeout from the recipe,
a 10-second poll interval, a free-memory gate of `gpu_memory_utilization` ×
the smallest node's memory, an 8 GiB swap bound and a 180-second recovery
timeout.

## Update settings

The Updates page writes `<data>/update-settings.json` atomically with mode
`0600`:

```json
{
  "schema_version": 1,
  "enabled": true,
  "weekdays": ["sat", "sun"],
  "time": "03:00",
  "source": "directory",
  "directory": "/Volumes/mirror/update-cache",
  "updated_at": "2026-10-29T09:00:00+00:00"
}
```

| Key | Meaning |
| --- | --- |
| `enabled` | run updates on the schedule; default `false` |
| `weekdays` | `mon` to `sun`, at least one; default `["sun"]` |
| `time` | `HH:MM` in the controller host's local time; default `03:00` |
| `source` | `online` (the Internet, through the relay) or `directory`; default `online` |
| `directory` | resolved absolute path of the source directory, only with `directory` |
| `updated_at` | when the settings were saved; no earlier slot is ever started |

Unknown keys, wrong types, invalid values and other schema versions fail
closed: the Updates page shows the error (`invalid_update_settings` or the
field's code), and the defaults, with the schedule off, apply until the
settings are saved again.

In online mode the relay keeps every complete file it fetched in
`<data>/update-cache/<host>/<path>`, the layout a `directory` source expects,
so the cache can be copied to an offline controller. The controller never
prunes it; delete it to reclaim space.

## Secret boundary

`ssh.key_path` is a path reference. The parser never expands or reads it; the
SSH transport passes it to `ssh -i`. Remote paths must be absolute and are
never interpreted as shell syntax. No configuration file holds a token,
password or private key; the sudo password for the update helper is never
written.

## Errors

Parser errors carry a stable code and the JSON path: `config-file-not-found`,
`config-invalid-json`, `config-schema-unsupported`, `config-unknown-key`,
`config-missing-key`, `config-invalid-value` and `duplicate-id`. An invalid
`nodes.json` or profile keeps the service in setup mode and is reported as
`setup.config_error`; see [Troubleshooting](TROUBLESHOOTING.md).

## Changing a key or default

1. Update the dataclass in `controller/domain.py` and the parser in
   `controller/config.py` together.
2. Update both node examples or the example profile, and
   `tests/unit/test_config.py` (valid, defaulted, unknown-key, wrong-type and
   unsafe forms).
3. Update this page, and [Model profiles](MODEL_PROFILES.md) and the
   [state and error catalog](STATE_AND_ERROR_CATALOG.md) when behaviour or
   vocabulary changes.
4. If the recipe builder emits the key, update `controller/recipes/render.py`
   and `tests/unit/test_recipes.py`.
5. Run the offline checks in [Testing](TESTING.md).
