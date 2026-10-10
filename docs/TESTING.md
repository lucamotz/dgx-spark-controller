# Testing

All normal verification is offline. Tests use local JSON, recorded upstream
responses, an in-memory fake transport and a loopback HTTP server. They never
contact a DGX Spark, run SSH against a real host, or change a remote model.

## Set up

```bash
uv venv --python 3.12 .venv            # or: python3 -m venv .venv
uv pip install --python .venv/bin/python -e '.[dev]'
.venv/bin/pre-commit install
```

The `dev` extra installs pytest, pre-commit, mypy and ruff. The service itself
needs only the standard library.

## Run the checks

```bash
.venv/bin/python -m pytest tests -q                       # everything below except Node.js
.venv/bin/python -m pytest tests/unit/test_recipes.py -q  # one module
.venv/bin/python tests/e2e_fake/ui_static_test.py         # UI static checks on their own
for test_file in tests/frontend/*_test.mjs; do node "$test_file"; done  # Node.js, no packages
.venv/bin/pre-commit run --all-files
.venv/bin/pre-commit run eslint --all-files                # JavaScript lint only
```

The frontend tests import the UI modules directly, so they cover only pure,
exported helpers that need no DOM. Without a system Node.js, the runtime that
pre-commit provisions for the ESLint and markdownlint hooks works too, for
example `~/.cache/pre-commit/repo*/node_env-lts/bin/node`.

One recipe test runs `prepare.sh` and needs GNU coreutils; it is skipped
elsewhere, for example on macOS.

## What the tests cover

| Layer | Location | Scope |
| --- | --- | --- |
| domain and config | `tests/unit/test_domain.py`, `test_config.py`, `test_paths.py` | records, transitions, strict parsing, per-OS directories |
| journal and service | `test_operation_journal.py`, `test_application_service.py` | durability, idempotency, redaction, identifier-only projection |
| SSH and runners | `test_ssh_transport.py`, `test_managed_ssh_transport.py`, `test_systemd_user_runner.py`, `test_telemetry.py` | exact argv, fixed scripts, per-node and head-script launch, ownership, bounded telemetry |
| discovery | `test_discovery_network.py`, `test_discovery_scanner.py`, `test_discovery_mdns.py`, `test_discovery_hostkeys.py`, `test_discovery_identify.py` | private-range validation, scan bounds, banner and mDNS parsing, host-key pinning and the read-only `known_hosts` comparison (temporary files only), recorded identify probes (NVIDIA runtime, CDI only, neither) |
| setup and host | `test_setup_and_host.py` | host-key trust, identify-before-save, setup mode, reload, a generated recipe from setup to load |
| catalog | `test_catalog_http.py`, `test_catalog_huggingface.py`, `test_catalog_vllm_releases.py` | allowlist and redirects, size caps, link parsing, digest pinning, against `tests/fixtures/catalog/` |
| recipes | `test_recipes.py` | draft bounds, rendering, environment allowlist, template scripts, prepare service |
| launcher | `test_launcher.py` | window/browser choice, child ownership, no remote calls |
| fake engine | `tests/integration/test_fake_operation_engine.py` | load, switch, unload, recovery, wrong alias, unmanaged conflicts, partial failures, restart proof |
| HTTP | `tests/integration/test_http_api.py` | loopback binding, routes, envelopes, idempotency, setup mode |
| UI | `tests/e2e_fake/ui_static_test.py`, `tests/frontend/*.mjs` | assets, accessibility markers, this documentation's route list, telemetry and deployment rendering |

The fake transport models memory use, delayed readiness, wrong aliases,
unreachable nodes, unmanaged processes and start/stop failures. A healthy fake
endpoint with the wrong alias must fail readiness, and an unmanaged process
must never be stopped by a switch.

## Fake mode by hand

Run the service against a throwaway directory:

```bash
DGX_CONTROLLER_HOME=$(mktemp -d) .venv/bin/python -m controller --mode fake --open-browser
```

1. **Setup** opens first. **Scan network** finds two simulated Sparks. Fetch
   and trust each host key, create a controller SSH key (this runs
   `ssh-keygen` inside the temporary directory), enter a user name such as
   `spark`, **Identify** both nodes, **Check direct links**, and save.
2. **Recipes**: inspect a public model such as `Qwen/Qwen3-8B` and pick a
   release. This step contacts Hugging Face, GitHub and Docker Hub. **Prepare**
   is simulated and finishes after a few polls.
3. **Load…** the generated profile, watch the operation, then unload it from
   the Dashboard. Telemetry is labelled **Demo**.

For the lifecycle alone, without network access or the wizard:

```bash
DGX_CONTROLLER_HOME=$(mktemp -d) .venv/bin/python -m controller --mode fake --open-browser \
  --nodes config/examples/nodes.cluster.example.json \
  --profiles config/examples/profiles
```

Fake mode never becomes a live qualification; do not switch it to
`--mode live` as a shortcut.

## Continuous integration

`.github/workflows/ci.yml` runs on every push and pull request:

| Job | Runs |
| --- | --- |
| `pre-commit` | all hooks on all files (Python 3.12) |
| `test` | Ubuntu and macOS × Python 3.12, 3.13, 3.14: `pytest tests -q`, the UI static checks, `json.tool` on every file in `config/examples`, `bash -n` on the packaging scripts |
| `frontend` | Node.js 22: every `tests/frontend/*_test.mjs` |
| `package` | builds the sdist and wheel, checks the wheel contains the UI and assets, installs it, and runs both entry points with `--help` |
| `macos-app` | builds `build/DGXSparkController.app`, checks the plist, icon and executable, and verifies the ad-hoc signature |

The pre-commit hooks are ruff (lint and format), typos, markdownlint-cli2,
actionlint, shellcheck, JSON and whitespace checks, mypy on `controller` and
`tools`, ESLint, an SPDX header check, a no-spaces-in-file-names check, and a
`commit-msg` hook that adds a DCO `Signed-off-by` line.

ESLint runs as a local hook with pinned `eslint`, `@eslint/js` and `globals`
packages on the Node.js LTS runtime that pre-commit installs. The flat config
`eslint.config.mjs` applies the `@eslint/js` recommended rules with browser
globals to `controller/webui/**` and Node.js globals to `tests/frontend/**`.
It loads those packages with `createRequire`, because pre-commit exposes them
through `NODE_PATH`, which ES module imports ignore. There is no formatter
for JavaScript.

## Live checks

There is no automated live test. Loading, switching or unloading on real nodes
interrupts whatever they serve, so run live checks only deliberately, on nodes
you own, when an interruption is acceptable. A generated profile stays
`qualified: false` regardless of a successful live load; see
[Model profiles](MODEL_PROFILES.md#qualification).
