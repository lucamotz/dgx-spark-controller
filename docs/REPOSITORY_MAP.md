# Repository map

A first orientation for maintainers. The service is a small standard-library
Python package with a zero-build browser UI. Configuration and profiles are
reviewed inputs; the browser never supplies runtime commands. Module
responsibilities and dependency rules are in [Architecture](ARCHITECTURE.md).

## Tree

```text
controller/                 the installable package
├── __main__.py, main.py    `python -m controller` / `dgx-spark-controller`: CLI
├── launcher.py             `dgx-spark-controller-app`: desktop launcher
├── host.py                 ServiceHost: transport assembly, setup mode and hot reload
├── server.py               loopback HTTP routes and static files
├── app.py                  lifecycle API requests and worker
├── resources.py            browser-safe API resource shapes and text redaction
├── projection.py           profile + nodes -> DeploymentSpec
├── orchestrator.py         load, unload, recover
├── reconcile.py            reconciliation
├── operations.py           operation records and phase transitions
├── persistence.py          SQLite journal, events, idempotency, leases
├── domain.py, config.py    immutable records and strict JSON parsing
├── paths.py                per-OS directories, DGX_CONTROLLER_HOME
├── telemetry.py            shared capacity sample cache
├── transport/              NodeTransport, fake, SSH executor, managed SSH and its probe/preflight/cluster scripts, telemetry probe
├── runners/                systemd --user runner for single-node profiles
├── discovery/              setup wizard: network, scanner, mdns, hostkeys, sshkeys, identify, setup, commands
├── catalog/                allowlisted HTTPS: http, huggingface, vllm_releases
├── recipes/                recipe builder: spec, store, generated, render, nodeops, service, prepare, delete, journaling, catalog_client
│   └── templates/vllm-release/   pinned prepare.sh, start.sh, stop.sh
├── updates/                system updates: settings, service, engine, nodeops, relay
│   └── templates/          root helper dgx-spark-controller-update.sh, installed on the nodes
├── assets/                 application icons and their generator
└── webui/                  index.html, styles.css, app.js, ui-*.js
config/examples/            node examples and, in profiles/, one hand-reviewed profile
packaging/macos/            build_app.sh, Info.plist and launcher templates
packaging/linux/            install.sh, uninstall.sh, desktop entry and user unit templates
requirements/               pinned test and lint requirements for CI
tools/pre_commit/           repository-local pre-commit checks (SPDX headers)
tests/unit/                 pure contracts and isolated adapters
tests/integration/          fake-node engine and local HTTP API
tests/e2e_fake/             UI static checks
tests/frontend/             Node.js tests for UI rendering logic
tests/fixtures/catalog/     recorded Hugging Face, GitHub and Docker Hub responses
docs/                       this documentation; docs/adr/ holds decision records
.github/workflows/ci.yml    CI
eslint.config.mjs           ESLint flat config for controller/webui and tests/frontend
```

`build/` (the macOS app bundle) and `.venv/` are generated and not committed.

## Entry points

| Command | Module | Purpose |
| --- | --- | --- |
| `dgx-spark-controller`, `python -m controller` | `controller.main` | run the loopback service |
| `dgx-spark-controller-app`, `python -m controller.launcher` | `controller.launcher` | start or reuse the service and open the UI |

Service options: `--mode live|fake` (default `live`), `--host` (loopback
only), `--port` (default 18876), `--nodes`, `--profiles` (repeatable),
`--database`, `--open-browser`, and `--no-startup-reconcile` (diagnostics
only). `live` is the only mode that can change a remote node. Installation is
in [Install](INSTALL.md).

## Where to change what

| Change | Files that change together |
| --- | --- |
| domain enum, state or error code | `domain.py`, its unit test, the parser, [state and error catalog](STATE_AND_ERROR_CATALOG.md), affected docs |
| JSON key or default | `config.py`, `config/examples/`, `tests/unit/test_config.py`, [Configuration](CONFIGURATION.md) |
| lifecycle invariant | the profile, [Model profiles](MODEL_PROFILES.md), [Operations](OPERATIONS.md), a fake-node scenario test before the code |
| API shape | `server.py`, `app.py`, `resources.py` or `host.py`, [API](API.md), [UI](UI.md), `tests/integration/test_http_api.py` |
| transport or runner | the protocol, the fake transport, the SSH tests and the remote-script goldens in `tests/fixtures/remote_scripts/`, [Security](SECURITY.md) or [Operations](OPERATIONS.md) if ownership or command boundaries change |
| setup wizard | `discovery/`, `webui/ui-setup.js`, discovery and setup tests, [Setup](SETUP.md) |
| recipe builder or templates | `recipes/`, `catalog/`, `webui/ui-recipes.js`, `tests/unit/test_recipes.py`, [Recipes](RECIPES.md); a template change produces new script hashes, so existing generated profiles keep their old pinned copies on the nodes |
| system updates or the node helper | `updates/`, `webui/ui-updates.js`, `tests/unit/test_updates_*.py` and the update scenarios in `tests/integration/`, [Operations](OPERATIONS.md#update-the-cluster), [Security](SECURITY.md#system-updates); a helper change changes its SHA-256, so every node reports `outdated` until the helper is installed again from the Updates page |

Run the offline checks in [Testing](TESTING.md) before every change.
