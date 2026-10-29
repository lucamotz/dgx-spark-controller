# DGX Spark Controller

A local control plane for running and switching vLLM models on one or more
[NVIDIA DGX Spark](https://www.nvidia.com/en-us/products/workstations/dgx-spark/)
nodes (and GB10 systems such as the ASUS Ascent GX10). It runs on your Linux or
macOS machine, talks to the Sparks over SSH, and serves a browser UI on
`127.0.0.1` only.

- **Connect a cluster.** A setup wizard scans your private network for Sparks,
  shows each node's SSH host key for you to confirm, identifies the node with a
  read-only probe (GB10 GPU, memory, Docker, ConnectX-7 links, systemd
  lingering), checks the direct links between nodes, and writes the node
  configuration.
- **Change recipes on the fly.** Paste a Hugging Face model link, pick a
  released vLLM version from GitHub, and tune a small, bounded set of serving
  settings. *Prepare* pulls the digest-pinned `vllm/vllm-openai` arm64 image
  and downloads the pinned model revision on every selected node without
  touching the model that is currently serving. *Load* then switches models
  through the controller's verified lifecycle.
- **Switch safely.** Every load runs read-only preflight checks, stops only the
  conflicting deployment the controller owns (on every node it uses, never on
  part of it), proves processes, endpoints, aliases and memory are released,
  starts the pinned recipe, and requires the exact served model name on a
  healthy endpoint before it reports ready. Profiles can add a one-request
  completion canary; generated recipes do. Operations are journaled in SQLite
  and survive restarts.
- **Update the nodes.** On a schedule or on demand, the controller checks
  every Spark for OS package and firmware updates, downloads them while the
  model keeps serving, then stops it, installs, reboots, and loads it again
  through the same readiness checks. The nodes need no Internet access: the
  controller relays every download, from the Internet or a local directory.
- **One codebase for Linux and macOS.** The service uses only the Python
  standard library. An optional native window uses pywebview (WebKit on macOS,
  GTK or Qt on Linux); otherwise the UI opens in your browser.

> **Status: alpha.** The lifecycle engine, SSH transport, and fake-mode flows
> are tested in CI on Linux and macOS. The onboarding wizard and the recipe
> builder have not yet been exercised end to end against live Sparks, and the
> official vLLM release images are not yet qualified on GB10 (sm_121), so
> generated profiles are labelled *unqualified* until a live load proves them.
> See [docs/ROADMAP.md](docs/ROADMAP.md).

## Quick start

Requirements: Python 3.12+, an OpenSSH client, and SSH key access to your Sparks
(the wizard helps you set this up). On the Sparks: Docker usable by your user,
the NVIDIA Container Toolkit, and `loginctl enable-linger` for that user.

```bash
git clone https://github.com/lucamotz/dgx-spark-controller.git
cd dgx-spark-controller
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -e '.[desktop]'
.venv/bin/dgx-spark-controller-app
```

The first start opens **Setup**. To try everything without touching a real node,
run the deterministic fake mode against a throwaway directory:

```bash
DGX_CONTROLLER_HOME="$(mktemp -d)" .venv/bin/python -m controller --mode fake --open-browser
```

Platform-specific installation (macOS app bundle, Linux desktop entry and
`systemd --user` service) is in [docs/INSTALL.md](docs/INSTALL.md). The setup
walkthrough is in [docs/SETUP.md](docs/SETUP.md) and the recipe builder in
[docs/RECIPES.md](docs/RECIPES.md).

## Safety model

- The browser never submits a shell command, remote path, flag, environment
  variable, or SSH invocation. Requests carry identifiers and bounded values
  that the service validates and projects into fixed remote scripts.
- Remote launch scripts are pinned by SHA-256 and verified before every start
  and stop. Recipe values reach them as an environment file that the scripts
  parse as data, never execute.
- Readiness requires process identity, `GET /health == 200`, and the exact
  `/v1/models` alias. A healthy port alone is not enough.
- The controller never kills a process it does not own. Unmanaged workloads are
  reported, not adopted.
- Closing the app or stopping the service never unloads a remote model; only an
  explicit unload does.
- Network discovery is limited to private subnets of at most 1,024 addresses
  and never logs in. SSH uses keys only, host keys are pinned after you confirm
  the fingerprint, and private key contents are never read.
- System updates run as root only through a hash-verified helper on each node
  that the node user may run for five fixed subcommands without a password.
  Installing it asks once for the node user's sudo password, which goes only
  to `sudo` on that node and is never stored. The one local path the browser
  may submit, an update source directory, is only read by the controller's
  update relay.

Details: [docs/SECURITY.md](docs/SECURITY.md) and
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Development

The project follows the vLLM project's quality bar: ruff (lint and format,
88 columns), mypy, typos, markdownlint, shellcheck, actionlint, SPDX headers,
Google-style docstrings, pytest, and DCO sign-off.

```bash
uv pip install --python .venv/bin/python -e '.[dev]'
.venv/bin/pre-commit install
.venv/bin/python -m pytest tests -q
.venv/bin/pre-commit run --all-files
```

Contributor and agent guidance is in [AGENTS.md](AGENTS.md); testing details are
in [docs/TESTING.md](docs/TESTING.md).

## License

Apache-2.0. See [LICENSE](LICENSE). This project is not affiliated with or
endorsed by NVIDIA or the vLLM project.
