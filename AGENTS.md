# Agent and contributor instructions

These instructions apply to all human and AI-assisted changes to this
repository. The project controls model processes on real, shared GPU nodes, so
correctness and safety come before convenience.

## 1. Start here

Read, in order:

1. `README.md`: product scope and safety model.
2. `docs/ARCHITECTURE.md` and `docs/REPOSITORY_MAP.md`: module boundaries.
3. `controller/domain.py` and `controller/config.py`: reviewed desired-state inputs.
4. `controller/operations.py`, `controller/persistence.py`, `controller/orchestrator.py`:
   operation records, journal, locks, and side-effect sequencing.
5. `controller/transport/` and `controller/runners/`: remote boundaries.
6. `controller/host.py`, `controller/discovery/`, `controller/catalog/`,
   `controller/recipes/`: setup mode, onboarding, and the recipe builder.
7. `docs/SECURITY.md` and `docs/STATE_AND_ERROR_CATALOG.md`.

## 2. Development workflow

- Never use system `python3` or bare `pip`. Use `uv` and `.venv/bin/python`.

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -e '.[dev]'
.venv/bin/pre-commit install
```

### Tests

```bash
.venv/bin/python -m pytest tests -q
.venv/bin/python -m pytest tests/unit/test_recipes.py -v
.venv/bin/python tests/e2e_fake/ui_static_test.py
for test_file in tests/frontend/*_test.mjs; do node "$test_file"; done
```

When adding tests: decide what failure you are guarding against and test it at
the cheapest level (unit over integration over end to end). Assert observable
behavior through public APIs. Extend existing files and helpers
(`tests/unit/catalog_fakes.py`, the fake transport, `FakeDiscoveryBackend`,
`FakeRecipeNodeOps`) before adding new ones. Tests must be offline and
deterministic; they never touch a real node. One behavior per test with the
smallest setup; no trivial tests (defaults, getters, "no exception") and no
tests for states real deployments cannot reach. Never synchronize with a fixed
sleep; poll a condition with a timeout.

### Linters

```bash
.venv/bin/pre-commit run --all-files
.venv/bin/pre-commit run ruff-check --all-files
.venv/bin/mypy controller tools
```

### Coding style

- Python lines are at most 88 characters (`ruff format`).
- Every Python file starts with the two SPDX lines used throughout the tree.
- Google-style docstrings (`Args:`/`Returns:`/`Raises:`) where a docstring
  helps. Prefer self-explanatory code; keep comments brief and only where they
  explain *why*.
- Match the existing style: frozen dataclasses for records, explicit
  validation with stable error codes, no new runtime dependencies in the
  service (it uses only the standard library).
- Shell scripts pass `shellcheck -s bash` with the repository `.shellcheckrc`.
- Make the most surgical change. Reuse existing functions instead of adding
  near-duplicates; inline a trivial helper that has one caller.
- No speculative generality: no unused options, flags, parameters or
  single-implementation abstractions, and no defensive `try`/`except` or
  checks for states that cannot happen. Validation at trust boundaries and the
  fail-closed checks in section 3 are required, not defensive.
- Comments explain non-obvious *why* only: no restating the code, no history
  of a fix. Do not touch unrelated code or formatting.
- Give these rules to every coding subagent before it starts.

### Commit messages

Use imperative subject lines and add trailers:

```text
Add per-node launch for generated recipes

Co-authored-by: Agent Name Here
Signed-off-by: Your Name <you@example.com>
```

The pre-commit `commit-msg` hook adds `Signed-off-by` automatically.

## 3. Frozen contracts

- JSON `schema_version` is required and equals `1` for nodes and profiles.
  Unknown JSON keys fail closed. Optional defaults live only in
  `controller/config.py`.
- The browser never submits a shell command, arbitrary path, flag, environment
  variable, or SSH invocation. New request fields must be identifiers or
  bounded values validated server-side.
- Remote model/script paths in profiles are absolute. Launch scripts are pinned
  by SHA-256 and verified before every start and stop.
- The recipe template scripts (`controller/recipes/templates/`) parse their
  environment file as data with an explicit key allowlist; values are limited
  to `[A-Za-z0-9._:/@+-]`. Never `source` or `eval` recipe input.
- Readiness requires process identity, HTTP `/health == 200`, and the exact
  `/v1/models` alias. A healthy port alone is insufficient.
- A conflicting model is stopped and memory recovery is verified before a new
  model starts.
- App exit never implies model unload. Only an explicit unload operation may
  stop a model endpoint.
- Recipe preparation never starts, stops, or adopts a model process. Each
  successful prepare registers a new generated profile; a running deployment's
  contract is never mutated.
- Discovery only reads: private subnets of at most /22, SSH banners, and a
  fixed read-only probe. Host keys are trusted only after the operator confirms
  the fingerprint.

## 4. Files that change together

- Domain field, enum, or transition: `controller/domain.py`, its unit test, the
  JSON parser, `docs/STATE_AND_ERROR_CATALOG.md`, and schema docs.
- JSON key or default: `controller/config.py`, the examples in
  `config/examples/`, `tests/unit/test_config.py`, `docs/CONFIGURATION.md`.
- API route: `controller/server.py`, `docs/API.md`, an HTTP test, and the UI
  module that calls it.
- Recipe template: the script, `controller/recipes/render.py` (identity
  fragments must match the launched command), and
  `tests/unit/test_recipes.py` (the contract tests run the real script).
- Safety invariant: the code, `docs/SECURITY.md`, an ADR in `docs/adr/`, and a
  scenario test written before the implementation.

## 5. Known traps

- Do not add `pkill -f`, broad process names, or port-only checks to runtime
  code.
- Do not duplicate launch flags outside the pinned script; profiles carry
  identity fragments as verification evidence only.
- Do not treat an inventory file as runnable; only a reviewed or generated
  profile is runnable.
- Do not silently adopt or kill an unmanaged process during reconciliation.
- Do not put private key contents, API tokens, or unredacted command output in
  JSON, operation events, fixtures, logs, or documentation.
- Controller node ids are configuration identity; never require them to equal
  the remote hostname.
- Live operations interrupt whatever model is serving on the selected nodes.
  Never run live-node commands from the test suite.
