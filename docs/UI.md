# UI contract

The browser UI in `controller/webui/` is static HTML, CSS and ES modules with no
build step or runtime dependency. The controller serves it from the same
origin as the API and stays the source of truth: the UI never derives observed
readiness from desired state. It tolerates an empty first boot, partial
snapshots, and either `snake_case` or `camelCase` field names. The full API is
in [API.md](API.md).

## Static shell

`controller.server` serves these files with `Cache-Control: no-cache` and a
restrictive same-origin Content Security Policy:

```text
/                 index.html
/styles.css
/app.js           entry module
/ui-core.js       fetch helper, formatting, normalization
/ui-dashboard.js  Dashboard
/ui-models.js     Models
/ui-operations.js Operations
/ui-setup.js      Setup wizard
/ui-recipes.js    Recipe builder
```

JSON responses use `Cache-Control: no-store`.

## Pages

| Page | Purpose |
| --- | --- |
| Dashboard | node status, observed model, unified memory and GPU utilization, recent operations; per-node refresh and load/unload under **Node details & actions** |
| Models | profiles with their origin badge (`reviewed` or `generated`), a **Loaded** badge, weights and reserved memory, provenance, presence and **Load** |
| Recipes | inspect a Hugging Face link, choose a vLLM release, placement and settings, prepare, then load |
| Operations | journal list with search, detail, redacted events, per-node logs, and cancel |
| Diagnostics | controller health, warnings, **Recover baseline**, and a redacted support context to copy |
| Setup | onboarding wizard: scan, host-key trust, identify, link check, save |

When `bootstrap.setup.setup_required` is true the UI shows a notice, the
header pill reads **Setup required** in the warning tone instead of
**Controller ready**, and Setup opens once instead of the Dashboard. A
`setup.config_error` is shown as a notice with its message.

### Setup

The wizard follows [Setup](SETUP.md) in three cards:

1. **Find nodes**: editable private subnets prefilled from `GET
   /api/v1/setup`, **Scan network**, a progress bar while the scan runs
   (polled every 0.7 s), and candidate rows with host name, address, evidence
   and a confidence badge. Likely Sparks are preselected. A host can also be
   added by address or name.
2. **Connect and identify**: per node, SSH user (prefilled with the
   controller host's login name from `default_ssh_user`, editable), port and
   key. The host-key step lists one key per line (key type and `SHA256:`
   fingerprint, wrapping monospace) with how it compares with the operator's
   `~/.ssh/known_hosts`: "matches your ~/.ssh/known_hosts", "differs from
   your ~/.ssh/known_hosts" (or revoked there), or "not in your
   known_hosts". A differing or revoked key adds a danger-toned warning.
   **Trust fingerprint…** opens a confirmation with the same list and
   warning that tells the operator to compare against
   `ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub` on the node's console.
   **Identify** shows facts, warnings and editable node ID, display name and
   model root. On `auth_failed` it shows the `ssh-copy-id` command with a copy
   button and states that the password is entered in the operator's terminal,
   never in the UI. **Create a controller SSH key** shows the new public key.
   **Check direct links** appears once two nodes are identified.
3. **Save the cluster**: counts identified nodes, offers **Replace** when a
   `nodes.json` exists, and asks for confirmation before writing it.

### Models

Each profile card shows its origin badge and, while a `ready` deployment uses
the profile, a **Loaded** badge. Long titles wrap beside the badges. Facts are
alias, format, quantization, context, mode, **Weights** (`weights_bytes`) and,
for generated profiles, **Reserved by recipe**: `reserved_memory_bytes` with
its share of the smallest node (`gpu_memory_utilization`). Generated recipes
that share weights differ in this reservation and in context. The description
of a generated profile is its provenance, for example
`Qwen/Qwen3-8B @ b968826d on vLLM 0.31.0`.

### Recipes

- A link field and **Inspect** show the resolved revision, weight size and
  format, quantization, parameters, maximum context, architecture, license,
  a gated badge, upstream warnings, and fit rows for one to four nodes.
- The form offers the release list (stable releases, with retry on error), a
  CUDA build (`default`, `cu129`), served model name, recipe name, node
  checkboxes with a head-node choice for multi-node placements, the bounded
  settings, and toggles for prefix caching, eager mode and trust remote code.
  Field groups align to the top of their grid row, so inputs line up even
  when the hints below them wrap differently.
- Recipe cards show release, node count, status (`draft`, `preparing`,
  `prepared`, `failed`), the pinned revision, alias, size and context, the last
  message, and that generated recipes stay unqualified. Actions: **Prepare**
  (with a confirmation naming the download size), **Load…** once prepared,
  **Edit as new**, and **Delete** (draft file only).
- While any recipe is preparing, the page refreshes every 4 s and refreshes
  the whole snapshot when a prepare finishes. Progress and cancel live on the
  Operations page.

## Request conventions

All versioned routes are under `/api/v1`. For every non-GET request the UI
sends an `Idempotency-Key` header it generates, unless the caller supplied
one; the service still validates and replays keys. Mutation responses are
`202` with an operation, which the UI accepts directly or wrapped in
`operation` or `data.operation`. Errors use the standard envelope; the UI
shows `message`, `remediation` and operation IDs, and never displays shell
commands, SSH key contents, tokens or unredacted environment values.

## Read routes the UI uses

- `GET /api/health`: rendered as available when `status` is `ok`; also
  carries `mode` and `setup_required`.
- `GET /api/v1/bootstrap`: the preferred single request. All collection keys
  are optional; a missing key means an empty collection. If bootstrap fails,
  the UI falls back to `GET /api/v1/nodes`, `GET /api/v1/models`,
  `GET /api/v1/profiles`, `GET /api/v1/deployments` and
  `GET /api/v1/operations?limit=50`, each accepting an array, `{"items": []}`
  or `{"data": {"items": []}}`.
- `GET /api/v1/telemetry`: polled about ten seconds after the previous
  request finishes, never overlapping, with a 30-second client timeout.
- `GET /api/v1/operations/{id}`: polled about every 1.8 s while an operation
  is active; after a reload the UI resumes from the operation list.
- `GET /api/v1/operations/{id}/logs`: per-node redacted log chunks with a
  cursor.
- `GET /api/v1/setup`, `GET /api/v1/setup/scans/{id}`,
  `GET /api/v1/setup/ssh-keys`, `GET /api/v1/recipes` and
  `GET /api/v1/catalog/vllm-releases` for the Setup and Recipes pages.

Node fields may arrive as `snake_case` or `camelCase` (`active_model_alias` or
`activeModelAlias`, `memory.total_bytes` or `memory.totalBytes`, and so on).

## Mutations the UI sends

- `POST /api/v1/deployments` with only `profile_id`, `node_ids` and an
  optional `confirmation`. If a selected node already has an active
  deployment the UI asks for a destructive confirmation that names every node
  the switch will stop: each selected node's observed deployment contributes
  all of its `node_ids`, so a node outside the selection is named too. The
  service enforces the conflict check independently.
- `DELETE /api/v1/deployments/{id}` for unload. Node actions use only the node
  resource's observed `deployment`; a null deployment never falls back to a
  historical placement, and an observed alias alone does not grant unload.
- `POST /api/v1/operations/{id}/cancel` from the Operations page. A running
  lifecycle operation answers `409 operation_not_cancellable` and stays
  visible; a running prepare is asked to stop.
- `POST /api/v1/nodes/{id}/refresh` and `POST /api/v1/reconcile`
  (**Refresh observations**).
- `POST /api/v1/recover` from Diagnostics after an explicit confirmation.
- Setup routes (`/setup/scans`, `/setup/host-keys`, `/setup/host-keys/trust`,
  `/setup/identify`, `/setup/links`, `/setup/ssh-keys`, `/setup/nodes`) and
  recipe routes (`/catalog/models/inspect`, `/recipes`,
  `/recipes/{id}/prepare`, `DELETE /recipes/{id}`).

## Telemetry display

Memory is the unified system memory shared by CPU and GPU (total minus
available), shown in GiB. GPU utilization is compute activity, not memory.
Missing values are **Unavailable**, never zero; simulated values are labelled
**Demo**. Readings older than 30 seconds or kept after a failure are
**Stale**; paused readings keep their values with a **Paused** label. Polling
stops when the page is hidden, the user leaves the Dashboard, or **Pause
monitoring** is selected; an in-flight probe may still finish. Metric updates
touch only the non-interactive part of each card, so focus and expanded
details survive. Telemetry never changes a readiness badge. The lifecycle
snapshot refreshes separately every 15 seconds while the page is visible.

## State and accessibility

- `desired_state` and observed state are kept apart; the observed state drives
  badges and available actions.
- An empty array is an empty state, not an endless spinner. A failed bootstrap
  keeps the shell usable.
- Network failures set the connection pill to **Controller offline** with
  retry guidance.
- Navigation uses buttons, destructive actions need confirmation, dialogs trap
  focus and close on `Escape`, and status regions use live announcements.
- `prefers-reduced-motion` disables non-essential transitions.

## Desktop launcher

`dgx-spark-controller-app` (`controller/launcher.py`) starts the local service
or reuses a healthy one on the port, then opens the UI in a pywebview window
or the default browser. It sends only `GET /api/health`. Closing the app
window, Cmd-Q, Ctrl-C or a signal stops only the service child the launcher
started; it never calls a deployment or unload route, so remote models keep
serving until an operator unloads them. See
[Install](INSTALL.md#what-closing-the-app-does).
