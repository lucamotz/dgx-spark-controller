# Architecture

The controller is a local, loopback-only Python service (standard library only)
with a zero-build browser UI. It reaches DGX Spark nodes over SSH, keeps a
durable operation journal in SQLite, and is the single owner of model
lifecycles on the nodes it manages.

## Module map

```text
controller.launcher           desktop launcher: native window or browser; owns only its local child
controller.main               CLI: parses options, builds ServiceHost and the HTTP server
controller.server             loopback HTTP routes and static UI files
controller.host               ServiceHost: setup mode, hot reload, long-lived services
├─ controller.app             ControllerApplication: lifecycle resources and one lifecycle worker
│  ├─ controller.projection   profile + node IDs -> DeploymentSpec
│  ├─ controller.orchestrator load, unload, recover; leases, readiness, rollback
│  ├─ controller.reconcile    restart and on-demand reconciliation from fresh observations
│  ├─ controller.telemetry    shared, rate-limited capacity sample cache
│  └─ controller.transport    NodeTransport protocol
│     ├─ fake                 deterministic in-memory nodes
│     └─ managed_ssh          observations and lifecycle over SSH
│        ├─ ssh               bounded `ssh ... bash -s` execution of fixed scripts
│        └─ runners.systemd_user  named `systemd --user` units for single-node profiles
├─ controller.discovery       onboarding wizard backend
│  ├─ network, scanner, mdns  private-subnet scan and candidate ranking
│  ├─ hostkeys                ssh-keyscan, fingerprints, controller known_hosts
│  ├─ sshkeys                 key discovery and generation (never reads private keys)
│  ├─ identify                read-only identity probe and link check
│  └─ setup                   SetupService: wizard state; writes nodes.json
└─ controller.recipes         recipe builder
   ├─ spec, store             validated drafts, one JSON file per draft
   ├─ catalog_client          cached access to controller.catalog
   ├─ render                  remote environment files and the generated profile
   ├─ nodeops                 fixed prepare primitives over SSH (and a fake)
   ├─ service                 RecipeService: drafts and the prepare worker
   └─ templates/vllm-release  pinned prepare.sh, start.sh, stop.sh
controller.catalog            allowlisted HTTPS JSON: huggingface, vllm_releases, http
controller.config, domain     strict JSON parsing into immutable records
controller.operations, persistence  operation records and the SQLite journal
controller.paths              per-OS config, data and log directories
controller.webui              static HTML, CSS and ES modules
```

## Dependency direction

```text
server -> host -> app -> projection / orchestrator / reconcile -> persistence + NodeTransport
          host -> discovery.setup -> discovery.{network, scanner, hostkeys, sshkeys, identify}
          host -> recipes.service -> recipes.{render, nodeops, store} + catalog_client -> catalog
config JSON -> config -> domain records -> projection -> DeploymentSpec
```

- `domain` is side-effect free. `config` reads local JSON and never connects to
  a node. `paths` only resolves directories (and creates them on request).
- Transports, runners, discovery and recipe node operations never import the
  server or the UI. The browser reaches remote work only through journaled
  operations or fixed, read-only wizard probes.
- `catalog.http` is the only module that opens outbound HTTPS connections.
- Persistence stores records and events; it never decides whether a process is
  safe to stop.

## Ownership boundaries

| Concern | Owner | Must not |
| --- | --- | --- |
| JSON syntax, defaults, unknown-key rejection | `controller.config` | connect to nodes or run commands |
| state vocabulary and immutable contracts | `controller.domain` | perform I/O or infer observed state |
| process lifetime, setup mode, reloads | `controller.host` | rebuild while a lifecycle operation is active |
| lifecycle mutation | `controller.orchestrator` | accept commands, paths or flags |
| desired/observed reconciliation | `controller.reconcile` | resolve an operation that is still running in this process |
| SSH execution | `controller.transport` | expose raw SSH details to the API or UI |
| single-node process start/stop | `controller.runners` | broad `pkill`, port-only identity, unpinned scripts |
| node onboarding | `controller.discovery` | authenticate during a scan, accept passwords, read private keys |
| model and release metadata | `controller.catalog` | contact hosts outside the allowlist or download weights |
| recipe drafts and prepare | `controller.recipes` | start, stop or adopt a model process |
| operation history | `controller.persistence` | store secrets or rewrite events |
| local window | `controller.launcher` | call any lifecycle route |
| inference | OpenAI-compatible clients | start, stop or restart model servers |

## Service host and setup mode

`ServiceHost` owns the journal, the setup wizard, and the recipe builder for
the whole process. It builds a `ControllerApplication` only when `nodes.json`
exists and parses:

- **Setup mode.** Without a valid `nodes.json` there is no application. Setup,
  catalog and recipe routes work; lifecycle routes answer `409
  setup_required`; `GET /api/v1/bootstrap` returns empty collections and a
  `setup` object. An invalid file is reported as `setup.config_error` instead
  of stopping the service.
- **Hot reload.** Saving nodes in the wizard, or a prepare registering a
  generated profile, requests a rebuild. The host rebuilds only while no load,
  unload, recover or reconcile operation is queued or running; until then
  `setup.reload_pending` is true. A rebuild reloads `nodes.json` and every
  profile directory, does not run startup reconciliation again, and refreshes
  node observations in the background. The journal, wizard state and the
  prepare worker survive rebuilds.

Profiles are loaded from the hand-reviewed directory (`<config>/profiles`) and
the generated directory (`<data>/profiles`); see
[Configuration](CONFIGURATION.md#file-locations).

## Desired and observed state

Desired state is the profile and placement an operator requested. Observed
state is measured on the node and endpoint. A deployment is `ready` only when
all readiness facts agree:

```mermaid
flowchart TD
  P["process or container identity"] --> R{all facts pass?}
  H["HTTP /health = 200"] --> R
  M["/v1/models exact alias"] --> R
  R -->|yes| READY["observed ready"]
  R -->|no| FAIL["failed or degraded; never ready"]
```

When a profile enables `readiness.completion_canary`, a load additionally
requires one bounded chat completion to succeed before it commits `ready`; see
[Operations](OPERATIONS.md#completion-canary).

A matching managed process may be adopted as observed state after a restart. A
process that does not match is `unmanaged` and is never stopped automatically.

## Load/switch state machine

```mermaid
stateDiagram-v2
  [*] --> planned
  planned --> preflighting
  preflighting --> stopping_conflicts
  stopping_conflicts --> starting_workers
  stopping_conflicts --> starting_head
  stopping_conflicts --> loading
  starting_workers --> starting_head
  starting_head --> loading
  loading --> ready
  preflighting --> failed
  stopping_conflicts --> failed
  starting_workers --> failed
  starting_head --> failed
  loading --> failed
  ready --> draining
  draining --> stopping
  stopping --> stopped
  stopping --> unknown
  failed --> preflighting
  unknown --> preflighting
```

`controller.domain.can_transition_deployment` freezes these edges. The
orchestrator persists each transition before the next side effect.

## Deployment modes

| Mode | Backend | Placement | Launch |
| --- | --- | --- | --- |
| `independent` | llama.cpp | one node | an allowlisted, hash-pinned script in a named unit `dgx-model-<id>.service` |
| `shared-vllm`, `head-script` | vLLM | two or more nodes | one hand-reviewed launcher on the control node starts every rank |
| `shared-vllm`, `per-node` | vLLM | one or more nodes | the controller starts the same pinned script on each node |

`shared-llama-rpc` is reserved in the vocabulary but not implemented.

Shared modes are one transaction: the orchestrator holds a cluster lease plus
per-node leases, preflights every node before stopping anything, starts worker
roles before the head, stops the head before workers, and records the
previous placement so a failed load can be rolled back on every node. A
deployment never runs on part of its placement: a load that replaces one
extends its leases to the replaced deployment's nodes outside the requested
placement and stops, verifies and, on rollback, restores it on all of them
(see [Operations](OPERATIONS.md#switching-across-placements)).

### Per-node launch contract

Generated recipes always use `launch_mode: per-node`. The profile carries a
profile-wide `required_environment` and one `node_environment` overlay per
allowlisted node (`NODE_RANK`, `VLLM_HOST_IP`, `MODEL_ROOT`). For each
assignment, workers first, the transport:

1. Renders a mode-`0600` overlay `.env.dgx-controller-<profile-id>` in the
   recipe's working directory on that node from the installed `recipe.env`
   plus that node's values.
2. Writes the node's ownership marker
   (`$XDG_STATE_HOME/dgx-controller/<deployment-id>.owner`).
3. Verifies `start.sh` with `bash -n` and its pinned SHA-256, and checks every
   expected `KEY=value` line of the overlay.
4. Starts `start.sh` with `ENV_FILE` set, in the unit
   `dgx-cluster-<deployment-id>.service`.

Each node reports its own launcher outcome, so a failed rank fails the load
promptly. Unload and recovery run the pinned, hash-checked `stop.sh` on each
node, head first; it removes only the container whose ownership label matches
the deployment. Nodes never need SSH access to one another.

The `head-script` mode runs one reviewed launcher on the control node, which
starts the worker before the head. It requires at least two nodes. The example
profile in `config/examples/profiles/` uses it.

## Prepare runs beside the lifecycle

A recipe `prepare` is a journaled operation (`action: prepare`, phases
`validating` → `preparing` → `committing` → `complete`) executed by the recipe
service's own single worker, not by the lifecycle worker:

- It takes no cluster or node lease and never starts, stops or adopts a model
  process. A long download therefore never blocks loading or unloading the
  current model.
- Its node-side work runs in transient `dgx-prepare-<id>.service` units and
  writes only to the recipe directory, the model root and the Docker image
  store.
- The reconciler receives the set of prepare operations that are alive in
  this process and skips them, so a running prepare is never marked
  interrupted. Startup cleanup of queued work also skips prepare operations.
- A controller restart ends a running prepare as `needs-attention`
  (`operation_interrupted`) and marks its draft `failed`. Preparing again
  reuses complete downloads.

The generated profile becomes loadable only after the host reloads, and
loading it goes through the ordinary orchestrated lifecycle.

## Concurrency and restart

The journal has one cluster lock plus per-node leases, renewed by a lease
keeper during long SSH work. An operation may extend its leases once it knows
the full set of nodes it will stop; the extension is all-or-nothing and
refuses before any mutation if another operation holds one of them. A request with a known idempotency key returns
the existing operation; the same key for a different action is a conflict.
On restart, running lifecycle operations are resolved by reconciliation:
proven outcomes succeed, everything else becomes `needs-attention`. Queued
lifecycle operations are cancelled.

Closing the UI, the launcher, or the service never unloads a remote model.

## Telemetry

`controller.telemetry.TelemetryCache` shares read-only capacity samples across
browser tabs and serializes collection. Each node is sampled at most once per
ten seconds; failures are cached too. The SSH probe reads `/proc/meminfo` and
one `nvidia-smi` utilization value and never touches readiness. See the
[API contract](API.md#live-capacity-monitoring).

## Node identity versus host name

The controller ID (for example `spark-a`) is stable configuration identity and
appears in profile allowlists. The host name a node reports is an observed
fact. The wizard suggests an ID derived from the host name, but the two are
never required to match.
