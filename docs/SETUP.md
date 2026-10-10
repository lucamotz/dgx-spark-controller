# Connect your cluster

The **Setup** page is an onboarding wizard. It finds DGX Spark nodes on your
network, pins their SSH host keys after you confirm the fingerprints,
identifies each node with a read-only probe, and writes `nodes.json`. Nothing
changes on a node during setup. On the controller host the wizard writes only
`nodes.json`, the controller `known_hosts` file and, if you ask for one, a
controller SSH key.

The controller starts in setup mode whenever `nodes.json` is missing. In setup
mode the Dashboard, Models and Operations pages stay empty and lifecycle API
routes answer `409 setup_required`. Saving the wizard reloads the controller
without a restart.

## Prerequisites

On the controller host:

- The OpenSSH client tools: `ssh`, `ssh-keyscan`, `ssh-keygen` and
  `ssh-copy-id`.
- Network reach to the nodes' management addresses.

On every node (DGX OS, or another GB10 system such as the ASUS Ascent GX10):

- SSH access for one Linux user. The controller logs in with a key only and
  never asks for or stores a password.
- Docker usable by that user without `sudo`:

    ```bash
    sudo usermod -aG docker "$USER"   # then log out and back in
    docker info >/dev/null && echo ok
    ```

- The NVIDIA Container Toolkit, so `docker info` lists the `nvidia` runtime.
  DGX OS normally includes it.
- Lingering for that user, so controller-started `systemd --user` units keep
  running after the SSH session ends. Recipe preparation refuses to start on
  a node without it (`linger_required`):

    ```bash
    sudo loginctl enable-linger <user>
    ```

- For a multi-node placement: the same user name and home directory on every
  node, the same first model root (default `~/models`), and the direct
  ConnectX-7 link configured with an IPv4 address on each end.
- For gated or private Hugging Face models: `hf auth login` on every node. The
  weights are downloaded on the nodes with the token stored there
  (`~/.cache/huggingface/token`); the controller never reads it.
- Free disk space in the model root for the models you plan to prepare.

## Step 1: Find nodes

The wizard proposes scan targets from your local private networks. A network
larger than a /22 is narrowed to the /24 that contains your own address. You
can edit the list (comma- or space-separated, up to eight targets) or add a
host by address or name and skip the scan.

The scan is bounded and read-only:

- Every target must lie inside 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16,
  100.64.0.0/10 or 169.254.0.0/16 and hold at most 1,024 addresses (a /22).
  Other targets are rejected with `scan_target_not_private` or
  `scan_target_too_large`. VPN, container and virtual-machine interfaces are
  not proposed.
- It TCP-connects to port 22 on every address, skipping this computer's own
  addresses. For hosts that answer, it reads the SSH identification banner,
  resolves reverse DNS, and sends `GET /v1/models` to whichever of ports 8000,
  8888 and 11434 are open. A short mDNS query (answered by unicast) adds host
  names. It never authenticates, and every probe has a deadline and a size cap.
- Only one scan runs at a time; **Cancel scan** stops it.

Results are ranked by evidence. Factory hostnames such as `spark-xxxx` or
`gx10-...` give `high` confidence; a generic `dgx` or `spark` name, or an
Ubuntu SSH banner together with a served model, gives `medium`. Likely Sparks
are preselected.

## Step 2: Connect and identify

For each selected host, enter the SSH user and port and choose a key.

### Choose a key

The key list contains, in this order:

- The key NVIDIA Sync created when it paired with a Spark (`nvsync.key`), if
  present:

    | OS | Location |
    | --- | --- |
    | macOS | `~/Library/Application Support/NVIDIA/Sync/config/nvsync.key` |
    | Linux | `$XDG_CONFIG_HOME/NVIDIA/Sync/config/nvsync.key` (default `~/.config/...`) or `$XDG_DATA_HOME/NVIDIA/Sync/config/nvsync.key` (default `~/.local/share/...`) |

- Private keys in `~/.ssh` that have a matching `.pub` file.
- A controller key at `<config dir>/ssh/id_ed25519`, once created.

**Create a controller SSH key** runs `ssh-keygen -t ed25519` without a
passphrase, writes the key with mode `0600`, never overwrites an existing
file, and shows the public key for you to copy. The controller lists keys by
file metadata and reads only `.pub` files; it never reads private key
contents.

The controller never prompts for a passphrase (`BatchMode=yes`). Use a key
without a passphrase, or one that is loaded into an `ssh-agent` the controller
process can reach.

### Confirm the host key

**Fetch host key** runs `ssh-keyscan` and shows each key's `SHA256:`
fingerprint. Before you press **Trust fingerprint**, compare it on the node's
own console (not over the network you are verifying):

```bash
ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
```

Trusting fetches the keys again and pins only keys whose fingerprint you
confirmed into the controller `known_hosts` file (`<config dir>/known_hosts`).
A key that changed in between is rejected with `host_key_mismatch`. Every
later SSH connection uses `StrictHostKeyChecking=yes` against
`~/.ssh/known_hosts` plus that file, so an unknown or changed host key fails
closed. A host already present in your `~/.ssh/known_hosts` can be identified
without this step.

### Authorize the key

If identification reports `auth_failed`, the wizard shows an `ssh-copy-id`
command such as:

```bash
ssh-copy-id -i ~/.ssh/id_ed25519.pub spark@192.0.2.10
```

Run it in your own terminal. It asks for the node password there; the password
never passes through the controller. Then press **Identify** again.

### Identify

**Identify** runs one fixed read-only probe over SSH. It reads the host name,
OS, architecture, DMI product name, GPU and driver (`nvidia-smi`), CUDA
version, memory, free disk in the home directory, Docker version and runtimes,
the `systemd --user` state, lingering, IPv4 interfaces with their drivers and
link speeds, RDMA devices, and whether `~/models` and a Hugging Face token file
exist. It installs and changes nothing.

The result shows whether the host is a DGX Spark (GB10 GPU on aarch64) and any
warnings, for example Docker not usable by the user, no NVIDIA runtime,
lingering off, or no `systemd --user` manager. Review the suggested node ID
(derived from the host name), display name, and model root (`<home>/models`).
Addresses on ConnectX interfaces (driver `mlx5_*`) other than the management
address and link-local addresses become the node's `cluster_addresses`.

An identification is valid for 30 minutes and is kept in memory only. Identify
again after a controller restart.

### Check direct links

With two or more identified nodes, **Check direct links** pings every other
node's cluster addresses from the selected node (two pings, one-second
timeout) and reports each as reachable or unreachable.

## Step 3: Save

**Save configuration** writes `nodes.json` in the controller config directory
(see [Configuration](CONFIGURATION.md#file-locations)) with mode `0600`. Only
nodes identified in this session can be saved; their SSH user, address,
cluster addresses, labels and capabilities come from the identification, not
from the save request. New nodes are merged into an existing file by node ID and management
address unless you select **Replace**. An existing file is first copied to
`nodes.json.bak-<epoch>`.

Saved nodes get the endpoint port range 8000–8999. The controller reloads as
soon as no load, unload, recover or reconcile operation is running. Remote
models keep running.

Next: build and prepare a model in [Recipes](RECIPES.md).

## Editing nodes later

Run the wizard again to add nodes, or edit `nodes.json` by hand (the format is
in [Configuration](CONFIGURATION.md#nodes-document)) and restart the
controller. An invalid file does not crash the service: it stays in setup mode
and `GET /api/v1/setup` reports `config_error`.

## Fake mode

`--mode fake` replaces discovery with two simulated Sparks, so you can walk
through the wizard without a network:

```bash
DGX_CONTROLLER_HOME=$(mktemp -d) .venv/bin/python -m controller --mode fake --open-browser
```

Generating a controller key in fake mode still runs `ssh-keygen`, inside the
temporary `DGX_CONTROLLER_HOME`.
