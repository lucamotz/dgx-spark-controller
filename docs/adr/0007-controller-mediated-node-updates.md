# ADR-0007: controller-mediated node updates

- Status: accepted
- Date: 2026-10-29

## Context

DGX OS nodes need regular package and firmware updates: kernel, NVIDIA driver,
CUDA, DGX OS packages and device firmware. Most of them take effect only after
a reboot and change the runtime under a serving model, so updating a node
interrupts its endpoint. By hand it is an SSH session, `sudo`, a reboot and a
reload per node, outside the controller's leases and journal.

DGX OS requires a password for `sudo` and allows password-free updates only
from a local desktop session. The nodes may have no Internet access. The owner
requires the controller to be deployable without manual steps on the nodes.

Two frozen contracts stand in the way: the controller never receives a
password ([Security](../SECURITY.md#ssh)), and the browser never submits a
path.

## Decision

1. **One journaled `update` operation.** It runs on the lifecycle worker and
   holds the cluster lease and every node lease from its first check until
   the deployments it stopped are restored. It refuses while a recipe
   operation runs, a node is unreachable, an unmanaged workload runs, a
   managed deployment is not ready, or a serving deployment fails preflight.
   It downloads every update while the model keeps serving, then stops the
   serving deployments, installs, reboots the nodes that installed something,
   and restores the deployments through preflight, readiness and the
   completion canary. A node whose install fails is not rebooted and does not
   hold back the others. Nothing is retried.
2. **A root-owned helper with five password-free subcommands.** The packaged
   `dgx-spark-controller-update.sh` is installed at
   `/usr/local/sbin/dgx-spark-controller-update`. A sudoers rule lets the node
   user run exactly `check`, `download`, `apply`, `status` and `reboot` without
   a password. The helper accepts its subcommand and nothing else. The
   controller compares the installed helper's SHA-256 with the packaged copy
   before every `sudo` call and never runs a differing helper.
3. **The sudo password, once (exception to the no-password contract).** The
   Updates page asks for the node user's sudo password only to install the
   helper. The controller stages the helper over key-authenticated SSH, runs
   `/usr/bin/sudo -S -k -p "" -- /bin/bash <staged copy> install` with the
   password on that SSH session's stdin, and verifies the installed helper's
   SHA-256. The password is held in memory for that one request and is never
   written to disk, the journal, logs or a response.
4. **The controller relays every download.** For each check or update the
   controller serves a loopback HTTP relay, and each node reaches it at
   `127.0.0.1:18877` through an SSH reverse tunnel bound to the node's
   loopback. A per-session token proves each tunnel reaches this relay. The
   relay serves only hosts the nodes' apt sources name plus LVFS. Integrity
   is verified on the node by apt and fwupd, so neither the relay nor its
   source is trusted.
5. **A local directory as an offline source (exception to the no-path
   contract).** Instead of the Internet, the relay can serve a directory on
   the controller host laid out as `<host>/<path>`, such as an exported relay
   cache or an apt-mirror tree. The browser submits its path in the update
   settings. The relay only reads it, below its resolved location, and the
   path never reaches a remote command.
6. **The schedule runs on the controller host.** Weekdays and a local time;
   each slot starts at most one operation, only within an hour after it, and
   never for a slot before the settings were saved.

## Alternatives

- **Install the helper and sudoers rule by hand on every node.** Rejected:
  the owner requires deployment without manual node steps, and a hand-made
  rule drifts from the helper the controller verifies.
- **Store the sudo password and use it for every update.** Rejected: a stored
  root password on the controller host. The helper bounds what the
  password-free path can do.
- **`NOPASSWD: ALL` for the node user.** Rejected: the controller's SSH key
  would be full root on every node.
- **unattended-upgrades or the DGX OS updater on each node.** Rejected: they
  install under a serving model outside the leases and cannot restore the
  deployment, and the desktop updater needs a local session.
- **Let the nodes download from the Internet.** Rejected: nodes may have no
  Internet access, and one relay lets an offline directory replace the
  Internet without a node-side change.
- **Install without stopping the model.** Rejected: driver, kernel and
  firmware updates need a reboot, and a model serving on a half-updated
  runtime cannot be proven ready.

## Consequences

- The controller receives one secret and the browser submits one path. Both
  exceptions are stated in [Security](../SECURITY.md#system-updates).
- Anyone who can act as the node user, including whoever holds the
  controller's SSH key, can install pending updates and reboot that node
  without a password. Nothing else runs as root.
- The password reaches `sudo` through the node account's session, so
  whoever controls that account at install time can observe it.
- A controller release that changes the helper changes its SHA-256: every
  node reports `outdated` and updates refuse until the helper is installed
  again with the sudo password.
- An update interrupts the endpoint for the install, reboot and reload time.
  A failed download interrupts nothing.
- Port 18877 on each node's loopback must be free during checks and updates.
- Scheduled updates need the controller host running and awake at the slot.
- The relay cache grows with every online update; the controller never
  prunes it.
- Sources that need credentials, such as Ubuntu Pro ESM, and sources apt
  trusts without a signature work only online.
