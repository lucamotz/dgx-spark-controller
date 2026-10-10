# Troubleshooting

Start with read-only checks. The error code and operation ID are the starting
point; the Operations page shows each operation's redacted events and per-node
logs. Do not retry a destructive operation with a hand-written command.

Where to look:

- The service log, `controller-app.log` in the log directory (see
  [Configuration](CONFIGURATION.md#file-locations)) when started by the
  launcher, or `journalctl --user -u dgx-spark-controller.service` for the
  Linux user service.
- `GET /api/v1/setup` for `config_error` and `reload_pending`.
- On a node, as the SSH user: `systemctl --user list-units 'dgx-*'` and
  `journalctl --user -u <unit>`.

Messages that contain paths or shell fragments are shown as
`[redacted path]` or `Remote command detail redacted.` in the UI. The detail
is on the node, in the unit journal or the prepare status file.

## Setup

| Symptom or code | Cause | Fix |
| --- | --- | --- |
| `409 setup_required` | no `nodes.json`, or it failed validation | run Setup; if `setup.config_error` is set, fix the file it names and restart |
| `reload_pending` stays true | a load, unload, recover or reconcile is still running | wait; the reload happens when it finishes |
| `scan_target_not_private`, `scan_target_too_large` | target outside the private ranges or larger than a /22 | enter the node's /24, or add the host by address |
| scan finds nothing | different subnet, SSH not listening, a firewall, or a VPN interface | add the host by address or name; check `ssh <user>@<host>` from a terminal |
| `host_key_scan_failed` | node off, SSH not on that port, or `ssh-keyscan` missing | check reachability and the OpenSSH client |
| `host_key_untrusted` | the host key has not been confirmed | fetch and trust the fingerprint in Setup |
| `host_key_changed`, `host_key_mismatch` | the node's host key changed (reinstall, or something in between) | see [Host key changed](#host-key-changed) |
| `auth_failed` | the key is not authorized on the node | run the shown `ssh-copy-id` command in your terminal |
| `ssh_key_permissions` | the private key is readable by others | `chmod 600 <key>` |
| identify hangs or fails with a passphrase key | `BatchMode=yes` cannot prompt | use a key without a passphrase or load it into `ssh-agent` |
| `node_not_identified` | identification is older than 30 minutes or the controller restarted | identify the node again, then save |

### Identify warnings

- **Docker is not usable by this user.** Add the user to the `docker` group,
  then start a new login session:

    ```bash
    sudo usermod -aG docker <user>
    ```

- **No NVIDIA runtime.** Install or repair the NVIDIA Container Toolkit so
  `docker info` lists the `nvidia` runtime.
- **systemd lingering is off.** Units the controller starts would stop when
  the SSH session ends:

    ```bash
    sudo loginctl enable-linger <user>
    ```

- **The systemd user manager is not available over SSH.** Usually fixed by
  enabling lingering; check with `systemctl --user is-system-running` in an
  SSH session.
- **Does not look like a DGX Spark.** The host lacks a GB10 GPU on aarch64.
  Continue only if you know it is compatible.

### Host key changed

A changed host key is expected after reinstalling a node and suspicious
otherwise. Confirm the new fingerprint on the node's console:

```bash
ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
```

Then remove the stale entries and trust the key again in Setup. For a
non-default port use `'[host]:port'`:

```bash
ssh-keygen -R <host> -f "<config dir>/known_hosts"
ssh-keygen -R <host>        # only if ~/.ssh/known_hosts has the old key
```

## Recipes and prepare

| Symptom or code | Cause | Fix |
| --- | --- | --- |
| `model_gated` | the repository requires accepted terms | accept them on huggingface.co, set `HF_TOKEN` for the controller, run `hf auth login` on every node |
| `model_not_found` | wrong link, or a private repository without a token | check the link; set `HF_TOKEN` |
| `unsupported_weight_format` | GGUF or `.bin` only | choose a safetensors repository |
| `host_not_allowed` | `config.json` is stored in Git LFS | not supported; choose another repository |
| `upstream_rate_limited` | GitHub API limit | wait, or set `GITHUB_TOKEN` |
| `runtime_image_unavailable` | no `linux/arm64` image for that release or CUDA variant | choose another release or variant |
| `port_not_allowed`, `model_root_mismatch` | the placement does not fit `nodes.json` | pick a port inside every node's range; use the same first model root |
| `home_directory_mismatch` | nodes use different users or home directories | use the same user name on every node |
| `recipe_busy` | the draft is being prepared | wait or cancel the prepare |
| `prepare_failed`: not enough free disk space | weights + 5% + 2 GiB do not fit | free space in the model root |
| `prepare_failed`: runtime image pull failed | no registry access, or Docker not usable | check `docker pull` on the node |
| `prepare_failed` on a multi-node placement, message redacted | the release's `vllm serve` lacks `--nnodes` (see the status file) | choose a newer release, or one node |
| `prepare_failed`: weight download failed | gated model without a token on that node, or network | run `hf auth login` on that node; prepare again |
| `prepare_failed`: weight file size mismatch | incomplete or changed download | prepare again; it resumes |
| `linger_required` | lingering is off for the node user, so the prepare unit would stop with the SSH session | run the `sudo loginctl enable-linger <user>` command from the remediation on that node, then prepare again |
| `prepare_unobservable` | the prepare unit stopped reporting: SSH, lingering, or the unit died | check SSH and lingering, then prepare again |
| `prepare_timeout` | longer than 6 hours | prepare again; complete files are reused |
| `prepare_mismatch` | nodes pulled different images or hold different weights | prepare again |
| `profile_not_found` when loading a just-prepared recipe | the controller reloads profiles only when no lifecycle operation runs (`reload_pending`) | wait for the running operation to finish, then load |
| `prepare_interrupted` (`needs-attention`) | after a restart the controller could not read a node's prepare state | fix SSH to that node, then prepare again; the old unit may still be running and downloads resume |
| `prepare_interrupted` (`failed`) | after a restart a node's prepare unit was gone without a result | prepare again; complete downloads are reused |
| `prepare_cleanup_incomplete` event | a failed prepare's recipe directory could not be removed on a node | once its unit has stopped, remove `~/.local/share/dgx-spark-controller/recipes/<profile-id>/` on that node |
| `recipe_prepared` | `DELETE` on a recipe with generated profiles | use **Delete** on the recipe card, which runs the journaled delete |
| `profile_in_use` | a deployment of the recipe's profile is loaded, or a lifecycle operation for it is queued or running | unload it and wait for the operation, then delete again |
| `recipe_delete_incomplete` | a node refused or could not remove a profile's directory (see the `profile_kept` event) | fix the cause (unreachable node, leftover container), then delete again |
| `delete_interrupted` | the controller restarted during a delete | delete again; finished steps are skipped |

To inspect a prepare on a node (IDs are in the operation's events and the
profile ID):

```bash
systemctl --user status dgx-prepare-<prepare-id>.service
journalctl --user -u dgx-prepare-<prepare-id>.service
cat ~/.local/share/dgx-spark-controller/recipes/<profile-id>/prepare-<prepare-id>.status
systemctl --user stop dgx-prepare-<prepare-id>.service   # stop a leftover unit
```

## Loading and unloading

| Code | Likely cause | Safe checks | Recovery |
| --- | --- | --- | --- |
| `preflight_failed`, `script-hash-mismatch` | a pinned script, image or model file changed or is missing | compare the hash and files named in the events | restore the exact bytes, or prepare the recipe again |
| `cluster_start_failed`, `cluster_launcher_failed`, `cluster_container_exited` | a rank failed to start or exited | the operation's per-node logs (the rank container's log while it exists) and `journalctl --user -u dgx-cluster-<deployment-id>.service` on each node | fix the cause; rollback already removed the new ranks |
| `readiness_timeout`, `health-check-failed` | still loading, crashed, or too slow | the per-node operation logs | raise `startup_timeout_s`, lower `max_model_len` or `gpu_memory_utilization`, or try eager mode, then prepare again |
| `model-alias-mismatch` | another process serves the port | `curl http://<node>:<port>/v1/models` | stop that workload yourself; the controller will not |
| `unmanaged_conflict`, `unmanaged_deployment` | a process the controller does not own uses the node | identify it on the node | stop it deliberately, then reconcile |
| `memory_release_unverified`, `memory-not-recovered` | memory did not recover after stop | `free -g` on the node | wait and reconcile; do not start another large model |
| `operation_not_cancellable` | the operation already started | follow it to the end | reconcile afterwards if needed |
| `idempotency_conflict` | a key was reused for another request | none | use a new key |
| `rollback_incomplete`, `needs-attention` | the previous placement could not be restored or proven | node observations and events | reconcile, then recover or unload deliberately |

## Escalation rule

If a process is not a managed unit, if memory is ambiguous, or if a rollback
cannot be proven, stop mutating and investigate on the node. A healthy port or
a plausible log line is not enough evidence to continue.
