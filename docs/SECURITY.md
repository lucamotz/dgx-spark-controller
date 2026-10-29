# Security model

The controller is a single-user, local tool. It assumes a trusted operator
account on the controller host and is not designed for Internet exposure or
multi-user access.

## Trust boundaries

- **Browser and API input is untrusted.** Requests carry identifiers and
  bounded values only. The browser can never submit a shell command, script
  path, flag, environment variable, SSH option or arbitrary URL. System
  updates add two deliberate exceptions, the node user's sudo password and
  one local directory path; see [System updates](#system-updates).
- **The HTTP service binds to loopback only.** The server refuses any
  non-loopback bind address, sends no CORS headers, rejects `OPTIONS`, and
  serves a restrictive same-origin Content Security Policy. Every `/api/v1/`
  request must name `localhost` or a loopback address in its `Host` header
  (`403 invalid_host_header`), which defeats DNS rebinding. Update mutations require a JSON body sent as
  `application/json`, which another site cannot send without a preflight the
  service rejects. There is no authentication, so do not forward the port to
  other machines; for a headless host, use an SSH tunnel (see
  [Install](INSTALL.md#headless-machine)).
- **Nodes are a remote trust boundary** reached over SSH with fixed scripts.
- **Upstream catalogs are untrusted.** Hugging Face, GitHub and Docker Hub
  responses are parsed defensively and bounded.
- **Inference endpoints** are served by the model runtime on the nodes'
  networks. The controller does not add authentication to them.

## Network discovery

The setup wizard's scan cannot become a general network scanner:

- Targets must lie entirely inside 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16,
  100.64.0.0/10 or 169.254.0.0/16, each target holds at most 1,024 addresses
  (a /22), one request accepts at most eight targets, and only one scan runs
  at a time. Default targets skip VPN, container and virtual-machine
  interfaces.
- Probes are read-only: TCP connects, one SSH banner read (no key exchange,
  no authentication), reverse DNS, an unauthenticated `GET /v1/models` on
  ports 8000, 8888 and 11434 of hosts that answered on port 22, and an mDNS
  query that asks for unicast replies and never announces anything.
- Every probe has a deadline and a byte cap, HTTP probes ignore proxy
  settings, and everything a host returns is stripped of control characters
  and truncated before display.

## SSH

- **Host keys are pinned after explicit confirmation.** `ssh-keyscan` runs
  with a validated host and a timeout. Only keys whose `SHA256:` fingerprint
  the operator confirmed, and that the node still presents at confirmation
  time, are written to the controller's own `known_hosts` file (mode `0600`).
  `~/.ssh` is never modified. To show whether a presented key matches what
  the operator's ssh already trusts, the wizard only reads
  `~/.ssh/known_hosts` with `ssh-keygen -F <host> -f <file>` (argv, validated
  host); a differing or revoked key is shown as a warning.
- **Strict checking everywhere.** Identification and every lifecycle,
  prepare and update connection, relay tunnels included, use
  `StrictHostKeyChecking=yes` against
  `~/.ssh/known_hosts` plus the controller file. An unknown or changed host
  key fails closed and is never accepted on first use.
- **Key-only authentication.** SSH runs with `BatchMode=yes` and never
  authenticates with a password. Authorizing a key is a step the operator
  runs with `ssh-copy-id` in their own terminal. The only password the
  controller receives is the node user's sudo password, once, to install the
  update helper (see [System updates](#system-updates)); it never stores one.
- **Private key contents are never read.** Key discovery uses file metadata
  and the public `.pub` file only. Typed key paths must resolve inside the home
  directory and have no group or other permission bits. A generated controller
  key is created by `ssh-keygen`, never overwrites a file, and must end up
  mode `0600`.
- **Fixed argv.** `ssh`, `ssh-keyscan` and `ssh-keygen` run as argument
  lists. Host names are validated so that neither option injection (a
  leading `-`) nor `known_hosts` line injection is possible. Remote work is a
  fixed script sent on stdin to `bash -s`; interpolated values are
  shell-quoted and validated first. The update helper install is the one
  fixed remote argv, with the sudo password on stdin.

## Catalog requests

- Only `GET` requests over HTTPS to `huggingface.co`, `api.github.com` and
  `hub.docker.com`, without credentials in the URL and without an explicit
  port. Each redirect target is checked against the same allowlist before it
  is followed (at most five hops). CDN hosts that serve weight blobs are not
  allowlisted; the controller host never downloads weights.
- An `Authorization` header is forwarded on a redirect only to the same host,
  so a Hugging Face token never reaches GitHub or Docker Hub.
- Bodies are capped (8 MiB for API documents, 1 MiB for `config.json` and
  Docker Hub tag documents), compressed responses are not requested, and
  header values never appear in error messages.
- A pasted model link is reduced to a validated `org/name` and revision; no
  part of it becomes a URL host, path traversal, shell word or flag. Weights
  are pinned to the 40-character commit the Hub resolved, and images to the
  immutable `linux/arm64` manifest digest.

## Recipes and remote scripts

- **Allowlisted values.** A recipe draft has no free-form flag, path,
  environment variable or command. Every value that reaches a node
  environment file matches `[A-Za-z0-9._:/@+-]` and is checked again when the
  file is rendered.
- **Data, not code.** The pinned `prepare.sh`, `start.sh` and `stop.sh` read
  their environment file line by line against an explicit key allowlist and
  the same character set. They never `source` or evaluate it.
- **Pinned scripts.** The controller installs the templates unchanged and
  verifies each transferred file's SHA-256. Before every start and stop it
  runs `bash -n` and compares the script against the SHA-256 pinned in the
  profile.
- **Tokens stay on the nodes.** Gated weights are downloaded on each node with
  the token stored there by `hf auth login`, mounted read-only into the
  download container. The controller never reads it. An optional `HF_TOKEN`
  or `GITHUB_TOKEN` on the controller host is used only for catalog metadata
  requests.
- **`trust_remote_code` is opt-in.** Models that ship custom code are refused
  until the operator enables it explicitly; the UI asks to review the
  repository first.
- Hand-reviewed profiles follow the same rules: pinned start and stop hashes,
  exact environment values, image identity and process identity.
- **Completion canary.** The optional readiness canary is a fixed script run
  over SSH on the serving node. Its URL, alias, prompt, token limit and
  timeout come from the reviewed profile and are shell-quoted; the request
  body reaches `curl` on stdin. The response is read back bounded (64 KiB),
  parsed on the controller and discarded: only pass or fail, HTTP status,
  latency, completion token count and an allowlisted finish reason are
  journaled.

## Deleting recipes

Cleanup after a failed prepare and the recipe delete operation remove files
on nodes, so their reach is fixed in code, not taken from a request or a
profile file:

- **Only names the builder produces.** The browser sends a draft ID and one
  boolean. A recipe directory is addressed as the node user's home plus the
  fixed `.local/share/dgx-spark-controller/recipes/` subdirectory plus a
  generated profile ID of the exact shape `g-<slug>-<6 hex>`. Weights are
  addressed as a model root configured for that node in `nodes.json` plus a
  directory name of the exact shape `<org>--<name>-<8 hex>` that
  `render.model_dir_for` produces. The controller validates both before it
  renders a script and refuses anything else, including `..`, `.`, empty
  components, a trailing slash and the root directory. A profile whose
  recorded directory differs from that computed path is never removed.
- **Fixed scripts, literal targets.** Each removal is a fixed script template.
  The target is a single shell-quoted literal path, never a variable, so it
  cannot expand to `/` or a parent. `rm -rf --` runs only on that path and
  only after the script checked it: the target must not be a symlink, must be
  a directory, and its physical path (`cd -P` / `pwd -P`) must equal the
  physical home or model root plus the expected name. A symlinked component
  below the home directory or model root is refused. The node's `$HOME` must
  resolve to the same directory the controller computed.
- **Never under a running workload.** A recipe directory is refused while its
  prepare unit is active or a container named for the profile or its download
  exists (running or stopped), and when `docker` is installed but cannot be
  queried. A model directory is refused while any `dgx-prepare-*` unit or
  download container runs, or while a running container's command names the
  directory. Before any of this, the controller refuses the delete while a
  deployment of the profile is not stopped or a lifecycle operation for it is
  queued or running, and it holds the node leases while it removes files.
- **Shared artifacts stay.** Weights and the image are removed only on
  request, and only when no other profile (reviewed or generated) and no
  other prepared or preparing recipe on that node references the same model
  directory or image reference.
- **Images by digest only.** Image removal runs `docker image rm` without
  `--force`, only on the exact pinned `vllm/vllm-openai@sha256:<64 hex>`
  reference, and only when no container, running or stopped, was created
  from that image. The `dgx-spark-controller-cache` volume is removed with
  `docker volume rm` without `--force`, only after the last generated profile
  on that node is deleted and no container uses the volume.
- **Profiles last.** A generated profile file is deleted only after its
  directory is gone on every configured node, so a failed delete leaves a
  registered profile that can be deleted again, not node files that nothing
  refers to.

## System updates

An `update` operation installs packages and firmware as root on every node
and reboots it (see [Operations](OPERATIONS.md#update-the-cluster) and
[ADR-0007](adr/0007-controller-mediated-node-updates.md)). Its root reach is
fixed on the node:

- **One helper, five subcommands.** All root work goes through
  `/usr/local/sbin/dgx-spark-controller-update` (`root:root`, `0755`). The
  sudoers rule `/etc/sudoers.d/dgx-spark-controller-update` (`0440`, checked
  with `visudo -c` before an atomic rename) lets the node user run exactly
  `check`, `download`, `apply`, `status` and `reboot` without a password. The
  helper accepts exactly one argument and sets its own `PATH` and locale.
  `install` and `uninstall` are not in the rule.
- **What the rule grants.** Anyone who can act as the node user, including
  whoever holds the controller's SSH key, can install every pending update
  from the node's own apt sources and LVFS and reboot the node. They cannot
  choose a package, source, argument or command. Nothing else runs as root.
- **Verified before every use.** Each helper call is a fixed script that
  compares the installed helper's SHA-256 with the packaged copy and runs
  `sudo -n` only on a match. The read-only probe also requires `root:root`
  ownership; a differing helper is reported `outdated` and never run.
- **The sudo password, once (exception).** Installing the helper needs the
  node user's sudo password: DGX OS requires one for `sudo` and allows
  password-free updates only from a local desktop session, and the owner
  requires the controller to be deployable without manual node steps. The
  controller stages the helper in
  `~/.local/share/dgx-spark-controller/update-helper/` over BatchMode SSH,
  checks its SHA-256, runs `/usr/bin/sudo -S -k -p "" -- /bin/bash <staged
  copy> install` with the password on that SSH session's stdin, deletes the
  staged copy, and verifies the SHA-256 the installed helper reports
  (`helper_mismatch` otherwise). The password never appears in an argv. It
  is held in memory for that one request and never written to disk, the
  journal, logs or a response. It reaches `sudo` through the node account's
  session, so whoever controls that account at install time can observe it.
  Removal is manual on the node:
  `sudo /usr/local/sbin/dgx-spark-controller-update uninstall` deletes the
  helper, the rule and `/var/lib/dgx-spark-controller-update/`.
- **One local path (exception).** The update source directory is the one
  path the browser submits. It must be absolute, at most 1,024 characters
  and an existing, readable directory, and is stored resolved. Only the relay
  reads it: request paths are checked component by component (no `.`, `..`
  or empty part, a restricted character set), and a file whose resolved
  path leaves the directory is refused. The path never reaches a node or a
  remote command.
- **Loopback relay.** The relay binds `127.0.0.1` on an ephemeral port only
  while a check or update runs. A node reaches it only through
  `ssh -o ExitOnForwardFailure=yes -R 127.0.0.1:18877:127.0.0.1:<port> <node>
  'cat >/dev/null'`, bound to the node's loopback; the tunnel ends when the
  controller process that holds its stdin ends. Each relay session serves a
  random token, and the controller reads it through each node's port before
  any helper call, so another listener on port 18877 is never accepted.
  While the tunnel is open, any process on that node can use it within the
  same limits. The relay accepts absolute `http://` URLs without credentials,
  query or a port other than 80, for hosts that the nodes' apt sources name
  plus `cdn.fwupd.org` and `fwupd.org`. Upstream it uses the scheme the
  sources use, follows at most five redirects, each only to a host sharing
  the last two DNS labels of an allowed host and without an explicit port,
  refuses upstream addresses on loopback or link-local networks, never caches
  a directory listing, and caps each file at 8 GiB. `CONNECT` is allowed only
  in online mode, only to port 443 of an allowed host, for sources the helper
  keeps unchanged (below); TLS stays between the node and that host. In
  directory mode the relay opens no network connection. It never logs request
  lines.
- **Integrity on the node.** Neither the relay, its cache nor a source
  directory is trusted. The helper rewrites apt sources to `http://` only in
  a private copy under `/var/lib/dgx-spark-controller-update/`, and its apt
  configuration denies insecure repositories; apt verifies the signed indexes
  and package hashes against the node's keyrings. Sources with credentials in
  `/etc/apt/auth.conf*` (Ubuntu Pro) and sources apt trusts without a
  signature (`trusted=yes`, `allow-insecure=yes`, `allow-weak=yes`, or their
  deb822 forms) keep their URLs, so an `https://` one keeps end-to-end TLS
  through `CONNECT`; directory mode leaves them out. `fwupdmgr refresh`
  verifies the LVFS metadata signature, the helper checks each firmware
  cabinet's SHA-256 against that metadata before `fwupdmgr local-install`,
  and the helper's own downloads (LVFS metadata and cabinets) are capped at
  1 GiB per file.
- **Bounded evidence.** After a failed run the helper returns the last 30
  lines of `/var/log/dgx-spark-controller-update.log` (300 characters each).
  The journal keeps them in the event details on the controller host; the
  event message, which the browser sees, carries at most five of them after
  redaction. Operation results carry only
  counts, kernel and driver versions, deployment IDs, relay counters and the
  text-free canary evidence.

## Process safety

Process identity uses the executable and observed argv or container command,
plus the controller's ownership marker and container labels. Stops target a
named unit or an exact, labelled container. Broad `pkill`, shell globbing and
port-only ownership are prohibited. A process that does not match a managed
identity is reported as unmanaged and never stopped automatically. Recipe
`stop.sh` refuses to remove a container whose ownership label belongs to
another deployment.

## Data and logs

- The journal, configuration and logs live in per-user directories with mode
  `0700`; `nodes.json`, `known_hosts`, drafts, generated profiles, rendered
  environment files and `update-settings.json` are written atomically with
  mode `0600`.
- Operation events and API messages are bounded and redacted: absolute paths,
  secret-looking `KEY=value` pairs and command fragments are replaced before
  they reach the browser. Text with a shell fragment (`;`, `$(`, or `--`
  after a space) or a leading command word is replaced as a whole, so
  controller-authored messages avoid them. Prompt contents, private keys, tokens and full
  environments are never journaled.
- The HTTP server does not log request paths or headers.
