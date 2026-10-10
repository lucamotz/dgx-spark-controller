# Security model

The controller is a single-user, local tool. It assumes a trusted operator
account on the controller host and is not designed for Internet exposure or
multi-user access.

## Trust boundaries

- **Browser and API input is untrusted.** Requests carry identifiers and
  bounded values only. The browser can never submit a shell command, script
  path, flag, environment variable, SSH option or arbitrary URL.
- **The HTTP service binds to loopback only.** The server refuses any
  non-loopback bind address, sends no CORS headers, rejects `OPTIONS`, and
  serves a restrictive same-origin Content Security Policy. There is no
  authentication, so do not forward the port to other machines; for a headless
  host, use an SSH tunnel (see [Install](INSTALL.md#headless-machine)).
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
- **Strict checking everywhere.** Identification and every lifecycle and
  prepare connection use `StrictHostKeyChecking=yes` against
  `~/.ssh/known_hosts` plus the controller file. An unknown or changed host
  key fails closed and is never accepted on first use.
- **Key-only authentication.** SSH runs with `BatchMode=yes`; the controller
  never asks for, receives or stores a password. Authorizing a key is a step
  the operator runs with `ssh-copy-id` in their own terminal.
- **Private key contents are never read.** Key discovery uses file metadata
  and the public `.pub` file only. Typed key paths must resolve inside the home
  directory and have no group or other permission bits. A generated controller
  key is created by `ssh-keygen`, never overwrites a file, and must end up
  mode `0600`.
- **Fixed argv.** `ssh`, `ssh-keyscan` and `ssh-keygen` run as argument
  lists. Host names are validated so that neither option injection (a
  leading `-`) nor `known_hosts` line injection is possible. Remote work is a
  fixed script sent on stdin to `bash -s`; interpolated values are
  shell-quoted and validated first.

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
  `0700`; `nodes.json`, `known_hosts`, drafts, generated profiles and rendered
  environment files are written atomically with mode `0600`.
- Operation events and API messages are bounded and redacted: absolute paths,
  secret-looking `KEY=value` pairs and command fragments are replaced before
  they reach the browser. Prompt contents, private keys, tokens and full
  environments are never journaled.
- The HTTP server does not log request paths or headers.
