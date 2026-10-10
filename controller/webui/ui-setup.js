// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

// Onboarding wizard: find Sparks on the LAN, confirm host keys, identify them
// with a read-only probe, check direct cluster links, and save nodes.json.
// Passwords never enter the controller; key authorisation is a terminal step
// the operator runs themselves with the shown ssh-copy-id command.

import { escapeHTML, formatBytes } from "./ui-core.js";

const wizard = {
  overview: null,
  scan: null,
  candidates: [],
  selected: new Map(),
  keys: [],
  busy: new Set(),
  generatedPublicKey: "",
  replace: false,
};

let ctx = null;
let root = null;
let scanTimer = null;

const KNOWN_HOSTS_NOTES = {
  match: { tone: "success", text: "matches your ~/.ssh/known_hosts" },
  mismatch: { tone: "danger", text: "differs from your ~/.ssh/known_hosts" },
  revoked: { tone: "danger", text: "revoked in your ~/.ssh/known_hosts" },
  absent: { tone: "neutral", text: "not in your known_hosts" },
};

// How one presented host key compares with the operator's own known_hosts;
// null when the controller could not read it.
export function knownHostsNote(status) {
  return KNOWN_HOSTS_NOTES[status] || null;
}

export function hostKeyConflict(keys) {
  return (keys || []).some((key) => ["mismatch", "revoked"].includes(key.user_known_hosts));
}

// One key per line: type, SHA256 fingerprint, and the known_hosts comparison.
export function fingerprintListMarkup(keys) {
  const items = (keys || []).map((key) => {
    const note = knownHostsNote(key.user_known_hosts);
    return `<li class="fingerprint-item"><span class="fingerprint-type">${escapeHTML(key.key_type)}</span><code class="fingerprint-value">${escapeHTML(key.fingerprint_sha256)}</code>${note ? `<span class="known-hosts-note" data-tone="${note.tone}">${escapeHTML(note.text)}</span>` : ""}</li>`;
  }).join("");
  return items ? `<ul class="fingerprint-list">${items}</ul>` : "";
}

export function hostKeyConflictMarkup(keys) {
  if (!hostKeyConflict(keys)) return "";
  return `<p class="setup-error" role="alert"><strong>This host key does not match your ~/.ssh/known_hosts.</strong> The node may have been reinstalled, or another machine may be answering at this address. Compare the fingerprint on the node's own console before trusting it.</p>`;
}

export function mountSetup(element, context) {
  root = element;
  ctx = context;
  root.addEventListener("click", onClick);
  root.addEventListener("change", onChange);
  root.addEventListener("input", onInput);
}

export async function refreshSetup() {
  if (!root) return;
  try {
    wizard.overview = await ctx.apiFetch("/setup");
    wizard.keys = wizard.overview.ssh_keys || [];
    for (const entry of wizard.selected.values()) {
      if (!entry.user && !entry.identity) entry.user = wizard.overview.default_ssh_user || "";
    }
  } catch (error) {
    ctx.toast(error.message, "danger");
  }
  render();
}

function nodeEntry(host) {
  if (!wizard.selected.has(host)) {
    wizard.selected.set(host, {
      host,
      port: 22,
      user: wizard.overview?.default_ssh_user || "",
      keyId: wizard.keys[0]?.id || "",
      hostKeys: null,
      identity: null,
      error: null,
      links: null,
      nodeId: "",
      displayName: "",
      modelRoot: "",
    });
  }
  return wizard.selected.get(host);
}

async function withBusy(key, action) {
  if (wizard.busy.has(key)) return;
  wizard.busy.add(key);
  render();
  try {
    await action();
  } catch (error) {
    ctx.toast(error.remediation ? `${error.message} ${error.remediation}` : error.message, "danger");
  } finally {
    wizard.busy.delete(key);
    render();
  }
}

async function startScan() {
  const raw = root.querySelector("#setup-targets")?.value || "";
  const targets = raw.split(/[\s,]+/).map((item) => item.trim()).filter(Boolean);
  await withBusy("scan", async () => {
    const payload = await ctx.apiFetch("/setup/scans", {
      method: "POST",
      body: targets.length ? { targets } : {},
    });
    applyScan(payload.scan);
    pollScan();
  });
}

function applyScan(scan) {
  wizard.scan = scan;
  wizard.candidates = scan.candidates || [];
  if (scan.state === "running") return;
  for (const candidate of wizard.candidates) {
    if (candidate.likely_spark) nodeEntry(candidate.address);
  }
}

function pollScan() {
  window.clearTimeout(scanTimer);
  if (!wizard.scan || wizard.scan.state !== "running") return;
  scanTimer = window.setTimeout(async () => {
    try {
      const payload = await ctx.apiFetch(`/setup/scans/${encodeURIComponent(wizard.scan.id)}`);
      applyScan(payload.scan);
    } catch (error) {
      ctx.toast(error.message, "danger");
      wizard.scan = { ...wizard.scan, state: "failed" };
    }
    render();
    pollScan();
  }, 700);
}

async function fetchHostKeys(entry) {
  await withBusy(`hostkey:${entry.host}`, async () => {
    entry.hostKeys = await ctx.apiFetch("/setup/host-keys", {
      method: "POST",
      body: { host: entry.host, port: entry.port },
    });
  });
}

async function trustHostKeys(entry) {
  const keys = entry.hostKeys?.keys || [];
  const fingerprints = keys.map((key) => key.fingerprint_sha256);
  const confirmed = await ctx.requestConfirmation({
    title: `Trust ${entry.host}?`,
    message: "Only continue if these fingerprints match the node. Compare them on its console with: ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub",
    details: hostKeyConflictMarkup(keys) + fingerprintListMarkup(keys),
    confirmLabel: "Trust host key",
  });
  if (!confirmed) return;
  await withBusy(`hostkey:${entry.host}`, async () => {
    await ctx.apiFetch("/setup/host-keys/trust", {
      method: "POST",
      body: { host: entry.host, port: entry.port, fingerprints },
    });
    entry.hostKeys = await ctx.apiFetch("/setup/host-keys", {
      method: "POST",
      body: { host: entry.host, port: entry.port },
    });
  });
}

async function identify(entry) {
  if (!entry.user || !entry.keyId) {
    ctx.toast("Enter the SSH user and choose a key first.", "danger");
    return;
  }
  await withBusy(`identify:${entry.host}`, async () => {
    const payload = await ctx.apiFetch("/setup/identify", {
      method: "POST",
      body: { host: entry.host, port: entry.port, user: entry.user, key_id: entry.keyId },
    });
    const identity = payload.identity;
    if (identity.ok) {
      entry.identity = identity;
      entry.error = null;
      entry.nodeId ||= identity.suggested?.id || "";
      entry.displayName ||= identity.suggested?.display_name || "";
      entry.modelRoot ||= identity.suggested?.model_roots?.[0] || "";
    } else {
      entry.identity = null;
      entry.error = identity;
    }
  });
}

async function checkLinks(entry) {
  await withBusy(`links:${entry.host}`, async () => {
    const payload = await ctx.apiFetch("/setup/links", {
      method: "POST",
      body: { host: entry.host, port: entry.port },
    });
    entry.links = payload.links;
  });
}

async function generateKey() {
  await withBusy("keygen", async () => {
    const payload = await ctx.apiFetch("/setup/ssh-keys", { method: "POST", body: {} });
    wizard.generatedPublicKey = payload.public_key;
    const keys = await ctx.apiFetch("/setup/ssh-keys");
    wizard.keys = keys.items || [];
    for (const entry of wizard.selected.values()) entry.keyId ||= payload.key.id;
  });
}

async function saveNodes() {
  const ready = [...wizard.selected.values()].filter((entry) => entry.identity);
  if (!ready.length) {
    ctx.toast("Identify at least one node before saving.", "danger");
    return;
  }
  const confirmed = await ctx.requestConfirmation({
    title: "Save cluster configuration?",
    message: `Write ${ready.length} node(s) to the local nodes.json${wizard.replace ? ", replacing the existing configuration" : ""}. Nothing changes on the nodes.`,
    confirmLabel: "Save configuration",
  });
  if (!confirmed) return;
  await withBusy("save", async () => {
    await ctx.apiFetch("/setup/nodes", {
      method: "POST",
      body: {
        replace: wizard.replace,
        nodes: ready.map((entry) => ({
          host: entry.host,
          port: entry.port,
          key_id: entry.keyId,
          node_id: entry.nodeId,
          display_name: entry.displayName,
          model_root: entry.modelRoot,
        })),
      },
    });
    ctx.toast("Cluster configuration saved.", "success");
    await ctx.refresh();
    ctx.showPage("dashboard");
  });
}

function onClick(event) {
  const button = event.target.closest("[data-setup-action]");
  if (!button) return;
  const action = button.dataset.setupAction;
  const entry = button.dataset.host ? nodeEntry(button.dataset.host) : null;
  if (action === "scan") startScan();
  else if (action === "cancel-scan" && wizard.scan) {
    ctx.apiFetch(`/setup/scans/${encodeURIComponent(wizard.scan.id)}/cancel`, { method: "POST", body: {} });
  } else if (action === "add-manual") {
    const host = root.querySelector("#setup-manual-host")?.value.trim();
    if (host) { nodeEntry(host); render(); }
  } else if (action === "remove") { wizard.selected.delete(button.dataset.host); render(); }
  else if (action === "host-keys") fetchHostKeys(entry);
  else if (action === "trust") trustHostKeys(entry);
  else if (action === "identify") identify(entry);
  else if (action === "links") checkLinks(entry);
  else if (action === "keygen") generateKey();
  else if (action === "copy") ctx.copy(button.dataset.copy || "");
  else if (action === "save") saveNodes();
}

function onChange(event) {
  const field = event.target;
  if (field.matches("[data-candidate]")) {
    if (field.checked) nodeEntry(field.dataset.candidate);
    else wizard.selected.delete(field.dataset.candidate);
    render();
  } else if (field.id === "setup-replace") {
    wizard.replace = field.checked;
  } else if (field.matches("[data-node-field]")) {
    updateField(field);
  }
}

function onInput(event) {
  if (event.target.matches("[data-node-field]")) updateField(event.target);
}

function updateField(field) {
  const entry = nodeEntry(field.dataset.host);
  const name = field.dataset.nodeField;
  entry[name] = name === "port" ? Number(field.value) || 22 : field.value.trim();
  if (["user", "keyId", "port"].includes(name)) {
    entry.identity = null;
    entry.error = null;
  }
}

function confidenceTone(confidence) {
  if (confidence === "high") return "success";
  if (confidence === "medium") return "working";
  return "neutral";
}

function renderCandidates() {
  const scan = wizard.scan;
  if (!scan) return `<p class="field-help">The scan covers private subnets only and reads SSH banners; it never logs in.</p>`;
  const progress = scan.progress?.total ? Math.round((scan.progress.done / scan.progress.total) * 100) : 0;
  const header = scan.state === "running"
    ? `<progress class="setup-progress" max="100" value="${progress}"></progress><p class="field-help">Scanning ${escapeHTML(scan.targets.join(", "))} · ${progress}%</p>`
    : `<p class="field-help">Scan ${escapeHTML(scan.state)} · ${wizard.candidates.length} host(s) with SSH found.</p>`;
  if (scan.error) return `${header}<p class="setup-error">${escapeHTML(scan.error.message)}</p>`;
  const rows = wizard.candidates.map((candidate) => `
    <label class="target-option">
      <input type="checkbox" data-candidate="${escapeHTML(candidate.address)}" ${wizard.selected.has(candidate.address) ? "checked" : ""} />
      <span class="target-copy">
        <span class="target-name">${escapeHTML(candidate.hostname || candidate.address)} <span class="setup-muted">${escapeHTML(candidate.address)}</span></span>
        <span class="target-detail">${escapeHTML((candidate.reasons || []).join(" · ") || "SSH reachable")}${candidate.served_models?.length ? ` · serving ${escapeHTML(candidate.served_models.join(", "))}` : ""}</span>
      </span>
      <span class="status-badge" data-tone="${confidenceTone(candidate.confidence)}">${escapeHTML(candidate.likely_spark ? `likely Spark · ${candidate.confidence}` : "other host")}</span>
    </label>`).join("");
  return `${header}<div class="target-list">${rows}</div>`;
}

function keyOptions(selected) {
  if (!wizard.keys.length) return `<option value="">No SSH key found</option>`;
  return wizard.keys.map((key) => `<option value="${escapeHTML(key.id)}" ${key.id === selected ? "selected" : ""}>${escapeHTML(key.display_path)} (${escapeHTML(key.source)})</option>`).join("");
}

function renderIdentity(entry) {
  if (entry.error) {
    const command = entry.error.copy_id_command;
    return `<div class="setup-error"><strong>${escapeHTML(entry.error.message)}</strong>${command ? `
      <p>Authorise the key by running this in your own terminal (it asks for the node password there, never here):</p>
      <div class="setup-command"><code>${escapeHTML(command)}</code><button class="text-button" type="button" data-setup-action="copy" data-copy="${escapeHTML(command)}">Copy</button></div>` : ""}</div>`;
  }
  const identity = entry.identity;
  if (!identity) return "";
  const connectx = (identity.interfaces || []).filter((item) => item.is_connectx && item.addresses.length);
  const facts = [
    ["Host name", identity.hostname],
    ["System", identity.product_name || identity.sys_vendor || "unknown"],
    ["GPU", (identity.gpus || []).join(", ") || "none"],
    ["Memory", identity.memory_total_bytes ? formatBytes(identity.memory_total_bytes) : "unknown"],
    ["Docker", identity.docker_version || "not usable"],
    ["Free disk (home)", identity.disk_free_bytes ? formatBytes(identity.disk_free_bytes) : "unknown"],
    ["Cluster links", connectx.map((item) => `${item.name} ${item.addresses.join(" ")}`).join(", ") || "none detected"],
    ["HF token on node", identity.hf_token_present ? "present" : "not found"],
  ];
  const links = entry.links ? Object.entries(entry.links).map(([peer, ok]) => `${peer}: ${ok ? "reachable" : "unreachable"}`).join(" · ") : "";
  return `
    <div class="setup-identity" data-spark="${identity.is_dgx_spark}">
      <p><span class="status-badge" data-tone="${identity.is_dgx_spark ? "success" : "working"}">${identity.is_dgx_spark ? "DGX Spark identified" : "Not a DGX Spark"}</span> ${escapeHTML((identity.spark_evidence || []).join(" · "))}</p>
      <dl class="facts-list">${facts.map(([label, value]) => `<div class="fact-pair"><dt>${escapeHTML(label)}</dt><dd>${escapeHTML(value)}</dd></div>`).join("")}</dl>
      ${(identity.warnings || []).map((warning) => `<p class="setup-warning">${escapeHTML(warning)}</p>`).join("")}
      ${links ? `<p class="field-help">Direct links from this node: ${escapeHTML(links)}</p>` : ""}
      <div class="setup-grid">
        <label class="field-group"><span class="field-label">Node id</span><input class="select-input" data-node-field="nodeId" data-host="${escapeHTML(entry.host)}" value="${escapeHTML(entry.nodeId)}" /></label>
        <label class="field-group"><span class="field-label">Display name</span><input class="select-input" data-node-field="displayName" data-host="${escapeHTML(entry.host)}" value="${escapeHTML(entry.displayName)}" /></label>
        <label class="field-group"><span class="field-label">Model root</span><input class="select-input" data-node-field="modelRoot" data-host="${escapeHTML(entry.host)}" value="${escapeHTML(entry.modelRoot)}" /></label>
      </div>
    </div>`;
}

function renderNode(entry) {
  const busy = (kind) => wizard.busy.has(`${kind}:${entry.host}`);
  const keys = entry.hostKeys?.keys || [];
  const trusted = keys.length && keys.every((key) => key.trusted);
  const identifiedCount = [...wizard.selected.values()].filter((item) => item.identity).length;
  return `
    <article class="setup-node">
      <div class="card-heading-row">
        <div><p class="eyebrow">Node</p><h3>${escapeHTML(entry.host)}</h3></div>
        <button class="text-button" type="button" data-setup-action="remove" data-host="${escapeHTML(entry.host)}">Remove</button>
      </div>
      <div class="setup-grid">
        <label class="field-group"><span class="field-label">SSH user</span><input class="select-input" data-node-field="user" data-host="${escapeHTML(entry.host)}" value="${escapeHTML(entry.user)}" placeholder="the Linux user on the Spark" autocomplete="off" /></label>
        <label class="field-group"><span class="field-label">SSH port</span><input class="select-input" type="number" min="1" max="65535" data-node-field="port" data-host="${escapeHTML(entry.host)}" value="${entry.port}" /></label>
        <label class="field-group"><span class="field-label">SSH key</span><select class="select-input" data-node-field="keyId" data-host="${escapeHTML(entry.host)}">${keyOptions(entry.keyId)}</select></label>
      </div>
      <div class="setup-step">
        <span class="setup-step-label">1. Host key</span>
        ${trusted ? `<span class="status-badge" data-tone="success">trusted</span>` : keys.length
          ? `<button class="button button-secondary" type="button" data-setup-action="trust" data-host="${escapeHTML(entry.host)}" ${busy("hostkey") ? "disabled" : ""}>Trust fingerprint…</button>`
          : `<button class="button button-secondary" type="button" data-setup-action="host-keys" data-host="${escapeHTML(entry.host)}" ${busy("hostkey") ? "disabled" : ""}>Fetch host key</button>`}
      </div>
      ${fingerprintListMarkup(keys)}
      ${hostKeyConflictMarkup(keys)}
      <div class="setup-step">
        <span class="setup-step-label">2. Identify (read-only)</span>
        <button class="button button-primary" type="button" data-setup-action="identify" data-host="${escapeHTML(entry.host)}" ${busy("identify") ? "disabled" : ""}>${busy("identify") ? "Identifying…" : entry.identity ? "Identify again" : "Identify"}</button>
        ${entry.identity && identifiedCount > 1 ? `<button class="button button-secondary" type="button" data-setup-action="links" data-host="${escapeHTML(entry.host)}" ${busy("links") ? "disabled" : ""}>Check direct links</button>` : ""}
      </div>
      ${renderIdentity(entry)}
    </article>`;
}

function render() {
  if (!root) return;
  const overview = wizard.overview || {};
  const targets = (overview.default_targets || []).join(", ");
  const scanning = wizard.scan?.state === "running";
  const selected = [...wizard.selected.values()];
  const identified = selected.filter((entry) => entry.identity).length;
  root.innerHTML = `
    <div class="page-intro">
      <p class="eyebrow">${overview.setup_required ? "First run" : "Cluster membership"}</p>
      <h1>Connect your DGX Spark cluster</h1>
      <p class="lede">Find Sparks on your network, confirm each node's SSH host key, identify it with a read-only probe, and save the cluster. Nothing on a node changes during setup.</p>
    </div>
    <section class="diagnostics-card setup-section">
      <div class="card-heading-row"><div><p class="eyebrow">Step 1</p><h2>Find nodes</h2></div>
        ${scanning ? `<button class="button button-secondary" type="button" data-setup-action="cancel-scan">Cancel scan</button>` : ""}</div>
      <div class="setup-grid setup-grid-wide">
        <label class="field-group"><span class="field-label">Private subnets to scan</span>
          <input class="select-input" id="setup-targets" value="${escapeHTML(targets)}" placeholder="192.168.1.0/24" autocomplete="off" /></label>
        <div class="field-group setup-actions"><button class="button button-primary" type="button" data-setup-action="scan" ${scanning || wizard.busy.has("scan") ? "disabled" : ""}>${scanning ? "Scanning…" : "Scan network"}</button></div>
      </div>
      ${renderCandidates()}
      <div class="setup-grid setup-grid-wide">
        <label class="field-group"><span class="field-label">Or add a host by address or name</span>
          <input class="select-input" id="setup-manual-host" placeholder="spark-1a2b.local" autocomplete="off" /></label>
        <div class="field-group setup-actions"><button class="button button-secondary" type="button" data-setup-action="add-manual">Add host</button></div>
      </div>
    </section>
    <section class="diagnostics-card setup-section">
      <div class="card-heading-row"><div><p class="eyebrow">Step 2</p><h2>Connect and identify</h2></div>
        <button class="button button-secondary" type="button" data-setup-action="keygen" ${wizard.busy.has("keygen") ? "disabled" : ""}>Create a controller SSH key</button></div>
      <p class="card-copy">Use the key NVIDIA Sync created, your own SSH key, or a dedicated controller key. The controller authenticates with keys only.</p>
      ${wizard.generatedPublicKey ? `<div class="setup-command"><code>${escapeHTML(wizard.generatedPublicKey)}</code><button class="text-button" type="button" data-setup-action="copy" data-copy="${escapeHTML(wizard.generatedPublicKey)}">Copy public key</button></div>` : ""}
      ${selected.length ? selected.map(renderNode).join("") : `<div class="empty-state"><h2>No nodes selected</h2><p>Select scan results or add a host above.</p></div>`}
    </section>
    <section class="diagnostics-card setup-section">
      <div class="card-heading-row"><div><p class="eyebrow">Step 3</p><h2>Save the cluster</h2></div></div>
      <p class="card-copy">${identified} of ${selected.length} selected node(s) identified. Saving writes the local configuration and reloads the controller; remote models keep running.</p>
      ${overview.nodes_file_exists ? `<label class="confirm-row"><input type="checkbox" id="setup-replace" ${wizard.replace ? "checked" : ""} /> Replace the existing configuration instead of merging these nodes into it.</label>` : ""}
      <div class="intro-actions"><button class="button button-primary" type="button" data-setup-action="save" ${identified && !wizard.busy.has("save") ? "" : "disabled"}>Save configuration</button></div>
    </section>`;
}
