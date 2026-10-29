// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

// Cluster updates: check every node for OS packages and firmware, download
// them while the model keeps serving, then stop it, install, reboot and load
// it again. Runs on a schedule or on demand; the controller relays every
// download to the nodes over SSH.

import {
  busyRunner,
  countLabel,
  emptyState,
  errorText,
  escapeHTML,
  factPairs,
  formatDate,
  formatRelativeTime,
  normalizeOperation,
  stateBadge,
  statusBadge,
  statusLabel,
} from "./ui-core.js";

const WEEKDAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
const HELPER_BADGES = {
  ready: ["helper ready", "success"],
  missing: ["helper missing", "warning"],
  outdated: ["helper outdated", "warning"],
  "no-sudo": ["sudo rule missing", "warning"],
  unreachable: ["unreachable", "danger"],
};

const page = { data: null, form: null, dirty: false, busy: new Set() };

let ctx = null;
let root = null;
let pollTimer = null;

const withBusy = busyRunner(page.busy, render, (error) => ctx.toast(errorText(error), "danger"));

export function mountUpdates(element, context) {
  root = element;
  ctx = context;
  root.addEventListener("click", onClick);
  root.addEventListener("change", onField);
  root.addEventListener("input", onField);
}

export async function refreshUpdates() {
  if (!root) return;
  try {
    apply(await ctx.apiFetch("/updates"));
  } catch (error) {
    if (error.code !== "setup_required") ctx.toast(error.message, "danger");
  }
  render();
  schedulePoll();
}

function apply(payload) {
  page.data = payload;
  if (!page.dirty) page.form = { ...payload.settings, directory: payload.settings.directory || "" };
}

function schedulePoll() {
  window.clearTimeout(pollTimer);
  const data = page.data;
  if (!data || (!data.checking && !data.active_operation_id) || ctx.state().page !== "updates") return;
  pollTimer = window.setTimeout(async () => {
    if (document.hidden) schedulePoll();
    else await refreshUpdates();
  }, 3000);
}

export function helperTone(state) {
  return HELPER_BADGES[state]?.[1] || "neutral";
}

export function scheduleSummary(settings, nextRunAt, timezone) {
  if (!settings.enabled) return "Scheduled updates are off.";
  const days = WEEKDAYS.filter((day) => settings.weekdays.includes(day)).map((day) => statusLabel(day)).join(", ");
  return `${days} at ${settings.time}. Next run: ${formatDate(nextRunAt, "none")} (${timezone}).`;
}

export function updateConfirmation(updates, appState) {
  const serving = appState.deployments
    .filter((deployment) => deployment.state === "ready")
    .map((deployment) => appState.profiles.find((profile) => profile.id === deployment.profileId)?.name || deployment.profileId)
    .join(" and ");
  const nodes = countLabel(updates.nodes.length, "node");
  return {
    title: "Update the cluster now?",
    message: serving
      ? `Serving continues while updates download. Then the controller stops ${serving}, installs the updates on all ${nodes}, reboots each node that installed something, and loads ${serving} again, verified by its readiness checks and completion canary. Inference is unavailable from the stop until the reload passes; reloading a large model can take 15 minutes or more.`
      : `No model is serving. The controller downloads and installs the updates on all ${nodes} and reboots each node that installed something.`,
    confirmLabel: serving ? "Stop model and update" : "Update now",
  };
}

export function nodeRowMarkup(node) {
  const count = (value) => (node.checked && value !== null ? String(value) : "not checked");
  const run = node.run;
  const lastRun = !run || run.state === "idle" ? "none"
    : `${run.mode || "update"} ${run.state}${run.state === "running" ? ` · ${run.step}` : run.failed_step ? ` at ${run.failed_step}` : ""}`;
  return `
    <article class="setup-node">
      <div class="card-heading-row">
        <div><p class="eyebrow">${escapeHTML(node.node_id)}</p><h3>${escapeHTML(node.display_name)}</h3></div>
        ${statusBadge(HELPER_BADGES[node.helper]?.[0] || node.helper, helperTone(node.helper))}
      </div>
      <dl class="facts-list">${factPairs([
        ["OS", node.os_version || "unknown"],
        ["DGX OS", node.dgx_version || "unknown"],
        ["Kernel", node.kernel || "unknown"],
        ["Driver", node.driver || "unknown"],
        ["Packages", count(node.packages)],
        ["Firmware", count(node.firmware)],
        ["Reboot required", typeof node.reboot_required === "boolean" ? (node.reboot_required ? "yes" : "no") : "not checked"],
        ["Last run", lastRun],
      ])}</dl>
      ${node.message ? `<p class="field-help">${escapeHTML(node.message)}</p>` : ""}
    </article>`;
}

async function runNow() {
  if (!await ctx.requestConfirmation(updateConfirmation(page.data, ctx.state()))) return;
  await withBusy("run", async () => {
    await ctx.runMutation("Cluster update", () => ctx.apiFetch("/updates/run", { method: "POST", body: { confirmation: true } }));
    await refreshUpdates();
  });
}

async function installHelper() {
  const field = root.querySelector("#update-sudo-password");
  await withBusy("helper", async () => {
    let payload;
    try {
      payload = await ctx.apiFetch("/updates/helper", { method: "POST", body: { password: field.value } });
    } finally {
      field.value = "";
    }
    apply(payload.updates);
    const failed = payload.results.filter((result) => result.state !== "installed");
    ctx.toast(failed.length
      ? `Helper not installed on ${failed.map((result) => `${result.node_id}: ${result.message || result.code}`).join("; ")}`
      : "Update helper installed.", failed.length ? "danger" : "success");
  });
}

async function saveSettings() {
  const { enabled, weekdays, time, source, directory } = page.form;
  await withBusy("save", async () => {
    const payload = await ctx.apiFetch("/updates/settings", {
      method: "POST",
      body: { enabled, weekdays, time, source, directory: source === "directory" ? directory.trim() : null },
    });
    page.dirty = false;
    apply(payload);
    ctx.toast("Update settings saved.", "success");
  });
}

function onClick(event) {
  const button = event.target.closest("[data-updates-action]");
  if (!button) return;
  const action = button.dataset.updatesAction;
  if (action === "check") {
    withBusy("check", async () => {
      apply(await ctx.apiFetch("/updates/check", { method: "POST", body: {} }));
      schedulePoll();
    });
  } else if (action === "run") runNow();
  else if (action === "install-helper") installHelper();
  else if (action === "save") saveSettings();
  else if (action === "open-operation") ctx.selectOperation(button.dataset.operation);
}

// Bound to both input and change, so every branch must be idempotent.
function onField(event) {
  const field = event.target;
  const { updatesDay: day, updatesField: key } = field.dataset;
  if (day) page.form.weekdays = field.checked ? [...new Set([...page.form.weekdays, day])] : page.form.weekdays.filter((item) => item !== day);
  else if (key) page.form[key] = field.type === "checkbox" ? field.checked : field.value;
  else return;
  page.dirty = true;
  if (event.type === "change" && field.type === "radio") render();
}

// A count the node never reported is shown as unknown, not as zero.
function resultCount(value, noun) {
  return value === null || value === undefined ? `? ${noun}s` : countLabel(value, noun);
}

function operationMarkup(data) {
  const activeId = data.active_operation_id;
  const raw = activeId && data.last_operation?.id !== activeId
    ? ctx.state().operations.find((operation) => operation.id === activeId) || { id: activeId, state: "queued", phase: "queued", progress_percent: 0 }
    : data.last_operation;
  if (!raw) return "";
  const operation = normalizeOperation(raw);
  const result = operation.result || {};
  const progress = escapeHTML(operation.progress);
  const names = new Map(data.nodes.map((node) => [node.node_id, node.display_name]));
  const rows = activeId
    ? Object.entries(operation.progress_by_node || {}).map(([nodeId, item]) => [nodeId, item.step || item.state])
    : (result.nodes || []).map((item) => [item.node_id, `${resultCount(item.packages, "package")} · ${resultCount(item.firmware, "firmware update")}${item.rebooted ? " · rebooted" : ""} · kernel ${item.kernel_before} → ${item.kernel_after} · driver ${item.driver_before} → ${item.driver_after}`]);
  const error = operation.error;
  return `
    <section class="diagnostics-card setup-section">
      <div class="card-heading-row">
        <div><p class="eyebrow">${activeId ? "Update running" : `Last update · ${escapeHTML(formatRelativeTime(operation.updated_at))}`}</p>
          <h2>${escapeHTML(activeId ? statusLabel(operation.phase) : result.up_to_date ? "Cluster was already up to date" : `Update ${statusLabel(operation.state).toLowerCase()}`)}</h2></div>
        ${activeId ? stateBadge(operation.state) : ""}
      </div>
      ${activeId ? `<div class="progress-summary"><p class="progress-label">${escapeHTML(operation.message)}</p><strong class="progress-percent">${progress}%</strong></div>
      <progress class="progress-track" data-tone="working" max="100" value="${progress}" aria-label="Update ${progress}% complete">${progress}%</progress>` : ""}
      ${error ? `<div class="detail-error"><strong>${escapeHTML(error.message)}</strong>${escapeHTML(error.remediation || "")}</div>` : ""}
      ${rows.length ? `<div class="recipe-fit">${rows.map(([nodeId, text]) => `<div class="recipe-fit-row"><strong>${escapeHTML(names.get(nodeId) || nodeId)}</strong><span>${escapeHTML(text)}</span></div>`).join("")}</div>` : ""}
      ${result.restored_deployments?.length ? `<p class="field-help">Restored ${escapeHTML(result.restored_deployments.join(", "))}.</p>` : ""}
      <button class="button button-secondary" type="button" data-updates-action="open-operation" data-operation="${escapeHTML(operation.id)}">Open in Operations</button>
    </section>`;
}

function settingsMarkup(data) {
  const form = page.form;
  return `
    <div class="diagnostics-grid">
      <section class="diagnostics-card">
        <p class="eyebrow">Schedule</p>
        <h2>Automatic updates</h2>
        ${data.settings_error ? `<p class="setup-error">${escapeHTML(data.settings_error.message)}</p>` : ""}
        ${data.schedule_error ? `<p class="setup-error">${escapeHTML(data.schedule_error)}</p>` : ""}
        <div class="recipe-toggles"><label><input type="checkbox" data-updates-field="enabled" ${form.enabled ? "checked" : ""} /> Update on a schedule</label></div>
        <div class="setup-grid">
          <div class="field-group"><span class="field-label">Days</span>
            <div class="recipe-toggles">${WEEKDAYS.map((day) => `<label><input type="checkbox" data-updates-day="${day}" ${form.weekdays.includes(day) ? "checked" : ""} /> ${statusLabel(day)}</label>`).join("")}</div></div>
          <label class="field-group"><span class="field-label">Local time</span>
            <input class="select-input" type="time" data-updates-field="time" value="${escapeHTML(form.time)}" /></label>
        </div>
        <p class="card-copy">${escapeHTML(scheduleSummary(data.settings, data.next_run_at, data.timezone))} The controller app must be running then; a run missed by more than an hour is skipped.</p>
      </section>
      <section class="diagnostics-card">
        <p class="eyebrow">Download source</p>
        <h2>Where updates come from</h2>
        <div class="target-list">
          <label class="target-option"><input type="radio" name="update-source" value="online" data-updates-field="source" ${form.source === "online" ? "checked" : ""} />
            <span class="target-copy"><span class="target-name">Internet, through this controller</span></span></label>
          <label class="target-option"><input type="radio" name="update-source" value="directory" data-updates-field="source" ${form.source === "directory" ? "checked" : ""} />
            <span class="target-copy"><span class="target-name">A directory on this computer</span></span></label>
        </div>
        <div class="setup-grid">
          <label class="field-group"><span class="field-label">Directory</span>
            <input class="select-input" data-updates-field="directory" value="${escapeHTML(form.directory)}" placeholder="/Volumes/mirror/update-cache" autocomplete="off" ${form.source === "directory" ? "" : "disabled"} /></label>
        </div>
        <p class="card-copy">Nodes never need Internet access. In Internet mode the controller keeps every download in its data directory under <code>update-cache/&lt;host&gt;/&lt;path&gt;</code>, the layout a directory source needs, so the cache can be copied to an offline controller. Sources that need credentials, such as Ubuntu Pro, work only in Internet mode.</p>
      </section>
    </div>
    <div class="support-actions"><button class="button button-primary" type="button" data-updates-action="save" ${page.busy.has("save") ? "disabled" : ""}>Save settings</button></div>`;
}

function render() {
  if (!root) return;
  const data = page.data;
  const nodes = data?.nodes || [];
  const blocked = !data || data.checking || data.active_operation_id || !nodes.every((node) => node.helper === "ready") || page.busy.has("check") || page.busy.has("run");
  const installable = nodes.filter((node) => ["missing", "outdated", "no-sudo"].includes(node.helper));
  // The sudo password lives only in its input, so a poll re-render keeps that element.
  const password = root.querySelector("#update-sudo-password");
  const focused = password && document.activeElement === password;
  root.innerHTML = `
    <div class="page-intro split-intro">
      <div>
        <p class="eyebrow">Maintenance</p>
        <h1>Cluster updates</h1>
        <p class="lede">Check every DGX Spark for OS packages and firmware, download them while the model keeps serving, then install, reboot, and load that model again.</p>
      </div>
      <div class="intro-actions">
        <button class="button button-secondary" type="button" data-updates-action="check" ${blocked ? "disabled" : ""}>${data?.checking ? "Checking…" : "Check for updates"}</button>
        <button class="button button-danger" type="button" data-updates-action="run" ${blocked ? "disabled" : ""}>Update now</button>
      </div>
    </div>
    ${!data ? "" : !nodes.length ? emptyState("No nodes yet", "Finish Setup before updating the cluster.") : `
    ${operationMarkup(data)}
    <section class="diagnostics-card setup-section">
      <div class="card-heading-row"><div><p class="eyebrow">Nodes</p><h2>Versions and pending updates</h2></div>
        <span class="section-meta">${data.checking ? "Checking…" : data.checked_at ? `Checked ${escapeHTML(formatRelativeTime(data.checked_at))}` : "Not checked yet"}</span></div>
      ${data.check_error ? `<p class="setup-error">${escapeHTML(data.check_error)}</p>` : ""}
      ${nodes.map(nodeRowMarkup).join("")}
      ${installable.length ? `<div class="setup-node">
        <h3>Install the update helper on ${escapeHTML(installable.map((node) => node.display_name).join(", "))}</h3>
        <p class="card-copy">Needed once per node. The controller sends the node user's sudo password to sudo on each node over SSH and never stores it; afterwards it can run only the helper's fixed commands without a password.</p>
        <div class="setup-grid setup-grid-wide">
          <label class="field-group"><span class="field-label">Sudo password</span>
            <input class="select-input" id="update-sudo-password" type="password" autocomplete="off" /></label>
          <div class="field-group setup-actions"><button class="button button-primary" type="button" data-updates-action="install-helper" ${page.busy.has("helper") || data.active_operation_id ? "disabled" : ""}>Install helper</button></div>
        </div>
      </div>` : ""}
    </section>
    ${settingsMarkup(data)}`}`;
  const slot = root.querySelector("#update-sudo-password");
  if (password && slot) {
    slot.replaceWith(password);
    if (focused) password.focus();
  }
}
