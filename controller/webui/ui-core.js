/* Shared, dependency-free UI helpers. The normalizers read exactly the fields
 * controller/resources.py emits; the UI and the API ship together. */

export const API_ROOT = "/api/v1";

export class ApiError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "ApiError";
    this.code = details.code || "request_failed";
    this.remediation = details.remediation || "Try again or open Diagnostics for more context.";
  }
}

export function escapeHTML(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

export function firstDefined(...values) {
  return values.find((value) => value !== undefined && value !== null && value !== "");
}

export function numberValue(...values) {
  const value = firstDefined(...values);
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "" && Number.isFinite(Number(value))) return Number(value);
  return null;
}

export function formatBytes(value) {
  const bytes = numberValue(value);
  if (bytes === null || bytes < 0) return "—";
  if (bytes < 1024) return `${Math.round(bytes)} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let amount = bytes;
  let unit = -1;
  while (amount >= 1024 && unit < units.length - 1) {
    amount /= 1024;
    unit += 1;
  }
  return `${amount >= 10 || unit === 0 ? amount.toFixed(0) : amount.toFixed(1)} ${units[unit]}`;
}

export function formatPercent(value) {
  const percent = numberValue(value);
  if (percent === null) return "—";
  return `${Math.round(Math.max(0, Math.min(100, percent)))}%`;
}

export function formatDate(value, fallback = "Not recorded") {
  if (!value) return fallback;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(date);
}

export function formatRelativeTime(value) {
  if (!value) return "not recorded";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  const seconds = Math.round((Date.now() - date.getTime()) / 1000);
  if (seconds < 10) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return formatDate(value);
}

export function formatDuration(start, end = null) {
  if (!start) return "—";
  const startDate = new Date(start);
  const endDate = end ? new Date(end) : new Date();
  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) return "—";
  let seconds = Math.max(0, Math.round((endDate - startDate) / 1000));
  const hours = Math.floor(seconds / 3600);
  seconds -= hours * 3600;
  const minutes = Math.floor(seconds / 60);
  seconds -= minutes * 60;
  if (hours) return `${hours}h ${minutes}m`;
  if (minutes) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

function createIdempotencyKey(prefix) {
  if (globalThis.crypto?.randomUUID) return `${prefix}-${globalThis.crypto.randomUUID()}`;
  return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export async function apiFetch(path, options = {}) {
  const method = (options.method || "GET").toUpperCase();
  const headers = new Headers(options.headers || {});
  headers.set("Accept", "application/json");
  let body = options.body;
  if (body && typeof body === "object" && !(body instanceof Blob) && !(body instanceof FormData)) {
    body = JSON.stringify(body);
    headers.set("Content-Type", "application/json");
  }
  if (method !== "GET" && method !== "HEAD") headers.set("Idempotency-Key", createIdempotencyKey(method.toLowerCase()));
  let response;
  try {
    response = await fetch(path === "/api/health" ? path : `${API_ROOT}${path}`, { ...options, method, headers, body });
  } catch {
    throw new ApiError("The local controller could not be reached.", {
      code: "controller_unreachable",
      remediation: "Make sure the controller is running, then refresh this view.",
    });
  }

  const raw = await response.text();
  let payload = null;
  if (raw) {
    try { payload = JSON.parse(raw); } catch { payload = { raw }; }
  }
  if (!response.ok) {
    const error = payload?.error || {};
    throw new ApiError(error.message || `The controller returned HTTP ${response.status}.`, error);
  }
  return payload ?? {};
}

// Node actions act on the observed deployment unless it is stopped or failed.
export function normalizeNode(raw) {
  const deployment = raw.deployment && !["stopped", "failed"].includes(raw.deployment.state) ? normalizeDeployment(raw.deployment) : null;
  return {
    ...raw,
    name: raw.display_name,
    address: raw.management_address,
    lastRefresh: raw.last_refresh_at,
    modelAlias: raw.active_model_alias,
    runtime: raw.runtime_version || "Runtime not recorded",
    deployment,
  };
}

export function normalizeProfile(raw) {
  return {
    ...raw,
    name: raw.display_name,
    alias: raw.api_alias,
    mode: raw.deployment_mode,
    nodeCount: raw.node_count,
    weightsBytes: raw.weights_bytes,
    reservedMemoryBytes: raw.reserved_memory_bytes,
    gpuMemoryUtilization: raw.gpu_memory_utilization,
  };
}

export function normalizeDeployment(raw) {
  return { ...raw, profileId: raw.profile_id, nodeIds: raw.node_ids };
}

export function normalizeOperation(raw) {
  return {
    ...raw,
    progress: raw.progress_percent,
    profileId: raw.profile_id,
    deploymentId: raw.deployment_id,
    nodeIds: raw.node_ids,
    requestedAt: raw.requested_at,
    startedAt: raw.started_at,
  };
}

// 202 responses wrap the operation they queued. A recipe cancel answers
// {id, cancel_requested} instead, which is not an operation to merge.
export function operationFromPayload(payload) {
  return payload.operation?.action ? normalizeOperation(payload.operation) : null;
}

export function isTerminalOperation(operation) {
  return ["succeeded", "failed", "cancelled", "needs-attention"].includes(operation.state);
}

export function statusTone(value) {
  const state = String(value || "unknown").toLowerCase().replaceAll("_", "-");
  if (["ready", "idle", "online", "present", "succeeded", "success", "completed", "stopped"].includes(state)) return "success";
  if (["loading", "running", "queued", "busy", "preflighting", "starting", "stopping", "draining", "partial"].some((item) => state.includes(item))) return "working";
  if (["unmanaged", "warning", "setup-required"].includes(state)) return "warning";
  if (["failed", "offline", "degraded", "unknown", "needs-attention", "missing", "unreadable", "error"].some((item) => state.includes(item))) return "danger";
  return "neutral";
}

// The header pill: a controller without configured nodes is reachable but not
// ready to manage anything yet.
export function controllerStatus(setup) {
  if (setup.setup_required) return { state: "warning", label: "Setup required" };
  return { state: "ready", label: "Controller ready" };
}

export function statusLabel(value) {
  const text = String(value || "unknown").replaceAll("_", "-");
  return text.replace(/(^|-)([a-z])/g, (_, prefix, letter) => `${prefix ? " " : ""}${letter.toUpperCase()}`);
}

// Each OperationAction with the OperationPhase values it passes through, in
// order. Claiming (validating), rolling back and finishing (complete) are not
// steps of their own.
export const ACTIONS = {
  load: {
    label: "Load model",
    glyph: "↑",
    phases: ["locking", "refreshing", "preflighting", "snapshotting", "draining", "stopping-conflicts", "verifying-stopped", "starting-workers", "starting-head", "loading", "verifying-ready", "committing"],
  },
  unload: {
    label: "Unload model",
    glyph: "↓",
    phases: ["locking", "refreshing", "draining", "stopping-conflicts", "verifying-stopped", "committing"],
  },
  recover: {
    label: "Recover baseline",
    glyph: "↺",
    phases: ["locking", "refreshing", "preflighting", "snapshotting", "stopping-conflicts", "verifying-stopped"],
  },
  reconcile: { label: "Refresh observations", glyph: "↻", phases: ["locking", "refreshing", "reconciling"] },
  prepare: { label: "Prepare recipe", glyph: "⇩", phases: ["preparing", "committing"] },
  "delete-recipe": { label: "Delete recipe", glyph: "✕", phases: ["deleting"] },
  update: {
    label: "Cluster update",
    glyph: "⇪",
    phases: ["locking", "refreshing", "preflighting", "snapshotting", "draining", "stopping-conflicts", "verifying-stopped", "updating", "rebooting", "starting-workers", "starting-head", "loading", "verifying-ready", "committing"],
  },
};

export function countLabel(count, singular, plural = `${singular}s`) {
  return `${count} ${count === 1 ? singular : plural}`;
}

export function newestFirst(operations) {
  return [...operations].sort((a, b) => new Date(b.requestedAt || 0) - new Date(a.requestedAt || 0));
}

export function isNodeOnline(node) {
  return !["offline", "unknown"].includes(node.state);
}

/* Markup builders. Each escapes every value it inserts, so callers can pass
 * remote text straight through. */

export function statusBadge(label, tone) {
  return `<span class="status-badge" data-tone="${escapeHTML(tone)}">${escapeHTML(label)}</span>`;
}

export function stateBadge(state) {
  return statusBadge(statusLabel(state), statusTone(state));
}

export function factPairs(facts) {
  return facts.map(([label, value]) => `<div class="fact-pair"><dt>${escapeHTML(label)}</dt><dd>${escapeHTML(value)}</dd></div>`).join("");
}

export function emptyState(title, copy) {
  return `<div class="empty-state">${title ? `<h2>${escapeHTML(title)}</h2>` : ""}<p>${escapeHTML(copy)}</p></div>`;
}

export function warningNotes(warnings) {
  return (warnings || []).map((warning) => `<p class="setup-warning">${escapeHTML(warning)}</p>`).join("");
}

export function actionMark(action) {
  return `<span class="operation-action-mark" data-action="${escapeHTML(action)}" aria-hidden="true">${escapeHTML(ACTIONS[action].glyph)}</span>`;
}

/* Nodes a load on targetIds will stop. A deployment observed on a selected node
 * is replaced on every node it uses, because it cannot keep running on part of
 * its placement, so nodes outside the selection are named too. */
export function switchStopEntries(targetIds, nodes) {
  const selected = new Set(targetIds);
  const nodesById = new Map(nodes.map((node) => [node.id, node]));
  const entries = new Map();
  for (const node of nodes) {
    if (!selected.has(node.id)) continue;
    const { deployment } = node;
    if (!deployment) continue;
    for (const nodeId of [node.id, ...deployment.nodeIds]) {
      if (entries.has(nodeId)) continue;
      entries.set(nodeId, {
        node: nodesById.get(nodeId) || { id: nodeId, name: nodeId },
        deployment,
        outsideSelection: !selected.has(nodeId),
      });
    }
  }
  return [...entries.values()];
}

export function copyText(text) {
  if (navigator.clipboard?.writeText) return navigator.clipboard.writeText(text);
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.append(textarea);
  textarea.select();
  document.execCommand("copy");
  textarea.remove();
  return Promise.resolve();
}

export function trapModalFocus(modal, event) {
  if (event.key !== "Tab") return;
  const focusable = [...modal.querySelectorAll("button, input, select, textarea, [href], [tabindex]:not([tabindex='-1'])")]
    .filter((element) => !element.disabled && element.offsetParent !== null);
  if (!focusable.length) return;
  const first = focusable[0];
  const last = focusable.at(-1);
  if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
  else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
}

export function errorText(error) {
  return error.remediation ? `${error.message} ${error.remediation}` : error.message;
}

/* Returns `run(key, action)`: at most one action per key at a time, with a
 * render before and after so busy buttons disable, and failures reported. */
export function busyRunner(busy, render, onError) {
  return async (key, action) => {
    if (busy.has(key)) return;
    busy.add(key);
    render();
    try {
      await action();
    } catch (error) {
      onError(error);
    } finally {
      busy.delete(key);
      render();
    }
  };
}

export function startPoller(callback, interval = 2000) {
  let timer = null;
  let stopped = false;
  const run = async () => {
    if (stopped) return;
    try { await callback(); } finally { if (!stopped) timer = setTimeout(run, interval); }
  };
  run();
  return () => { stopped = true; if (timer) clearTimeout(timer); };
}
