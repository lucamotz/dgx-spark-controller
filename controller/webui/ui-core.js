/* Shared, dependency-free UI helpers. Keep these functions total: a half-filled
 * bootstrap response should still produce a useful screen. */

export const API_ROOT = "/api/v1";

export class ApiError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "ApiError";
    this.code = details.code || "request_failed";
    this.status = details.status || 0;
    this.retryable = Boolean(details.retryable);
    this.remediation = details.remediation || "Try again or open Diagnostics for more context.";
    this.operationId = details.operation_id || details.operationId || null;
    this.payload = details.payload || null;
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

export function asArray(value) {
  if (Array.isArray(value)) return value;
  if (value && typeof value === "object") {
    if (Array.isArray(value.items)) return value.items;
    if (Array.isArray(value.results)) return value.results;
    if (Array.isArray(value.data)) return value.data;
  }
  return [];
}

export function collection(payload, key) {
  const roots = [payload, payload?.data, payload?.result, payload?.snapshot];
  for (const root of roots) {
    if (!root || typeof root !== "object") continue;
    const value = root[key] ?? root[`${key}_items`];
    if (Array.isArray(value)) return value;
    if (value && typeof value === "object") {
      const items = asArray(value);
      if (items.length || Object.prototype.hasOwnProperty.call(value, "items")) return items;
    }
  }
  if (Array.isArray(payload)) return payload;
  return [];
}

export function objectValue(raw, key) {
  if (!raw || typeof raw !== "object") return {};
  const value = raw[key];
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
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

export function createIdempotencyKey(prefix = "ui") {
  if (globalThis.crypto?.randomUUID) return `${prefix}-${globalThis.crypto.randomUUID()}`;
  return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function apiURL(path) {
  if (/^https?:\/\//i.test(path)) return path;
  if (path === "/api/health" || path === "/api/bootstrap") return path;
  if (path.startsWith("/api/v1/")) return path;
  return `${API_ROOT}${path.startsWith("/") ? path : `/${path}`}`;
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
  if (method !== "GET" && method !== "HEAD" && !headers.has("Idempotency-Key")) {
    headers.set("Idempotency-Key", createIdempotencyKey(method.toLowerCase()));
  }
  let response;
  try {
    response = await fetch(apiURL(path), { ...options, method, headers, body });
  } catch (cause) {
    throw new ApiError("The local controller could not be reached.", {
      code: "controller_unreachable",
      retryable: true,
      remediation: "Make sure the controller is running, then refresh this view.",
      cause,
    });
  }

  const raw = await response.text();
  let payload = null;
  if (raw) {
    try { payload = JSON.parse(raw); } catch { payload = { raw }; }
  }
  if (!response.ok) {
    const error = payload?.error || payload?.errors?.[0] || payload || {};
    throw new ApiError(error.message || `The controller returned HTTP ${response.status}.`, {
      code: error.code,
      status: response.status,
      retryable: error.retryable ?? response.status >= 500,
      remediation: error.remediation,
      operation_id: error.operation_id || error.operationId,
      payload,
    });
  }
  return payload ?? {};
}

export function normalizeNode(raw = {}) {
  const observed = objectValue(raw, "observed");
  const memory = { ...objectValue(observed, "memory"), ...objectValue(raw, "memory") };
  const gpu = { ...objectValue(observed, "gpu"), ...objectValue(raw, "gpu") };
  const deploymentCandidate = raw.deployment || raw.active_deployment || observed.deployment || null;
  const deploymentState = String(firstDefined(deploymentCandidate?.state, deploymentCandidate?.observed_state, "unknown")).toLowerCase();
  const deployment = deploymentCandidate && !["stopped", "failed"].includes(deploymentState) ? deploymentCandidate : null;
  const totalMemory = numberValue(memory.total_bytes, memory.totalBytes, memory.total, raw.memory_total_bytes);
  const usedMemory = numberValue(memory.used_bytes, memory.usedBytes, memory.used, raw.memory_used_bytes);
  const memoryPercent = numberValue(memory.used_percent, memory.usedPercent, totalMemory ? (usedMemory / totalMemory) * 100 : null);
  return {
    ...raw,
    id: String(firstDefined(raw.id, raw.node_id, raw.name, "unknown-node")),
    name: String(firstDefined(raw.display_name, raw.displayName, raw.name, raw.hostname, raw.id, "Unnamed Spark")),
    address: String(firstDefined(raw.management_address, raw.address, raw.host, raw.hostname, "address not recorded")),
    state: String(firstDefined(raw.state, observed.state, "unknown")).toLowerCase(),
    lastRefresh: firstDefined(raw.last_refresh_at, raw.lastRefreshAt, raw.observed_at, observed.observed_at, raw.updated_at),
    endpoint: firstDefined(raw.endpoint, raw.endpoint_url, raw.endpointUrl, deployment?.endpoint, "—"),
    modelAlias: firstDefined(raw.active_model_alias, raw.activeModelAlias, deployment?.alias, deployment?.model_alias, observed.model_alias),
    deployment,
    memory: { ...memory, totalBytes: totalMemory, usedBytes: usedMemory, usedPercent: memoryPercent },
    gpu: { ...gpu, name: firstDefined(gpu.name, gpu.model, gpu.product, "GPU state unavailable"), utilization: numberValue(gpu.utilization, gpu.utilization_percent, gpu.utilizationPercent) },
    runtime: firstDefined(raw.runtime_version, raw.runtimeVersion, raw.runtime, observed.runtime, "Runtime not recorded"),
    uptime: firstDefined(raw.uptime, observed.uptime),
    warnings: asArray(raw.warnings || observed.warnings),
  };
}

export function normalizeModel(raw = {}) {
  const inventory = raw.inventory || raw.presence || {};
  return {
    ...raw,
    id: String(firstDefined(raw.id, raw.model_id, raw.profile_id, "unknown-model")),
    name: String(firstDefined(raw.display_name, raw.displayName, raw.name, raw.alias, "Unnamed model")),
    alias: firstDefined(raw.alias, raw.api_alias, raw.apiAlias, raw.model_alias, "alias not recorded"),
    format: String(firstDefined(raw.format, raw.backend, "model")),
    description: firstDefined(raw.description, raw.summary, "Allowlisted deployment profile."),
    sizeBytes: numberValue(raw.size_bytes, raw.sizeBytes, raw.size),
    quantization: firstDefined(raw.quantization, raw.quant, raw.variant, "—"),
    context: firstDefined(raw.context, raw.context_length, raw.contextLength, "—"),
    expectedMemory: numberValue(raw.expected_memory_bytes, raw.expectedMemoryBytes, raw.memory_bytes),
    validation: String(firstDefined(raw.validation_state, raw.validationState, raw.state, "unknown")).toLowerCase(),
    inventory,
    nodes: asArray(raw.nodes || raw.presence_by_node || raw.presenceByNode),
  };
}

export function normalizeProfile(raw = {}) {
  const model = raw.model || {};
  return {
    ...raw,
    id: String(firstDefined(raw.id, raw.profile_id, "unknown-profile")),
    name: String(firstDefined(raw.display_name, raw.displayName, raw.name, model.display_name, model.name, "Unnamed profile")),
    modelId: firstDefined(raw.model_id, raw.modelId, model.id),
    alias: firstDefined(raw.alias, raw.api_alias, raw.apiAlias, model.alias, "alias not recorded"),
    backend: String(firstDefined(raw.backend, raw.runtime, "runtime not recorded")),
    mode: String(firstDefined(raw.mode, raw.deployment_mode, raw.deploymentMode, "single-node")),
    nodeCount: numberValue(raw.node_count, raw.nodeCount) || 1,
    script: firstDefined(raw.script_path, raw.scriptPath, raw.runner?.script, raw.recipe?.script, "allowlisted recipe"),
    startupTimeout: firstDefined(raw.startup_timeout_seconds, raw.startupTimeoutSeconds, raw.timeouts?.startup, "bounded"),
    weightsBytes: numberValue(raw.weights_bytes, raw.weightsBytes, raw.expected_memory_bytes, raw.expectedMemoryBytes),
    reservedMemoryBytes: numberValue(raw.reserved_memory_bytes, raw.reservedMemoryBytes),
    gpuMemoryUtilization: numberValue(raw.gpu_memory_utilization, raw.gpuMemoryUtilization),
    origin: String(firstDefined(raw.origin, "reviewed")).toLowerCase(),
    provenance: objectValue(raw, "provenance"),
    validation: String(firstDefined(raw.validation_state, raw.validationState, raw.state, "unknown")).toLowerCase(),
    description: firstDefined(raw.description, model.description, "An allowlisted launch recipe with an explicit readiness contract."),
    invariants: asArray(raw.invariants || raw.required_invariants || raw.requiredInvariants),
    model,
  };
}

function nodeIdList(raw) {
  return asArray(raw.node_ids || raw.nodeIds || raw.nodes).map((value) => typeof value === "object" ? String(firstDefined(value.id, value.node_id, "")) : String(value));
}

export function normalizeDeployment(raw = {}) {
  return {
    ...raw,
    id: String(firstDefined(raw.id, raw.deployment_id, "unknown-deployment")),
    state: String(firstDefined(raw.state, raw.observed_state, raw.observedState, "unknown")).toLowerCase(),
    desiredState: String(firstDefined(raw.desired_state, raw.desiredState, "unknown")).toLowerCase(),
    profileId: firstDefined(raw.profile_id, raw.profileId, raw.model_profile_id),
    alias: firstDefined(raw.alias, raw.api_alias, raw.apiAlias, raw.model_alias),
    endpoint: firstDefined(raw.endpoint, raw.endpoint_url, raw.endpointUrl),
    nodeIds: nodeIdList(raw),
    updatedAt: firstDefined(raw.updated_at, raw.updatedAt, raw.created_at),
  };
}

export function normalizeOperation(raw = {}) {
  const events = asArray(raw.events || raw.event_log || raw.eventLog);
  const progress = numberValue(raw.progress_percent, raw.progressPercent, raw.progress, raw.percent);
  const state = String(firstDefined(raw.state, raw.status, "unknown")).toLowerCase();
  const action = String(firstDefined(raw.action, raw.kind, raw.type, "operation")).toLowerCase();
  return {
    ...raw,
    id: String(firstDefined(raw.id, raw.operation_id, raw.operationId, "unknown-operation")),
    action,
    state,
    phase: firstDefined(raw.phase, raw.current_phase, raw.currentPhase, events.at(-1)?.phase, "observing"),
    progress: progress === null ? operationProgressFromState(state) : Math.max(0, Math.min(100, progress)),
    message: firstDefined(raw.message, raw.current_message, raw.currentMessage, events.at(-1)?.message, "Waiting for controller update."),
    profileId: firstDefined(raw.profile_id, raw.profileId),
    deploymentId: firstDefined(raw.deployment_id, raw.deploymentId),
    nodeIds: nodeIdList(raw),
    requestedAt: firstDefined(raw.requested_at, raw.requestedAt, raw.created_at, raw.createdAt),
    startedAt: firstDefined(raw.started_at, raw.startedAt),
    finishedAt: firstDefined(raw.finished_at, raw.finishedAt, raw.completed_at, raw.completedAt),
    events,
    logs: asArray(raw.logs || raw.log_tail || raw.logTail),
    error: raw.error || raw.failure || null,
    result: raw.result || null,
  };
}

export function operationProgressFromState(state) {
  if (["succeeded", "ready", "completed"].includes(state)) return 100;
  if (["failed", "cancelled", "needs-attention"].includes(state)) return 100;
  if (["running", "queued", "loading", "unloading"].includes(state)) return 35;
  return 0;
}

export function isTerminalOperation(operation) {
  const state = typeof operation === "string" ? operation : operation?.state;
  return ["succeeded", "failed", "cancelled", "needs-attention", "completed"].includes(String(state).toLowerCase());
}

export function operationProgress(operation) {
  return Math.round(numberValue(operation?.progress, operationProgressFromState(operation?.state)) || 0);
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
  if (setup?.setup_required || setup?.setupRequired) return { state: "warning", label: "Setup required" };
  return { state: "ready", label: "Controller ready" };
}

export function statusLabel(value) {
  const text = String(value || "unknown").replaceAll("_", "-");
  return text.replace(/(^|-)([a-z])/g, (_, prefix, letter) => `${prefix ? " " : ""}${letter.toUpperCase()}`);
}

export function actionLabel(action) {
  const value = String(action || "operation").toLowerCase();
  if (value.includes("recover")) return "Recover baseline";
  if (value.includes("unload") || value.includes("stop")) return "Unload model";
  if (value.includes("load") || value.includes("start") || value.includes("switch")) return "Load model";
  if (value.includes("reconcile") || value.includes("refresh")) return "Refresh observations";
  return statusLabel(value);
}

export function actionGlyph(action) {
  const value = String(action || "").toLowerCase();
  if (value.includes("recover")) return "↺";
  if (value.includes("unload") || value.includes("stop")) return "↓";
  if (value.includes("reconcile") || value.includes("refresh")) return "↻";
  return "↑";
}

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
  return `<span class="operation-action-mark" data-action="${escapeHTML(action)}" aria-hidden="true">${escapeHTML(actionGlyph(action))}</span>`;
}

export function deploymentForNode(node) {
  const inline = node?.deployment;
  if (inline && typeof inline === "object") {
    const deployment = normalizeDeployment(inline);
    if (!["stopped", "failed"].includes(deployment.state)) return deployment;
  }
  return null;
}

/* Nodes a load on targetIds will stop. A deployment observed on a selected node
 * is replaced on every node it uses, because it cannot keep running on part of
 * its placement, so nodes outside the selection are named too. */
export function switchStopEntries(targetIds, nodes, deployments = []) {
  const selected = new Set(asArray(targetIds).map(String));
  const nodeList = asArray(nodes);
  const nodesById = new Map(nodeList.map((node) => [node.id, node]));
  const listed = new Map(asArray(deployments).map((item) => {
    const deployment = normalizeDeployment(item);
    return [deployment.id, deployment];
  }));
  const entries = new Map();
  for (const node of nodeList) {
    if (!selected.has(node.id)) continue;
    const deployment = deploymentForNode(node);
    if (!deployment) continue;
    const nodeIds = [node.id, ...deployment.nodeIds, ...(listed.get(deployment.id)?.nodeIds || [])];
    for (const nodeId of nodeIds) {
      if (!nodeId || entries.has(nodeId)) continue;
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
