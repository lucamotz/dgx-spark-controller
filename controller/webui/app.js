import {
  apiFetch,
  asArray,
  collection,
  controllerStatus,
  copyText,
  countLabel,
  createIdempotencyKey,
  deploymentForNode,
  errorText,
  escapeHTML,
  factPairs,
  firstDefined,
  formatDate,
  formatRelativeTime,
  isTerminalOperation,
  normalizeDeployment,
  normalizeNode,
  normalizeOperation,
  normalizeProfile,
  startPoller,
  switchStopEntries,
  trapModalFocus,
} from "./ui-core.js";
import { monitorStatus, renderDashboard, renderTelemetry } from "./ui-dashboard.js";
import { compatibleTargetNodes, deployFormMarkup, profileNodeCount, renderModelCatalog } from "./ui-models.js";
import { getOperationLogs, renderOperations } from "./ui-operations.js";
import { mountRecipes, refreshRecipes } from "./ui-recipes.js";
import { mountSetup, refreshSetup } from "./ui-setup.js";
import { mountUpdates, refreshUpdates } from "./ui-updates.js";

const PAGES = ["dashboard", "models", "recipes", "operations", "updates", "diagnostics", "setup"];

// Snapshot collections: the route that serves each one alone (the fallback when
// /bootstrap fails) and how its items are normalized.
const RESOURCES = [
  ["nodes", "/nodes", normalizeNode],
  ["models", "/models", (model) => ({ ...model })],
  ["profiles", "/profiles", normalizeProfile],
  ["deployments", "/deployments", normalizeDeployment],
  ["operations", "/operations?limit=50", normalizeOperation],
];

const state = {
  nodes: [],
  models: [],
  profiles: [],
  deployments: [],
  operations: [],
  warnings: [],
  health: {},
  setup: null,
  setupRedirected: false,
  selectedOperationId: null,
  modelFilter: "all",
  operationQuery: "",
  syncing: false,
  lastSync: null,
  page: "dashboard",
  telemetry: new Map(),
  monitoringPaused: false,
  telemetryFailed: false,
};

const elements = {
  apiStatus: document.querySelector("#api-status"),
  apiStatusLabel: document.querySelector("#api-status .connection-label"),
  syncTime: document.querySelector("#sync-time"),
  globalNotice: document.querySelector("#global-notice"),
  refreshButton: document.querySelector("#refresh-button"),
  reconcileButton: document.querySelector("#reconcile-button"),
  recoverButton: document.querySelector("#recover-button"),
  dashboardLoadButton: document.querySelector("#dashboard-load-button"),
  monitorStatus: document.querySelector("#monitor-status"),
  monitorToggle: document.querySelector("#monitor-toggle"),
  summary: document.querySelector("#summary-grid"),
  nodes: document.querySelector("#node-grid"),
  recentOperations: document.querySelector("#recent-operation-list"),
  models: document.querySelector("#model-grid"),
  operations: document.querySelector("#operation-list"),
  operationDetail: document.querySelector("#operation-detail"),
  operationCount: document.querySelector("#operation-count"),
  operationSearch: document.querySelector("#operation-search"),
  healthBadge: document.querySelector("#health-badge"),
  healthFacts: document.querySelector("#health-facts"),
  healthRefresh: document.querySelector("#health-refresh-button"),
  warningCount: document.querySelector("#warning-count"),
  warningList: document.querySelector("#warning-list"),
  supportPreview: document.querySelector("#support-preview"),
  copySupport: document.querySelector("#copy-support-button"),
  openUIDocs: document.querySelector("#open-ui-docs-button"),
  toast: document.querySelector("#toast"),
  deployBackdrop: document.querySelector("#deploy-modal-backdrop"),
  deployModal: document.querySelector("#deploy-modal"),
  deployForm: document.querySelector("#deploy-form"),
  deployFormContent: document.querySelector("#deploy-form-content"),
  deploySubmit: document.querySelector("#deploy-submit-button"),
  deployClose: document.querySelector("#deploy-modal-close"),
  deployCancel: document.querySelector("#deploy-cancel-button"),
  logBackdrop: document.querySelector("#log-modal-backdrop"),
  logModal: document.querySelector("#log-modal"),
  logClose: document.querySelector("#log-modal-close"),
  logMeta: document.querySelector("#log-modal-meta"),
  logView: document.querySelector("#log-view"),
  logCopy: document.querySelector("#log-copy-button"),
  confirmBackdrop: document.querySelector("#confirm-modal-backdrop"),
  confirmModal: document.querySelector("#confirm-modal"),
  confirmTitle: document.querySelector("#confirm-modal-title"),
  confirmMessage: document.querySelector("#confirm-modal-message"),
  confirmDetails: document.querySelector("#confirm-modal-details"),
  confirmAccept: document.querySelector("#confirm-modal-accept"),
  confirmCancel: document.querySelector("#confirm-modal-cancel"),
  confirmClose: document.querySelector("#confirm-modal-close"),
};

// In Escape priority order: Escape closes only the first open dialog.
const MODALS = [
  { backdrop: elements.deployBackdrop, dialog: elements.deployModal, close: closeDeployModal },
  { backdrop: elements.logBackdrop, dialog: elements.logModal, close: closeLogs },
  { backdrop: elements.confirmBackdrop, dialog: elements.confirmModal, close: () => closeConfirmation(false) },
];

let stopOperationPoller = null;
let telemetryPending = false;
let modalReturnFocus = null;
let activeLogText = "";
let deployContext = { profileId: null, targetNodeIds: [], conflictConfirmed: false };
let toastTimer = null;
let pendingConfirmation = null;

function setApiStatus(kind, label) {
  elements.apiStatus.dataset.state = kind;
  elements.apiStatusLabel.textContent = label;
}

function setNotice(message, tone = "danger") {
  if (!message) {
    elements.globalNotice.hidden = true;
    elements.globalNotice.textContent = "";
    return;
  }
  elements.globalNotice.hidden = false;
  elements.globalNotice.dataset.tone = tone;
  elements.globalNotice.innerHTML = `<span aria-hidden="true">${tone === "success" ? "✓" : "!"}</span><span>${escapeHTML(message)}</span>`;
}

function toast(message, tone = "neutral") {
  window.clearTimeout(toastTimer);
  elements.toast.hidden = false;
  elements.toast.dataset.tone = tone;
  elements.toast.textContent = message;
  toastTimer = window.setTimeout(() => { elements.toast.hidden = true; }, 4200);
}

async function copyWithToast(text, message) {
  await copyText(text);
  toast(message, "success");
}

function showModal(backdrop, dialog) {
  modalReturnFocus = document.activeElement;
  backdrop.hidden = false;
  window.setTimeout(() => dialog.focus(), 0);
}

function hideModal(backdrop) {
  backdrop.hidden = true;
  if (modalReturnFocus?.focus) modalReturnFocus.focus();
}

// `details` is markup built by a module helper that escapes every value.
function requestConfirmation({ title, message, details = "", confirmLabel = "Continue" }) {
  if (pendingConfirmation) pendingConfirmation(false);
  elements.confirmTitle.textContent = title;
  elements.confirmMessage.textContent = message;
  elements.confirmDetails.innerHTML = details;
  elements.confirmDetails.hidden = !details;
  elements.confirmAccept.textContent = confirmLabel;
  showModal(elements.confirmBackdrop, elements.confirmModal);
  return new Promise((resolve) => { pendingConfirmation = resolve; });
}

function closeConfirmation(confirmed = false) {
  if (elements.confirmBackdrop.hidden) return;
  const resolve = pendingConfirmation;
  pendingConfirmation = null;
  hideModal(elements.confirmBackdrop);
  if (resolve) resolve(confirmed);
}

function operationFromPayload(payload) {
  const candidate = payload?.operation || payload?.data?.operation || payload?.result?.operation || payload?.data || payload;
  if (!candidate || typeof candidate !== "object") return null;
  const hasExplicitOperationId = Boolean(candidate.operation_id || candidate.operationId);
  const hasOperationShape = Boolean(
    candidate.id
    && (candidate.action || candidate.kind || candidate.type)
    && (candidate.state || candidate.status),
  );
  if (!hasExplicitOperationId && !hasOperationShape) return null;
  return normalizeOperation(candidate);
}

function applySnapshot(payload, resourcePayloads = {}) {
  for (const [key, , normalize] of RESOURCES) {
    const resource = resourcePayloads[key];
    state[key] = (resource ? resourceItems(resource, key) : collection(payload, key)).map(normalize);
  }
  state.warnings = collection(payload, "warnings");
  state.health = payload?.health || payload?.data?.health || {};
  state.setup = payload?.setup || null;
  state.lastSync = new Date().toISOString();
}

function resourceItems(payload, key) {
  const named = collection(payload, key);
  if (named.length) return named;
  return asArray(payload);
}

async function loadSnapshot({ quiet = false } = {}) {
  if (state.syncing) return;
  state.syncing = true;
  if (!quiet) {
    setApiStatus("loading", "Syncing");
    elements.refreshButton.classList.add("is-spinning");
  }
  let bootstrap = null;
  let bootstrapError = null;
  try {
    bootstrap = await apiFetch("/bootstrap");
  } catch (error) {
    bootstrapError = error;
  }

  if (bootstrap) {
    applySnapshot(bootstrap);
  } else {
    const results = await Promise.allSettled(RESOURCES.map(([, path]) => apiFetch(path)));
    if (!results.some((result) => result.status === "fulfilled")) {
      state.syncing = false;
      setApiStatus("error", "Controller offline");
      elements.refreshButton.classList.remove("is-spinning");
      setNotice(bootstrapError?.message || "The controller did not return a usable snapshot.");
      renderAll();
      return;
    }
    applySnapshot({}, Object.fromEntries(RESOURCES.map(([key], index) => (
      [key, results[index].status === "fulfilled" ? results[index].value : null]
    ))));
    setNotice("Bootstrap is unavailable; showing the resource responses that did arrive.");
  }

  state.syncing = false;
  const status = controllerStatus(state.setup);
  setApiStatus(status.state, status.label);
  elements.refreshButton.classList.remove("is-spinning");
  elements.syncTime.textContent = `last sync ${formatRelativeTime(state.lastSync)}`;
  if (!bootstrapError) setNotice("");
  if (state.setup?.config_error) {
    setNotice(`The node configuration is invalid: ${state.setup.config_error.message}`);
  } else if (state.setup?.setup_required) {
    setNotice("No DGX Spark nodes are configured yet. Use Setup to find and connect them.", "neutral");
    if (!state.setupRedirected && state.page === "dashboard") {
      state.setupRedirected = true;
      showPage("setup");
    }
  }
  renderAll();
  watchActiveOperation();
}

function renderAll() {
  renderDashboard(state, { summary: elements.summary, nodes: elements.nodes, recentOperations: elements.recentOperations }, {
    onNodeAction: handleNodeAction,
    onOperationSelect: selectOperation,
  });
  renderModelsPage();
  renderOperationsPage();
  renderDiagnostics();
  updateTelemetry();
}

function renderModelsPage() {
  renderModelCatalog(elements.models, state, state.modelFilter, openDeployModal);
}

function renderOperationsPage() {
  renderOperations({ list: elements.operations, detail: elements.operationDetail, count: elements.operationCount }, state, {
    query: state.operationQuery,
    onSelect: selectOperation,
    onLogs: openLogs,
    onCancel: cancelOperation,
  });
}

function showPage(page) {
  // A new page starts at its top instead of inheriting the old scroll offset.
  if (state.page && state.page !== page) window.scrollTo(0, 0);
  state.page = page;
  document.querySelectorAll("[data-page]").forEach((section) => {
    const active = section.dataset.page === page;
    section.classList.toggle("is-active", active);
    section.hidden = !active;
  });
  document.querySelectorAll("[data-page-link]").forEach((link) => {
    const active = link.dataset.pageLink === page;
    link.classList.toggle("is-active", active);
    if (link.tagName === "BUTTON") {
      if (active) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    }
  });
  if (window.location.hash !== `#${page}`) window.history.replaceState(null, "", `#${page}`);
  if (page === "setup") refreshSetup();
  if (page === "recipes") refreshRecipes();
  if (page === "updates") refreshUpdates();
  updateTelemetry();
  refreshTelemetry();
}

function monitoringActive() {
  return state.page === "dashboard" && !document.hidden && !state.monitoringPaused;
}

function updateTelemetry() {
  const options = { paused: !monitoringActive(), failed: state.telemetryFailed };
  renderTelemetry(elements.nodes, state.telemetry, options);
  const monitor = monitorStatus(state.nodes, state.telemetry, options);
  elements.monitorStatus.textContent = monitor.label;
  elements.monitorStatus.dataset.state = monitor.state;
  elements.monitorToggle.textContent = state.monitoringPaused ? "Resume monitoring" : "Pause monitoring";
  elements.monitorToggle.setAttribute("aria-pressed", String(state.monitoringPaused));
}

async function refreshTelemetry() {
  if (!monitoringActive() || telemetryPending) return;
  telemetryPending = true;
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), 30000);
  try {
    const payload = await apiFetch("/telemetry", { signal: controller.signal });
    state.telemetry = new Map(collection(payload, "items").map((sample) => [sample.node_id, sample]));
    state.telemetryFailed = false;
  } catch {
    state.telemetryFailed = true;
  } finally {
    window.clearTimeout(timeout);
    telemetryPending = false;
    updateTelemetry();
  }
}

function renderDiagnostics() {
  const health = state.health || {};
  const healthy = String(firstDefined(health.status, health.state, "unknown")).toLowerCase() === "ok" || health.healthy === true || state.lastSync;
  elements.healthBadge.textContent = healthy ? "Available" : "Unknown";
  elements.healthBadge.dataset.tone = healthy ? "success" : "neutral";
  elements.healthFacts.innerHTML = factPairs([
    ["API status", firstDefined(health.status, health.state, state.lastSync ? "reachable" : "not checked")],
    ["Controller version", firstDefined(health.version, health.controller_version, "not reported")],
    ["Schema", firstDefined(health.schema_version, health.schemaVersion, "not reported")],
    ["Last sync", state.lastSync ? formatDate(state.lastSync) : "not recorded"],
  ]);
  const warnings = collectWarnings();
  elements.warningCount.textContent = countLabel(warnings.length, "item");
  elements.warningList.innerHTML = warnings.length
    ? warnings.map(warningMarkup).join("")
    : `<div class="warning-item" data-tone="success">No warnings reported by the latest observation.</div>`;
  elements.supportPreview.textContent = JSON.stringify(buildSupportContext(), null, 2);
}

// Controller warnings followed by each node's own, prefixed with the node name.
function collectWarnings() {
  const warnings = [...state.warnings];
  for (const node of state.nodes) {
    for (const warning of node.warnings) {
      warnings.push(typeof warning === "string" ? `${node.name}: ${warning}` : { ...warning, message: `${node.name}: ${warning.message || warning.detail || "warning"}` });
    }
  }
  return warnings;
}

function warningMarkup(warning) {
  const text = typeof warning === "string" ? warning : warning.message || warning.detail || warning.code || JSON.stringify(warning);
  const tone = typeof warning === "object" && (warning.severity === "error" || warning.severity === "danger") ? "danger" : "warning";
  return `<div class="warning-item" data-tone="${tone}">${escapeHTML(text)}</div>`;
}

function buildSupportContext() {
  return {
    generated_at: new Date().toISOString(),
    controller: {
      status: firstDefined(state.health?.status, state.health?.state, "unknown"),
      version: firstDefined(state.health?.version, state.health?.controller_version, "unknown"),
      schema_version: firstDefined(state.health?.schema_version, state.health?.schemaVersion, "unknown"),
    },
    nodes: state.nodes.map((node) => ({
      id: node.id,
      name: node.name,
      address: node.address,
      state: node.state,
      endpoint: node.endpoint,
      model_alias: node.modelAlias || null,
      runtime: node.runtime,
      memory: { used_bytes: node.memory.usedBytes, total_bytes: node.memory.totalBytes, used_percent: node.memory.usedPercent },
      gpu: { name: node.gpu.name, utilization: node.gpu.utilization },
      last_refresh: node.lastRefresh,
    })),
    operations: state.operations.slice(0, 20).map((operation) => ({
      id: operation.id,
      action: operation.action,
      state: operation.state,
      phase: operation.phase,
      progress: operation.progress,
      requested_at: operation.requestedAt,
      profile_id: operation.profileId || null,
    })),
    warnings: state.warnings,
  };
}

async function runHealthCheck() {
  elements.healthRefresh.disabled = true;
  try {
    state.health = await apiFetch("/api/health");
    const status = controllerStatus(state.health);
    setApiStatus(status.state, status.label);
    renderDiagnostics();
    toast("Health check complete.", "success");
  } catch (error) {
    state.health = { status: "error", message: error.message };
    setApiStatus("error", "Controller offline");
    renderDiagnostics();
    toast(error.message, "danger");
  } finally {
    elements.healthRefresh.disabled = false;
  }
}

function selectOperation(operationId) {
  if (!operationId) return;
  state.selectedOperationId = operationId;
  showPage("operations");
  renderAll();
  const selected = state.operations.find((operation) => operation.id === operationId);
  if (selected && !isTerminalOperation(selected)) startOperationPolling(selected.id);
}

function mergeOperation(operation) {
  if (!operation) return;
  const index = state.operations.findIndex((item) => item.id === operation.id);
  if (index < 0) state.operations.unshift(operation);
  else state.operations[index] = { ...state.operations[index], ...operation };
  state.selectedOperationId = operation.id;
}

async function refreshOperation(operationId) {
  try {
    const payload = await apiFetch(`/operations/${encodeURIComponent(operationId)}`);
    const operation = normalizeOperation(payload?.operation || payload?.data?.operation || payload);
    mergeOperation(operation);
    renderAll();
    return operation;
  } catch (error) {
    toast(error.message || "Operation update failed.", "danger");
    return null;
  }
}

function startOperationPolling(operationId) {
  if (!operationId) return;
  if (stopOperationPoller) stopOperationPoller();
  stopOperationPoller = startPoller(async () => {
    const operation = await refreshOperation(operationId);
    if (operation && isTerminalOperation(operation)) {
      if (stopOperationPoller) stopOperationPoller();
      stopOperationPoller = null;
      await loadSnapshot({ quiet: true });
      toast(operation.state === "succeeded" ? "Operation completed." : "Operation needs attention.", operation.state === "succeeded" ? "success" : "danger");
    }
  }, 1800);
}

function watchActiveOperation() {
  const active = state.operations.find((operation) => !isTerminalOperation(operation));
  if (active && (!stopOperationPoller || state.selectedOperationId !== active.id)) startOperationPolling(active.id);
}

function idempotencyHeader(prefix) {
  return { "Idempotency-Key": createIdempotencyKey(prefix) };
}

async function runMutation(label, request) {
  try {
    const payload = await request();
    const operation = operationFromPayload(payload);
    if (operation) {
      mergeOperation(operation);
      renderAll();
      startOperationPolling(operation.id);
      toast(`${label} started. Progress will remain visible if you reload.`, "success");
    } else {
      toast(`${label} accepted. Refreshing observations.`, "success");
      await loadSnapshot({ quiet: true });
    }
    return operation;
  } catch (error) {
    setNotice(errorText(error) || `${label} failed.`);
    toast(`${label} failed.`, "danger");
    return null;
  }
}

async function handleNodeAction(action, nodeId) {
  const node = state.nodes.find((item) => item.id === nodeId);
  if (!node) return;
  if (action === "load") {
    openDeployModal(null, nodeId);
    return;
  }
  if (action === "unload") {
    const deployment = deploymentForNode(node);
    if (!deployment) return toast("No managed deployment is attached to this node.");
    const affectedNodes = deployment.nodeIds.map((id) => state.nodes.find((item) => item.id === id)?.name || id);
    const confirmed = await requestConfirmation({
      title: "Unload active model?",
      message: `Unload the active model from ${affectedNodes.join(", ") || node.name}? This stops the remote deployment; closing the app does not.`,
      confirmLabel: "Unload model",
    });
    if (!confirmed) return;
    await unloadDeployment(deployment);
    return;
  }
  if (action === "refresh") {
    await runMutation("Observation refresh", () => apiFetch(`/nodes/${encodeURIComponent(nodeId)}/refresh`, { method: "POST", body: {} }));
  }
}

async function unloadDeployment(deployment) {
  await runMutation("Unload", () => apiFetch(`/deployments/${encodeURIComponent(deployment.id)}`, {
    method: "DELETE",
    headers: idempotencyHeader("unload"),
  }));
}

async function recoverBaseline() {
  const activeDeployment = state.deployments.find((deployment) =>
    !["stopped", "failed"].includes(deployment.state)
    && state.profiles.some((profile) => profile.id === deployment.profileId && profile.mode === "shared-vllm")
  );
  const activeAlias = state.nodes.find((node) => node.modelAlias)?.modelAlias;
  const profile = state.profiles.find((item) => item.id === activeDeployment?.profileId)
    || state.profiles.find((item) => item.mode === "shared-vllm" && item.alias === activeAlias)
    || state.profiles.find((item) => item.mode === "shared-vllm");
  if (!profile) return toast("No reviewed recoverable cluster profile is registered.", "danger");
  const confirmed = await requestConfirmation({
    title: "Recover DGX Spark baseline?",
    message: `Stop only containers matching ${profile.name || profile.id}, then verify processes, endpoints, aliases, memory, and swap are clear on every assigned node. Unknown processes will not be stopped.`,
    confirmLabel: "Recover baseline",
  });
  if (!confirmed) return;
  await runMutation("Baseline recovery", () => apiFetch("/recover", {
    method: "POST",
    body: { profile_id: profile.id, confirmation: true },
    headers: idempotencyHeader("recover"),
  }));
}

async function cancelOperation(operation) {
  if (!operation || isTerminalOperation(operation)) return;
  const confirmed = await requestConfirmation({
    title: "Cancel operation?",
    message: "Ask the controller to cancel this operation if the current phase is safe to stop?",
    confirmLabel: "Cancel operation",
  });
  if (!confirmed) return;
  await runMutation("Cancellation", () => apiFetch(`/operations/${encodeURIComponent(operation.id)}/cancel`, {
    method: "POST",
    body: {},
    headers: idempotencyHeader("cancel"),
  }));
}

function conflictsForTargets(targetIds) {
  return switchStopEntries(targetIds, state.nodes, state.deployments);
}

function openDeployModal(profileId = null, preferredNodeId = null) {
  const fallbackProfile = profileId || state.profiles[0]?.id || null;
  const targets = compatibleTargetNodes(state, fallbackProfile);
  const requiredCount = profileNodeCount(state.profiles.find((item) => item.id === fallbackProfile));
  const preferred = preferredNodeId && targets.some((node) => node.id === preferredNodeId)
    ? [targets.find((node) => node.id === preferredNodeId), ...targets.filter((node) => node.id !== preferredNodeId)]
    : targets;
  deployContext = {
    profileId: fallbackProfile,
    targetNodeIds: preferred.slice(0, requiredCount).map((node) => node.id),
    conflictConfirmed: false,
  };
  renderDeployForm();
  showModal(elements.deployBackdrop, elements.deployModal);
}

function renderDeployForm() {
  const profiles = state.profiles;
  const profile = profiles.find((item) => item.id === deployContext.profileId) || null;
  const requiredCount = profileNodeCount(profile);
  const targets = compatibleTargetNodes(state, deployContext.profileId);
  deployContext.targetNodeIds = deployContext.targetNodeIds.filter((id) => targets.some((node) => node.id === id));
  if (deployContext.targetNodeIds.length !== requiredCount && targets.length >= requiredCount) {
    deployContext.targetNodeIds = targets.slice(0, requiredCount).map((node) => node.id);
  }
  const conflicts = conflictsForTargets(deployContext.targetNodeIds);
  const content = elements.deployFormContent;
  content.innerHTML = deployFormMarkup({ ...deployContext, profiles, profile, requiredCount, targets, conflicts });
  elements.deploySubmit.disabled = !profile || deployContext.targetNodeIds.length !== requiredCount;
  elements.deploySubmit.textContent = conflicts.length ? "Confirm & load" : "Load profile";
  elements.deploySubmit.classList.toggle("button-danger", conflicts.length);
  elements.deploySubmit.classList.toggle("button-primary", !conflicts.length);
  content.querySelector("#deploy-profile-select")?.addEventListener("change", (event) => {
    deployContext.profileId = event.target.value;
    const nextCount = profileNodeCount(profiles.find((item) => item.id === deployContext.profileId));
    deployContext.targetNodeIds = compatibleTargetNodes(state, deployContext.profileId).slice(0, nextCount).map((node) => node.id);
    deployContext.conflictConfirmed = false;
    renderDeployForm();
  });
  content.querySelectorAll("input[name=target-node]").forEach((input) => input.addEventListener("change", () => {
    deployContext.targetNodeIds = [...content.querySelectorAll("input[name=target-node]:checked")].map((item) => item.value);
    deployContext.conflictConfirmed = false;
    renderDeployForm();
  }));
  content.querySelector("#confirm-conflict")?.addEventListener("change", (event) => { deployContext.conflictConfirmed = event.target.checked; });
}

function closeDeployModal() {
  hideModal(elements.deployBackdrop);
}

async function submitDeployment(event) {
  event.preventDefault();
  const conflicts = conflictsForTargets(deployContext.targetNodeIds);
  if (conflicts.length && !deployContext.conflictConfirmed) {
    setNotice("Review the active deployment warning and confirm the affected nodes before continuing.");
    elements.deployFormContent.querySelector("#confirm-conflict")?.focus();
    return;
  }
  const profileId = deployContext.profileId;
  const nodeIds = [...deployContext.targetNodeIds];
  closeDeployModal();
  await runMutation("Load", () => apiFetch("/deployments", {
    method: "POST",
    body: { profile_id: profileId, node_ids: nodeIds },
    headers: idempotencyHeader("load"),
  }));
}

async function openLogs(operation) {
  elements.logMeta.textContent = `${operation.id} · loading redacted tail`;
  elements.logView.textContent = "Loading logs…";
  showModal(elements.logBackdrop, elements.logModal);
  let text = getOperationLogs(operation);
  try {
    const payload = await apiFetch(`/operations/${encodeURIComponent(operation.id)}/logs`);
    const lines = collection(payload, "logs");
    if (lines.length) text = lines.map((line) => typeof line === "string" ? line : line.message || line.detail || JSON.stringify(line)).join("\n");
  } catch (error) {
    if (!text) text = `Unable to load the remote log tail. ${error.message}`;
  }
  activeLogText = text || "No redacted logs were recorded for this operation.";
  elements.logMeta.textContent = `${operation.id} · ${activeLogText.split("\n").filter(Boolean).length} lines`;
  elements.logView.textContent = activeLogText;
}

function closeLogs() {
  hideModal(elements.logBackdrop);
}

function bindEvents() {
  document.querySelectorAll("[data-page-link]").forEach((link) => link.addEventListener("click", (event) => {
    event.preventDefault();
    showPage(link.dataset.pageLink);
  }));
  document.querySelectorAll("[data-model-filter]").forEach((button) => button.addEventListener("click", () => {
    state.modelFilter = button.dataset.modelFilter;
    document.querySelectorAll("[data-model-filter]").forEach((item) => item.classList.toggle("is-selected", item === button));
    renderModelsPage();
  }));
  elements.monitorToggle.addEventListener("click", () => {
    state.monitoringPaused = !state.monitoringPaused;
    updateTelemetry();
    refreshTelemetry();
  });
  document.addEventListener("visibilitychange", () => {
    updateTelemetry();
    refreshTelemetry();
  });
  elements.refreshButton.addEventListener("click", () => loadSnapshot());
  elements.reconcileButton.addEventListener("click", () => runMutation("Observation refresh", () => apiFetch("/reconcile", { method: "POST", body: {} })));
  elements.recoverButton.addEventListener("click", recoverBaseline);
  elements.dashboardLoadButton.addEventListener("click", () => openDeployModal());
  elements.healthRefresh.addEventListener("click", runHealthCheck);
  elements.operationSearch.addEventListener("input", (event) => {
    state.operationQuery = event.target.value;
    renderOperationsPage();
  });
  elements.deployForm.addEventListener("submit", submitDeployment);
  elements.deployClose.addEventListener("click", closeDeployModal);
  elements.deployCancel.addEventListener("click", closeDeployModal);
  elements.logClose.addEventListener("click", closeLogs);
  elements.confirmClose.addEventListener("click", () => closeConfirmation(false));
  elements.confirmCancel.addEventListener("click", () => closeConfirmation(false));
  elements.confirmAccept.addEventListener("click", () => closeConfirmation(true));
  elements.logCopy.addEventListener("click", () => copyWithToast(activeLogText, "Logs copied."));
  elements.copySupport.addEventListener("click", () => copyWithToast(elements.supportPreview.textContent, "Redacted support context copied."));
  elements.openUIDocs.addEventListener("click", () => window.open("https://github.com/lucamotz/dgx-spark-controller/blob/main/docs/UI.md", "_blank", "noopener"));
  for (const { backdrop, close } of MODALS) {
    backdrop.addEventListener("click", (event) => { if (event.target === backdrop) close(); });
  }
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") MODALS.find(({ backdrop }) => !backdrop.hidden)?.close();
    for (const { backdrop, dialog } of MODALS) if (!backdrop.hidden) trapModalFocus(dialog, event);
  });
}

function serviceContext() {
  return {
    apiFetch,
    toast,
    requestConfirmation,
    copy: (text) => copyWithToast(text, "Copied."),
    refresh: () => loadSnapshot({ quiet: true }),
    showPage,
    selectOperation,
    runMutation,
    openDeployModal,
    state: () => state,
  };
}

function boot() {
  bindEvents();
  const services = serviceContext();
  mountSetup(document.querySelector("#page-setup"), services);
  mountRecipes(document.querySelector("#page-recipes"), services);
  mountUpdates(document.querySelector("#page-updates"), services);
  const initialPage = window.location.hash.slice(1);
  showPage(PAGES.includes(initialPage) ? initialPage : "dashboard");
  renderAll();
  loadSnapshot();
  startPoller(refreshTelemetry, 10000);
  window.setInterval(updateTelemetry, 5000);
  startPoller(async () => {
    if (!document.hidden) await loadSnapshot({ quiet: true });
  }, 15000);
}

boot();
