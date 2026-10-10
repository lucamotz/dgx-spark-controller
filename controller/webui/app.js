import {
  ApiError,
  apiFetch,
  asArray,
  collection,
  copyText,
  createIdempotencyKey,
  deploymentForNode,
  escapeHTML,
  firstDefined,
  formatDate,
  formatRelativeTime,
  isTerminalOperation,
  normalizeDeployment,
  normalizeNode,
  normalizeOperation,
  normalizeProfile,
  startPoller,
  statusLabel,
  statusTone,
  switchStopEntries,
  trapModalFocus,
} from "./ui-core.js";
import { renderDashboard, renderTelemetry, telemetryStatus } from "./ui-dashboard.js";
import { renderModelCatalog } from "./ui-models.js";
import { getOperationLogs, renderOperations } from "./ui-operations.js";
import { mountRecipes, refreshRecipes } from "./ui-recipes.js";
import { mountSetup, refreshSetup } from "./ui-setup.js";

const PAGES = ["dashboard", "models", "recipes", "operations", "diagnostics", "setup"];

const state = {
  nodes: [],
  models: [],
  profiles: [],
  deployments: [],
  operations: [],
  warnings: [],
  health: {},
  setup: null,
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
  modelCount: document.querySelector("#model-count"),
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
  confirmAccept: document.querySelector("#confirm-modal-accept"),
  confirmCancel: document.querySelector("#confirm-modal-cancel"),
  confirmClose: document.querySelector("#confirm-modal-close"),
};

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

function requestConfirmation({ title, message, confirmLabel = "Continue" }) {
  if (pendingConfirmation) pendingConfirmation(false);
  modalReturnFocus = document.activeElement;
  elements.confirmTitle.textContent = title;
  elements.confirmMessage.textContent = message;
  elements.confirmAccept.textContent = confirmLabel;
  elements.confirmBackdrop.hidden = false;
  window.setTimeout(() => elements.confirmModal.focus(), 0);
  return new Promise((resolve) => { pendingConfirmation = resolve; });
}

function closeConfirmation(confirmed = false) {
  if (elements.confirmBackdrop.hidden) return;
  elements.confirmBackdrop.hidden = true;
  const resolve = pendingConfirmation;
  pendingConfirmation = null;
  if (modalReturnFocus?.focus) modalReturnFocus.focus();
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
  state.nodes = collection(payload, "nodes").map(normalizeNode);
  state.models = collection(payload, "models").map((model) => ({ ...model }));
  state.profiles = collection(payload, "profiles").map(normalizeProfile);
  state.deployments = collection(payload, "deployments").map(normalizeDeployment);
  state.operations = collection(payload, "operations").map(normalizeOperation);
  state.warnings = collection(payload, "warnings");
  state.health = payload?.health || payload?.data?.health || resourcePayloads.health || {};
  state.setup = payload?.setup || null;
  if (resourcePayloads.nodes) state.nodes = resourceItems(resourcePayloads.nodes, "nodes").map(normalizeNode);
  if (resourcePayloads.models) state.models = resourceItems(resourcePayloads.models, "models").map((model) => ({ ...model }));
  if (resourcePayloads.profiles) state.profiles = resourceItems(resourcePayloads.profiles, "profiles").map(normalizeProfile);
  if (resourcePayloads.deployments) state.deployments = resourceItems(resourcePayloads.deployments, "deployments").map(normalizeDeployment);
  if (resourcePayloads.operations) state.operations = resourceItems(resourcePayloads.operations, "operations").map(normalizeOperation);
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
    const resources = await Promise.allSettled([
      apiFetch("/nodes"),
      apiFetch("/models"),
      apiFetch("/profiles"),
      apiFetch("/deployments"),
      apiFetch("/operations?limit=50"),
    ]);
    const [nodes, models, profiles, deployments, operations] = resources;
    const succeeded = resources.some((result) => result.status === "fulfilled");
    if (!succeeded) {
      state.syncing = false;
      setApiStatus("error", "Controller offline");
      elements.refreshButton.classList.remove("is-spinning");
      setNotice(bootstrapError?.message || "The controller did not return a usable snapshot.");
      renderAll();
      return;
    }
    applySnapshot({}, {
      nodes: nodes.status === "fulfilled" ? nodes.value : null,
      models: models.status === "fulfilled" ? models.value : null,
      profiles: profiles.status === "fulfilled" ? profiles.value : null,
      deployments: deployments.status === "fulfilled" ? deployments.value : null,
      operations: operations.status === "fulfilled" ? operations.value : null,
    });
    setNotice("Bootstrap is unavailable; showing the resource responses that did arrive.");
  }

  state.syncing = false;
  setApiStatus("ready", "Controller ready");
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
  renderModelCatalog(elements.models, state, state.modelFilter, openDeployModal);
  renderOperations({ list: elements.operations, detail: elements.operationDetail, count: elements.operationCount }, state, {
    query: state.operationQuery,
    onSelect: selectOperation,
    onLogs: openLogs,
    onCancel: cancelOperation,
  });
  renderDiagnostics();
  updateTelemetry();
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
  updateTelemetry();
  refreshTelemetry();
}

function monitoringActive() {
  return state.page === "dashboard" && !document.hidden && !state.monitoringPaused;
}

function updateTelemetry() {
  const paused = !monitoringActive();
  const options = { paused, failed: state.telemetryFailed };
  renderTelemetry(elements.nodes, state.telemetry, options);
  const statuses = state.nodes.map((node) => telemetryStatus(state.telemetry.get(node.id), options));
  const unavailable = statuses.some((status) => status.state === "unavailable");
  const stale = statuses.some((status) => status.state === "stale");
  const samples = [...state.telemetry.values()];
  const simulated = samples.some((sample) => sample.simulated);
  const partial = samples.some((sample) => sample.status === "partial");
  let label = "Live metrics";
  if (paused) label = "Monitoring paused";
  else if (stale) label = "Samples stale";
  else if (unavailable) label = "Waiting / unavailable";
  else if (!statuses.length) label = "No samples";
  else if (simulated) label = "Demo metrics";
  else if (partial) label = "Partial metrics";
  elements.monitorStatus.textContent = label;
  elements.monitorStatus.dataset.state = paused ? "paused" : stale || unavailable || partial ? "stale" : "live";
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
  const facts = [
    ["API status", firstDefined(health.status, health.state, state.lastSync ? "reachable" : "not checked")],
    ["Controller version", firstDefined(health.version, health.controller_version, "not reported")],
    ["Schema", firstDefined(health.schema_version, health.schemaVersion, "not reported")],
    ["Last sync", state.lastSync ? formatDate(state.lastSync) : "not recorded"],
  ];
  elements.healthFacts.innerHTML = facts.map(([label, value]) => `<div class="fact-pair"><dt>${escapeHTML(label)}</dt><dd>${escapeHTML(value)}</dd></div>`).join("");
  const warnings = [...state.warnings];
  state.nodes.forEach((node) => node.warnings.forEach((warning) => warnings.push(typeof warning === "string" ? `${node.name}: ${warning}` : { ...warning, message: `${node.name}: ${warning.message || warning.detail || "warning"}` })));
  elements.warningCount.textContent = `${warnings.length} ${warnings.length === 1 ? "item" : "items"}`;
  elements.warningList.innerHTML = warnings.length ? warnings.map((warning) => {
    const text = typeof warning === "string" ? warning : warning.message || warning.detail || warning.code || JSON.stringify(warning);
    const tone = typeof warning === "object" && (warning.severity === "error" || warning.severity === "danger") ? "danger" : "warning";
    return `<div class="warning-item" data-tone="${tone}">${escapeHTML(text)}</div>`;
  }).join("") : `<div class="warning-item" data-tone="success">No warnings reported by the latest observation.</div>`;
  elements.supportPreview.textContent = JSON.stringify(buildSupportContext(), null, 2);
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
    const message = error instanceof ApiError && error.remediation ? `${error.message} ${error.remediation}` : error.message || `${label} failed.`;
    setNotice(message);
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
    const deployment = deploymentForNode(node, state.deployments);
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
  const id = deployment.id;
  await runMutation("Unload", () => apiFetch(`/deployments/${encodeURIComponent(id)}`, {
    method: "DELETE",
    headers: { "Idempotency-Key": createIdempotencyKey("unload") },
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
    headers: { "Idempotency-Key": createIdempotencyKey("recover") },
  }));
}

function compatibleTargetNodes(profileId, preferredNodeId = null) {
  const profile = state.profiles.find((item) => item.id === profileId);
  const selected = state.nodes.filter((node) => node.state !== "offline" && node.state !== "unknown");
  if (!profile) return selected;
  if (Array.isArray(profile.node_ids) && profile.node_ids.length) {
    const allowed = new Set(profile.node_ids.map(String));
    const compatible = selected.filter((node) => allowed.has(node.id));
    return compatible.length ? compatible : selected;
  }
  const model = state.models.find((item) => item.id === profile.modelId || item.id === profileId);
  if (!model || !model.nodes?.length) return selected;
  const compatibleIds = new Set(model.nodes.map((item) => String(item?.id || item?.node_id || item)));
  const compatible = selected.filter((node) => compatibleIds.has(node.id));
  return compatible.length ? compatible : selected;
}

function conflictsForTargets(targetIds) {
  return switchStopEntries(targetIds, state.nodes, state.deployments);
}

function conflictWarning(conflicts) {
  const names = conflicts.map(({ node }) => node.name || node.id);
  const outside = conflicts.filter((entry) => entry.outsideSelection).map(({ node }) => node.name || node.id);
  const extra = outside.length
    ? ` ${outside.join(", ")} ${outside.length === 1 ? "is" : "are"} not selected but ${outside.length === 1 ? "runs" : "run"} part of the same deployment, which cannot keep running on only some of its nodes.`
    : "";
  return `This will stop the active model on ${names.join(", ")}.${extra} I have reviewed the affected placement and want to continue.`;
}

function openDeployModal(profileId = null, preferredNodeId = null) {
  const fallbackProfile = profileId || state.profiles[0]?.id || null;
  const targets = compatibleTargetNodes(fallbackProfile, preferredNodeId);
  const profile = state.profiles.find((item) => item.id === fallbackProfile);
  const requiredCount = Number(profile?.nodeCount || profile?.node_count || 1);
  const preferred = preferredNodeId && targets.some((node) => node.id === preferredNodeId)
    ? [targets.find((node) => node.id === preferredNodeId), ...targets.filter((node) => node.id !== preferredNodeId)]
    : targets;
  deployContext = {
    profileId: fallbackProfile,
    targetNodeIds: preferred.slice(0, requiredCount).map((node) => node.id),
    conflictConfirmed: false,
  };
  modalReturnFocus = document.activeElement;
  renderDeployForm();
  elements.deployBackdrop.hidden = false;
  window.setTimeout(() => elements.deployModal.focus(), 0);
}

function renderDeployForm() {
  const profiles = state.profiles;
  const selectedProfile = profiles.find((profile) => profile.id === deployContext.profileId) || null;
  const requiredCount = Number(selectedProfile?.nodeCount || selectedProfile?.node_count || 1);
  const targets = compatibleTargetNodes(deployContext.profileId);
  deployContext.targetNodeIds = deployContext.targetNodeIds.filter((id) => targets.some((node) => node.id === id));
  if (deployContext.targetNodeIds.length !== requiredCount && targets.length >= requiredCount) {
    deployContext.targetNodeIds = targets.slice(0, requiredCount).map((node) => node.id);
  }
  const conflicts = conflictsForTargets(deployContext.targetNodeIds);
  elements.deployFormContent.innerHTML = `
    <div class="field-group">
      <label class="field-label" for="deploy-profile-select">Model profile</label>
      <select class="select-input" id="deploy-profile-select" ${profiles.length ? "" : "disabled"}>
        ${profiles.length ? profiles.map((profile) => `<option value="${escapeHTML(profile.id)}" ${profile.id === deployContext.profileId ? "selected" : ""}>${escapeHTML(profile.name)} · ${escapeHTML(profile.mode)}</option>`).join("") : `<option>No profiles available</option>`}
      </select>
      <p class="field-help">Only reviewed or recipe-generated profiles appear here. The browser cannot edit scripts, paths, flags, or SSH commands.</p>
    </div>
    <div class="field-group">
      <span class="field-label">Target placement</span>
      <p class="field-help">Select exactly ${requiredCount} compatible ${requiredCount === 1 ? "node" : "nodes"} for this profile.</p>
      <div class="target-list">
        ${targets.length ? targets.map((node) => `<label class="target-option"><input type="checkbox" name="target-node" value="${escapeHTML(node.id)}" ${deployContext.targetNodeIds.includes(node.id) ? "checked" : ""} /><span class="target-copy"><span class="target-name">${escapeHTML(node.name)}</span><span class="target-detail">${escapeHTML(node.address)} · ${escapeHTML(statusLabel(node.state))}</span></span></label>`).join("") : `<div class="empty-state"><p>No compatible online nodes are currently observed.</p></div>`}
      </div>
    </div>
    ${selectedProfile ? `<div class="impact-card"><h3>What will happen</h3><p>Preflight <strong>${escapeHTML(selectedProfile.name)}</strong>, verify files and runtime, stop conflicting managed deployments on every node they use after all gates pass, then wait for the expected endpoint alias before marking ready.</p></div>` : ""}
    ${conflicts.length ? `<label class="confirm-row"><input type="checkbox" id="confirm-conflict" ${deployContext.conflictConfirmed ? "checked" : ""} /><span>${escapeHTML(conflictWarning(conflicts))}</span></label>` : ""}`;
  elements.deploySubmit.disabled = !selectedProfile || deployContext.targetNodeIds.length !== requiredCount;
  elements.deploySubmit.textContent = conflicts.length ? "Confirm & load" : "Load profile";
  elements.deploySubmit.classList.toggle("button-danger", conflicts.length);
  elements.deploySubmit.classList.toggle("button-primary", !conflicts.length);
  elements.deployFormContent.querySelector("#deploy-profile-select")?.addEventListener("change", (event) => {
    deployContext.profileId = event.target.value;
    const nextProfile = profiles.find((profile) => profile.id === deployContext.profileId);
    const nextCount = Number(nextProfile?.nodeCount || nextProfile?.node_count || 1);
    deployContext.targetNodeIds = compatibleTargetNodes(deployContext.profileId).slice(0, nextCount).map((node) => node.id);
    deployContext.conflictConfirmed = false;
    renderDeployForm();
  });
  elements.deployFormContent.querySelectorAll("input[name=target-node]").forEach((input) => input.addEventListener("change", () => {
    deployContext.targetNodeIds = [...elements.deployFormContent.querySelectorAll("input[name=target-node]:checked")].map((item) => item.value);
    deployContext.conflictConfirmed = false;
    renderDeployForm();
  }));
  elements.deployFormContent.querySelector("#confirm-conflict")?.addEventListener("change", (event) => { deployContext.conflictConfirmed = event.target.checked; });
}

function closeDeployModal() {
  elements.deployBackdrop.hidden = true;
  if (modalReturnFocus?.focus) modalReturnFocus.focus();
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
    headers: { "Idempotency-Key": createIdempotencyKey("load") },
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
    headers: { "Idempotency-Key": createIdempotencyKey("cancel") },
  }));
}

async function openLogs(operation) {
  elements.logBackdrop.hidden = false;
  elements.logMeta.textContent = `${operation.id} · loading redacted tail`;
  elements.logView.textContent = "Loading logs…";
  modalReturnFocus = document.activeElement;
  window.setTimeout(() => elements.logModal.focus(), 0);
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
  elements.logBackdrop.hidden = true;
  if (modalReturnFocus?.focus) modalReturnFocus.focus();
}

function bindEvents() {
  document.querySelectorAll("[data-page-link]").forEach((link) => link.addEventListener("click", (event) => {
    event.preventDefault();
    showPage(link.dataset.pageLink);
  }));
  document.querySelectorAll("[data-model-filter]").forEach((button) => button.addEventListener("click", () => {
    state.modelFilter = button.dataset.modelFilter;
    document.querySelectorAll("[data-model-filter]").forEach((item) => item.classList.toggle("is-selected", item === button));
    renderModelCatalog(elements.models, state, state.modelFilter, openDeployModal);
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
  elements.healthRefresh.addEventListener("click", async () => {
    elements.healthRefresh.disabled = true;
    try {
      state.health = await apiFetch("/api/health");
      setApiStatus("ready", "Controller ready");
      renderDiagnostics();
      toast("Health check complete.", "success");
    } catch (error) {
      state.health = { status: "error", message: error.message };
      setApiStatus("error", "Controller offline");
      renderDiagnostics();
      toast(error.message, "danger");
    } finally { elements.healthRefresh.disabled = false; }
  });
  elements.operationSearch.addEventListener("input", (event) => { state.operationQuery = event.target.value; renderOperations({ list: elements.operations, detail: elements.operationDetail, count: elements.operationCount }, state, { query: state.operationQuery, onSelect: selectOperation, onLogs: openLogs, onCancel: cancelOperation }); });
  elements.deployForm.addEventListener("submit", submitDeployment);
  elements.deployClose.addEventListener("click", closeDeployModal);
  elements.deployCancel.addEventListener("click", closeDeployModal);
  elements.logClose.addEventListener("click", closeLogs);
  elements.confirmClose.addEventListener("click", () => closeConfirmation(false));
  elements.confirmCancel.addEventListener("click", () => closeConfirmation(false));
  elements.confirmAccept.addEventListener("click", () => closeConfirmation(true));
  elements.logCopy.addEventListener("click", async () => { await copyText(activeLogText); toast("Logs copied.", "success"); });
  elements.copySupport.addEventListener("click", async () => { await copyText(elements.supportPreview.textContent); toast("Redacted support context copied.", "success"); });
  elements.openUIDocs.addEventListener("click", () => window.open("https://github.com/lucamotz/dgx-spark-controller/blob/main/docs/UI.md", "_blank", "noopener"));
  [elements.deployBackdrop, elements.logBackdrop, elements.confirmBackdrop].forEach((backdrop) => backdrop.addEventListener("click", (event) => {
    if (event.target !== backdrop) return;
    if (backdrop === elements.deployBackdrop) closeDeployModal();
    else if (backdrop === elements.logBackdrop) closeLogs();
    else closeConfirmation(false);
  }));
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      if (!elements.deployBackdrop.hidden) closeDeployModal();
      else if (!elements.logBackdrop.hidden) closeLogs();
      else if (!elements.confirmBackdrop.hidden) closeConfirmation(false);
    }
    if (!elements.deployBackdrop.hidden) trapModalFocus(elements.deployModal, event);
    if (!elements.logBackdrop.hidden) trapModalFocus(elements.logModal, event);
    if (!elements.confirmBackdrop.hidden) trapModalFocus(elements.confirmModal, event);
  });
}

function serviceContext() {
  return {
    apiFetch,
    toast,
    requestConfirmation,
    copy: async (text) => { await copyText(text); toast("Copied.", "success"); },
    refresh: () => loadSnapshot({ quiet: true }),
    showPage,
    openDeployModal,
    state: () => state,
  };
}

function boot() {
  bindEvents();
  mountSetup(document.querySelector("#page-setup"), serviceContext());
  mountRecipes(document.querySelector("#page-recipes"), serviceContext());
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
