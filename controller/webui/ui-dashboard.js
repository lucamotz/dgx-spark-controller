import {
  actionLabel,
  actionMark,
  countLabel,
  deploymentForNode,
  emptyState,
  escapeHTML,
  factPairs,
  formatPercent,
  formatRelativeTime,
  isNodeOnline,
  isTerminalOperation,
  newestFirst,
  normalizeDeployment,
  normalizeNode,
  stateBadge,
  statusLabel,
  statusTone,
} from "./ui-core.js";

export function renderSummary(root, state) {
  const nodes = state.nodes.map(normalizeNode);
  const deployments = state.deployments.map(normalizeDeployment);
  const activeOperations = state.operations.filter((operation) => !isTerminalOperation(operation));
  const readyNodes = nodes.filter(isNodeOnline).length;
  const readyDeployments = deployments.filter((deployment) => ["ready", "running"].includes(deployment.state)).length;
  const warningCount = state.warnings.length + nodes.reduce((count, node) => count + node.warnings.length, 0);
  const cards = [
    { label: "Nodes online", value: `${readyNodes}/${nodes.length}`, detail: nodes.length ? "from last observation" : "no nodes registered", tone: readyNodes < nodes.length ? "warning" : "success" },
    { label: "Deployments ready", value: String(readyDeployments), detail: readyDeployments ? "endpoint and alias verified" : "no verified deployment", tone: readyDeployments ? "success" : "neutral" },
    { label: "Active operations", value: String(activeOperations.length), detail: activeOperations.length ? "progress is being watched" : "no operations in progress", tone: activeOperations.length ? "warning" : "success" },
    { label: "Attention needed", value: String(warningCount), detail: warningCount ? "review Diagnostics" : "no warnings reported", tone: warningCount ? "danger" : "success" },
  ];
  root.innerHTML = cards.map((card) => `
    <article class="summary-card" data-tone="${card.tone}">
      <div class="summary-card-label">${escapeHTML(card.label)}</div>
      <div class="summary-card-value">${escapeHTML(card.value)}</div>
      <div class="summary-card-detail">${escapeHTML(card.detail)}</div>
    </article>`).join("");
}

function gibibytes(value) {
  return Number.isFinite(value) ? `${(value / 2 ** 30).toFixed(1)} GiB` : "—";
}

export function telemetryStatus(sample, { paused = false, failed = false, now = Date.now() } = {}) {
  if (paused) return { label: "Paused", state: "paused" };
  if (!sample) return { label: failed ? "Unavailable" : "Waiting for sample", state: "unavailable" };
  if (sample.status === "unavailable" || !sample.sampled_at) return { label: "Unavailable", state: "unavailable" };
  if (failed || now - Date.parse(sample.sampled_at) > 30000) return { label: "Stale", state: "stale" };
  return { label: sample.simulated ? "Demo" : sample.status === "partial" ? "Partial" : "Live", state: "live" };
}

// The monitoring label beside the node grid, summarizing every node's sample.
export function monitorStatus(nodes, samples, options) {
  const statuses = nodes.map((node) => telemetryStatus(samples.get(node.id), options));
  const unavailable = statuses.some((status) => status.state === "unavailable");
  const stale = statuses.some((status) => status.state === "stale");
  const values = [...samples.values()];
  const simulated = values.some((sample) => sample.simulated);
  const partial = values.some((sample) => sample.status === "partial");
  let label = "Live metrics";
  if (options.paused) label = "Monitoring paused";
  else if (stale) label = "Samples stale";
  else if (unavailable) label = "Waiting / unavailable";
  else if (!statuses.length) label = "No samples";
  else if (simulated) label = "Demo metrics";
  else if (partial) label = "Partial metrics";
  return { label, state: options.paused ? "paused" : stale || unavailable || partial ? "stale" : "live" };
}

function metric(label, value, detail, percent, tone = "success") {
  const available = Number.isFinite(percent);
  return `<div><span class="metric-label">${escapeHTML(label)}</span><strong class="metric-value">${escapeHTML(value)}</strong>
    ${available ? `<meter class="usage-meter" min="0" max="100" value="${Math.max(0, Math.min(100, percent))}" data-tone="${tone}" aria-label="${escapeHTML(label)}">${escapeHTML(formatPercent(percent))}</meter>` : '<div class="metric-unavailable" aria-hidden="true"></div>'}
    <span class="metric-detail">${escapeHTML(detail)}</span></div>`;
}

export function telemetryMarkup(sample) {
  const memory = sample?.memory;
  const percent = memory?.used_percent;
  const gpu = sample?.gpu?.utilization_percent;
  const memoryDetail = memory ? `${gibibytes(memory.used_bytes)} / ${gibibytes(memory.total_bytes)} · ${gibibytes(memory.available_bytes)} available · Swap ${gibibytes(memory.swap_used_bytes)}` : "No memory sample available";
  const memoryTone = percent >= 92 ? "danger" : percent >= 78 ? "warning" : "success";
  return metric("Unified memory", formatPercent(percent), memoryDetail, percent, memoryTone)
    + metric("GPU utilization", formatPercent(gpu), Number.isFinite(gpu) ? "Compute activity" : "Not reported by this node", gpu);
}

export function renderTelemetry(root, samples, options = {}) {
  root.querySelectorAll("[data-telemetry-node]").forEach((region) => {
    const sample = samples.get(region.dataset.telemetryNode);
    const status = telemetryStatus(sample, options);
    region.dataset.state = status.state;
    region.querySelector(".node-metrics").innerHTML = telemetryMarkup(sample);
    const stamp = region.querySelector(".telemetry-stamp");
    stamp.textContent = `${status.label}${sample?.sampled_at ? ` · sampled ${formatRelativeTime(sample.sampled_at)}` : ""}${sample?.message ? ` · ${sample.message}` : ""}`;
  });
}

export function renderNodes(root, state, onAction) {
  const nodes = state.nodes.map(normalizeNode);
  const signature = JSON.stringify([nodes, state.deployments]);
  if (root.nodeSignature === signature) return;
  root.nodeSignature = signature;
  const expanded = new Set([...root.querySelectorAll("details[open]")].map((details) => details.closest("[data-node-card]").dataset.nodeCard));
  const focus = root.contains(document.activeElement) ? document.activeElement : null;
  const focusNode = focus?.closest("[data-node-card]")?.dataset.nodeCard;
  const focusAction = focus?.dataset.nodeAction;
  root.closest("section")?.querySelector("#node-count")?.replaceChildren(document.createTextNode(countLabel(nodes.length, "node")));
  if (!nodes.length) {
    root.innerHTML = emptyState("No Sparks registered", "Add nodes to the controller configuration to see their capacity and workloads here.");
    return;
  }
  root.innerHTML = nodes.map((node) => {
    const deployment = deploymentForNode(node);
    const action = deployment ? "unload" : "load";
    const endpoint = node.endpoint && node.endpoint !== "—" ? node.endpoint : "Not observed";
    return `
      <article class="node-card" data-node-card="${escapeHTML(node.id)}" data-state="${escapeHTML(node.state)}">
        <div class="node-card-header">
          <div>
            <h3 class="node-name" title="${escapeHTML(node.name)}">${escapeHTML(node.name)}</h3>
            <p class="node-address">${escapeHTML(node.address)}</p>
          </div>
          ${stateBadge(node.state)}
        </div>
        <div class="node-model">
          <span class="node-model-label">Observed model</span>
          ${node.modelAlias || deployment?.alias ? `<strong class="node-model-name">${escapeHTML(node.modelAlias || deployment.alias)}</strong>` : '<span class="node-model-empty">No model observed</span>'}
          ${node.state === "unmanaged" ? '<span class="node-model-note">Not started by this controller; it will not be stopped or replaced.</span>' : ""}
        </div>
        <div class="node-telemetry" data-telemetry-node="${escapeHTML(node.id)}">
          <div class="node-metrics">${telemetryMarkup(null)}</div>
          <p class="telemetry-stamp">Waiting for sample</p>
        </div>
        <details class="node-details" ${expanded.has(node.id) ? "open" : ""}>
          <summary>Node details &amp; actions</summary>
          <dl class="facts-list">${factPairs([
            ["Endpoint", endpoint],
            ["Runtime", node.runtime],
            ["Hardware", node.gpu.name],
            ["Model observation", formatRelativeTime(node.lastRefresh)],
          ])}</dl>
          <div class="node-actions">
            <button class="button button-secondary" type="button" data-node-action="refresh" data-node-id="${escapeHTML(node.id)}">Refresh observation</button>
            <button class="button ${action === "unload" ? "button-danger" : "button-secondary"}" type="button" data-node-action="${action}" data-node-id="${escapeHTML(node.id)}" ${isNodeOnline(node) ? "" : "disabled"}>${action === "unload" ? "Unload model" : "Load model"}</button>
          </div>
        </details>
      </article>`;
  }).join("");
  root.querySelectorAll("[data-node-action]").forEach((button) => button.addEventListener("click", () => onAction(button.dataset.nodeAction, button.dataset.nodeId)));
  if (focusNode) {
    const card = [...root.querySelectorAll("[data-node-card]")].find((item) => item.dataset.nodeCard === focusNode);
    const target = focusAction ? [...(card?.querySelectorAll("[data-node-action]") || [])].find((item) => item.dataset.nodeAction === focusAction) : card?.querySelector("summary");
    target?.focus({ preventScroll: true });
  }
}

export function renderRecentOperations(root, state, onSelect) {
  const operations = newestFirst(state.operations).slice(0, 5);
  if (!operations.length) {
    root.innerHTML = emptyState("", "No operations recorded yet. A load, unload, or refresh will leave a durable trail here.");
    return;
  }
  root.innerHTML = operations.map((operation) => `
    <button class="recent-operation" type="button" data-operation-id="${escapeHTML(operation.id)}">
      ${actionMark(operation.action)}
      <span class="recent-operation-copy">
        <strong class="recent-operation-title">${escapeHTML(actionLabel(operation.action))}</strong>
        <span class="recent-operation-subtitle">${escapeHTML(operation.message)} · ${escapeHTML(formatRelativeTime(operation.requestedAt))}</span>
      </span>
      <span class="recent-operation-status" data-tone="${statusTone(operation.state)}">${escapeHTML(statusLabel(operation.state))}</span>
    </button>`).join("");
  root.querySelectorAll("[data-operation-id]").forEach((button) => button.addEventListener("click", () => onSelect(button.dataset.operationId)));
}

export function renderDashboard(state, elements, actions) {
  renderSummary(elements.summary, state);
  renderNodes(elements.nodes, state, actions.onNodeAction);
  renderRecentOperations(elements.recentOperations, state, actions.onOperationSelect);
}
