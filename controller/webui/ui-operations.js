import {
  actionLabel,
  actionMark,
  countLabel,
  emptyState,
  escapeHTML,
  factPairs,
  formatDate,
  formatDuration,
  formatRelativeTime,
  isTerminalOperation,
  newestFirst,
  operationProgress,
  stateBadge,
  statusLabel,
  statusTone,
} from "./ui-core.js";

export function operationText(operation) {
  // "unload" contains "load", so it must be checked first.
  if (operation.action.includes("unload") || operation.action.includes("stop")) return operation.deploymentId ? `Deployment ${operation.deploymentId}` : "Managed deployment";
  if (operation.action.includes("load") || operation.action.includes("start") || operation.action.includes("switch")) return operation.profileId ? `Profile ${operation.profileId}` : "Managed deployment";
  return operation.nodeIds.length ? countLabel(operation.nodeIds.length, "node") : "Controller observations";
}

function phaseNames(action) {
  const value = String(action || "").toLowerCase();
  if (value.includes("recover")) return ["locking", "identity-check", "stopping", "verifying-baseline", "recovered"];
  if (value.includes("unload") || value.includes("stop")) return ["preflighting", "draining", "stopping", "verifying-release", "stopped"];
  if (value.includes("reconcile") || value.includes("refresh")) return ["refreshing", "observing", "recording", "complete"];
  return ["preflighting", "stopping-conflicts", "starting-workers", "loading", "ready"];
}

function phaseIndex(operation, phases) {
  const phase = String(operation.phase || "").toLowerCase().replaceAll("_", "-");
  const exact = phases.findIndex((item) => phase.includes(item) || item.includes(phase));
  if (exact >= 0) return exact;
  const progress = operationProgress(operation);
  return Math.min(phases.length - 1, Math.floor((progress / 100) * phases.length));
}

function eventTime(event) {
  return event?.at || event?.timestamp || event?.created_at || event?.createdAt;
}

function eventMessage(event) {
  return typeof event === "string" ? event : event?.message || event?.detail || event?.code || "Controller event";
}

function operationLogs(operation) {
  const lines = operation.logs.map((line) => typeof line === "string" ? line : `${line?.timestamp || line?.at || ""} ${line?.message || line?.detail || JSON.stringify(line)}`.trim());
  if (lines.length) return lines;
  return operation.events.map((event) => {
    const stamp = eventTime(event);
    return `${stamp ? `${formatDate(stamp)} ` : ""}${eventMessage(event)}`;
  });
}

export function getOperationLogs(operation) {
  return operationLogs(operation).join("\n");
}

function renderList(root, operations, selectedId, onSelect) {
  if (!operations.length) {
    root.innerHTML = emptyState("No matching operations", "Load, unload, recover, refresh, and reconcile actions will be recorded here.");
    return;
  }
  root.innerHTML = operations.map((operation) => `
    <button class="operation-row ${operation.id === selectedId ? "is-selected" : ""}" type="button" data-operation-id="${escapeHTML(operation.id)}">
      ${actionMark(operation.action)}
      <span class="operation-row-copy">
        <strong class="operation-row-title">${escapeHTML(actionLabel(operation.action))}</strong>
        <span class="operation-row-subtitle">${escapeHTML(operationText(operation))} · ${escapeHTML(formatRelativeTime(operation.requestedAt))}</span>
      </span>
      ${stateBadge(operation.state)}
    </button>`).join("");
  root.querySelectorAll("[data-operation-id]").forEach((button) => button.addEventListener("click", () => onSelect(button.dataset.operationId)));
}

function renderPhases(operation) {
  const phases = phaseNames(operation.action);
  const current = phaseIndex(operation, phases);
  return `<ol class="phase-list">
    ${phases.map((phase, index) => {
      const done = isTerminalOperation(operation) && operation.state === "succeeded" ? index <= current : index < current;
      const active = !isTerminalOperation(operation) && index === current;
      return `<li class="phase-item ${done ? "is-done" : ""} ${active ? "is-current" : ""}">
        <span class="phase-icon" aria-hidden="true">${done ? "✓" : active ? "•" : ""}</span>
        <span>${escapeHTML(statusLabel(phase))}</span>
        ${active ? `<span class="phase-time">now</span>` : done ? `<span class="phase-time">done</span>` : ""}
      </li>`;
    }).join("")}
  </ol>`;
}

function renderDetail(root, operation, actions) {
  if (!operation) {
    root.innerHTML = `<div class="empty-detail"><span class="empty-detail-mark" aria-hidden="true">⌁</span><h2>Select an operation</h2><p>Progress, phase notes, and redacted log tails will appear here.</p></div>`;
    return;
  }
  const progress = operationProgress(operation);
  const tone = statusTone(operation.state);
  const logs = operationLogs(operation);
  const error = operation.error;
  root.innerHTML = `
    <div class="detail-header">
      <div>
        <p class="eyebrow">${escapeHTML(actionLabel(operation.action))}</p>
        <h2>${escapeHTML(operationText(operation))}</h2>
        <p class="detail-id">${escapeHTML(operation.id)}</p>
      </div>
      ${stateBadge(operation.state)}
    </div>
    <div class="detail-actions">
      ${!isTerminalOperation(operation) ? `<button class="button button-secondary" type="button" data-operation-cancel="${escapeHTML(operation.id)}">Cancel if safe</button>` : ""}
      <button class="button button-secondary" type="button" data-operation-logs="${escapeHTML(operation.id)}">Open logs</button>
    </div>
    <div class="progress-summary"><p class="progress-label">${escapeHTML(operation.message)}</p><strong class="progress-percent">${progress}%</strong></div>
    <progress class="progress-track" data-tone="${tone}" max="100" value="${progress}" aria-label="Operation ${progress}% complete">${progress}%</progress>
    ${renderPhases(operation)}
    ${error ? `<div class="detail-error"><strong>${escapeHTML(error.code || "Operation stopped")}</strong>${escapeHTML(error.message || error.detail || String(error))}${error.remediation ? `<br /><span>${escapeHTML(error.remediation)}</span>` : ""}</div>` : ""}
    <div class="detail-section-heading"><h3>Run details</h3><span class="section-meta">${escapeHTML(formatDuration(operation.startedAt || operation.requestedAt, operation.finishedAt))}</span></div>
    <dl class="facts-list">${factPairs([
      ["Requested", formatDate(operation.requestedAt)],
      ["Phase", statusLabel(operation.phase)],
      ["Profile", operation.profileId || "—"],
      ["Nodes", operation.nodeIds.join(", ") || "—"],
    ])}</dl>
    <div class="detail-section-heading"><h3>Latest redacted events</h3><button class="text-button" type="button" data-operation-logs="${escapeHTML(operation.id)}">View full tail</button></div>
    <pre class="log-tail">${escapeHTML(logs.slice(-8).join("\n") || "No events reported yet.")}</pre>`;
  root.querySelectorAll("[data-operation-logs]").forEach((button) => button.addEventListener("click", () => actions.onLogs(operation)));
  root.querySelectorAll("[data-operation-cancel]").forEach((button) => button.addEventListener("click", () => actions.onCancel(operation)));
}

function matchesQuery(operation, query) {
  const fields = [operation.id, operation.action, operation.state, operation.phase, operation.profileId, operation.message];
  return !query || fields.some((value) => String(value || "").toLowerCase().includes(query));
}

export function renderOperations(elements, state, options) {
  const query = String(options.query || "").trim().toLowerCase();
  const operations = newestFirst(state.operations.filter((operation) => matchesQuery(operation, query)));
  elements.count.textContent = countLabel(operations.length, "run");
  renderList(elements.list, operations, state.selectedOperationId, options.onSelect);
  const selected = state.operations.find((operation) => operation.id === state.selectedOperationId) || null;
  renderDetail(elements.detail, selected, options);
}
