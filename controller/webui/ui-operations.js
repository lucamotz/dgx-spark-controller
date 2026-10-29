import {
  ACTIONS,
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
  stateBadge,
  statusLabel,
  statusTone,
} from "./ui-core.js";

export function operationText(operation) {
  if (operation.action === "unload") return `Deployment ${operation.deploymentId}`;
  if (operation.action === "load") return `Profile ${operation.profileId}`;
  return operation.nodeIds.length ? countLabel(operation.nodeIds.length, "node") : "Controller observations";
}

// The listed phases a run reached, in list order. The event trail keeps only the
// newest events, so phases before the earliest one it shows count as reached.
function reachedPhases(operation, phases) {
  const seen = new Set([operation.phase, ...operation.events.map((event) => event.phase)]);
  const first = phases.findIndex((phase) => seen.has(phase));
  return first < 0 ? [] : phases.filter((phase, index) => index < first || seen.has(phase));
}

function operationLogs(operation) {
  return operation.events.map((event) => `${formatDate(event.timestamp)} ${event.message}`);
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
        <strong class="operation-row-title">${escapeHTML(ACTIONS[operation.action].label)}</strong>
        <span class="operation-row-subtitle">${escapeHTML(operationText(operation))} · ${escapeHTML(formatRelativeTime(operation.requestedAt))}</span>
      </span>
      ${stateBadge(operation.state)}
    </button>`).join("");
  root.querySelectorAll("[data-operation-id]").forEach((button) => button.addEventListener("click", () => onSelect(button.dataset.operationId)));
}

function renderPhases(operation) {
  const { phases } = ACTIONS[operation.action];
  const reached = reachedPhases(operation, phases);
  // Unless the run succeeded, the furthest phase reached is the one it is in or stopped in.
  const last = operation.state === "succeeded" ? null : reached.at(-1);
  return `<ol class="phase-list">
    ${phases.map((phase) => {
      const done = reached.includes(phase) && phase !== last;
      const active = !isTerminalOperation(operation) && phase === operation.phase;
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
  const progress = escapeHTML(operation.progress);
  const tone = statusTone(operation.state);
  const logs = operationLogs(operation);
  const error = operation.error;
  root.innerHTML = `
    <div class="detail-header">
      <div>
        <p class="eyebrow">${escapeHTML(ACTIONS[operation.action].label)}</p>
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
    ${error ? `<div class="detail-error"><strong>${escapeHTML(error.code)}</strong>${escapeHTML(error.message)}<br /><span>${escapeHTML(error.remediation)}</span></div>` : ""}
    <div class="detail-section-heading"><h3>Run details</h3><span class="section-meta">${escapeHTML(formatDuration(operation.startedAt || operation.requestedAt, isTerminalOperation(operation) ? operation.updated_at : null))}</span></div>
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
