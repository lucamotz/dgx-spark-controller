import {
  countLabel,
  emptyState,
  escapeHTML,
  formatBytes,
  firstDefined,
  isNodeOnline,
  stateBadge,
  statusBadge,
  statusLabel,
} from "./ui-core.js";

// A profile may pin its placement to named nodes; null means any node.
function profilePlacement(profile) {
  return profile.node_ids.length ? profile.node_ids : null;
}

// "Qwen/Qwen3-8B @ b968826d on vLLM 0.31.0" for a recipe-generated profile.
export function profileProvenance(profile) {
  const { provenance } = profile;
  if (!provenance.hf_repo) return null;
  const revision = provenance.hf_revision ? ` @ ${provenance.hf_revision.slice(0, 8)}` : "";
  const runtime = firstDefined(provenance.vllm_version, provenance.vllm_release);
  return `${provenance.hf_repo}${revision}${runtime ? ` on vLLM ${runtime}` : ""}`;
}

export function profileDescription(profile) {
  return (profile.origin === "generated" && profileProvenance(profile)) || "An allowlisted launch recipe with an explicit readiness contract.";
}

// Several generated recipes can share one set of weights; what differs is the
// unified memory each one reserves (gpu_memory_utilization × smallest node).
export function profileMemoryFacts(profile) {
  const facts = [["Weights", formatBytes(profile.weightsBytes)]];
  if (profile.reservedMemoryBytes) {
    const share = profile.gpuMemoryUtilization ? ` · ${Math.round(profile.gpuMemoryUtilization * 100)}% of smallest node` : "";
    facts.push(["Reserved by recipe", `${formatBytes(profile.reservedMemoryBytes)}${share}`]);
  }
  return facts;
}

// Profiles that a ready deployment is currently serving.
export function loadedProfileIds(deployments) {
  return new Set(deployments.filter((deployment) => deployment.state === "ready").map((deployment) => deployment.profileId));
}

// Profiles that share weights share a model id, so the compatible nodes of the
// model found are narrowed to the profile's own placement.
function profileModel(profile, models) {
  const model = models.find((item) => item.id === profile.modelId);
  const placement = profilePlacement(profile);
  return placement ? { ...model, nodes: model.nodes.filter((item) => placement.includes(item.id)) } : model;
}

function nodePresence(model, node) {
  return model.nodes.find((item) => item.id === node.id)?.state || (model.nodes.length ? "missing" : "unknown");
}

function hasReadyNode(model, nodes) {
  return nodes.some((node) => isNodeOnline(node) && nodePresence(model, node) !== "missing");
}

export function renderModelCatalog(root, state, filter, onLoad) {
  const { profiles, models, nodes } = state;
  const loaded = loadedProfileIds(state.deployments);
  const cards = profiles.map((profile) => ({ profile, model: profileModel(profile, models) })).filter(({ model }) => {
    if (filter === "ready") return hasReadyNode(model, nodes);
    if (filter === "needs-attention") return !hasReadyNode(model, nodes);
    return true;
  });
  const count = document.querySelector("#model-count");
  if (count) count.textContent = countLabel(cards.length, "profile");
  if (!cards.length) {
    root.innerHTML = emptyState("No matching profiles", "Profiles appear here when the controller has an allowlisted launch recipe to show.");
    return;
  }
  root.innerHTML = cards.map(({ profile, model }) => {
    const placement = profilePlacement(profile);
    const placed = placement ? nodes.filter((node) => placement.includes(node.id)) : nodes;
    const presence = placed.map((node) => ({ node, state: nodePresence(model, node) }));
    const available = hasReadyNode(model, nodes);
    const profileId = profile.id;
    const facts = [
      ["Alias", profile.alias],
      ["Format", model.format],
      ["Quantization", model.quantization || "—"],
      ["Context", profile.context_length || "—"],
      ...profileMemoryFacts(profile),
      ["Mode", profile.mode],
    ];
    return `
      <article class="model-card">
        <div class="model-card-top">
          <div>
            <div class="model-type">${escapeHTML(profile.backend)} / ${escapeHTML(profile.mode)}</div>
            <h2>${escapeHTML(profile.name)}</h2>
          </div>
          <div class="model-card-badges">
            ${loaded.has(profileId) ? statusBadge("Loaded", "success") : ""}
            ${stateBadge(profile.origin)}
          </div>
        </div>
        <p class="model-card-description">${escapeHTML(profileDescription(profile))}</p>
        <dl class="model-facts">
          ${facts.map(([label, value]) => `<div class="model-fact"><dt>${escapeHTML(label)}</dt><dd title="${escapeHTML(value)}">${escapeHTML(value)}</dd></div>`).join("")}
        </dl>
        <div class="presence-row">
          <span class="presence-label">Node presence</span>
          <span class="presence-dots">
            ${presence.length ? presence.map(({ node, state }) => `<span class="presence-dot" data-state="${escapeHTML(state)}" title="${escapeHTML(node.name)}: ${escapeHTML(statusLabel(state))}">${escapeHTML(node.name)}</span>`).join("") : `<span class="presence-label">No nodes registered</span>`}
          </span>
        </div>
        <div class="model-card-footer">
          <button class="button button-primary" type="button" data-profile-load="${escapeHTML(profileId)}" ${available ? "" : "disabled"} title="${available ? "Open the deployment review" : "No compatible node is currently observed"}">Load profile</button>
        </div>
      </article>`;
  }).join("");
  root.querySelectorAll("[data-profile-load]").forEach((button) => button.addEventListener("click", () => onLoad(button.dataset.profileLoad)));
}

export function profileNodeCount(profile) {
  return profile ? profile.nodeCount : 1;
}

/* Online nodes the load dialog offers for a profile: its pinned placement,
 * else the nodes that hold its model. When neither yields an online node, every
 * online node is offered. */
export function compatibleTargetNodes(state, profileId) {
  const profile = state.profiles.find((item) => item.id === profileId);
  const online = state.nodes.filter(isNodeOnline);
  if (!profile) return online;
  const placement = profilePlacement(profile);
  if (placement) {
    const compatible = online.filter((node) => placement.includes(node.id));
    return compatible.length ? compatible : online;
  }
  const model = state.models.find((item) => item.id === profile.modelId);
  if (!model.nodes.length) return online;
  const compatibleIds = new Set(model.nodes.map((item) => item.id));
  const compatible = online.filter((node) => compatibleIds.has(node.id));
  return compatible.length ? compatible : online;
}

export function conflictWarning(conflicts) {
  const names = conflicts.map(({ node }) => node.name || node.id);
  const outside = conflicts.filter((entry) => entry.outsideSelection).map(({ node }) => node.name || node.id);
  const extra = outside.length
    ? ` ${outside.join(", ")} ${outside.length === 1 ? "is" : "are"} not selected but ${outside.length === 1 ? "runs" : "run"} part of the same deployment, which cannot keep running on only some of its nodes.`
    : "";
  return `This will stop the active model on ${names.join(", ")}.${extra} I have reviewed the affected placement and want to continue.`;
}

export function deployFormMarkup({ profiles, profileId, profile, requiredCount, targets, targetNodeIds, conflicts, conflictConfirmed }) {
  const profileOptions = profiles.length
    ? profiles.map((item) => `<option value="${escapeHTML(item.id)}" ${item.id === profileId ? "selected" : ""}>${escapeHTML(item.name)} · ${escapeHTML(item.mode)}</option>`).join("")
    : "<option>No profiles available</option>";
  const targetOptions = targets.length
    ? targets.map((node) => `<label class="target-option"><input type="checkbox" name="target-node" value="${escapeHTML(node.id)}" ${targetNodeIds.includes(node.id) ? "checked" : ""} /><span class="target-copy"><span class="target-name">${escapeHTML(node.name)}</span><span class="target-detail">${escapeHTML(node.address)} · ${escapeHTML(statusLabel(node.state))}</span></span></label>`).join("")
    : emptyState("", "No compatible online nodes are currently observed.");
  return `
    <div class="field-group">
      <label class="field-label" for="deploy-profile-select">Model profile</label>
      <select class="select-input" id="deploy-profile-select" ${profiles.length ? "" : "disabled"}>
        ${profileOptions}
      </select>
      <p class="field-help">Only reviewed or recipe-generated profiles appear here. The browser cannot edit scripts, paths, flags, or SSH commands.</p>
    </div>
    <div class="field-group">
      <span class="field-label">Target placement</span>
      <p class="field-help">Select exactly ${requiredCount} compatible ${requiredCount === 1 ? "node" : "nodes"} for this profile.</p>
      <div class="target-list">
        ${targetOptions}
      </div>
    </div>
    ${profile ? `<div class="impact-card"><h3>What will happen</h3><p>Preflight <strong>${escapeHTML(profile.name)}</strong>, verify files and runtime, stop conflicting managed deployments on every node they use after all gates pass, then wait for the expected endpoint alias before marking ready.</p></div>` : ""}
    ${conflicts.length ? `<label class="confirm-row"><input type="checkbox" id="confirm-conflict" ${conflictConfirmed ? "checked" : ""} /><span>${escapeHTML(conflictWarning(conflicts))}</span></label>` : ""}`;
}
