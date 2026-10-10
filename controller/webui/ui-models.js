import {
  escapeHTML,
  formatBytes,
  firstDefined,
  normalizeDeployment,
  normalizeModel,
  normalizeProfile,
  statusLabel,
  statusTone,
} from "./ui-core.js";

// "Qwen/Qwen3-8B @ b968826d on vLLM 0.31.0" for a recipe-generated profile.
export function profileProvenance(profile) {
  const provenance = profile?.provenance || {};
  if (!provenance.hf_repo) return null;
  const revision = provenance.hf_revision ? ` @ ${String(provenance.hf_revision).slice(0, 8)}` : "";
  const runtime = firstDefined(provenance.vllm_version, provenance.vllm_release);
  return `${provenance.hf_repo}${revision}${runtime ? ` on vLLM ${runtime}` : ""}`;
}

export function profileDescription(profile) {
  if (profile?.origin === "generated") return profileProvenance(profile) || profile.description;
  return profile?.description;
}

// Several generated recipes can share one set of weights; what differs is the
// unified memory each one reserves (gpu_memory_utilization × smallest node).
export function profileMemoryFacts(profile, model = {}) {
  const facts = [["Weights", formatBytes(firstDefined(profile.weightsBytes, model.sizeBytes))]];
  if (profile.reservedMemoryBytes) {
    const share = profile.gpuMemoryUtilization ? ` · ${Math.round(profile.gpuMemoryUtilization * 100)}% of smallest node` : "";
    facts.push(["Reserved by recipe", `${formatBytes(profile.reservedMemoryBytes)}${share}`]);
  }
  return facts;
}

// Profiles that a ready deployment is currently serving.
export function loadedProfileIds(deployments) {
  return new Set((deployments || []).map(normalizeDeployment).filter((deployment) => deployment.state === "ready" && deployment.profileId).map((deployment) => String(deployment.profileId)));
}

function profileModel(profile, models) {
  const modelId = String(profile.modelId || "");
  const model = models.map(normalizeModel).find((item) => item.id === modelId || item.id === profile.id) || normalizeModel(profile.model || profile);
  // Several recipes can share one set of weights; context and placement are
  // profile settings.
  const placement = Array.isArray(profile.node_ids) && profile.node_ids.length ? new Set(profile.node_ids.map(String)) : null;
  return {
    ...model,
    context: firstDefined(profile.context_length, model.context),
    nodes: placement ? model.nodes.filter((item) => placement.has(String(item?.id || item?.node_id || item))) : model.nodes,
  };
}

function nodePresence(model, node) {
  const inventory = model.inventory;
  const direct = inventory && typeof inventory === "object" && !Array.isArray(inventory) ? inventory[node.id] : null;
  const entry = model.nodes.find((item) => String(item?.id || item?.node_id || item) === node.id);
  const state = typeof direct === "string" ? direct : direct?.state || direct?.status || entry?.state || entry?.status;
  if (state) return String(state).toLowerCase().replaceAll("_", "-");
  return model.nodes.length ? "missing" : "unknown";
}

function validationCopy(profile, model) {
  const state = String(firstDefined(profile.validation, model.validation, "unknown")).toLowerCase();
  if (["valid", "ready", "ok", "verified"].includes(state)) return "validated profile";
  if (["invalid", "failed", "error"].includes(state)) return "profile validation failed";
  if (state === "partial") return "validation needs review";
  return "validation not reported";
}

function hasReadyNode(profile, model, nodes) {
  if (!nodes.length) return false;
  const hasOnlineCandidate = nodes.some((node) => {
    const presence = nodePresence(model, node);
    return !["missing", "unreadable", "incompatible"].includes(presence) && !["offline", "unknown"].includes(node.state);
  });
  return hasOnlineCandidate && !["invalid", "failed", "error"].includes(String(profile.validation).toLowerCase());
}

export function renderModelCatalog(root, state, filter, onLoad) {
  const profiles = state.profiles.map(normalizeProfile);
  const models = state.models.map(normalizeModel);
  const nodes = state.nodes;
  const loaded = loadedProfileIds(state.deployments);
  const cards = profiles.map((profile) => ({ profile, model: profileModel(profile, models) })).filter(({ profile, model }) => {
    const available = hasReadyNode(profile, model, nodes);
    if (filter === "ready") return available;
    if (filter === "needs-attention") return !available || ["invalid", "failed", "error"].includes(profile.validation);
    return true;
  });
  const count = document.querySelector("#model-count");
  if (count) count.textContent = `${cards.length} ${cards.length === 1 ? "profile" : "profiles"}`;
  if (!cards.length) {
    root.innerHTML = `<div class="empty-state"><h2>No matching profiles</h2><p>Profiles appear here when the controller has an allowlisted launch recipe to show.</p></div>`;
    return;
  }
  root.innerHTML = cards.map(({ profile, model }) => {
    const tone = statusTone(profile.validation);
    const placed = Array.isArray(profile.node_ids) && profile.node_ids.length ? nodes.filter((node) => profile.node_ids.includes(node.id)) : nodes;
    const presence = placed.map((node) => ({ node, state: nodePresence(model, node) }));
    const available = hasReadyNode(profile, model, nodes);
    const validation = validationCopy(profile, model);
    const profileId = profile.id;
    const facts = [
      ["Alias", profile.alias || model.alias || "—"],
      ["Format", model.format || profile.backend || "—"],
      ["Quantization", model.quantization || "—"],
      ["Context", model.context || "—"],
      ...profileMemoryFacts(profile, model),
      ["Mode", profile.mode || "single-node"],
    ];
    return `
      <article class="model-card" data-validation="${escapeHTML(profile.validation)}">
        <div class="model-card-top">
          <div>
            <div class="model-type">${escapeHTML(profile.backend)} / ${escapeHTML(profile.mode)}</div>
            <h2>${escapeHTML(profile.name)}</h2>
          </div>
          <div class="model-card-badges">
            ${loaded.has(profileId) ? `<span class="status-badge" data-tone="success">Loaded</span>` : ""}
            <span class="status-badge" data-tone="${tone}">${escapeHTML(statusLabel(profile.validation))}</span>
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
          <span class="validation-message">${escapeHTML(validation)}${profile.script ? ` · ${escapeHTML(profile.script)}` : ""}</span>
          <button class="button button-primary" type="button" data-profile-load="${escapeHTML(profileId)}" ${available ? "" : "disabled"} title="${available ? "Open the deployment review" : "No compatible node is currently observed"}">Load profile</button>
        </div>
      </article>`;
  }).join("");
  root.querySelectorAll("[data-profile-load]").forEach((button) => button.addEventListener("click", () => onLoad(button.dataset.profileLoad)));
}
