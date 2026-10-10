import {
  escapeHTML,
  formatBytes,
  firstDefined,
  normalizeModel,
  normalizeProfile,
  statusLabel,
  statusTone,
} from "./ui-core.js";

function profileModel(profile, models) {
  const modelId = String(profile.modelId || "");
  return models.map(normalizeModel).find((model) => model.id === modelId || model.id === profile.id) || normalizeModel(profile.model || profile);
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
    const presence = nodes.map((node) => ({ node, state: nodePresence(model, node) }));
    const available = hasReadyNode(profile, model, nodes);
    const validation = validationCopy(profile, model);
    const profileId = profile.id;
    const facts = [
      ["Alias", profile.alias || model.alias || "—"],
      ["Format", model.format || profile.backend || "—"],
      ["Quantization", model.quantization || "—"],
      ["Context", model.context || "—"],
      ["Expected memory", formatBytes(profile.expectedMemory || model.expectedMemory)],
      ["Mode", profile.mode || "single-node"],
    ];
    return `
      <article class="model-card" data-validation="${escapeHTML(profile.validation)}">
        <div class="model-card-top">
          <div>
            <div class="model-type">${escapeHTML(profile.backend)} / ${escapeHTML(profile.mode)}</div>
            <h2 title="${escapeHTML(profile.name)}">${escapeHTML(profile.name)}</h2>
          </div>
          <span class="status-badge" data-tone="${tone}">${escapeHTML(statusLabel(profile.validation))}</span>
        </div>
        <p class="model-card-description">${escapeHTML(profile.description)}</p>
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
