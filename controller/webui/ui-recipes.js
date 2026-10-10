// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

// Recipe builder: paste a Hugging Face link, pick a released vLLM version, tune
// a bounded set of serving settings, prepare (pull image + download weights on
// every node), then load the generated profile through the normal lifecycle.
// Deleting a prepared recipe is a journaled operation that removes its
// generated profiles and node files, optionally with unshared weights/images.

import { escapeHTML, formatBytes, formatRelativeTime } from "./ui-core.js";

const SETTING_FIELDS = [
  ["max_model_len", "Context length", "number", "Tokens per request; capped by the model."],
  ["max_num_seqs", "Concurrent sequences", "number", "Upper bound on simultaneously served requests."],
  ["max_num_batched_tokens", "Batched tokens", "number", "Prefill chunk budget per step."],
  ["gpu_memory_utilization", "Memory fraction", "number", "Share of unified memory vLLM may reserve (0.30–0.95)."],
  ["port", "Endpoint port", "number", "Must be inside every node's configured port range."],
];
const KV_DTYPES = ["auto", "fp8", "fp8_e4m3", "fp8_e5m2"];

const builder = {
  link: "",
  inspection: null,
  releases: [],
  releasesError: "",
  form: null,
  recipes: [],
  busy: new Set(),
  purge: new Set(),
};
const BUSY_STATUSES = new Set(["preparing", "deleting"]);

let ctx = null;
let root = null;
let pollTimer = null;

export function mountRecipes(element, context) {
  root = element;
  ctx = context;
  root.addEventListener("click", onClick);
  root.addEventListener("change", onChange);
  root.addEventListener("input", onInput);
}

export async function refreshRecipes() {
  if (!root) return;
  try {
    const payload = await ctx.apiFetch("/recipes");
    builder.recipes = payload.items || [];
  } catch (error) {
    if (error.code !== "setup_required") ctx.toast(error.message, "danger");
  }
  if (!builder.releases.length && !builder.releasesError) loadReleases();
  render();
  schedulePoll();
}

async function loadReleases() {
  try {
    const payload = await ctx.apiFetch("/catalog/vllm-releases");
    builder.releases = payload.items || [];
    builder.releasesError = "";
  } catch (error) {
    builder.releasesError = error.message;
  }
  render();
}

function schedulePoll() {
  window.clearTimeout(pollTimer);
  if (!builder.recipes.some((recipe) => BUSY_STATUSES.has(recipe.status))) return;
  pollTimer = window.setTimeout(async () => {
    const wasBusy = new Set(builder.recipes.filter((r) => BUSY_STATUSES.has(r.status)).map((r) => r.draft_id));
    await refreshRecipes();
    const settled = [...wasBusy].some((id) => !builder.recipes.some((recipe) => recipe.draft_id === id && BUSY_STATUSES.has(recipe.status)));
    if (settled) await ctx.refresh();
  }, 4000);
}

async function withBusy(key, action) {
  if (builder.busy.has(key)) return;
  builder.busy.add(key);
  render();
  try {
    await action();
  } catch (error) {
    ctx.toast(error.remediation ? `${error.message} ${error.remediation}` : error.message, "danger");
  } finally {
    builder.busy.delete(key);
    render();
  }
}

function defaultForm(inspection) {
  const nodes = ctx.state().nodes;
  const suggested = inspection.suggested || {};
  return {
    release_tag: builder.releases[0]?.tag || "",
    cuda_variant: "default",
    node_ids: nodes.slice(0, 1).map((node) => node.id),
    head_node_id: nodes[0]?.id || "",
    served_alias: suggested.served_alias || "",
    display_name: suggested.display_name || "",
    settings: {
      max_model_len: suggested.max_model_len || 32768,
      max_num_seqs: 8,
      max_num_batched_tokens: 8192,
      gpu_memory_utilization: 0.85,
      kv_cache_dtype: "auto",
      enable_prefix_caching: true,
      trust_remote_code: Boolean(suggested.trust_remote_code),
      enforce_eager: false,
      tool_call_parser: "",
      reasoning_parser: "",
      port: 8000,
    },
  };
}

async function inspect() {
  const link = root.querySelector("#recipe-link")?.value.trim() || "";
  builder.link = link;
  if (!link) return;
  await withBusy("inspect", async () => {
    builder.inspection = await ctx.apiFetch("/catalog/models/inspect", { method: "POST", body: { link } });
    builder.form = defaultForm(builder.inspection);
  });
}

async function saveRecipe() {
  const form = builder.form;
  if (!form) return;
  const settings = { ...form.settings };
  for (const key of ["tool_call_parser", "reasoning_parser"]) if (!settings[key]) settings[key] = null;
  await withBusy("save", async () => {
    await ctx.apiFetch("/recipes", {
      method: "POST",
      body: {
        model_link: builder.link,
        release_tag: form.release_tag,
        cuda_variant: form.cuda_variant,
        node_ids: form.node_ids,
        head_node_id: form.head_node_id,
        served_alias: form.served_alias,
        display_name: form.display_name,
        settings,
      },
    });
    ctx.toast("Recipe saved. Prepare it to download the runtime and weights.", "success");
    builder.inspection = null;
    builder.form = null;
    await refreshRecipes();
  });
}

async function prepare(recipe) {
  const gib = (recipe.model.weight_bytes / 1024 ** 3).toFixed(1);
  const confirmed = await ctx.requestConfirmation({
    title: `Prepare ${recipe.display_name}?`,
    message: `Each of ${recipe.placement.node_ids.length} node(s) pulls vLLM ${recipe.runtime.release_tag} and downloads ${gib} GiB of weights into its model root. The running model is not touched.`,
    confirmLabel: "Start preparation",
  });
  if (!confirmed) return;
  await withBusy(`prepare:${recipe.draft_id}`, async () => {
    await ctx.apiFetch(`/recipes/${encodeURIComponent(recipe.draft_id)}/prepare`, { method: "POST", body: {} });
    ctx.toast("Preparation started. Progress appears in Operations.", "success");
    await refreshRecipes();
    await ctx.refresh();
  });
}

function hasNodeFiles(recipe) {
  return (recipe.generated_profile_ids || []).length > 0;
}

export function deleteConfirmation(recipe, removeArtifacts) {
  const profiles = (recipe.generated_profile_ids || []).length;
  const nodes = recipe.placement?.node_ids?.length || recipe.node_count || 0;
  if (!profiles && !removeArtifacts) {
    return "This deletes the local draft only. Nothing on the nodes changes.";
  }
  const parts = [];
  if (profiles) parts.push(`Removes ${profiles} generated profile${profiles === 1 ? " and its" : "s and their"} recipe directories on the nodes. A loaded profile must be unloaded first.`);
  parts.push(removeArtifacts
    ? `Also removes the downloaded weights and the vLLM image on ${nodes} node${nodes === 1 ? "" : "s"} where no other recipe or profile uses them, and the shared cache volume once a node has no generated profile left.`
    : "Downloaded weights and the vLLM image stay on the nodes for other recipes and later prepares.");
  return parts.join(" ");
}

async function removeRecipe(recipe) {
  const removeArtifacts = builder.purge.has(recipe.draft_id);
  const remote = hasNodeFiles(recipe) || removeArtifacts;
  const confirmed = await ctx.requestConfirmation({
    title: `Delete recipe ${recipe.display_name}?`,
    message: deleteConfirmation(recipe, removeArtifacts),
    confirmLabel: removeArtifacts ? "Delete recipe and files" : "Delete recipe",
  });
  if (!confirmed) return;
  await withBusy(`delete:${recipe.draft_id}`, async () => {
    const path = `/recipes/${encodeURIComponent(recipe.draft_id)}`;
    if (remote) {
      await ctx.apiFetch(`${path}/delete`, { method: "POST", body: { remove_artifacts: removeArtifacts } });
      ctx.toast("Deletion started. Progress appears in Operations.", "success");
    } else {
      await ctx.apiFetch(path, { method: "DELETE" });
    }
    builder.purge.delete(recipe.draft_id);
    await refreshRecipes();
    if (remote) await ctx.refresh();
  });
}

function editFrom(recipe) {
  builder.link = `${recipe.model.repo_id}@${recipe.model.revision}`;
  builder.inspection = { model: { repo_id: recipe.model.repo_id, revision: recipe.model.revision, weight_bytes: recipe.model.weight_bytes, max_position_embeddings: recipe.model.max_position_embeddings }, fit: [], suggested: {} };
  builder.form = {
    release_tag: recipe.runtime.release_tag,
    cuda_variant: recipe.runtime.cuda_variant,
    node_ids: [...recipe.placement.node_ids],
    head_node_id: recipe.placement.head_node_id,
    served_alias: recipe.served_alias,
    display_name: `${recipe.display_name} (copy)`,
    settings: { ...recipe.settings, tool_call_parser: recipe.settings.tool_call_parser || "", reasoning_parser: recipe.settings.reasoning_parser || "" },
  };
  render();
  root.querySelector("#recipe-form")?.scrollIntoView({ behavior: "smooth" });
}

function onClick(event) {
  const button = event.target.closest("[data-recipe-action]");
  if (!button) return;
  const recipe = builder.recipes.find((item) => item.draft_id === button.dataset.recipe);
  const action = button.dataset.recipeAction;
  if (action === "inspect") inspect();
  else if (action === "save") saveRecipe();
  else if (action === "discard") { builder.inspection = null; builder.form = null; render(); }
  else if (action === "prepare" && recipe) prepare(recipe);
  else if (action === "delete" && recipe) removeRecipe(recipe);
  else if (action === "copy" && recipe) editFrom(recipe);
  else if (action === "load" && recipe?.profile_id) ctx.openDeployModal(recipe.profile_id);
  else if (action === "reload-releases") { builder.releasesError = ""; loadReleases(); }
}

function onChange(event) {
  const field = event.target;
  if (field.matches("[data-recipe-purge]")) {
    if (field.checked) builder.purge.add(field.dataset.recipePurge); else builder.purge.delete(field.dataset.recipePurge);
    return;
  }
  if (!builder.form) return;
  if (field.matches("[data-recipe-node]")) {
    const ids = new Set(builder.form.node_ids);
    if (field.checked) ids.add(field.dataset.recipeNode); else ids.delete(field.dataset.recipeNode);
    builder.form.node_ids = ctx.state().nodes.map((node) => node.id).filter((id) => ids.has(id));
    if (!ids.has(builder.form.head_node_id)) builder.form.head_node_id = builder.form.node_ids[0] || "";
    render();
    return;
  }
  updateFormField(field);
  if (field.tagName === "SELECT" || field.type === "checkbox") render();
}

function onInput(event) {
  if (builder.form && event.target.matches("[data-recipe-field], [data-recipe-setting]")) updateFormField(event.target);
}

function updateFormField(field) {
  if (field.dataset.recipeField) builder.form[field.dataset.recipeField] = field.value;
  if (field.dataset.recipeSetting) {
    const key = field.dataset.recipeSetting;
    let value = field.type === "checkbox" ? field.checked : field.value;
    if (field.type === "number") value = key === "gpu_memory_utilization" ? Number(Number(value).toFixed(2)) : Number.parseInt(value, 10);
    builder.form.settings[key] = value;
  }
}

function fitRows(fit) {
  if (!fit?.length) return "";
  return `<div class="recipe-fit">${fit.map((row) => `
    <div class="recipe-fit-row" data-fits="${row.fits}">
      <strong>${row.node_count} node${row.node_count === 1 ? "" : "s"}</strong>
      <span>${formatBytes(row.weights_per_node_bytes)} weights per node</span>
      <span>${row.fits ? "fits" : "does not fit"}</span>
    </div>`).join("")}</div>`;
}

function renderInspection() {
  const inspection = builder.inspection;
  if (!inspection) return "";
  const model = inspection.model || {};
  const facts = [
    ["Revision", (model.revision || "").slice(0, 12)],
    ["Weights", model.weight_bytes ? formatBytes(model.weight_bytes) : "unknown"],
    ["Format", model.weight_format || "safetensors"],
    ["Quantization", model.quantization || "none reported"],
    ["Parameters", model.parameter_count ? `${(model.parameter_count / 1e9).toFixed(1)} B` : "unknown"],
    ["Max context", model.max_position_embeddings || "unknown"],
    ["Architecture", (model.architectures || []).join(", ") || "unknown"],
    ["License", model.license || "not stated"],
  ];
  return `
    <div class="recipe-inspection">
      <h3>${escapeHTML(model.repo_id || "")}${model.gated ? ` <span class="status-badge" data-tone="working">gated</span>` : ""}</h3>
      <dl class="facts-list">${facts.map(([label, value]) => `<div class="fact-pair"><dt>${escapeHTML(label)}</dt><dd>${escapeHTML(value)}</dd></div>`).join("")}</dl>
      ${(model.warnings || []).map((warning) => `<p class="setup-warning">${escapeHTML(warning)}</p>`).join("")}
      ${fitRows(inspection.fit)}
    </div>`;
}

function renderForm() {
  const form = builder.form;
  if (!form) return "";
  const nodes = ctx.state().nodes;
  const releaseOptions = builder.releases.map((release) => `<option value="${escapeHTML(release.tag)}" ${release.tag === form.release_tag ? "selected" : ""}>${escapeHTML(release.tag)} · ${escapeHTML((release.published_at || "").slice(0, 10))}</option>`).join("");
  const settings = form.settings;
  return `
    <div class="recipe-form" id="recipe-form">
      <div class="setup-grid">
        <label class="field-group"><span class="field-label">vLLM release</span>
          <select class="select-input" data-recipe-field="release_tag">${releaseOptions || `<option value="">Releases unavailable</option>`}</select>
          ${builder.releasesError ? `<span class="field-help">${escapeHTML(builder.releasesError)} <button class="text-button" type="button" data-recipe-action="reload-releases">Retry</button></span>` : `<span class="field-help">Official vllm/vllm-openai arm64 image, pinned by digest.</span>`}</label>
        <label class="field-group"><span class="field-label">CUDA build</span>
          <select class="select-input" data-recipe-field="cuda_variant">${["default", "cu129"].map((variant) => `<option ${variant === form.cuda_variant ? "selected" : ""}>${variant}</option>`).join("")}</select></label>
        <label class="field-group"><span class="field-label">Served model name</span>
          <input class="select-input" data-recipe-field="served_alias" value="${escapeHTML(form.served_alias)}" /></label>
        <label class="field-group"><span class="field-label">Recipe name</span>
          <input class="select-input" data-recipe-field="display_name" value="${escapeHTML(form.display_name)}" /></label>
      </div>
      <div class="field-group"><span class="field-label">Nodes (tensor parallel across selected Sparks)</span>
        <div class="target-list">${nodes.map((node) => `
          <label class="target-option"><input type="checkbox" data-recipe-node="${escapeHTML(node.id)}" ${form.node_ids.includes(node.id) ? "checked" : ""} />
            <span class="target-copy"><span class="target-name">${escapeHTML(node.displayName || node.id)}</span><span class="target-detail">${escapeHTML(node.id)}</span></span></label>`).join("")}</div>
      </div>
      ${form.node_ids.length > 1 ? `<label class="field-group"><span class="field-label">Head node (serves the endpoint)</span>
        <select class="select-input" data-recipe-field="head_node_id">${form.node_ids.map((id) => `<option ${id === form.head_node_id ? "selected" : ""}>${escapeHTML(id)}</option>`).join("")}</select></label>` : ""}
      <div class="setup-grid">
        ${SETTING_FIELDS.map(([key, label, type, help]) => `<label class="field-group"><span class="field-label">${escapeHTML(label)}</span>
          <input class="select-input" type="${type}" ${key === "gpu_memory_utilization" ? `step="0.01" min="0.30" max="0.95"` : ""} data-recipe-setting="${key}" value="${escapeHTML(settings[key])}" />
          <span class="field-help">${escapeHTML(help)}</span></label>`).join("")}
        <label class="field-group"><span class="field-label">KV cache dtype</span>
          <select class="select-input" data-recipe-setting="kv_cache_dtype">${KV_DTYPES.map((dtype) => `<option ${dtype === settings.kv_cache_dtype ? "selected" : ""}>${dtype}</option>`).join("")}</select></label>
        <label class="field-group"><span class="field-label">Tool-call parser</span>
          <input class="select-input" data-recipe-setting="tool_call_parser" value="${escapeHTML(settings.tool_call_parser || "")}" placeholder="optional, e.g. hermes" /></label>
        <label class="field-group"><span class="field-label">Reasoning parser</span>
          <input class="select-input" data-recipe-setting="reasoning_parser" value="${escapeHTML(settings.reasoning_parser || "")}" placeholder="optional, e.g. qwen3" /></label>
      </div>
      <div class="recipe-toggles">
        <label><input type="checkbox" data-recipe-setting="enable_prefix_caching" ${settings.enable_prefix_caching ? "checked" : ""} /> Prefix caching</label>
        <label><input type="checkbox" data-recipe-setting="enforce_eager" ${settings.enforce_eager ? "checked" : ""} /> Eager mode (no CUDA graphs)</label>
        <label><input type="checkbox" data-recipe-setting="trust_remote_code" ${settings.trust_remote_code ? "checked" : ""} /> Trust remote code (review the repository first)</label>
      </div>
      <div class="intro-actions">
        <button class="button button-secondary" type="button" data-recipe-action="discard">Discard</button>
        <button class="button button-primary" type="button" data-recipe-action="save" ${builder.busy.has("save") || !form.release_tag || !form.node_ids.length ? "disabled" : ""}>Save recipe</button>
      </div>
    </div>`;
}

function statusTone(status) {
  return { prepared: "success", preparing: "working", deleting: "working", failed: "danger" }[status] || "neutral";
}

// Per-node prepare progress from the bounded `progress_by_node` resource field.
// Uses <progress> elements because the CSP forbids inline styles.
export function nodeProgressMarkup(progressByNode) {
  const entries = Object.entries(progressByNode && typeof progressByNode === "object" ? progressByNode : {});
  if (!entries.length) return "";
  return `<div class="recipe-fit">${entries.map(([nodeId, item]) => {
    const total = Math.max(0, Number(item?.total_bytes) || 0);
    const downloaded = Math.max(0, Number(item?.downloaded_bytes) || 0);
    const succeeded = item?.state === "succeeded";
    const percent = succeeded ? 100 : total ? Math.min(100, Math.round((downloaded / total) * 100)) : 0;
    const step = String(item?.step || item?.state || "waiting");
    const size = succeeded && total ? formatBytes(total)
      : total ? `${formatBytes(Math.min(downloaded, total))} of ${formatBytes(total)}` : formatBytes(downloaded);
    return `<div class="field-group">
      <span class="field-help">${escapeHTML(nodeId)} · ${escapeHTML(step)} · ${escapeHTML(size)}</span>
      <progress class="setup-progress" max="100" value="${percent}" aria-label="${escapeHTML(`${nodeId} ${percent}%`)}">${percent}%</progress>
    </div>`;
  }).join("")}</div>`;
}

function renderRecipe(recipe) {
  const busy = builder.busy.has(`prepare:${recipe.draft_id}`) || builder.busy.has(`delete:${recipe.draft_id}`) || BUSY_STATUSES.has(recipe.status);
  const offerPurge = !BUSY_STATUSES.has(recipe.status) && (hasNodeFiles(recipe) || recipe.last_operation_id);
  return `
    <article class="model-card recipe-card">
      <div class="model-card-top">
        <div><p class="eyebrow">${escapeHTML(recipe.runtime.release_tag)} · ${recipe.node_count} node${recipe.node_count === 1 ? "" : "s"}</p><h2>${escapeHTML(recipe.display_name)}</h2></div>
        <span class="status-badge" data-tone="${statusTone(recipe.status)}">${escapeHTML(recipe.status)}</span>
      </div>
      <p class="model-card-description">${escapeHTML(recipe.model.repo_id)}@${escapeHTML(recipe.model.revision.slice(0, 8))} · serves <code>${escapeHTML(recipe.served_alias)}</code> · ${formatBytes(recipe.model.weight_bytes)} · ctx ${escapeHTML(recipe.settings.max_model_len)}</p>
      ${recipe.message ? `<p class="field-help">${escapeHTML(recipe.message)}</p>` : ""}
      ${recipe.status === "preparing" ? nodeProgressMarkup(recipe.progress_by_node) : ""}
      ${recipe.status === "failed" ? `<p class="field-help">Downloaded weights and the vLLM image stay on the nodes so the next prepare resumes. To free the space, delete the recipe with “Also remove weights and image”.</p>` : ""}
      <p class="field-help">Updated ${escapeHTML(formatRelativeTime(recipe.updated_at))}. Generated recipes stay unqualified until a live load passes readiness.</p>
      ${offerPurge ? `<div class="recipe-toggles"><label><input type="checkbox" data-recipe-purge="${escapeHTML(recipe.draft_id)}" ${builder.purge.has(recipe.draft_id) ? "checked" : ""} /> Also remove weights and image when deleting (only where nothing else uses them)</label></div>` : ""}
      <div class="model-card-footer">
        <button class="text-button" type="button" data-recipe-action="copy" data-recipe="${escapeHTML(recipe.draft_id)}">Edit as new</button>
        <button class="text-button" type="button" data-recipe-action="delete" data-recipe="${escapeHTML(recipe.draft_id)}" ${busy ? "disabled" : ""}>Delete</button>
        ${recipe.status === "prepared" && recipe.profile_id
          ? `<button class="button button-primary" type="button" data-recipe-action="load" data-recipe="${escapeHTML(recipe.draft_id)}">Load…</button>`
          : `<button class="button button-secondary" type="button" data-recipe-action="prepare" data-recipe="${escapeHTML(recipe.draft_id)}" ${busy ? "disabled" : ""}>${recipe.status === "preparing" ? "Preparing…" : "Prepare"}</button>`}
      </div>
    </article>`;
}

function render() {
  if (!root) return;
  const configured = ctx.state().nodes.length > 0;
  root.innerHTML = `
    <div class="page-intro">
      <p class="eyebrow">Recipe builder</p>
      <h1>Change models and runtimes</h1>
      <p class="lede">Paste a Hugging Face link, choose a released vLLM version, and tune a bounded set of serving settings. Preparing downloads everything first; switching happens only when you load the prepared profile.</p>
    </div>
    ${configured ? "" : `<div class="empty-state"><h2>No nodes yet</h2><p>Finish Setup before building a recipe.</p></div>`}
    <section class="diagnostics-card setup-section">
      <div class="card-heading-row"><div><p class="eyebrow">New recipe</p><h2>Weights from Hugging Face</h2></div></div>
      <div class="setup-grid setup-grid-wide">
        <label class="field-group"><span class="field-label">Model link</span>
          <input class="select-input" id="recipe-link" value="${escapeHTML(builder.link)}" placeholder="https://huggingface.co/org/model" autocomplete="off" /></label>
        <div class="field-group setup-actions"><button class="button button-primary" type="button" data-recipe-action="inspect" ${builder.busy.has("inspect") ? "disabled" : ""}>${builder.busy.has("inspect") ? "Inspecting…" : "Inspect"}</button></div>
      </div>
      ${renderInspection()}
      ${renderForm()}
    </section>
    <section class="content-section">
      <div class="section-heading"><div><h2>Recipes</h2></div><span class="section-meta">${builder.recipes.length}</span></div>
      <div class="model-grid">${builder.recipes.length ? builder.recipes.map(renderRecipe).join("") : `<div class="empty-state"><h2>No recipes yet</h2><p>Inspect a model link to create the first one.</p></div>`}</div>
    </section>`;
}
