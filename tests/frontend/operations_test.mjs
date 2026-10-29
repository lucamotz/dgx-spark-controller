import assert from "node:assert/strict";
import test from "node:test";
import { normalizeOperation, operationFromPayload } from "../../controller/webui/ui-core.js";
import { operationText, renderOperations } from "../../controller/webui/ui-operations.js";
import { fakeRoot, nodeResource, operationResource } from "./helpers.mjs";

function renderDetail(raw) {
  const operation = normalizeOperation(raw);
  const elements = { list: fakeRoot(), detail: fakeRoot(), count: fakeRoot() };
  renderOperations(elements, { operations: [operation], selectedOperationId: operation.id }, { query: "" });
  return elements.detail.innerHTML;
}

// [label, "done" | "current" | ""] for each step of the detail's phase list.
function phaseSteps(html) {
  return [...html.matchAll(/<li class="phase-item ([^"]*)">\s*<span class="phase-icon"[^>]*>[^<]*<\/span>\s*<span>([^<]+)<\/span>/g)]
    .map(([, classes, label]) => [label, classes.includes("is-done") ? "done" : classes.includes("is-current") ? "current" : ""]);
}

const events = (...phases) => phases.map((phase, sequence) => ({ sequence, timestamp: "2026-10-29T12:00:05+00:00", phase, code: "step", message: "Step." }));

test("operation rows name the profile a load starts and the deployment an unload stops", () => {
  const load = normalizeOperation(operationResource({ profile_id: "g-qwen3-8b-00aa11", deployment_id: "dep-g-qwen3-8b-00aa11" }));
  assert.equal(operationText(load), "Profile g-qwen3-8b-00aa11");
  assert.equal(operationText({ ...load, action: "unload" }), "Deployment dep-g-qwen3-8b-00aa11");
  assert.equal(operationText({ ...load, action: "reconcile", nodeIds: ["spark-a", "spark-b"] }), "2 nodes");
});

test("a finished operation's run time ends when it finished", () => {
  const html = renderDetail(operationResource({
    state: "succeeded",
    phase: "complete",
    progress_percent: 100,
    started_at: "2026-10-29T12:00:00+00:00",
    updated_at: "2026-10-29T12:01:30+00:00",
  }));
  assert.match(html, /<span class="section-meta">1m 30s<\/span>/);
});

test("each action lists the phases it passes through", () => {
  const unload = renderDetail(operationResource({ action: "unload", phase: "draining", events: events("queued", "validating", "locking", "refreshing", "draining") }));
  assert.deepEqual(phaseSteps(unload), [
    ["Locking", "done"], ["Refreshing", "done"], ["Draining", "current"],
    ["Stopping Conflicts", ""], ["Verifying Stopped", ""], ["Committing", ""],
  ]);
  assert.deepEqual(phaseSteps(renderDetail(operationResource({ action: "prepare", phase: "preparing" }))), [["Preparing", "current"], ["Committing", ""]]);
  const update = phaseSteps(renderDetail(operationResource({ action: "update", phase: "rebooting" })));
  assert.deepEqual(update.filter(([, step]) => step !== "done").slice(0, 2), [["Rebooting", "current"], ["Starting Workers", ""]]);
});

test("a finished run marks only the phases it completed", () => {
  const failed = phaseSteps(renderDetail(operationResource({
    state: "failed",
    phase: "complete",
    progress_percent: 100,
    events: events("locking", "refreshing", "preflighting", "complete"),
  })));
  assert.deepEqual(failed.slice(0, 4), [["Locking", "done"], ["Refreshing", "done"], ["Preflighting", ""], ["Snapshotting", ""]]);
  assert.ok(failed.every(([, step]) => step !== "current"));
  const succeeded = renderDetail(operationResource({
    action: "reconcile", state: "succeeded", phase: "complete", progress_percent: 100,
    events: events("locking", "refreshing", "reconciling", "complete"),
  }));
  assert.deepEqual(phaseSteps(succeeded), [["Locking", "done"], ["Refreshing", "done"], ["Reconciling", "done"]]);
});

test("an update that found nothing to install does not mark the stop, install or reboot phases", () => {
  const steps = phaseSteps(renderDetail(operationResource({
    action: "update", state: "succeeded", phase: "complete", progress_percent: 100,
    events: events("locking", "refreshing", "preflighting", "committing", "complete"),
  })));
  assert.deepEqual(steps.filter(([, step]) => step === "done").map(([label]) => label), ["Locking", "Refreshing", "Preflighting", "Committing"]);
});

test("a rollback marks no forward phase as current", () => {
  const steps = phaseSteps(renderDetail(operationResource({
    phase: "rolling-back", events: events("locking", "refreshing", "preflighting", "rolling-back"),
  })));
  assert.ok(steps.every(([, step]) => step !== "current"));
  assert.deepEqual(steps.slice(0, 3), [["Locking", "done"], ["Refreshing", "done"], ["Preflighting", ""]]);
});

test("only an operation in a 202 response is followed", () => {
  assert.equal(operationFromPayload({ operation: operationResource({ state: "queued", phase: "queued" }) }).progress, 10);
  // A recipe cancel and a synchronous node refresh answer without an operation.
  assert.equal(operationFromPayload({ operation: { id: "op-1", cancel_requested: true } }), null);
  assert.equal(operationFromPayload(nodeResource("spark-a")), null);
});
