// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

import assert from "node:assert/strict";
import test from "node:test";
import { ACTIONS, apiFetch, controllerStatus, statusTone, watchdogLines } from "../../controller/webui/ui-core.js";

test("a controller without nodes is reachable but not ready to manage anything", () => {
  assert.deepEqual(controllerStatus({ setup_required: true, config_error: null }), { state: "warning", label: "Setup required" });
  assert.deepEqual(controllerStatus({ setup_required: true, config_error: { code: "invalid_node_config" } }), { state: "warning", label: "Setup required" });
  assert.deepEqual(controllerStatus({ setup_required: false }), { state: "ready", label: "Controller ready" });
  assert.deepEqual(controllerStatus({ status: "ok", setup_required: true }), { state: "warning", label: "Setup required" });
});

test("setup and unmanaged states use the warning tone", () => {
  assert.equal(statusTone("setup_required"), "warning");
  assert.equal(statusTone("setup-required"), "warning");
  assert.equal(statusTone("unmanaged"), "warning");
  assert.equal(statusTone("ready"), "success");
  assert.equal(statusTone("offline"), "danger");
});

test("every mutation carries an idempotency key and reads do not", async () => {
  const sent = [];
  globalThis.fetch = async (url, init) => {
    sent.push([init.method, init.headers.get("Idempotency-Key")]);
    return { ok: true, text: async () => "{}" };
  };
  for (const method of ["GET", "POST", "DELETE"]) await apiFetch("/nodes", { method });
  assert.equal(sent[0][1], null);
  assert.match(sent[1][1], /^post-/);
  assert.match(sent[2][1], /^delete-/);
});

test("the watchdog card names each deployment's recovery state", () => {
  const lines = watchdogLines({
    paused: false,
    deployments: [
      { deployment_id: "dep-a", state: "watching", auto_recover: true },
      { deployment_id: "dep-b", state: "failing", failed_probes: 2 },
      { deployment_id: "dep-c", state: "needs-attention", message: "Crash loop." },
    ],
  });
  assert.deepEqual(lines, [
    "dep-a: serving. Automatic recovery is on.",
    "dep-b: 2 failed probes, confirming a crash.",
    "dep-c: needs attention. Crash loop.",
  ]);
  assert.equal(ACTIONS["crash-report"].label, "Crash report");
});

test("the watchdog card shows the draft acceptance and a degradation", () => {
  const watch = { min_acceptance_percent: 12, consecutive_windows: 2, recover: true };
  const lines = watchdogLines({
    paused: false,
    deployments: [
      { deployment_id: "dep-a", state: "watching", auto_recover: true, quality_watch: { ...watch, acceptance_percent: null, low_windows: 0 } },
      { deployment_id: "dep-b", state: "watching", auto_recover: true, quality_watch: { ...watch, acceptance_percent: 31.4, low_windows: 0 } },
      { deployment_id: "dep-c", state: "watching", auto_recover: true, quality_watch: { ...watch, acceptance_percent: 5.2, low_windows: 1 } },
      { deployment_id: "dep-d", state: "superseded", degraded: true },
      { deployment_id: "dep-e", state: "report-due", degraded: true },
    ],
  });
  assert.deepEqual(lines, [
    "dep-a: serving. Automatic recovery is on. Draft acceptance not measured yet.",
    "dep-b: serving. Automatic recovery is on. Draft acceptance 31.4 %.",
    "dep-c: serving. Automatic recovery is on. Draft acceptance 5.2 %, below the 12 % floor in 1 of 2 windows.",
    "dep-d: output degraded, left to the operator after a later operation.",
    "dep-e: output degraded, a new quality report decides again.",
  ]);
  assert.equal(ACTIONS["quality-report"].label, "Quality report");
});
