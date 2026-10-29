// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

import assert from "node:assert/strict";
import test from "node:test";
import { apiFetch, controllerStatus, statusTone } from "../../controller/webui/ui-core.js";

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
