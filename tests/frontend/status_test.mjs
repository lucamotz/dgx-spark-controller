// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

import assert from "node:assert/strict";
import { controllerStatus, statusTone } from "../../controller/webui/ui-core.js";

// A controller without nodes is reachable but not ready to manage anything.
assert.deepEqual(controllerStatus({ setup_required: true, config_error: null }), { state: "warning", label: "Setup required" });
assert.deepEqual(controllerStatus({ setup_required: true, config_error: { code: "invalid_node_config" } }), { state: "warning", label: "Setup required" });
assert.deepEqual(controllerStatus({ setup_required: false }), { state: "ready", label: "Controller ready" });
assert.deepEqual(controllerStatus({ status: "ok", setup_required: true }), { state: "warning", label: "Setup required" });
// The resource fallback has no setup block; reachable resources mean ready.
assert.deepEqual(controllerStatus(null), { state: "ready", label: "Controller ready" });

assert.equal(statusTone("setup_required"), "warning");
assert.equal(statusTone("setup-required"), "warning");
assert.equal(statusTone("unmanaged"), "warning");
assert.equal(statusTone("ready"), "success");
assert.equal(statusTone("offline"), "danger");
console.log("Controller status and tone checks passed.");
