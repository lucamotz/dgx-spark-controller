// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

import assert from "node:assert/strict";
import test from "node:test";
import { helperTone, nodeRowMarkup, scheduleSummary, updateConfirmation } from "../../controller/webui/ui-updates.js";
import { countMatches } from "./helpers.mjs";

const node = {
  node_id: "spark-a", display_name: "DGX Spark 1", reachable: true, helper: "ready", message: "",
  os_version: "Ubuntu 24.04.5 LTS", dgx_version: "7.5.0", kernel: "7.0.0-1019-nvidia", driver: "580.178.04",
  packages: 197, firmware: 0, reboot_required: false, checked: true,
  run: { state: "failed", mode: "apply", step: "apt-upgrade", failed_step: "apt-upgrade" },
};

test("a node row shows its versions, pending updates and last helper run", () => {
  const row = nodeRowMarkup(node);
  assert.match(row, /<dt>Packages<\/dt><dd>197<\/dd>/);
  assert.match(row, /<dt>Firmware<\/dt><dd>0<\/dd>/);
  assert.match(row, /<dt>Reboot required<\/dt><dd>no<\/dd>/);
  assert.match(row, /apply failed at apt-upgrade/);
  assert.doesNotMatch(row, /style=/, "CSP forbids inline styles");
});

test("before the first relay check the counts are unknown, not zero", () => {
  const unchecked = nodeRowMarkup({ ...node, packages: null, firmware: null, reboot_required: null, checked: false, run: null });
  assert.equal(countMatches(unchecked, /<dd>not checked<\/dd>/g), 3);
  assert.equal(countMatches(nodeRowMarkup({ ...node, checked: false }), /<dd>not checked<\/dd>/g), 2);
});

test("node facts are escaped", () => {
  const hostile = nodeRowMarkup({ ...node, node_id: '"><svg onload=x>', display_name: "<img src=x>", kernel: "<script>", message: "<b>sudo</b>" });
  assert.doesNotMatch(hostile, /<img|<script|<svg|<b>/);
});

test("a helper that cannot run yet is a warning; an unreachable node is a danger", () => {
  assert.equal(helperTone("ready"), "success");
  for (const state of ["missing", "outdated", "no-sudo"]) assert.equal(helperTone(state), "warning");
  assert.equal(helperTone("unreachable"), "danger");
});

test("the confirmation names the serving model it stops and reloads", () => {
  const appState = {
    deployments: [{ state: "ready", profileId: "glm" }, { state: "stopped", profileId: "qwen" }],
    profiles: [{ id: "glm", name: "GLM-5.3-Flash NVFP4" }, { id: "qwen", name: "Qwen3 8B" }],
  };
  const serving = updateConfirmation({ nodes: [node, { ...node, node_id: "spark-b" }] }, appState);
  assert.match(serving.message, /stops GLM-5\.3-Flash NVFP4, installs the updates on all 2 nodes, reboots each node that installed something, and loads GLM-5\.3-Flash NVFP4 again/);
  assert.doesNotMatch(serving.message, /Qwen3/, "only a ready deployment is serving");
  assert.match(updateConfirmation({ nodes: [node] }, { deployments: [], profiles: [] }).message, /^No model is serving\. .*reboots each node that installed something/);
});

test("the schedule summary names the days, time and next run", () => {
  assert.equal(scheduleSummary({ enabled: false, weekdays: ["sun"], time: "03:00" }, null, "CET"), "Scheduled updates are off.");
  assert.match(scheduleSummary({ enabled: true, weekdays: ["sun", "mon"], time: "03:00" }, "2026-11-01T03:00:00+01:00", "CET"), /^Mon, Sun at 03:00\. Next run: .*2026.* \(CET\)\.$/);
});
