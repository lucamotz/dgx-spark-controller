import assert from "node:assert/strict";
import test from "node:test";
import { monitorStatus, telemetryMarkup, telemetryStatus, renderTelemetry } from "../../controller/webui/ui-dashboard.js";
import { GIB, countMatches } from "./helpers.mjs";

const now = Date.parse("2026-09-19T12:00:00Z");
const sample = {
  node_id: "spark-1",
  sampled_at: "2026-09-19T12:00:00Z",
  status: "ok",
  simulated: false,
  memory: { used_bytes: 48 * GIB, total_bytes: 128 * GIB, available_bytes: 80 * GIB, used_percent: 37.5, swap_used_bytes: 0 },
  gpu: { utilization_percent: 0 },
};

test("a sample is live, stale, paused, demo, partial or unavailable", () => {
  assert.deepEqual(telemetryStatus(sample, { now }), { label: "Live", state: "live" });
  assert.equal(telemetryStatus(sample, { now: now + 31000 }).state, "stale");
  assert.equal(telemetryStatus(sample, { now, failed: true }).state, "stale");
  assert.equal(telemetryStatus(sample, { now, paused: true }).state, "paused");
  assert.equal(telemetryStatus({ ...sample, simulated: true }, { now }).label, "Demo");
  assert.equal(telemetryStatus({ ...sample, status: "partial" }, { now }).label, "Partial");
  assert.equal(telemetryStatus({ ...sample, status: "unavailable" }, { now }).state, "unavailable");
  assert.equal(telemetryStatus(null, { now, failed: true }).label, "Unavailable");
});

test("a missing reading is shown as unavailable, never as zero", () => {
  const known = telemetryMarkup(sample);
  assert.match(known, /48\.0 GiB \/ 128\.0 GiB/);
  assert.match(known, /80\.0 GiB available/);
  assert.match(known, /Swap 0\.0 GiB/);
  assert.match(known, />0%<\/strong>/);
  assert.equal(countMatches(known, /<meter /g), 2);
  const missingGPU = telemetryMarkup({ ...sample, gpu: { utilization_percent: null } });
  assert.match(missingGPU, /Not reported by this node/);
  assert.equal(countMatches(missingGPU, /<meter /g), 1);
  assert.doesNotMatch(missingGPU, />0%<\/strong>/);
  assert.doesNotMatch(telemetryMarkup(null), /<meter /);
});

test("polling updates the noninteractive metric region, leaving node controls intact", () => {
  const metrics = { innerHTML: "" };
  const stamp = { textContent: "" };
  const region = {
    dataset: { telemetryNode: "spark-1" },
    querySelector(selector) { return selector === ".node-metrics" ? metrics : stamp; },
  };
  const root = { querySelectorAll() { return [region]; } };
  renderTelemetry(root, new Map([["spark-1", { ...sample, message: '<img src=x onerror="bad()">' }]]), { now });
  assert.equal(region.dataset.state, "live");
  assert.equal(metrics.innerHTML, telemetryMarkup(sample));
  assert.match(stamp.textContent, /<img src=x/); // textContent, never HTML.
  renderTelemetry(root, new Map([["spark-1", sample]]), { paused: true });
  assert.equal(region.dataset.state, "paused");
  assert.match(stamp.textContent, /^Paused/);
});

test("the monitor label summarizes every node; pause wins, then staleness, then gaps", () => {
  const spark1 = { id: "spark-1" };
  const spark2 = { id: "spark-2" };
  const live = new Map([["spark-1", sample]]);
  assert.deepEqual(monitorStatus([spark1], live, { now }), { label: "Live metrics", state: "live" });
  assert.deepEqual(monitorStatus([spark1], live, { now, paused: true }), { label: "Monitoring paused", state: "paused" });
  assert.deepEqual(monitorStatus([spark1], live, { now: now + 31000 }), { label: "Samples stale", state: "stale" });
  assert.deepEqual(monitorStatus([spark1, spark2], live, { now }), { label: "Waiting / unavailable", state: "stale" });
  assert.deepEqual(monitorStatus([], new Map(), { now }), { label: "No samples", state: "live" });
  assert.deepEqual(monitorStatus([spark1], new Map([["spark-1", { ...sample, simulated: true }]]), { now }), { label: "Demo metrics", state: "live" });
  assert.deepEqual(monitorStatus([spark1], new Map([["spark-1", { ...sample, status: "partial" }]]), { now }), { label: "Partial metrics", state: "stale" });
});
