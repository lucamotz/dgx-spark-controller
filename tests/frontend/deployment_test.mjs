import assert from "node:assert/strict";
import { deploymentForNode, normalizeNode } from "../../controller/webui/ui-core.js";

const historical = { id: "old", state: "unknown", node_ids: ["spark-1"], alias: "old-model" };
const unidentified = normalizeNode({ id: "spark-1", deployment: null, active_model_alias: "running-model" });
assert.equal(deploymentForNode(unidentified, [historical]), null);
assert.equal(deploymentForNode(normalizeNode({ id: "spark-1" }), [historical]), null);
const current = { id: "current", state: "ready", node_ids: ["spark-1"], alias: "running-model" };
assert.equal(deploymentForNode(normalizeNode({ id: "spark-1", deployment: current }), [historical]).id, "current");
assert.equal(deploymentForNode(normalizeNode({ id: "spark-1", deployment: { ...current, state: "stopped" } }), [historical]), null);
console.log("Node actions require the observed deployment identity.");
