import assert from "node:assert/strict";
import { deploymentForNode, normalizeNode, switchStopEntries } from "../../controller/webui/ui-core.js";

const historical = { id: "old", state: "unknown", node_ids: ["spark-1"], alias: "old-model" };
const unidentified = normalizeNode({ id: "spark-1", deployment: null, active_model_alias: "running-model" });
assert.equal(deploymentForNode(unidentified, [historical]), null);
assert.equal(deploymentForNode(normalizeNode({ id: "spark-1" }), [historical]), null);
const current = { id: "current", state: "ready", node_ids: ["spark-1"], alias: "running-model" };
assert.equal(deploymentForNode(normalizeNode({ id: "spark-1", deployment: current }), [historical]).id, "current");
assert.equal(deploymentForNode(normalizeNode({ id: "spark-1", deployment: { ...current, state: "stopped" } }), [historical]), null);
console.log("Node actions require the observed deployment identity.");

// A one-node load on spark-1 replaces a two-node deployment on both of its nodes.
const tensorParallel = { id: "dep-tp", state: "ready", node_ids: ["spark-1", "spark-2"], alias: "tp-model" };
const cluster = [
  normalizeNode({ id: "spark-1", display_name: "Spark 1", deployment: tensorParallel }),
  normalizeNode({ id: "spark-2", display_name: "Spark 2", deployment: tensorParallel }),
  normalizeNode({ id: "spark-3", display_name: "Spark 3" }),
];
const stopped = switchStopEntries(["spark-1"], cluster, [tensorParallel]);
assert.deepEqual(stopped.map(({ node, outsideSelection }) => [node.name, outsideSelection]), [["Spark 1", false], ["Spark 2", true]]);
assert.equal(stopped[1].deployment.id, "dep-tp");
assert.deepEqual(switchStopEntries(["spark-1", "spark-2"], cluster, []).map(({ outsideSelection }) => outsideSelection), [false, false]);
assert.deepEqual(switchStopEntries(["spark-3"], cluster, [tensorParallel]), []);
// Only the observed deployment counts; a historical record never adds nodes.
const idle = [normalizeNode({ id: "spark-1" }), normalizeNode({ id: "spark-2" })];
assert.deepEqual(switchStopEntries(["spark-1"], idle, [{ ...tensorParallel, state: "unknown" }]), []);
console.log("A switch names every node of each replaced deployment.");
