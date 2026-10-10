import assert from "node:assert/strict";
import { deploymentForNode, normalizeNode, normalizeProfile, switchStopEntries } from "../../controller/webui/ui-core.js";
import { compatibleTargetNodes, conflictWarning, deployFormMarkup, profileNodeCount } from "../../controller/webui/ui-models.js";

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

// The load dialog offers a profile's pinned placement, else the nodes holding
// its model, else every online node.
const online = (id, name = id) => normalizeNode({ id, display_name: name, state: "ready" });
const dialogState = {
  nodes: [online("spark-1"), online("spark-2"), online("spark-3"), normalizeNode({ id: "spark-4", state: "offline" })],
  profiles: [
    normalizeProfile({ id: "pinned", node_ids: ["spark-2"] }),
    normalizeProfile({ id: "pinned-offline", node_ids: ["spark-4"] }),
    normalizeProfile({ id: "by-model", model_id: "m" }),
    normalizeProfile({ id: "anywhere", node_count: 2 }),
  ],
  models: [{ id: "m", nodes: [{ id: "spark-3" }] }],
};
const targetIds = (profileId) => compatibleTargetNodes(dialogState, profileId).map((node) => node.id);
assert.deepEqual(targetIds("pinned"), ["spark-2"]);
assert.deepEqual(targetIds("pinned-offline"), ["spark-1", "spark-2", "spark-3"]);
assert.deepEqual(targetIds("by-model"), ["spark-3"]);
assert.deepEqual(targetIds("anywhere"), ["spark-1", "spark-2", "spark-3"]);
assert.deepEqual(targetIds("missing"), ["spark-1", "spark-2", "spark-3"]);
assert.equal(profileNodeCount(dialogState.profiles[3]), 2);
assert.equal(profileNodeCount(null), 1);

assert.equal(
  conflictWarning(stopped),
  "This will stop the active model on Spark 1, Spark 2. Spark 2 is not selected but runs part of the same deployment, which cannot keep running on only some of its nodes. I have reviewed the affected placement and want to continue.",
);
const hostileProfile = normalizeProfile({ id: "p-1", display_name: "<img src=x onerror=bad()>", mode: "single-node" });
const form = deployFormMarkup({
  profiles: [hostileProfile],
  profileId: "p-1",
  profile: hostileProfile,
  requiredCount: 1,
  targets: [online("spark-1", '"><script>')],
  targetNodeIds: ["spark-1"],
  conflicts: stopped,
  conflictConfirmed: false,
});
assert.doesNotMatch(form, /<img|<script/);
assert.match(form, /<option value="p-1" selected>/);
assert.match(form, /name="target-node" value="spark-1" checked/);
assert.match(form, /id="confirm-conflict"\s+\/>/, "the conflict starts unconfirmed");
const empty = deployFormMarkup({ profiles: [], profileId: null, profile: null, requiredCount: 1, targets: [], targetNodeIds: [], conflicts: [], conflictConfirmed: false });
assert.match(empty, /<option>No profiles available<\/option>/);
assert.match(empty, /No compatible online nodes are currently observed\./);
assert.doesNotMatch(empty, /impact-card|confirm-conflict/);
console.log("The load dialog offers compatible targets and escapes its markup.");
