import assert from "node:assert/strict";
import test from "node:test";
import { normalizeNode, normalizeProfile, switchStopEntries } from "../../controller/webui/ui-core.js";
import { compatibleTargetNodes, conflictWarning, deployFormMarkup, profileNodeCount } from "../../controller/webui/ui-models.js";
import { nodeResource, profileResource } from "./helpers.mjs";

const deploymentResource = (fields) => ({ profile_id: "profile", desired_state: "ready", state: "ready", endpoint: null, alias: "model", ...fields });
const tensorParallel = deploymentResource({ id: "dep-tp", node_ids: ["spark-1", "spark-2"], alias: "tp-model" });
const cluster = [
  normalizeNode(nodeResource("spark-1", { display_name: "Spark 1", deployment: tensorParallel })),
  normalizeNode(nodeResource("spark-2", { display_name: "Spark 2", deployment: tensorParallel })),
  normalizeNode(nodeResource("spark-3", { display_name: "Spark 3" })),
];
const switchToSpark1 = switchStopEntries(["spark-1"], cluster);

test("node actions require the observed deployment identity", () => {
  const current = deploymentResource({ id: "current", node_ids: ["spark-1"], alias: "running-model" });
  assert.equal(normalizeNode(nodeResource("spark-1", { active_model_alias: "running-model" })).deployment, null);
  assert.equal(normalizeNode(nodeResource("spark-1", { deployment: current })).deployment.id, "current");
  for (const state of ["stopped", "failed"]) {
    assert.equal(normalizeNode(nodeResource("spark-1", { deployment: { ...current, state } })).deployment, null);
  }
});

test("a switch names every node of each replaced deployment", () => {
  // A one-node load on spark-1 replaces a two-node deployment on both of its nodes.
  assert.deepEqual(switchToSpark1.map(({ node, outsideSelection }) => [node.name, outsideSelection]), [["Spark 1", false], ["Spark 2", true]]);
  assert.equal(switchToSpark1[1].deployment.id, "dep-tp");
  assert.deepEqual(switchStopEntries(["spark-1", "spark-2"], cluster).map(({ outsideSelection }) => outsideSelection), [false, false]);
  assert.deepEqual(switchStopEntries(["spark-3"], cluster), []);
});

test("the load dialog offers a profile's placement, else its model's nodes, else every online node", () => {
  const online = (id) => normalizeNode(nodeResource(id, { state: "ready" }));
  const state = {
    nodes: [online("spark-1"), online("spark-2"), online("spark-3"), normalizeNode(nodeResource("spark-4", { state: "offline" }))],
    profiles: [
      normalizeProfile(profileResource({ id: "pinned", model_id: "any", node_ids: ["spark-2"] })),
      normalizeProfile(profileResource({ id: "pinned-offline", model_id: "any", node_ids: ["spark-4"] })),
      normalizeProfile(profileResource({ id: "by-model", model_id: "m" })),
      normalizeProfile(profileResource({ id: "anywhere", model_id: "any", node_count: 2 })),
    ],
    models: [
      { id: "m", profile_id: "by-model", nodes: [{ id: "spark-3", state: "unverified" }] },
      { id: "m", profile_id: "sibling", nodes: [{ id: "spark-1", state: "unverified" }] },
      ...["pinned", "pinned-offline", "anywhere"].map((id) => ({ id: "any", profile_id: id, nodes: [] })),
    ],
  };
  const targetIds = (profileId) => compatibleTargetNodes(state, profileId).map((node) => node.id);
  assert.deepEqual(targetIds("pinned"), ["spark-2"]);
  assert.deepEqual(targetIds("pinned-offline"), ["spark-1", "spark-2", "spark-3"]);
  assert.deepEqual(targetIds("by-model"), ["spark-3"]);
  assert.deepEqual(targetIds("anywhere"), ["spark-1", "spark-2", "spark-3"]);
  assert.deepEqual(targetIds("missing"), ["spark-1", "spark-2", "spark-3"]);
  assert.equal(profileNodeCount(state.profiles[3]), 2);
  assert.equal(profileNodeCount(undefined), 1);
});

test("the switch warning names nodes outside the selection", () => {
  assert.equal(
    conflictWarning(switchToSpark1),
    "This will stop the active model on Spark 1, Spark 2. Spark 2 is not selected but runs part of the same deployment, which cannot keep running on only some of its nodes. I have reviewed the affected placement and want to continue.",
  );
});

test("the load form escapes its markup and starts unconfirmed", () => {
  const hostileProfile = normalizeProfile(profileResource({ id: "p-1", display_name: "<img src=x onerror=bad()>" }));
  const form = deployFormMarkup({
    profiles: [hostileProfile],
    profileId: "p-1",
    profile: hostileProfile,
    requiredCount: 1,
    targets: [normalizeNode(nodeResource("spark-1", { display_name: '"><script>' }))],
    targetNodeIds: ["spark-1"],
    conflicts: switchToSpark1,
    conflictConfirmed: false,
  });
  assert.doesNotMatch(form, /<img|<script/);
  assert.match(form, /<option value="p-1" selected>/);
  assert.match(form, /name="target-node" value="spark-1" checked/);
  assert.match(form, /id="confirm-conflict"\s+\/>/);
  const empty = deployFormMarkup({ profiles: [], profileId: null, profile: null, requiredCount: 1, targets: [], targetNodeIds: [], conflicts: [], conflictConfirmed: false });
  assert.match(empty, /<option>No profiles available<\/option>/);
  assert.match(empty, /No compatible online nodes are currently observed\./);
  assert.doesNotMatch(empty, /impact-card|confirm-conflict/);
});
