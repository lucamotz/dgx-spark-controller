import assert from "node:assert/strict";
import { operationText } from "../../controller/webui/ui-operations.js";

const base = { profileId: "g-qwen3-8b-00aa11", deploymentId: "dep-g-qwen3-8b-00aa11", nodeIds: ["spark-a"] };
assert.equal(operationText({ ...base, action: "load" }), "Profile g-qwen3-8b-00aa11");
// "unload" contains "load"; it must still name the deployment it stops.
assert.equal(operationText({ ...base, action: "unload" }), "Deployment dep-g-qwen3-8b-00aa11");
assert.equal(operationText({ ...base, action: "unload", deploymentId: null }), "Managed deployment");
assert.equal(operationText({ ...base, action: "reconcile", nodeIds: ["spark-a", "spark-b"] }), "2 nodes");
console.log("Operation rows name the profile a load starts and the deployment an unload stops.");
