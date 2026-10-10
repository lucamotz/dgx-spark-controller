import assert from "node:assert/strict";
import { deleteConfirmation, hasNodeFiles, nodeProgressMarkup } from "../../controller/webui/ui-recipes.js";

const GIB = 2 ** 30;
const markup = nodeProgressMarkup({
  "spark-a": { state: "running", step: "downloading", downloaded_bytes: 4 * GIB, total_bytes: 16 * GIB },
  "spark-b": { state: "succeeded", step: "done", downloaded_bytes: null, total_bytes: 16 * GIB },
});
assert.equal((markup.match(/<progress /g) || []).length, 2);
assert.match(markup, /value="25"/);
assert.match(markup, /value="100"/);
assert.match(markup, /spark-a · downloading · 4\.0 GB of 16 GB/);
assert.doesNotMatch(markup, /style=/, "CSP forbids inline styles");
assert.equal(nodeProgressMarkup(null), "");
assert.equal(nodeProgressMarkup({}), "");

const hostile = nodeProgressMarkup({ "<img src=x>": { state: "running", step: "<b>", downloaded_bytes: 2 * GIB, total_bytes: GIB } });
assert.doesNotMatch(hostile, /<img|<b>/);
assert.match(hostile, /value="100"/, "progress is clamped to the total");

const local = deleteConfirmation({ generated_profile_ids: [], placement: { node_ids: ["spark-a"] } }, false);
assert.match(local, /local draft only/);
const prepared = deleteConfirmation({ generated_profile_ids: ["g-qwen3-8b-00aa11"], placement: { node_ids: ["spark-a", "spark-b"] } }, false);
assert.match(prepared, /1 generated profile and its recipe directories/);
assert.match(prepared, /weights and the vLLM image stay/);
const purge = deleteConfirmation({ generated_profile_ids: ["g-qwen3-8b-00aa11"], placement: { node_ids: ["spark-a", "spark-b"] } }, true);
assert.match(purge, /removes the downloaded weights and the vLLM image on 2 nodes/);
// An earlier prepare that could not clean up routes the delete through the nodes.
const leftover = { generated_profile_ids: [], node_files_left: true, placement: { node_ids: ["spark-a"] } };
assert.equal(hasNodeFiles(leftover), true);
assert.equal(hasNodeFiles({ generated_profile_ids: [] }), false);
assert.match(deleteConfirmation(leftover, false), /recipe directory an earlier prepare left/);
assert.doesNotMatch(deleteConfirmation(leftover, false), /local draft only/);
console.log("Recipe cards render per-node progress bars and explain what a delete removes.");
