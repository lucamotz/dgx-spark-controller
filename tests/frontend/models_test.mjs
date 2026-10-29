// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

import assert from "node:assert/strict";
import test from "node:test";
import { normalizeDeployment, normalizeNode, normalizeProfile } from "../../controller/webui/ui-core.js";
import {
  loadedProfileIds,
  profileDescription,
  profileMemoryFacts,
  profileProvenance,
  renderModelCatalog,
} from "../../controller/webui/ui-models.js";
import { GIB, fakeRoot, nodeResource, profileResource } from "./helpers.mjs";

const generated = normalizeProfile(profileResource({
  id: "g-qwen3-8b-1a2b3c",
  display_name: "Qwen3-8B (vLLM v0.31.0)",
  origin: "generated",
  weights_bytes: 16381516776,
  gpu_memory_utilization: 0.85,
  reserved_memory_bytes: 111411 * 1024 ** 2,
  provenance: { hf_repo: "Qwen/Qwen3-8B", hf_revision: "b968826d9c46dd6066d109eabc6255188de91218", vllm_release: "v0.31.0", vllm_version: "0.31.0", draft_id: "r-0123456789ab" },
}));
const reviewed = normalizeProfile(profileResource({ id: "mimo-reviewed", display_name: "MiMo", weights_bytes: 300 * GIB }));

test("a generated profile is described by its provenance", () => {
  assert.equal(profileProvenance(generated), "Qwen/Qwen3-8B @ b968826d on vLLM 0.31.0");
  assert.equal(profileProvenance({ provenance: { hf_repo: "org/model", vllm_release: "v0.30.1" } }), "org/model on vLLM v0.30.1");
  assert.equal(profileProvenance(reviewed), null);
  assert.equal(profileDescription(generated), "Qwen/Qwen3-8B @ b968826d on vLLM 0.31.0");
  assert.equal(profileDescription(reviewed), "An allowlisted launch recipe with an explicit readiness contract.");
  assert.equal(profileDescription(normalizeProfile(profileResource({ origin: "generated" }))), "An allowlisted launch recipe with an explicit readiness contract.");
});

test("weights are not labelled as expected memory; generated recipes add their reservation", () => {
  assert.deepEqual(profileMemoryFacts(generated), [["Weights", "15 GB"], ["Reserved by recipe", "109 GB · 85% of smallest node"]]);
  assert.deepEqual(profileMemoryFacts(reviewed), [["Weights", "300 GB"]]);
  assert.deepEqual(profileMemoryFacts(normalizeProfile(profileResource())), [["Weights", "—"]]);
});

test("only a ready deployment marks its profile loaded", () => {
  const deployment = (id, profileId, state) => normalizeDeployment({ id, profile_id: profileId, state, node_ids: ["spark-a"] });
  const loaded = loadedProfileIds([
    deployment("dep-a", "g-qwen3-8b-1a2b3c", "ready"),
    deployment("dep-b", "mimo-reviewed", "stopped"),
    deployment("dep-c", "loading-profile", "starting"),
  ]);
  assert.deepEqual([...loaded], ["g-qwen3-8b-1a2b3c"]);
});

test("profiles that share weights are each loadable on their own placement", () => {
  globalThis.document = { querySelector: () => null };
  // The API lists one model resource per profile, both under the weights' id.
  const model = (nodeId) => ({ id: "qwen", format: "safetensors", quantization: null, nodes: [{ id: nodeId, state: "unverified" }] });
  const state = {
    nodes: [normalizeNode(nodeResource("spark-a")), normalizeNode(nodeResource("spark-b"))],
    profiles: [
      normalizeProfile(profileResource({ id: "g-a", model_id: "qwen", node_ids: ["spark-a"] })),
      normalizeProfile(profileResource({ id: "g-b", model_id: "qwen", node_ids: ["spark-b"] })),
    ],
    models: [model("spark-a"), model("spark-b")],
    deployments: [],
  };
  const root = fakeRoot();
  renderModelCatalog(root, state, "all", () => {});
  assert.doesNotMatch(root.innerHTML, /data-profile-load="g-[ab]" disabled/);
  assert.doesNotMatch(root.innerHTML, /data-state="missing"/);
});
