// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

import assert from "node:assert/strict";
import { normalizeProfile } from "../../controller/webui/ui-core.js";
import {
  loadedProfileIds,
  profileDescription,
  profileMemoryFacts,
  profileProvenance,
} from "../../controller/webui/ui-models.js";
import { GIB } from "./helpers.mjs";

const generated = normalizeProfile({
  id: "g-qwen3-8b-1a2b3c",
  display_name: "Qwen3-8B (vLLM v0.31.0)",
  origin: "generated",
  validation_state: "generated",
  expected_memory_bytes: 16381516776,
  weights_bytes: 16381516776,
  gpu_memory_utilization: 0.85,
  reserved_memory_bytes: 111411 * 1024 ** 2,
  provenance: { hf_repo: "Qwen/Qwen3-8B", hf_revision: "b968826d9c46dd6066d109eabc6255188de91218", vllm_release: "v0.31.0", vllm_version: "0.31.0", draft_id: "r-0123456789ab" },
});
const reviewed = normalizeProfile({
  id: "mimo-reviewed",
  display_name: "MiMo",
  origin: "reviewed",
  expected_memory_bytes: 300 * GIB,
  reserved_memory_bytes: null,
  provenance: {},
});

assert.equal(profileProvenance(generated), "Qwen/Qwen3-8B @ b968826d on vLLM 0.31.0");
assert.equal(profileProvenance({ provenance: { hf_repo: "org/model", vllm_release: "v0.30.1" } }), "org/model on vLLM v0.30.1");
assert.equal(profileProvenance(reviewed), null);
assert.equal(profileDescription(generated), "Qwen/Qwen3-8B @ b968826d on vLLM 0.31.0");
assert.equal(profileDescription(reviewed), "An allowlisted launch recipe with an explicit readiness contract.");
assert.equal(profileDescription(normalizeProfile({ origin: "generated" })), "An allowlisted launch recipe with an explicit readiness contract.");

// Weights are not labelled as expected memory; generated recipes add their reservation.
assert.deepEqual(profileMemoryFacts(generated), [["Weights", "15 GB"], ["Reserved by recipe", "109 GB · 85% of smallest node"]]);
assert.deepEqual(profileMemoryFacts(reviewed), [["Weights", "300 GB"]]);
// Older controllers only sent expected_memory_bytes, which always meant the weights.
assert.deepEqual(profileMemoryFacts(normalizeProfile({ expected_memory_bytes: 8 * GIB })), [["Weights", "8.0 GB"]]);
assert.deepEqual(profileMemoryFacts(normalizeProfile({}), { sizeBytes: 4 * GIB }), [["Weights", "4.0 GB"]]);
assert.deepEqual(profileMemoryFacts(normalizeProfile({})), [["Weights", "—"]]);

const loaded = loadedProfileIds([
  { id: "dep-a", profile_id: "g-qwen3-8b-1a2b3c", state: "ready" },
  { id: "dep-b", profile_id: "mimo-reviewed", state: "stopped" },
  { id: "dep-c", profileId: "loading-profile", state: "loading" },
]);
assert.deepEqual([...loaded], ["g-qwen3-8b-1a2b3c"]);
assert.equal(loadedProfileIds(undefined).size, 0);
console.log("Model card memory, provenance and loaded checks passed.");
