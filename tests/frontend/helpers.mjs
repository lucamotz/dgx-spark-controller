// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

// Shared fixtures for the frontend tests; not a test file itself.

export const GIB = 2 ** 30;

export function countMatches(text, pattern) {
  return (text.match(pattern) || []).length;
}

// Resource shapes as controller/resources.py emits them.

export function nodeResource(id, fields = {}) {
  return {
    id,
    display_name: id,
    management_address: "192.0.2.10",
    state: "idle",
    last_refresh_at: "2026-10-29T12:00:00+00:00",
    endpoint: null,
    active_model_alias: null,
    runtime_version: null,
    memory: { total_bytes: 128 * GIB, used_bytes: 8 * GIB, used_percent: 6.3, free_bytes: 120 * GIB, swap_used_bytes: null },
    gpu: { name: "GB10", utilization_percent: null },
    warnings: [],
    deployment: null,
    ...fields,
  };
}

export function profileResource(fields = {}) {
  return {
    id: "profile",
    display_name: "Profile",
    model_id: "model",
    api_alias: "model",
    backend: "vllm",
    deployment_mode: "independent",
    node_count: 1,
    weights_bytes: null,
    reserved_memory_bytes: null,
    gpu_memory_utilization: null,
    origin: "reviewed",
    provenance: {},
    node_ids: [],
    context_length: null,
    ...fields,
  };
}

export function operationResource(fields = {}) {
  return {
    id: "op-1",
    action: "load",
    state: "running",
    phase: "locking",
    progress_percent: 10,
    message: "Acquiring placement leases.",
    profile_id: "profile",
    deployment_id: "dep-1",
    node_ids: ["spark-a"],
    requested_at: "2026-10-29T12:00:00+00:00",
    started_at: "2026-10-29T12:00:01+00:00",
    updated_at: "2026-10-29T12:00:02+00:00",
    events: [],
    ...fields,
  };
}

// A stand-in for an element a render function fills through innerHTML.
export function fakeRoot() {
  return { innerHTML: "", textContent: "", querySelectorAll: () => [] };
}
