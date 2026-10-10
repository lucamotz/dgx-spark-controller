# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Read-only upstream catalog lookups for model and runtime selection.

The catalog answers two operator questions without touching a Spark node:

* ``huggingface`` turns a pasted Hugging Face model link into validated metadata
  pinned to an exact 40-hex commit, plus a rough unified-memory fit estimate.
* ``vllm_releases`` lists released vLLM versions from GitHub and resolves one to
  an exact, digest-pinned ``linux/arm64`` image of ``vllm/vllm-openai``.

Safety boundary: every request is an HTTPS ``GET`` to an allowlisted public host
(``http.ALLOWED_HOSTS``) with bounded size and time. Upstream responses are
untrusted input: every field is type-checked and pattern-checked before it is
returned, and unusable shapes fail closed as ``CatalogError``. Nothing here
connects to a node, writes a file, executes a command, or makes a model runnable;
a catalog result is a proposal that a later reviewed recipe and live readiness
check must qualify.
"""
