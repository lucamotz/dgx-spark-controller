// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

// Shared fixtures for the frontend tests; not a test file itself.

export const GIB = 2 ** 30;

export function countMatches(text, pattern) {
  return (text.match(pattern) || []).length;
}
