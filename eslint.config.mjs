// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

// ESLint flat config for the zero-build web UI and its Node.js tests.
// The pre-commit hook installs the packages into its own environment and
// exposes them through NODE_PATH, which require() honours but ES module
// imports do not, so the shared configs are loaded with createRequire.
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const js = require("@eslint/js");
const globals = require("globals");

export default [
  js.configs.recommended,
  {
    files: ["controller/webui/**/*.js"],
    languageOptions: { sourceType: "module", globals: globals.browser },
  },
  {
    files: ["tests/frontend/**/*.mjs", "eslint.config.mjs"],
    languageOptions: { sourceType: "module", globals: globals.node },
  },
];
