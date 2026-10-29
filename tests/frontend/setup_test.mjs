// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

import assert from "node:assert/strict";
import test from "node:test";
import {
  fingerprintListMarkup,
  hostKeyConflict,
  hostKeyConflictMarkup,
  knownHostsNote,
} from "../../controller/webui/ui-setup.js";
import { countMatches } from "./helpers.mjs";

const ed25519 = { key_type: "ssh-ed25519", fingerprint_sha256: "SHA256:ZkAslGjFiUHdGf/WUL8rQvkib4PTvQatUV0OUQSncCA", trusted: false, user_known_hosts: "match" };
const ecdsa = { key_type: "ecdsa-sha2-nistp256", fingerprint_sha256: "SHA256:fcnX8bTy2fadY1720RFvPERxO/I/Edqh2SAA8+K0OBs", trusted: false, user_known_hosts: "absent" };
const rsa = { key_type: "ssh-rsa", fingerprint_sha256: "SHA256:TabztiQrWJmAw5SZMv3Dd+xRME/F9Hx/4hq6EyUFdks", trusted: false, user_known_hosts: "mismatch" };

test("each known_hosts comparison has its own note", () => {
  assert.deepEqual(knownHostsNote("match"), { tone: "success", text: "matches your ~/.ssh/known_hosts" });
  assert.deepEqual(knownHostsNote("mismatch"), { tone: "danger", text: "differs from your ~/.ssh/known_hosts" });
  assert.equal(knownHostsNote("revoked").tone, "danger");
  assert.deepEqual(knownHostsNote("absent"), { tone: "neutral", text: "not in your known_hosts" });
  assert.equal(knownHostsNote("unknown"), null);
  assert.equal(knownHostsNote(undefined), null);
});

test("one list item per key: type and full fingerprint, never joined on one line", () => {
  const markup = fingerprintListMarkup([ed25519, ecdsa, rsa]);
  assert.equal(countMatches(markup, /<li class="fingerprint-item">/g), 3);
  assert.match(markup, /^<ul class="fingerprint-list">/);
  assert.match(markup, /<span class="fingerprint-type">ssh-ed25519<\/span><code class="fingerprint-value">SHA256:ZkAslGjFiUHdGf\/WUL8rQvkib4PTvQatUV0OUQSncCA<\/code>/);
  assert.match(markup, /data-tone="success">matches your ~\/\.ssh\/known_hosts</);
  assert.match(markup, /data-tone="neutral">not in your known_hosts</);
  assert.match(markup, /data-tone="danger">differs from your ~\/\.ssh\/known_hosts</);
  assert.equal(fingerprintListMarkup([]), "");
  assert.equal(fingerprintListMarkup(undefined), "");
  assert.doesNotMatch(fingerprintListMarkup([{ ...ed25519, user_known_hosts: "unknown" }]), /known-hosts-note/);
});

test("fingerprint values are escaped even though the server validates them", () => {
  const hostile = fingerprintListMarkup([{ key_type: "<img src=x onerror=bad()>", fingerprint_sha256: '"><script>', user_known_hosts: "absent" }]);
  assert.doesNotMatch(hostile, /<img|<script/);
});

test("a mismatched or revoked key is a danger-toned warning; matches are not", () => {
  assert.equal(hostKeyConflict([ed25519, ecdsa]), false);
  assert.equal(hostKeyConflict([ed25519, rsa]), true);
  assert.equal(hostKeyConflict([{ ...ed25519, user_known_hosts: "revoked" }]), true);
  assert.equal(hostKeyConflictMarkup([ed25519]), "");
  assert.match(hostKeyConflictMarkup([rsa]), /class="setup-error" role="alert"/);
});
