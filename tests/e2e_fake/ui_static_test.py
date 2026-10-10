# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Dependency-free static checks for the controller's browser shell.

These checks intentionally do not require the controller backend or a browser.
They protect the handoff contract while the fake service is being built.
"""

from __future__ import annotations

import ast
import re
import unittest
from html.parser import HTMLParser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WEBUI = ROOT / "controller" / "webui"


class AssetParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids: set[str] = set()
        self.scripts: list[str] = []
        self.styles: list[str] = []
        self.buttons: list[str] = []
        self._in_button = False
        self._button_text: list[str] = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if attributes.get("id"):
            self.ids.add(attributes["id"])
        if tag == "script" and attributes.get("src"):
            self.scripts.append(attributes["src"])
        if tag == "link" and attributes.get("rel") == "stylesheet":
            self.styles.append(attributes.get("href", ""))
        if tag == "button":
            self._in_button = True
            self._button_text = []

    def handle_data(self, data):
        if self._in_button:
            self._button_text.append(data)

    def handle_endtag(self, tag):
        if tag == "button" and self._in_button:
            self.buttons.append(" ".join(self._button_text).strip())
            self._in_button = False


class UIShellStaticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.index = (WEBUI / "index.html").read_text(encoding="utf-8")
        cls.css = (WEBUI / "styles.css").read_text(encoding="utf-8")
        cls.core = (WEBUI / "ui-core.js").read_text(encoding="utf-8")
        cls.app = (WEBUI / "app.js").read_text(encoding="utf-8")
        cls.dashboard = (WEBUI / "ui-dashboard.js").read_text(encoding="utf-8")
        cls.models = (WEBUI / "ui-models.js").read_text(encoding="utf-8")
        cls.operations = (WEBUI / "ui-operations.js").read_text(encoding="utf-8")
        cls.docs = (ROOT / "docs" / "UI.md").read_text(encoding="utf-8")
        cls.launcher = (ROOT / "controller" / "launcher.py").read_text(encoding="utf-8")

    def test_shell_references_all_zero_build_assets(self):
        parser = AssetParser()
        parser.feed(self.index)
        self.assertIn("/styles.css", parser.styles)
        self.assertIn("/app.js", parser.scripts)
        for script in (
            "ui-core.js",
            "ui-dashboard.js",
            "ui-models.js",
            "ui-operations.js",
        ):
            self.assertTrue((WEBUI / script).is_file(), script)
        for element_id in (
            "summary-grid",
            "node-grid",
            "model-grid",
            "operation-list",
            "operation-detail",
            "warning-list",
            "deploy-modal",
            "log-modal",
            "confirm-modal",
        ):
            self.assertIn(element_id, parser.ids)

    def test_shell_has_accessible_navigation_and_dialogs(self):
        parser = AssetParser()
        parser.feed(self.index)
        self.assertIn('aria-live="polite"', self.index)
        self.assertIn('role="dialog"', self.index)
        self.assertIn('aria-modal="true"', self.index)
        self.assertIn("Skip to main content", self.index)
        self.assertGreaterEqual(len(parser.buttons), 10)
        self.assertIn("Dashboard", parser.buttons)
        self.assertIn("Diagnostics", parser.buttons)

    def test_visual_shell_has_focus_responsive_and_reduced_motion_rules(self):
        self.assertIn(":focus-visible", self.css)
        self.assertIn("@media (max-width: 720px)", self.css)
        self.assertIn("@media (prefers-reduced-motion: reduce)", self.css)
        self.assertIn("@media (max-width: 430px)", self.css)

    def test_core_contains_pure_tolerant_helpers(self):
        for function_name in (
            "escapeHTML",
            "formatBytes",
            "normalizeNode",
            "normalizeOperation",
            "isTerminalOperation",
            "operationProgress",
            "startPoller",
        ):
            self.assertRegex(self.core, rf"export function {function_name}\b")
        self.assertIn("controller_unreachable", self.core)
        self.assertIn("Idempotency-Key", self.core)
        self.assertIn("collection(payload, key)", self.core)
        self.assertIn('["stopped", "failed"].includes(deployment.state)', self.core)

    def test_app_covers_durable_mutations_and_polling(self):
        for route in (
            "/bootstrap",
            "/nodes",
            "/models",
            "/profiles",
            "/deployments",
            "/operations",
            "/reconcile",
            "/recover",
            "/api/health",
        ):
            self.assertIn(route, self.app)
        self.assertIn("startPoller", self.app)
        self.assertIn("startOperationPolling", self.app)
        self.assertIn("/operations/${encodeURIComponent(operationId)}", self.app)
        self.assertIn("Idempotency-Key", self.app)
        self.assertIn('method: "DELETE"', self.app)
        self.assertIn("confirm-conflict", self.app)
        self.assertIn("requestConfirmation", self.app)
        self.assertNotIn("window.confirm", self.app)

    def test_synchronous_node_refresh_is_not_treated_as_an_operation(self):
        helper = re.search(
            r"function operationFromPayload\(payload\) \{(?P<body>.*?)\n\}",
            self.app,
            re.DOTALL,
        )
        self.assertIsNotNone(helper)
        body = helper.group("body")
        self.assertIn("candidate.operation_id || candidate.operationId", body)
        self.assertIn("candidate.action || candidate.kind || candidate.type", body)
        self.assertIn("candidate.state || candidate.status", body)
        self.assertNotIn("if (!candidate.id &&", body)

    def test_feature_modules_render_the_requested_views(self):
        self.assertIn("renderDashboard", self.dashboard)
        self.assertIn("renderModelCatalog", self.models)
        self.assertIn("renderOperations", self.operations)
        self.assertIn("getOperationLogs", self.operations)
        self.assertIn("Latest redacted events", self.operations)

    def test_markup_has_no_inline_style_attributes(self):
        # The CSP has style-src 'self', so a style="" attribute is dropped.
        for script in sorted(WEBUI.glob("*.js")):
            with self.subTest(script=script.name):
                text = script.read_text(encoding="utf-8")
                self.assertIsNone(re.search(r"\bstyle=", text))

    def test_ui_contract_documents_routes_payloads_and_safety(self):
        for route in (
            "GET /api/health",
            "GET /api/v1/bootstrap",
            "GET /api/v1/nodes",
            "GET /api/v1/models",
            "GET /api/v1/profiles",
            "GET /api/v1/deployments",
            "GET /api/v1/operations/{id}",
            "GET /api/v1/operations/{id}/logs",
            "POST /api/v1/deployments",
            "DELETE /api/v1/deployments/{id}",
            "POST /api/v1/recover",
        ):
            self.assertIn(route, self.docs)
        for term in (
            "Idempotency-Key",
            "observed state",
            "redacted",
            "closing the app",
            "snake_case",
            "camelCase",
        ):
            self.assertIn(term.lower(), self.docs.lower())

    def test_desktop_launcher_is_python_syntax_and_owns_only_local_child(self):
        tree = ast.parse(self.launcher)
        self.assertIn("never unloads a remote model", ast.get_docstring(tree) or "")
        self.assertIn('import_module("webview")', self.launcher)
        self.assertIn("window.events.closed += on_closed", self.launcher)
        self.assertIn("applicationWillTerminate_", self.launcher)
        self.assertIn("DGX_CONTROLLER_EXIT_WITH_PARENT", self.launcher)
        self.assertIn("process.terminate()", self.launcher)
        self.assertIn("/api/health", self.launcher)
        self.assertNotIn("/api/v1", self.launcher)
        self.assertNotIn("DELETE", self.launcher)

    def test_no_frontend_runtime_dependency_or_build_marker(self):
        combined = "\n".join(
            (
                self.index,
                self.css,
                self.core,
                self.app,
                self.dashboard,
                self.models,
                self.operations,
            )
        )
        self.assertNotRegex(
            combined, r"(?:react|vue|svelte|webpack|vite|npm run build)"
        )
        self.assertNotIn("<script>", self.index)
        self.assertNotIn("eval(", combined)


if __name__ == "__main__":
    unittest.main()
