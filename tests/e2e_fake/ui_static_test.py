# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the DGX Spark Controller project

"""Dependency-free static checks for the controller's browser shell.

These checks intentionally do not require the controller backend or a browser.
They protect the handoff contract while the fake service is being built.
"""

from __future__ import annotations

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
            "ui-recipes.js",
            "ui-setup.js",
            "ui-updates.js",
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
        ):
            self.assertIn(term.lower(), self.docs.lower())

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
        # No eval, and dialogs go through the in-page confirmation modal.
        for script in WEBUI.glob("*.js"):
            source = script.read_text(encoding="utf-8")
            self.assertNotIn("eval(", source)
            self.assertNotIn("window.confirm", source)


if __name__ == "__main__":
    unittest.main()
