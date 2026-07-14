from __future__ import annotations

import unittest

from fastapi.routing import APIRoute

from app.api import crawler as crawler_api
from app.core.auth import require_superadmin


class AiTitleApiTests(unittest.TestCase):
    def test_serialize_sse_event_outputs_valid_json_data(self) -> None:
        payload = crawler_api.serialize_sse_event({"type": "delta", "content": "春物"})

        self.assertEqual(payload, 'data: {"type": "delta", "content": "春物"}\n\n'.encode())

    def test_ai_settings_routes_require_superadmin(self) -> None:
        expected = {
            ("GET", "/crawler/settings/ai-title"),
            ("PUT", "/crawler/settings/ai-title"),
            ("POST", "/crawler/settings/ai-title/test"),
        }
        actual = {
            (method, route.path)
            for route in crawler_api.router.routes
            if isinstance(route, APIRoute)
            for method in route.methods
            if route.path.startswith("/crawler/settings/ai-title")
        }

        self.assertEqual(actual, expected)
        for method, path in expected:
            route = next(
                route
                for route in crawler_api.router.routes
                if isinstance(route, APIRoute) and route.path == path and method in route.methods
            )
            self.assertIn(require_superadmin, [item.call for item in route.dependant.dependencies])
