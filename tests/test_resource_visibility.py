from __future__ import annotations

import inspect
import unittest
from unittest.mock import patch

from fastapi.routing import APIRoute

from app.api import crawler as crawler_api
from app.core.auth import require_superadmin
from app.services import crawler_service


class ResourceVisibilityTests(unittest.TestCase):
    def test_global_time_settings_routes_require_superadmin(self) -> None:
        expected_routes = {
            ("GET", "/crawler/settings/time"),
            ("PUT", "/crawler/settings/time"),
            ("POST", "/crawler/settings/time/scheduled-task-cleanup/run"),
            ("POST", "/crawler/settings/time/unlisted-products/run"),
        }
        for method, path in expected_routes:
            route = next(
                route
                for route in crawler_api.router.routes
                if isinstance(route, APIRoute) and route.path == path and method in route.methods
            )
            dependency_calls = [dependency.call for dependency in route.dependant.dependencies]
            self.assertIn(require_superadmin, dependency_calls, f"{method} {path} should require superadmin")

    def test_superadmin_time_settings_include_queue_health(self) -> None:
        with patch.object(
            crawler_api.crawler_service,
            "get_time_settings",
            return_value={"cleanupWeekday": 6, "cleanupTime": "09:00", "queueHealth": {"status": "ok"}},
        ) as get_settings:
            result = crawler_api.get_time_settings({"role": "superadmin"})

        self.assertEqual(result["settings"]["queueHealth"], {"status": "ok"})
        get_settings.assert_called_once_with(include_queue_health=True)

    def test_time_settings_can_skip_queue_health_probe(self) -> None:
        with patch.object(crawler_service, "task_queue_health") as queue_health:
            result = crawler_service.time_settings_to_public(
                None,
                {"cleanupWeekday": 6, "cleanupTime": "09:00"},
                include_queue_health=False,
            )

        queue_health.assert_not_called()
        self.assertNotIn("queueHealth", result)

    def test_proxy_usage_endpoint_requires_superadmin(self) -> None:
        dependency = inspect.signature(crawler_api.get_proxy_resource_usage).parameters["user"].default

        self.assertIs(dependency.dependency, require_superadmin)


if __name__ == "__main__":
    unittest.main()
