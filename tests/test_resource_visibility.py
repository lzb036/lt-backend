from __future__ import annotations

import inspect
import unittest
from unittest.mock import patch

from app.api import crawler as crawler_api
from app.core.auth import require_superadmin
from app.services import crawler_service


class ResourceVisibilityTests(unittest.TestCase):
    def test_non_superadmin_time_settings_omit_queue_health(self) -> None:
        with patch.object(
            crawler_api.crawler_service,
            "get_time_settings",
            return_value={"cleanupWeekday": 6, "cleanupTime": "09:00"},
        ) as get_settings:
            result = crawler_api.get_time_settings({"role": "operator"})

        self.assertNotIn("queueHealth", result["settings"])
        get_settings.assert_called_once_with(include_queue_health=False)

    def test_superadmin_time_settings_include_queue_health(self) -> None:
        with patch.object(
            crawler_api.crawler_service,
            "get_time_settings",
            return_value={
                "cleanupWeekday": 6,
                "cleanupTime": "09:00",
                "queueHealth": {"status": "ok"},
            },
        ) as get_settings:
            result = crawler_api.get_time_settings({"role": "superadmin"})

        self.assertEqual(result["settings"]["queueHealth"], {"status": "ok"})
        get_settings.assert_called_once_with(include_queue_health=True)

    def test_non_superadmin_time_actions_skip_queue_health(self) -> None:
        user = {"role": "operator"}
        payload = crawler_api.TimeSettingsPayload(cleanupWeekday=6, cleanupTime="09:00")
        with (
            patch.object(crawler_api.crawler_service, "save_time_settings", return_value={}) as save_settings,
            patch.object(
                crawler_api.crawler_service,
                "run_completed_scheduled_crawl_tasks_cleanup_now",
                return_value={},
            ) as run_task_cleanup,
            patch.object(
                crawler_api.crawler_service,
                "run_store_unlisted_product_cleanup_now",
                return_value={"settings": {}, "summary": {"taskCount": 0, "productCount": 0}},
            ) as run_product_cleanup,
        ):
            crawler_api.update_time_settings(payload, user)
            crawler_api.run_scheduled_task_cleanup(user)
            crawler_api.run_unlisted_product_cleanup(user)

        save_settings.assert_called_once_with(payload, include_queue_health=False)
        run_task_cleanup.assert_called_once_with(include_queue_health=False)
        run_product_cleanup.assert_called_once_with(include_queue_health=False)

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
