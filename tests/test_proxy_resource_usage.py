from __future__ import annotations

import unittest
from threading import Event, Lock, Thread
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from app.services import crawler_service


class ProxyResourceUsageTests(unittest.TestCase):
    def setUp(self) -> None:
        with crawler_service.PROXY_USAGE_CACHE_LOCK:
            crawler_service.PROXY_USAGE_CACHE.clear()

    def tearDown(self) -> None:
        with crawler_service.PROXY_USAGE_CACHE_LOCK:
            crawler_service.PROXY_USAGE_CACHE.clear()

    def test_subscription_userinfo_ignores_expiry(self) -> None:
        result = crawler_service.parse_subscription_userinfo(
            "upload=1073741824; download=2147483648; total=10737418240; expire=1806653566"
        )

        self.assertEqual(result, {
            "uploadBytes": 1073741824,
            "downloadBytes": 2147483648,
            "totalBytes": 10737418240,
        })

    def test_next_reset_uses_current_month_before_reset_day(self) -> None:
        result = crawler_service.next_proxy_traffic_reset_at(
            2,
            now=datetime(2026, 7, 1, 12, 0, 0),
        )
        self.assertEqual(result, datetime(2026, 7, 2, 0, 0, 0))

    def test_next_reset_moves_to_next_month_after_reset_day(self) -> None:
        result = crawler_service.next_proxy_traffic_reset_at(
            2,
            now=datetime(2026, 7, 12, 12, 0, 0),
        )
        self.assertEqual(result, datetime(2026, 8, 2, 0, 0, 0))

    def test_public_payload_uses_current_time_for_reset_countdown(self) -> None:
        result = crawler_service.proxy_usage_public_payload(
            upload_bytes=0,
            download_bytes=5 * 1024 ** 3,
            total_bytes=10 * 1024 ** 3,
            source="mihomo_config",
            stale=True,
            checked_at=datetime(2026, 6, 20, 12, 0, 0),
            now=datetime(2026, 7, 12, 12, 0, 0),
        )

        self.assertEqual(result["resetAt"], "2026-08-02 00:00:00")
        self.assertEqual(result["resetRemainingSeconds"], 20 * 24 * 60 * 60 + 12 * 60 * 60)

    def test_subscription_request_streams_without_downloading_body(self) -> None:
        response = Mock()
        response.headers = {
            "subscription-userinfo": "upload=1; download=2; total=10; expire=1806653566",
        }
        with (
            patch.object(crawler_service.settings, "proxy_subscription_url", "https://example.com/sub"),
            patch.object(crawler_service.requests, "get", return_value=response) as request_get,
        ):
            result = crawler_service.fetch_proxy_subscription_usage()

        request_get.assert_called_once_with(
            "https://example.com/sub",
            headers={"User-Agent": "clash.meta"},
            timeout=crawler_service.settings.crawler_timeout_seconds,
            stream=True,
        )
        response.close.assert_called_once_with()
        self.assertEqual(result["usedBytes"], 3)

    def test_force_refresh_falls_back_to_mihomo_and_persists_stale_result(self) -> None:
        cached_payload = crawler_service.proxy_usage_public_payload(
            upload_bytes=1,
            download_bytes=2,
            total_bytes=10,
            source="subscription",
            stale=False,
            checked_at=datetime(2026, 7, 12, 12, 0, 0),
        )
        with crawler_service.PROXY_USAGE_CACHE_LOCK:
            crawler_service.PROXY_USAGE_CACHE.update({
                "payload": cached_payload,
                "cachedAt": datetime.now() - timedelta(seconds=1),
            })
        fallback_payload = crawler_service.proxy_usage_public_payload(
            upload_bytes=0,
            download_bytes=4,
            total_bytes=10,
            source="mihomo_config",
            stale=True,
            checked_at=datetime(2026, 7, 2, 12, 0, 0),
        )

        with (
            patch.object(crawler_service, "fetch_proxy_subscription_usage", side_effect=RuntimeError("offline")),
            patch.object(crawler_service, "proxy_usage_from_mihomo_config", return_value=fallback_payload) as fallback,
        ):
            result = crawler_service.get_proxy_resource_usage(force=True)

        fallback.assert_called_once_with()
        self.assertEqual(result["source"], "mihomo_config")
        self.assertTrue(result["stale"])
        with crawler_service.PROXY_USAGE_CACHE_LOCK:
            self.assertTrue(crawler_service.PROXY_USAGE_CACHE["payload"]["stale"])
            self.assertEqual(crawler_service.PROXY_USAGE_CACHE["payload"]["source"], "mihomo_config")

    def test_force_refresh_does_not_treat_equal_wall_clock_as_new_cache(self) -> None:
        class FrozenDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 7, 12, 12, 0, 0, tzinfo=tz)

        cached_payload = crawler_service.proxy_usage_public_payload(
            upload_bytes=1,
            download_bytes=2,
            total_bytes=10,
            source="subscription",
            stale=False,
            checked_at=datetime(2026, 7, 12, 11, 0, 0),
        )
        with crawler_service.PROXY_USAGE_CACHE_LOCK:
            crawler_service.PROXY_USAGE_CACHE.update({
                "payload": cached_payload,
                "cachedAt": FrozenDateTime.now(),
            })
        refreshed_payload = {
            **cached_payload,
            "usedBytes": 5,
            "remainingBytes": 5,
        }

        with (
            patch.object(crawler_service, "datetime", FrozenDateTime),
            patch.object(
                crawler_service,
                "fetch_proxy_subscription_usage",
                return_value=refreshed_payload,
            ) as fetch,
        ):
            result = crawler_service.get_proxy_resource_usage(force=True)

        fetch.assert_called_once_with()
        self.assertEqual(result["usedBytes"], 5)

    def test_concurrent_force_refreshes_share_one_external_request(self) -> None:
        request_started = Event()
        release_request = Event()
        counter_lock = Lock()
        request_count = 0
        results: list[dict[str, object]] = []

        def fetch_usage() -> dict[str, object]:
            nonlocal request_count
            with counter_lock:
                request_count += 1
            request_started.set()
            release_request.wait(timeout=2)
            return crawler_service.proxy_usage_public_payload(
                upload_bytes=1,
                download_bytes=2,
                total_bytes=10,
                source="subscription",
                stale=False,
                checked_at=datetime.now(),
            )

        def refresh() -> None:
            results.append(crawler_service.get_proxy_resource_usage(force=True))

        with patch.object(crawler_service, "fetch_proxy_subscription_usage", side_effect=fetch_usage):
            first = Thread(target=refresh)
            second = Thread(target=refresh)
            first.start()
            self.assertTrue(request_started.wait(timeout=2))
            second.start()
            release_request.set()
            first.join(timeout=2)
            second.join(timeout=2)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(request_count, 1)
        self.assertEqual(len(results), 2)


if __name__ == "__main__":
    unittest.main()
