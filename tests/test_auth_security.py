from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi import HTTPException
from starlette.requests import Request

from app.api import auth as auth_api
from app.main import is_same_origin_browser_request


def request_with_headers(headers: dict[str, str], *, method: str = "POST") -> Request:
    raw_headers = [(key.lower().encode(), value.encode()) for key, value in headers.items()]
    return Request({
        "type": "http",
        "http_version": "1.1",
        "method": method,
        "scheme": "https",
        "path": "/api/auth/login",
        "raw_path": b"/api/auth/login",
        "query_string": b"",
        "headers": raw_headers,
        "client": ("127.0.0.1", 12345),
        "server": ("wujiancm.com", 443),
    })


class AuthSecurityTests(unittest.TestCase):
    def setUp(self) -> None:
        auth_api.LOGIN_FAILURES.clear()

    def tearDown(self) -> None:
        auth_api.LOGIN_FAILURES.clear()

    def test_login_client_ip_uses_proxy_overwritten_real_ip(self) -> None:
        request = request_with_headers({
            "host": "wujiancm.com",
            "x-real-ip": "203.0.113.8",
            "x-forwarded-for": "198.51.100.99, 203.0.113.8",
        })

        self.assertEqual(auth_api.login_client_ip(request), "203.0.113.8")

    def test_memory_fallback_locks_after_configured_failures(self) -> None:
        keys = (("pair-key", 2),)
        with patch.object(auth_api, "redis_connection", side_effect=RuntimeError("redis unavailable")):
            auth_api.record_login_failure(keys)
            auth_api.record_login_failure(keys)
            with self.assertRaises(HTTPException) as context:
                auth_api.assert_login_not_locked(keys)

        self.assertEqual(context.exception.status_code, 429)

    def test_memory_fallback_unlocks_after_lockout_expires(self) -> None:
        auth_api.LOGIN_FAILURES["pair-key"] = {
            "count": 5,
            "locked_until": 999,
        }
        with patch.object(auth_api.time, "time", return_value=1000):
            auth_api.assert_memory_login_not_locked("pair-key", 5)

        self.assertNotIn("pair-key", auth_api.LOGIN_FAILURES)

    def test_same_origin_request_accepts_matching_origin(self) -> None:
        request = request_with_headers({
            "host": "wujiancm.com",
            "origin": "https://wujiancm.com",
            "sec-fetch-site": "same-origin",
        })

        self.assertTrue(is_same_origin_browser_request(request))

    def test_same_origin_request_rejects_cross_site_origin(self) -> None:
        request = request_with_headers({
            "host": "wujiancm.com",
            "origin": "https://attacker.example",
            "sec-fetch-site": "cross-site",
        })

        self.assertFalse(is_same_origin_browser_request(request))


if __name__ == "__main__":
    unittest.main()
