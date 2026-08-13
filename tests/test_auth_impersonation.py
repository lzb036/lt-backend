from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from app.api import auth


class FakeRedis:
    def __init__(self):
        self.values = {}

    def setex(self, key, _ttl, value):
        self.values[key] = value

    def eval(self, _script, _numkeys, key):
        return self.values.pop(key, None)


def test_impersonation_token_is_single_use_and_sets_target_session_cookie():
    redis = FakeRedis()
    target = {
        "username": "alice",
        "displayName": "Alice",
        "role": "operator",
        "enabled": True,
    }
    with (
        patch.object(auth, "redis_connection", return_value=redis),
        patch.object(auth, "require_existing_account", return_value=target),
        patch.object(auth.secrets, "token_urlsafe", return_value="one-time-token"),
    ):
        created = auth.create_impersonation_token(
            auth.ImpersonationRequest(username="alice"),
            {"username": "superadmin", "role": "superadmin"},
        )
        assert "one-time-token" in created["path"]

        first = auth.consume_impersonation_token(
            auth.ImpersonationConsumeRequest(token="one-time-token"),
        )
        assert first["session"]["username"] == "alice"
        assert first["sessionToken"]

        with pytest.raises(HTTPException, match="无效或已过期"):
            auth.consume_impersonation_token(
                auth.ImpersonationConsumeRequest(token="one-time-token")
            )


def test_impersonation_consume_rejects_user_disabled_after_token_issue():
    redis = FakeRedis()
    enabled_target = {
        "username": "alice",
        "displayName": "Alice",
        "role": "operator",
        "enabled": True,
    }
    with (
        patch.object(auth, "redis_connection", return_value=redis),
        patch.object(
            auth,
            "require_existing_account",
            side_effect=[enabled_target, RuntimeError("用户不存在或已停用。")],
        ),
        patch.object(auth.secrets, "token_urlsafe", return_value="one-time-token"),
    ):
        auth.create_impersonation_token(
            auth.ImpersonationRequest(username="alice"),
            {"username": "superadmin", "role": "superadmin"},
        )

        with pytest.raises(HTTPException, match="目标用户已不存在或凭证无效"):
            auth.consume_impersonation_token(
                auth.ImpersonationConsumeRequest(token="one-time-token")
            )
