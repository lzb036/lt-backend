from __future__ import annotations

from app.services.user_service import (
    DEFAULT_USER_PERMISSIONS,
    SUPERADMIN_PERMISSIONS,
    fixed_permissions_for_role,
)


def test_operator_permissions_exclude_global_settings() -> None:
    assert fixed_permissions_for_role("operator") == DEFAULT_USER_PERMISSIONS
    assert "settings.manage" not in DEFAULT_USER_PERMISSIONS
    assert set(DEFAULT_USER_PERMISSIONS) == {
        "crawler.manage",
        "products.manage",
        "stores.manage",
        "ai.manage",
    }


def test_superadmin_permissions_keep_global_settings() -> None:
    assert fixed_permissions_for_role("superadmin") == SUPERADMIN_PERMISSIONS
    assert "settings.manage" in SUPERADMIN_PERMISSIONS
