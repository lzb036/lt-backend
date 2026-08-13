from __future__ import annotations

from unittest.mock import patch

from app.api import crawler as crawler_api


def test_superadmin_store_list_reveals_credentials() -> None:
    with (
        patch.object(
            crawler_api,
            "require_existing_account",
            return_value={"username": "operator", "role": "operator"},
        ),
        patch.object(
            crawler_api.crawler_service,
            "list_stores",
            return_value=[],
        ) as list_stores,
    ):
        crawler_api.list_stores(
            page=1,
            pageSize=30,
            ownerUsername="operator",
            user={
                "username": "superadmin",
                "role": "superadmin",
            },
        )

    list_stores.assert_called_once_with(
        "operator",
        page=1,
        page_size=30,
        reveal=True,
    )


def test_operator_store_list_keeps_credentials_hidden() -> None:
    with patch.object(
        crawler_api.crawler_service,
        "list_stores",
        return_value=[],
    ) as list_stores:
        crawler_api.list_stores(
            page=1,
            pageSize=30,
            ownerUsername=None,
            user={
                "username": "operator",
                "role": "operator",
            },
        )

    list_stores.assert_called_once_with(
        "operator",
        page=1,
        page_size=30,
        reveal=False,
    )


def test_superadmin_single_store_verify_reveals_credentials() -> None:
    with (
        patch.object(
            crawler_api,
            "require_existing_account",
            return_value={"username": "operator", "role": "operator"},
        ),
        patch.object(
            crawler_api.crawler_service,
            "verify_store",
            return_value={"id": 7},
        ) as verify_store,
    ):
        result = crawler_api.verify_store(
            store_id=7,
            ownerUsername="operator",
            user={
                "username": "superadmin",
                "role": "superadmin",
            },
        )

    assert result == {"store": {"id": 7}}
    verify_store.assert_called_once_with(
        "operator",
        7,
        reveal=True,
    )


def test_operator_single_store_verify_keeps_credentials_hidden() -> None:
    with patch.object(
        crawler_api.crawler_service,
        "verify_store",
        return_value={"id": 7},
    ) as verify_store:
        result = crawler_api.verify_store(
            store_id=7,
            ownerUsername=None,
            user={
                "username": "operator",
                "role": "operator",
            },
        )

    assert result == {"store": {"id": 7}}
    verify_store.assert_called_once_with(
        "operator",
        7,
        reveal=False,
    )
