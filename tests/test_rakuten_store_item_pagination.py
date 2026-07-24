from __future__ import annotations

from types import SimpleNamespace

from app.services import crawler_service


def rakuten_item(manage_number: str, *, hidden: bool = False) -> dict:
    return {
        "manageNumber": manage_number,
        "title": f"Product {manage_number}",
        "hideItem": hidden,
    }


def test_store_item_fetch_uses_raw_results_to_continue_pagination(monkeypatch):
    pages = {
        0: {
            "offset": 0,
            "numFound": 5,
            "results": [
                {"item": rakuten_item("item-1")},
                {"item": {"manageNumber": "deleted-index-entry"}},
            ],
        },
        2: {
            "offset": 2,
            "numFound": 5,
            "results": [
                {"item": rakuten_item("item-2")},
                {"item": rakuten_item("item-3", hidden=True)},
            ],
        },
        4: {
            "offset": 4,
            "numFound": 5,
            "results": [{"item": rakuten_item("item-4")}],
        },
    }
    requested_offsets: list[int] = []

    monkeypatch.setattr(crawler_service, "RAKUTEN_ITEM_SEARCH_HITS", 2)

    def fake_request(_headers, offset):
        requested_offsets.append(offset)
        return pages[offset]

    monkeypatch.setattr(crawler_service, "request_rakuten_items_page", fake_request)

    items, total_count = crawler_service.fetch_rakuten_store_items_with_total("secret", "key")

    assert requested_offsets == [0, 2, 4]
    assert [item["manageNumber"] for item in items] == ["item-1", "item-2", "item-3", "item-4"]
    assert total_count == 4


def test_store_product_counts_use_only_readable_items():
    row = SimpleNamespace(
        rakuten_product_total_count=None,
        rakuten_product_listed_count=None,
        rakuten_product_unlisted_count=None,
        rakuten_product_total_exceeds_limit=False,
        last_checked_at=None,
    )
    items = [
        rakuten_item("listed"),
        rakuten_item("unlisted", hidden=True),
    ]

    crawler_service.apply_store_product_counts(row, items)

    assert row.rakuten_product_total_count == 2
    assert row.rakuten_product_listed_count == 1
    assert row.rakuten_product_unlisted_count == 1
    assert row.rakuten_product_total_exceeds_limit is False
