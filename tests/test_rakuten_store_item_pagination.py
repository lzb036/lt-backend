from __future__ import annotations

from datetime import datetime
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


def test_store_item_fetch_shards_overflow_by_manage_number_year_and_month(monkeypatch):
    current_year = datetime.now().year
    monkeypatch.setattr(crawler_service, "RAKUTEN_ITEM_SEARCH_HITS", 2)
    monkeypatch.setattr(crawler_service, "RAKUTEN_ITEM_SEARCH_MAX_OFFSET", 2)
    monkeypatch.setattr(crawler_service, "RAKUTEN_ITEM_SEARCH_SHARD_YEAR_LOOKBACK", 1)
    monkeypatch.setattr(crawler_service, "RAKUTEN_ITEM_SEARCH_SHARD_YEAR_LOOKAHEAD", 1)
    requested: list[tuple[str, int]] = []

    def page(total: int, values: list[str], offset: int) -> dict:
        return {
            "offset": offset,
            "numFound": total,
            "results": [{"item": rakuten_item(value)} for value in values],
        }

    def fake_request(_headers, offset, *, manage_number=""):
        requested.append((manage_number, offset))
        if not manage_number:
            return page(7, ["root-item"], offset)
        if manage_number == str(current_year - 1):
            return page(2, ["old-1", "old-2"], offset)
        if manage_number == str(current_year):
            return page(5, [], offset)
        if manage_number == str(current_year + 1):
            return page(0, [], offset)
        if manage_number == f"{current_year}01":
            return page(2, ["jan-1", "jan-2"], offset)
        if manage_number == f"{current_year}02":
            return (
                page(3, ["feb-1", "feb-2"], offset)
                if offset == 0
                else page(3, ["feb-3"], offset)
            )
        return page(0, [], offset)

    monkeypatch.setattr(crawler_service, "request_rakuten_items_page", fake_request)

    items, total_count = crawler_service.fetch_rakuten_store_items_with_total("secret", "key")

    assert total_count == 7
    assert {
        item["manageNumber"]
        for item in items
    } == {"old-1", "old-2", "jan-1", "jan-2", "feb-1", "feb-2", "feb-3"}
    assert max(offset for _manage_number, offset in requested) <= 2
    assert (str(current_year), 0) in requested
    assert (f"{current_year}01", 0) in requested
    assert (f"{current_year}02", 2) in requested


def test_store_item_fetch_stops_when_shards_do_not_cover_total(monkeypatch):
    current_year = datetime.now().year
    monkeypatch.setattr(crawler_service, "RAKUTEN_ITEM_SEARCH_HITS", 2)
    monkeypatch.setattr(crawler_service, "RAKUTEN_ITEM_SEARCH_MAX_OFFSET", 2)
    monkeypatch.setattr(crawler_service, "RAKUTEN_ITEM_SEARCH_SHARD_YEAR_LOOKBACK", 1)
    monkeypatch.setattr(crawler_service, "RAKUTEN_ITEM_SEARCH_SHARD_YEAR_LOOKAHEAD", 1)

    def fake_request(_headers, offset, *, manage_number=""):
        if not manage_number:
            return {
                "offset": offset,
                "numFound": 7,
                "results": [{"item": rakuten_item("root-item")}],
            }
        if manage_number == str(current_year - 1):
            return {
                "offset": offset,
                "numFound": 2,
                "results": [{"item": rakuten_item("old-1")}],
            }
        if manage_number == str(current_year):
            return {"offset": offset, "numFound": 0, "results": []}
        return {"offset": offset, "numFound": 0, "results": []}

    monkeypatch.setattr(crawler_service, "request_rakuten_items_page", fake_request)

    try:
        crawler_service.fetch_rakuten_store_items_with_total("secret", "key")
    except RuntimeError as exc:
        assert "分片只读取到" in str(exc)
    else:
        raise AssertionError("incomplete shard coverage must stop synchronization")


def test_store_product_counts_use_only_readable_items():
    row = SimpleNamespace(
        rakuten_product_total_count=None,
        rakuten_product_listed_count=None,
        rakuten_product_unlisted_count=None,
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
