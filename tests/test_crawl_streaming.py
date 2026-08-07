from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import CrawlTaskModel, UserAccountModel
from app.services import crawler_service


def mysql_lock_wait_error() -> OperationalError:
    return OperationalError(
        "INSERT INTO lt_products ...",
        {},
        RuntimeError(1205, "Lock wait timeout exceeded; try restarting transaction"),
    )


def mysql_deadlock_error() -> OperationalError:
    return OperationalError(
        "INSERT INTO lt_products ...",
        {},
        RuntimeError(1213, "Deadlock found when trying to get lock; try restarting transaction"),
    )


def test_save_collected_item_retries_mysql_lock_wait_with_new_attempts() -> None:
    item = {
        "title": "商品 1",
        "source_url": "https://item.rakuten.co.jp/shop/item-1/",
    }
    with (
        patch.object(
            crawler_service,
            "save_collected_item",
            side_effect=[
                mysql_lock_wait_error(),
                mysql_lock_wait_error(),
                {"saved": True, "skipped": False, "error": ""},
            ],
        ) as save_item,
        patch.object(crawler_service, "raise_if_task_cancelled") as cancel_check,
        patch.object(crawler_service.time, "sleep") as sleep,
    ):
        result = crawler_service.save_collected_item_with_lock_retry(
            "alice",
            "task-id",
            item,
        )

    assert result["saved"] is True
    assert save_item.call_count == 3
    assert cancel_check.call_count == 2
    assert [call.args[0] for call in sleep.call_args_list] == [0.5, 1.0]


def test_save_collected_item_marks_only_item_failed_after_lock_retries_exhausted() -> None:
    item = {
        "title": "商品 1",
        "source_url": "https://item.rakuten.co.jp/shop/item-1/",
    }
    with (
        patch.object(
            crawler_service,
            "save_collected_item",
            side_effect=mysql_lock_wait_error(),
        ) as save_item,
        patch.object(crawler_service, "raise_if_task_cancelled"),
        patch.object(crawler_service.time, "sleep"),
    ):
        result = crawler_service.save_collected_item_with_lock_retry(
            "alice",
            "task-id",
            item,
        )

    assert result["saved"] is False
    assert result["skipped"] is False
    assert "重试 4 次" in result["error"]
    assert save_item.call_count == 4


def test_save_collected_item_retries_mysql_deadlock() -> None:
    item = {
        "title": "商品 1",
        "source_url": "https://item.rakuten.co.jp/shop/item-1/",
    }
    with (
        patch.object(
            crawler_service,
            "save_collected_item",
            side_effect=[
                mysql_deadlock_error(),
                {"saved": True, "skipped": False, "error": ""},
            ],
        ) as save_item,
        patch.object(crawler_service, "raise_if_task_cancelled") as cancel_check,
        patch.object(crawler_service.time, "sleep") as sleep,
    ):
        result = crawler_service.save_collected_item_with_lock_retry(
            "alice",
            "task-id",
            item,
        )

    assert result["saved"] is True
    assert save_item.call_count == 2
    cancel_check.assert_called_once()
    sleep.assert_called_once_with(0.5)


def test_save_collected_item_does_not_retry_other_database_errors() -> None:
    item = {
        "title": "商品 1",
        "source_url": "https://item.rakuten.co.jp/shop/item-1/",
    }
    database_error = OperationalError(
        "INSERT INTO lt_products ...",
        {},
        RuntimeError(2006, "MySQL server has gone away"),
    )
    with (
        patch.object(
            crawler_service,
            "save_collected_item",
            side_effect=database_error,
        ) as save_item,
        patch.object(crawler_service.time, "sleep") as sleep,
    ):
        try:
            crawler_service.save_collected_item_with_lock_retry(
                "alice",
                "task-id",
                item,
            )
        except OperationalError as exc:
            assert exc is database_error
        else:
            raise AssertionError("Expected OperationalError")

    save_item.assert_called_once()
    sleep.assert_not_called()


def test_listing_progress_total_prefers_limit_until_actual_total_is_lower() -> None:
    assert crawler_service.listing_progress_total_count(
        ranking_total=None,
        requested_limit=3000,
        collected_count=30,
    ) == 3000
    assert crawler_service.listing_progress_total_count(
        ranking_total=5000,
        requested_limit=3000,
        collected_count=30,
    ) == 3000
    assert crawler_service.listing_progress_total_count(
        ranking_total=2000,
        requested_limit=3000,
        collected_count=30,
    ) == 2000
    assert crawler_service.listing_progress_total_count(
        ranking_total=5000,
        requested_limit=None,
        collected_count=30,
    ) == 5000


def test_single_product_collection_does_not_apply_task_price_rule() -> None:
    with (
        patch.object(
            crawler_service,
            "normalize_rakuten_product_targets",
            return_value=["https://item.rakuten.co.jp/shop/item-1/"],
        ),
        patch.object(crawler_service, "crawl_price_rule_for_task") as price_rule,
        patch.object(
            crawler_service,
            "iter_enriched_collected_items_with_detail",
            return_value=iter(()),
        ) as enrich,
        patch.object(crawler_service, "raise_if_task_cancelled"),
    ):
        plan = crawler_service.collect_item_plan_for_target(
            "product_url",
            "https://item.rakuten.co.jp/shop/item-1/",
            task_id="task-id",
        )

    assert plan.total_count == 1
    price_rule.assert_not_called()
    assert enrich.call_args.kwargs["price_rule"] == {"operator": "all"}


def test_requested_limit_uses_actual_discovered_item_count() -> None:
    listing_items = [
        {
            "title": f"商品 {index}",
            "source_url": f"https://item.rakuten.co.jp/shop/item-{index}/",
        }
        for index in range(2000)
    ]

    with (
        patch.object(crawler_service, "crawl_price_rule_for_task", return_value={}),
        patch.object(
            crawler_service,
            "collect_whole_shop_listing_items",
            return_value=listing_items,
        ) as collect_listing,
        patch.object(
            crawler_service,
            "existing_collected_source_hashes_for_task",
            return_value=set(),
        ),
        patch.object(
            crawler_service,
            "iter_enriched_collected_items_with_detail",
            side_effect=lambda items, **_: iter(items),
        ),
    ):
        plan = crawler_service.collect_item_plan_for_target(
            "whole_shop",
            "整店:415734 全店采集 前 3000",
        )

    assert plan.total_count == 2000
    assert len(list(plan.items)) == 2000
    collect_listing.assert_called_once_with(
        "415734",
        "all",
        {},
        3000,
        task_id=None,
    )


def test_detail_collection_yields_completed_items_with_bounded_concurrency() -> None:
    items = [
        {
            "title": f"商品 {index}",
            "source_url": f"https://item.rakuten.co.jp/shop/item-{index}/",
        }
        for index in range(8)
    ]
    lock = threading.Lock()
    release_fast_workers = threading.Event()
    release_slow_worker = threading.Event()
    started_workers = threading.Event()
    active_count = 0
    maximum_active_count = 0

    def enrich(item, **_kwargs):
        nonlocal active_count, maximum_active_count
        with lock:
            active_count += 1
            maximum_active_count = max(maximum_active_count, active_count)
            if active_count >= 3:
                started_workers.set()
        if item["title"] == "商品 0":
            release_slow_worker.wait(timeout=2)
        else:
            release_fast_workers.wait(timeout=2)
        with lock:
            active_count -= 1
        return item

    def release_fast_items() -> None:
        assert started_workers.wait(timeout=2)
        release_fast_workers.set()

    release_thread = threading.Thread(target=release_fast_items)
    release_thread.start()
    try:
        with (
            patch.object(crawler_service.settings, "crawler_detail_workers", 3),
            patch.object(
                crawler_service,
                "enrich_collected_item_with_detail",
                side_effect=enrich,
            ),
            patch.object(crawler_service, "raise_if_task_cancelled"),
        ):
            result_iterator = crawler_service.iter_enriched_collected_items_with_detail(
                items
            )
            first_result = next(result_iterator)
            assert first_result["title"] != "商品 0"
            release_slow_worker.set()
            result = [first_result, *result_iterator]
    finally:
        release_fast_workers.set()
        release_slow_worker.set()
        release_thread.join(timeout=2)

    assert maximum_active_count == 3
    assert {item["source_url"] for item in result} == {
        item["source_url"] for item in items
    }


def test_run_task_saves_and_updates_progress_before_requesting_next_item() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)

    @contextmanager
    def local_session_scope():
        session = factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    with local_session_scope() as session:
        session.add(
            UserAccountModel(
                username="alice",
                display_name="Alice",
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        session.add(
            CrawlTaskModel(
                id="stream-task",
                owner_username="alice",
                source_type="whole_shop",
                target="整店:415734 全店采集 前 3000",
                status="queued",
                mode="manual",
                created_at=datetime.now(),
            )
        )

    events: list[str] = []
    progress_snapshots: list[dict[str, object]] = []

    def item_stream():
        events.append("yield-1")
        yield {
            "title": "商品 1",
            "source_url": "https://item.rakuten.co.jp/shop/item-1/",
        }
        events.append("yield-2")
        yield {
            "title": "商品 2",
            "source_url": "https://item.rakuten.co.jp/shop/item-2/",
        }

    def save_item(_owner, _task_id, item, **_kwargs):
        events.append(f"save-{item['title'][-1]}")
        return {"saved": True, "skipped": False, "error": ""}

    def update_progress(_model, _task_id, **kwargs):
        progress_snapshots.append(dict(kwargs))
        processed = int(kwargs.get("success_count") or 0) + int(
            kwargs.get("failed_count") or 0
        )
        if processed:
            events.append(f"progress-{processed}")

    with (
        patch.object(crawler_service, "session_scope", local_session_scope),
        patch.object(crawler_service, "should_use_redis_task_queue", return_value=False),
        patch.object(
            crawler_service,
            "collect_item_plan",
            return_value=crawler_service.CollectedItemPlan(2, item_stream()),
        ),
        patch.object(crawler_service, "active_sensitive_words", return_value=[]),
        patch.object(
            crawler_service,
            "collection_genre_policy_snapshot",
            return_value=crawler_service.CollectionGenrePolicySnapshot(),
        ),
        patch.object(
            crawler_service,
            "save_collected_item_with_lock_retry",
            side_effect=save_item,
        ),
        patch.object(crawler_service, "update_task_progress", side_effect=update_progress),
        patch.object(crawler_service, "log_event"),
        patch.object(crawler_service, "raise_if_task_cancelled"),
    ):
        crawler_service.run_task("stream-task")

    assert events == [
        "yield-1",
        "save-1",
        "progress-1",
        "yield-2",
        "save-2",
        "progress-2",
    ]
    assert progress_snapshots[0]["total_count"] == 2
    assert progress_snapshots[0]["success_count"] == 0
    assert progress_snapshots[-1]["success_count"] == 2
    assert "2 / 2" in str(progress_snapshots[-1]["message"])

    with local_session_scope() as session:
        task = session.get(CrawlTaskModel, "stream-task")
        assert task is not None
        assert task.status == "success"
        assert task.total_count == 2
        assert task.success_count == 2
        assert task.saved_count == 2
