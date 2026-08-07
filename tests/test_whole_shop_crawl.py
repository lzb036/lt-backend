from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.routing import APIRoute
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.api import crawler as crawler_api
from app.db.database import Base
from app.db.models import CrawlTaskModel, UserAccountModel
from app.services import crawler_service


def search_page_html(*, total: int, review_count: int = 0) -> str:
    review = (
        f'<a href="https://review.rakuten.co.jp/item/1/415734_10000001/1.1/">'
        f'4.5 ({review_count}件)</a>'
        if review_count
        else ""
    )
    return f"""
    <html>
      <body>
        <h1>Bellissima</h1>
        <div>検索結果 1〜1件 （{total:,}件）</div>
        <div class="searchresultitem">
          <a href="https://item.rakuten.co.jp/bellissima-km/item-1/?variantId=1">商品一</a>
          {review}
        </div>
        <script type="application/ld+json">
          {{
            "@type": "ItemList",
            "itemListElement": [
              {{
                "item": {{
                  "@type": "Product",
                  "name": "商品一",
                  "url": "https://item.rakuten.co.jp/bellissima-km/item-1/"
                }}
              }}
            ]
          }}
        </script>
        <script>window.__INITIAL_STATE__ = {{"ichibaSearch": {{"pagination": {{"numFound": {total}}}}}}};</script>
      </body>
    </html>
    """


def test_parse_search_page_total_and_review_count() -> None:
    html = search_page_html(total=6287, review_count=3)

    assert crawler_service.parse_ranking_total_count(html) == 6287
    items = crawler_service.parse_search_items(
        html,
        "https://search.rakuten.co.jp/search/mall/?sid=415734",
    )

    assert len(items) == 1
    assert items[0]["source_url"] == "https://item.rakuten.co.jp/bellissima-km/item-1/"
    assert items[0]["raw"]["reviewCount"] == 3


def test_whole_shop_target_and_standard_search_urls() -> None:
    assert crawler_service.build_whole_shop_task_target("415734", "all") == "整店:415734 全店采集 全部"
    assert crawler_service.build_whole_shop_task_target("415734", "reviewed", 120) == "整店:415734 评论采集 前 120"
    assert crawler_service.parse_whole_shop_task_target("整店:415734 评论采集") == ("415734", "reviewed")
    assert crawler_service.parse_whole_shop_task_options("整店:415734 评论采集 前 120") == (
        "415734",
        "reviewed",
        120,
    )
    assert crawler_service.build_whole_shop_search_url("415734", "all") == (
        "https://search.rakuten.co.jp/search/mall/?sid=415734"
    )
    assert crawler_service.build_whole_shop_search_url("415734", "reviewed") == (
        "https://search.rakuten.co.jp/search/mall/?sid=415734&review=1"
    )
    assert crawler_service.build_whole_shop_search_url(
        "415734",
        "reviewed",
        price_min=2500,
        price_max=5000,
    ) == "https://search.rakuten.co.jp/search/mall/?sid=415734&review=1&min=2500&max=5000"
    assert crawler_service.whole_shop_price_bounds({"operator": "gte", "value": 2500}) == (
        2500,
        None,
    )
    assert crawler_service.whole_shop_price_bounds({"operator": "lt", "value": 2500}) == (
        None,
        2499,
    )


def test_preview_reads_exact_all_and_reviewed_counts_without_writes() -> None:
    all_html = search_page_html(total=6287)
    reviewed_html = search_page_html(total=342, review_count=1)

    def fetch(url: str) -> str:
        return reviewed_html if "review=1" in url else all_html

    with (
        patch.object(crawler_service, "fetch_listing_html", side_effect=fetch),
        patch.object(crawler_service, "session_scope") as session_scope,
    ):
        preview = crawler_service.preview_whole_shop_crawl(
            "alice",
            "https://search.rakuten.co.jp/search/mall/?sid=415734",
            "reviewed",
        )

    session_scope.assert_not_called()
    assert preview == {
        "valid": True,
        "shopName": "Bellissima",
        "sid": "415734",
        "filter": "reviewed",
        "totalFound": 6287,
        "collectableCount": 342,
        "reviewedCount": 342,
        "pageCount": 342,
        "message": "本次预计采集 342 个商品",
    }


def test_preview_applies_price_rule_to_collectable_count() -> None:
    all_html = search_page_html(total=6287)
    filtered_html = search_page_html(total=4210)

    def fetch(url: str) -> str:
        return filtered_html if "min=2500" in url else all_html

    with (
        patch.object(crawler_service, "fetch_listing_html", side_effect=fetch),
        patch.object(crawler_service, "session_scope") as session_scope,
    ):
        preview = crawler_service.preview_whole_shop_crawl(
            "alice",
            "415734",
            "all",
            {"operator": "gte", "value": 2500},
        )

    session_scope.assert_not_called()
    assert preview["totalFound"] == 6287
    assert preview["collectableCount"] == 4210
    assert preview["pageCount"] == 4210
    assert preview["message"] == "本次预计采集 4210 个商品"


def test_whole_shop_execution_uses_review_filter_and_shared_listing_collector() -> None:
    listing_items = [
        {
            "title": "商品一",
            "source_url": "https://item.rakuten.co.jp/bellissima-km/item-1/",
            "raw": {"reviewCount": 1},
        }
    ]
    with (
        patch.object(
            crawler_service,
            "crawl_price_rule_for_task",
            return_value={"operator": "gte", "value": 2500},
        ),
        patch.object(
            crawler_service,
            "collect_whole_shop_listing_items",
            return_value=listing_items,
        ) as collect_listing,
        patch.object(crawler_service, "existing_collected_source_hashes_for_task", return_value=set()),
        patch.object(
            crawler_service,
            "iter_enriched_collected_items_with_detail",
            return_value=iter(listing_items),
        ) as enrich,
        patch.object(crawler_service, "update_task_progress"),
        patch.object(crawler_service, "raise_if_task_cancelled"),
    ):
        items = crawler_service.collect_items_for_target(
            "whole_shop",
            "整店:415734 评论采集 前 120",
            task_id="task-id",
        )

    assert items == listing_items
    collect_listing.assert_called_once_with(
        "415734",
        "reviewed",
        {"operator": "gte", "value": 2500},
        120,
        task_id="task-id",
    )
    enrich.assert_called_once()


def test_large_whole_shop_collection_uses_price_partitions_and_deduplicates() -> None:
    partitions = [
        (2500, 3749, 4592),
        (3750, 4999, 3845),
        (5000, 9999, 5000),
        (10000, None, 1538),
    ]

    def collect(url: str, _limit: int | None, **_kwargs):
        suffix = url.split("min=", 1)[-1]
        return [
            {
                "title": suffix,
                "source_url": f"https://item.rakuten.co.jp/shop/{suffix}/",
            },
            {
                "title": "duplicate",
                "source_url": "https://item.rakuten.co.jp/shop/shared/",
            },
        ]

    with (
        patch.object(crawler_service, "whole_shop_search_count", return_value=(12886, 287)),
        patch.object(crawler_service, "whole_shop_price_partitions", return_value=partitions),
        patch.object(crawler_service, "collect_listing_items", side_effect=collect) as collect_items,
        patch.object(crawler_service, "update_task_progress") as update_progress,
    ):
        items = crawler_service.collect_whole_shop_listing_items(
            "415734",
            "all",
            {"operator": "gte", "value": 2500},
            None,
            task_id="task-id",
        )

    assert len(items) == 5
    assert collect_items.call_count == 4
    assert "min=2500&max=3749" in collect_items.call_args_list[0].args[0]
    assert "min=10000" in collect_items.call_args_list[-1].args[0]
    assert "max=" not in collect_items.call_args_list[-1].args[0]
    assert update_progress.call_count == 4
    assert update_progress.call_args.kwargs["total_count"] == 12886


def test_unbounded_price_rule_is_recursively_partitioned() -> None:
    totals = {
        "https://search.rakuten.co.jp/search/mall/?sid=415734&min=2500": 12886,
        "https://search.rakuten.co.jp/search/mall/?sid=415734&min=2500&max=4999": 7000,
        "https://search.rakuten.co.jp/search/mall/?sid=415734&min=2500&max=3749": 4000,
        "https://search.rakuten.co.jp/search/mall/?sid=415734&min=3750&max=4999": 3000,
        "https://search.rakuten.co.jp/search/mall/?sid=415734&min=5000": 5886,
        "https://search.rakuten.co.jp/search/mall/?sid=415734&min=5000&max=9999": 5000,
        "https://search.rakuten.co.jp/search/mall/?sid=415734&min=10000": 886,
    }

    def search_count(url: str) -> tuple[int, int]:
        return totals[url], 1

    with patch.object(crawler_service, "whole_shop_search_count", side_effect=search_count):
        partitions = crawler_service.whole_shop_price_partitions(
            "415734",
            "all",
            {"operator": "gte", "value": 2500},
        )

    assert partitions == [
        (2500, 3749, 4000),
        (3750, 4999, 3000),
        (5000, 9999, 5000),
        (10000, None, 886),
    ]


def test_create_whole_shop_task_persists_normalized_target() -> None:
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

    payload = SimpleNamespace(
        sourceId=None,
        scheduledCrawlId=None,
        sourceType="whole_shop",
        target="https://search.rakuten.co.jp/search/mall/?sid=415734",
        wholeShopFilter="reviewed",
        crawlLimit=120,
        crawlPriceRule={"operator": "range", "minPrice": 2500, "maxPrice": 5000},
        mode="manual",
    )
    with (
        patch.object(crawler_service, "session_scope", local_session_scope),
        patch.object(crawler_service, "should_use_redis_task_queue", return_value=True),
        patch.object(crawler_service, "dispatch_queued_crawl_tasks_safely"),
    ):
        task = crawler_service.create_task("alice", payload)

    with local_session_scope() as session:
        stored = session.scalar(select(CrawlTaskModel).where(CrawlTaskModel.id == task["id"]))

    assert stored is not None
    assert stored.source_type == "whole_shop"
    assert stored.target == "整店:415734 评论采集 前 120"
    assert stored.crawl_price_rule_json == '{"operator": "range", "minPrice": 2500, "maxPrice": 5000}'
    assert task["crawlPriceRule"] == {"operator": "range", "minPrice": 2500, "maxPrice": 5000}
    engine.dispose()


def test_task_price_rule_snapshot_overrides_user_global_setting() -> None:
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
                crawl_price_rule_json='{"operator": "gte", "value": 3800}',
            )
        )
        session.add(
            CrawlTaskModel(
                id="manual-task",
                owner_username="alice",
                source_type="shop",
                target="店铺:example 日榜 全部",
                crawl_price_rule_json='{"operator": "lte", "value": 2500}',
                mode="manual",
                status="queued",
            )
        )
        session.add(
            CrawlTaskModel(
                id="legacy-task",
                owner_username="alice",
                source_type="shop",
                target="店铺:example 日榜 全部",
                mode="manual",
                status="queued",
            )
        )

    with patch.object(crawler_service, "session_scope", local_session_scope):
        assert crawler_service.crawl_price_rule_for_task("manual-task") == {
            "operator": "lte",
            "value": 2500,
        }
        assert crawler_service.crawl_price_rule_for_task("legacy-task") == {
            "operator": "gte",
            "value": 3800,
        }

    engine.dispose()


def test_product_url_task_always_disables_price_filtering() -> None:
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
                crawl_price_rule_json='{"operator": "gte", "value": 3800}',
            )
        )

    payload = SimpleNamespace(
        sourceId=None,
        scheduledCrawlId=None,
        sourceType="product_url",
        target="https://item.rakuten.co.jp/example/item-1/",
        crawlPriceRule={"operator": "gte", "value": 5000},
        mode="manual",
    )
    with (
        patch.object(crawler_service, "session_scope", local_session_scope),
        patch.object(crawler_service, "should_use_redis_task_queue", return_value=True),
        patch.object(crawler_service, "dispatch_queued_crawl_tasks_safely"),
    ):
        task = crawler_service.create_task("alice", payload)

    with patch.object(crawler_service, "session_scope", local_session_scope):
        assert task["crawlPriceRule"] == {"operator": "all"}
        assert crawler_service.crawl_price_rule_for_task(task["id"]) == {"operator": "all"}

    engine.dispose()


def test_whole_shop_preview_route_uses_crawler_permission() -> None:
    route = next(
        route
        for route in crawler_api.router.routes
        if isinstance(route, APIRoute)
        and route.path == "/crawler/tasks/preview"
        and "POST" in route.methods
    )
    dependency_calls = [dependency.call for dependency in route.dependant.dependencies]
    assert crawler_api.require_crawler_permission in dependency_calls
