from __future__ import annotations

from contextlib import contextmanager
import json
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core import task_queue
from app.db.database import Base
from app.db.models import ListingTaskModel, ProductModel, StoreModel, SyncTaskModel, UserAccountModel
from app.services import crawler_service


@pytest.fixture()
def session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    try:
        yield factory
    finally:
        engine.dispose()


@pytest.fixture()
def local_session_scope(monkeypatch, session_factory):
    @contextmanager
    def scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(crawler_service, "session_scope", scope)
    return scope


def seed_listing_products(session_factory):
    with session_factory() as session:
        session.add(
            UserAccountModel(
                username="alice",
                display_name="Alice",
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        store = StoreModel(
            owner_username="alice",
            store_code="alice-store",
            store_name="Alice Store",
            alias_name="Alice",
            enabled=True,
            rakuten_service_secret_encrypted="secret",
            rakuten_license_key_encrypted="key",
        )
        session.add(store)
        session.flush()
        source = ProductModel(
            owner_username="alice",
            title="Source",
            source_url="https://item.rakuten.co.jp/source/item/",
            source_url_hash="source-hash",
            image_url="https://example.com/1.jpg",
            genre_id="123456",
            review_status="listed_master",
            raw_payload_json=json.dumps(
                {
                    "images": [
                        "https://example.com/1.jpg",
                        "https://example.com/2.jpg",
                    ],
                    "descriptions": [
                        {
                            "label": "商品説明",
                            "value": '<img src="https://example.com/detail.jpg">',
                        }
                    ],
                    "variants": {
                        "sku-1": {
                            "standardPrice": 1000,
                        }
                    },
                }
            ),
        )
        session.add(source)
        session.flush()
        listed = ProductModel(
            owner_username="alice",
            parent_product_id=source.id,
            store_id=store.id,
            title="Source",
            source_url="https://item.rakuten.co.jp/alice-store/item/",
            source_url_hash="listed-hash",
            image_url="https://image.rakuten.co.jp/alice-store/cabinet/p1.jpg",
            genre_id="123456",
            review_status="listed",
            rakuten_manage_number="manage-1",
            item_number="manage-1",
            raw_payload_json=json.dumps(
                {
                    "images": [
                        {
                            "location": "/folder/p1.jpg",
                            "sourceUrl": "https://example.com/1.jpg",
                        }
                    ],
                    "listingImageCompletion": {"status": "queued"},
                }
            ),
        )
        session.add(listed)
        session.commit()
        return int(store.id), int(source.id), int(listed.id)


def test_legacy_listing_image_upload_uses_unified_listing_queue() -> None:
    listing_queue_name = crawler_service.settings.task_queue_listing_name

    assert crawler_service.sync_task_queue_kind("listing_image_upload") == "listing"
    assert task_queue.task_queue_name_for_kind("listing-image-upload") == listing_queue_name
    assert task_queue.all_task_queue_names().count(listing_queue_name) == 1
    assert "lt-tasks-listing-image-upload" not in task_queue.all_task_queue_names()


def test_cabinet_system_error_107_is_retried(monkeypatch) -> None:
    transient = Mock(
        status_code=200,
        text="<result><systemStatus>NG</systemStatus><message>SystemError(107)</message></result>",
    )
    success = Mock(
        status_code=200,
        text="<result><systemStatus>OK</systemStatus></result>",
    )
    transient.close.return_value = None
    monkeypatch.setattr(crawler_service, "throttle_rakuten_cabinet_request", lambda *_args: None)
    monkeypatch.setattr(crawler_service, "mark_rakuten_cabinet_qps_limited", lambda *_args: None)
    monkeypatch.setattr(crawler_service.time, "sleep", lambda _seconds: None)
    request = Mock(side_effect=[transient, success])
    monkeypatch.setattr(crawler_service.requests, "request", request)

    result = crawler_service.rakuten_cabinet_request("GET", "https://example.com")

    assert result is success
    assert request.call_count == 2
    transient.close.assert_called_once()


def test_cabinet_upstream_connection_error_is_retried(monkeypatch) -> None:
    transient = Mock(
        status_code=503,
        text=(
            "upstream connect error or disconnect/reset before headers. "
            "reset reason: connection termination"
        ),
    )
    success = Mock(
        status_code=200,
        text="<result><systemStatus>OK</systemStatus></result>",
    )
    transient.close.return_value = None
    monkeypatch.setattr(crawler_service, "throttle_rakuten_cabinet_request", lambda *_args: None)
    qps_limited = Mock()
    monkeypatch.setattr(crawler_service, "mark_rakuten_cabinet_qps_limited", qps_limited)
    monkeypatch.setattr(crawler_service.time, "sleep", lambda _seconds: None)
    request = Mock(side_effect=[transient, success])
    monkeypatch.setattr(crawler_service.requests, "request", request)

    result = crawler_service.rakuten_cabinet_request("GET", "https://example.com")

    assert result is success
    assert request.call_count == 2
    transient.close.assert_called_once()
    qps_limited.assert_not_called()


def test_cabinet_http_502_is_retried(monkeypatch) -> None:
    transient = Mock(status_code=502, text="Bad Gateway")
    success = Mock(status_code=200, text="<result><systemStatus>OK</systemStatus></result>")
    transient.close.return_value = None
    monkeypatch.setattr(crawler_service, "throttle_rakuten_cabinet_request", lambda *_args: None)
    monkeypatch.setattr(crawler_service.time, "sleep", lambda _seconds: None)
    request = Mock(side_effect=[transient, success])
    monkeypatch.setattr(crawler_service.requests, "request", request)

    result = crawler_service.rakuten_cabinet_request("GET", "https://example.com")

    assert result is success
    assert request.call_count == 2
    transient.close.assert_called_once()


def test_create_listing_image_upload_task_is_persisted_and_dispatched(
    monkeypatch,
    session_factory,
    local_session_scope,
) -> None:
    store_id, _source_id, listed_id = seed_listing_products(session_factory)
    dispatched: list[tuple[str, str, str, float]] = []
    monkeypatch.setattr(
        crawler_service,
        "dispatch_sync_task",
        lambda owner, task_id, *, task_type, delay_seconds=0: dispatched.append(
            (owner, task_id, task_type, delay_seconds)
        ),
    )

    task_id = crawler_service.create_listing_image_upload_task(
        "alice",
        store_id,
        "listing-task",
        [listed_id],
    )

    assert task_id is not None
    with session_factory() as session:
        task = session.get(SyncTaskModel, task_id)
        listed = session.get(ProductModel, listed_id)
        assert task is not None
        assert task.task_type == "listing_image_upload"
        assert json.loads(task.payload_json)["listingTaskId"] == "listing-task"
        assert crawler_service.product_raw_payload(listed)["listingImageCompletion"]["taskId"] == task_id
    assert dispatched == [
        ("alice", task_id, "listing_image_upload", crawler_service.TASK_START_RETRY_DELAY_SECONDS)
    ]


def test_image_upload_waits_while_same_store_listing_is_active(
    session_factory,
    local_session_scope,
) -> None:
    store_id, _source_id, listed_id = seed_listing_products(session_factory)
    with session_factory() as session:
        session.add(
            ListingTaskModel(
                id="listing-task",
                owner_username="alice",
                store_id=store_id,
                task_name="Listing",
                status="queued",
                product_ids_json=json.dumps(
                    {
                        "productIds": [1],
                        "storeIds": [store_id],
                    }
                ),
            )
        )
        image_task = SyncTaskModel(
            id="image-task",
            owner_username="alice",
            store_id=store_id,
            store_name="Alice",
            task_name="Images",
            task_type="listing_image_upload",
            payload_json=json.dumps({"productIds": [listed_id]}),
            status="queued",
        )
        session.add(image_task)
        session.commit()

    with session_factory() as session:
        image_task = session.get(SyncTaskModel, "image-task")
        reason = crawler_service.specialized_sync_task_wait_reason(session, image_task)

    assert "优先等待" in reason


def test_running_image_upload_pauses_when_new_listing_arrives(monkeypatch) -> None:
    active_states = iter([True, True, False])
    checks: list[int] = []
    monkeypatch.setattr(
        crawler_service,
        "listing_creation_active_for_store",
        lambda store_id: checks.append(store_id) is None and next(active_states),
    )
    monkeypatch.setattr(crawler_service, "is_task_cancel_requested", lambda *_args: False)
    monkeypatch.setattr(crawler_service.time, "sleep", lambda _seconds: None)

    crawler_service.wait_for_listing_creation_priority(7, task_id="image-task")

    assert checks == [7, 7, 7]


def test_image_upload_adds_remaining_main_and_description_images(
    monkeypatch,
    session_factory,
    local_session_scope,
) -> None:
    store_id, _source_id, listed_id = seed_listing_products(session_factory)
    upload_calls: list[dict] = []

    def upload_main(*_args, **kwargs):
        upload_calls.append(kwargs)
        return [
            {
                "location": "/folder/p2.jpg",
                "sourceUrl": "https://example.com/2.jpg",
            }
        ]

    monkeypatch.setattr(crawler_service, "upload_product_images_to_rakuten", upload_main)
    monkeypatch.setattr(
        crawler_service,
        "upload_product_description_images_to_rakuten",
        lambda *_args, **_kwargs: {
            "rawPayload": {
                "variants": {"sku-1": {"standardPrice": 1000}},
                "descriptions": [{"label": "商品説明", "value": "updated"}],
            },
            "uploadedImages": [
                {
                    "location": "/folder/d1.jpg",
                    "sourceUrl": "https://example.com/detail.jpg",
                }
            ],
        },
    )
    monkeypatch.setattr(
        crawler_service,
        "build_rakuten_item_upsert_payload",
        lambda *_args, **_kwargs: {
            "itemNumber": "manage-1",
            "images": [],
            "variants": {"sku-1": {"standardPrice": 1000}},
        },
    )
    monkeypatch.setattr(
        crawler_service,
        "put_rakuten_item_with_attribute_retry",
        lambda _secret, _key, _manage, payload: payload,
    )
    monkeypatch.setattr(
        crawler_service,
        "build_rakuten_cabinet_image_url",
        lambda _shop, location: f"https://image.example{location}",
    )
    monkeypatch.setattr(crawler_service, "rollback_uploaded_listing_images", lambda *_args: "")

    crawler_service.enrich_listed_product_images(
        "alice",
        store_id,
        listed_id,
        "secret",
        "key",
        task_id="image-task",
    )

    assert upload_calls[0]["source_images"] == ["https://example.com/2.jpg"]
    assert upload_calls[0]["start_index"] == 2
    with session_factory() as session:
        listed = session.get(ProductModel, listed_id)
        raw = crawler_service.product_raw_payload(listed)
        assert len(raw["images"]) == 2
        assert len(raw["descriptionImages"]) == 1
        assert raw["listingImageCompletion"]["status"] == "success"
