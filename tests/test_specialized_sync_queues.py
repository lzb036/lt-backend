from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta
import json

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.dialects import mysql
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import ProductModel, StoreModel, SyncTaskModel, UserAccountModel
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


def install_session_scope(monkeypatch, session_factory):
    @contextmanager
    def local_session_scope():
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)


def seed_users(session_factory, *usernames: str) -> None:
    with session_factory() as session:
        session.add_all(
            UserAccountModel(
                username=username,
                display_name=username,
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
            for username in usernames
        )
        session.commit()


def add_task(
    session_factory,
    *,
    task_id: str,
    owner: str = "alice",
    task_type: str = "store_sync",
    status: str = "queued",
    store_id: int | None = None,
    payload: dict | None = None,
    created_at: datetime | None = None,
) -> None:
    with session_factory() as session:
        task = SyncTaskModel(
            id=task_id,
            owner_username=owner,
            store_id=store_id,
            task_name=task_id,
            task_type=task_type,
            status=status,
            payload_json=json.dumps(payload or {}, ensure_ascii=False),
        )
        if created_at is not None:
            task.created_at = created_at
            task.updated_at = created_at
        session.add(task)
        session.commit()


def seed_store_products(session_factory, *, product_count: int = 2) -> list[int]:
    with session_factory() as session:
        store = StoreModel(
            owner_username="alice",
            store_code="shop",
            store_name="Shop",
            enabled=True,
            rakuten_service_secret_encrypted="secret",
            rakuten_license_key_encrypted="key",
        )
        session.add(store)
        session.flush()
        products = [
            ProductModel(
                owner_username="alice",
                store_id=store.id,
                title=f"Product {index}",
                source_url=f"https://example.com/products/{index}",
                source_url_hash=f"hash-{index}",
                review_status="listed",
            )
            for index in range(product_count)
        ]
        session.add_all(products)
        session.flush()
        product_ids = [int(product.id) for product in products]
        session.commit()
        return product_ids


@pytest.mark.parametrize(
    ("task_type", "expected_kind"),
    [
        ("store_sync", "sync"),
        ("product_delete", "sync"),
        ("title_optimization", "title-optimization"),
        ("deleted_product_image_cleanup", "image-cleanup"),
    ],
)
def test_sync_task_queue_kind(task_type, expected_kind):
    assert crawler_service.sync_task_queue_kind(task_type) == expected_kind


def test_dispatch_sync_task_routes_each_task_type(monkeypatch):
    queued = []
    monkeypatch.setattr(crawler_service, "should_use_redis_task_queue", lambda: True)
    monkeypatch.setattr(
        crawler_service,
        "task_queue_name_for_kind",
        lambda kind: f"queue:{kind}",
    )
    monkeypatch.setattr(
        crawler_service,
        "enqueue_task",
        lambda *args, **kwargs: queued.append((args, kwargs)),
    )

    crawler_service.dispatch_sync_task("alice", "sync-1", task_type="product_delete")
    crawler_service.dispatch_sync_task(
        "alice",
        "title-1",
        task_type="title_optimization",
    )
    crawler_service.dispatch_sync_task(
        "alice",
        "image-1",
        task_type="deleted_product_image_cleanup",
    )

    assert [item[1]["queue_name"] for item in queued] == [
        "queue:sync",
        "queue:title-optimization",
        "queue:image-cleanup",
    ]


def test_title_creation_dispatches_directly_to_specialized_queue(
    monkeypatch,
    session_factory,
):
    install_session_scope(monkeypatch, session_factory)
    seed_users(session_factory, "alice")
    product_ids = seed_store_products(session_factory)
    dispatched = []
    monkeypatch.setattr(
        crawler_service,
        "dispatch_sync_task",
        lambda owner, task_id, **kwargs: dispatched.append((owner, task_id, kwargs)),
    )
    monkeypatch.setattr(
        crawler_service,
        "dispatch_next_sync_task",
        lambda: (_ for _ in ()).throw(
            AssertionError("title tasks must not use the normal dispatcher")
        ),
    )

    result = crawler_service.create_product_title_optimization_task(
        "alice",
        product_ids,
    )

    assert len(result["syncTasks"]) == 1
    assert len(dispatched) == 1
    assert dispatched[0][0] == "alice"
    assert dispatched[0][2] == {"task_type": "title_optimization"}


def test_product_delete_creation_stays_on_normal_dispatcher(
    monkeypatch,
    session_factory,
):
    install_session_scope(monkeypatch, session_factory)
    seed_users(session_factory, "alice")
    product_ids = seed_store_products(session_factory)
    normal_dispatches = []
    monkeypatch.setattr(
        crawler_service,
        "dispatch_next_sync_task",
        lambda: normal_dispatches.append(True),
    )
    monkeypatch.setattr(
        crawler_service,
        "dispatch_sync_task",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("product delete must stay on the normal dispatcher")
        ),
    )

    result = crawler_service.create_product_delete_sync_task(
        "alice",
        product_ids,
    )

    assert len(result["syncTasks"]) == 1
    assert normal_dispatches == [True]


def test_normal_dispatcher_skips_specialized_tasks(
    monkeypatch,
    session_factory,
):
    install_session_scope(monkeypatch, session_factory)
    seed_users(session_factory, "alice")
    add_task(
        session_factory,
        task_id="title-first",
        task_type="title_optimization",
    )
    add_task(
        session_factory,
        task_id="sync-second",
        task_type="product_delete",
    )
    dispatched = []
    monkeypatch.setattr(
        crawler_service,
        "finalize_stale_cancel_requested_tasks",
        lambda *args, **kwargs: 0,
    )
    monkeypatch.setattr(
        crawler_service,
        "reconcile_interrupted_running_tasks",
        lambda *args, **kwargs: 0,
    )
    monkeypatch.setattr(crawler_service, "running_sync_task_count", lambda *args, **kwargs: 0)
    monkeypatch.setattr(crawler_service, "sync_task_has_active_background_job", lambda *args, **kwargs: False)
    monkeypatch.setattr(crawler_service, "running_store_task_count", lambda *args, **kwargs: 0)
    monkeypatch.setattr(
        crawler_service,
        "dispatch_sync_task",
        lambda owner, task_id, **kwargs: dispatched.append((owner, task_id, kwargs)),
    )

    crawler_service.dispatch_next_sync_task()

    assert dispatched == [
        ("alice", "sync-second", {"task_type": "product_delete"})
    ]


def test_specialized_wait_rules_are_scoped_by_user_and_store(
    session_factory,
):
    seed_users(session_factory, "alice", "bob")
    add_task(
        session_factory,
        task_id="title-running",
        owner="alice",
        task_type="title_optimization",
        status="running",
        store_id=1,
    )
    add_task(
        session_factory,
        task_id="title-waiting",
        owner="alice",
        task_type="title_optimization",
        store_id=2,
    )
    add_task(
        session_factory,
        task_id="title-other-user",
        owner="bob",
        task_type="title_optimization",
        store_id=3,
    )
    add_task(
        session_factory,
        task_id="image-running",
        owner="alice",
        task_type="deleted_product_image_cleanup",
        status="running",
        store_id=7,
    )
    add_task(
        session_factory,
        task_id="image-waiting",
        owner="bob",
        task_type="deleted_product_image_cleanup",
        store_id=7,
    )
    add_task(
        session_factory,
        task_id="image-other-store",
        owner="alice",
        task_type="deleted_product_image_cleanup",
        store_id=8,
    )

    with session_factory() as session:
        assert crawler_service.specialized_sync_task_wait_reason(
            session,
            session.get(SyncTaskModel, "title-waiting"),
        ) == "排队中，等待该用户当前标题优化任务完成"
        assert crawler_service.specialized_sync_task_wait_reason(
            session,
            session.get(SyncTaskModel, "title-other-user"),
        ) == ""
        assert crawler_service.specialized_sync_task_wait_reason(
            session,
            session.get(SyncTaskModel, "image-waiting"),
        ) == "排队中，等待该店铺当前图片清理任务完成"
        assert crawler_service.specialized_sync_task_wait_reason(
            session,
            session.get(SyncTaskModel, "image-other-store"),
        ) == ""


def test_specialized_running_task_reads_use_mysql_row_locks():
    title_query = (
        select(SyncTaskModel.id)
        .where(
            SyncTaskModel.owner_username == "alice",
            SyncTaskModel.task_type.in_(crawler_service.TITLE_OPTIMIZATION_TASK_TYPES),
            SyncTaskModel.status == "running",
        )
        .limit(1)
        .with_for_update()
    )
    image_query = (
        select(SyncTaskModel.id)
        .where(
            SyncTaskModel.store_id == 7,
            SyncTaskModel.task_type.in_(crawler_service.IMAGE_CLEANUP_TASK_TYPES),
            SyncTaskModel.status == "running",
        )
        .limit(1)
        .with_for_update()
    )

    assert "FOR UPDATE" in str(title_query.compile(dialect=mysql.dialect()))
    assert "FOR UPDATE" in str(image_query.compile(dialect=mysql.dialect()))


def test_task_groups_keep_normal_sync_separate(
    monkeypatch,
    session_factory,
):
    install_session_scope(monkeypatch, session_factory)
    seed_users(session_factory, "alice", "bob")
    add_task(session_factory, task_id="sync", task_type="store_sync")
    add_task(
        session_factory,
        task_id="title",
        task_type="title_optimization",
    )
    add_task(
        session_factory,
        task_id="image-alice",
        task_type="deleted_product_image_cleanup",
    )
    add_task(
        session_factory,
        task_id="image-bob",
        owner="bob",
        task_type="deleted_product_image_cleanup",
    )
    monkeypatch.setattr(crawler_service, "dispatch_next_sync_task_safely", lambda: None)
    monkeypatch.setattr(
        crawler_service,
        "finalize_stale_cancel_requested_tasks",
        lambda *args, **kwargs: 0,
    )
    monkeypatch.setattr(
        crawler_service,
        "reconcile_interrupted_running_tasks",
        lambda *args, **kwargs: 0,
    )

    normal = crawler_service.list_sync_tasks("alice", task_group="sync")
    titles = crawler_service.list_sync_tasks(
        "alice",
        task_group="title_optimization",
    )
    images = crawler_service.list_sync_tasks(
        "alice",
        task_group="image_cleanup",
        all_owners=True,
    )

    assert [task["id"] for task in normal] == ["sync"]
    assert [task["id"] for task in titles] == ["title"]
    assert {task["id"] for task in images} == {"image-alice", "image-bob"}


def test_title_retry_only_requeues_failed_and_unfinished_ids(
    monkeypatch,
    session_factory,
):
    install_session_scope(monkeypatch, session_factory)
    seed_users(session_factory, "alice")
    add_task(
        session_factory,
        task_id="title-retry",
        task_type="title_optimization",
        status="cancelled",
        payload={
            "productIds": [1, 2, 3, 4],
            "result": {
                "successIds": [1, 2],
                "failedIds": [3],
            },
        },
    )
    dispatched = []
    monkeypatch.setattr(
        crawler_service,
        "dispatch_sync_task",
        lambda owner, task_id, **kwargs: dispatched.append((owner, task_id, kwargs)),
    )

    crawler_service.retry_sync_task("alice", "title-retry")

    with session_factory() as session:
        task = session.get(SyncTaskModel, "title-retry")
        assert task.status == "queued"
        assert task.total_count == 2
        assert crawler_service.sync_task_payload(task)["productIds"] == [3, 4]
    assert dispatched == [
        ("alice", "title-retry", {"task_type": "title_optimization"})
    ]


def test_reconciliation_checks_specialized_queue_kinds(
    monkeypatch,
    session_factory,
):
    install_session_scope(monkeypatch, session_factory)
    seed_users(session_factory, "alice")
    stale_at = datetime.now() - timedelta(hours=5)
    add_task(
        session_factory,
        task_id="sync-running",
        task_type="store_sync",
        status="running",
        created_at=stale_at,
    )
    add_task(
        session_factory,
        task_id="title-running",
        task_type="title_optimization",
        status="running",
        created_at=stale_at,
    )
    add_task(
        session_factory,
        task_id="image-running",
        task_type="deleted_product_image_cleanup",
        status="running",
        created_at=stale_at,
    )
    calls = []
    monkeypatch.setattr(crawler_service, "should_use_redis_task_queue", lambda: True)

    def fake_states(task_ids, queue_kind, **kwargs):
        calls.append((set(task_ids), queue_kind))
        return {
            task_id: {"status": "started"}
            for task_id in task_ids
        }

    monkeypatch.setattr(crawler_service, "redis_task_states", fake_states)

    with session_factory() as session:
        assert crawler_service.reconcile_interrupted_running_tasks(
            session,
            SyncTaskModel,
        ) == 0

    assert set((frozenset(ids), kind) for ids, kind in calls) == {
        (frozenset({"sync-running"}), "sync"),
        (frozenset({"title-running"}), "title-optimization"),
        (frozenset({"image-running"}), "image-cleanup"),
    }
