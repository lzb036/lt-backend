from __future__ import annotations

from contextlib import contextmanager
import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import (
    ListingTaskModel,
    ProductModel,
    StoreModel,
    SyncTaskModel,
    UserAccountModel,
)
from app.services import crawler_service


@pytest.fixture()
def session_factory(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    factory = sessionmaker(
        bind=engine,
        expire_on_commit=False,
        future=True,
    )

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

    monkeypatch.setattr(crawler_service, "session_scope", local_session_scope)
    try:
        yield factory
    finally:
        engine.dispose()


def seed_owner_products(session_factory, owner: str, suffix: str):
    with session_factory() as session:
        session.add(
            UserAccountModel(
                username=owner,
                display_name=owner,
                password_salt_b64="salt",
                password_hash_b64="hash",
            )
        )
        store = StoreModel(
            owner_username=owner,
            store_code=f"store-{suffix}",
            store_name=f"Store {suffix}",
            enabled=True,
            rakuten_service_secret_encrypted="secret",
            rakuten_license_key_encrypted="key",
        )
        session.add(store)
        session.flush()
        parents = []
        children = []
        for index in range(2):
            parent = ProductModel(
                owner_username=owner,
                title=f"Parent {suffix}-{index}",
                source_url=f"https://example.com/{suffix}/parent/{index}",
                source_url_hash=f"{suffix}-parent-{index}",
                review_status="listed_master",
            )
            session.add(parent)
            session.flush()
            child = ProductModel(
                owner_username=owner,
                store_id=store.id,
                parent_product_id=parent.id,
                title=f"Child {suffix}-{index}",
                source_url=f"https://example.com/{suffix}/child/{index}",
                source_url_hash=f"{suffix}-child-{index}",
                rakuten_manage_number=f"manage-{suffix}-{index}",
                review_status="listed",
            )
            session.add(child)
            parents.append(parent)
            children.append(child)
        session.flush()
        result = (
            int(store.id),
            [int(parent.id) for parent in parents],
            [int(child.id) for child in children],
        )
        session.commit()
        return result


def test_delete_batch_splits_conflicting_product_from_ready_product(
    monkeypatch,
    session_factory,
):
    store_id, parent_ids, child_ids = seed_owner_products(
        session_factory,
        "alice",
        "alice",
    )
    with session_factory() as session:
        session.add(
            ListingTaskModel(
                id="listing-running",
                owner_username="alice",
                store_id=store_id,
                task_name="listing-running",
                status="running",
                product_ids_json=json.dumps(
                    {
                        "productIds": [parent_ids[0]],
                        "storeIds": [store_id],
                    }
                ),
            )
        )
        session.commit()

    dispatched = []
    monkeypatch.setattr(
        crawler_service,
        "dispatch_next_sync_task",
        lambda: dispatched.append(True),
    )

    result = crawler_service.create_product_delete_sync_task(
        "alice",
        child_ids,
    )

    assert len(result["syncTasks"]) == 2
    assert result["syncTasks"][0]["payload"]["productIds"] == [child_ids[1]]
    assert result["syncTasks"][1]["payload"]["productIds"] == [child_ids[0]]
    assert "同一商品" in result["syncTasks"][1]["message"]
    assert dispatched == [True]


def test_sync_dispatch_runs_other_products_and_other_users(
    monkeypatch,
    session_factory,
):
    alice_store, alice_parents, alice_children = seed_owner_products(
        session_factory,
        "alice",
        "alice",
    )
    bob_store, _, bob_children = seed_owner_products(
        session_factory,
        "bob",
        "bob",
    )
    with session_factory() as session:
        session.add(
            ListingTaskModel(
                id="listing-running",
                owner_username="alice",
                store_id=alice_store,
                task_name="listing-running",
                status="running",
                product_ids_json=json.dumps(
                    {
                        "productIds": [alice_parents[0]],
                        "storeIds": [alice_store],
                    }
                ),
            )
        )
        session.add_all(
            [
                SyncTaskModel(
                    id="alice-conflict",
                    owner_username="alice",
                    store_id=alice_store,
                    task_name="alice-conflict",
                    task_type="product_delete",
                    status="queued",
                    payload_json=json.dumps(
                        {"productIds": [alice_children[0]]}
                    ),
                ),
                SyncTaskModel(
                    id="alice-ready",
                    owner_username="alice",
                    store_id=alice_store,
                    task_name="alice-ready",
                    task_type="product_delete",
                    status="queued",
                    payload_json=json.dumps(
                        {"productIds": [alice_children[1]]}
                    ),
                ),
                SyncTaskModel(
                    id="bob-ready",
                    owner_username="bob",
                    store_id=bob_store,
                    task_name="bob-ready",
                    task_type="product_delete",
                    status="queued",
                    payload_json=json.dumps(
                        {"productIds": [bob_children[0]]}
                    ),
                ),
            ]
        )
        session.commit()

    monkeypatch.setattr(
        crawler_service,
        "finalize_stale_cancel_requested_tasks",
        lambda *_args, **_kwargs: 0,
    )
    monkeypatch.setattr(
        crawler_service,
        "reconcile_interrupted_running_tasks",
        lambda *_args, **_kwargs: 0,
    )
    monkeypatch.setattr(
        crawler_service,
        "sync_task_has_active_background_job",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        crawler_service,
        "listing_task_has_active_background_job",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        crawler_service.settings,
        "max_running_sync_tasks_global",
        3,
    )
    monkeypatch.setattr(
        crawler_service.settings,
        "max_running_sync_tasks_per_user",
        2,
    )
    dispatched = []
    monkeypatch.setattr(
        crawler_service,
        "dispatch_sync_task",
        lambda owner, task_id, **kwargs: dispatched.append(
            (owner, task_id, kwargs["task_type"])
        ),
    )

    crawler_service.dispatch_next_sync_task()

    assert dispatched == [
        ("alice", "alice-ready", "product_delete"),
        ("bob", "bob-ready", "product_delete"),
    ]
    with session_factory() as session:
        conflict = session.get(SyncTaskModel, "alice-conflict")
        assert conflict is not None
        assert "同一商品" in conflict.message


def test_listing_dispatch_runs_other_product_while_delete_is_active(
    monkeypatch,
    session_factory,
):
    store_id, parent_ids, child_ids = seed_owner_products(
        session_factory,
        "alice",
        "alice",
    )
    with session_factory() as session:
        session.add(
            SyncTaskModel(
                id="delete-running",
                owner_username="alice",
                store_id=store_id,
                task_name="delete-running",
                task_type="product_delete",
                status="running",
                payload_json=json.dumps({"productIds": [child_ids[0]]}),
            )
        )
        session.add(
            ListingTaskModel(
                id="listing-ready",
                owner_username="alice",
                store_id=store_id,
                task_name="listing-ready",
                status="queued",
                product_ids_json=json.dumps(
                    {
                        "productIds": [parent_ids[1]],
                        "storeIds": [store_id],
                    }
                ),
            )
        )
        session.commit()

    monkeypatch.setattr(
        crawler_service,
        "finalize_stale_cancel_requested_tasks",
        lambda *_args, **_kwargs: 0,
    )
    monkeypatch.setattr(
        crawler_service,
        "reconcile_interrupted_running_tasks",
        lambda *_args, **_kwargs: 0,
    )
    monkeypatch.setattr(
        crawler_service,
        "listing_task_has_active_background_job",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        crawler_service,
        "sync_task_has_active_background_job",
        lambda *_args, **_kwargs: False,
    )
    dispatched = []
    monkeypatch.setattr(
        crawler_service,
        "dispatch_listing_task",
        lambda owner, task_id: dispatched.append((owner, task_id)),
    )

    crawler_service.dispatch_next_listing_task()

    assert dispatched == [("alice", "listing-ready")]
