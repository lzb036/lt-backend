from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from app.services import crawler_service


def freeze_now(monkeypatch, value: datetime) -> None:
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return value.replace(tzinfo=tz) if tz is not None else value

    monkeypatch.setattr(crawler_service, "datetime", FrozenDateTime)


def test_existing_yx_folder_is_reused_before_creating_new_folder(monkeypatch):
    folders = [
        {
            "folderId": 1,
            "folderName": "LT Store 2026-07 001",
            "directoryName": "lt-store-202607-001",
            "fileCount": 10,
        },
        {
            "folderId": 2,
            "folderName": "YX20260717-1",
            "directoryName": "yx20260717-1",
            "fileCount": 499,
        },
        {
            "folderId": 3,
            "folderName": "YX20260718-1",
            "directoryName": "yx20260718-1",
            "fileCount": 100,
        },
    ]
    monkeypatch.setattr(
        crawler_service,
        "fetch_rakuten_cabinet_folders",
        lambda *_: folders,
    )
    monkeypatch.setattr(
        crawler_service,
        "create_rakuten_cabinet_folder",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("未满时不应创建新文件夹")
        ),
    )

    folder = crawler_service.ensure_listing_cabinet_folder(
        "secret",
        "key",
        SimpleNamespace(id=1, store_code="store"),
        20,
    )

    assert folder["folderId"] == 2
    assert folder["directoryName"] == "yx20260717-1"


def test_new_folder_uses_today_and_next_daily_sequence(monkeypatch):
    freeze_now(monkeypatch, datetime(2026, 7, 18, 12, 0, 0))
    folders = [
        {
            "folderId": 1,
            "folderName": "YX20260717-4",
            "directoryName": "yx20260717-4",
            "fileCount": 500,
        },
        {
            "folderId": 2,
            "folderName": "YX20260718-1",
            "directoryName": "yx20260718-1",
            "fileCount": 500,
        },
        {
            "folderId": 3,
            "folderName": "YX20260718-2",
            "directoryName": "yx20260718-2",
            "fileCount": 500,
        },
    ]
    created = {}
    monkeypatch.setattr(
        crawler_service,
        "fetch_rakuten_cabinet_folders",
        lambda *_: folders,
    )
    monkeypatch.setattr(
        crawler_service,
        "fetch_rakuten_cabinet_usage",
        lambda *_: {"remainingFolderCount": 10},
    )

    def create_folder(*_args, **kwargs):
        created.update(kwargs)
        return {
            "folderId": 4,
            "folderName": kwargs["folder_name"],
            "directoryName": kwargs["directory_name"],
            "fileCount": 0,
        }

    monkeypatch.setattr(
        crawler_service,
        "create_rakuten_cabinet_folder",
        create_folder,
    )

    folder = crawler_service.ensure_listing_cabinet_folder(
        "secret",
        "key",
        SimpleNamespace(id=1, store_code="store"),
        1,
    )

    assert created == {
        "folder_name": "YX20260718-3",
        "directory_name": "yx20260718-3",
    }
    assert folder["folderId"] == 4


def test_concurrently_created_folder_is_reused(monkeypatch):
    freeze_now(monkeypatch, datetime(2026, 7, 30, 12, 0, 0))
    fetch_results = [
        [],
        [],
        [
            {
                "folderId": 88,
                "folderName": "YX20260730-1",
                "directoryName": "yx20260730-1",
                "fileCount": 12,
            }
        ],
    ]
    monkeypatch.setattr(
        crawler_service,
        "fetch_rakuten_cabinet_folders",
        lambda *_: fetch_results.pop(0),
    )
    monkeypatch.setattr(
        crawler_service,
        "create_rakuten_cabinet_folder",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            crawler_service.CabinetFolderAlreadyExistsError("yx20260730-1")
        ),
    )
    monkeypatch.setattr(crawler_service.time, "sleep", lambda _seconds: None)

    folder = crawler_service.ensure_listing_cabinet_folder(
        "secret",
        "key",
        SimpleNamespace(id=2, store_code="japaneden"),
        1,
        usage={"remainingFolderCount": 10},
    )

    assert folder["folderId"] == 88
    assert folder["directoryName"] == "yx20260730-1"


def test_created_folder_waits_until_visible_before_releasing_lock(monkeypatch):
    freeze_now(monkeypatch, datetime(2026, 7, 30, 12, 0, 0))
    visible_folder = {
        "folderId": 89,
        "folderName": "YX20260730-1",
        "directoryName": "yx20260730-1",
        "fileCount": 0,
    }
    fetch_results = [[], [visible_folder]]
    monkeypatch.setattr(
        crawler_service,
        "fetch_rakuten_cabinet_folders",
        lambda *_: fetch_results.pop(0),
    )
    monkeypatch.setattr(
        crawler_service,
        "create_rakuten_cabinet_folder",
        lambda *_args, **kwargs: {
            "folderId": 89,
            "folderName": kwargs["folder_name"],
            "directoryName": kwargs["directory_name"],
            "fileCount": 0,
        },
    )
    monkeypatch.setattr(crawler_service.time, "sleep", lambda _seconds: None)

    folder = crawler_service.ensure_listing_cabinet_folder(
        "secret",
        "key",
        SimpleNamespace(id=2, store_code="japaneden"),
        1,
        usage={"remainingFolderCount": 10},
    )

    assert folder == visible_folder
    assert fetch_results == []


def test_folder_creation_uses_store_scoped_distributed_lock(monkeypatch):
    events = []

    class FakeLock:
        def acquire(self, blocking=True):
            events.append(("acquire", blocking))
            return True

        def release(self):
            events.append(("release",))

    class FakeRedis:
        def lock(self, name, **kwargs):
            events.append(("lock", name, kwargs))
            return FakeLock()

    monkeypatch.setattr(crawler_service, "should_use_redis_task_queue", lambda: True)
    monkeypatch.setattr(crawler_service, "redis_connection", lambda: FakeRedis())
    monkeypatch.setattr(
        crawler_service,
        "fetch_rakuten_cabinet_folders",
        lambda *_: [
            {
                "folderId": 1,
                "folderName": "YX20260730-1",
                "directoryName": "yx20260730-1",
                "fileCount": 1,
            }
        ],
    )

    folder = crawler_service.ensure_listing_cabinet_folder(
        "secret",
        "key",
        SimpleNamespace(id=2, store_code="japaneden"),
        1,
    )

    assert folder["folderId"] == 1
    assert events[0][0] == "lock"
    assert events[0][1].startswith("lt:lock:cabinet-folder:")
    assert events[1:] == [("acquire", True), ("release",)]


def test_product_images_fill_current_folder_before_switching(monkeypatch):
    first_folder = {
        "folderId": 1,
        "folderName": "YX20260717-1",
        "directoryName": "yx20260717-1",
        "fileCount": 499,
    }
    second_folder = {
        "folderId": 2,
        "folderName": "YX20260718-1",
        "directoryName": "yx20260718-1",
        "fileCount": 0,
    }
    context = {"currentFolder": first_folder}
    selected_folder_ids = []

    monkeypatch.setattr(
        crawler_service,
        "ensure_listing_cabinet_folder",
        lambda *_args, **_kwargs: second_folder,
    )
    monkeypatch.setattr(
        crawler_service,
        "recover_missing_local_product_images",
        lambda _product, images: images,
    )
    monkeypatch.setattr(
        crawler_service,
        "is_gif_image_url",
        lambda _url: False,
    )
    monkeypatch.setattr(
        crawler_service,
        "load_product_image_bytes",
        lambda *_args, **_kwargs: b"image",
    )
    monkeypatch.setattr(
        crawler_service,
        "prepare_rakuten_cabinet_image",
        lambda _content: {
            "suffix": ".jpg",
            "content": b"image",
            "contentType": "image/jpeg",
        },
    )

    def insert_file(*_args, **kwargs):
        selected_folder_ids.append(kwargs["folder_id"])
        return {
            "fileId": len(selected_folder_ids),
            "filePath": kwargs["file_path"],
        }

    monkeypatch.setattr(
        crawler_service,
        "insert_rakuten_cabinet_file",
        insert_file,
    )

    uploaded = crawler_service.upload_product_images_to_rakuten(
        "secret",
        "key",
        SimpleNamespace(store_code="store"),
        SimpleNamespace(title="Product"),
        "manage-number",
        cabinet_context=context,
        source_images=["https://example.com/1.jpg", "https://example.com/2.jpg"],
    )

    assert selected_folder_ids == [1, 2]
    assert [row["folderPath"] for row in uploaded] == [
        "yx20260717-1",
        "yx20260718-1",
    ]
    assert first_folder["fileCount"] == 500
    assert second_folder["fileCount"] == 1


def test_redis_slot_reservation_prevents_two_contexts_using_same_last_slot(monkeypatch):
    class FakeLock:
        def acquire(self, blocking=True):
            return True

        def release(self):
            return None

    class FakeRedis:
        def __init__(self):
            self.values = {}

        def lock(self, _name, **_kwargs):
            return FakeLock()

        def get(self, name):
            return self.values.get(name)

        def set(self, name, value, **_kwargs):
            self.values[name] = value
            return True

    folders = [
        {
            "folderId": 1,
            "folderName": "YX20260730-1",
            "directoryName": "yx20260730-1",
            "fileCount": 499,
        },
        {
            "folderId": 2,
            "folderName": "YX20260730-2",
            "directoryName": "yx20260730-2",
            "fileCount": 0,
        },
    ]
    redis = FakeRedis()
    monkeypatch.setattr(crawler_service, "should_use_redis_task_queue", lambda: True)
    monkeypatch.setattr(crawler_service, "redis_connection", lambda: redis)
    monkeypatch.setattr(crawler_service, "fetch_rakuten_cabinet_folders", lambda *_: folders)

    first = crawler_service.ensure_listing_cabinet_folder_for_upload(
        "secret",
        "key",
        SimpleNamespace(id=1, store_code="store"),
        1,
        cabinet_context={},
    )
    second = crawler_service.ensure_listing_cabinet_folder_for_upload(
        "secret",
        "key",
        SimpleNamespace(id=1, store_code="store"),
        1,
        cabinet_context={},
    )

    assert first["folderId"] == 1
    assert second["folderId"] == 2
    assert first["_cabinetReservation"]["token"] != second["_cabinetReservation"]["token"]


def test_cabinet_3006_refreshes_and_rotates_folder_before_retry(monkeypatch):
    class FakeLock:
        def acquire(self, blocking=True):
            return True

        def release(self):
            return None

    class FakeRedis:
        def __init__(self):
            self.values = {}

        def lock(self, _name, **_kwargs):
            return FakeLock()

        def get(self, name):
            return self.values.get(name)

        def set(self, name, value, **_kwargs):
            self.values[name] = value
            return True

    folders = [
        {
            "folderId": 1,
            "folderName": "YX20260730-1",
            "directoryName": "yx20260730-1",
            "fileCount": 0,
        },
        {
            "folderId": 2,
            "folderName": "YX20260730-2",
            "directoryName": "yx20260730-2",
            "fileCount": 0,
        },
    ]
    redis = FakeRedis()
    selected_folder_ids = []
    monkeypatch.setattr(crawler_service, "should_use_redis_task_queue", lambda: True)
    monkeypatch.setattr(crawler_service, "redis_connection", lambda: redis)
    monkeypatch.setattr(crawler_service, "fetch_rakuten_cabinet_folders", lambda *_: folders)
    monkeypatch.setattr(
        crawler_service,
        "recover_missing_local_product_images",
        lambda _product, images: images,
    )
    monkeypatch.setattr(
        crawler_service,
        "prepare_rakuten_listing_images",
        lambda *_args, **_kwargs: [
            {
                "sourceUrl": "https://example.com/1.jpg",
                "suffix": ".jpg",
                "content": b"image",
                "contentType": "image/jpeg",
            }
        ],
    )

    def insert_file(*_args, **kwargs):
        selected_folder_ids.append(kwargs["folder_id"])
        if len(selected_folder_ids) == 1:
            raise crawler_service.RakutenCabinetFolderFullError(
                "Number of files is upper limit，resultCode=3006",
                folder_id=kwargs["folder_id"],
            )
        return {
            "fileId": 2,
            "filePath": kwargs["file_path"],
        }

    monkeypatch.setattr(crawler_service, "insert_rakuten_cabinet_file", insert_file)

    uploaded = crawler_service.upload_product_images_to_rakuten(
        "secret",
        "key",
        SimpleNamespace(store_code="store"),
        SimpleNamespace(title="Product"),
        "manage-number",
        cabinet_context={},
        source_images=["https://example.com/1.jpg"],
    )

    assert selected_folder_ids == [1, 2]
    assert uploaded[0]["folderId"] == "2"
    assert uploaded[0]["folderPath"] == "yx20260730-2"


def test_insert_cabinet_file_raises_specific_error_for_result_code_3006(monkeypatch):
    response = SimpleNamespace(
        text=(
            "<result><systemStatus>NG</systemStatus>"
            "<message>Number of files is upper limit</message>"
            "<resultCode>3006</resultCode></result>"
        ),
        status_code=200,
    )
    monkeypatch.setattr(crawler_service, "rakuten_cabinet_request", lambda *_args, **_kwargs: response)

    with pytest.raises(crawler_service.RakutenCabinetFolderFullError) as error:
        crawler_service.insert_rakuten_cabinet_file(
            "secret",
            "key",
            file_name="image",
            file_path="image.jpg",
            content=b"image",
            content_type="image/jpeg",
            folder_id=7,
        )

    assert error.value.folder_id == 7
    assert "3006" in str(error.value)
