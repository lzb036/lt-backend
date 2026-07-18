from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

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
