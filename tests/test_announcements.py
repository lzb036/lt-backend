from __future__ import annotations

from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import pytest
from PIL import Image
from fastapi import UploadFile
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.db.models import UserAccountModel
from app.services import announcement_service


@pytest.fixture()
def announcement_database(tmp_path):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    factory = sessionmaker(
        bind=engine,
        expire_on_commit=False,
        future=True,
    )
    image_dir = tmp_path / "announcement-images"
    with (
        patch.object(announcement_service, "SessionLocal", factory),
        patch.object(
            announcement_service,
            "LOCAL_ANNOUNCEMENT_IMAGE_DIR",
            image_dir,
        ),
        patch.object(
            announcement_service.product_image_storage,
            "enabled",
            False,
        ),
    ):
        yield factory, image_dir
    engine.dispose()


def png_upload(filename: str = "notice.png") -> UploadFile:
    content = BytesIO()
    Image.new("RGB", (2, 2), color=(36, 94, 124)).save(
        content,
        format="PNG",
    )
    content.seek(0)
    return UploadFile(
        filename=filename,
        file=content,
        headers={"content-type": "image/png"},
    )


def test_announcements_publish_and_cleanup_images(
    announcement_database,
) -> None:
    _factory, image_dir = announcement_database
    first_image = announcement_service.save_announcement_image(png_upload())
    second_image = announcement_service.save_announcement_image(
        png_upload("second.png")
    )

    draft = announcement_service.create_announcement(
        title="维护预告",
        content="今晚进行系统升级。",
        image_urls=[first_image, second_image],
        link_label="查看详情",
        link_url="/docs/notice",
        published=False,
        operated_by="superadmin",
    )
    assert announcement_service.list_announcements() == []
    assert len(
        announcement_service.list_announcements(include_unpublished=True)
    ) == 1

    published = announcement_service.update_announcement(
        draft["id"],
        title="维护预告",
        content="今晚进行系统升级。",
        image_urls=[second_image],
        link_label="查看详情",
        link_url="/docs/notice",
        published=True,
        operated_by="superadmin",
    )
    assert published["published"] is True
    assert published["linkLabel"] == "查看详情"
    assert published["linkUrl"] == "/docs/notice"
    assert [row["id"] for row in announcement_service.list_announcements()] == [
        draft["id"]
    ]
    first_filename = Path(first_image).name
    second_filename = Path(second_image).name
    assert not (image_dir / first_filename).exists()
    assert (image_dir / second_filename).exists()

    announcement_service.delete_announcement(draft["id"])
    assert not (image_dir / second_filename).exists()
    assert announcement_service.list_announcements() == []


def test_announcement_image_validation_and_reference_protection(
    announcement_database,
) -> None:
    _factory, image_dir = announcement_database
    with pytest.raises(RuntimeError, match="仅支持"):
        announcement_service.save_announcement_image(
            UploadFile(
                filename="notice.txt",
                file=BytesIO(b"not an image"),
                headers={"content-type": "text/plain"},
            )
        )

    image_url = announcement_service.save_announcement_image(png_upload())
    announcement_service.create_announcement(
        title="图片公告",
        content="查看图片。",
        image_urls=[image_url],
        published=True,
        operated_by="superadmin",
    )
    with pytest.raises(RuntimeError, match="正在被公告使用"):
        announcement_service.delete_unreferenced_announcement_image(image_url)
    assert (image_dir / Path(image_url).name).exists()


def test_announcement_payload_requires_content_or_image(
    announcement_database,
) -> None:
    with pytest.raises(RuntimeError, match="不能同时为空"):
        announcement_service.create_announcement(
            title="空公告",
            content="",
            image_urls=[],
            published=True,
            operated_by="superadmin",
        )


def test_announcement_read_state_is_per_user_and_resets_after_update(
    announcement_database,
) -> None:
    factory, _image_dir = announcement_database
    with factory() as session:
        for username in ("operator-a", "operator-b"):
            session.add(
                UserAccountModel(
                    username=username,
                    display_name=username,
                    password_salt_b64="salt",
                    password_hash_b64="hash",
                    password_iterations=1,
                )
            )
        session.commit()

    announcement = announcement_service.create_announcement(
        title="系统公告",
        content="第一版内容。",
        image_urls=[],
        published=True,
        operated_by="superadmin",
    )
    assert announcement_service.has_unread_announcements("operator-a") is True
    assert announcement_service.mark_announcements_read(
        [announcement["id"]],
        username="operator-a",
    ) == [announcement["id"]]

    operator_a = announcement_service.list_announcements(username="operator-a")
    operator_b = announcement_service.list_announcements(username="operator-b")
    assert operator_a[0]["isRead"] is True
    assert operator_b[0]["isRead"] is False
    assert announcement_service.has_unread_announcements("operator-a") is False

    announcement_service.update_announcement(
        announcement["id"],
        title="系统公告",
        content="第二版内容。",
        image_urls=[],
        published=True,
        operated_by="superadmin",
    )
    assert announcement_service.has_unread_announcements("operator-a") is True
    assert announcement_service.list_announcements(
        username="operator-a"
    )[0]["isRead"] is False


def test_default_manual_announcement_is_seeded_once(
    announcement_database,
) -> None:
    factory, _image_dir = announcement_database
    with factory() as session:
        assert announcement_service.ensure_default_manual_announcement(session)
        session.commit()
        assert not announcement_service.ensure_default_manual_announcement(session)
        session.commit()

    announcements = announcement_service.list_announcements(
        include_unpublished=True
    )
    assert len(announcements) == 1
    assert announcements[0]["title"] == "商品采集系统使用手册"
    assert announcements[0]["linkLabel"] == "查看使用手册"
    assert announcements[0]["linkUrl"] == "/help/operator-manual"
