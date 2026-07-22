from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

from app.core.config import settings
from app.main import app
from app.services import crawler_service


def test_api_docs_are_disabled_by_default() -> None:
    assert settings.api_docs_enabled is False
    assert app.docs_url is None
    assert app.redoc_url is None
    assert app.openapi_url is None


def test_draft_image_url_has_expiring_signature(monkeypatch) -> None:
    monkeypatch.setattr(crawler_service.time, "time", lambda: 1000)
    monkeypatch.setattr(settings, "product_image_draft_url_ttl_seconds", 300)

    image_url = crawler_service.local_product_image_draft_url(7, "draft image.jpg")
    parsed = urlsplit(image_url)
    query = parse_qs(parsed.query)

    assert parsed.path.endswith("/7/draft%20image.jpg")
    assert query["expires"] == ["1300"]
    assert crawler_service.verify_product_image_draft_access(
        7,
        "draft image.jpg",
        1300,
        query["signature"][0],
        now=1299,
    )
    assert not crawler_service.verify_product_image_draft_access(
        7,
        "other.jpg",
        1300,
        query["signature"][0],
        now=1299,
    )
    assert not crawler_service.verify_product_image_draft_access(
        7,
        "draft image.jpg",
        1300,
        query["signature"][0],
        now=1300,
    )
