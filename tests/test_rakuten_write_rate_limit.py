from __future__ import annotations

from typing import Any

from app.services import crawler_service


class FakeResponse:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise crawler_service.requests.HTTPError(f"HTTP {self.status_code}")


def test_rakuten_write_retries_ga0003_response(monkeypatch) -> None:
    limited = FakeResponse(
        403,
        '{"errors":[{"code":"GA0003","message":"Forbidden- Exceeded QPS Limitation"}]}',
    )
    success = FakeResponse(204)
    responses = iter([limited, success])
    calls: list[tuple[str, str]] = []
    sleeps: list[float] = []

    def fake_request(method: str, url: str, **_kwargs: Any) -> FakeResponse:
        calls.append((method, url))
        return next(responses)

    monkeypatch.setattr(crawler_service.requests, "request", fake_request)
    monkeypatch.setattr(crawler_service.time, "sleep", sleeps.append)

    response = crawler_service.request_rakuten_write(
        "DELETE",
        "https://example.test/item",
        headers={},
        operation="删除商品",
    )

    assert response is success
    assert limited.closed is True
    assert calls == [
        ("DELETE", "https://example.test/item"),
        ("DELETE", "https://example.test/item"),
    ]
    assert sleeps == [1.5]


def test_delete_rakuten_item_uses_throttled_write_request(monkeypatch) -> None:
    waits: list[bool] = []
    requests: list[dict[str, Any]] = []

    monkeypatch.setattr(
        crawler_service,
        "wait_for_rakuten_item_delete_slot",
        lambda: waits.append(True),
    )

    def fake_write(method: str, url: str, **kwargs: Any) -> FakeResponse:
        requests.append({"method": method, "url": url, **kwargs})
        return FakeResponse(204)

    monkeypatch.setattr(crawler_service, "request_rakuten_write", fake_write)

    crawler_service.delete_rakuten_item("secret", "key", "manage number")

    assert waits == [True]
    assert len(requests) == 1
    assert requests[0]["method"] == "DELETE"
    assert requests[0]["operation"] == "乐天商品 manage number 删除"
    assert requests[0]["url"].endswith("/manage-numbers/manage%20number")
