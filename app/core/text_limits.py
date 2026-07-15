from __future__ import annotations


RAKUTEN_TITLE_MAX_BYTES = 255
RAKUTEN_TAGLINE_MAX_BYTES = 174


def truncate_utf8_bytes(value: object, max_bytes: int) -> str:
    text = str(value or "").strip()
    limit = max(0, int(max_bytes or 0))
    if len(text.encode("utf-8")) <= limit:
        return text
    return text.encode("utf-8")[:limit].decode("utf-8", errors="ignore").rstrip()
