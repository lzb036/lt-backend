from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

BRACKET_RULE = "【】"
BRACKET_SEGMENT_RE = re.compile(r"【[^】]*】")
WHITESPACE_RE = re.compile(r"\s+")
SANITIZE_FIELD_NAMES = {
    "title",
    "itemName",
    "name",
    "tagline",
    "subtitle",
    "subTitle",
    "catchCopy",
    "catchcopy",
    "catchCopyTrans",
    "saleComment",
    "sale_comment",
}


def normalize_sensitive_word(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def sanitize_sensitive_text(value: Any, words: Iterable[str]) -> str:
    text = normalize_sensitive_word(value)
    if not text:
        return ""

    normalized_words: list[str] = []
    bracket_rule_enabled = False
    for word in words:
        normalized = normalize_sensitive_word(word)
        if not normalized:
            continue
        if normalized == BRACKET_RULE:
            bracket_rule_enabled = True
            continue
        normalized_words.append(normalized)

    if bracket_rule_enabled:
        text = BRACKET_SEGMENT_RE.sub("", text)

    for word in normalized_words:
        text = text.replace(word, "")

    return WHITESPACE_RE.sub(" ", text).strip()


def sanitize_product_payload(payload: dict[str, Any], words: Iterable[str]) -> tuple[dict[str, Any], bool]:
    normalized_words = [normalized for normalized in (normalize_sensitive_word(word) for word in words) if normalized]
    cleaned, changed = _sanitize_payload_node(payload, normalized_words)
    return cleaned, changed


def _sanitize_payload_node(value: Any, words: list[str]) -> tuple[Any, bool]:
    if isinstance(value, dict):
        changed = False
        cleaned: dict[str, Any] = {}
        for key, item in value.items():
            if key in SANITIZE_FIELD_NAMES and not isinstance(item, (dict, list)):
                sanitized = sanitize_sensitive_text(item, words)
                cleaned[key] = sanitized
                changed = changed or sanitized != item
                continue
            cleaned_item, item_changed = _sanitize_payload_node(item, words)
            cleaned[key] = cleaned_item
            changed = changed or item_changed
        return cleaned, changed

    if isinstance(value, list):
        changed = False
        cleaned_items: list[Any] = []
        for item in value:
            cleaned_item, item_changed = _sanitize_payload_node(item, words)
            cleaned_items.append(cleaned_item)
            changed = changed or item_changed
        return cleaned_items, changed

    return value, False
