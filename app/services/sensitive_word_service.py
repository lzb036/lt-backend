from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

BRACKET_RULE = "【】"
BRACKET_SEGMENT_RE = re.compile(r"【[^】]*】")
WHITESPACE_RE = re.compile(r"\s+")
RECURSIVE_SANITIZE_FIELD_NAMES = {
    "title",
    "itemName",
    "tagline",
    "subtitle",
    "subTitle",
    "catchCopy",
    "catchcopy",
    "catchCopyTrans",
    "saleComment",
    "sale_comment",
}
ROOT_ONLY_SANITIZE_FIELD_NAMES = {
    "name",
}


def normalize_sensitive_word(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def sanitize_sensitive_text(value: Any, words: Iterable[str]) -> str:
    text = normalize_sensitive_word(value)
    if not text:
        return ""

    bracket_rule_enabled, normalized_words = _prepare_sensitive_words(words)

    if bracket_rule_enabled:
        text = BRACKET_SEGMENT_RE.sub("", text)

    for word in normalized_words:
        text = text.replace(word, "")

    return WHITESPACE_RE.sub(" ", text).strip()


def sanitize_product_payload(payload: dict[str, Any], words: Iterable[str]) -> tuple[dict[str, Any], bool]:
    bracket_rule_enabled, normalized_words = _prepare_sensitive_words(words)
    cleaned, changed = _sanitize_payload_node(payload, normalized_words, bracket_rule_enabled)

    for key in ROOT_ONLY_SANITIZE_FIELD_NAMES:
        value = cleaned.get(key)
        if isinstance(value, (dict, list)):
            continue
        sanitized = _sanitize_text_with_prepared_words(value, normalized_words, bracket_rule_enabled)
        if sanitized != value:
            cleaned[key] = sanitized
            changed = True

    return cleaned, changed


def _prepare_sensitive_words(words: Iterable[str]) -> tuple[bool, list[str]]:
    bracket_rule_enabled = False
    literal_words: set[str] = set()

    for word in words:
        normalized = normalize_sensitive_word(word)
        if not normalized:
            continue
        if normalized == BRACKET_RULE:
            bracket_rule_enabled = True
            continue
        literal_words.add(normalized)

    return bracket_rule_enabled, sorted(literal_words, key=lambda item: (-len(item), item))


def _sanitize_text_with_prepared_words(value: Any, literal_words: list[str], bracket_rule_enabled: bool) -> str:
    text = normalize_sensitive_word(value)
    if not text:
        return ""

    if bracket_rule_enabled:
        text = BRACKET_SEGMENT_RE.sub("", text)

    for word in literal_words:
        text = text.replace(word, "")

    return WHITESPACE_RE.sub(" ", text).strip()


def _sanitize_payload_node(value: Any, words: list[str], bracket_rule_enabled: bool) -> tuple[Any, bool]:
    if isinstance(value, dict):
        changed = False
        cleaned: dict[str, Any] = {}
        for key, item in value.items():
            if key in RECURSIVE_SANITIZE_FIELD_NAMES and not isinstance(item, (dict, list)):
                sanitized = _sanitize_text_with_prepared_words(item, words, bracket_rule_enabled)
                cleaned[key] = sanitized
                changed = changed or sanitized != item
                continue
            cleaned_item, item_changed = _sanitize_payload_node(item, words, bracket_rule_enabled)
            cleaned[key] = cleaned_item
            changed = changed or item_changed
        return cleaned, changed

    if isinstance(value, list):
        changed = False
        cleaned_items: list[Any] = []
        for item in value:
            cleaned_item, item_changed = _sanitize_payload_node(item, words, bracket_rule_enabled)
            cleaned_items.append(cleaned_item)
            changed = changed or item_changed
        return cleaned_items, changed

    return value, False
