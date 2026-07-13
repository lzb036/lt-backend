from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.db.database import SessionLocal
from app.db.models import SensitiveWordModel

BRACKET_RULE = "【】"
BRACKET_SEGMENT_RE = re.compile(r"【[^】]*】")
WHITESPACE_RE = re.compile(r"\s+")
MAX_PAGE_SIZE = 500
DEFAULT_SENSITIVE_WORDS: tuple[str, ...] = (
    "500円OFFクーポン",
    "【全店2点購入で10％OFF】",
    "300円OFFクーポン",
    "【お買い物マラソン 当店ポイント5倍】",
    "翌日出荷",
    "翌日配達",
    "楽天1位",
    "楽天2位",
    "楽天3位",
    "【】",
    "【P10&最大600円OFF】",
    "【楽天倉庫出荷】",
    "【日本国内発送】",
    "【楽天1位】",
    "【楽天2位】",
    "【楽天3位】",
    "新生活",
    "即納",
    "【10%OFFクーポン】",
    "【8%OFFクーポン】",
    "【期間限定5％OFF】",
    "一部即納",
    "【P5倍期間限定】",
    "【P10倍期間限定】",
    "お買い物マラソン",
    "【P5倍】",
    "【LINE追加で5%OFF】",
    "【限定10%OFF】",
    "【スーパーDEAL&お買い物マラソンP10】",
    "【お買い物マラソン最大2000円OFF】",
    "＼7%OFFクーポン利用可2.18まで／",
    "【短納期】",
)
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


def seed_default_sensitive_words(session: Any) -> int:
    existing_words = {
        normalize_sensitive_word(word)
        for word in session.scalars(select(SensitiveWordModel.word)).all()
        if normalize_sensitive_word(word)
    }
    created_count = 0
    for word in DEFAULT_SENSITIVE_WORDS:
        normalized = normalize_sensitive_word(word)
        if not normalized or normalized in existing_words:
            continue
        session.add(SensitiveWordModel(word=normalized, enabled=True))
        existing_words.add(normalized)
        created_count += 1
    if created_count:
        session.flush()
    return created_count


def list_sensitive_words(page: int, page_size: int, keyword: str = "") -> dict[str, Any]:
    normalized_page, normalized_page_size = _normalize_page_params(page, page_size)
    normalized_keyword = normalize_sensitive_word(keyword)

    with SessionLocal() as session:
        query = select(SensitiveWordModel)
        if normalized_keyword:
            query = query.where(SensitiveWordModel.word.like(f"%{normalized_keyword}%"))
        total = int(session.scalar(select(func.count()).select_from(query.order_by(None).subquery())) or 0)
        if total:
            max_page = max(1, (total + normalized_page_size - 1) // normalized_page_size)
            normalized_page = min(normalized_page, max_page)
        rows = session.scalars(
            query.order_by(SensitiveWordModel.created_at.asc(), SensitiveWordModel.id.asc())
            .offset((normalized_page - 1) * normalized_page_size)
            .limit(normalized_page_size)
        ).all()
        return {
            "items": [_sensitive_word_to_public(row) for row in rows],
            "total": total,
            "page": normalized_page,
            "pageSize": normalized_page_size,
        }


def create_sensitive_word(word: str, enabled: bool = True) -> dict[str, Any]:
    normalized_word = normalize_sensitive_word(word)
    if not normalized_word:
        raise RuntimeError("敏感词不能为空。")

    with SessionLocal() as session:
        row = SensitiveWordModel(word=normalized_word, enabled=bool(enabled))
        session.add(row)
        try:
            session.commit()
        except IntegrityError as exc:
            session.rollback()
            raise RuntimeError("敏感词已存在。") from exc
        session.refresh(row)
        return _sensitive_word_to_public(row)


def update_sensitive_word(word_id: int, word: str, enabled: bool) -> dict[str, Any]:
    normalized_word = normalize_sensitive_word(word)
    if not normalized_word:
        raise RuntimeError("敏感词不能为空。")

    with SessionLocal() as session:
        row = session.get(SensitiveWordModel, int(word_id))
        if row is None:
            raise RuntimeError("敏感词不存在。")
        row.word = normalized_word
        row.enabled = bool(enabled)
        try:
            session.commit()
        except IntegrityError as exc:
            session.rollback()
            raise RuntimeError("敏感词已存在。") from exc
        session.refresh(row)
        return _sensitive_word_to_public(row)


def delete_sensitive_word(word_id: int) -> bool:
    with SessionLocal() as session:
        row = session.get(SensitiveWordModel, int(word_id))
        if row is None:
            return False
        session.delete(row)
        session.commit()
        return True


def active_sensitive_words(session: Any) -> list[str]:
    return [
        normalize_sensitive_word(word)
        for word in session.scalars(
            select(SensitiveWordModel.word)
            .where(SensitiveWordModel.enabled.is_(True))
            .order_by(func.length(SensitiveWordModel.word).desc(), SensitiveWordModel.word.asc(), SensitiveWordModel.id.asc())
        ).all()
        if normalize_sensitive_word(word)
    ]


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


def _normalize_page_params(page: int | None, page_size: int | None) -> tuple[int, int]:
    normalized_page = max(1, int(page or 1))
    normalized_page_size = min(MAX_PAGE_SIZE, max(1, int(page_size or 1)))
    return normalized_page, normalized_page_size


def _rule_type_for_word(word: str) -> str:
    return "bracket" if word == BRACKET_RULE else "literal"


def _sensitive_word_to_public(row: SensitiveWordModel) -> dict[str, Any]:
    return {
        "id": row.id,
        "word": row.word,
        "ruleType": _rule_type_for_word(row.word),
        "enabled": bool(row.enabled),
        "createdAt": row.created_at.isoformat() if row.created_at else "",
        "updatedAt": row.updated_at.isoformat() if row.updated_at else "",
    }


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
