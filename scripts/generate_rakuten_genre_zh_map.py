from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

import requests


BACKEND_DIR = Path(__file__).resolve().parents[1]
RULES_PATH = BACKEND_DIR / "app" / "resources" / "rakuten_attribute_rules.json"
OUTPUT_PATH = BACKEND_DIR / "app" / "resources" / "rakuten_genre_zh_map.json"
AUTH_URL = "https://edge.microsoft.com/translate/auth"
TRANSLATE_URL = (
    "https://api-edge.cognitive.microsofttranslator.com/translate"
    "?api-version=3.0&from=ja&to=zh-Hans"
)
TRANSLATION_OVERRIDES = {
    "靴": "鞋靴",
    "食品": "食品",
    "本・雑誌・コミック": "图书、杂志、漫画",
    "車・バイク": "汽车、摩托车",
    "車用品・バイク用品": "汽车用品、摩托车用品",
    "花": "鲜花",
    "酒類": "酒类",
    "寝具": "床上用品",
    "日用品雑貨": "日用杂货",
    "腕時計": "手表",
    "文房具": "文具",
    "医薬品・コンタクト・介護": "医药、隐形眼镜、护理",
}


def load_segments() -> list[str]:
    rules = json.loads(RULES_PATH.read_text(encoding="utf-8"))
    genres = rules.get("genres") if isinstance(rules.get("genres"), dict) else {}
    segments = {
        part.strip()
        for genre in genres.values()
        if isinstance(genre, dict)
        for part in str(genre.get("genrePath") or "").split(">")
        if part.strip()
    }
    return sorted(segments)


def load_existing() -> dict[str, str]:
    if not OUTPUT_PATH.exists():
        return {}
    payload = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    translations = payload.get("translations") if isinstance(payload, dict) else {}
    return {
        str(source): str(target)
        for source, target in translations.items()
        if str(source).strip() and str(target).strip()
    } if isinstance(translations, dict) else {}


def access_token(session: requests.Session) -> str:
    response = session.get(AUTH_URL, timeout=30)
    response.raise_for_status()
    return response.text.strip()


def translate_batch(
    session: requests.Session,
    token: str,
    sources: list[str],
) -> list[str]:
    response = session.post(
        TRANSLATE_URL,
        headers={"Authorization": f"Bearer {token}"},
        json=[{"Text": source} for source in sources],
        timeout=90,
    )
    if response.status_code == 401:
        raise PermissionError("translation token expired")
    response.raise_for_status()
    payload: list[dict[str, Any]] = response.json()
    return [
        str(item.get("translations", [{}])[0].get("text") or "").strip()
        for item in payload
    ]


def save(translations: dict[str, str], source_count: int) -> None:
    translations.update({
        source: target
        for source, target in TRANSLATION_OVERRIDES.items()
        if source in translations
    })
    payload = {
        "schemaVersion": 1,
        "source": RULES_PATH.name,
        "sourceSegmentCount": source_count,
        "translatedSegmentCount": len(translations),
        "translations": dict(sorted(translations.items())),
    }
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate the fixed Rakuten genre Japanese-to-Chinese map.")
    parser.add_argument("--batch-size", type=int, default=80)
    parser.add_argument("--pause", type=float, default=0.15)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    segments = load_segments()
    translations = load_existing()
    pending = [segment for segment in segments if segment not in translations]
    if args.limit > 0:
        pending = pending[:args.limit]

    session = requests.Session()
    token = access_token(session)
    batch_size = min(max(args.batch_size, 1), 100)
    for offset in range(0, len(pending), batch_size):
        sources = pending[offset:offset + batch_size]
        for attempt in range(5):
            try:
                targets = translate_batch(session, token, sources)
                break
            except PermissionError:
                token = access_token(session)
            except requests.RequestException:
                if attempt == 4:
                    raise
                time.sleep(2 ** attempt)
        else:
            raise RuntimeError("translation batch failed")
        if len(targets) != len(sources) or any(not target for target in targets):
            raise RuntimeError(f"translation response mismatch at offset {offset}")
        translations.update(zip(sources, targets, strict=True))
        save(translations, len(segments))
        print(f"translated={len(translations)}/{len(segments)}")
        time.sleep(max(args.pause, 0))

    save(translations, len(segments))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
