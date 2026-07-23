from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bs4 import BeautifulSoup
from sqlalchemy import func, select

from app.db.database import SessionLocal
from app.db.models import ProductModel
from app.services import crawler_service
from scripts.cleanup_pending_unrelated_product_images import cleanup_image_references


DESCRIPTION_KEYS = {
    "description",
    "descriptions",
    "newproductdescription",
    "pcdescription",
    "productdescription",
    "salesdescription",
    "smartphonedescription",
    "spdescription",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify pending-product images that exceed trusted Rakuten main images.",
    )
    parser.add_argument("--max-id", type=int, default=0, help="Do not inspect product IDs above this value.")
    parser.add_argument("--progress-every", type=int, default=1000, help="Print progress every N products.")
    parser.add_argument("--report", default="", help="Output JSONL report path.")
    return parser.parse_args()


def description_image_link_contexts(payload: dict[str, Any]) -> dict[str, list[str]]:
    contexts: dict[str, list[str]] = {}

    def inspect_html(value: str) -> None:
        soup = BeautifulSoup(value or "", "lxml")
        for image in soup.select("img, source"):
            source = (
                image.get("src")
                or image.get("data-src")
                or image.get("data-original")
                or image.get("data-lazy-src")
            )
            image_url = crawler_service.normalize_description_image_url(source)
            if not image_url:
                continue
            link = image.find_parent("a")
            href = crawler_service.normalize_text(link.get("href")) if link else ""
            contexts.setdefault(image_url, []).append(href)

    def inspect_description_value(value: Any) -> None:
        if isinstance(value, str):
            inspect_html(value)
        elif isinstance(value, dict):
            for child in value.values():
                inspect_description_value(child)
        elif isinstance(value, list):
            for child in value:
                inspect_description_value(child)

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if str(key).lower() in DESCRIPTION_KEYS:
                    inspect_description_value(child)
                else:
                    walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(payload)
    return contexts


def is_shop_home_link(value: str, shop_code: str) -> bool:
    href = crawler_service.normalize_text(value)
    normalized_shop = crawler_service.normalize_shop_code(shop_code).lower()
    if not href or not normalized_shop:
        return False
    try:
        parsed = urlsplit(href)
    except Exception:
        return False
    host = parsed.netloc.lower()
    parts = [unquote(part).lower() for part in parsed.path.split("/") if part]
    if host == "www.rakuten.ne.jp":
        return len(parts) >= 2 and parts[0] == "gold" and parts[1] == normalized_shop
    if host == "www.rakuten.co.jp":
        return bool(parts) and parts[0] == normalized_shop
    return False


def source_matches_item_number(source_url: str, item_number: str) -> bool:
    if not source_url:
        return False
    try:
        path = unquote(urlsplit(source_url).path).lower()
    except Exception:
        return False
    return any(token in path for token in crawler_service.item_number_image_tokens(item_number))


def classify_extra_image(
    *,
    hrefs: list[str],
    source_url: str,
    shop_code: str,
    item_number: str,
) -> str:
    if hrefs and all(is_shop_home_link(href, shop_code) for href in hrefs):
        return "safe_remove"
    if any(not href for href in hrefs):
        return "keep"
    if source_matches_item_number(source_url, item_number):
        return "keep"
    return "manual_review"


def preferred_original_sources(payload: dict[str, Any], *, shop_code: str) -> list[str]:
    values = payload.get("ltOriginalImages")
    if not isinstance(values, list):
        return []
    normalized = [
        crawler_service.normalize_product_image_url(value, shop_code=shop_code)
        for value in values
    ]
    preferred, _ = crawler_service.preferred_rakuten_image_urls(
        [value for value in normalized if value]
    )
    return preferred


def main() -> int:
    args = parse_args()
    session = SessionLocal()
    try:
        snapshot_max_id = int(
            session.scalar(
                select(func.max(ProductModel.id)).where(ProductModel.review_status == "pending")
            )
            or 0
        )
    finally:
        session.close()
    max_id = min(snapshot_max_id, args.max_id) if args.max_id else snapshot_max_id
    report_path = Path(
        args.report
        or f"data/maintenance/pending-extra-image-classification-{datetime.now():%Y%m%d-%H%M%S}.jsonl"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)

    stats = {
        "snapshotMaxId": max_id,
        "processed": 0,
        "candidateProducts": 0,
        "safeRemoveProducts": 0,
        "keepProducts": 0,
        "manualReviewProducts": 0,
        "safeRemoveImages": 0,
        "keepImages": 0,
        "manualReviewImages": 0,
    }
    shop_counts: dict[str, Counter[str]] = {
        "safe_remove": Counter(),
        "keep": Counter(),
        "manual_review": Counter(),
    }

    session = SessionLocal()
    try:
        with report_path.open("w", encoding="utf-8") as report:
            products = session.scalars(
                select(ProductModel)
                .where(
                    ProductModel.review_status == "pending",
                    ProductModel.id <= max_id,
                )
                .order_by(ProductModel.id)
                .execution_options(yield_per=100)
            )
            for product in products:
                payload = crawler_service.product_raw_payload(product)
                shop_code = crawler_service.product_shop_code(product, payload)
                item_number = (
                    crawler_service.first_text_from_keys(payload, ("itemNumber", "manageNumber"))
                    or product.item_number
                )
                current_images = crawler_service.product_editable_image_urls(
                    payload,
                    shop_code=shop_code,
                )
                kept_images, extra_images, trusted_sources = cleanup_image_references(
                    payload,
                    current_images,
                    shop_code=shop_code,
                )
                stats["processed"] += 1
                if not extra_images:
                    if stats["processed"] % max(1, args.progress_every) == 0:
                        print(json.dumps(stats, ensure_ascii=False), flush=True)
                    continue

                stats["candidateProducts"] += 1
                link_contexts = description_image_link_contexts(payload)
                original_sources = preferred_original_sources(payload, shop_code=shop_code)
                image_records = []
                classifications: set[str] = set()
                for offset, image_url in enumerate(extra_images):
                    source_index = len(kept_images) + offset
                    source_url = original_sources[source_index] if source_index < len(original_sources) else ""
                    classification = classify_extra_image(
                        hrefs=link_contexts.get(image_url, []),
                        source_url=source_url,
                        shop_code=shop_code,
                        item_number=item_number,
                    )
                    classifications.add(classification)
                    image_records.append(
                        {
                            "imageUrl": image_url,
                            "sourceUrl": source_url,
                            "hrefs": link_contexts.get(image_url, []),
                            "classification": classification,
                        }
                    )

                counts = Counter(record["classification"] for record in image_records)
                stats["safeRemoveImages"] += counts["safe_remove"]
                stats["keepImages"] += counts["keep"]
                stats["manualReviewImages"] += counts["manual_review"]
                if classifications == {"safe_remove"}:
                    product_classification = "safe_remove"
                    stats["safeRemoveProducts"] += 1
                elif "keep" in classifications:
                    product_classification = "keep"
                    stats["keepProducts"] += 1
                else:
                    product_classification = "manual_review"
                    stats["manualReviewProducts"] += 1
                shop_counts[product_classification][shop_code or product.shop_name or "unknown"] += 1

                report.write(
                    json.dumps(
                        {
                            "id": int(product.id),
                            "title": product.title,
                            "shopCode": shop_code,
                            "shopName": product.shop_name,
                            "itemNumber": item_number,
                            "sourceUrl": product.source_url,
                            "currentImageCount": len(current_images),
                            "trustedImageCount": len(trusted_sources),
                            "extraImageCount": len(extra_images),
                            "classification": product_classification,
                            "images": image_records,
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                if stats["processed"] % max(1, args.progress_every) == 0:
                    print(json.dumps(stats, ensure_ascii=False), flush=True)
    finally:
        session.close()

    stats["topShops"] = {
        key: value.most_common(20)
        for key, value in shop_counts.items()
    }
    stats["report"] = str(report_path)
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
