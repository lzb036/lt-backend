from __future__ import annotations

import argparse
import hashlib
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import func, select

from app.db.database import session_scope
from app.db.models import ProductModel
from app.services import crawler_service
from app.services.product_image_storage import parse_product_image_url, product_image_storage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deduplicate pending-product image references by SHA-256 content.",
    )
    parser.add_argument("--apply", action="store_true", help="Persist changes. Default is dry-run.")
    parser.add_argument("--limit", type=int, default=0, help="Process at most this many products.")
    parser.add_argument("--start-after", type=int, default=0, help="Resume after this product ID.")
    parser.add_argument("--max-id", type=int, default=0, help="Do not process product IDs above this value.")
    parser.add_argument("--workers", type=int, default=12, help="Concurrent OSS metadata lookups per product.")
    parser.add_argument("--progress-every", type=int, default=100, help="Print progress every N products.")
    parser.add_argument("--backup", default="", help="JSONL backup path used in apply mode.")
    return parser.parse_args()


def product_image_references(product: ProductModel) -> tuple[dict[str, Any], list[str], list[str]]:
    payload = crawler_service.product_raw_payload(product)
    shop_code = crawler_service.product_shop_code(product, payload)
    main_urls = crawler_service.product_editable_image_urls(payload, shop_code=shop_code)
    if product.image_url and product.image_url not in main_urls:
        main_urls.insert(0, product.image_url)
    description_urls = crawler_service.unique_texts(
        [
            image_url
            for description in crawler_service.product_descriptions(payload)
            for image_url in crawler_service.description_image_urls(description.get("value"))
        ]
    )
    return payload, main_urls, description_urls


def image_content_digest(image_url: str) -> str:
    stored_image = parse_product_image_url(image_url)
    if stored_image is not None and product_image_storage.enabled:
        fingerprint = product_image_storage.object_fingerprint(stored_image.object_key)
        if fingerprint is not None and fingerprint.sha256:
            return fingerprint.sha256
    image_data = crawler_service.load_product_image_bytes(
        image_url,
        max_bytes=crawler_service.MAX_PRODUCT_IMAGE_DOWNLOAD_BYTES,
        size_error_message="图片下载大小不能超过 20MB。",
    )
    return hashlib.sha256(image_data["content"]).hexdigest()


def build_deduplication(
    main_urls: list[str],
    description_urls: list[str],
    *,
    workers: int,
) -> tuple[list[str], dict[str, str], list[str]]:
    all_urls = crawler_service.unique_texts([*main_urls, *description_urls])
    digests: dict[str, str] = {}
    errors: list[str] = []

    def resolve(image_url: str) -> tuple[str, str, str]:
        try:
            return image_url, image_content_digest(image_url), ""
        except Exception as exc:
            return image_url, "", str(exc)

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        for image_url, digest, error in executor.map(resolve, all_urls):
            if digest:
                digests[image_url] = digest
            elif error:
                errors.append(f"{image_url}: {error}")

    canonical_by_digest: dict[str, str] = {}
    replacement_map: dict[str, str] = {}
    deduplicated_main_urls: list[str] = []

    for image_url in [*main_urls, *description_urls]:
        digest = digests.get(image_url)
        canonical_url = canonical_by_digest.setdefault(digest, image_url) if digest else image_url
        if canonical_url != image_url:
            replacement_map[image_url] = canonical_url
        if image_url in main_urls and canonical_url not in deduplicated_main_urls:
            deduplicated_main_urls.append(canonical_url)

    return deduplicated_main_urls, replacement_map, errors


def backup_product(path: Path, product: ProductModel) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "id": product.id,
        "reviewStatus": product.review_status,
        "imageUrl": product.image_url,
        "rawPayloadJson": product.raw_payload_json,
        "updatedAt": product.updated_at.isoformat() if product.updated_at else "",
    }
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        output.flush()


def main() -> int:
    args = parse_args()
    with session_scope() as session:
        snapshot_max_id = int(
            session.scalar(
                select(func.max(ProductModel.id)).where(ProductModel.review_status == "pending")
            )
            or 0
        )
    max_id = min(snapshot_max_id, args.max_id) if args.max_id else snapshot_max_id
    backup_path = Path(
        args.backup
        or f"data/maintenance/pending-image-dedupe-{datetime.now():%Y%m%d-%H%M%S}.jsonl"
    )

    with session_scope() as session:
        product_ids = list(
            session.scalars(
                select(ProductModel.id)
                .where(
                    ProductModel.review_status == "pending",
                    ProductModel.id > max(0, args.start_after),
                    ProductModel.id <= max_id,
                )
                .order_by(ProductModel.id)
                .limit(args.limit if args.limit > 0 else None)
            )
        )

    stats = {
        "mode": "apply" if args.apply else "dry-run",
        "maxId": max_id,
        "selected": len(product_ids),
        "processed": 0,
        "changedProducts": 0,
        "removedMainRefs": 0,
        "replacedRefs": 0,
        "imageReadErrors": 0,
        "concurrentSkips": 0,
    }

    for product_id in product_ids:
        with session_scope() as session:
            product = session.get(ProductModel, product_id)
            if product is None or product.review_status != "pending":
                continue
            original_payload_json = product.raw_payload_json
            original_image_url = product.image_url
            payload, main_urls, description_urls = product_image_references(product)

        deduplicated_main_urls, replacement_map, errors = build_deduplication(
            main_urls,
            description_urls,
            workers=args.workers,
        )
        stats["processed"] += 1
        stats["imageReadErrors"] += len(errors)
        removed_main_refs = max(0, len(main_urls) - len(deduplicated_main_urls))
        if not removed_main_refs and not replacement_map:
            if stats["processed"] % max(1, args.progress_every) == 0:
                print(json.dumps(stats, ensure_ascii=False), flush=True)
            continue

        updated_payload = crawler_service.set_product_image_urls(payload, deduplicated_main_urls)
        if replacement_map:
            updated_payload = crawler_service.replace_product_description_image_urls(
                updated_payload,
                replacement_map,
            )
        updated_payload_json = json.dumps(updated_payload, ensure_ascii=False)
        updated_image_url = deduplicated_main_urls[0] if deduplicated_main_urls else ""

        stats["changedProducts"] += 1
        stats["removedMainRefs"] += removed_main_refs
        stats["replacedRefs"] += len(replacement_map)

        if args.apply:
            with session_scope() as session:
                product = session.get(ProductModel, product_id)
                if (
                    product is None
                    or product.review_status != "pending"
                    or product.raw_payload_json != original_payload_json
                    or product.image_url != original_image_url
                ):
                    stats["concurrentSkips"] += 1
                    continue
                backup_product(backup_path, product)
                product.raw_payload_json = updated_payload_json
                product.image_url = updated_image_url
                session.flush()

        if stats["processed"] % max(1, args.progress_every) == 0:
            print(json.dumps(stats, ensure_ascii=False), flush=True)

    if args.apply:
        stats["backup"] = str(backup_path)
    print(json.dumps(stats, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
