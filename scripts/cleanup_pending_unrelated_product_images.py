from __future__ import annotations

import argparse
import json
import sys
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove appended promotion/topic images from pending products.",
    )
    parser.add_argument("--apply", action="store_true", help="Persist changes. Default is dry-run.")
    parser.add_argument("--limit", type=int, default=0, help="Process at most this many products.")
    parser.add_argument("--start-after", type=int, default=0, help="Resume after this product ID.")
    parser.add_argument("--max-id", type=int, default=0, help="Do not process product IDs above this value.")
    parser.add_argument("--progress-every", type=int, default=100, help="Print progress every N products.")
    parser.add_argument("--backup", default="", help="JSONL backup path used in apply mode.")
    return parser.parse_args()


def cleanup_image_references(
    payload: dict[str, Any],
    current_images: list[str],
    *,
    shop_code: str,
) -> tuple[list[str], list[str], list[str]]:
    trusted_sources = crawler_service.trusted_product_main_image_urls(
        payload,
        shop_code=shop_code,
    )
    trusted_sources, _ = crawler_service.preferred_rakuten_image_urls(trusted_sources)
    if not trusted_sources:
        return list(current_images), [], []
    trusted_count = len(trusted_sources)
    original_sources = payload.get("ltOriginalImages")
    if isinstance(original_sources, list):
        normalized_originals = [
            crawler_service.normalize_product_image_url(image, shop_code=shop_code)
            for image in original_sources
        ]
        normalized_originals, _ = crawler_service.preferred_rakuten_image_urls(
            [image for image in normalized_originals if image]
        )
        if normalized_originals:
            trusted_sources = normalized_originals[:trusted_count]
    kept_images = list(current_images[:trusted_count])
    removed_images = list(current_images[trusted_count:])
    return kept_images, removed_images, trusted_sources


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
        or f"data/maintenance/pending-unrelated-image-cleanup-{datetime.now():%Y%m%d-%H%M%S}.jsonl"
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
        "skippedWithoutTrustedImages": 0,
        "concurrentSkips": 0,
    }

    for product_id in product_ids:
        with session_scope() as session:
            product = session.get(ProductModel, product_id)
            if product is None or product.review_status != "pending":
                continue
            original_payload_json = product.raw_payload_json
            original_image_url = product.image_url
            payload = crawler_service.product_raw_payload(product)
            shop_code = crawler_service.product_shop_code(product, payload)
            current_images = crawler_service.product_editable_image_urls(
                payload,
                shop_code=shop_code,
            )

        kept_images, removed_images, trusted_sources = cleanup_image_references(
            payload,
            current_images,
            shop_code=shop_code,
        )
        stats["processed"] += 1
        if not trusted_sources:
            stats["skippedWithoutTrustedImages"] += 1
        if not removed_images:
            if stats["processed"] % max(1, args.progress_every) == 0:
                print(json.dumps(stats, ensure_ascii=False), flush=True)
            continue

        updated_payload = crawler_service.set_product_image_urls(payload, kept_images)
        updated_payload["ltOriginalImages"] = trusted_sources
        updated_payload_json = json.dumps(updated_payload, ensure_ascii=False)
        updated_image_url = kept_images[0] if kept_images else ""
        stats["changedProducts"] += 1
        stats["removedMainRefs"] += len(removed_images)

        if args.apply:
            referenced_urls: list[str] = []
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
                referenced_urls = crawler_service.collect_local_product_image_urls(updated_payload)
                session.flush()
            crawler_service.remove_unused_local_product_images(product_id, referenced_urls)

        if stats["processed"] % max(1, args.progress_every) == 0:
            print(json.dumps(stats, ensure_ascii=False), flush=True)

    if args.apply:
        stats["backup"] = str(backup_path)
    print(json.dumps(stats, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
