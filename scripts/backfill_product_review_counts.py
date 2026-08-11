from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import select

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.db.database import SessionLocal, init_database
from app.db.models import ProductModel
from app.services import crawler_service


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill product review counts from Rakuten shop listing pages. "
            "Default is dry-run."
        ),
    )
    parser.add_argument("--sid", required=True, help="Rakuten shop SID.")
    parser.add_argument(
        "--status",
        default="pending",
        choices=("pending", "approved", "error", "listed_master", "listed"),
        help="Only update products with this review status.",
    )
    parser.add_argument(
        "--collection-source",
        default="manual",
        choices=("manual", "scheduled"),
        help="Only update products from this collection source.",
    )
    parser.add_argument("--owner", default="", help="Optional owner username.")
    parser.add_argument("--apply", action="store_true", help="Persist matched review counts.")
    return parser.parse_args(argv)


def canonical_source_url(value: Any) -> str:
    source_url = crawler_service.normalize_text(value)
    if not source_url:
        return ""
    try:
        return crawler_service.normalize_rakuten_product_target(source_url)
    except RuntimeError:
        return source_url.split("?", 1)[0]


def listing_review_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        source_url = canonical_source_url(item.get("source_url"))
        raw_payload = item.get("raw") if isinstance(item.get("raw"), dict) else {}
        review_count = crawler_service.product_review_count(raw_payload)
        if source_url and review_count is not None:
            counts[source_url] = review_count
    return counts


def product_query(args: argparse.Namespace):
    query = select(ProductModel).where(
        ProductModel.review_status == args.status,
        ProductModel.collection_source == args.collection_source,
        ProductModel.review_count.is_(None),
    )
    owner_username = crawler_service.normalize_text(args.owner)
    if owner_username:
        query = query.where(ProductModel.owner_username == owner_username)
    return query


def load_candidates(args: argparse.Namespace) -> list[dict[str, Any]]:
    session = SessionLocal()
    try:
        return [
            {
                "id": int(product.id),
                "sourceUrl": canonical_source_url(product.source_url),
            }
            for product in session.scalars(product_query(args)).all()
        ]
    finally:
        session.close()


def update_product_review_counts(
    args: argparse.Namespace,
    matched_counts: dict[int, int],
) -> int:
    if not args.apply or not matched_counts:
        return 0
    session = SessionLocal()
    try:
        rows = session.scalars(
            select(ProductModel).where(
                ProductModel.id.in_(list(matched_counts)),
                ProductModel.review_count.is_(None),
            )
        ).all()
        updated_count = 0
        for row in rows:
            review_count = matched_counts.get(int(row.id))
            if review_count is None:
                continue
            row.review_count = review_count
            raw_payload = crawler_service.product_raw_payload(row)
            raw_payload["reviewCount"] = review_count
            row.raw_payload_json = json.dumps(raw_payload, ensure_ascii=False)
            updated_count += 1
        session.commit()
        return updated_count
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    init_database()

    candidates = load_candidates(args)
    candidate_ids_by_url: dict[str, list[int]] = {}
    for candidate in candidates:
        source_url = candidate["sourceUrl"]
        if source_url:
            candidate_ids_by_url.setdefault(source_url, []).append(candidate["id"])

    listing_items = crawler_service.collect_whole_shop_listing_items(
        crawler_service.normalize_text(args.sid),
        "all",
        {"operator": "all"},
        None,
    )
    discovered_counts = listing_review_counts(listing_items)
    matched_counts = {
        product_id: discovered_counts[source_url]
        for source_url, product_ids in candidate_ids_by_url.items()
        if source_url in discovered_counts
        for product_id in product_ids
    }
    positive_count = sum(value > 0 for value in matched_counts.values())
    zero_count = sum(value == 0 for value in matched_counts.values())
    updated_count = update_product_review_counts(args, matched_counts)

    summary = {
        "apply": bool(args.apply),
        "sid": crawler_service.normalize_text(args.sid),
        "status": args.status,
        "collectionSource": args.collection_source,
        "owner": crawler_service.normalize_text(args.owner) or None,
        "candidateCount": len(candidates),
        "listingItemCount": len(listing_items),
        "discoveredReviewCount": len(discovered_counts),
        "matchedCount": len(matched_counts),
        "positiveCount": positive_count,
        "zeroCount": zero_count,
        "unmatchedCount": len(candidates) - len(matched_counts),
        "updatedCount": updated_count,
    }
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
