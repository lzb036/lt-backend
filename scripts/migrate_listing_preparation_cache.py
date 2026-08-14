from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy import select

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.db.database import Base, engine, session_scope
from app.db.models import ProductListingPreparationModel, ProductModel
from app.services.crawler_service import LISTING_PREPARATION_CACHE_KEY


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Move listing preparation caches out of lt_products.raw_payload_json.",
    )
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument(
        "--state-file",
        type=Path,
        default=Path("data/maintenance/listing-preparation-cache-migration.json"),
    )
    return parser.parse_args()


def load_last_id(path: Path) -> int:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError):
        return 0
    return max(0, int(payload.get("lastId") or 0))


def save_state(path: Path, *, last_id: int, scanned: int, migrated: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(
            {
                "lastId": int(last_id),
                "scanned": int(scanned),
                "migrated": int(migrated),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    temporary.replace(path)


def run() -> int:
    args = parse_args()
    Base.metadata.create_all(bind=engine)
    last_id = load_last_id(args.state_file)
    scanned = 0
    migrated = 0
    while True:
        with session_scope() as session:
            rows = session.scalars(
                select(ProductModel)
                .where(ProductModel.id > last_id)
                .order_by(ProductModel.id.asc())
                .limit(max(1, int(args.batch_size)))
            ).all()
            if not rows:
                break
            for product in rows:
                last_id = max(last_id, int(product.id))
                scanned += 1
                try:
                    raw_payload = json.loads(product.raw_payload_json or "{}")
                except (TypeError, ValueError):
                    continue
                if not isinstance(raw_payload, dict):
                    continue
                cache = raw_payload.pop(LISTING_PREPARATION_CACHE_KEY, None)
                if not isinstance(cache, dict):
                    continue
                source_fingerprint = str(cache.get("sourceFingerprint") or "").strip()
                if not source_fingerprint:
                    continue
                preparation = session.get(ProductListingPreparationModel, int(product.id))
                if preparation is None:
                    preparation = ProductListingPreparationModel(product_id=int(product.id))
                    session.add(preparation)
                preparation.source_fingerprint = source_fingerprint
                preparation.cache_json = json.dumps(cache, ensure_ascii=False)
                product.raw_payload_json = json.dumps(raw_payload, ensure_ascii=False)
                migrated += 1
        save_state(
            args.state_file,
            last_id=last_id,
            scanned=scanned,
            migrated=migrated,
        )
        print(
            json.dumps(
                {
                    "lastId": last_id,
                    "scanned": scanned,
                    "migrated": migrated,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
