from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from sqlalchemy import func, select

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.db.database import session_scope
from app.db.models import ProductModel
from app.services.crawler_service import prepare_collected_product_for_listing


logger = logging.getLogger("listing-preparation-backfill")
STOP_REQUESTED = False


def request_stop(_signum: int, _frame: object) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True
    logger.warning("stop requested; finishing current batch")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Rakuten listing preparation caches.")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--state-file", type=Path, default=Path("data/maintenance/listing-preparation-backfill.json"))
    parser.add_argument("--max-products", type=int, default=0)
    return parser.parse_args()


def load_state(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError):
        return {
            "lastId": 0,
            "total": 0,
            "processed": 0,
            "success": 0,
            "failed": 0,
            "failedIds": [],
        }
    failed_ids = payload.get("failedIds")
    return {
        "lastId": max(0, int(payload.get("lastId") or 0)),
        "total": max(0, int(payload.get("total") or 0)),
        "processed": max(0, int(payload.get("processed") or 0)),
        "success": max(0, int(payload.get("success") or 0)),
        "failed": max(0, int(payload.get("failed") or 0)),
        "failedIds": [
            int(product_id)
            for product_id in failed_ids
            if isinstance(product_id, int) or str(product_id).isdigit()
        ] if isinstance(failed_ids, list) else [],
    }


def save_state(path: Path, state: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def product_ids_after(last_id: int, batch_size: int) -> list[int]:
    with session_scope() as session:
        return [
            int(product_id)
            for product_id in session.scalars(
                select(ProductModel.id)
                .where(
                    ProductModel.id > int(last_id),
                    ProductModel.parent_product_id.is_(None),
                    ProductModel.store_id.is_(None),
                    ProductModel.review_status.in_(("pending", "approved", "error", "listed_master")),
                )
                .order_by(ProductModel.id.asc())
                .limit(max(1, int(batch_size)))
            ).all()
        ]


def total_product_count() -> int:
    with session_scope() as session:
        return int(
            session.scalar(
                select(func.count())
                .select_from(ProductModel)
                .where(
                    ProductModel.parent_product_id.is_(None),
                    ProductModel.store_id.is_(None),
                    ProductModel.review_status.in_(("pending", "approved", "error", "listed_master")),
                )
            )
            or 0
        )


def process_product(product_id: int) -> bool:
    try:
        result = prepare_collected_product_for_listing(None, product_id)
        return bool(result) and int(result.get("missingImageCount") or 0) == 0
    except Exception:
        logger.exception("product preparation failed product_id=%s", product_id)
        return False


def process_batch(product_ids: list[int], workers: int) -> tuple[int, list[int]]:
    failed_ids: list[int] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="listing-prep") as executor:
        futures = {executor.submit(process_product, product_id): product_id for product_id in product_ids}
        for future in as_completed(futures):
            if not future.result():
                failed_ids.append(futures[future])
    return len(product_ids) - len(failed_ids), failed_ids


def run() -> int:
    global STOP_REQUESTED
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logging.getLogger("oss2").setLevel(logging.WARNING)
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    state = load_state(args.state_file)
    if not int(state["total"]):
        state["total"] = total_product_count()
        save_state(args.state_file, state)
    workers = max(1, int(args.workers))
    processed_this_run = 0

    while not STOP_REQUESTED:
        if args.max_products and processed_this_run >= args.max_products:
            break
        batch_size = max(1, int(args.batch_size))
        if args.max_products:
            batch_size = min(batch_size, args.max_products - processed_this_run)
        product_ids = product_ids_after(int(state["lastId"]), batch_size)
        if not product_ids:
            break
        success_count, failed_ids = process_batch(product_ids, workers)
        state["lastId"] = max(product_ids)
        state["processed"] = int(state["processed"]) + len(product_ids)
        state["success"] = int(state["success"]) + success_count
        known_failed_ids = {
            int(product_id)
            for product_id in state.get("failedIds", [])
        }
        known_failed_ids.update(failed_ids)
        state["failedIds"] = sorted(known_failed_ids)
        state["failed"] = len(known_failed_ids)
        processed_this_run += len(product_ids)
        save_state(args.state_file, state)
        logger.info(
            "backfill progress last_id=%s batch=%s processed=%s success=%s failed=%s",
            state["lastId"],
            len(product_ids),
            state["processed"],
            state["success"],
            state["failed"],
        )
        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    if not STOP_REQUESTED and not args.max_products and state.get("failedIds"):
        retry_ids = [int(product_id) for product_id in state["failedIds"]]
        logger.info("retrying failed products count=%s", len(retry_ids))
        retry_success_count, retry_failed_ids = process_batch(retry_ids, workers)
        state["success"] = int(state["success"]) + retry_success_count
        state["failedIds"] = sorted(retry_failed_ids)
        state["failed"] = len(retry_failed_ids)

    save_state(args.state_file, state)
    logger.info("backfill finished state=%s", state)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
