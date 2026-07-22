import sys

from app.db.database import wait_for_database_ready
from app.core.task_queue import run_worker
from app.services.crawler_service import LOCAL_PRODUCT_IMAGE_DIR, LOCAL_PRODUCT_IMAGE_DRAFT_DIR


if __name__ == "__main__":
    LOCAL_PRODUCT_IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_PRODUCT_IMAGE_DRAFT_DIR.mkdir(parents=True, exist_ok=True)
    wait_for_database_ready()
    run_worker(sys.argv[1:] or None)
