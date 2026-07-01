from app.db.database import init_database
from app.core.task_queue import run_worker
from app.services.crawler_service import LOCAL_PRODUCT_IMAGE_DIR, LOCAL_PRODUCT_IMAGE_DRAFT_DIR


if __name__ == "__main__":
    LOCAL_PRODUCT_IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_PRODUCT_IMAGE_DRAFT_DIR.mkdir(parents=True, exist_ok=True)
    init_database()
    run_worker()
