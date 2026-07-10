from __future__ import annotations

import unittest

from app.db.models import CrawlTaskModel


class CrawlDispatchModelTests(unittest.TestCase):
    def test_crawl_task_persists_reserved_queue_job_id(self):
        self.assertIn("queue_job_id", CrawlTaskModel.__table__.columns)
        column = CrawlTaskModel.__table__.columns["queue_job_id"]
        self.assertTrue(column.nullable)
        self.assertEqual(column.type.length, 64)


if __name__ == "__main__":
    unittest.main()
