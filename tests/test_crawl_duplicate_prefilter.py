from __future__ import annotations

import unittest
from unittest.mock import patch

from app.db.models import make_source_url_hash
from app.services import crawler_service


class CrawlDuplicatePrefilterTests(unittest.TestCase):
    def test_existing_product_skips_detail_request(self) -> None:
        existing_url = "https://item.rakuten.co.jp/example/existing/"
        new_url = "https://item.rakuten.co.jp/example/new/"
        items = [
            {"title": "existing", "source_url": existing_url, "raw": {"pageUrl": "list"}},
            {"title": "new", "source_url": new_url, "raw": {"pageUrl": "list"}},
        ]

        with patch.object(
            crawler_service,
            "collect_product_detail",
            return_value={"title": "new detail", "source_url": new_url, "raw": {}},
        ) as collect_detail:
            result = crawler_service.enrich_collected_items_with_detail(
                items,
                existing_source_hashes={make_source_url_hash(existing_url)},
            )

        collect_detail.assert_called_once_with(new_url)
        self.assertEqual(result[0]["source_url"], existing_url)
        self.assertEqual(result[1]["title"], "new detail")


if __name__ == "__main__":
    unittest.main()
