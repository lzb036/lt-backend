from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from app.core.config import build_settings


class OssConfigTests(unittest.TestCase):
    def test_invalid_storage_mode_is_rejected(self):
        with patch.dict(
            os.environ,
            {
                "LT_DATABASE_URL": "sqlite:///test.db",
                "LT_PRODUCT_IMAGE_STORAGE": "typo",
            },
            clear=False,
        ):
            with self.assertRaisesRegex(RuntimeError, "LT_PRODUCT_IMAGE_STORAGE"):
                build_settings()

    def test_oss_mode_requires_all_connection_settings(self):
        with patch.dict(
            os.environ,
            {
                "LT_DATABASE_URL": "sqlite:///test.db",
                "LT_PRODUCT_IMAGE_STORAGE": "oss",
                "LT_OSS_BUCKET": "",
                "LT_OSS_ENDPOINT": "",
                "LT_OSS_REGION": "",
                "LT_OSS_ECS_ROLE_NAME": "",
            },
            clear=False,
        ):
            with self.assertRaisesRegex(RuntimeError, "LT_OSS_BUCKET"):
                build_settings()

    def test_oss_mode_rejects_non_https_endpoint(self):
        with patch.dict(
            os.environ,
            {
                "LT_DATABASE_URL": "sqlite:///test.db",
                "LT_PRODUCT_IMAGE_STORAGE": "oss",
                "LT_OSS_BUCKET": "product-images",
                "LT_OSS_ENDPOINT": "http://oss.example.com",
                "LT_OSS_REGION": "ap-northeast-1",
                "LT_OSS_ECS_ROLE_NAME": "example-role",
            },
            clear=False,
        ):
            with self.assertRaisesRegex(RuntimeError, "HTTPS"):
                build_settings()


if __name__ == "__main__":
    unittest.main()
