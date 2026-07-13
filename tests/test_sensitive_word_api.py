from __future__ import annotations

import asyncio
import unittest
from io import BytesIO
from unittest.mock import patch
from urllib.parse import quote

from fastapi import HTTPException, UploadFile
from fastapi.routing import APIRoute

from app.core.auth import require_superadmin


class SensitiveWordApiTests(unittest.TestCase):
    def setUp(self) -> None:
        from app.api import crawler as crawler_api

        self.crawler_api = crawler_api
        self.superadmin = {"username": "superadmin", "role": "superadmin"}

    def test_sensitive_word_routes_require_superadmin(self) -> None:
        expected_routes = {
            ("GET", "/crawler/settings/sensitive-words"),
            ("POST", "/crawler/settings/sensitive-words"),
            ("GET", "/crawler/settings/sensitive-words/template"),
            ("POST", "/crawler/settings/sensitive-words/import"),
            ("PUT", "/crawler/settings/sensitive-words/{word_id}"),
            ("DELETE", "/crawler/settings/sensitive-words/{word_id}"),
        }

        actual_routes = {
            (method, route.path)
            for route in self.crawler_api.router.routes
            if isinstance(route, APIRoute)
            for method in route.methods
            if route.path.startswith("/crawler/settings/sensitive-words")
        }

        self.assertEqual(actual_routes, expected_routes)

        for method, path in expected_routes:
            route = next(
                route
                for route in self.crawler_api.router.routes
                if isinstance(route, APIRoute) and route.path == path and method in route.methods
            )
            dependency_calls = [dependency.call for dependency in route.dependant.dependencies]
            self.assertIn(require_superadmin, dependency_calls, f"{method} {path} should depend on require_superadmin")

    def test_list_sensitive_words_returns_service_result_and_query_params(self) -> None:
        service_result = {
            "items": [
                {
                    "id": 1,
                    "word": "即納",
                    "ruleType": "literal",
                    "enabled": True,
                    "createdAt": "2026-07-13T10:00:00",
                    "updatedAt": "2026-07-13T10:00:00",
                }
            ],
            "total": 1,
            "page": 2,
            "pageSize": 5,
        }

        with patch.object(self.crawler_api.sensitive_word_service, "list_sensitive_words", return_value=service_result) as mock_list:
            response = self.crawler_api.list_sensitive_words(page=2, pageSize=5, keyword="即", _=self.superadmin)

        self.assertEqual(response, service_result)
        mock_list.assert_called_once_with(page=2, page_size=5, keyword="即")

    def test_create_sensitive_word_returns_wrapped_service_result(self) -> None:
        payload = self.crawler_api.SensitiveWordPayload(word="翌日配達", enabled=False)
        created = {
            "id": 7,
            "word": "翌日配達",
            "ruleType": "literal",
            "enabled": False,
            "createdAt": "2026-07-13T10:00:00",
            "updatedAt": "2026-07-13T10:00:01",
        }

        with patch.object(self.crawler_api.sensitive_word_service, "create_sensitive_word", return_value=created) as mock_create:
            response = self.crawler_api.create_sensitive_word(payload, _=self.superadmin)

        self.assertEqual(response, {"sensitiveWord": created})
        mock_create.assert_called_once_with("翌日配達", False)

    def test_create_sensitive_word_maps_duplicate_to_conflict(self) -> None:
        payload = self.crawler_api.SensitiveWordPayload(word="即納", enabled=True)

        with patch.object(
            self.crawler_api.sensitive_word_service,
            "create_sensitive_word",
            side_effect=RuntimeError("敏感词已存在。"),
        ):
            with self.assertRaises(HTTPException) as context:
                self.crawler_api.create_sensitive_word(payload, _=self.superadmin)

        self.assertEqual(context.exception.status_code, 409)
        self.assertEqual(context.exception.detail, "敏感词已存在。")

    def test_create_sensitive_word_maps_service_validation_to_bad_request(self) -> None:
        payload = self.crawler_api.SensitiveWordPayload(word="   ", enabled=True)

        with patch.object(
            self.crawler_api.sensitive_word_service,
            "create_sensitive_word",
            side_effect=RuntimeError("敏感词不能为空。"),
        ):
            with self.assertRaises(HTTPException) as context:
                self.crawler_api.create_sensitive_word(payload, _=self.superadmin)

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "敏感词不能为空。")

    def test_update_sensitive_word_returns_wrapped_service_result(self) -> None:
        payload = self.crawler_api.SensitiveWordPayload(word="【】", enabled=True)
        updated = {
            "id": 9,
            "word": "【】",
            "ruleType": "bracket",
            "enabled": True,
            "createdAt": "2026-07-13T10:00:00",
            "updatedAt": "2026-07-13T11:00:00",
        }

        with patch.object(self.crawler_api.sensitive_word_service, "update_sensitive_word", return_value=updated) as mock_update:
            response = self.crawler_api.update_sensitive_word(9, payload, _=self.superadmin)

        self.assertEqual(response, {"sensitiveWord": updated})
        mock_update.assert_called_once_with(9, "【】", True)

    def test_update_sensitive_word_maps_missing_to_not_found(self) -> None:
        payload = self.crawler_api.SensitiveWordPayload(word="即納", enabled=True)

        with patch.object(
            self.crawler_api.sensitive_word_service,
            "update_sensitive_word",
            side_effect=RuntimeError("敏感词不存在。"),
        ):
            with self.assertRaises(HTTPException) as context:
                self.crawler_api.update_sensitive_word(123, payload, _=self.superadmin)

        self.assertEqual(context.exception.status_code, 404)
        self.assertEqual(context.exception.detail, "敏感词不存在。")

    def test_delete_sensitive_word_returns_deleted_flag(self) -> None:
        with patch.object(self.crawler_api.sensitive_word_service, "delete_sensitive_word", return_value=True) as mock_delete:
            response = self.crawler_api.delete_sensitive_word(8, _=self.superadmin)

        self.assertEqual(response, {"deleted": True})
        mock_delete.assert_called_once_with(8)

    def test_delete_sensitive_word_maps_missing_to_not_found(self) -> None:
        with patch.object(self.crawler_api.sensitive_word_service, "delete_sensitive_word", return_value=False):
            with self.assertRaises(HTTPException) as context:
                self.crawler_api.delete_sensitive_word(8, _=self.superadmin)

        self.assertEqual(context.exception.status_code, 404)
        self.assertEqual(context.exception.detail, "敏感词不存在。")

    def test_download_sensitive_word_template_uses_utf8_filename(self) -> None:
        content = b"template-content"
        encoded_filename = quote("敏感词导入模板.xlsx")
        fallback_filename = "sensitive-word-template.xlsx"

        with patch.object(
            self.crawler_api.sensitive_word_service,
            "build_sensitive_word_template",
            return_value=content,
        ) as mock_template:
            response = self.crawler_api.download_sensitive_word_template(_=self.superadmin)

        body = asyncio.run(self._read_streaming_response(response))

        self.assertEqual(body, content)
        self.assertEqual(
            response.media_type,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        self.assertEqual(
            response.headers["content-disposition"],
            f"attachment; filename={fallback_filename}; filename*=UTF-8''{encoded_filename}",
        )
        mock_template.assert_called_once_with()

    def test_import_sensitive_words_returns_service_result(self) -> None:
        import_result = {"createdCount": 2, "duplicateCount": 1, "invalidCount": 3}
        upload = UploadFile(filename="sensitive-words.xlsx", file=BytesIO(b"excel-content"))

        with patch.object(
            self.crawler_api.sensitive_word_service,
            "import_sensitive_words",
            return_value=import_result,
        ) as mock_import:
            response = asyncio.run(self.crawler_api.import_sensitive_words(upload, _=self.superadmin))

        self.assertEqual(response, import_result)
        mock_import.assert_called_once_with(b"excel-content", "sensitive-words.xlsx")

    def test_import_sensitive_words_maps_runtime_errors_to_bad_request(self) -> None:
        upload = UploadFile(filename="sensitive-words.xlsx", file=BytesIO(b""))

        with patch.object(
            self.crawler_api.sensitive_word_service,
            "import_sensitive_words",
            side_effect=RuntimeError("导入文件为空。"),
        ):
            with self.assertRaises(HTTPException) as context:
                asyncio.run(self.crawler_api.import_sensitive_words(upload, _=self.superadmin))

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "导入文件为空。")

    async def _read_streaming_response(self, response) -> bytes:
        chunks: list[bytes] = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)
        return b"".join(chunks)


if __name__ == "__main__":
    unittest.main()
