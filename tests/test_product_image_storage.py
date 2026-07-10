from __future__ import annotations

import threading
import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.services.product_image_storage import (
    DRAFT_IMAGE_URL_PREFIX,
    ObjectFingerprint,
    PRODUCT_IMAGE_URL_PREFIX,
    ProductImageStorage,
    StoredObject,
    parse_product_image_url,
)


@dataclass
class FakeObject:
    key: str
    size: int
    last_modified: int


class FakeGetResult:
    def __init__(self, content: bytes):
        self._content = content
        self._offset = 0
        self.content_length = len(content)
        self.headers = {"Content-Type": "image/jpeg"}
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self._content) - self._offset
        chunk = self._content[self._offset:self._offset + size]
        self._offset += len(chunk)
        return chunk

    def close(self) -> None:
        self.closed = True


class FakeBucket:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.headers: dict[str, dict[str, str]] = {}
        self.put_calls: list[tuple[str, bytes, dict[str, str]]] = []
        self.delete_calls: list[str] = []
        self.copy_calls: list[tuple[str, str, str]] = []
        self.put_file_calls: list[tuple[str, str, dict[str, str]]] = []

    def put_object(self, key: str, content: bytes, headers=None):
        normalized_headers = dict(headers or {})
        self.objects[key] = bytes(content)
        self.headers[key] = normalized_headers
        self.put_calls.append((key, bytes(content), normalized_headers))
        return object()

    def get_object(self, key: str):
        if key not in self.objects:
            raise FakeNotFoundError()
        return FakeGetResult(self.objects[key])

    def put_object_from_file(self, key: str, filename: str, headers=None):
        normalized_headers = dict(headers or {})
        self.objects[key] = Path(filename).read_bytes()
        self.headers[key] = normalized_headers
        self.put_file_calls.append((key, filename, normalized_headers))
        return object()

    def object_exists(self, key: str) -> bool:
        return key in self.objects

    def head_object(self, key: str):
        if key not in self.objects:
            raise FakeNotFoundError()
        return type(
            "HeadResult",
            (),
            {
                "content_length": len(self.objects[key]),
                "headers": self.headers.get(key, {}),
            },
        )()

    def copy_object(self, source_bucket: str, source_key: str, target_key: str):
        if source_key not in self.objects:
            raise FakeNotFoundError()
        self.objects[target_key] = self.objects[source_key]
        self.headers[target_key] = dict(self.headers.get(source_key, {}))
        self.copy_calls.append((source_bucket, source_key, target_key))
        return object()

    def delete_object(self, key: str):
        self.objects.pop(key, None)
        self.delete_calls.append(key)
        return object()

    def list_objects(self, prefix: str = "", max_keys: int = 100):
        return object()


class FakeNotFoundError(Exception):
    status = 404


class FakeStorageError(Exception):
    def __init__(self, status: int):
        self.status = status
        super().__init__(f"storage error {status}")


class ServerError(FakeStorageError):
    pass


class ProductImageUrlTests(unittest.TestCase):
    def test_parses_permanent_image_url_to_deterministic_object_key(self):
        image = parse_product_image_url(f"{PRODUCT_IMAGE_URL_PREFIX}/42/photo%20one.jpg?x=1")

        self.assertIsNotNone(image)
        self.assertEqual(image.kind, "product-images")
        self.assertEqual(image.product_id, 42)
        self.assertEqual(image.filename, "photo one.jpg")
        self.assertEqual(image.object_key, "product-images/42/photo one.jpg")

    def test_parses_draft_image_url(self):
        image = parse_product_image_url(f"{DRAFT_IMAGE_URL_PREFIX}/9/draft.png")

        self.assertIsNotNone(image)
        self.assertEqual(image.kind, "product-image-drafts")
        self.assertEqual(image.object_key, "product-image-drafts/9/draft.png")

    def test_rejects_unsafe_or_malformed_image_urls(self):
        invalid_urls = [
            f"{PRODUCT_IMAGE_URL_PREFIX}/not-a-number/image.jpg",
            f"{PRODUCT_IMAGE_URL_PREFIX}/1/a/b.jpg",
            f"{PRODUCT_IMAGE_URL_PREFIX}/1/%2E%2E",
            "/other/1/image.jpg",
        ]

        for image_url in invalid_urls:
            with self.subTest(image_url=image_url):
                self.assertIsNone(parse_product_image_url(image_url))


class ProductImageStorageTests(unittest.TestCase):
    def setUp(self):
        self.bucket = FakeBucket()
        self.created_buckets: list[tuple[int, FakeBucket]] = []

        def bucket_factory():
            self.created_buckets.append((threading.get_ident(), self.bucket))
            return self.bucket

        self.storage = ProductImageStorage(
            enabled=True,
            bucket_name="lt-product-images-prod-8350",
            endpoint="https://oss-ap-northeast-1-internal.aliyuncs.com",
            region="ap-northeast-1",
            role_name="AliyunOSSFullAccess",
            bucket_factory=bucket_factory,
            object_iterator_factory=self._iter_objects,
        )
        self.storage._retry_sleep = lambda _seconds: None

    def _iter_objects(self, _bucket, prefix: str):
        timestamp = int(datetime(2026, 7, 10, tzinfo=timezone.utc).timestamp())
        return [
            FakeObject(key=key, size=len(content), last_modified=timestamp)
            for key, content in sorted(self.bucket.objects.items())
            if key.startswith(prefix)
        ]

    def test_disabled_storage_does_not_create_a_bucket(self):
        storage = ProductImageStorage(enabled=False)

        self.assertFalse(storage.exists("product-images/1/a.jpg"))
        self.assertEqual(storage.list_objects("product-images/"), [])

    def test_put_read_exists_and_size_use_the_bucket(self):
        self.storage.put_bytes("product-images/1/a.jpg", b"image", "image/jpeg")

        self.assertTrue(self.storage.exists("product-images/1/a.jpg"))
        self.assertEqual(self.storage.object_size("product-images/1/a.jpg"), 5)
        self.assertEqual(self.storage.read_bytes("product-images/1/a.jpg", max_bytes=10), b"image")
        self.assertEqual(
            self.bucket.put_calls,
            [
                (
                    "product-images/1/a.jpg",
                    b"image",
                    {
                        "Content-Type": "image/jpeg",
                        "x-oss-meta-sha256": "6105d6cc76af400325e94d588ce511be5bfdbb73b437dc51eca43917d7a43e3d",
                    },
                )
            ],
        )
        self.assertEqual(
            self.storage.object_fingerprint("product-images/1/a.jpg"),
            ObjectFingerprint(
                size=5,
                sha256="6105d6cc76af400325e94d588ce511be5bfdbb73b437dc51eca43917d7a43e3d",
            ),
        )

    def test_put_file_uploads_without_loading_through_the_caller(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "photo.png"
            source.write_bytes(b"file-content")

            self.storage.put_file("product-images/1/photo.png", source, "image/png")

        self.assertEqual(self.bucket.objects["product-images/1/photo.png"], b"file-content")
        self.assertEqual(
            self.bucket.put_file_calls[0][2],
            {
                "Content-Type": "image/png",
                "x-oss-meta-sha256": "2239ce4df9ee8db012834642ec801b55ba2c92b28bdd11f4d73d9c55d39f3b0a",
            },
        )

    def test_put_bytes_retries_retryable_server_errors(self):
        original_put_object = self.bucket.put_object
        attempts = 0

        def flaky_put_object(key: str, content: bytes, headers=None):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise FakeStorageError(503)
            return original_put_object(key, content, headers=headers)

        self.bucket.put_object = flaky_put_object

        self.storage.put_bytes("product-images/1/retry.jpg", b"image", "image/jpeg")

        self.assertEqual(attempts, 3)
        self.assertEqual(self.bucket.objects["product-images/1/retry.jpg"], b"image")

    def test_put_bytes_does_not_retry_non_retryable_client_errors(self):
        attempts = 0

        def rejected_put_object(_key: str, _content: bytes, headers=None):
            nonlocal attempts
            attempts += 1
            raise FakeStorageError(400)

        self.bucket.put_object = rejected_put_object

        with self.assertRaises(FakeStorageError):
            self.storage.put_bytes("product-images/1/rejected.jpg", b"image", "image/jpeg")

        self.assertEqual(attempts, 1)

    def test_put_bytes_does_not_retry_generic_server_error_with_4xx_status(self):
        attempts = 0

        def rejected_put_object(_key: str, _content: bytes, headers=None):
            nonlocal attempts
            attempts += 1
            raise ServerError(409)

        self.bucket.put_object = rejected_put_object

        with self.assertRaises(ServerError):
            self.storage.put_bytes("product-images/1/conflict.jpg", b"image", "image/jpeg")

        self.assertEqual(attempts, 1)

    def test_read_enforces_maximum_size(self):
        self.bucket.objects["product-images/1/large.jpg"] = b"12345"

        with self.assertRaisesRegex(RuntimeError, "图片大小不能超过"):
            self.storage.read_bytes("product-images/1/large.jpg", max_bytes=4)

    def test_open_stream_yields_content_and_closes_result(self):
        self.bucket.objects["product-images/1/a.jpg"] = b"streamed"

        stream = self.storage.open_stream("product-images/1/a.jpg", max_bytes=20)

        self.assertIsNotNone(stream)
        self.assertEqual(stream.size, 8)
        self.assertEqual(b"".join(stream), b"streamed")
        self.assertTrue(stream.body.closed)

    def test_open_stream_returns_none_for_missing_object(self):
        self.assertIsNone(
            self.storage.open_stream("product-images/1/missing.jpg", max_bytes=20)
        )

    def test_missing_objects_return_false_or_none(self):
        self.assertFalse(self.storage.exists("product-images/1/missing.jpg"))
        self.assertIsNone(self.storage.object_size("product-images/1/missing.jpg"))

    def test_copy_and_delete_object(self):
        self.bucket.objects["product-image-drafts/1/draft.jpg"] = b"draft"

        self.storage.copy(
            "product-image-drafts/1/draft.jpg",
            "product-images/1/saved.jpg",
        )
        self.storage.delete("product-image-drafts/1/draft.jpg")

        self.assertEqual(self.bucket.objects["product-images/1/saved.jpg"], b"draft")
        self.assertNotIn("product-image-drafts/1/draft.jpg", self.bucket.objects)

    def test_delete_retries_retryable_server_errors(self):
        self.bucket.objects["product-images/1/delete.jpg"] = b"delete"
        original_delete_object = self.bucket.delete_object
        attempts = 0

        def flaky_delete_object(key: str):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise FakeStorageError(503)
            return original_delete_object(key)

        self.bucket.delete_object = flaky_delete_object

        self.storage.delete("product-images/1/delete.jpg")

        self.assertEqual(attempts, 3)
        self.assertNotIn("product-images/1/delete.jpg", self.bucket.objects)

    def test_list_and_delete_prefix(self):
        self.bucket.objects.update(
            {
                "product-images/1/a.jpg": b"a",
                "product-images/1/b.jpg": b"bb",
                "product-images/2/c.jpg": b"ccc",
            }
        )

        objects = self.storage.list_objects("product-images/1/")
        deleted = self.storage.delete_prefix("product-images/1/")

        self.assertEqual(
            objects,
            [
                StoredObject("product-images/1/a.jpg", 1, objects[0].last_modified),
                StoredObject("product-images/1/b.jpg", 2, objects[1].last_modified),
            ],
        )
        self.assertEqual(deleted, 2)
        self.assertEqual(set(self.bucket.objects), {"product-images/2/c.jpg"})

    def test_bucket_is_cached_per_thread(self):
        self.storage.exists("product-images/1/a.jpg")
        self.storage.exists("product-images/1/a.jpg")

        self.assertEqual(len(self.created_buckets), 1)

    def test_health_check_verifies_write_read_and_delete(self):
        self.assertTrue(self.storage.health_check())
        self.assertEqual(self.bucket.objects, {})
        self.assertEqual(len(self.bucket.put_calls), 1)
        self.assertEqual(len(self.bucket.delete_calls), 1)


if __name__ == "__main__":
    unittest.main()
