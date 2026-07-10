from __future__ import annotations

import hashlib
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import quote, unquote, urlsplit

from app.core.config import settings

PRODUCT_IMAGE_URL_PREFIX = "/api/static/product-images"
DRAFT_IMAGE_URL_PREFIX = "/api/static/product-image-drafts"
PRODUCT_IMAGE_OBJECT_PREFIX = "product-images"
DRAFT_IMAGE_OBJECT_PREFIX = "product-image-drafts"


@dataclass(frozen=True)
class StoredProductImage:
    kind: str
    product_id: int
    filename: str

    @property
    def object_key(self) -> str:
        return f"{self.kind}/{self.product_id}/{self.filename}"


@dataclass(frozen=True)
class StoredObject:
    key: str
    size: int
    last_modified: int


@dataclass(frozen=True)
class ObjectFingerprint:
    size: int
    sha256: str
    last_modified: int = 0


@dataclass
class ObjectStream:
    body: Any
    size: int
    max_bytes: int

    def __iter__(self):
        read_size = 0
        try:
            while True:
                chunk = self.body.read(256 * 1024)
                if not chunk:
                    break
                read_size += len(chunk)
                if read_size > self.max_bytes:
                    raise RuntimeError("图片大小不能超过允许的限制。")
                yield chunk
        finally:
            close = getattr(self.body, "close", None)
            if callable(close):
                close()


def parse_product_image_url(image_url: str) -> StoredProductImage | None:
    text = str(image_url or "").strip()
    if not text:
        return None
    path = urlsplit(text).path
    prefix_to_kind = (
        (DRAFT_IMAGE_URL_PREFIX, DRAFT_IMAGE_OBJECT_PREFIX),
        (PRODUCT_IMAGE_URL_PREFIX, PRODUCT_IMAGE_OBJECT_PREFIX),
    )
    for prefix, kind in prefix_to_kind:
        if not path.startswith(f"{prefix}/"):
            continue
        relative = path.removeprefix(prefix).lstrip("/")
        parts = [unquote(part) for part in relative.split("/") if part]
        if len(parts) != 2:
            return None
        product_id_text, filename = parts
        if not product_id_text.isdigit():
            return None
        if not filename or filename in {".", ".."} or "/" in filename or "\\" in filename:
            return None
        return StoredProductImage(
            kind=kind,
            product_id=int(product_id_text),
            filename=filename,
        )
    return None


class ProductImageStorage:
    def __init__(
        self,
        *,
        enabled: bool,
        bucket_name: str = "",
        endpoint: str = "",
        region: str = "",
        role_name: str = "",
        connect_timeout: int = 10,
        write_retries: int = 3,
        bucket_factory: Callable[[], Any] | None = None,
        object_iterator_factory: Callable[[Any, str], Iterable[Any]] | None = None,
    ) -> None:
        self.enabled = bool(enabled)
        self.bucket_name = str(bucket_name or "").strip()
        self.endpoint = self._normalize_endpoint(endpoint)
        self.region = str(region or "").strip()
        self.role_name = str(role_name or "").strip()
        self.connect_timeout = max(1, int(connect_timeout or 10))
        self.write_retries = max(1, int(write_retries or 1))
        self._bucket_factory = bucket_factory
        self._object_iterator_factory = object_iterator_factory
        self._thread_local = threading.local()
        self._retry_sleep = time.sleep

    @staticmethod
    def _normalize_endpoint(endpoint: str) -> str:
        text = str(endpoint or "").strip().rstrip("/")
        if text and not text.startswith(("http://", "https://")):
            text = f"https://{text}"
        return text

    def _build_bucket(self) -> Any:
        if self._bucket_factory is not None:
            return self._bucket_factory()
        if not all((self.bucket_name, self.endpoint, self.region, self.role_name)):
            raise RuntimeError("OSS 图片存储配置不完整。")
        try:
            import oss2
        except ImportError as exc:
            raise RuntimeError("服务器缺少 oss2，不能使用 OSS 图片存储。") from exc
        metadata_url = (
            "http://100.100.100.200/latest/meta-data/ram/security-credentials/"
            f"{quote(self.role_name, safe='')}"
        )
        provider = oss2.EcsRamRoleCredentialsProvider(
            metadata_url,
            timeout=self.connect_timeout,
        )
        auth = oss2.ProviderAuth(provider)
        return oss2.Bucket(
            auth,
            self.endpoint,
            self.bucket_name,
            connect_timeout=self.connect_timeout,
            app_name="lt-product-image-storage",
            region=self.region,
        )

    def _bucket(self) -> Any:
        if not self.enabled:
            raise RuntimeError("OSS 图片存储未启用。")
        bucket = getattr(self._thread_local, "bucket", None)
        if bucket is None:
            bucket = self._build_bucket()
            self._thread_local.bucket = bucket
        return bucket

    @staticmethod
    def _is_not_found(exc: Exception) -> bool:
        if int(getattr(exc, "status", 0) or 0) == 404:
            return True
        return exc.__class__.__name__ in {"NoSuchKey", "NotFound"}

    @staticmethod
    def _is_retryable_write_error(exc: Exception) -> bool:
        status = int(getattr(exc, "status", 0) or 0)
        if status == 429 or 500 <= status < 600:
            return True
        return exc.__class__.__name__ == "RequestError"

    def _run_idempotent_write(self, operation: Callable[[], Any]) -> Any:
        for attempt in range(self.write_retries):
            try:
                return operation()
            except Exception as exc:
                if (
                    attempt + 1 >= self.write_retries
                    or not self._is_retryable_write_error(exc)
                ):
                    raise
                self._retry_sleep(min(2 ** attempt, 5))
        raise RuntimeError("OSS 写入重试状态异常。")

    def put_bytes(self, key: str, content: bytes, content_type: str = "") -> None:
        headers = {
            "x-oss-meta-sha256": hashlib.sha256(content).hexdigest(),
        }
        if content_type:
            headers["Content-Type"] = content_type
        self._run_idempotent_write(
            lambda: self._bucket().put_object(key, content, headers=headers)
        )

    def put_file(
        self,
        key: str,
        source: Path,
        content_type: str = "",
        sha256: str = "",
    ) -> None:
        digest = sha256 or file_sha256(source)
        headers = {"x-oss-meta-sha256": digest}
        if content_type:
            headers["Content-Type"] = content_type
        self._run_idempotent_write(
            lambda: self._bucket().put_object_from_file(key, str(source), headers=headers)
        )

    def read_bytes(self, key: str, *, max_bytes: int) -> bytes:
        stream = self.open_stream(key, max_bytes=max_bytes)
        if stream is None:
            raise FileNotFoundError(key)
        return b"".join(stream)

    def open_stream(self, key: str, *, max_bytes: int) -> ObjectStream | None:
        try:
            result = self._bucket().get_object(key)
        except Exception as exc:
            if self._is_not_found(exc):
                return None
            raise
        content_length = int(getattr(result, "content_length", 0) or 0)
        if content_length > max_bytes:
            close = getattr(result, "close", None)
            if callable(close):
                close()
            raise RuntimeError("图片大小不能超过允许的限制。")
        return ObjectStream(
            body=result,
            size=content_length,
            max_bytes=max_bytes,
        )

    def exists(self, key: str) -> bool:
        if not self.enabled:
            return False
        try:
            return bool(self._bucket().object_exists(key))
        except Exception as exc:
            if self._is_not_found(exc):
                return False
            raise

    def object_size(self, key: str) -> int | None:
        fingerprint = self.object_fingerprint(key)
        return fingerprint.size if fingerprint is not None else None

    def object_fingerprint(self, key: str) -> ObjectFingerprint | None:
        if not self.enabled:
            return None
        try:
            result = self._bucket().head_object(key)
        except Exception as exc:
            if self._is_not_found(exc):
                return None
            raise
        headers = getattr(result, "headers", {}) or {}
        sha256 = ""
        for header_name, value in headers.items():
            if str(header_name).lower() == "x-oss-meta-sha256":
                sha256 = str(value or "").strip().lower()
                break
        return ObjectFingerprint(
            size=int(getattr(result, "content_length", 0) or 0),
            sha256=sha256,
            last_modified=int(getattr(result, "last_modified", 0) or 0),
        )

    def copy(self, source_key: str, target_key: str) -> None:
        self._run_idempotent_write(
            lambda: self._bucket().copy_object(self.bucket_name, source_key, target_key)
        )

    def delete(self, key: str) -> None:
        if not self.enabled:
            return
        self._run_idempotent_write(
            lambda: self._bucket().delete_object(key)
        )

    def list_objects(self, prefix: str) -> list[StoredObject]:
        if not self.enabled:
            return []
        bucket = self._bucket()
        if self._object_iterator_factory is not None:
            iterator = self._object_iterator_factory(bucket, prefix)
        else:
            try:
                import oss2
            except ImportError as exc:
                raise RuntimeError("服务器缺少 oss2，不能使用 OSS 图片存储。") from exc
            iterator = oss2.ObjectIterator(bucket, prefix=prefix, max_keys=1000)
        return [
            StoredObject(
                key=str(item.key),
                size=int(getattr(item, "size", 0) or 0),
                last_modified=int(getattr(item, "last_modified", 0) or 0),
            )
            for item in iterator
        ]

    def delete_prefix(self, prefix: str) -> int:
        objects = self.list_objects(prefix)
        for item in objects:
            self.delete(item.key)
        return len(objects)

    def health_check(self) -> bool:
        if not self.enabled:
            return True
        key = f"_healthcheck/{uuid.uuid4().hex}.txt"
        probe = b"ok"
        try:
            self.put_bytes(key, probe, "text/plain")
            content = self.read_bytes(key, max_bytes=len(probe))
            self.delete(key)
            return content == probe
        except Exception:
            try:
                self.delete(key)
            except Exception:
                pass
            return False


product_image_storage = ProductImageStorage(
    enabled=settings.product_image_storage == "oss",
    bucket_name=settings.oss_bucket,
    endpoint=settings.oss_endpoint,
    region=settings.oss_region,
    role_name=settings.oss_ecs_role_name,
    connect_timeout=settings.oss_connect_timeout_seconds,
)


def file_sha256(source: Path) -> str:
    digest = hashlib.sha256()
    with source.open("rb") as file:
        while True:
            chunk = file.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
