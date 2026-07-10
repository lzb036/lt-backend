from __future__ import annotations

import tempfile
import unittest
import hashlib
import subprocess
import sys
from pathlib import Path

from app.services.product_image_storage import ObjectFingerprint
from scripts.migrate_product_images_to_oss import (
    delete_verified_file,
    migrate_file,
    run_migration,
)


class FakeMigrationStorage:
    def __init__(self):
        self.sizes: dict[str, int] = {}
        self.put_calls: list[tuple[str, Path, str]] = []
        self.failures_remaining = 0
        self.reported_size_after_put: int | None = None
        self.sha256: dict[str, str] = {}

    def object_fingerprint(self, key: str) -> ObjectFingerprint | None:
        size = self.sizes.get(key)
        if size is None:
            return None
        return ObjectFingerprint(size=size, sha256=self.sha256.get(key, ""))

    def put_file(self, key: str, source: Path, content_type: str, sha256: str = "") -> None:
        self.put_calls.append((key, source, content_type))
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise RuntimeError("temporary upload failure")
        self.sizes[key] = (
            self.reported_size_after_put
            if self.reported_size_after_put is not None
            else source.stat().st_size
        )
        self.sha256[key] = sha256


class ProductImageMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.source = Path(self.temp_dir.name) / "1" / "photo.jpg"
        self.source.parent.mkdir(parents=True)
        self.source.write_bytes(b"image-data")
        self.storage = FakeMigrationStorage()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_matching_remote_size_is_skipped(self):
        key = "product-images/1/photo.jpg"
        digest = hashlib.sha256(self.source.read_bytes()).hexdigest()
        self.storage.sizes[key] = self.source.stat().st_size
        self.storage.sha256[key] = digest

        result = migrate_file(
            self.storage,
            self.source,
            key,
            retries=3,
        )

        self.assertEqual(result.status, "skipped")
        self.assertEqual(self.storage.put_calls, [])
        self.assertTrue(self.source.exists())

    def test_same_size_with_wrong_hash_is_uploaded_and_verified(self):
        key = "product-images/1/photo.jpg"
        self.storage.sizes[key] = self.source.stat().st_size
        self.storage.sha256[key] = "wrong"

        result = migrate_file(
            self.storage,
            self.source,
            key,
            retries=3,
        )

        self.assertEqual(result.status, "uploaded")
        self.assertEqual(len(self.storage.put_calls), 1)
        self.assertEqual(result.size, self.source.stat().st_size)
        self.assertEqual(self.storage.sha256[key], result.sha256)

    def test_upload_retries_transient_failures(self):
        self.storage.failures_remaining = 1

        result = migrate_file(
            self.storage,
            self.source,
            "product-images/1/photo.jpg",
            retries=2,
        )

        self.assertEqual(result.status, "uploaded")
        self.assertEqual(len(self.storage.put_calls), 2)

    def test_size_mismatch_fails_upload_verification(self):
        self.storage.reported_size_after_put = self.source.stat().st_size - 1

        result = migrate_file(
            self.storage,
            self.source,
            "product-images/1/photo.jpg",
            retries=1,
        )

        self.assertEqual(result.status, "failed")
        self.assertTrue(self.source.exists())

    def test_verified_manifest_and_remote_hash_can_delete_local_file(self):
        key = "product-images/1/photo.jpg"
        digest = hashlib.sha256(self.source.read_bytes()).hexdigest()
        self.storage.sizes[key] = self.source.stat().st_size
        self.storage.sha256[key] = digest

        result = delete_verified_file(
            self.storage,
            self.source,
            key,
            expected_size=self.source.stat().st_size,
            expected_sha256=digest,
        )

        self.assertEqual(result.status, "deleted")
        self.assertFalse(self.source.exists())

    def test_manifest_hash_mismatch_never_deletes_local_file(self):
        key = "product-images/1/photo.jpg"
        digest = hashlib.sha256(self.source.read_bytes()).hexdigest()
        self.storage.sizes[key] = self.source.stat().st_size
        self.storage.sha256[key] = digest

        result = delete_verified_file(
            self.storage,
            self.source,
            key,
            expected_size=self.source.stat().st_size,
            expected_sha256="wrong",
        )

        self.assertEqual(result.status, "failed")
        self.assertTrue(self.source.exists())

    def test_completed_migration_writes_manifest_used_by_delete_phase(self):
        manifest = Path(self.temp_dir.name) / "manifest.jsonl"
        source_root = self.source.parents[1]

        upload_summary = run_migration(
            self.storage,
            source_root=source_root,
            prefix="product-images",
            workers=1,
            retries=1,
            verify_only=False,
            delete_local=False,
            progress_every=0,
            manifest_path=manifest,
        )
        delete_summary = run_migration(
            self.storage,
            source_root=source_root,
            prefix="product-images",
            workers=1,
            retries=1,
            verify_only=False,
            delete_local=True,
            progress_every=0,
            manifest_path=manifest,
        )

        self.assertEqual(upload_summary["failed"], [])
        self.assertTrue(manifest.is_file())
        self.assertEqual(delete_summary["statuses"], {"deleted": 1})
        self.assertFalse(self.source.exists())

    def test_incomplete_manifest_is_rejected_before_any_local_file_is_deleted(self):
        source_root = self.source.parents[1]
        second_source = source_root / "2" / "second.jpg"
        second_source.parent.mkdir(parents=True)
        second_source.write_bytes(b"second-image")
        first_key = "product-images/1/photo.jpg"
        second_key = "product-images/2/second.jpg"
        first_digest = hashlib.sha256(self.source.read_bytes()).hexdigest()
        second_digest = hashlib.sha256(second_source.read_bytes()).hexdigest()
        self.storage.sizes.update(
            {
                first_key: self.source.stat().st_size,
                second_key: second_source.stat().st_size,
            }
        )
        self.storage.sha256.update(
            {
                first_key: first_digest,
                second_key: second_digest,
            }
        )
        manifest = Path(self.temp_dir.name) / "manifest.jsonl"
        manifest.write_text(
            (
                '{"key":"product-images/1/photo.jpg",'
                f'"size":{self.source.stat().st_size},'
                f'"sha256":"{first_digest}"}}'
            )
            + "\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(RuntimeError, "没有覆盖当前图片目录"):
            run_migration(
                self.storage,
                source_root=source_root,
                prefix="product-images",
                workers=1,
                retries=1,
                verify_only=False,
                delete_local=True,
                progress_every=0,
                manifest_path=manifest,
            )

        self.assertTrue(self.source.exists())
        self.assertTrue(second_source.exists())

    def test_script_can_run_directly_from_repository_root(self):
        project_root = Path(__file__).resolve().parents[1]

        result = subprocess.run(
            [sys.executable, "scripts/migrate_product_images_to_oss.py", "--help"],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
