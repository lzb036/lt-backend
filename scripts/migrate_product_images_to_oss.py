from __future__ import annotations

import argparse
import json
import mimetypes
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import settings
from app.services.product_image_storage import (
    ProductImageStorage,
    file_sha256,
    product_image_storage,
)


@dataclass(frozen=True)
class MigrationResult:
    source: str
    key: str
    size: int
    status: str
    sha256: str = ""
    error: str = ""


def migrate_file(
    storage: ProductImageStorage,
    source: Path,
    key: str,
    *,
    retries: int,
) -> MigrationResult:
    try:
        local_size = source.stat().st_size
    except OSError as exc:
        return MigrationResult(str(source), key, 0, "failed", error=str(exc))

    try:
        sha256 = file_sha256(source)
        remote = storage.object_fingerprint(key)
    except Exception as exc:
        return MigrationResult(str(source), key, local_size, "failed", error=str(exc))

    uploaded = False
    if remote is None or remote.size != local_size or remote.sha256 != sha256:
        content_type = mimetypes.guess_type(source.name)[0] or "application/octet-stream"
        last_error: Exception | None = None
        for attempt in range(max(1, int(retries or 1))):
            try:
                storage.put_file(key, source, content_type, sha256=sha256)
                uploaded = True
                break
            except Exception as exc:
                last_error = exc
                if attempt + 1 < max(1, int(retries or 1)):
                    time.sleep(min(2 ** attempt, 5))
        if not uploaded:
            return MigrationResult(
                str(source),
                key,
                local_size,
                "failed",
                sha256=sha256,
                error=str(last_error or "上传失败"),
            )

        try:
            remote = storage.object_fingerprint(key)
        except Exception as exc:
            return MigrationResult(
                str(source),
                key,
                local_size,
                "failed",
                sha256=sha256,
                error=str(exc),
            )

    if remote is None or remote.size != local_size or remote.sha256 != sha256:
        return MigrationResult(
            str(source),
            key,
            local_size,
            "failed",
            sha256=sha256,
            error=(
                "远端指纹不一致："
                f"local_size={local_size}, remote_size={getattr(remote, 'size', None)}, "
                f"local_sha256={sha256}, remote_sha256={getattr(remote, 'sha256', None)}"
            ),
        )
    return MigrationResult(
        str(source),
        key,
        local_size,
        "uploaded" if uploaded else "skipped",
        sha256=sha256,
    )


def verify_file(storage: ProductImageStorage, source: Path, key: str) -> MigrationResult:
    try:
        local_size = source.stat().st_size
        sha256 = file_sha256(source)
        remote = storage.object_fingerprint(key)
    except Exception as exc:
        return MigrationResult(str(source), key, 0, "failed", error=str(exc))
    if remote is None or remote.size != local_size or remote.sha256 != sha256:
        return MigrationResult(
            str(source),
            key,
            local_size,
            "failed",
            sha256=sha256,
            error=(
                "远端指纹不一致："
                f"local_size={local_size}, remote_size={getattr(remote, 'size', None)}, "
                f"local_sha256={sha256}, remote_sha256={getattr(remote, 'sha256', None)}"
            ),
        )
    return MigrationResult(str(source), key, local_size, "verified", sha256=sha256)


def delete_verified_file(
    storage: ProductImageStorage,
    source: Path,
    key: str,
    *,
    expected_size: int,
    expected_sha256: str,
) -> MigrationResult:
    try:
        local_size = source.stat().st_size
        local_sha256 = file_sha256(source)
        remote = storage.object_fingerprint(key)
    except Exception as exc:
        return MigrationResult(str(source), key, 0, "failed", error=str(exc))
    if local_size != expected_size or local_sha256 != expected_sha256:
        return MigrationResult(
            str(source),
            key,
            local_size,
            "failed",
            sha256=local_sha256,
            error="本地文件与迁移 manifest 不一致。",
        )
    if remote is None or remote.size != expected_size or remote.sha256 != expected_sha256:
        return MigrationResult(
            str(source),
            key,
            local_size,
            "failed",
            sha256=local_sha256,
            error="远端对象与迁移 manifest 不一致。",
        )
    try:
        source.unlink()
    except OSError as exc:
        return MigrationResult(
            str(source),
            key,
            local_size,
            "failed",
            sha256=local_sha256,
            error=str(exc),
        )
    return MigrationResult(str(source), key, local_size, "deleted", sha256=local_sha256)


def source_files(source_root: Path) -> list[Path]:
    return sorted(
        path
        for path in source_root.rglob("*")
        if path.is_file() and not path.is_symlink()
    )


def object_key(source_root: Path, source: Path, prefix: str) -> str:
    relative = source.relative_to(source_root).as_posix()
    return f"{prefix.strip('/')}/{relative}"


def remove_empty_directories(source_root: Path) -> None:
    directories = sorted(
        (path for path in source_root.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    )
    for directory in directories:
        try:
            directory.rmdir()
        except OSError:
            continue


def load_manifest(path: Path) -> dict[str, dict[str, object]]:
    records: dict[str, dict[str, object]] = {}
    with path.open("r", encoding="utf-8") as manifest:
        for line_number, line in enumerate(manifest, start=1):
            text = line.strip()
            if not text:
                continue
            record = json.loads(text)
            key = str(record.get("key") or "")
            sha256 = str(record.get("sha256") or "")
            size = int(record.get("size") or 0)
            if not key or len(sha256) != 64 or size < 0:
                raise RuntimeError(f"迁移 manifest 第 {line_number} 行无效。")
            records[key] = {"key": key, "size": size, "sha256": sha256}
    return records


def run_migration(
    storage: ProductImageStorage,
    *,
    source_root: Path,
    prefix: str,
    workers: int,
    retries: int,
    verify_only: bool,
    delete_local: bool,
    progress_every: int,
    manifest_path: Path,
) -> dict[str, object]:
    partial_manifest_path = manifest_path.with_suffix(f"{manifest_path.suffix}.partial")
    excluded_paths = {manifest_path.resolve(), partial_manifest_path.resolve()}
    files = [
        source
        for source in source_files(source_root)
        if source.resolve() not in excluded_paths
    ]
    counters: Counter[str] = Counter()
    total_bytes = 0
    failures: list[dict[str, object]] = []
    manifest_records = load_manifest(manifest_path) if delete_local else {}
    if delete_local:
        missing_manifest_keys = [
            object_key(source_root, source, prefix)
            for source in files
            if object_key(source_root, source, prefix) not in manifest_records
        ]
        if missing_manifest_keys:
            preview = ", ".join(missing_manifest_keys[:3])
            raise RuntimeError(
                "迁移 manifest 没有覆盖当前图片目录中的全部文件："
                f"缺少 {len(missing_manifest_keys)} 个对象"
                f"{f'（例如 {preview}）' if preview else ''}。"
            )
    manifest_file = None
    if not delete_local:
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_file = partial_manifest_path.open("w", encoding="utf-8", newline="\n")

    def process(source: Path) -> MigrationResult:
        key = object_key(source_root, source, prefix)
        if delete_local:
            record = manifest_records.get(key)
            if record is None:
                return MigrationResult(
                    str(source),
                    key,
                    source.stat().st_size,
                    "failed",
                    error="迁移 manifest 中没有该文件。",
                )
            return delete_verified_file(
                storage,
                source,
                key,
                expected_size=int(record["size"]),
                expected_sha256=str(record["sha256"]),
            )
        if verify_only:
            return verify_file(storage, source, key)
        return migrate_file(
            storage,
            source,
            key,
            retries=retries,
        )

    try:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            for index, result in enumerate(executor.map(process, files), start=1):
                counters[result.status] += 1
                total_bytes += result.size
                if result.status == "failed":
                    failures.append(asdict(result))
                    print(json.dumps(asdict(result), ensure_ascii=False), file=sys.stderr, flush=True)
                elif manifest_file is not None:
                    manifest_file.write(
                        json.dumps(
                            {
                                "key": result.key,
                                "size": result.size,
                                "sha256": result.sha256,
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                if progress_every > 0 and index % progress_every == 0:
                    print(
                        json.dumps(
                            {
                                "processed": index,
                                "total": len(files),
                                "statuses": dict(counters),
                            },
                            ensure_ascii=False,
                        ),
                        flush=True,
                    )
    finally:
        if manifest_file is not None:
            manifest_file.close()

    if delete_local:
        remove_empty_directories(source_root)
    elif not failures:
        os.replace(partial_manifest_path, manifest_path)
    return {
        "source": str(source_root),
        "prefix": prefix,
        "files": len(files),
        "bytes": total_bytes,
        "statuses": dict(counters),
        "failed": failures,
        "manifest": str(manifest_path),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Migrate local product images to OSS with size/SHA-256 verification.",
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=settings.backend_dir / "data" / "product-images",
    )
    parser.add_argument("--prefix", default="product-images")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--verify-only", action="store_true", help="Verify remote fingerprints without uploading.")
    parser.add_argument("--delete-local", action="store_true", help="Delete only files verified by the completed manifest.")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=settings.backend_dir / "data" / "product-images-oss-manifest.jsonl",
        help="JSONL manifest written by a successful migration or verification pass.",
    )
    parser.add_argument("--progress-every", type=int, default=500)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    source_root = args.source.resolve()
    if not source_root.exists() or not source_root.is_dir():
        print(f"图片目录不存在：{source_root}", file=sys.stderr)
        return 2
    if not product_image_storage.enabled:
        print("LT_PRODUCT_IMAGE_STORAGE 必须配置为 oss。", file=sys.stderr)
        return 2
    manifest_path = args.manifest.resolve()
    if args.delete_local and args.verify_only:
        print("--delete-local 不能和 --verify-only 同时使用。", file=sys.stderr)
        return 2
    if args.delete_local and not manifest_path.is_file():
        print(f"删除本地文件前必须提供完整迁移 manifest：{manifest_path}", file=sys.stderr)
        return 2
    summary = run_migration(
        product_image_storage,
        source_root=source_root,
        prefix=args.prefix,
        workers=max(1, args.workers),
        retries=max(1, args.retries),
        verify_only=bool(args.verify_only),
        delete_local=bool(args.delete_local),
        progress_every=max(0, args.progress_every),
        manifest_path=manifest_path,
    )
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 1 if summary["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
