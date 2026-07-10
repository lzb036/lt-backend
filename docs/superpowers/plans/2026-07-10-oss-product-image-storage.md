# OSS Product Image Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Store all current and future product images in the private Alibaba
Cloud OSS bucket while preserving existing application image URLs.

**Architecture:** A focused product-image storage service maps existing URLs to
OSS keys and authenticates with the ECS RAM role. Crawler and API code use that
service for writes, reads, finalization, and cleanup, while local files remain a
temporary read fallback during migration.

**Tech Stack:** Python 3.11+, FastAPI, `oss2==2.19.1`, ECS RAM role credentials,
standard-library `unittest`, Alibaba Cloud OSS internal endpoint.

## Global Constraints

- Do not start an additional frontend or backend process.
- Preserve `/api/static/product-images/...` and
  `/api/static/product-image-drafts/...` URLs.
- Do not store long-lived AccessKeys.
- Do not delete a local production image before remote size and sample-content
  verification.
- Keep the bucket private.

---

### Task 1: OSS configuration and URL mapping

**Files:**
- Create: `app/services/product_image_storage.py`
- Modify: `app/core/config.py`
- Modify: `.env.example`
- Modify: `requirements.txt`
- Test: `tests/test_product_image_storage.py`

**Interfaces:**
- Produces: `StoredProductImage`, `parse_product_image_url(url)`,
  `ProductImageStorage`, and module singleton `product_image_storage`.

- [ ] **Step 1: Write failing mapping and disabled-storage tests**

Create tests asserting that permanent and draft URLs map to deterministic
object keys, invalid paths are rejected, and disabled storage reports no remote
object.

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```powershell
python -m unittest tests.test_product_image_storage -v
```

Expected: import failure because `app.services.product_image_storage` does not
exist.

- [ ] **Step 3: Implement configuration and mapping**

Add settings:

```text
LT_PRODUCT_IMAGE_STORAGE=local|oss
LT_OSS_BUCKET
LT_OSS_ENDPOINT
LT_OSS_REGION
LT_OSS_ECS_ROLE_NAME
LT_OSS_CONNECT_TIMEOUT_SECONDS
LT_PRODUCT_IMAGE_ORPHAN_RETENTION_DAYS
```

Implement strict URL parsing and key generation. Lazily import `oss2` so fake
clients can be used in unit tests. When OSS mode is enabled, normalize bare
endpoints to HTTPS and reject explicitly non-HTTPS endpoints.

- [ ] **Step 4: Run the focused test and verify GREEN**

Run the same unittest command and expect all Task 1 tests to pass.

### Task 2: OSS object operations and fallback reads

**Files:**
- Modify: `app/services/product_image_storage.py`
- Modify: `tests/test_product_image_storage.py`

**Interfaces:**
- Produces: `put_bytes`, `put_file`, `read_bytes`, `open_stream`, `exists`,
  `object_fingerprint`, `copy`, `delete`, `delete_prefix`, `list_objects`, and
  `health_check`.

- [ ] **Step 1: Add failing fake-bucket behavior tests**

Cover content-type propagation, bounded reads, not-found handling, copy/delete,
prefix deletion, and thread-local client creation.

- [ ] **Step 2: Run the tests and verify expected failures**

Run:

```powershell
python -m unittest tests.test_product_image_storage -v
```

- [ ] **Step 3: Implement minimal OSS operations**

Use `oss2.ProviderAuth(oss2.EcsRamRoleCredentialsProvider(metadata_url))`,
`oss2.Bucket`, CRC-enabled uploads, SHA-256 object metadata, bounded streaming
reads, not-found detection, and bounded retries for idempotent upload, copy,
and delete operations on network, 429, and retryable 5xx failures.

- [ ] **Step 4: Run focused tests and verify GREEN**

All storage service tests must pass.

### Task 3: Integrate product image writes and reads

**Files:**
- Modify: `app/services/crawler_service.py`
- Test: `tests/test_crawler_product_image_storage.py`

**Interfaces:**
- Consumes: the Task 2 storage singleton.
- Produces: OSS-aware implementations of existing crawler image helpers.

- [ ] **Step 1: Add failing crawler helper tests**

Patch the storage singleton with a fake and assert:

- uploads write directly to OSS when enabled;
- remote localization writes to OSS;
- OSS reads precede local fallback;
- missing OSS objects fall back to local files;
- missing remote and local objects are reported as missing.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```powershell
python -m unittest tests.test_crawler_product_image_storage -v
```

- [ ] **Step 3: Integrate minimal storage calls**

Refactor existing helpers without changing their public URL return values.
Keep filesystem behavior when OSS is disabled.

- [ ] **Step 4: Run both test modules and verify GREEN**

Run:

```powershell
python -m unittest discover -s tests -v
```

### Task 4: Draft finalization and cleanup

**Files:**
- Modify: `app/services/crawler_service.py`
- Modify: `tests/test_crawler_product_image_storage.py`

**Interfaces:**
- Produces OSS-aware `finalize_product_image_url`,
  `remove_local_product_image_if_unused`,
  `remove_unused_local_product_images`,
  `clear_product_temp_image_files`, and
  `cleanup_expired_product_image_drafts`.

- [ ] **Step 1: Add failing copy and cleanup tests**

Assert draft-to-permanent copy with post-commit source deletion,
referenced-object preservation, per-product deletion, ownership validation, and
expiry-based draft deletion.

- [ ] **Step 2: Run and verify RED**

Run the crawler storage tests and confirm failures are caused by missing OSS
cleanup behavior.

- [ ] **Step 3: Implement cleanup behavior**

Stage irreversible cleanup until after database commit. Delete OSS objects
before local fallbacks. Keep existing local cleanup for disabled mode and
migration rollback. Run localization's local fallback cleanup only after its
database commit succeeds. Never prefix-delete objects during concurrent
localization. Orphan cleanup must delete only old object keys captured by its
initial snapshot, re-check the database, and re-read each object's
`last_modified` before deletion.

- [ ] **Step 4: Run all tests and verify GREEN**

Run unittest discovery.

### Task 5: Stable HTTP image routes and health checks

**Files:**
- Modify: `app/main.py`
- Modify: `app/api/crawler.py`
- Modify: `tests/test_crawler_product_image_storage.py`

**Interfaces:**
- Produces same-path GET/HEAD/OPTIONS image routes and OSS-aware download
responses.

- [ ] **Step 1: Add failing response-info tests**

Assert OSS-backed product downloads return bounded content and correct media
types while third-party URLs retain remote streaming behavior.

- [ ] **Step 2: Run and verify RED**

Run crawler storage tests.

- [ ] **Step 3: Replace static mounts**

Stream local-style image URLs through existing FastAPI routes, preserving CORS
headers and local fallback. HEAD reads metadata only. Add a real OSS
write/read/delete probe to health checks.

- [ ] **Step 4: Run all tests and compile**

Run:

```powershell
python -m unittest discover -s tests -v
python -m compileall app
```

### Task 6: Resumable migration command

**Files:**
- Create: `scripts/migrate_product_images_to_oss.py`
- Test: `tests/test_product_image_migration.py`

**Interfaces:**
- Produces a CLI that uploads, verifies, reports, and optionally deletes
verified local files.

- [ ] **Step 1: Add failing migration decision tests**

Cover skip-on-matching size/SHA-256, upload-on-missing-or-fingerprint-mismatch,
bounded retry, manifest generation, and delete-only-after-manifest verification.

- [ ] **Step 2: Run and verify RED**

Run:

```powershell
python -m unittest tests.test_product_image_migration -v
```

- [ ] **Step 3: Implement the migration CLI**

Arguments:

```text
--source
--prefix
--workers
--retries
--verify-only
--delete-local
--manifest
--progress-every
```

Use deterministic keys and remote size/SHA-256 verification. Write a complete
JSONL manifest only after a failure-free pass. Require that manifest before
local deletion, and reject the delete phase before deleting anything when the
manifest does not cover every file currently in the source directory. Print
machine-readable final counters and return nonzero when any file fails.

- [ ] **Step 4: Run all tests and compile scripts**

Run unittest discovery and `python -m compileall app scripts`.

### Task 7: Local verification and repository delivery

**Files:**
- Verify all modified files.

- [ ] **Step 1: Run complete verification**

```powershell
python -m unittest discover -s tests -v
python -m compileall app scripts
git diff --check
git status --short
```

- [ ] **Step 2: Review the diff against the design**

Confirm stable URLs, private-bucket access, local fallback, draft lifecycle,
cleanup, and migration safety are all represented.

- [ ] **Step 3: Commit and push**

Commit the backend changes and push `main` so the production repository can
pull the exact verified revision.

### Task 8: Production deployment and migration

**Files:**
- Modify production `.env` outside Git.
- Execute the committed migration command.

- [ ] **Step 1: Back up production configuration**

Copy the current `.env` to a timestamped backup and add the OSS settings without
printing existing secrets.

- [ ] **Step 2: Pull and install**

Pull `main` in `/www/wwwroot/lt/lt-backend` and install the pinned requirements
inside the existing virtual environment.

- [ ] **Step 3: Reload managed services**

Reload or restart the existing `lt-backend.service` and existing worker
supervisor entries. Do not launch duplicate processes.

- [ ] **Step 4: Verify production storage behavior**

Check health and perform an isolated OSS write/read/delete probe through the
application storage service.

- [ ] **Step 5: Migrate without deletion**

Run the migration command against `data/product-images`, compare local and
remote counts/bytes, and run deterministic sampled hashes.

- [ ] **Step 6: Delete verified local files**

Run the same command with `--delete-local`; only files covered by the completed
manifest and matching the current local and OSS size/SHA-256 fingerprints may
remove their local source.

- [ ] **Step 7: Final production verification**

Confirm health, selected existing image reads, product image downloads, worker
processes, OSS object totals, and reclaimed disk space.
