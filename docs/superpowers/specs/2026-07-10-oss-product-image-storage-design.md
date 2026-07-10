# OSS Product Image Storage Design

## Goal

Move all persisted product images from the production server filesystem to the
private Alibaba Cloud OSS bucket `lt-product-images-prod-8350`, and store all
future product and draft images in that bucket without changing the image URLs
already persisted in product records and description HTML.

## Constraints

- The production ECS instance and OSS bucket are both in `ap-northeast-1`.
- The application authenticates through the attached ECS RAM role
  `AliyunOSSFullAccess`; no long-lived AccessKey is stored.
- Server-side traffic uses
  `https://oss-ap-northeast-1-internal.aliyuncs.com`.
- The bucket remains private and uses OSS-managed AES256 encryption.
- Existing URLs under `/api/static/product-images/...` and
  `/api/static/product-image-drafts/...` remain stable.
- No additional frontend or backend process may be started for verification.
- Existing managed services may be reloaded or restarted through their current
  service managers when deployment requires it.
- No local production image is deleted until its OSS object has been verified.

## Architecture

Create a focused `product_image_storage` service. It maps stable application
image URLs to deterministic OSS object keys:

- `/api/static/product-images/{product_id}/{filename}` becomes
  `product-images/{product_id}/{filename}`.
- `/api/static/product-image-drafts/{product_id}/{filename}` becomes
  `product-image-drafts/{product_id}/{filename}`.

The service obtains rotating temporary credentials from the ECS metadata
endpoint and uses the OSS internal endpoint for object operations. Each process
thread owns its own OSS client so request sessions are not shared across worker
threads. OSS mode rejects non-HTTPS endpoints.

## Read Behavior

For stable application image URLs:

1. Read the matching OSS object when OSS storage is enabled and the object
   exists.
2. Fall back to the existing local file while migration is incomplete.
3. Return a missing-image error only when neither source is available.

The existing FastAPI static mounts are replaced by GET/HEAD routes that stream
image bytes through the same application URL. HEAD requests use object metadata
without downloading the body. This preserves same-origin behavior for the
image editor and avoids exposing private or expiring OSS URLs.

Remote third-party image URLs keep the existing HTTP download behavior.

## Write Behavior

When OSS storage is enabled:

- New product images and drafts are written directly to OSS.
- Remote images localized during crawling are written directly to OSS.
- Saving a draft copies the draft object to the permanent prefix and removes
  the draft object only after the database transaction commits.
- Replaced, removed, orphaned, expired, and product-deletion images are deleted
  from OSS and from any remaining local fallback path after the database
  transaction commits.
- A failed OSS write fails the operation instead of silently creating a new
  local-only image.
- Internal image URLs are rejected when their product ID differs from the
  product being edited.
- Localization never performs prefix-wide OSS deletion, preventing concurrent
  workers from deleting each other's newly uploaded objects.
- OSS-only orphan prefixes have a seven-day grace period before cleanup so an
  uncommitted concurrent write cannot be mistaken for abandoned data.
- Orphan cleanup deletes only old keys from its initial listing snapshot,
  re-checks that the product is still absent, and revalidates each object's
  modification time before deletion. It never re-lists and deletes the whole
  prefix after making the orphan decision.

When OSS storage is disabled, the current filesystem behavior remains available
for local development and rollback.

## Migration

The migration is resumable because each local file maps to one deterministic
object key. Each migrated object stores a SHA-256 metadata value. A file is
considered verified only when the OSS object size and SHA-256 metadata match
the local file.

Production migration order:

1. Deploy dual-read and OSS-write code with local fallback enabled.
2. Confirm the managed backend and workers can read and write a probe object.
3. Upload `data/product-images` to the `product-images/` prefix.
4. Write a complete JSONL verification manifest containing each object key,
   size, and SHA-256 value.
5. Compare local and remote object counts and total bytes and verify hashes.
6. Exercise existing image, download, edit, and listing code paths through
   backend-level checks without opening the UI.
7. Delete only local files that match the completed manifest and whose current
   local and OSS size/SHA-256 fingerprints still match.
8. Remove empty product directories and report reclaimed disk space.

The historical draft directory is effectively empty and is not migrated.
Before deletion begins, the completed manifest must cover every file that is
currently present under the migration source directory. A missing record stops
the delete phase before any local file is removed.

## Failure Handling

- Missing OSS objects fall back to local files during migration.
- OSS authentication, timeout, or service failures are surfaced when no local
  fallback is available.
- Upload and migration operations use bounded retries.
- Idempotent OSS uploads, copies, and deletes retry network, throttling, and
  retryable service failures without retrying permanent client errors.
- Cleanup deletes OSS objects first, then local fallback files.
- Localization removes obsolete local fallback files only after its database
  transaction commits.
- Migration can be rerun safely; matching size/SHA-256 objects are skipped.
- An incomplete or failed migration cannot replace the completed deletion
  manifest or start a partially covered delete pass.

## Verification

- Standard-library unit tests cover URL parsing, object-key mapping, OSS-first
  fallback reads, direct OSS writes, post-commit draft finalization, deletion,
  localization commit boundaries, stream closure, ownership validation, and
  manifest-gated migration.
- Backend verification runs without starting the application:
  `python -m unittest discover -s tests -v`,
  `python -m compileall app scripts`, and `git diff --check`.
- Production verification checks ECS role credentials, OSS list/write/read/
  delete operations, managed service health, object counts, total bytes,
  sampled hashes, and reclaimed filesystem space.
