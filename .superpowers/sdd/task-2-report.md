# Task 2 Report: Rakuten Read-Only Order Client

Date: 2026-07-16
Worktree: `D:\ERP\worktrees\lt-backend-ai-product-sales`
Branch: `feature/ai-product-sales-analysis`

## Scope

Owned implementation files:

- `app/services/rakuten_order_service.py`
- `tests/test_rakuten_order_service.py`

Requested report file:

- `.superpowers/sdd/task-2-report.md`

## Requirements Covered

Implemented the Task 2 Rakuten read-only order client with the requested public interfaces:

- `search_order_numbers(service_secret, license_key, start_at, end_at, statuses)`
- `get_orders(service_secret, license_key, order_numbers)`
- `iter_order_items(order)`

The implementation now:

- builds Rakuten `ESA base64(secret:key)` authorization headers
- uses `requests.Session.post` for read-only order API calls
- reuses the existing backend timeout setting via `settings.crawler_timeout_seconds`
- reuses the existing proxy convention via `crawler_request_proxies()`
- paginates `searchOrder`
- requests `getOrder` with `version = 7`
- batches `getOrder` calls with the verified RMS-safe cap of `30`
- normalizes package item output from `PackageModelList[*].ItemModelList[*]`
- raises concise `RuntimeError` messages for credential rejection, rate limiting, malformed JSON, missing pagination, and network failure
- avoids echoing credentials or remote response bodies in raised errors

## TDD Evidence

### Red

Created `tests/test_rakuten_order_service.py` first and ran:

```powershell
pytest tests/test_rakuten_order_service.py -v
```

Initial result:

- test collection failed because `app.services.rakuten_order_service` did not exist

Observed failure:

```text
ImportError: cannot import name 'rakuten_order_service' from 'app.services' (unknown location)
```

### Green

Implemented `app/services/rakuten_order_service.py` only after confirming the missing-module failure, then re-ran:

```powershell
pytest tests/test_rakuten_order_service.py -v
```

Result:

```text
8 passed in 0.86s
```

## Test Coverage Added

`tests/test_rakuten_order_service.py` now verifies:

1. `search_order_numbers(...)` paginates and sends the expected `ESA` authorization header
2. `search_order_numbers(...)` uses the current timeout and proxy conventions
3. `get_orders(...)` sends `version = 7`
4. `get_orders(...)` batches requests at or below the RMS-safe batch limit
5. `iter_order_items(...)` normalizes package item payloads into the required fields
6. credential rejection raises a concise redacted `RuntimeError`
7. missing pagination raises a concise `RuntimeError`
8. rate limiting raises a concise redacted `RuntimeError`
9. malformed JSON raises a concise `RuntimeError`
10. network failure raises a concise `RuntimeError`

## Implementation Notes

### `app/services/rakuten_order_service.py`

- Added focused constants for:
  - `RAKUTEN_ORDER_SEARCH_URL`
  - `RAKUTEN_ORDER_GET_URL`
  - `RAKUTEN_ORDER_SEARCH_PAGE_SIZE`
  - `RMS_SAFE_ORDER_BATCH_SIZE`
  - `RAKUTEN_ORDER_DETAIL_VERSION`
- Reused:
  - `build_rakuten_authorization_header(...)`
  - `crawler_request_proxies()`
  - `normalize_text(...)`
  - `first_text_from_keys(...)`
- Centralized POST + response handling in `_post_order_json(...)`
- Kept raised errors compact and body-free so secrets and remote payloads do not leak through exception text
- Normalized item iteration to emit:
  - `orderNumber`
  - `packagePosition`
  - `itemDetailId`
  - `manageNumber`
  - `itemNumber`
  - `SkuModelList`
  - `units`
  - `price`
  - `priceTaxIncl`
  - `deleteItemFlag`
  - `restoreInventoryFlag`

## Verification

Commands executed:

```powershell
pytest tests/test_rakuten_order_service.py -v
python -m compileall app/services/rakuten_order_service.py tests/test_rakuten_order_service.py
git diff --check -- app/services/rakuten_order_service.py tests/test_rakuten_order_service.py
```

Observed results:

- focused Task 2 tests passed
- compile check exited successfully
- `git diff --check` exited successfully

## Self-Review

What I checked:

- edits are limited to the requested service and test files, plus this requested report
- the service stays read-only and only uses POST to Rakuten read endpoints
- timeout and proxy behavior follow current backend conventions instead of introducing new request settings
- raised errors do not include Secret, Key, or remote response body text
- the batching, pagination, and item-normalization paths are all covered by focused tests

## Concerns

- The order-client behavior was validated through mocked HTTP contract tests and compile checks only. No live RMS request was made in this task.
- The `RMS_SAFE_ORDER_BATCH_SIZE = 30` ceiling is intentionally conservative and test-covered, but it was not validated against a live Rakuten sandbox or production credential in this task.

## Review Fix Pass

Date: 2026-07-16

### Findings fixed

1. Changed `getOrder` batching from `100` to the verified existing ceiling `30`.
   - Added a literal `30` assertion in tests.
   - Verified the batch split is `30 / 30 / 5` for a 65-order request.

2. Treated `totalPages = 0` as a valid empty `searchOrder` result.
   - Empty order searches now return `[]` instead of raising a pagination error.

3. Reworked pagination progression to use a local monotonic request-page counter.
   - The client now advances by the locally requested page number, not the remote echo.
   - It validates stale or malformed pagination responses and fails fast instead of looping or skipping pages.

4. Added a stable `SortModelList` to `searchOrder` requests.
   - The request now sends a deterministic sort payload to keep pagination ordering stable.

5. Separated `itemDetailId` and `itemId` handling in `iter_order_items(...)`.
   - `itemId` no longer backfills `itemDetailId`.
   - When `itemDetailId` is missing, the client now emits:
     - `packagePosition`
     - `lineFingerprintInputs`
     - `lineFingerprint`

6. Added `MessageModelList` error inspection for RMS API responses.
   - `ERROR` entries are categorized into:
     - credential failure
     - rate limiting
     - general API error
   - Raised messages stay generic and do not include raw remote message text.

7. Tightened redaction by removing chained causes.
   - Request and JSON parsing failures now raise with `from None`.
   - Tests inspect both `__cause__` and rendered traceback output to confirm no leaked chained detail.

### Additional test coverage

`tests/test_rakuten_order_service.py` now also verifies:

1. `searchOrder` accepts `totalPages = 0` as a valid empty result.
2. `searchOrder` rejects stale `requestPage` echoes without looping.
3. `searchOrder` rejects `totalPages` values below the requested page.
4. `searchOrder` includes a stable `SortModelList`.
5. `getOrder` uses a literal `30` batch limit.
6. `iter_order_items(...)` keeps `itemDetailId` and `itemId` separate.
7. `iter_order_items(...)` emits deterministic fallback fingerprint fields when `itemDetailId` is missing.
8. `MessageModelList` credential errors are categorized and redacted.
9. `MessageModelList` rate-limit errors are categorized and redacted.
10. `MessageModelList` general API errors are categorized and redacted.
11. Network failures suppress chained causes and redact traceback output.
12. Malformed JSON suppresses chained causes and redact traceback output.

### Verification

Commands executed:

```powershell
pytest tests/test_rakuten_order_service.py -v
pytest -q
python -m compileall app/services/rakuten_order_service.py tests/test_rakuten_order_service.py
git diff --check -- app/services/rakuten_order_service.py tests/test_rakuten_order_service.py .superpowers/sdd/task-2-report.md
```

Observed results:

- `pytest tests/test_rakuten_order_service.py -v` -> `13 passed in 0.86s`
- `pytest -q` -> `241 passed, 2 warnings, 4 subtests passed in 10.01s`
- compile check exited successfully
- `git diff --check` exited `0`; only Git's existing LF-to-CRLF working-copy warnings were printed

### Remaining concern

- This pass hardened the mocked HTTP contract and traceback-redaction behavior, but it still did not exercise a live Rakuten RMS response with real credentials.

## Review Fix Pass 2

Date: 2026-07-16

### Findings fixed

1. Moved `SortModelList` into `PaginationRequestModel`.
   - The payload now uses the verified numeric codes:
     - `sortColumn = 1`
     - `sortDirection = 2`
   - The top-level request body no longer carries `SortModelList`.

2. Tightened the `totalPages = 0` rule.
   - It is now accepted only on requested page `1` with no prior collected results.
   - A later zero-page response is treated as malformed and raises instead of discarding already collected order numbers.

3. Kept HTTP status precedence over body messages.
   - `401` and `403` always map to credential failure first.
   - `429` always maps to rate limiting first.
   - `MessageModelList` inspection only applies after those status checks.

4. Reworked fallback line fingerprints.
   - The fallback fingerprint now excludes mutable `units` and status flags.
   - It uses structured JSON serialization with sorted keys and stable list ordering.
   - Fingerprint inputs now include:
     - `itemId`
     - `itemNumber`
     - `manageNumber`
     - canonical SKU JSON
     - `price`
     - `priceTaxIncl`
     - `packagePosition`
     - `linePosition`

5. Tightened strict redaction semantics.
   - Request and JSON parsing failures are now caught, the `except` scope is exited, and then a fresh sanitized `RuntimeError` is raised.
   - The resulting exception now clears both:
     - `__cause__`
     - `__context__`
   - Tests verify both object fields and formatted traceback output.

### Additional test coverage

This pass added focused assertions for:

1. nested numeric `SortModelList` placement under `PaginationRequestModel`
2. rejection of `totalPages = 0` after earlier successful page collection
3. `401` status winning over a body-level rate-limit message
4. `429` status winning over a body-level credential message
5. canonical SKU JSON with sorted dict keys and stable list order
6. fallback fingerprints staying unchanged when only `units` and status flags differ
7. cleared `__context__` for sanitized request and JSON parse failures

### Verification

Commands executed:

```powershell
pytest tests/test_rakuten_order_service.py -v
pytest -q
python -m compileall app/services/rakuten_order_service.py tests/test_rakuten_order_service.py
git diff --check -- app/services/rakuten_order_service.py tests/test_rakuten_order_service.py .superpowers/sdd/task-2-report.md
```

Observed results:

- `pytest tests/test_rakuten_order_service.py -v` -> `17 passed in 0.89s`
- `pytest -q` -> `245 passed, 2 warnings, 4 subtests passed in 10.21s`
- compile check exited successfully
- `git diff --check` exited `0`; only Git's existing LF-to-CRLF working-copy warnings were printed

### Remaining concern

- The stricter classification, canonicalization, and redaction paths were verified through focused mocks and full local regression only. No live RMS response was exercised in this pass.
