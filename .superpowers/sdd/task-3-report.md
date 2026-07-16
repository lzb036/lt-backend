# Task 3 Report: Idempotent Sales Synchronization

Date: 2026-07-16
Worktree: `D:\ERP\worktrees\lt-backend-ai-product-sales`
Branch: `feature/ai-product-sales-analysis`

## Scope

Implemented only:

- `app/services/sales_sync_service.py`
- `tests/test_sales_sync_service.py`
- `.superpowers/sdd/task-3-report.md`

## Requirements Covered

- Enforces `owner_username + store_id` before reading credentials or calling the order client.
- Uses a tokenized conditional `UPDATE` as the store-level lease; no row lock is held during remote API calls or reconciliation. A short `FOR UPDATE` current read is used only after failed acquisition to read the active state in a fresh transaction.
- Returns the existing running state instead of starting a duplicate synchronization.
- Preserves the first observed `ordered_units` while updating `latest_units`.
- Reconciles orders and item rows idempotently by store/order/item identity.
- Derives confirmed cancellation, refund, and return deductions.
- Prevents explicit refund/return attribution from being double-counted as a quantity-reduction cancellation.
- Records unattributed partial refunds as `unresolved` without reducing effective units.
- Marks removed snapshot adjustments as `reverted`.
- Deduplicates return-refund overlap by retaining the return deduction and removing the overlapping refund deduction.
- Treats items missing from a later complete snapshot as canceled while preserving their original ordered quantity.
- Rebuilds affected daily sales rows in the same database transaction as snapshot reconciliation.
- Leaves `rebuild_daily_sales(...)` commit control with the caller.
- Marks failed synchronization state as `error` after rolling back the failed reconciliation transaction.

## TDD Evidence

Initial red run:

```powershell
pytest tests/test_sales_sync_service.py -v
```

Result:

```text
ImportError: cannot import name 'sales_sync_service' from 'app.services'
```

During self-review, a second red test exposed double deduction when a quantity reduction and an attributed refund described the same two units:

```powershell
pytest tests/test_sales_sync_service.py::test_attributed_refund_explains_quantity_reduction_without_double_deduction -v
```

Result before the fix:

```text
1 failed
expected canceled_units == 0, got 2
```

After the minimal reconciliation fix:

```powershell
pytest tests/test_sales_sync_service.py -v
```

Result:

```text
17 passed in 1.02s
```

## Test Coverage

The focused tests cover:

1. Normal sale
2. Whole-order cancellation
3. Deleted item
4. Quantity reduction
5. Item-level refund
6. Item-level return
7. Return-refund deduplication
8. Unresolved partial refund
9. Attributed refund versus quantity-reduction deduplication
10. Reverted adjustment
11. Owner/store isolation
12. Running synchronization lock response
13. Repeated adjusted snapshot idempotency
14. Missing item reconciliation
15. Daily aggregation replacement
16. Daily rebuild rollback without an internal commit

## Verification

Focused:

```powershell
pytest tests/test_sales_sync_service.py -v
```

```text
17 passed in 1.02s
```

Full:

```powershell
pytest -q
```

```text
265 passed, 2 warnings, 4 subtests passed in 10.66s
```

The two warnings are the existing FastAPI `on_event` deprecation warnings from `app/main.py`.

Compilation:

```powershell
python -m compileall app tests/test_sales_sync_service.py
```

Result: exit code `0`.

Diff validation:

```powershell
git diff --check -- app/services/sales_sync_service.py tests/test_sales_sync_service.py
```

Result: exit code `0`.

## Self-Review

- Confirmed every store lookup in the public synchronization path includes both owner and store identifiers.
- Confirmed credentials are decrypted only after ownership validation.
- Confirmed API calls occur after the running state is committed, while order reconciliation and daily rebuild share one later transaction.
- Confirmed adjustment rows use stable snapshot sources and are updated or reverted instead of duplicated.
- Confirmed unresolved adjustments do not flow into item or daily effective-unit deductions.
- Confirmed `rebuild_daily_sales(...)` performs delete and replacement writes without calling `commit()`.
- Confirmed no frontend/backend process or live API was started.

## Concerns

- The HTTP contract remains mock-tested only; no live Rakuten RMS request was made.
- SQLite does not enforce `SELECT ... FOR UPDATE`; MySQL acquisition/current-read SQL is dialect-compiled and token/rowcount behavior is tested locally, but real concurrent MySQL contention was not integration-tested in this task.
- Remote refund/return field aliases are handled defensively from the approved snapshot rules, but exact production payload variants should be checked when a sanitized real version-7 order sample is available.

## Review Fix Takeover

The interrupted review patch was preserved and finalized. The review fixes now:

- Acquire the store lease with one conditional `UPDATE`, return an active running state without API calls, reclaim only stale running leases, maintain an independent periodic heartbeat with the current token, and protect completion/error writes with token-matched row counts.
- Reject older or unversioned snapshots before they can overwrite a versioned order, including an older duplicate later in the same API batch.
- Skip malformed package/item containers and incomplete item records before mutation.
- Treat empty item lists as authoritative only for explicit cancellation, full refund, or full return; completed/shipped empty snapshots do not cancel existing lines.
- Preserve first-observed `ordered_units` while applying newer `latest_units` such as `5 -> 3`.
- Keep attributed refund/return deductions confirmed while recording any order-level residual units or amounts as unresolved.
- Rebuild daily rows across multiple orders and SKUs with distinct order counts and aggregated unit totals.
- Compile the acquisition statement with the MySQL dialect and verify exact `rowcount == 1` acquisition semantics with mocks.

### Additional TDD Evidence

The takeover added regressions before changing the incomplete-snapshot rules:

```powershell
pytest tests/test_sales_sync_service.py -q
```

```text
4 failed, 37 passed in 1.93s
```

The failures showed completed-empty snapshots and incomplete item records still bypassed the prior validation.

A second parser-alignment regression exposed `"5.0"` being accepted by validation but normalized to zero:

```powershell
pytest tests/test_sales_sync_service.py::test_incomplete_item_record_is_skipped_without_canceling_lines -q
```

```text
1 failed, 3 passed in 1.28s
```

After the narrow validation fixes:

```powershell
pytest tests/test_sales_sync_service.py -v
```

```text
44 passed in 1.73s
```

### Final Verification

Full suite:

```powershell
pytest -q
```

```text
292 passed, 2 warnings, 4 subtests passed in 5.41s
```

The warnings remain the existing FastAPI `on_event` deprecations from `app/main.py`.

Compilation:

```powershell
python -m compileall app tests/test_sales_sync_service.py
```

Result: exit code `0`.

Diff validation:

```powershell
git diff --check
```

Result: exit code `0`.

### Final Self-Review

- Confirmed lease acquisition, heartbeat, completion, and failure writes all use the lease token or atomic acquisition predicate.
- Confirmed an active lease returns before credential decryption or either Rakuten order API call.
- Confirmed stale and incomplete snapshots do not alter orders, items, adjustments, or daily aggregates.
- Confirmed accepted item quantities use the same integer semantics as downstream normalization.
- Confirmed unresolved refund/return remainders do not reduce effective units.
- Confirmed the focused tests cover `5 -> 3`, same-batch out-of-order snapshots, multi-order/SKU aggregation, MySQL SQL compilation, and rowcount behavior.
- Confirmed no frontend/backend process, UI, browser, or live Rakuten request was started.

### Remaining Concern

- Real concurrent MySQL contention is represented by dialect compilation and rowcount/token tests, not a live multi-connection MySQL integration test.

## Remaining Review Findings

The final Task 3 review findings were implemented without changing files outside the owned Task 3 service, tests, and report.

### Lease Lifecycle And Contention

- Added an immediate and periodic lease heartbeat that runs throughout credential handling, remote search, local candidate selection, `getOrder`, reconciliation, and daily rebuild.
- Each heartbeat pulse uses a separate short-lived database session.
- Heartbeat token loss is captured by the worker thread and raised in the main synchronization flow before later API/reconciliation work can continue.
- Heartbeat shutdown waits for the active short database pulse to finish before the terminal lease compare-and-set.
- Sync-state creation commits before acquisition. Failed acquisition ends its transaction and reads the active task in a fresh `FOR UPDATE` current-read transaction, avoiding a stale MySQL `REPEATABLE READ` snapshot.

### Reconciliation And Parsing

- Residual refund/return units and amounts subtract only final confirmed item attribution after quantity clamping and return-refund deduplication.
- Missing item adjustment amounts are inferred as `unit_price * confirmed_units`.
- `partialRefund`, `hasPartialRefund`, `unresolvedRefund`, and matching return flags create zero-value unresolved marker rows even when no numeric fields are supplied.
- Text, integer, decimal, and boolean alias readers skip null or malformed values and continue to later aliases.
- Aware timestamps are normalized to UTC-naive before comparison/storage.
- Strictly older snapshots cannot change accepted state. Equal remote versions update only local sync metadata, so the first accepted business snapshot wins.

### Daily Keys And Recheck Policy

- Daily aggregation uses `manageNumber`, then prefixed `item-number:`, then prefixed `item-id:` as the deterministic product key, with an `item-detail:` last-resort safeguard.
- Products with blank `manageNumber` no longer merge solely because they share a SKU.
- Every sync performs a seven-day remote search.
- Local candidates add incomplete/adjusted orders from the last 30 days.
- Completed orders up to 90 days old are added only when their stored sync/update timestamp is at least one day old.
- Remote and local order numbers are normalized and order-preserving deduplicated before `getOrder`.
- The legacy `initial_days` argument remains interface-compatible but no longer changes the approved 7/30/90-day policy.

### Additional Red-Green Evidence

Periodic heartbeat and fresh-transaction acquisition tests initially produced:

```text
5 failed, 44 deselected in 1.13s
```

Residual/parser/product-key/timestamp tests initially produced:

```text
13 failed, 49 deselected in 3.95s
```

The recheck-policy regression initially failed because initial sync still searched the legacy caller window:

```text
1 failed in 2.34s
```

Confirmed-attribution and fixed 90-day policy edge tests initially produced:

```text
2 failed, 64 deselected in 2.47s
```

### Final Verification

Focused:

```powershell
pytest tests/test_sales_sync_service.py -q
```

```text
66 passed in 5.03s
```

Full:

```powershell
pytest -q
```

```text
314 passed, 2 warnings, 4 subtests passed in 13.67s
```

The warnings remain the existing FastAPI `on_event` deprecations from `app/main.py`.

Compilation:

```powershell
python -m compileall app tests/test_sales_sync_service.py
```

Result: exit code `0`.

Diff validation:

```powershell
git diff --check
```

Result: exit code `0`.

### Final Self-Review

- Confirmed no heartbeat survives into the terminal `idle`/`error` update.
- Confirmed active contention returns before credential decryption or Rakuten API calls.
- Confirmed every requested order number is deduplicated before `getOrder`.
- Confirmed unresolved markers never reduce effective units.
- Confirmed equal-version conflicts cannot revert accepted adjustments.
- Confirmed no frontend/backend process, UI, browser, or live Rakuten request was started.

### Remaining Concerns

- Real multi-connection MySQL contention is not available in the local test environment; MySQL behavior is covered through dialect compilation, fresh-transaction boundaries, current-read SQL, rowcount mocks, and token tests.
- Rakuten adjustment aliases remain based on the approved snapshot contract rather than a live sanitized production version-7 payload.
