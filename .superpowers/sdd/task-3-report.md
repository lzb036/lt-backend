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
- Uses the owned store row and `SalesSyncStateModel` row with `FOR UPDATE` as the store-level synchronization lock.
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
- SQLite does not enforce `SELECT ... FOR UPDATE`, so lock query construction and running-state behavior are covered locally, but real concurrent MySQL contention was not integration-tested in this task.
- Remote refund/return field aliases are handled defensively from the approved snapshot rules, but exact production payload variants should be checked when a sanitized real version-7 order sample is available.
