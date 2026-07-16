# Task 1 Report: Sales Persistence Models and Schema Compatibility

Date: 2026-07-16
Worktree: `D:\ERP\worktrees\lt-backend-ai-product-sales`
Branch: current feature branch in isolated backend worktree

## Scope

Owned files only:

- `app/db/models.py`
- `app/db/database.py`
- `tests/test_sales_models.py`

## Requirements Covered

Implemented the Task 1 persistence layer for AI product sales analysis:

- Added `SalesOrderModel`
- Added `SalesOrderItemModel`
- Added `SalesItemAdjustmentModel`
- Added `ProductSalesDailyModel`
- Added `SalesSyncStateModel`
- Added `SalesAnalysisConversationModel`
- Added `SalesAnalysisMessageModel`
- Extended `ensure_schema_compatibility()` to create missing sales tables, columns, unique constraints, indexes, and MySQL `LONGTEXT` variants idempotently

## TDD Evidence

### Red

Created `tests/test_sales_models.py` first and ran:

```powershell
pytest tests/test_sales_models.py -v
```

Initial result:

- collection failed because the sales models did not exist in `app/db/models.py`

### Green

Implemented the models and compatibility logic, then re-ran:

```powershell
pytest tests/test_sales_models.py -v
```

Result:

- `5 passed in 0.54s`

## Test Coverage Added

`tests/test_sales_models.py` now verifies:

1. Sales tables are created in isolated SQLite schema builds
2. `lt_sales_orders` enforces `store_id + order_number` uniqueness
3. `lt_sales_order_items` enforces `store_id + order_number + item_detail_id` uniqueness
4. `SalesOrderItemModel.from_service_payload(...)` clamps `effective_units` to zero and persists a non-negative `effective_amount`
5. `lt_product_sales_daily` enforces `store_id + sales_date + manage_number + sku_key` uniqueness

## Implementation Notes

### `app/db/models.py`

- Reused existing `TimestampMixin`
- Used `LONGTEXT` MySQL variants for JSON text payload fields
- Added explicit unique constraints and named indexes for the new sales tables
- Added `SalesOrderItemModel.calculate_effective_units(...)`
- Added `SalesOrderItemModel.from_service_payload(...)` as the service-facing constructor that computes and clamps `effective_units`

### `app/db/database.py`

- Kept existing MySQL-only compatibility gate intact
- Added inspector-driven helpers to:
  - detect current tables
  - detect current columns
  - detect current unique constraints
  - detect current indexes
- Added idempotent layout enforcement for the new sales tables:
  - create missing tables
  - add missing columns
  - normalize `LONGTEXT` columns where needed
  - add missing unique constraints
  - add missing indexes

## Verification

Executed:

```powershell
pytest tests/test_sales_models.py -v
python -m compileall app/db/models.py app/db/database.py tests/test_sales_models.py
```

Observed:

- focused Task 1 test suite passed
- compile check exited successfully

## Self-Review

What I checked:

- edits are limited to the three owned files
- model names and table names match the approved design/plan
- uniqueness rules required by the design are enforced in schema
- the service-facing constructor prevents persisting negative `effective_units`
- compatibility logic is idempotent in structure and scoped to MySQL only

## Concerns

- `ensure_schema_compatibility()` for the new sales tables was verified structurally and by code review, but not exercised against a live MySQL schema in this task because the requested verification scope was the focused local test file only.

## Review Fix Pass

Date: 2026-07-16

### Important findings addressed

1. `ensure_table_layout` was made additive-only for existing partial populated tables.
   - It now creates missing sales tables from scratch when the table is absent.
   - For already-existing tables, it only auto-adds columns that are safe to append:
     - nullable columns
     - non-null columns with a safe server default
   - It skips unsafe additions such as:
     - primary key columns
     - foreign key columns
     - non-null columns without a safe server default
   - Unique constraints are only added when the dialect supports `ALTER TABLE` and all referenced columns already exist.
   - Added realistic SQLite partial-schema tests that start from hand-written legacy tables with existing rows.

2. `SalesOrderItemModel.from_service_payload(...)` now enforces deduction invariants.
   - Confirmed deductions (`canceled_units + refunded_units + returned_units`) must not exceed `ordered_units`.
   - `unresolved_refunded_units` is accepted as input for service-level reconciliation but does not reduce `effective_units`.
   - Added focused tests for both behaviors.

3. Cross-owner and cross-store parent-child references are now blocked at the schema level.
   - `SalesOrderItemModel` now references `SalesOrderModel` through a composite foreign key on `(sales_order_id, owner_username, store_id)`.
   - `SalesItemAdjustmentModel` now references `SalesOrderItemModel` through a composite foreign key on `(sales_order_item_id, owner_username, store_id)`.
   - `SalesAnalysisMessageModel` now references `SalesAnalysisConversationModel` through a composite foreign key on `(conversation_id, owner_username)`.
   - Added targeted tests proving cross-owner / cross-store inserts fail with `IntegrityError`.

### Commands run

```powershell
pytest tests/test_sales_models.py -v
python -m compileall app/db/models.py app/db/database.py tests/test_sales_models.py
```

### Command outputs

`pytest tests/test_sales_models.py -v`

```text
============================= test session starts =============================
platform win32 -- Python 3.12.3, pytest-9.1.1, pluggy-1.6.0 -- D:\apps\pycharm\python.exe
cachedir: .pytest_cache
rootdir: D:\ERP\worktrees\lt-backend-ai-product-sales
plugins: anyio-4.13.0
collecting ... collected 12 items

tests/test_sales_models.py::test_sales_tables_are_created PASSED         [  8%]
tests/test_sales_models.py::test_sales_order_enforces_store_and_order_number_uniqueness PASSED [ 16%]
tests/test_sales_models.py::test_sales_order_item_enforces_store_order_item_detail_uniqueness PASSED [ 25%]
tests/test_sales_models.py::test_sales_order_item_service_constructor_clamps_effective_units_to_zero PASSED [ 33%]
tests/test_sales_models.py::test_product_sales_daily_enforces_daily_product_sku_uniqueness PASSED [ 41%]
tests/test_sales_models.py::test_sales_order_item_service_constructor_rejects_deductions_above_ordered_units PASSED [ 50%]
tests/test_sales_models.py::test_sales_order_item_service_constructor_does_not_reduce_effective_units_for_unresolved_refunds PASSED [ 58%]
tests/test_sales_models.py::test_sales_order_item_rejects_cross_owner_or_store_order_reference PASSED [ 66%]
tests/test_sales_models.py::test_sales_adjustment_rejects_cross_owner_or_store_item_reference PASSED [ 75%]
tests/test_sales_models.py::test_sales_analysis_message_rejects_cross_owner_conversation_reference PASSED [ 83%]
tests/test_sales_models.py::test_ensure_table_layout_skips_unsafe_non_null_foreign_key_columns_for_partial_populated_table PASSED [ 91%]
tests/test_sales_models.py::test_ensure_table_layout_adds_safe_defaulted_columns_to_partial_populated_daily_sales_table PASSED [100%]

============================= 12 passed in 0.66s ==============================
```

`python -m compileall app/db/models.py app/db/database.py tests/test_sales_models.py`

```text
Compiling 'tests/test_sales_models.py'...
```
