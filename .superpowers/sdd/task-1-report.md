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
