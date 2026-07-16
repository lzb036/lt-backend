# AI Product Sales Analysis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an AI product-analysis workspace that synchronizes each user's own Rakuten orders, computes traceable effective product sales, and answers natural-language questions through fixed read-only analysis tools.

**Architecture:** Add focused sales models and services beside the existing crawler service instead of extending the already large `crawler_service.py`. A Rakuten order client synchronizes order snapshots and item facts; an analysis service reads daily aggregates; an AI orchestration service may call only registered analysis tools. The Vue page consumes typed endpoints and streaming chat events.

**Tech Stack:** FastAPI, SQLAlchemy, PyMySQL, Pydantic, pytest, LiteLLM, Vue 3, TypeScript, Element Plus, Axios.

## Global Constraints

- Every query and sync must enforce `owner_username + store_id`; superadmin does not automatically gain access to another user's stores.
- Effective sales are `ordered units - canceled units - refunded units - returned units`.
- A return-refund must be deducted once, not once as refund and again as return.
- Unassignable partial refunds remain unresolved and do not change a product's effective units.
- AI may call fixed read-only tools only; it may not generate or execute arbitrary SQL.
- Store credentials and customer PII must never be sent to the AI model or frontend.
- “近期” means the latest 30 complete calendar days; “销量” means effective units.
- Initial synchronization covers the latest 90 days; automatic incremental synchronization runs every 30 minutes.
- Every incremental run rechecks the latest 7 days and continues rechecking incomplete or adjusted orders from the latest 30 days.
- Do not start another frontend or backend process and do not open the UI for verification.

---

### Task 1: Sales Persistence Models and Schema Compatibility

**Files:**
- Modify: `app/db/models.py`
- Modify: `app/db/database.py`
- Create: `tests/test_sales_models.py`

**Interfaces:**
- Produces: `SalesOrderModel`, `SalesOrderItemModel`, `SalesItemAdjustmentModel`, `ProductSalesDailyModel`, `SalesSyncStateModel`, `SalesAnalysisConversationModel`, and `SalesAnalysisMessageModel`.

- [ ] **Step 1: Write failing model tests**

Create tests that build an isolated SQLite schema and assert the tables exist, `store_id + order_number` is unique, and `effective_units` cannot be persisted below zero through the service-facing constructor.

```python
def test_sales_tables_are_created(sqlite_engine):
    Base.metadata.create_all(sqlite_engine)
    names = set(inspect(sqlite_engine).get_table_names())
    assert {"lt_sales_orders", "lt_sales_order_items", "lt_product_sales_daily"} <= names
```

- [ ] **Step 2: Run the focused tests**

Run: `pytest tests/test_sales_models.py -v`

Expected: FAIL because the sales models do not exist.

- [ ] **Step 3: Add the seven models**

Use existing `TimestampMixin`, MySQL `LONGTEXT` variants, indexed `owner_username`, indexed `store_id`, and the exact uniqueness rules in the approved design. Store raw remote payloads as JSON text and never expose them directly through API serializers.

- [ ] **Step 4: Extend schema compatibility**

Add idempotent table/column/index creation to `ensure_schema_compatibility()` following the existing inspector-based pattern. Do not call `init_database()` in tests against the developer's MySQL database.

- [ ] **Step 5: Verify and commit**

Run: `pytest tests/test_sales_models.py -v`

Expected: PASS.

Commit:

```powershell
git add app/db/models.py app/db/database.py tests/test_sales_models.py
git commit -m "feat: add product sales persistence models"
```

### Task 2: Rakuten Read-Only Order Client

**Files:**
- Create: `app/services/rakuten_order_service.py`
- Create: `tests/test_rakuten_order_service.py`

**Interfaces:**
- Produces:
  - `search_order_numbers(service_secret: str, license_key: str, start_at: datetime, end_at: datetime, statuses: list[int]) -> list[str]`
  - `get_orders(service_secret: str, license_key: str, order_numbers: list[str]) -> list[dict[str, Any]]`
  - `iter_order_items(order: dict[str, Any]) -> Iterable[dict[str, Any]]`

- [ ] **Step 1: Write HTTP-contract tests**

Mock `requests.Session.post` and assert `Authorization` uses `ESA base64(secret:key)`, `searchOrder` paginates, `getOrder` uses `version: 7`, and batches do not exceed the existing RMS-safe batch size.

- [ ] **Step 2: Run tests and confirm failure**

Run: `pytest tests/test_rakuten_order_service.py -v`

Expected: FAIL because the module is missing.

- [ ] **Step 3: Implement the client**

Use the existing request timeout and proxy conventions. Parse `PackageModelList[*].ItemModelList[*]` and emit normalized item dictionaries containing `itemDetailId`, `manageNumber`, `itemNumber`, `SkuModelList`, `units`, prices, deletion flags, and package position.

- [ ] **Step 4: Add response validation**

Raise concise `RuntimeError` messages for credential rejection, rate limiting, malformed JSON, missing pagination, and network failure. Never include credentials or complete response bodies in errors.

- [ ] **Step 5: Verify and commit**

Run: `pytest tests/test_rakuten_order_service.py -v`

Commit:

```powershell
git add app/services/rakuten_order_service.py tests/test_rakuten_order_service.py
git commit -m "feat: add Rakuten order read client"
```

### Task 3: Idempotent Sales Synchronization and Effective-Unit Calculation

**Files:**
- Create: `app/services/sales_sync_service.py`
- Create: `tests/test_sales_sync_service.py`

**Interfaces:**
- Consumes: Task 1 models and Task 2 client.
- Produces:
  - `sync_owned_store(owner_username: str, store_id: int, *, initial_days: int = 90) -> dict[str, Any]`
  - `calculate_effective_units(ordered: int, canceled: int, refunded: int, returned: int) -> int`
  - `rebuild_daily_sales(session: Session, store_id: int, start_date: date, end_date: date) -> None`

- [ ] **Step 1: Write calculation and idempotency tests**

Cover normal sale, whole-order cancellation, deleted item, quantity reduction, item-level refund, item-level return, return-refund deduplication, unresolved partial refund, reverted adjustment, and syncing the same snapshot twice.

```python
def test_return_refund_is_deducted_once():
    result = derive_adjustments(ordered_units=5, refund_units=2, return_units=2, return_refund=True)
    assert result.refunded_units == 0
    assert result.returned_units == 2
    assert result.effective_units == 3
```

- [ ] **Step 2: Run tests and confirm failure**

Run: `pytest tests/test_sales_sync_service.py -v`

- [ ] **Step 3: Implement ownership and locking**

Resolve stores with a query equivalent to:

```python
select(StoreModel).where(
    StoreModel.id == store_id,
    StoreModel.owner_username == owner_username,
)
```

Use a store-level sync-state lock so concurrent manual and scheduled sync attempts return the active task rather than duplicating work.

- [ ] **Step 4: Implement snapshot reconciliation**

Preserve the first observed `ordered_units`, update `latest_units`, derive confirmed adjustments from remote status and item flags, and create `unresolved` adjustments when a partial refund has no item attribution. Recompute affected daily rows transactionally.

- [ ] **Step 5: Verify and commit**

Run: `pytest tests/test_sales_sync_service.py -v`

Commit:

```powershell
git add app/services/sales_sync_service.py tests/test_sales_sync_service.py
git commit -m "feat: synchronize effective product sales"
```

### Task 4: Read-Only Sales Analysis Tools

**Files:**
- Create: `app/services/sales_analysis_service.py`
- Create: `tests/test_sales_analysis_service.py`

**Interfaces:**
- Consumes: `ProductSalesDailyModel`, unresolved adjustments, owned stores.
- Produces: `SALES_ANALYSIS_TOOLS`, `execute_sales_tool(owner_username: str, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]`.

- [ ] **Step 1: Write tool tests**

Create fixtures for two users and assert every tool rejects the other user's `storeId`. Cover overview, ranking, trend, product comparison, SKU breakdown, slow movers, and adjustment summary.

- [ ] **Step 2: Run tests and confirm failure**

Run: `pytest tests/test_sales_analysis_service.py -v`

- [ ] **Step 3: Implement strict argument models**

Define Pydantic models with maximum 366-day ranges, ranking limits of 1-100, supported grains `day|week|month`, and explicit product-list limits. Resolve relative language before tool execution; tools accept concrete ISO dates only.

- [ ] **Step 4: Implement aggregate queries**

All queries must include `owner_username` and `store_id`. Return compact chart-ready rows and metadata:

```json
{
  "store": {"id": 1, "name": "店铺"},
  "range": {"start": "2026-06-16", "end": "2026-07-15"},
  "metric": "effectiveUnits",
  "dataUpdatedAt": "2026-07-16T14:30:00+08:00",
  "unresolvedAdjustmentCount": 0,
  "rows": []
}
```

- [ ] **Step 5: Verify and commit**

Run: `pytest tests/test_sales_analysis_service.py -v`

Commit:

```powershell
git add app/services/sales_analysis_service.py tests/test_sales_analysis_service.py
git commit -m "feat: add controlled sales analysis tools"
```

### Task 5: AI Analysis Orchestration and Conversation Storage

**Files:**
- Create: `app/services/sales_ai_service.py`
- Create: `tests/test_sales_ai_service.py`

**Interfaces:**
- Consumes: existing user AI settings and `execute_sales_tool`.
- Produces:
  - `list_conversations(owner_username: str) -> list[dict[str, Any]]`
  - `create_conversation(owner_username: str, title: str = "新分析") -> dict[str, Any]`
  - `stream_analysis(owner_username: str, conversation_id: int, message: str) -> Iterator[dict[str, Any]]`

- [ ] **Step 1: Write orchestration tests**

Mock LiteLLM and assert the model sees only tool schemas and compact sales results, never credentials, raw orders, buyer fields, or SQL. Verify one-store automatic selection, multi-store clarification, “近期” date expansion, and tool-call rejection for unknown tools.

- [ ] **Step 2: Run tests and confirm failure**

Run: `pytest tests/test_sales_ai_service.py -v`

- [ ] **Step 3: Implement tool-call loop**

Reuse `ensure_user_settings()` and `resolved_model_name()`. Limit a turn to four tool calls. Emit `status`, `tool_call`, `tool_result`, `delta`, `completed`, or `error`; persist user message, sanitized tool metadata, and final assistant text.

- [ ] **Step 4: Implement deterministic fallback**

If AI explanation fails after tools succeed, emit a basic answer containing store, exact dates, effective-sales definition, update time, unresolved count, and returned table rows.

- [ ] **Step 5: Verify and commit**

Run: `pytest tests/test_sales_ai_service.py -v`

Commit:

```powershell
git add app/services/sales_ai_service.py tests/test_sales_ai_service.py
git commit -m "feat: orchestrate AI sales analysis"
```

### Task 6: Sales Analysis API and Scheduled Entry Point

**Files:**
- Modify: `app/api/crawler.py`
- Modify: `app/services/crawler_service.py`
- Create: `tests/test_sales_analysis_api.py`

**Interfaces:**
- Consumes: Tasks 3-5.
- Produces the approved `/crawler/sales-analysis/*` endpoints.

- [ ] **Step 1: Write API permission and SSE tests**

Use FastAPI `TestClient`; assert `ai.manage` is required, cross-owner store IDs return 404/403 without disclosing store data, sync returns a task status, and message streaming uses `serialize_sse_event`.

- [ ] **Step 2: Run tests and confirm failure**

Run: `pytest tests/test_sales_analysis_api.py -v`

- [ ] **Step 3: Add request models and routes**

Add bounded Pydantic payloads for store sync, conversation creation, and messages. Keep route handlers thin and delegate ownership validation to services.

- [ ] **Step 4: Add scheduled sync dispatch**

Extend the existing schedule runner with a guarded sales-sync tick that queues stores whose last successful synchronization is at least 30 minutes old, without starting another scheduler process. Each run rechecks the latest 7 days plus incomplete or adjusted orders from the latest 30 days. Failure for one store must not stop other stores.

- [ ] **Step 5: Verify and commit**

Run: `pytest tests/test_sales_analysis_api.py -v`

Commit:

```powershell
git add app/api/crawler.py app/services/crawler_service.py tests/test_sales_analysis_api.py
git commit -m "feat: expose sales analysis APIs"
```

### Task 7: Frontend Types, API Client, Navigation, and Route

**Files:**
- Modify: `D:/ERP/lt-frontend/src/types/crawler.ts`
- Modify: `D:/ERP/lt-frontend/src/composables/useCollectorApi.ts`
- Modify: `D:/ERP/lt-frontend/src/components/crawler/AppShell.vue`
- Modify: `D:/ERP/lt-frontend/src/router.ts`
- Modify: `D:/ERP/lt-frontend/src/utils/permissions.ts`
- Create: `D:/ERP/lt-frontend/src/components/crawler/ProductSalesAnalysisView.vue`

**Interfaces:**
- Produces typed API methods for owned stores, sync state, conversations, messages, and SSE analysis events.

- [ ] **Step 1: Add route and API type tests where pure functions permit**

Add a permission-path assertion to the existing permissions tests and pure event-normalization tests if SSE parsing is extracted.

- [ ] **Step 2: Add route and navigation**

Register `/ai/product-analysis` with `ai.manage`. Add “商品分析” under “AI 管理” using an Element Plus chart/data icon.

- [ ] **Step 3: Build the working analysis page**

Implement an operational layout: conversation list, owned-store selector, update timestamp, sync button/status, quick questions, message stream, metric summaries, chart-ready trend table, ranked results, and unresolved-adjustment warning. Do not add explanatory marketing content.

- [ ] **Step 4: Implement stream lifecycle**

Disable duplicate sends, show tool progress, preserve partial assistant text, support cancellation on route leave, and keep tool results available when explanation fails.

- [ ] **Step 5: Verify and commit**

Run: `npm run build`

Expected: `vue-tsc --noEmit` and Vite production build pass.

Commit from `D:/ERP/lt-frontend`:

```powershell
git add src/types/crawler.ts src/composables/useCollectorApi.ts src/components/crawler/AppShell.vue src/components/crawler/ProductSalesAnalysisView.vue src/router.ts src/utils/permissions.ts
git commit -m "feat: add AI product sales workspace"
```

### Task 8: End-to-End Regression and Documentation

**Files:**
- Modify: `README.md` or the existing operator manual source if sales-analysis operations are documented there.
- Modify tests only if verification exposes a real regression.

**Interfaces:**
- Verifies the complete backend/frontend contract.

- [ ] **Step 1: Run focused backend suite**

Run:

```powershell
pytest tests/test_sales_models.py tests/test_rakuten_order_service.py tests/test_sales_sync_service.py tests/test_sales_analysis_service.py tests/test_sales_ai_service.py tests/test_sales_analysis_api.py -v
```

Expected: PASS.

- [ ] **Step 2: Run full backend regression**

Run: `pytest -q`

Expected: PASS without modifying live MySQL data.

- [ ] **Step 3: Run frontend verification**

Run from `D:/ERP/lt-frontend`: `npm run build`

Expected: PASS. Do not run `npm run dev`, `vite preview`, backend startup, or browser inspection.

- [ ] **Step 4: Check repository hygiene**

Run `git diff --check` in both repositories and verify no credentials, customer data, generated build output, or unrelated dirty files are staged.

- [ ] **Step 5: Commit final documentation**

```powershell
git add README.md
git commit -m "docs: document product sales analysis"
```

Skip this commit when no user-facing operational documentation needed a change.
