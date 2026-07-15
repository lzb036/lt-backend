# Pending Product Genre Editing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show and edit the complete Rakuten genre path on pending products, while preventing approval when a product has no valid genre.

**Architecture:** The backend derives genre paths from the existing Rakuten attribute rules file, exposes bounded search and pending-only update APIs, and enforces approval validation transactionally. A focused Vue selector performs remote search and immediate saving, while the workflow view updates list state and provides early single/batch approval feedback.

**Tech Stack:** Python, FastAPI, SQLAlchemy, pytest, Vue 3 Composition API, TypeScript, Element Plus, Vite.

## Global Constraints

- Use `app/resources/rakuten_attribute_rules.json` as the only genre ID-to-path source.
- Persist only `ProductModel.genre_id`; do not add a duplicate genre-name database column.
- Only pending products may be edited through the new genre update API.
- Invalid genre data must block the entire approval request before any status changes.
- Detail-dialog edits remain explicit-save only.
- Do not start or restart the frontend or backend application and do not open the UI for verification.

---

### Task 1: Backend Genre Domain Functions

**Files:**
- Create: `tests/test_product_genres.py`
- Modify: `app/services/crawler_service.py`

**Interfaces:**
- Produces: `rakuten_genre_path(genre_id: Any) -> str`
- Produces: `search_rakuten_genres(keyword: str = "", limit: int = 30) -> list[dict[str, str]]`
- Produces: `update_pending_product_genre(owner_username: str, product_id: int, genre_id: str) -> dict[str, Any]`
- Extends: `product_to_public(row)` with `genrePath`

- [ ] **Step 1: Write failing tests for genre lookup and search**

Test that a known fixture genre returns its full `genrePath`, search matches both ID and path text, and the result respects the requested limit.

- [ ] **Step 2: Run the focused tests and verify expected failures**

Run: `python -m pytest tests/test_product_genres.py -q`

Expected: failures because the genre helpers do not exist.

- [ ] **Step 3: Implement cached genre lookup and bounded search**

Read `load_rakuten_attribute_rules()["genres"]`, normalize IDs and paths, match numeric queries against IDs and all queries case-insensitively against paths, and cap limits to a small server-defined maximum.

- [ ] **Step 4: Write failing tests for pending-product genre updates**

Cover valid updates, raw payload synchronization, unknown IDs, malformed IDs, non-pending products, and another owner's product.

- [ ] **Step 5: Run focused tests and verify expected failures**

Run: `python -m pytest tests/test_product_genres.py -q`

Expected: update tests fail because the update function is missing.

- [ ] **Step 6: Implement pending-only genre persistence**

Validate the 6-digit ID against local genre data before mutation, update `row.genre_id`, set `raw_payload["genreId"]`, serialize the raw payload, flush, and return `product_to_public(row)`.

- [ ] **Step 7: Run the focused tests**

Run: `python -m pytest tests/test_product_genres.py -q`

Expected: all genre domain tests pass.

### Task 2: Backend API and Approval Guard

**Files:**
- Modify: `tests/test_product_genres.py`
- Modify: `app/api/crawler.py`
- Modify: `app/services/crawler_service.py`

**Interfaces:**
- Adds: `GET /crawler/products/genres?keyword=&limit=`
- Adds: `PUT /crawler/products/{product_id}/genre` with `{ "genreId": "xxxxxx" }`
- Strengthens: `update_product_status(..., status="approved")`

- [ ] **Step 1: Write failing API and approval tests**

Test permission-protected search, pending-product update, rejection of products with blank/unknown genre IDs, atomic batch rejection, and successful approval with a valid local genre.

- [ ] **Step 2: Run focused tests and verify expected failures**

Run: `python -m pytest tests/test_product_genres.py -q`

Expected: route tests fail with missing endpoints and approval tests fail because invalid genres are currently accepted.

- [ ] **Step 3: Add request models and API routes**

Add a genre update payload with a six-digit pattern, return `{ "genres": [...] }` from search, and return `{ "product": ... }` from update. Translate service `RuntimeError` values into HTTP 400 responses using the existing API pattern.

- [ ] **Step 4: Add transactional approval validation**

Before mutating any row for `approved`, collect rows whose `genre_id` does not resolve to a local full path. Raise one error containing the invalid count and up to three product names.

- [ ] **Step 5: Run focused and related backend tests**

Run: `python -m pytest tests/test_product_genres.py tests/test_ai_title_service.py -q`

Expected: all selected tests pass.

### Task 3: Frontend Genre Types, API, and Validation Utilities

**Files:**
- Create: `src/utils/productGenre.ts`
- Create: `src/utils/productGenre.test.ts` only if the repository's test runner supports it
- Modify: `src/types/crawler.ts`
- Modify: `src/composables/useCollectorApi.ts`

**Interfaces:**
- Adds: `RakutenGenreOption { genreId: string; genrePath: string }`
- Extends: `ProductItem.genrePath: string`
- Adds: `api.searchRakutenGenres(keyword, limit?)`
- Adds: `api.updatePendingProductGenre(productId, genreId)`
- Produces: `hasValidProductGenre(product: Pick<ProductItem, "genreId" | "genrePath">) -> boolean`
- Produces: `invalidGenreProducts(products: ProductItem[], productIds: number[]) -> ProductItem[]`

- [ ] **Step 1: Add failing utility tests when a configured frontend test runner exists**

Cover valid six-digit mapped genres, blank paths, malformed IDs, and selected-ID filtering.

- [ ] **Step 2: Run the focused frontend test or type check to verify failure**

Run the repository-provided test command when present; otherwise run `npm run build` and confirm missing symbols/types fail compilation.

- [ ] **Step 3: Implement types, API methods, and pure validation utilities**

Keep API response contracts typed and validation free of Vue state so the workflow can reuse it for single and batch approval.

- [ ] **Step 4: Run the focused frontend test or type check**

Expected: the utility tests or TypeScript compilation pass.

### Task 4: Pending Product Genre Selector

**Files:**
- Create: `src/components/crawler/PendingProductGenreSelect.vue`
- Modify: `src/components/crawler/ProductWorkflowView.vue`

**Interfaces:**
- Props: `product: ProductItem`, `disabled?: boolean`
- Emits: `updated(product: ProductItem)`
- Consumes: `searchRakutenGenres`, `updatePendingProductGenre`

- [ ] **Step 1: Implement the focused selector component**

Use an Element Plus remote searchable select. Seed the current option, debounce remote queries, render complete paths with IDs, save immediately on selection, retain the previous value until success, and emit the backend-returned product.

- [ ] **Step 2: Integrate the selector into pending product information**

Display it only for `status === "pending"`, show `品类编号：xxxxxx` or `品类编号：未设置`, and replace the matching product in `products` after a successful save.

- [ ] **Step 3: Add approval prechecks**

Before single approval, show `请先选择有效品类，当前商品不能审核通过`. Before batch approval, list the invalid count and up to three product titles, then return without opening the confirmation dialog or calling the API.

- [ ] **Step 4: Run frontend type and build validation**

Run: `npm run build`

Expected: Vue TypeScript checking and Vite production build complete successfully without starting the application.

### Task 5: Full Verification and Review

**Files:**
- Review all files changed by Tasks 1-4.

**Interfaces:**
- Verifies all preceding interfaces and constraints.

- [ ] **Step 1: Run backend verification**

Run: `python -m pytest tests/test_product_genres.py tests/test_ai_title_service.py -q`

- [ ] **Step 2: Run frontend verification**

Run: `npm run build`

- [ ] **Step 3: Run diff and whitespace checks**

Run backend and frontend `git diff --check`, inspect `git diff --stat`, and confirm no unrelated existing changes were reverted or included.

- [ ] **Step 4: Review requirement coverage**

Confirm full-path display, ID synchronization, pending-only edits, single and batch early warnings, backend approval enforcement, atomic failure, and no application startup/UI inspection.
