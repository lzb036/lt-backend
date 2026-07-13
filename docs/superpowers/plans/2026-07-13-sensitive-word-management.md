# Sensitive Word Management Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add superadmin-only global sensitive-word management, automatically sanitize collected product titles and taglines, and provide a dry-run-first cleanup script for existing pending products.

**Architecture:** Store normalized sensitive words in a dedicated database table and isolate CRUD, Excel parsing, text sanitization, payload traversal, and seed behavior in `app/services/sensitive_word_service.py`. Expose superadmin-only endpoints through the existing crawler router, call the sanitizer from the common product upsert path, and add a standalone Vue list page plus a one-time backend cleanup script.

**Tech Stack:** Python 3, FastAPI, SQLAlchemy 2, openpyxl, unittest, Vue 3 Composition API, TypeScript, Element Plus, Axios.

## Global Constraints

- The sensitive-word list is global across all users.
- Only users with role `superadmin` may view or manage sensitive words.
- The special sensitive word `【】` removes every complete `【...】` segment, including brackets and contents.
- Existing cleanup is restricted to products with `review_status = "pending"`.
- The cleanup script defaults to dry-run and requires an explicit apply flag to update data.
- Do not start additional frontend or backend processes and do not open the UI for verification.
- Preserve unrelated existing working-tree changes.

---

## File Structure

### Backend

- Create `app/services/sensitive_word_service.py`: sensitive-word normalization, CRUD, Excel, seed data, title/tagline sanitization, raw-payload traversal, and pending-product cleanup.
- Modify `app/db/models.py`: add `SensitiveWordModel`.
- Modify `app/db/database.py`: let existing `Base.metadata.create_all()` create the new table and seed defaults after schema initialization.
- Modify `app/api/crawler.py`: add superadmin-only sensitive-word endpoints and request payloads.
- Modify `app/services/crawler_service.py`: sanitize collected item data at `upsert_product()` before persistence.
- Create `scripts/cleanup_pending_sensitive_words.py`: dry-run-first operational cleanup command.
- Create `tests/test_sensitive_words.py`: service, persistence, sanitizer, seed, Excel, and cleanup coverage.
- Create `tests/test_sensitive_word_api.py`: endpoint dependency and response/error mapping coverage.

### Frontend

- Create `src/components/crawler/SensitiveWordManagementView.vue`: list, search, pagination, add/edit, enable/disable, delete, template download, and import.
- Modify `src/types/crawler.ts`: sensitive-word and import-result interfaces.
- Modify `src/composables/useCollectorApi.ts`: sensitive-word API methods.
- Modify `src/router.ts`: superadmin-only route.
- Modify `src/components/crawler/AppShell.vue`: superadmin-only menu entry.

---

### Task 1: Sensitive-Word Model and Text Sanitizer

**Files:**
- Modify: `app/db/models.py`
- Create: `app/services/sensitive_word_service.py`
- Test: `tests/test_sensitive_words.py`

**Interfaces:**
- Produces: `SensitiveWordModel`
- Produces: `normalize_sensitive_word(value: Any) -> str`
- Produces: `sanitize_sensitive_text(value: Any, words: Iterable[str]) -> str`
- Produces: `sanitize_product_payload(payload: dict[str, Any], words: Iterable[str]) -> tuple[dict[str, Any], bool]`

- [ ] **Step 1: Write failing sanitizer tests**

Add tests proving:

```python
def test_removes_normal_words_and_collapses_whitespace():
    assert sanitize_sensitive_text(
        "楽天1位  春物  即納",
        ["楽天1位", "即納"],
    ) == "春物"


def test_empty_bracket_rule_removes_every_bracketed_segment():
    assert sanitize_sensitive_text(
        "【楽天1位】【日本国内発送】 春物",
        ["【】"],
    ) == "春物"


def test_payload_sanitizer_updates_title_and_tagline_fields_recursively():
    payload = {
        "title": "【楽天1位】 春物",
        "itemName": "【楽天1位】 春物",
        "tagline": "即納 おすすめ",
        "item": {"subtitle": "【期間限定】 即納"},
        "description": "【期間限定】 即納",
    }
    cleaned, changed = sanitize_product_payload(payload, ["【】", "即納"])
    assert changed is True
    assert cleaned["title"] == "春物"
    assert cleaned["itemName"] == "春物"
    assert cleaned["tagline"] == "おすすめ"
    assert cleaned["item"]["subtitle"] == ""
    assert cleaned["description"] == "【期間限定】 即納"
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```powershell
python -m unittest tests.test_sensitive_words -v
```

Expected: import failure because `app.services.sensitive_word_service` does not exist.

- [ ] **Step 3: Add the model and minimal sanitizer**

Add `SensitiveWordModel` with:

```python
class SensitiveWordModel(TimestampMixin, Base):
    __tablename__ = "lt_sensitive_words"
    __table_args__ = (UniqueConstraint("word", name="uq_lt_sensitive_word"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    word: Mapped[str] = mapped_column(String(500), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="1")
```

Implement the sanitizer with a compiled `【[^】]*】` regular expression, literal `str.replace()` for normal words, recursive dictionary/list traversal, and an explicit allowed field-name set.

- [ ] **Step 4: Run tests and verify GREEN**

Run:

```powershell
python -m unittest tests.test_sensitive_words -v
```

Expected: sanitizer tests pass.

- [ ] **Step 5: Commit**

```powershell
git add app/db/models.py app/services/sensitive_word_service.py tests/test_sensitive_words.py
git commit -m "feat: add sensitive word sanitizer"
```

### Task 2: Persistence, Seed Data, and CRUD

**Files:**
- Modify: `app/services/sensitive_word_service.py`
- Modify: `app/db/database.py`
- Modify: `tests/test_sensitive_words.py`

**Interfaces:**
- Produces: `DEFAULT_SENSITIVE_WORDS: tuple[str, ...]`
- Produces: `seed_default_sensitive_words(session: Any) -> int`
- Produces: `list_sensitive_words(page: int, page_size: int, keyword: str = "") -> dict[str, Any]`
- Produces: `create_sensitive_word(word: str, enabled: bool = True) -> dict[str, Any]`
- Produces: `update_sensitive_word(word_id: int, word: str, enabled: bool) -> dict[str, Any]`
- Produces: `delete_sensitive_word(word_id: int) -> bool`
- Produces: `active_sensitive_words(session: Any) -> list[str]`

- [ ] **Step 1: Add failing database tests**

Use an in-memory SQLite engine with `Base.metadata.create_all()` and patch the service session factory. Test:

```python
def test_seed_is_idempotent_and_deduplicates_default_words():
    assert seed_default_sensitive_words(session) > 0
    session.commit()
    assert seed_default_sensitive_words(session) == 0
    assert session.scalar(select(func.count()).select_from(SensitiveWordModel)) == len(set(DEFAULT_SENSITIVE_WORDS))


def test_crud_normalizes_words_and_rejects_duplicates():
    created = create_sensitive_word("  即納  ")
    assert created["word"] == "即納"
    with self.assertRaisesRegex(RuntimeError, "已存在"):
        create_sensitive_word("即納")
```

Also test filtering, pagination, enabled-state updates, deletion, and active-word ordering by descending word length.

- [ ] **Step 2: Run tests and verify RED**

```powershell
python -m unittest tests.test_sensitive_words -v
```

Expected: failures for missing CRUD and seed functions.

- [ ] **Step 3: Implement seed and CRUD**

Seed the user-provided values after trimming and deduplication. Query active words on each product save through the current transaction so database failures propagate rather than silently skipping sanitization.

At the end of `init_database()`, open a session, call `seed_default_sensitive_words(session)`, and commit. Existing `Base.metadata.create_all(bind=engine)` creates `lt_sensitive_words`.

- [ ] **Step 4: Run tests and verify GREEN**

```powershell
python -m unittest tests.test_sensitive_words -v
```

Expected: all model, seed, sanitizer, and CRUD tests pass.

- [ ] **Step 5: Commit**

```powershell
git add app/db/database.py app/services/sensitive_word_service.py tests/test_sensitive_words.py
git commit -m "feat: persist global sensitive words"
```

### Task 3: Excel Template and Import

**Files:**
- Modify: `app/services/sensitive_word_service.py`
- Modify: `tests/test_sensitive_words.py`

**Interfaces:**
- Produces: `build_sensitive_word_template() -> bytes`
- Produces: `import_sensitive_words(content: bytes, filename: str) -> dict[str, int]`

- [ ] **Step 1: Add failing Excel tests**

Test that the template contains a worksheet named `敏感词导入` with header `敏感词`, and that imports return:

```python
{
    "createdCount": 2,
    "duplicateCount": 2,
    "invalidCount": 1,
}
```

for a workbook containing new values, an existing value, an in-file duplicate, and an empty row. Add tests for non-`.xlsx` files and a missing header.

- [ ] **Step 2: Run tests and verify RED**

```powershell
python -m unittest tests.test_sensitive_words -v
```

Expected: failures for missing template/import functions.

- [ ] **Step 3: Implement workbook generation and parsing**

Use `openpyxl.Workbook` and `openpyxl.load_workbook(BytesIO(content), read_only=True, data_only=True)`. Accept only `.xlsx`, require the normalized header `敏感词`, append new rows, and count database and workbook duplicates without raising.

- [ ] **Step 4: Run tests and verify GREEN**

```powershell
python -m unittest tests.test_sensitive_words -v
```

Expected: Excel tests pass.

- [ ] **Step 5: Commit**

```powershell
git add app/services/sensitive_word_service.py tests/test_sensitive_words.py
git commit -m "feat: import sensitive words from xlsx"
```

### Task 4: Superadmin-Only API

**Files:**
- Modify: `app/api/crawler.py`
- Create: `tests/test_sensitive_word_api.py`

**Interfaces:**
- Consumes: service CRUD, template, and import functions from Tasks 2 and 3.
- Produces endpoints under `/api/crawler/settings/sensitive-words`.

- [ ] **Step 1: Write failing API tests**

Verify every route dependency is `require_superadmin`:

```python
paths = {
    "/crawler/settings/sensitive-words",
    "/crawler/settings/sensitive-words/template",
    "/crawler/settings/sensitive-words/import",
    "/crawler/settings/sensitive-words/{word_id}",
}
```

Test list response shape, create/update/delete delegation, template headers, import response, duplicate conflict as HTTP 409, validation failures as HTTP 400, and missing rows as HTTP 404.

- [ ] **Step 2: Run tests and verify RED**

```powershell
python -m unittest tests.test_sensitive_word_api -v
```

Expected: route lookup or handler import failures.

- [ ] **Step 3: Add payloads and routes**

Add:

```python
class SensitiveWordPayload(BaseModel):
    word: str = Field(min_length=1, max_length=500)
    enabled: bool = True
```

Implement GET list, POST create, PUT update, DELETE, GET template, and POST import handlers. Return `StreamingResponse` with UTF-8 encoded filename `敏感词导入模板.xlsx`.

- [ ] **Step 4: Run API and service tests**

```powershell
python -m unittest tests.test_sensitive_word_api tests.test_sensitive_words -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```powershell
git add app/api/crawler.py tests/test_sensitive_word_api.py
git commit -m "feat: expose sensitive word admin api"
```

### Task 5: Collected-Product Integration

**Files:**
- Modify: `app/services/crawler_service.py`
- Modify: `tests/test_sensitive_words.py`

**Interfaces:**
- Consumes: `active_sensitive_words(session)` and `sanitize_product_payload(payload, words)`.
- Produces: sanitized `ProductModel.title` and `raw_payload_json` from `upsert_product()`.

- [ ] **Step 1: Write failing upsert tests**

Test an item such as:

```python
item = {
    "source_url": "https://example.test/item/1",
    "title": "【楽天1位】 即納 春物",
    "raw": {
        "title": "【楽天1位】 即納 春物",
        "itemName": "【楽天1位】 即納 春物",
        "tagline": "翌日配達 おすすめ",
    },
}
```

with active words `["【】", "即納", "翌日配達"]`. Assert stored title is `春物` and stored payload fields are sanitized. Add a test that a fully removed title makes `upsert_product()` return `False` without adding a row.

- [ ] **Step 2: Run the focused tests and verify RED**

```powershell
python -m unittest tests.test_sensitive_words.SensitiveWordUpsertTests -v
```

Expected: stored title and payload still contain sensitive text.

- [ ] **Step 3: Integrate sanitization at the start of `upsert_product()`**

Load active words through the current session, sanitize a deep copy of `item`, derive `source_url`, `title`, and `raw_payload` from the cleaned copy, and preserve existing deduplication and listing-product behavior.

- [ ] **Step 4: Run focused and crawler regression tests**

```powershell
python -m unittest tests.test_sensitive_words tests.test_crawl_duplicate_prefilter tests.test_crawl_min_price_filter -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```powershell
git add app/services/crawler_service.py tests/test_sensitive_words.py
git commit -m "feat: sanitize collected product text"
```

### Task 6: Pending-Product Cleanup Script

**Files:**
- Modify: `app/services/sensitive_word_service.py`
- Create: `scripts/cleanup_pending_sensitive_words.py`
- Modify: `tests/test_sensitive_words.py`

**Interfaces:**
- Produces: `cleanup_pending_products(session: Any, *, apply: bool = False) -> dict[str, int]`
- Produces CLI flag: `--apply`

- [ ] **Step 1: Write failing cleanup tests**

Create pending, approved, listed, and error products. Assert dry-run reports but changes none; apply mode changes only pending rows; a pending row whose title becomes empty remains unchanged and increments `emptyTitleCount`.

- [ ] **Step 2: Run tests and verify RED**

```powershell
python -m unittest tests.test_sensitive_words.SensitiveWordCleanupTests -v
```

Expected: missing cleanup function.

- [ ] **Step 3: Implement cleanup service and CLI**

Return:

```python
{
    "scannedCount": scanned_count,
    "matchedCount": matched_count,
    "updatedCount": updated_count,
    "emptyTitleCount": empty_title_count,
}
```

The CLI initializes the database, opens one session, prints the summary as JSON, rolls back in dry-run mode, and commits only with `--apply`.

- [ ] **Step 4: Run cleanup tests and CLI help**

```powershell
python -m unittest tests.test_sensitive_words.SensitiveWordCleanupTests -v
python scripts/cleanup_pending_sensitive_words.py --help
```

Expected: tests pass and help shows optional `--apply`.

- [ ] **Step 5: Commit**

```powershell
git add app/services/sensitive_word_service.py scripts/cleanup_pending_sensitive_words.py tests/test_sensitive_words.py
git commit -m "feat: clean pending product sensitive words"
```

### Task 7: Frontend API, Route, Menu, and Management Page

**Files:**
- Create: `D:/ERP/lt-frontend/src/components/crawler/SensitiveWordManagementView.vue`
- Modify: `D:/ERP/lt-frontend/src/types/crawler.ts`
- Modify: `D:/ERP/lt-frontend/src/composables/useCollectorApi.ts`
- Modify: `D:/ERP/lt-frontend/src/router.ts`
- Modify: `D:/ERP/lt-frontend/src/components/crawler/AppShell.vue`

**Interfaces:**
- Consumes backend endpoints from Task 4.
- Produces `SensitiveWord`, `SensitiveWordPayload`, and `SensitiveWordImportResult` TypeScript interfaces.

- [ ] **Step 1: Add types and API methods, then verify current build fails until exports are wired**

Define:

```typescript
export interface SensitiveWord {
  id: number
  word: string
  ruleType: 'literal' | 'bracket'
  enabled: boolean
  createdAt: string
  updatedAt: string
}

export interface SensitiveWordImportResult {
  createdCount: number
  duplicateCount: number
  invalidCount: number
}
```

Add list/create/update/delete/template/import methods to `useCollectorApi()`.

- [ ] **Step 2: Run type/build verification**

```powershell
npm run build
```

Expected before page/router wiring: build may pass for types alone; record the result and continue. No dev server is started.

- [ ] **Step 3: Implement the management page**

Use Element Plus table, pagination, search input, hidden file input, and dialogs. Provide exactly three primary toolbar commands: `下载模板`, `导入表格`, and `添加敏感词汇`. Add edit, enable/disable, and delete row actions with clear loading and error states.

- [ ] **Step 4: Add superadmin-only route and menu**

Import the page in `router.ts`, add:

```typescript
{
  path: 'system/sensitive-words',
  name: 'system-sensitive-words',
  component: SensitiveWordManagementView,
  meta: { title: '敏感词管理', superadminOnly: true },
}
```

In `AppShell.vue`, append the menu item only inside `if (isSuperadmin.value)`, while retaining the existing settings group for `settings.manage`.

- [ ] **Step 5: Run frontend build**

```powershell
npm run build
```

Expected: `vue-tsc --noEmit` and Vite build both succeed. Do not start or preview the application.

- [ ] **Step 6: Commit frontend changes**

```powershell
git add src/components/crawler/SensitiveWordManagementView.vue src/types/crawler.ts src/composables/useCollectorApi.ts src/router.ts src/components/crawler/AppShell.vue
git commit -m "feat: add sensitive word management page"
```

### Task 8: Full Verification and Production Cleanup

**Files:**
- No new source files.

**Interfaces:**
- Consumes all prior tasks.

- [ ] **Step 1: Run the complete backend test suite**

```powershell
python -m unittest discover -s tests -v
```

Expected: all tests pass.

- [ ] **Step 2: Run frontend production build**

```powershell
npm run build
```

Expected: type check and build pass without starting an application process.

- [ ] **Step 3: Review scoped diffs**

```powershell
git diff --check
git status --short
```

Run separately in `D:\ERP\lt-backend` and `D:\ERP\lt-frontend`. Expected: no whitespace errors; unrelated pre-existing changes remain untouched.

- [ ] **Step 4: Deploy through the repository's established production workflow**

Use only the existing deployment mechanism discovered from project documentation or server configuration. Do not invent a new service process or start a second frontend/backend instance.

- [ ] **Step 5: Run production cleanup dry-run**

```powershell
python scripts/cleanup_pending_sensitive_words.py
```

Expected: JSON summary with no database updates.

- [ ] **Step 6: Run production cleanup apply after reviewing dry-run counts**

```powershell
python scripts/cleanup_pending_sensitive_words.py --apply
```

Expected: only pending products are updated and the final JSON summary reports scanned, matched, updated, and empty-title counts.

