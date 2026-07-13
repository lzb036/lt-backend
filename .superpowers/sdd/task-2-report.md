# Task 2 Report: Persistence, Seed Data, and CRUD

## Scope

- Plan: `D:\ERP\lt-backend\docs\superpowers\plans\2026-07-13-sensitive-word-management.md`
- Implemented task: `Task 2: Persistence, Seed Data, and CRUD`
- Owned source files:
  - `D:\ERP\lt-backend\app\services\sensitive_word_service.py`
  - `D:\ERP\lt-backend\app\db\database.py`
  - `D:\ERP\lt-backend\tests\test_sensitive_words.py`
- Preserved unrelated working-tree changes in:
  - `D:\ERP\lt-backend\app\services\crawler_service.py`
  - `D:\ERP\lt-backend\tests\test_crawler_product_image_storage.py`
  - deleted files under `D:\ERP\lt-backend\docs\superpowers\...`
  - `D:\ERP\lt-backend\scripts\dedupe_pending_product_images.py`

## TDD Record

### RED

- Command:

```powershell
python -m unittest tests.test_sensitive_words -v
```

- Result: failed as expected before Task 2 implementation.
- Key evidence:

```text
ImportError: cannot import name 'DEFAULT_SENSITIVE_WORDS' from 'app.services.sensitive_word_service'
AttributeError: <module 'app.services.sensitive_word_service' ...> does not have the attribute 'SessionLocal'
FAILED (failures=1, errors=4)
```

### GREEN

- Command:

```powershell
python -m unittest tests.test_sensitive_words -v
```

- Result: passed after adding persistence, seeding, and CRUD support.
- Key evidence:

```text
Ran 11 tests in 0.987s

OK
```

## Implemented Changes

### `app/services/sensitive_word_service.py`

- Added `DEFAULT_SENSITIVE_WORDS` with the trimmed, deduplicated seed words used by the current design examples, including `【】`.
- Added `seed_default_sensitive_words(session) -> int`.
- Added `list_sensitive_words(page, page_size, keyword="") -> dict[str, Any]`.
- Added `create_sensitive_word(word, enabled=True) -> dict[str, Any]`.
- Added `update_sensitive_word(word_id, word, enabled) -> dict[str, Any]`.
- Added `delete_sensitive_word(word_id) -> bool`.
- Added `active_sensitive_words(session) -> list[str]`.
- Added pagination normalization and public-row serialization helpers.
- Mapped duplicate writes to `RuntimeError("敏感词已存在。")` and blank values to `RuntimeError("敏感词不能为空。")`.

### `app/db/database.py`

- Kept the existing `Base.metadata.create_all(bind=engine)` flow intact.
- Added post-initialization sensitive-word seeding inside `init_database()`.
- Wrapped the seed session with explicit commit / rollback / close handling so startup stays transactional.

### `tests/test_sensitive_words.py`

- Added in-memory SQLite persistence coverage.
- Added seed idempotence coverage.
- Added CRUD, pagination, filtering, enable/disable, delete, and active-word ordering coverage.
- Added `init_database()` coverage to prove seeding is wired into startup without skipping the existing bootstrap steps.

## Verification

- Tests:
  - `python -m unittest tests.test_sensitive_words -v`
- Compile check:
  - `python -m compileall app\services\sensitive_word_service.py app\db\database.py`
- Diff hygiene:
  - `git diff --check -- app\services\sensitive_word_service.py app\db\database.py tests\test_sensitive_words.py`
  - Result: no whitespace errors; Git printed line-ending warnings only.

## Commit

- Commit SHA: `See final response for the immutable HEAD commit SHA.`
- Commit message: `feat: persist global sensitive words`

## Self-Review

- Startup seeding is idempotent because it compares normalized words already present in the table before inserting missing defaults.
- CRUD paths use database-backed uniqueness instead of hand-maintained duplicate caches, so duplicate behavior remains correct under concurrent writes.
- The initial seed list was reconstructed from the current design/plan examples available in-repo. If there is a longer authoritative list outside the repo history, Task 2 may need a follow-up seed expansion.
