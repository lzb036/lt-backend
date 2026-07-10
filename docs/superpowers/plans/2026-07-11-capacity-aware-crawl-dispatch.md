# Capacity-Aware Crawl Dispatch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep the full crawl backlog in MySQL while allowing only the configured per-user capacity to occupy Redis crawl jobs, then deploy production with three concurrent crawl tasks.

**Architecture:** Persist each queued task's reserved RQ job ID in `lt_crawl_tasks.queue_job_id`. A Redis distributed lock serializes reservation, the dispatcher reserves only available per-user slots, workers clear reservations on start, and completion or periodic maintenance fills the next slots.

**Tech Stack:** Python 3.11, SQLAlchemy 2, MySQL 8, Redis, RQ, `unittest`, Supervisor, systemd.

## Global Constraints

- Do not start duplicate frontend or backend application processes.
- Preserve every MySQL crawl task during deployment.
- Keep current crawl delays, retry count, batch size, and batch pause.
- Keep existing public task statuses compatible.
- Set production per-user crawl concurrency and crawl Worker count to `3`.
- Use test-first red-green verification for every behavior change.

---

### Task 1: Add Queue Reservation Persistence

**Files:**
- Modify: `app/db/models.py:151-179`
- Modify: `app/db/database.py:374-410`
- Create: `tests/test_crawl_dispatch.py`

**Interfaces:**
- Produces: `CrawlTaskModel.queue_job_id: str | None`
- Produces: MySQL compatibility migration adding `queue_job_id VARCHAR(64) NULL`

- [ ] **Step 1: Write the failing model test**

```python
class CrawlDispatchModelTests(unittest.TestCase):
    def test_crawl_task_persists_reserved_queue_job_id(self):
        self.assertIn("queue_job_id", CrawlTaskModel.__table__.columns)
        column = CrawlTaskModel.__table__.columns["queue_job_id"]
        self.assertTrue(column.nullable)
        self.assertEqual(column.type.length, 64)
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```powershell
python -m unittest tests.test_crawl_dispatch.CrawlDispatchModelTests.test_crawl_task_persists_reserved_queue_job_id -v
```

Expected: `FAIL` because `queue_job_id` is absent.

- [ ] **Step 3: Add the model field and migration**

Add to `CrawlTaskModel`:

```python
queue_job_id: Mapped[str | None] = mapped_column(String(64))
```

Add to `ensure_schema_compatibility()`:

```python
if "queue_job_id" not in crawl_task_columns:
    connection.execute(
        text(
            "ALTER TABLE lt_crawl_tasks "
            "ADD COLUMN queue_job_id VARCHAR(64) NULL"
        )
    )
```

- [ ] **Step 4: Run the focused test and verify GREEN**

Run the command from Step 2. Expected: `OK`.

- [ ] **Step 5: Commit**

```powershell
git add app/db/models.py app/db/database.py tests/test_crawl_dispatch.py
git commit -m "feat: persist crawl queue reservations"
```

### Task 2: Reserve Only Available Crawl Slots

**Files:**
- Modify: `app/services/crawler_service.py:434-458`
- Modify: `app/services/crawler_service.py:12316-12360`
- Modify: `app/services/crawler_service.py:12363-12385`
- Modify: `tests/test_crawl_dispatch.py`

**Interfaces:**
- Produces: `crawl_dispatch_job_id(task_id: str) -> str`
- Produces: `crawl_dispatch_available_slots(running_count: int, reserved_count: int, limit: int | None = None) -> int`
- Produces: `reserve_queued_crawl_tasks(session: Any, owner_username: str | None = None) -> list[tuple[str, str]]`
- Produces: `dispatch_queued_crawl_tasks(owner_username: str | None = None) -> int`
- Produces: `dispatch_queued_crawl_tasks_safely(owner_username: str | None = None) -> int`

- [ ] **Step 1: Add failing dispatcher tests**

Use an in-memory SQLite engine and a patched `session_scope`. Add tests proving:

```python
def test_dispatch_reserves_only_available_oldest_tasks(self):
    # one running + one reserved + limit three => reserve one oldest task
    ...
    self.assertEqual(dispatched, 1)
    self.assertEqual(enqueued_task_ids, ["oldest-unreserved"])

def test_dispatch_does_not_exceed_capacity(self):
    # three running/reserved slots => enqueue nothing
    ...
    self.assertEqual(dispatched, 0)

def test_enqueue_failure_clears_matching_reservation(self):
    ...
    self.assertIsNone(reloaded.queue_job_id)
```

The fake Redis connection must expose a lock with `acquire()` and `release()` so
the tests exercise dispatcher serialization without a real Redis server.

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```powershell
python -m unittest tests.test_crawl_dispatch.CrawlDispatcherTests -v
```

Expected: `ERROR` or `FAIL` because dispatcher functions do not exist.

- [ ] **Step 3: Implement capacity calculation and reservation**

Add constants:

```python
CRAWL_DISPATCH_LOCK_NAME = "lt:crawl-dispatch"
CRAWL_DISPATCH_LOCK_TIMEOUT_SECONDS = 30
CRAWL_DISPATCH_LOCK_BLOCKING_TIMEOUT_SECONDS = 3
```

Implement:

```python
def crawl_dispatch_available_slots(
    running_count: int,
    reserved_count: int,
    limit: int | None = None,
) -> int:
    capacity = max(
        1,
        int(limit or settings.max_running_crawl_tasks_per_user),
    )
    return max(0, capacity - max(0, running_count) - max(0, reserved_count))
```

`reserve_queued_crawl_tasks()` must process owners in order of their oldest
queued task, count `running` and reserved `queued` rows, and assign unique IDs
formatted as `crawl-{task_id}-{8 hex characters}` to the oldest unreserved rows.

- [ ] **Step 4: Implement distributed dispatch**

`dispatch_queued_crawl_tasks()` must:

1. acquire the Redis lock;
2. commit database reservations;
3. enqueue each task with its reserved `job_id`;
4. clear only the matching reservation when enqueueing fails; and
5. release the lock in `finally`.

Extend `dispatch_crawl_task()` with:

```python
def dispatch_crawl_task(
    task_id: str,
    *,
    delay_seconds: float = 0.0,
    job_id: str | None = None,
) -> None:
```

Pass `job_id=job_id` to `enqueue_task()` or `enqueue_task_in()`.

- [ ] **Step 5: Route task creation and reruns through the dispatcher**

In Redis mode, `create_task()` and `run_existing_task()` call:

```python
dispatch_queued_crawl_tasks_safely(owner_username)
```

Thread mode keeps the existing direct `dispatch_crawl_task(task_id)` behavior.

- [ ] **Step 6: Run focused tests and verify GREEN**

Run the command from Step 2. Expected: all dispatcher tests pass.

- [ ] **Step 7: Commit**

```powershell
git add app/services/crawler_service.py tests/test_crawl_dispatch.py
git commit -m "feat: dispatch crawl tasks by available capacity"
```

### Task 3: Remove Retry Storm and Refill Capacity

**Files:**
- Modify: `app/services/crawler_service.py:2968-2994`
- Modify: `app/services/crawler_service.py:12417-12577`
- Modify: `tests/test_crawl_dispatch.py`

**Interfaces:**
- Consumes: `dispatch_queued_crawl_tasks_safely()`
- Behavior: a concurrency-rejected task stays queued and is not scheduled after five seconds
- Behavior: every terminal task result triggers a capacity refill

- [ ] **Step 1: Write failing worker regression tests**

Add:

```python
def test_concurrency_rejected_task_is_not_requeued(self):
    # task has queue_job_id and another running task occupies the limit
    ...
    crawler_service.run_task(task.id)
    dispatch_crawl_task.assert_not_called()
    self.assertIsNone(reloaded.queue_job_id)
    self.assertEqual(reloaded.status, "queued")

def test_completed_task_refills_crawl_capacity(self):
    ...
    crawler_service.run_task(task.id)
    refill.assert_called_once_with(task.owner_username)
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```powershell
python -m unittest tests.test_crawl_dispatch.CrawlWorkerDispatchTests -v
```

Expected: the rejected task calls the existing delayed requeue path and the
completion test has no refill call.

- [ ] **Step 3: Remove hot-path reconciliation**

Remove this call from `task_start_wait_reason()`:

```python
reconcile_interrupted_running_tasks(
    session,
    model,
    owner_username=owner_username,
)
```

Periodic maintenance remains responsible for interrupted-task reconciliation.

- [ ] **Step 4: Refactor the start decision**

At the beginning of `run_task(task_id, reserved_job_id=None)`:

1. load the task;
2. capture `owner_username`;
3. require `reserved_job_id` to equal the persisted `queue_job_id` whenever
   either value is non-null;
4. clear `queue_job_id` only after that identity check passes;
5. finalize cancellation or duplicate/non-queued starts;
6. leave Redis-mode concurrency-rejected tasks in `queued` without calling
   `dispatch_crawl_task(..., delay_seconds=5)`; and
7. retain the five-second delayed retry only for legacy thread mode; and
8. mark accepted tasks `running`.

After a rejected or cancelled start, call
`dispatch_queued_crawl_tasks_safely(owner_username)` and return.

- [ ] **Step 5: Refill after every terminal outcome**

Wrap collection and persistence in:

```python
try:
    # existing collection, success, cancellation, and failure handling
    ...
finally:
    dispatch_queued_crawl_tasks_safely(owner_username)
```

The refill helper must catch and log dispatch errors so a completed task result
is never changed by a refill failure.

- [ ] **Step 6: Run focused tests and verify GREEN**

Run the command from Step 2. Expected: all worker dispatch tests pass.

- [ ] **Step 7: Commit**

```powershell
git add app/services/crawler_service.py tests/test_crawl_dispatch.py
git commit -m "fix: eliminate crawl queue retry storm"
```

### Task 4: Recover Reserved Jobs and Periodically Refill

**Files:**
- Modify: `app/services/crawler_service.py:2460-2517`
- Modify: `app/services/crawler_service.py:7627-7632`
- Modify: `tests/test_crawl_dispatch.py`

**Interfaces:**
- Behavior: unreserved MySQL backlog is not classified as a missing Redis job
- Behavior: a missing reserved job clears its reservation
- Behavior: periodic maintenance invokes the dispatcher

- [ ] **Step 1: Write failing recovery tests**

Add:

```python
def test_missing_job_recovery_ignores_unreserved_backlog(self):
    ...
    self.assertEqual(recovered, 0)

def test_missing_reserved_job_clears_reservation_without_direct_requeue(self):
    ...
    self.assertIsNone(reloaded.queue_job_id)
    direct_dispatch.assert_not_called()

def test_periodic_maintenance_refills_crawl_capacity(self):
    ...
    refill.assert_called_once_with()
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```powershell
python -m unittest tests.test_crawl_dispatch.CrawlRecoveryTests -v
```

- [ ] **Step 3: Limit queued recovery to reservations**

For `CrawlTaskModel`, add `queue_job_id IS NOT NULL` to the queued recovery
query. When its RQ state is missing, clear `queue_job_id`, keep status `queued`,
and set the message to `采集队列已恢复，等待重新投递`.

Do not call `dispatch_crawl_task()` from inside the database transaction.

- [ ] **Step 4: Refill from periodic maintenance**

After `reconcile_interrupted_background_tasks_once()` completes, call:

```python
dispatch_queued_crawl_tasks_safely()
```

- [ ] **Step 5: Run focused tests and verify GREEN**

Run the command from Step 2. Expected: all recovery tests pass.

- [ ] **Step 6: Commit**

```powershell
git add app/services/crawler_service.py tests/test_crawl_dispatch.py
git commit -m "fix: recover capacity-aware crawl reservations"
```

### Task 5: Full Verification and Production Deployment

**Files:**
- Modify on server: `/www/wwwroot/lt/lt-backend/.env`
- Modify on server: `/www/server/panel/plugin/supervisor/profile/lt-worker-crawl.ini`

**Interfaces:**
- Production setting: `LT_MAX_RUNNING_CRAWL_TASKS_PER_USER=3`
- Production Supervisor setting: `numprocs=3`

- [ ] **Step 1: Run complete local verification**

```powershell
python -m unittest discover -s tests -v
python -m compileall app scripts tests
git diff --check
```

Expected: zero failed tests, successful compilation, and no whitespace errors.

- [ ] **Step 2: Review the final diff**

```powershell
git status --short
git diff --stat HEAD~4
git diff HEAD~4 -- app/db/models.py app/db/database.py app/services/crawler_service.py tests/test_crawl_dispatch.py
```

- [ ] **Step 3: Push the verified revision**

```powershell
git push origin main
```

- [ ] **Step 4: Prepare managed workers for graceful stop**

Back up the Supervisor profile. Set:

```ini
numprocs=3
stopsignal=TERM
stopwaitsecs=10830
stopasgroup=false
killasgroup=true
```

Run Supervisor reread/update without starting duplicate processes.

- [ ] **Step 5: Stop the existing crawl workers**

Use the existing Supervisor instance and wait until all crawl worker and
workhorse processes exit. Confirm no `/worker.py crawl` process remains.

- [ ] **Step 6: Clear only active Redis crawl jobs**

Empty `lt-tasks-crawl` queued, started, scheduled, and deferred active jobs.
Retain failed/finished registries and every MySQL task row.

Reset deployment-interrupted `running` crawl tasks to `queued`, clear
`queue_job_id`, `started_at`, and `finished_at`, and set the message to
`部署后等待重新执行`.

- [ ] **Step 7: Deploy code and configuration**

Pull `main`, install requirements, set:

```text
LT_MAX_RUNNING_CRAWL_TASKS_PER_USER=3
```

Restart the existing managed backend first, then start the three managed crawl
workers. Do not launch an additional Uvicorn or worker process manually.

- [ ] **Step 8: Trigger and verify dispatch**

Run one backend-level dispatcher invocation as user `www`, then verify:

```text
running tasks for superadmin <= 3
reserved queued tasks for superadmin + running tasks <= 3
Redis crawl queued + started + scheduled <= 3
Supervisor crawl processes = 3
backend health = ok
```

- [ ] **Step 9: Measure production improvement**

Observe at least ten newly completed scheduled crawls. Report completion rate,
task duration distribution, server load, available memory, Swap activity, Redis
CPU, MySQL CPU, and remaining ETA.
