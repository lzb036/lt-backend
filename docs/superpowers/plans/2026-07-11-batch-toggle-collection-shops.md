# Batch Toggle Collection Shops Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add batch enable and batch disable actions for selected collection shops while leaving already-running crawl tasks untouched.

**Architecture:** Add one owner-scoped backend service operation and a dedicated `PUT /crawler/schedules/status` endpoint. Reuse the collection-shop table's existing selection state in Vue, add a typed API method, and place the two new actions immediately before batch delete.

**Tech Stack:** Python 3.11, FastAPI, Pydantic, SQLAlchemy 2, `unittest`, Vue 3, TypeScript, Element Plus, Axios, Vite.

## Global Constraints

- Do not start or separately run the frontend or backend application.
- Batch disable prevents future scheduling but does not cancel an existing crawl task.
- Only schedules owned by the authenticated user may be updated.
- Ignore non-positive or non-numeric IDs, deduplicate valid IDs, and report missing or inaccessible valid IDs as failures.
- Button order is `批量启用`, `批量停用`, `批量删除`.
- Use success, warning, and danger button types for the three batch actions.
- Keep the existing collection-shop selection and refresh behavior.
- Use test-first red-green verification for backend behavior.

---

### Task 1: Add Service-Level Batch Status Behavior

**Files:**
- Create: `tests/test_scheduled_crawl_status.py`
- Modify: `app/services/crawler_service.py:7616-7657`

**Interfaces:**
- Produces: `update_scheduled_crawl_statuses(owner_username: str, schedule_ids: list[int], enabled: bool) -> dict[str, Any]`
- Produces response keys: `updatedIds`, `failedIds`, `updatedCount`, `enabled`

- [ ] **Step 1: Write the failing service tests**

Create an in-memory SQLite test case that patches `crawler_service.session_scope`.

```python
class ScheduledCrawlStatusTests(unittest.TestCase):
    def test_batch_enable_recalculates_next_run(self):
        schedule_id = self.add_schedule(
            owner="owner",
            enabled=False,
            status="disabled",
            next_run_at=None,
        )
        expected_next_run = datetime(2026, 7, 12, 20, 0)

        with (
            patch.object(crawler_service, "session_scope", self.session_scope),
            patch.object(
                crawler_service,
                "next_daily_run_at",
                return_value=expected_next_run,
            ),
        ):
            result = crawler_service.update_scheduled_crawl_statuses(
                "owner",
                [schedule_id],
                True,
            )

        row = self.get_schedule(schedule_id)
        self.assertTrue(row.enabled)
        self.assertEqual(row.status, "idle")
        self.assertEqual(row.next_run_at, expected_next_run)
        self.assertEqual(result["updatedIds"], [schedule_id])
        self.assertEqual(result["failedIds"], [])

    def test_batch_disable_does_not_cancel_running_crawl_task(self):
        schedule_id = self.add_schedule(
            owner="owner",
            enabled=True,
            status="running",
            next_run_at=datetime(2026, 7, 12, 20, 0),
        )
        self.add_running_crawl_task("running-task")

        with patch.object(crawler_service, "session_scope", self.session_scope):
            result = crawler_service.update_scheduled_crawl_statuses(
                "owner",
                [schedule_id],
                False,
            )

        row = self.get_schedule(schedule_id)
        task = self.get_crawl_task("running-task")
        self.assertFalse(row.enabled)
        self.assertEqual(row.status, "disabled")
        self.assertIsNone(row.next_run_at)
        self.assertEqual(task.status, "running")
        self.assertEqual(result["updatedCount"], 1)

    def test_batch_status_normalizes_ids_and_reports_inaccessible_rows(self):
        owned_id = self.add_schedule(owner="owner")
        other_id = self.add_schedule(owner="other")

        with patch.object(crawler_service, "session_scope", self.session_scope):
            result = crawler_service.update_scheduled_crawl_statuses(
                "owner",
                [owned_id, owned_id, other_id, 999999, 0, -1],
                False,
            )

        self.assertEqual(result["updatedIds"], [owned_id])
        self.assertEqual(result["failedIds"], [other_id, 999999])

    def test_batch_status_rejects_empty_normalized_ids(self):
        with (
            patch.object(crawler_service, "session_scope", self.session_scope),
            self.assertRaisesRegex(RuntimeError, "请选择要启用或停用的采集店铺"),
        ):
            crawler_service.update_scheduled_crawl_statuses(
                "owner",
                [0, -1],
                True,
            )
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```powershell
python -m unittest tests.test_scheduled_crawl_status -v
```

Expected: `ERROR` because `update_scheduled_crawl_statuses` does not exist.

- [ ] **Step 3: Implement the minimal service operation**

Add next to the existing single and batch delete functions:

```python
def update_scheduled_crawl_statuses(
    owner_username: str,
    schedule_ids: list[int],
    enabled: bool,
) -> dict[str, Any]:
    normalized_ids: list[int] = []
    seen: set[int] = set()
    for value in schedule_ids or []:
        try:
            schedule_id = int(value)
        except (TypeError, ValueError):
            continue
        if schedule_id <= 0 or schedule_id in seen:
            continue
        seen.add(schedule_id)
        normalized_ids.append(schedule_id)
    if not normalized_ids:
        raise RuntimeError("请选择要启用或停用的采集店铺。")

    with session_scope() as session:
        rows = session.scalars(
            select(ScheduledCrawlModel).where(
                ScheduledCrawlModel.owner_username == owner_username,
                ScheduledCrawlModel.id.in_(normalized_ids),
            )
        ).all()
        found_ids = {int(row.id) for row in rows}
        for row in rows:
            row.enabled = bool(enabled)
            row.status = "idle" if enabled else "disabled"
            row.next_run_at = next_daily_run_at(row.schedule_time) if enabled else None
        return {
            "updatedIds": sorted(found_ids),
            "failedIds": [
                schedule_id
                for schedule_id in normalized_ids
                if schedule_id not in found_ids
            ],
            "updatedCount": len(found_ids),
            "enabled": bool(enabled),
        }
```

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run the command from Step 2. Expected: all four tests pass.

- [ ] **Step 5: Commit the backend service behavior**

```powershell
git add app/services/crawler_service.py tests/test_scheduled_crawl_status.py
git commit -m "feat: batch update collection shop status"
```

### Task 2: Add the Batch Status API Endpoint

**Files:**
- Modify: `app/api/crawler.py:44-55`
- Modify: `app/api/crawler.py:816-838`

**Interfaces:**
- Consumes: `crawler_service.update_scheduled_crawl_statuses(owner_username, schedule_ids, enabled)`
- Produces: `PUT /crawler/schedules/status`
- Request: `{ "scheduleIds": number[], "enabled": boolean }`

- [ ] **Step 1: Add the request model**

```python
class ScheduleStatusBatchPayload(BaseModel):
    scheduleIds: list[int] = Field(default_factory=list)
    enabled: bool
```

- [ ] **Step 2: Add the static route before `/schedules/{schedule_id}`**

```python
@router.put("/schedules/status")
def update_schedule_statuses(
    payload: ScheduleStatusBatchPayload,
    user: dict = Depends(require_crawler_permission),
) -> dict:
    try:
        return crawler_service.update_scheduled_crawl_statuses(
            user["username"],
            payload.scheduleIds,
            payload.enabled,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
```

- [ ] **Step 3: Run backend syntax and focused behavior checks**

```powershell
python -m compileall app
python -m unittest tests.test_scheduled_crawl_status -v
```

Expected: compile succeeds and focused tests pass.

- [ ] **Step 4: Commit the API endpoint**

```powershell
git add app/api/crawler.py
git commit -m "feat: expose collection shop batch status api"
```

### Task 3: Add Frontend Batch Enable and Disable Actions

**Files:**
- Modify: `src/composables/useCollectorApi.ts:448-465`
- Modify: `src/composables/useCollectorApi.ts:634-646`
- Modify: `src/components/crawler/CollectionShopView.vue:1-22`
- Modify: `src/components/crawler/CollectionShopView.vue:404-444`
- Modify: `src/components/crawler/CollectionShopView.vue:488-506`

**Interfaces:**
- Produces: `updateScheduleStatuses(scheduleIds: number[], enabled: boolean)`
- Consumes response keys: `updatedIds`, `failedIds`, `updatedCount`, `enabled`

- [ ] **Step 1: Add the typed API method**

```typescript
async function updateScheduleStatuses(scheduleIds: number[], enabled: boolean) {
  const response = await apiClient.put<{
    updatedIds: number[]
    failedIds: number[]
    updatedCount: number
    enabled: boolean
  }>('/crawler/schedules/status', { scheduleIds, enabled })
  return response.data
}
```

Export `updateScheduleStatuses` next to `deleteSchedules`.

- [ ] **Step 2: Add view state and the shared action**

Import `CircleCheck` and `CircleClose`. Add:

```typescript
const batchStatusAction = shallowRef<'enable' | 'disable' | ''>('')

async function updateSelectedScheduleStatus(enabled: boolean) {
  if (selectedSchedules.value.length < 1) {
    ElMessage.warning(`请选择要${enabled ? '启用' : '停用'}的采集店铺`)
    return
  }
  const action = enabled ? '启用' : '停用'
  try {
    await ElMessageBox.confirm(
      `确认批量${action}选中的 ${selectedSchedules.value.length} 条采集店铺？${
        enabled ? '' : '已开始执行的采集任务不会被终止。'
      }`,
      `批量${action}`,
      {
        confirmButtonText: action,
        cancelButtonText: '取消',
        type: 'warning',
      },
    )
    batchStatusAction.value = enabled ? 'enable' : 'disable'
    const result = await api.updateScheduleStatuses(
      selectedSchedules.value.map((item) => item.id),
      enabled,
    )
    clearSelection()
    await loadSchedules()
    if (result.failedIds.length > 0) {
      ElMessage.warning(
        `已${action} ${result.updatedCount} 条，${result.failedIds.length} 条处理失败`,
      )
    } else {
      ElMessage.success(`已${action} ${result.updatedCount} 条采集店铺`)
    }
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error(
        toApiErrorMessage(error, `批量${action}采集店铺失败`),
      )
    }
  } finally {
    batchStatusAction.value = ''
  }
}
```

- [ ] **Step 3: Add the two toolbar buttons**

Place immediately before batch delete:

```vue
<el-button
  type="success"
  :icon="CircleCheck"
  :disabled="selectedSchedules.length < 1"
  :loading="batchStatusAction === 'enable'"
  @click="updateSelectedScheduleStatus(true)"
>
  批量启用
</el-button>
<el-button
  type="warning"
  :icon="CircleClose"
  :disabled="selectedSchedules.length < 1"
  :loading="batchStatusAction === 'disable'"
  @click="updateSelectedScheduleStatus(false)"
>
  批量停用
</el-button>
```

Keep the existing batch delete button directly after them.

- [ ] **Step 4: Run the frontend production build**

```powershell
npm run build
```

Expected: `vue-tsc --noEmit` and Vite build complete successfully. Existing non-blocking Rolldown or chunk-size warnings may remain.

- [ ] **Step 5: Commit the frontend feature**

```powershell
git add src/composables/useCollectorApi.ts src/components/crawler/CollectionShopView.vue
git commit -m "feat: batch toggle collection shops"
```

### Task 4: Run Final Static Verification

**Files:**
- Verify all files changed in Tasks 1-3.

**Interfaces:**
- No new interfaces.

- [ ] **Step 1: Run all backend tests**

```powershell
python -m unittest discover -s tests -v
```

Expected: all backend tests pass.

- [ ] **Step 2: Run backend compile and diff checks**

```powershell
python -m compileall app
git diff --check HEAD~2
```

Expected: no syntax or whitespace errors.

- [ ] **Step 3: Run frontend build and diff checks**

```powershell
npm run build
git diff --check HEAD~1
```

Expected: build succeeds and no whitespace errors are reported.

- [ ] **Step 4: Confirm no application process was started**

Review executed commands and verify that no `npm run dev`, `vite`, `uvicorn`, `python main.py`, or worker startup command was run.
