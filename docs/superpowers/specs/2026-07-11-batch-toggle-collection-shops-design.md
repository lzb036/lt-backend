# Batch Toggle Collection Shops Design

## Goal

Allow an operator to enable or disable multiple selected collection shops from the collection-shop list without editing each schedule individually.

## User Interface

- Add `批量启用` and `批量停用` immediately before the existing `批量删除` button.
- Keep the order `批量启用`, `批量停用`, `批量删除`.
- Use success, warning, and danger button colors to distinguish the three actions.
- Disable both new buttons when no rows are selected.
- Show a confirmation dialog containing the selected row count before submitting.
- After success, clear the current table selection, reload the current page, and report updated and failed counts.

## Behavior

- Batch enable sets `enabled = true`, `status = idle`, and recalculates `next_run_at` from each schedule's existing execution time.
- Batch disable sets `enabled = false`, `status = disabled`, and clears `next_run_at`.
- Disabling a schedule does not cancel or modify a crawl task that has already started.
- Only schedules owned by the authenticated user may be changed.
- Missing or inaccessible schedule IDs are returned as failed IDs instead of affecting other users' schedules.
- An empty or invalid ID list is rejected with a clear validation error.

## API

Add a dedicated batch status endpoint under `/crawler/schedules/status`.

Request:

```json
{
  "scheduleIds": [1, 2, 3],
  "enabled": true
}
```

Response:

```json
{
  "updatedIds": [1, 2],
  "failedIds": [3],
  "updatedCount": 2,
  "enabled": true
}
```

The endpoint uses the existing crawler permission dependency and delegates normalization and updates to `crawler_service`.

## Components

- `app/api/crawler.py`: request schema and API route.
- `app/services/crawler_service.py`: ID normalization, ownership filtering, state updates, and response construction.
- `src/composables/useCollectorApi.ts`: typed API method.
- `src/components/crawler/CollectionShopView.vue`: loading state, confirmation flow, selection handling, buttons, and result messages.

No new Vue component or composable is needed because this is a small extension of the existing collection-shop toolbar and selection workflow.

## Testing

- Service test: enabling selected owned schedules updates state and next execution time.
- Service test: disabling selected owned schedules clears the next execution time.
- Service test: running schedule plans are disabled for future dispatch without modifying existing crawl task rows.
- Service test: duplicate, invalid, missing, and other-owner IDs are normalized or returned as failures.
- Frontend verification: TypeScript compilation and production build pass.
- Backend verification: focused tests, compile check, and diff check pass.

