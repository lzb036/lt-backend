# Capacity-Aware Crawl Dispatch Design

## Goal

Increase scheduled-crawl throughput on the production `4 core / 8 GB` server
without losing tasks, creating duplicate crawls, or allowing a large backlog to
generate repeated Redis queue scans.

The initial production target is three concurrently running crawl tasks for one
user. After the change, the Redis crawl queue must contain only work that can
occupy an available crawl slot; MySQL remains the source of truth for the full
backlog.

## Current Problem

At `2026-07-11 00:44 CST`, all `1295` scheduled crawls belonged to
`superadmin`, while `LT_MAX_RUNNING_CRAWL_TASKS_PER_USER=1`. Six crawl workers
were present, but only one database task was running. The other five workers
repeatedly:

1. selected a queued RQ job;
2. scanned the active Redis queue while reconciling running tasks;
3. found the per-user limit occupied;
4. scheduled another copy of the task five seconds later; and
5. exited without performing a crawl.

This left `1192` database tasks queued, consumed Redis and MySQL capacity, and
limited actual throughput to about `21.5` tasks per hour.

## Constraints

- Do not start duplicate frontend or backend application processes.
- Existing managed backend and Supervisor workers may be restarted through
  their current service managers during deployment.
- MySQL must retain every queued crawl task during deployment.
- Manual and scheduled crawl tasks use the same dispatcher and concurrency
  rules.
- Existing task status values and frontend filters remain compatible.
- The current crawler request delay, retry count, batch size, and batch pause
  remain unchanged.
- Production starts with
  `LT_MAX_RUNNING_CRAWL_TASKS_PER_USER=3` and three crawl workers.
- A worker or Redis interruption must leave a task recoverable from MySQL.

## Architecture

Add a nullable `queue_job_id` column to `lt_crawl_tasks`. It records that a
queued database task has reserved one Redis execution slot.

The dispatcher uses a short Redis distributed lock so backend, schedule, and
crawl worker processes cannot reserve slots concurrently. While holding the
lock, it:

1. finds users with queued crawl tasks;
2. counts each user's running tasks and queued tasks with a reserved
   `queue_job_id`;
3. calculates available capacity from
   `LT_MAX_RUNNING_CRAWL_TASKS_PER_USER`;
4. reserves the oldest unreserved tasks by assigning unique RQ job IDs; and
5. enqueues only those reserved tasks after the database reservation commits.

If enqueueing fails, the matching reservation is cleared so periodic recovery
can try again. A process crash between reservation and enqueue is handled by
the existing queued-task recovery pass.

## Worker Behavior

The dispatcher passes the reserved job ID into `run_task()`. A worker may clear
`queue_job_id` and change the task from `queued` to `running` only when that
argument exactly matches the persisted reservation. A stale or duplicate RQ
job exits without clearing a newer reservation.

The per-user running-count check remains as a final safety guard. In Redis mode,
a rejected task stays queued without creating a five-second retry job, and the
dispatcher submits it after a real slot becomes available. Legacy thread mode
retains its existing delayed in-process retry.

After every success, partial result, failure, cancellation, or rejected start,
the worker asks the dispatcher to fill newly available capacity. Dispatch
errors are contained so they cannot overwrite the completed task result.

## Recovery

Periodic maintenance continues to reconcile interrupted running jobs. For
queued crawl tasks, recovery examines only rows with a non-null `queue_job_id`;
unreserved queued rows are expected backlog and are not treated as missing RQ
jobs.

When a reserved RQ job is missing or failed, recovery clears its
`queue_job_id`. The capacity dispatcher then reserves and enqueues the next
eligible task. Periodic maintenance also invokes the dispatcher, allowing the
queue to recover after backend, worker, or Redis restarts.

## Production Migration

Deployment must not combine the old full-backlog RQ queue with the new
capacity-aware dispatcher.

1. Pull and install the verified backend revision without starting a second
   application instance.
2. Update the managed crawl Supervisor configuration with a long graceful-stop
   timeout, parent-only `TERM` handling, and `numprocs=3`.
3. Gracefully stop the current crawl workers and wait for active workhorses to
   exit.
4. Confirm no crawl worker remains active.
5. Remove only active crawl jobs from the Redis crawl queue and its
   started/scheduled/deferred registries. Do not remove MySQL task rows or RQ
   failure/finished history.
6. Reset any deployment-interrupted crawl row to `queued`, clear its
   `queue_job_id`, and preserve already saved products; deduplication makes a
   rerun safe.
7. Restart the managed backend first so the schema compatibility step adds
   `queue_job_id`.
8. Set `LT_MAX_RUNNING_CRAWL_TASKS_PER_USER=3`.
9. Start the three managed crawl workers and invoke one dispatcher pass.
10. Verify that MySQL shows at most three running tasks for one user and Redis
    contains no more than three reserved/started crawl jobs for that user.

## Verification

- Unit tests cover slot calculation, oldest-first reservation, distributed
  dispatch serialization, enqueue failure rollback, rejected-start behavior,
  completion refill, and missing-job recovery.
- Regression tests prove that a task rejected by the concurrency guard is not
  scheduled again after five seconds.
- Backend verification runs without starting the application:
  `python -m unittest discover -s tests -v`,
  `python -m compileall app scripts tests`, and `git diff --check`.
- Production verification checks database status counts, Redis queued/started/
  scheduled counts, active worker processes, server load, memory availability,
  completed-task growth, and backend health.
- Throughput is measured for at least ten newly completed scheduled crawls.
  The expected initial improvement is approximately three times the previous
  single-task rate, subject to shop size and Rakuten response latency.
