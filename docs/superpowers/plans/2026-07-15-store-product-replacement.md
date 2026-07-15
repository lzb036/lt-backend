# Store Product Replacement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace a listed store product's content from a collected Rakuten product while preserving its identity and requiring preview plus explicit confirmation.

**Architecture:** Persist previews as `SyncTaskModel(task_type="product_replace", status="preview_ready")`. Reuse the existing collector, listing payload builders, R-Cabinet upload functions and sync worker, while keeping the target product unchanged until the remote update succeeds.

**Tech Stack:** FastAPI, SQLAlchemy, RQ sync workers, Vue 3, TypeScript, Element Plus.

## Tasks

- [ ] Add replacement draft normalization and field-difference helpers with failing unit tests.
- [ ] Add preview, read, draft-update, confirm and cancel service functions and API routes.
- [ ] Add `product_replace` execution to `run_sync_task`.
- [ ] Build a replacement-specific full Rakuten update that preserves target identity and uses existing inventory rules.
- [ ] Add frontend types and API methods.
- [ ] Add a focused replacement dialog with source input, before/after tabs, editable draft, difference markers and challenge confirmation.
- [ ] Add the row action only for `listed` products.
- [ ] Run backend tests, Python compilation and frontend build.
- [ ] Commit, push, deploy and verify service/workers/health.
