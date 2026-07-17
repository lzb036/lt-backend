# 销量订单同步历史与全局设置实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将销量订单同步周期改为超级管理员可配置，并新增独立订单获取历史、记录页面、重试和成功记录清理。

**Architecture:** 使用现有系统设置表保存一份全局销量订单同步设置；新增 `lt_sales_order_sync_runs` 保存每次自动、手动和重试执行。现有 `lt_sales_sync_states` 继续保存店铺最新状态，执行服务同时更新最新状态和历史记录。前端在商品分析设置中新增超级管理员标签，并新增独立历史页面。

**Tech Stack:** FastAPI、SQLAlchemy、MySQL/SQLite、RQ、Pytest、Vue 3、TypeScript、Element Plus。

## Global Constraints

- 销量订单同步记录不得写入 `lt_sync_tasks`。
- 全局设置只有超级管理员可以修改，普通 `ai.manage` 用户只能读取。
- 设置默认启用、30 分钟间隔、成功记录保留 30 天。
- 间隔范围 5 至 1440 分钟；保留范围 1 至 365 天。
- GET 默认设置不得写库，只有显式保存才持久化。
- 自动、手动和重试每次执行生成独立历史记录。
- 用户只能查看、重试和删除自己店铺的记录，超级管理员不自动跨用户。
- 成功记录按配置清理，其他状态不自动删除。
- MySQL TEXT/LONGTEXT 不得生成不兼容的数据库默认值。
- 不启动本地前端、后端、浏览器或界面。

---

### Task 1: 全局设置、历史模型和 API

**Files:**
- Modify: `app/db/models.py`
- Modify: `app/db/database.py`
- Create: `app/services/sales_order_sync_history_service.py`
- Modify: `app/api/crawler.py`
- Test: `tests/test_sales_order_sync_history.py`
- Test: `tests/test_sales_analysis_api.py`
- Test: `tests/test_sales_models.py`

**Produces:**
- `SalesOrderSyncRunModel`
- `get_global_settings()`
- `save_global_settings(payload)`
- `list_runs(owner_username, filters...)`
- `delete_runs(owner_username, run_ids)`
- 设置 GET/PUT 与历史 GET/DELETE API

- [ ] 写失败测试：默认设置 GET 不写库、非超级管理员 PUT 403、范围校验、owner 隔离、运行中不可删除、MySQL DDL 无 TEXT 默认值。
- [ ] 运行定向测试并确认因模型、服务和 API 不存在而失败。
- [ ] 实现模型、设置服务、分页查询和删除。
- [ ] 实现 API 与 Pydantic payload，PUT 使用 `require_superadmin`，GET 和历史查询使用 `require_ai_permission`。
- [ ] 运行测试、compileall、diff check 并提交 `feat: add sales order sync history APIs`。

---

### Task 2: 同步执行生命周期和结果计数

**Files:**
- Modify: `app/services/sales_sync_service.py`
- Modify: `app/services/crawler_service.py`
- Modify: `app/services/sales_order_sync_history_service.py`
- Modify: `app/api/crawler.py`
- Test: `tests/test_sales_sync_service.py`
- Test: `tests/test_sales_order_sync_history.py`
- Test: `tests/test_sales_analysis_api.py`

**Produces:**
- 自动、手动、重试记录创建和状态流转
- `retry_run(owner_username, run_id)`
- `cleanup_successful_runs_if_due()`
- 可配置调度间隔和启用状态

- [ ] 写失败测试：自动关闭时不调度、动态间隔生效、手动仍执行、每次执行生成记录、计数和进度更新、同店铺去重、重试新建子记录、成功记录按天清理。
- [ ] 运行测试确认当前固定 30 分钟和无历史行为失败。
- [ ] 扩展 `sync_owned_store(..., run_id=None)` 返回总数、新增、更新、无变化和失败计数，并同步更新 run。
- [ ] 让 `queue_sales_analysis_sync` 接收 `trigger_type` 和可选 `parent_run_id`；自动调度传 `automatic`，商品分析按钮传 `manual`，历史重试传 `retry`。
- [ ] 用全局设置替代固定 `SALES_ANALYSIS_SYNC_INTERVAL`；关闭时跳过自动调度。
- [ ] 接入成功记录清理和遗留运行记录恢复。
- [ ] 运行后端相关测试并提交 `feat: track sales order sync runs`。

---

### Task 3: 前端设置 API、类型和超级管理员标签

**Files:**
- Modify: `src/types/crawler.ts`
- Modify: `src/composables/useCollectorApi.ts`
- Modify: `src/composables/useCollectorApi.test.ts`
- Modify: `src/components/crawler/ProductSalesAnalysisSettingsView.vue`
- Modify: `src/composables/salesAnalysisSettingsState.ts`
- Test: `src/composables/salesAnalysisSettingsState.test.ts`

**Produces:**
- `SalesOrderSyncGlobalSettings`
- `SalesOrderSyncRun`
- 全局设置 GET/PUT 客户端
- “订单同步设置”超级管理员标签

- [ ] 写失败测试：准确路径/方法/解包、普通用户无标签、超级管理员可见、恢复默认仅草稿、保存时才 PUT。
- [ ] 运行契约和状态测试确认失败。
- [ ] 实现类型与 API 客户端。
- [ ] 在设置页增加超级管理员标签，配置开关、间隔和保留天数，保持显式保存和离开确认。
- [ ] 运行测试和生产构建并提交 `feat: add sales order sync global settings`。

---

### Task 4: 订单获取记录页面和导航

**Files:**
- Create: `src/components/crawler/SalesOrderSyncHistoryView.vue`
- Modify: `src/components/crawler/AppShell.vue`
- Modify: `src/router.ts`
- Modify: `src/utils/permissions.ts`
- Modify: `src/utils/permissions.test.ts`
- Modify: `src/composables/useCollectorApi.ts`
- Modify: `src/composables/useCollectorApi.test.ts`
- Modify: `src/components/crawler/ProductSalesAnalysisView.vue`
- Test: relevant helper/contract tests

**Produces:**
- `/ai/order-sync-history`
- 列表、筛选、分页、轮询、重试、批量删除
- 商品分析页当前店铺跳转

- [ ] 写失败测试：导航、路由权限、列表 API、触发/状态文案、筛选参数、重试/删除、当前店铺跳转。
- [ ] 运行测试确认缺失功能失败。
- [ ] 实现服务器分页页面，顶部显示全局设置，只读。
- [ ] 运行中当前页每 2 秒刷新；失败、部分成功、已终止可重试；运行中不可删除。
- [ ] 商品分析页面增加“查看订单获取记录”入口并携带 `storeId`。
- [ ] 运行测试和生产构建并提交 `feat: add sales order sync history workspace`。

---

### Task 5: 全量验证、审查和部署

- [ ] 后端运行 `python -m pytest`、`compileall`、`git diff --check`。
- [ ] 前端运行全部现有 TypeScript 测试和 `npm run build`。
- [ ] 审查权限、重复调度、任务状态竞争、MySQL DDL、显式保存、分页和删除安全。
- [ ] 推送两个 `main`。
- [ ] 服务器记录版本并备份前端 dist。
- [ ] 先拉取后端并完成建表，确认健康后重启所有 Worker。
- [ ] 拉取并构建前端。
- [ ] 验证新表、设置 API 权限、历史 API 权限、新路由、新静态资源、后端健康和 Worker。
