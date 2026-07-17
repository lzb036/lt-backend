# 商品分析严格 AI 与用户设置实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** AI 服务异常或回答校验失败时直接返回错误，并新增用户可编辑个人业务偏好、只读查看系统安全约束的“商品分析设置”页面。

**Architecture:** 后端新增用户级设置模型与独立服务，通过受控 API 暴露设置、能力说明和安全约束；AI 编排读取个人偏好但保持系统提示词和工具硬限制优先。销量分析消息增加完成/失败状态，严格失败路径只发送 `error`。前端新增三标签设置页，并让商品分析页面按设置生成默认问题和展示失败历史。

**Tech Stack:** FastAPI、SQLAlchemy、Pydantic、Pytest、Vue 3、TypeScript、Element Plus、现有无测试框架 TypeScript 契约测试。

## Global Constraints

- 只有点击“保存”按钮才持久化个人设置。
- 用户只能读取和修改自己的设置。
- 系统安全约束只读，不提供修改 API。
- AI 服务异常或回答未通过事实校验时不返回兜底答案。
- 店铺归属、密钥保护、SQL 禁止、工具白名单和硬上限不能被用户设置覆盖。
- 不启动本地前端、后端或浏览器；仅运行测试、类型检查和生产构建。

---

### Task 1: 严格失败与失败消息状态

**Files:**
- Modify: `app/db/models.py`
- Modify: `app/db/database.py`
- Modify: `app/services/sales_ai_service.py`
- Test: `tests/test_sales_ai_service.py`
- Test: `tests/test_sales_models.py`

**Interfaces:**
- Produces: `SalesAnalysisMessageModel.status`, `error_code`, `error_message`
- Produces: `_fail_message(..., error_code: str, error_message: str) -> dict[str, Any]`
- Changes: `stream_analysis()` 的异常路径只产生 `error`

- [ ] **Step 1: 写失败测试**

覆盖：

```python
def test_explanation_failure_after_tool_success_emits_error_without_fallback():
    events = list(stream_analysis(...))
    assert [event["type"] for event in events] == [
        "status", "tool_call", "tool_result", "error"
    ]
    assert all(event["type"] != "completed" for event in events)
    assert persisted.status == "error"
    assert persisted.error_code == "ai_service_error"
    assert persisted.answer == ""
```

以及模型编造数字、未支持定性结论、工具执行失败和旧表兼容字段测试。

- [ ] **Step 2: 运行定向测试确认失败**

Run:

```powershell
python -m pytest tests/test_sales_ai_service.py tests/test_sales_models.py -q
```

Expected: 新增断言因现有 `fallback=true` 完成消息和缺失状态字段而失败。

- [ ] **Step 3: 实现消息字段与兼容 DDL**

新增：

```python
status: Mapped[str] = mapped_column(String(32), nullable=False, default="completed")
error_code: Mapped[str] = mapped_column(String(64), nullable=False, default="")
error_message: Mapped[str] = mapped_column(Text, nullable=False, default="")
```

通过现有 `_ensure_table_layout()` 自动补列；历史记录默认 `completed`。

- [ ] **Step 4: 实现严格失败**

删除新请求中的 `_fallback_answer()` 调用。模型异常、最终回答校验失败和工具失败统一调用 `_fail_message()`，清空 `answer`，保存脱敏错误并发送单个 `error` 事件。

- [ ] **Step 5: 运行测试并提交**

Run:

```powershell
python -m pytest tests/test_sales_ai_service.py tests/test_sales_models.py -q
python -m compileall -q app tests
git diff --check
```

Commit:

```powershell
git commit -am "fix: make sales analysis AI failures explicit"
```

---

### Task 2: 用户级商品分析设置后端

**Files:**
- Modify: `app/db/models.py`
- Modify: `app/db/database.py`
- Create: `app/services/sales_analysis_settings_service.py`
- Modify: `app/api/crawler.py`
- Test: `tests/test_sales_analysis_settings.py`
- Test: `tests/test_sales_analysis_api.py`

**Interfaces:**
- Produces: `UserSalesAnalysisSettingsModel`
- Produces: `get_settings(owner_username)`, `update_settings(owner_username, payload)`
- Produces: `capability_catalog()`, `constraint_catalog()`
- Produces API:
  - `GET /crawler/settings/sales-analysis`
  - `PUT /crawler/settings/sales-analysis`
  - `GET /crawler/settings/sales-analysis/capabilities`
  - `GET /crawler/settings/sales-analysis/constraints`

- [ ] **Step 1: 写设置和权限失败测试**

验证默认值、字段边界、按用户隔离、后端忽略前端用户名、能力目录和约束目录只读。

示例：

```python
def test_user_sales_analysis_settings_are_owner_scoped():
    update_settings("alice", payload(defaultRankingLimit=20))
    assert get_settings("alice")["defaultRankingLimit"] == 20
    assert get_settings("bob")["defaultRankingLimit"] == 10
```

- [ ] **Step 2: 运行定向测试确认失败**

Run:

```powershell
python -m pytest tests/test_sales_analysis_settings.py tests/test_sales_analysis_api.py -q
```

Expected: 模型、服务和 API 尚不存在。

- [ ] **Step 3: 实现模型和服务**

设置模型字段严格采用设计文档定义。服务规范化枚举、布尔值、5 至 100 排行数量和 4000 字自定义要求，并返回 camelCase 公共结构。

- [ ] **Step 4: 实现 API**

新增 Pydantic payload，所有接口使用 `require_ai_permission`，所有者只取 `user["username"]`。

- [ ] **Step 5: 运行测试并提交**

Run:

```powershell
python -m pytest tests/test_sales_analysis_settings.py tests/test_sales_analysis_api.py -q
python -m compileall -q app tests
git diff --check
```

Commit:

```powershell
git add app tests
git commit -m "feat: add user sales analysis settings API"
```

---

### Task 3: 个人偏好注入 AI 编排

**Files:**
- Modify: `app/services/sales_ai_service.py`
- Modify: `app/services/sales_analysis_service.py`
- Modify: `app/services/sales_analysis_settings_service.py`
- Test: `tests/test_sales_ai_service.py`
- Test: `tests/test_sales_analysis_service.py`

**Interfaces:**
- Consumes: `get_settings(owner_username)`
- Produces: `_user_preference_prompt(settings) -> str`
- Produces: 默认时间窗口、排行数量、指标和粒度解析

- [ ] **Step 1: 写偏好优先级失败测试**

验证：

```text
系统硬规则 > 用户设置
当前问题明确参数 > 用户默认参数
用户默认参数 > 系统普通默认值
```

并验证自定义要求中的 SQL、跨店铺、密钥请求不能改变工具执行范围。

- [ ] **Step 2: 运行测试确认失败**

Run:

```powershell
python -m pytest tests/test_sales_ai_service.py tests/test_sales_analysis_service.py -q
```

- [ ] **Step 3: 实现最小偏好注入**

把结构化偏好转为受限模型上下文；默认周期用于“近期”等模糊表达，默认排行数量和趋势粒度只在问题未明确指定时生效。自定义要求经过现有脱敏和长度限制后放在固定安全提示词之后。

- [ ] **Step 4: 运行测试并提交**

Run:

```powershell
python -m pytest tests/test_sales_ai_service.py tests/test_sales_analysis_service.py -q
git diff --check
```

Commit:

```powershell
git commit -am "feat: apply user sales analysis preferences"
```

---

### Task 4: 前端设置 API 与类型

**Files:**
- Modify: `src/types/crawler.ts`
- Modify: `src/composables/useCollectorApi.ts`
- Modify: `src/composables/useCollectorApi.test.ts`

**Interfaces:**
- Produces: `SalesAnalysisSettings`, `SalesAnalysisSettingsPayload`
- Produces: `SalesAnalysisCapability`, `SalesAnalysisConstraintSection`
- Produces API client methods for four settings endpoints

- [ ] **Step 1: 写契约失败断言**

要求客户端包含准确路径、HTTP 方法和类型字段。

- [ ] **Step 2: 运行测试确认失败**

Run:

```powershell
npx tsx src\composables\useCollectorApi.test.ts
```

- [ ] **Step 3: 实现类型和 API 方法**

沿用现有 `requestJson`、错误映射和 camelCase 类型。

- [ ] **Step 4: 运行测试并提交**

Run:

```powershell
npx tsx src\composables\useCollectorApi.test.ts
git diff --check
```

Commit:

```powershell
git add src
git commit -m "feat: expose sales analysis settings client"
```

---

### Task 5: 商品分析设置页面

**Files:**
- Create: `src/components/crawler/ProductSalesAnalysisSettingsView.vue`
- Modify: `src/components/crawler/AppShell.vue`
- Modify: `src/router.ts`
- Modify: `src/utils/permissions.ts`
- Modify: `src/utils/permissions.test.ts`
- Modify: `src/composables/useCollectorApi.test.ts`

**Interfaces:**
- Consumes: Task 4 API client
- Produces route: `/ai/product-analysis-settings`

- [ ] **Step 1: 写菜单、路由、权限和显式保存失败测试**

契约测试检查三个标签名称、保存按钮、恢复默认草稿行为、未保存离开保护和 `ai.manage`。

- [ ] **Step 2: 运行测试确认失败**

Run:

```powershell
npx tsx src\utils\permissions.test.ts
npx tsx src\composables\useCollectorApi.test.ts
```

- [ ] **Step 3: 实现三标签页面**

使用 Element Plus `el-tabs`、表单控件、开关、分段选择和只读说明区。能力与安全约束从后端读取；个人设置使用独立草稿，只在保存时调用 PUT。

- [ ] **Step 4: 实现导航离开确认**

比较草稿和最近保存快照；浏览器卸载和 Vue 路由离开时仅在有未保存修改时确认。

- [ ] **Step 5: 运行测试和构建并提交**

Run:

```powershell
npx tsx src\utils\permissions.test.ts
npx tsx src\composables\useCollectorApi.test.ts
npm run build
git diff --check
```

Commit:

```powershell
git add src
git commit -m "feat: add sales analysis settings workspace"
```

---

### Task 6: 商品分析页面严格错误与偏好联动

**Files:**
- Modify: `src/components/crawler/ProductSalesAnalysisView.vue`
- Modify: `src/composables/salesAnalysisHelpers.ts`
- Modify: `src/types/crawler.ts`
- Modify: `src/composables/useCollectorApi.test.ts`

**Interfaces:**
- Consumes: `SalesAnalysisSettings`
- Changes: 历史消息支持 `status=error`
- Changes: 当前流错误不保留工具结果为成功答案

- [ ] **Step 1: 写失败测试**

验证错误事件不会渲染完成答案；失败历史显示错误信息和重新提问；快捷问题按默认周期和排行数量生成。

- [ ] **Step 2: 运行测试确认失败**

Run:

```powershell
npx tsx src\composables\useCollectorApi.test.ts
```

- [ ] **Step 3: 实现严格错误 UI**

流收到 `error` 后清除当前答案和结果展示，只保留问题与错误状态。历史错误消息使用警告样式并提供重新填入输入框的操作。

- [ ] **Step 4: 实现偏好联动**

加载个人设置并生成快捷问题默认值；页面固定数据不完整、未归属调整和安全提示不受用户开关影响。

- [ ] **Step 5: 运行测试和构建并提交**

Run:

```powershell
npx tsx src\composables\useCollectorApi.test.ts
npm run build
git diff --check
```

Commit:

```powershell
git add src
git commit -m "fix: surface strict sales analysis AI errors"
```

---

### Task 7: 全量验证与发布前审查

**Files:**
- Review all changed backend and frontend files

- [ ] **Step 1: 后端全量验证**

Run:

```powershell
python -m pytest
python -m compileall -q app tests
git diff --check
git status --short
```

Expected: 全部测试通过，工作区干净。

- [ ] **Step 2: 前端全量验证**

Run:

```powershell
npx tsx src\composables\useCollectorApi.test.ts
npx tsx src\utils\permissions.test.ts
npx tsx src\utils\operatorManual.test.ts
npx tsx src\utils\productGenre.test.ts
npm run build
git diff --check
git status --short
```

Expected: 契约测试和构建通过；只允许既有第三方构建警告。

- [ ] **Step 3: 差异审查**

检查越权访问、提示词覆盖安全规则、失败路径误发 `completed`、隐私泄漏、显式保存语义和旧数据兼容。

- [ ] **Step 4: 最终提交**

若审查产生修复，使用范围明确的 `fix:` 提交；最终两个仓库保持干净。
