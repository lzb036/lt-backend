# 店铺商品内容替换设计

## 目标

允许用户在店铺商品中输入一个乐天商品链接，采集来源商品并生成替换草稿，通过“替换前 / 替换后”对比和显式确认后，将来源商品内容覆盖到目标店铺商品，同时保留目标商品管理编号、商品 URL、店铺归属、本地商品 ID 和上架状态。

## 核心约束

- 采集和编辑阶段不得修改目标商品。
- 来源采集结果不进入普通待审核商品列表。
- 只有点击“确认替换”并通过管理编号挑战确认后才执行远端更新。
- SKU 和库存使用现有正常上架载荷生成规则，不强制新 SKU 库存为零。
- 先更新并验证乐天，再更新本地商品。
- 替换任务持久化保存目标快照、来源结果、编辑草稿、差异和错误。
- 图片上传或乐天更新失败时，本地目标商品保持不变。

## 持久化

复用 `lt_sync_tasks`：

- `task_type = product_replace`
- `status = preview_ready` 表示采集完成、等待确认。
- `payload_json` 保存 `targetProductId`、`sourceUrl`、`targetSnapshot`、`sourcePayload`、`draftPayload`、`difference`、`confirmManageNumber` 和执行结果。
- 确认后将状态改为 `queued`，由 sync worker 执行。

## API

- `POST /crawler/store-products/{product_id}/replacement`
- `GET /crawler/product-replacements/{task_id}`
- `PUT /crawler/product-replacements/{task_id}/draft`
- `POST /crawler/product-replacements/{task_id}/confirm`
- `POST /crawler/product-replacements/{task_id}/cancel`

## 前端

店铺商品行新增“替换商品”操作，打开独立对话框：

1. 输入来源链接并采集。
2. 显示替换前、替换后两个 Tab。
3. 替换后支持编辑标题、副标题、品类、SKU 价格、图片 URL 和详情说明。
4. 变化字段显示“已变更”，并展示差异摘要。
5. 确认时输入目标商品管理编号。
6. 替换任务运行期间轮询状态；成功后刷新店铺商品列表。

## 失败保护

- 目标商品存在进行中的同步、删除、上架或替换任务时拒绝确认。
- 更新乐天前保存最新目标快照。
- 新图片上传失败时清理本次上传内容。
- 乐天完整更新失败时不修改本地数据库。
- 远端写入后发生异常时记录恢复所需的目标快照和已上传图片，不自动删除可能正在被远端引用的图片。

## 验证

- 后端单元测试覆盖采集草稿、保留字段、差异计算、显式确认、任务锁和失败不改本地。
- 前端运行类型检查和生产构建。
- 不启动本地前后端，不打开界面检查。
