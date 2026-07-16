# AI 商品销量分析设计

## 目标

在当前采集系统的“AI 管理”下新增“商品分析”，允许用户使用自然语言查询自己店铺的商品销量、销售额、趋势、排行、退款退货和 SKU 表现。

系统通过乐天订单 API 增量同步订单及商品明细到本地数据仓库。AI 只负责理解问题、选择受控分析工具并解释结果，不直接访问数据库、不生成或执行任意 SQL，也不接触店铺密钥。

## 核心约束

- 每个用户只能同步、查看和分析 `owner_username` 属于自己的店铺。
- 超级管理员不因角色自动获得其他用户店铺的商品分析权限。
- 所有店铺权限必须由后端通过 `owner_username + store_id` 校验，不能依赖前端隐藏。
- 店铺密钥只在后端调用乐天 API 时解密，不能发送给 AI 模型或前端。
- 默认销量口径为：`有效销量 = 已下单商品数量 - 已取消数量 - 已退款数量 - 已退货数量`。
- 退款和退货优先按订单商品明细扣减；无法定位到商品的部分退款不得猜测或按金额自行分摊。
- 所有分析结果必须显示店铺、统计时间、销量口径和数据更新时间。
- AI 只能调用预定义的只读分析工具，不能直接生成 SQL。

## 总体架构

数据流分为四层：

1. 乐天订单同步层通过 `searchOrder` 和 `getOrder(version=7)` 获取订单及商品明细。
2. 本地事实层保存订单最新快照、商品明细和数量调整记录。
3. 分析层统一计算有效销量、销售额、趋势和排行。
4. AI 编排层将自然语言问题转换为受控工具参数，并基于工具结果生成回答。

商品分析查询默认读取本地数据。用户点击“立即更新”时，只增量同步当前用户选中的店铺，不在每次对话时遍历乐天历史订单。

## 订单同步

### 数据来源

- `searchOrder`：按时间范围、订单状态和分页条件获取订单号。
- `getOrder(version=7)`：批量获取订单、包裹、商品和 SKU 明细。

关键商品字段包括：

- `orderNumber`
- `orderProgress`
- `manageNumber`
- `itemNumber`
- `itemId`
- `itemDetailId`
- `itemName`
- `SkuModelList`
- `units`
- `price`
- `priceTaxIncl`
- `deleteItemFlag`
- `restoreInventoryFlag`

### 同步范围

- 首次同步默认拉取最近 90 天。
- 自动增量同步默认每 30 分钟执行一次。
- 最近 7 天订单每次增量同步都重新检查。
- 最近 30 天内未完成、退款中、退货中或状态异常的订单持续检查。
- 已完成订单每天复查一次，最多持续 90 天。
- “立即更新”只同步当前用户选择的店铺，并返回同步进度和结果。

### 幂等与更新

- 订单以 `store_id + order_number` 唯一。
- 商品明细优先以 `store_id + order_number + item_detail_id` 唯一。
- 若 `item_detail_id` 缺失，使用稳定的商品行指纹作为后备键，指纹由商品编号、SKU、价格和包裹位置组成。
- 每次同步保存最新订单快照，并比较旧快照生成数量和状态变化记录。
- 重复同步同一版本订单不得重复增加或扣减销量。

## 数据模型

### `lt_sales_orders`

保存订单级最新状态：

- `id`
- `owner_username`
- `store_id`
- `order_number`
- `order_progress`
- `order_status`
- `ordered_at`
- `updated_at_remote`
- `total_amount`
- `currency`
- `is_canceled`
- `has_unresolved_adjustment`
- `raw_order_json`
- `last_synced_at`
- `created_at`
- `updated_at`

唯一约束：`store_id + order_number`。

### `lt_sales_order_items`

保存订单商品事实：

- `id`
- `owner_username`
- `store_id`
- `sales_order_id`
- `order_number`
- `item_detail_id`
- `manage_number`
- `item_number`
- `item_id`
- `sku_key`
- `sku_json`
- `item_name`
- `unit_price`
- `ordered_units`
- `latest_units`
- `canceled_units`
- `refunded_units`
- `returned_units`
- `effective_units`
- `effective_amount`
- `delete_item_flag`
- `restore_inventory_flag`
- `ordered_at`
- `created_at`
- `updated_at`

商品长期归并键优先使用 `store_id + manage_number`；SKU 归并键使用 `store_id + manage_number + sku_key`。

### `lt_sales_item_adjustments`

保存商品数量变化和未决调整：

- `id`
- `owner_username`
- `store_id`
- `sales_order_item_id`
- `adjustment_type`
- `units`
- `amount`
- `source`
- `status`
- `reason`
- `remote_updated_at`
- `raw_payload_json`
- `created_at`
- `updated_at`

`adjustment_type` 支持 `cancel`、`refund`、`return` 和 `manual_correction`。`status` 支持 `confirmed`、`unresolved` 和 `reverted`。

同一件商品的退货退款只记为一次有效扣减，不能同时重复计入退款数量和退货数量。

### `lt_product_sales_daily`

保存按日、商品和 SKU 聚合的查询数据：

- `owner_username`
- `store_id`
- `sales_date`
- `manage_number`
- `item_number`
- `sku_key`
- `item_name_snapshot`
- `order_count`
- `ordered_units`
- `canceled_units`
- `refunded_units`
- `returned_units`
- `effective_units`
- `gross_sales_amount`
- `effective_sales_amount`
- `updated_at`

唯一约束：`store_id + sales_date + manage_number + sku_key`。

### `lt_sales_sync_states`

保存每家店铺的同步游标和运行状态：

- `owner_username`
- `store_id`
- `initial_sync_completed`
- `last_successful_sync_at`
- `last_remote_updated_at`
- `sync_status`
- `progress_current`
- `progress_total`
- `last_error`
- `created_at`
- `updated_at`

### AI 会话

使用独立表保存商品分析会话和消息：

- `lt_sales_analysis_conversations`
- `lt_sales_analysis_messages`

消息保存用户问题、AI回答、工具名称、脱敏后的工具参数、结果摘要、模型名称、店铺范围和统计时间。不得保存店铺密钥或完整客户资料。

## 有效销量计算

每条商品明细统一维护：

```text
effective_units =
    max(
        0,
        ordered_units
        - canceled_units
        - refunded_units
        - returned_units
    )
```

并满足：

```text
canceled_units + refunded_units + returned_units <= ordered_units
```

计算规则：

- 整单取消：订单内所有商品的 `canceled_units = ordered_units`。
- 商品行删除或取消：只扣除对应商品行。
- 数量从高值修改为低值：差额计入取消数量。
- 明确的部分退款：按对应商品及数量计入 `refunded_units`。
- 明确的退货：按对应商品及数量计入 `returned_units`。
- 退货退款：优先归为退货，不再次计入退款。
- 整单退款但没有商品级明细：全部商品按原数量扣减。
- 部分退款无法定位商品：创建 `unresolved` 调整，不修改具体商品有效销量，并在分析结果中提示待确认订单数和金额。

若后续同步证明调整已撤销，记录反向状态并重算对应日期汇总。

## 后端分析工具

AI 可调用以下固定只读工具：

1. `list_owned_stores`
   - 返回当前用户可分析的店铺。

2. `get_store_sales_overview`
   - 参数：店铺、时间范围、对比周期。
   - 返回：订单数、有效销量、有效销售额、退款退货数量和环比。

3. `get_product_sales_ranking`
   - 参数：店铺、时间范围、排序指标、数量、是否包含 SKU。
   - 返回：商品销量和销售额排行。

4. `get_product_sales_trend`
   - 参数：店铺、商品、时间范围、粒度。
   - 返回：按日、周或月的有效销量和销售额。

5. `compare_product_sales`
   - 参数：店铺、商品列表、时间范围。
   - 返回：多个商品的销量、销售额、退款率和趋势对比。

6. `get_sku_sales_breakdown`
   - 参数：店铺、商品、时间范围。
   - 返回：SKU 级销量、销售额和占比。

7. `get_slow_moving_products`
   - 参数：店铺、观察周期、最低上架天数和销量阈值。
   - 返回：低销量或零销量商品。

8. `get_sales_adjustment_summary`
   - 参数：店铺、时间范围。
   - 返回：取消、退款、退货和待确认调整。

所有工具在执行前必须根据当前登录用户重新校验店铺归属。时间范围、分页数量和返回记录数设置上限，防止模型生成超大查询。

## AI 编排

- 复用现有用户级 AI 配置、API Key、模型名称和连接验证。
- AI 系统提示词明确销量口径、工具使用规则和数据限制。
- 模型先选择工具，后端校验参数后执行，再把结构化结果交给模型解释。
- 模型不得自行补充工具结果中不存在的销量、金额或退款信息。
- 问题缺少店铺时：
  - 用户只有一家店铺则自动选择。
  - 用户有多家店铺则要求用户选择，不能自动合并。
- “近期”默认解释为最近 30 个完整自然日，并在回答中显示具体日期。
- “销量”默认指有效销量；“销售额”默认指有效销售额。
- 涉及无法定位商品的部分退款时，回答必须显示未决调整提示。

## API

建议新增：

- `GET /crawler/sales-analysis/stores`
- `GET /crawler/sales-analysis/sync-state`
- `POST /crawler/sales-analysis/sync`
- `GET /crawler/sales-analysis/sync/{task_id}`
- `GET /crawler/sales-analysis/conversations`
- `POST /crawler/sales-analysis/conversations`
- `DELETE /crawler/sales-analysis/conversations/{conversation_id}`
- `GET /crawler/sales-analysis/conversations/{conversation_id}/messages`
- `POST /crawler/sales-analysis/conversations/{conversation_id}/messages`

消息接口使用流式响应，事件类型包括：

- `status`
- `tool_call`
- `tool_result`
- `delta`
- `completed`
- `error`

前端只展示工具的业务名称和查询条件，不展示内部 SQL、密钥或模型提示词。

## 前端

在“AI 管理”下新增：

```text
AI 管理
├─ 标题优化
└─ 商品分析
```

路由：`/ai/product-analysis`，权限沿用 `ai.manage`，店铺数据权限由后端单独校验。

页面采用分析工作台：

- 左侧：历史会话列表、新建会话和删除会话。
- 顶部：店铺选择、数据更新时间、同步状态和“立即更新”。
- 中间：用户问题、AI回答、指标摘要、趋势图和明细表。
- 底部：问题输入框和发送按钮。

回答结果必须固定展示：

- 店铺名称
- 起止日期
- 有效销量口径
- 数据最后更新时间
- 未决退款或退货提示

第一版提供快捷问题：

- 最近 30 天销量最高的 10 个商品
- 本月销量和上月相比怎么样
- 最近 30 天没有销量的上架商品
- 哪些商品退款退货最多
- 查看某个商品最近 90 天趋势

## 错误与降级

- 店铺密钥无效：同步失败并提示用户检查店铺配置。
- 乐天 API 限流或网络失败：保留上次成功数据，显示数据时间和失败原因。
- 初始同步未完成：允许查看已同步数据，但明确标记结果不完整。
- AI 调用失败：保留工具查询结果，允许前端显示基础表格并重新生成解释。
- 用户请求其他用户店铺：后端返回无权限，不泄露店铺是否存在。
- 部分退款无法归属商品：生成未决调整，不擅自修改商品销量。
- 汇总表异常：允许从订单商品事实表重建指定店铺和日期范围的每日汇总。

## 安全与审计

- 所有销售查询必须包含当前登录用户。
- AI消息上下文不包含买家姓名、地址、电话、邮箱等客户资料。
- 工具结果只返回分析需要的商品、数量、金额和时间数据。
- 记录同步任务、AI工具调用、调用用户、店铺、时间范围和耗时。
- API Key、Service Secret 和 License Key 继续加密保存并仅在后端短暂解密。
- 对同步接口设置店铺级互斥锁，避免重复同步同一家店铺。

## 验证

后端测试覆盖：

- 用户不能同步或分析其他用户店铺。
- 重复同步不重复累计销量。
- 整单取消、商品删除、数量减少、退款和退货的扣减。
- 退货退款不重复扣减。
- 无法归属商品的部分退款进入未决状态。
- 每日汇总与订单商品事实表一致。
- AI只能调用允许的工具且参数经过校验。
- “近期”和“销量”的默认解释一致。

前端验证覆盖：

- “商品分析”导航和路由权限。
- 多店铺选择、单店铺自动选择。
- 同步进度、失败和数据过期提示。
- 流式回答、工具状态、表格和趋势数据渲染。
- 历史会话创建、切换和删除。

验证只运行测试、类型检查和生产构建；不启动新的前端或后端进程，不打开界面检查。

## 实施边界

第一版实现：

- 最近 90 天订单同步和增量更新。
- 商品及 SKU 有效销量。
- 店铺概览、排行、趋势、对比、滞销和调整汇总。
- AI自然语言查询和历史会话。
- 手动立即更新及自动定时同步入口。

第一版不实现：

- 跨用户或跨店铺自动合并分析。
- AI自由 SQL。
- 无商品归属依据时按退款金额推测退款商品。
- 预测未来销量、自动补货或自动调整价格。
- 导出大型 BI 报表。
