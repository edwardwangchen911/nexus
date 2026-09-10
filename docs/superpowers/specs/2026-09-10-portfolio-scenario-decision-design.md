# NEXUS 组合调整情景技术设计

**状态：** 目标架构通过；仅阶段 A 进入实施

**评审代码基线：** `873d380f`

**目标：** 基于真实只读组合和冻结诊断，用确定性代码生成一个“当前持仓内降低单名集中度”的分析候选，并在页面展示调整前后差异。首轮不保存候选、不调用 Agent、不选择方案、不启动 Shadow，也不解除执行锁。

## 1. 决策与范围

完整能力按三个独立阶段交付：

| 阶段 | 用户可见结果 | 是否立即实施 |
|---|---|---|
| A：确定性情景闭环 | 用户明确点击后，看到当前组合与一个合法候选的前后比较 | 是 |
| B：Agent 可调用 | Agent 提出研究方向，调用同一确定性引擎并解释结果 | 否，阶段 A 真实验收后另行准入 |
| C：冻结与观察 | 保存约束和候选，人工选择后进入独立 Shadow 准入 | 否，阶段 B 验收后另行设计 |

阶段 A 明确包含：

1. 请求级调整约束合同；
2. 纯确定性候选生成器；
3. 一个 Engine POST 与一个同名 BFF；
4. 组合页的显式“生成调整情景”操作；
5. 单元、接口、前端和一个真实本地只读验收。

阶段 A 明确不包含：

- 数据库迁移、Mandate 版本化或候选持久化；
- Agent 工具、模型调用、Job、自动重试或运行恢复；
- 人工选择、`decision_ready`、Shadow 适配、模拟盘或实盘；
- 新增标的、买入、做空、杠杆、衍生品或税务推断；
- 通用求解器、Pareto 多候选或新的第三方框架；
- 删除、重写或回填现有固定三方案历史记录。

## 2. 阶段 A 不变量

### 2.1 权限与副作用

- `execution_locked` 恒为 `true`。
- 请求不接受目标权重、数量、金额、订单或证券列表。
- 标的集合只来自所选冻结快照的正持仓。
- Engine 只读取业务事实，进行纯计算；持久化仅限现有安全边界要求的幂等声明和非敏感审计摘要。
- 不写候选、持仓、订单、成交、Shadow 或外部账户。
- 页面加载和普通刷新只读取既有持久化事实；只有用户明确点击“生成调整情景”才调用本地确定性 POST。
- 该 POST 不调用券商、行情提供商、模型或其他外部服务。

### 2.2 时间与绑定

- 请求必须显式提交 `portfolio_snapshot_id`、`diagnostic_report_id` 和 `as_of`。
- Engine 必须确认诊断绑定同一快照，且 `as_of` 等于快照的有效估值时间。
- 快照必须达到既有优化基线要求；展示可用但决策未就绪的快照返回稳定拒绝码。
- 响应绑定 `diagnostic_input_hash`、快照、诊断和估值时间。
- 新快照不会更新旧页面内存中的结果；用户必须基于新快照重新明确生成。

### 2.3 系统政策优先

请求约束只能收紧系统政策：

```python
effective_minimum_cash = max(policy.cash_minimum, constraints.minimum_cash)
```

首版集中度目标使用 `RealAdvisoryPolicy.single_name_warning`。`top3_warning` 只作为前后展示指标，不作为首版停止条件，避免一次开发同时实现多目标优化。

## 3. 冻结计算口径

### 3.1 权重与集中度

- 组合权重以快照的同一估值口径计算，现金与证券权重总和必须为 1。
- `security_top1`、`security_top3` 和 `security_hhi` 的分母是证券市值总额，不含现金。
- 页面必须写明“证券持仓内占比”，不得描述为含现金总资产占比。
- 候选只允许证券权重保持或下降，差额进入现金。

### 3.2 单边换手

现金腿必须计入 L1 差异：

```python
one_way_turnover = (
    sum(abs(target[i] - current[i]) for i in security_ids)
    + abs(target_cash - current_cash)
) / Decimal("2")
```

只减仓转现金时，出售 5% 组合净值等于 5% 单边换手。

### 3.3 精度

- 内部只使用 `Decimal`，拒绝二进制浮点输入。
- 调整步长为 `Decimal("0.0001")`。
- API 使用十进制字符串。
- 权重总和必须精确等于 1，舍入余数只进入现金。

## 4. 阶段 A 领域合同

新建 `engine/src/nexus/optimizer/scenario_contracts.py`。

```python
ScenarioObjective = Literal["reduce_security_concentration"]
ScenarioObjectiveStatus = Literal[
    "met", "improved_not_met", "unchanged", "infeasible"
]

class AdjustmentScenarioConstraintsV1(StrictModel):
    schema_version: Literal["adjustment_scenario_constraints_v1"]
    objective: ScenarioObjective
    max_turnover: Decimal
    minimum_cash: Decimal
    universe_policy: Literal["current_holdings_only"]
    allow_new_positions: Literal[False]

class PortfolioScenarioRequestV1(StrictModel):
    schema_version: Literal["portfolio_scenario_request_v1"]
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    as_of: datetime
    constraints: AdjustmentScenarioConstraintsV1

class ScenarioPositionV1(StrictModel):
    instrument_id: UUID
    current_weight: Decimal
    target_weight: Decimal
    action: Literal["reduce", "maintain"]
    reason_code: Literal["CONCENTRATION_REDUCTION", "BASELINE_WEIGHT"]

class ScenarioMetricSetV1(StrictModel):
    security_top1: TypedMetric
    security_top3: TypedMetric
    security_hhi: TypedMetric
    cash_weight: TypedMetric
    one_way_turnover: TypedMetric
    estimated_cost: TypedMetric

class PortfolioScenarioEvaluationV1(StrictModel):
    schema_version: Literal["portfolio_scenario_evaluation_v1"]
    request_hash: str
    result_hash: str
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    diagnostic_input_hash: str
    valuation_time: datetime
    objective: ScenarioObjective
    analysis_status: Literal["partial"]
    objective_status: ScenarioObjectiveStatus
    baseline: ScenarioMetricSetV1
    candidate: ScenarioMetricSetV1
    positions: tuple[ScenarioPositionV1, ...]
    cash_current_weight: Decimal
    cash_target_weight: Decimal
    hard_constraint_reasons: tuple[str, ...]
    unknown_metrics: tuple[str, ...]
    execution_locked: Literal[True]
```

模型校验必须保证：

- `0 <= minimum_cash <= 1`，`0 <= max_turnover <= 1`；
- `as_of` 为 UTC；
- 每个目标权重不高于当前权重；
- 标的集合等于快照的正证券持仓集合；
- 当前证券加当前现金、目标证券加目标现金分别精确等于 1；
- `request_hash` 对规范化请求计算，`result_hash` 对不含自身哈希字段的规范化结果计算；
- `execution_locked` 只能为 `true`。

阶段 A 不定义 `scenario_id`、`mandate_id`、版本、选择状态、`decision_status` 或 `shadow_eligible`。

## 5. 确定性引擎

新建 `engine/src/nexus/optimizer/scenario_engine.py`，公开唯一领域入口：

```python
def evaluate_adjustment_scenario(
    *,
    snapshot: PortfolioSnapshot,
    report: PortfolioDiagnosticReport,
    constraints: AdjustmentScenarioConstraintsV1,
    policy: RealAdvisoryPolicy,
    request_hash: str,
) -> PortfolioScenarioEvaluationV1:
    ...
```

算法固定如下：

1. 校验快照状态、正净值、诊断绑定和时间一致性；失败返回领域错误，不生成候选。
2. 从快照计算含现金组合权重，再计算 security-only Top1、Top3 和 HHI。
3. 计算有效最低现金和最大单边换手预算。
4. 若当前 `security_top1 <= policy.single_name_warning` 且现金满足要求，返回 `unchanged`。
5. 每轮按当前 security-only 权重降序选择可减仓证券，权重相同时按 `instrument_id` 字典序稳定决胜。阶段 A 不从组合级诊断反推逐标的严重度，也不因缺少流动性数据改变排序。
6. 将一个基点的组合总权重从该证券转入现金。
7. 每轮重新计算 security-only 指标、现金和单边换手，不缓存旧排名。
8. `security_top1` 达到阈值且现金达到有效最低值时返回 `met`。
9. 预算耗尽但 Top1 严格下降时返回 `improved_not_met`，不得越过预算。
10. 合法候选与基线相同或 Top1 未下降时返回 `unchanged`。
11. 单证券组合、无法保留至少一个基点正证券仓位或不存在合法一步动作时返回 `infeasible`。

`estimated_cost` 的基线为已知零，候选在阶段 A 保持 `unknown/COST_MODEL_NOT_AVAILABLE`。因此所有成功的阶段 A 响应固定为 `analysis_status=partial`；unknown 不阻止分析显示，但响应不得声称可执行。无法计算目标指标时不伪造 `unavailable` 结果，而是由 API 返回稳定拒绝码。

## 6. API 与数据流

只新增：

```text
POST /internal/v1/portfolio-scenarios/evaluate
POST /api/v1/portfolio-scenarios/evaluate
```

处理顺序：

```text
用户明确点击
  → BFF 校验封闭 JSON、Idempotency-Key 和身份
  → Engine 校验 HMAC、请求大小和请求合同
  → 数据库读取指定快照、诊断、当前系统政策
  → 纯函数计算
  → 追加不含持仓明细的审计摘要
  → 返回临时结果
  → 页面仅保存在组件内存中
```

Engine API 必须：

- 沿用现有 64 KiB 请求上限、严格 JSON media type、HMAC 和幂等声明校验；
- 核对请求中的快照和诊断属于同一组合；
- 不接受 `portfolio_id` 来替代明确快照；
- 审计只保存请求哈希、政策版本、标的数量、分析状态和目标状态，不保存个人持仓权重；
- 业务上的 `unchanged`、`improved_not_met` 和 `infeasible` 使用成功响应；
- 绑定或基线错误使用稳定错误码。

稳定错误码：

- `SCENARIO_BASELINE_NOT_READY`
- `SCENARIO_DIAGNOSTIC_MISMATCH`
- `SCENARIO_AS_OF_MISMATCH`
- `SCENARIO_OBJECTIVE_UNSUPPORTED`
- `SCENARIO_INPUT_INVALID`

## 7. 页面

在现有组合页新增独立的 `PortfolioScenario` 区块，不改写旧三方案页面。

页面初始状态只显示约束输入和说明：

- 最大换手率；
- 最低现金比例；
- 固定目标“降低单名集中度”；
- 固定范围“仅当前持仓、禁止新增标的”；
- “生成调整情景”按钮。

点击成功后显示：

- 当前组合与一个候选的 Top1、Top3、HHI、现金和换手；
- 每个当前持仓的“保持/降低”与目标权重；
- `met/improved_not_met/unchanged/infeasible` 的中文解释；
- 成本未知与其他阻塞；
- “仅分析，不保存，交易保持锁定”。

页面不得出现“推荐”“接受”“执行”“进入 Shadow”按钮。数值最多显示三位小数，不把非零小数格式化为零。

页面刷新、切换组合或快照变化时清除内存候选；不得用旧结果搭配新快照。

## 8. 阶段 A 验收

必须覆盖：

1. 高集中、预算充足：`met`，只减仓、现金增加、换手不超限。
2. 高集中、预算不足：`improved_not_met`，严格改善且不越过预算。
3. 已满足 Top1 和现金目标：`unchanged`，不为了产生动作强制交易。
4. 单一证券：`infeasible`，不清仓伪装降低 security-only 集中度。
5. 请求最低现金低于系统值：采用系统最低现金。
6. 成本未知：`partial` 可展示，`execution_locked=true`。
7. 快照、诊断或时间不一致：稳定拒绝且纯引擎不运行。
8. 请求包含权重、证券列表或额外字段：422。
9. 同一规范化输入重复计算得到相同 `result_hash`。
10. 页面加载和刷新没有 POST；只有明确点击产生一次本地确定性 POST。
11. 页面不调用 Provider、模型、Shadow、订单或成交入口。
12. 一个现有真实只读快照通过正常产品入口返回分析结果；若基线未达到要求，则准确返回基线拒绝，不伪造成功。

## 9. 阶段 A 完成定义

用户可以针对一个合格的真实冻结组合设置最大换手和最低现金，明确触发后获得一个确定性、可重复、只减仓的集中度候选，并看到真实 unknown；系统没有新增业务持久化，没有外部调用，没有选择或执行入口，`execution_locked=true`。

## 10. 后续阶段边界

### 10.1 阶段 B：Agent 可调用

只有阶段 A 的真实案例验收通过后才设计实施：

- Agent 只提交研究目标、期限、置信度、失效条件和证据引用，不提交权重；
- 只读工具复用阶段 A 纯引擎，并对模型隐藏逐标的目标权重；
- 引入模型调用时才建立组合级 durable run/Job、单次调用和断流恢复；
- 请求级约束直接冻结在运行输入中，仍不要求独立 Mandate 表。

### 10.2 阶段 C：冻结、选择与 Shadow

只有用户确实需要保存和选择候选时才重新评审：

- 是否需要 `portfolio_mandates`、`portfolio_scenarios`、positions 和 selection 表；
- JSONB 与规范化明细表的实际查询需求；
- `analysis_comparable`、`decision_ready` 和 Shadow 准入的权威派生；
- 选中候选到现有 Shadow 合同的显式适配。

阶段 C 仍不授权自动 Shadow、模拟盘或实盘。

## 11. 兼容与回滚

- 阶段 A 没有迁移和新依赖。
- 旧三方案模型、API、页面、历史记录和 Shadow 流程均不修改。
- 回滚只需移除新页面区块、BFF 路由、Engine 路由和两个纯领域文件。
- 不修改 `.openai/hosting.json`、券商配置、密钥、权限或外部数据套餐。
