# NEXUS 组合情景决策闭环技术设计

**状态：** 经技术审查后可进入实施

**设计基线：** `main@0b1ce31f`

**目标：** 在不改变交易锁、不引入通用优化器、不扩大标的范围的前提下，让 NEXUS 根据真实只读组合、冻结诊断和用户明确约束，确定性地产生一个“当前持仓内降低集中度”的候选情景；Agent 只能评估该情景并输出研究意图，不能提交权重、生成订单或批准 Shadow。

## 1. 范围

本设计交付以下最短闭环：

1. 保存版本化的 `PortfolioMandateV1`。
2. 根据同一冻结组合和诊断生成一个确定性候选情景。
3. 区分分析可用、决策就绪、Shadow 准入和执行锁。
4. 将确定性情景评估暴露为 Agent 只读工具。
5. 保存组合级 Agent 判断及其绑定的候选情景。
6. 页面展示“当前组合 vs 一个候选情景”，允许用户选择进入后续验证，但不自动启动 Shadow。

首版明确不包含：

- 新增标的、买入、做空、杠杆或衍生品；
- 通用数学求解器、cvxportfolio、LEAN、Qlib 或 OpenBB 依赖；
- 自动调度、自动重试、自动 Shadow、券商模拟盘或实盘订单；
- 税务推断、税损收割、收益预测或 Pareto 多候选；
- 删除或重写现有三方案历史记录。

## 2. 不变量

### 2.1 权限与执行

- `execution_locked` 始终为 `true`。
- Agent 输入和输出不得包含目标权重、数量、订单或券商凭据。
- Agent 工具只读数据库并执行纯计算，不写方案、Mandate、订单或 Shadow 状态。
- 只有应用服务可以在 Agent 结果通过确定性校验后保存候选情景。
- 用户选择候选只创建本地、只读的选择记录；启动 Shadow 保留现有独立人工确认入口。

### 2.2 时间与证据

- Mandate、组合快照、诊断报告、市场数据、Agent 运行和情景结果都绑定明确 UTC 时间。
- `diagnostic_input_hash`、`portfolio_snapshot_id`、`diagnostic_report_id` 和 `valuation_time` 必须一致。
- 新证据不回填旧情景；过期情景只能重新生成新记录。
- unknown 保持 unknown，不用零值、当前代码默认值或 Agent 文本替代。

### 2.3 风控层次

约束按以下顺序组合：

1. 系统硬政策 `RealAdvisoryPolicy`：不可由用户或 Agent 放宽。
2. 用户 Mandate：可增加限制，不可降低系统限制。
3. 本次情景目标：只能在前两层允许的空间内求解。

首版有效最低现金为：

```python
effective_minimum_cash = max(policy.cash_minimum, mandate.minimum_cash)
```

首版集中度阈值直接使用现有系统政策的 `single_name_warning` 和 `top3_warning`。Mandate 不覆盖这两个阈值。

## 3. 口径冻结

### 3.1 集中度

现有诊断的 `CONCENTRATION_TOP1`、`CONCENTRATION_TOP3` 和 HHI 使用证券仓位总额作为分母，不含现金。首版沿用该口径，并在 API 字段和页面文案中显式命名为：

- `security_top1`
- `security_top3`
- `security_hhi`

不得把这些指标描述为“占含现金总资产比例”。候选情景仍单独返回 `cash_weight`，用于解释出售后现金变化。

### 3.2 换手

首版使用标准单边组合换手，现金腿纳入 L1 差异：

```python
one_way_turnover = (
    sum(abs(target[i] - current[i]) for i in security_ids)
    + abs(target_cash - current_cash)
) / Decimal("2")
```

对于只减仓并转为现金的情景，出售 5% 组合净值等于 5% 换手，而不是 2.5%。`max_turnover` 和成本估计都使用此口径。

### 3.3 精度

- 所有权重使用 `Decimal`。
- 情景调整步长固定为 `0.0001`，即一个基点。
- API 仍以十进制字符串传输。
- 权重总和必须精确为 1；舍入余数只进入现金。

## 4. 领域合同

新建 `engine/src/nexus/optimizer/scenario_contracts.py`，包含以下封闭模型。

```python
ScenarioObjective = Literal["reduce_security_concentration"]
ScenarioAnalysisStatus = Literal["evaluable", "partial", "unavailable"]
ScenarioObjectiveStatus = Literal[
    "met", "improved_not_met", "unchanged", "infeasible"
]
ScenarioDecisionStatus = Literal["candidate_only", "decision_ready"]

class PortfolioMandateV1(StrictModel):
    schema_version: Literal["portfolio_mandate_v1"]
    mandate_id: UUID
    portfolio_id: UUID
    version: int
    objective: ScenarioObjective
    horizon: Literal["short_term", "medium_term", "long_term"]
    max_turnover: Decimal
    minimum_cash: Decimal
    universe_policy: Literal["current_holdings_only"]
    allow_new_positions: Literal[False]
    effective_at: datetime
    created_at: datetime

class PortfolioScenarioRequest(StrictModel):
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    mandate_id: UUID
    objective: ScenarioObjective
    as_of: datetime

class ScenarioPosition(StrictModel):
    instrument_id: UUID
    current_weight: Decimal
    target_weight: Decimal
    action: Literal["reduce", "maintain"]
    reason_code: Literal[
        "CONCENTRATION_REDUCTION", "BASELINE_WEIGHT"
    ]

class ScenarioMetricSet(StrictModel):
    security_top1: TypedMetric
    security_top3: TypedMetric
    security_hhi: TypedMetric
    cash_weight: TypedMetric
    sector_exposure: TypedMetric
    currency_exposure: TypedMetric
    volatility: TypedMetric
    drawdown: TypedMetric
    correlation: TypedMetric
    liquidity: TypedMetric
    one_way_turnover: TypedMetric
    estimated_cost: TypedMetric
    tax_impact: TypedMetric

class PortfolioScenarioEvaluation(StrictModel):
    schema_version: Literal["portfolio_scenario_v1"]
    request_hash: str
    result_hash: str
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    mandate_id: UUID
    diagnostic_input_hash: str
    valuation_time: datetime
    objective: ScenarioObjective
    analysis_status: ScenarioAnalysisStatus
    objective_status: ScenarioObjectiveStatus
    baseline: ScenarioMetricSet
    candidate: ScenarioMetricSet
    positions: tuple[ScenarioPosition, ...]
    cash_current_weight: Decimal
    cash_target_weight: Decimal
    hard_constraint_reasons: tuple[str, ...]
    unknown_metrics: tuple[str, ...]
    evidence_refs: tuple[EvidenceRef, ...]
    execution_locked: Literal[True]

class FrozenPortfolioScenario(PortfolioScenarioEvaluation):
    scenario_id: UUID
    decision_status: ScenarioDecisionStatus
    selection: Literal[
        "unselected", "selected_for_validation", "rejected"
    ]
```

模型校验必须保证：

- `version >= 1`；
- `0 <= minimum_cash <= 1`，`0 <= max_turnover <= 1`；
- `objective` 与 Mandate 一致；
- 每个目标权重不高于当前权重；
- 标的集合与当前正持仓集合完全一致；
- 证券目标权重加目标现金等于 1；
- `FrozenPortfolioScenario.decision_ready` 必须同时满足第 8 节的条件；
- `execution_locked` 只能为 `true`。

## 5. 确定性情景算法

新建 `scenario_engine.py`，公开唯一入口：

```python
def evaluate_adjustment_scenario(
    *,
    snapshot: PortfolioSnapshot,
    report: PortfolioDiagnosticReport,
    mandate: PortfolioMandateV1,
    policy: RealAdvisoryPolicy,
    evidence: ScenarioEvidence | None,
    cost: Decimal | None,
) -> PortfolioScenarioEvaluation:
    ...
```

算法步骤固定如下：

1. 验证快照为 `READY`，有正资产净值，且报告绑定同一快照和时间。
2. 计算当前证券权重、现金权重和 security-only 集中度。
3. 计算有效最低现金与最大单边换手预算。
4. 按当前证券权重降序识别 Top1/Top3 贡献者。
5. 每次只将一个基点从证券转到现金。
6. 候选选择顺序固定为：
   - 当前权重较高者优先；
   - 诊断严重度较高者优先；
   - 流动性评分较高者优先；
   - UUID 字典序作为最终稳定 tie-breaker。
7. 每次调整后重新计算 security-only Top1/Top3，不缓存旧排名。
8. 满足系统集中度阈值和最低现金后停止。
9. 达到最大换手预算仍未满足时停止并返回 `improved_not_met`；不得继续越过预算。
10. 没有合法减仓空间或一个基点精度无法推进时返回 `infeasible`。
11. 目标指标未严格改善时返回 `unchanged`；后置 readiness 必须把它判为 `candidate_only`。

状态判定：

- `met`：集中度阈值与最低现金全部满足。
- `improved_not_met`：目标指标严格下降，但预算不足以满足全部阈值。
- `unchanged`：合法候选与基线相同，或目标指标没有下降。
- `infeasible`：输入合法，但约束空间不存在可行的一个基点动作。

情景生成不要求所有风险维度已知；但目标指标、现金和换手必须已知，否则 `analysis_status=unavailable`。

## 6. 持久化

新增迁移 `0048_portfolio_scenario_decisions.py`，只新增表，不修改历史三方案表。

### 6.1 `portfolio_mandates`

- `id UUID PRIMARY KEY`
- `portfolio_id UUID NOT NULL REFERENCES portfolios(id)`
- `version INTEGER NOT NULL CHECK (version >= 1)`
- `schema_version VARCHAR(32) NOT NULL CHECK = 'portfolio_mandate_v1'`
- `objective VARCHAR(64) NOT NULL CHECK = 'reduce_security_concentration'`
- `horizon VARCHAR(20) NOT NULL`
- `max_turnover NUMERIC(38,12) NOT NULL CHECK BETWEEN 0 AND 1`
- `minimum_cash NUMERIC(38,12) NOT NULL CHECK BETWEEN 0 AND 1`
- `universe_policy VARCHAR(32) NOT NULL CHECK = 'current_holdings_only'`
- `allow_new_positions BOOLEAN NOT NULL CHECK = false`
- `effective_at TIMESTAMPTZ NOT NULL`
- `created_at TIMESTAMPTZ NOT NULL`
- `UNIQUE(portfolio_id, version)`

记录不可更新；新设置生成下一版本。删除不提供产品入口。

### 6.2 `portfolio_scenarios`

- 绑定 `portfolio_snapshot_id`、`diagnostic_report_id`、`mandate_id`。
- 保存 `request_hash`、`result_hash`、`diagnostic_input_hash`、`valuation_time`。
- 保存分析状态、目标状态、当前/目标现金、unknown 列表、硬约束原因、证据引用。
- `execution_locked` 数据库约束恒为 true。
- `UNIQUE(request_hash, result_hash)` 支持安全幂等落盘。

### 6.3 `portfolio_scenario_positions`

- 每个情景和标的一行。
- 保存当前/目标权重、动作和原因码。
- `UNIQUE(scenario_id, instrument_id)`。
- 数据库约束禁止 `target_weight > current_weight`。

### 6.4 `portfolio_decision_runs`

不把现有 `AgentResearchRunRecord.instrument_id` 改成 nullable。新增组合级运行表，避免破坏逐标的研究合同：

- 绑定 `scenario_id`、`portfolio_snapshot_id`、`diagnostic_report_id`、`mandate_id`。
- 保存 prompt/model/tool/input/output/evidence hash。
- 状态只允许 `prepared/running/completed/failed/abstained`。
- 部分唯一索引保证同一 `(portfolio_snapshot_id, mandate_id)` 最多一个 `prepared/running` 运行。
- `intent_payload JSONB` 保存经 schema 校验的 `PortfolioScenarioIntentV1`。
- `execution_locked` 恒为 true。
- 保留现有最多一次显式重试和用量结算约束。

### 6.5 `portfolio_scenario_selections`

- `id UUID PRIMARY KEY`
- `scenario_id UUID NOT NULL UNIQUE REFERENCES portfolio_scenarios(id)`
- `decision VARCHAR(32) NOT NULL`，只允许 `selected_for_validation/rejected`
- `expected_version INTEGER NOT NULL CHECK = 0`
- `created_at TIMESTAMPTZ NOT NULL`
- 记录追加后不可更新或删除；未产生记录即为 `unselected`

## 7. Agent 合同

### 7.1 只读工具

新增工具 `evaluate_adjustment_scenario`：

```python
class EvaluateAdjustmentScenarioInput(_ToolInput):
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    mandate_id: UUID
    objective: Literal["reduce_security_concentration"]
    as_of: datetime
```

工具执行器：

1. 加载冻结快照、诊断、Mandate、政策和已有证据。
2. 调用纯函数 `evaluate_adjustment_scenario`。
3. 不落盘。
4. 返回去敏后的结果：分析状态、目标状态、目标指标前后值、现金、换手、成本状态、unknown、原因码、`result_hash`。
5. 不把逐标的目标权重传给模型。

### 7.2 组合级研究意图

新建 `PortfolioScenarioIntentV1`：

```python
class PortfolioScenarioIntentV1(_IntentModel):
    schema_version: Literal["portfolio_scenario_intent_v1"]
    decision_id: UUID
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    mandate_id: UUID
    scenario_result_hash: str
    decision_time: datetime
    horizon: Literal["short_term", "medium_term", "long_term"]
    assessment: Literal[
        "advance_candidate_for_review", "observe", "do_not_advance"
    ]
    rationale: tuple[ResearchClaim, ...]
    uncertainties: tuple[str, ...]
    invalidation_conditions: tuple[ResearchCondition, ...]
    valid_until: datetime
    evidence_refs: tuple[EvidenceRef, ...]
    execution_locked: Literal[True]
```

该模型沿用现有安全扫描，并把 `weight/quantity/amount/order` 等字段继续列入禁止项。

### 7.3 运行与落盘顺序

1. 应用读取 Mandate 和冻结输入。
2. Agent 调用只读情景工具并得到 `result_hash`。
3. Agent 输出 `PortfolioScenarioIntentV1`。
4. 应用重新运行纯函数。
5. 重新计算结果的 `result_hash` 必须与 Agent 引用一致。
6. 应用验证 intent、工具调用记录和证据引用。
7. 在一个数据库事务中保存情景、仓位和 decision run；持久化边界生成 `scenario_id`。

这样既保持 Agent 工具只读，又避免信任模型转述的权重。

## 8. 状态与门禁

### 8.1 `analysis_status`

- `evaluable`：目标指标、现金、换手均已知，输入绑定有效。
- `partial`：目标可评估，但至少一个非目标风险或成本维度 unknown。
- `unavailable`：目标本身无法计算、基线不 READY、绑定错误或证据时间冲突。

### 8.2 `decision_status`

只有以下条件全部成立才是 `decision_ready`：

- `analysis_status=evaluable`；
- `objective_status=met`；
- Mandate、快照、诊断和结果未过期；
- 无系统硬约束违反；
- 适用 FX、行业、历史风险、流动性和实际成本证据完整；
- `tax_impact` 仍为 unknown 时，维持 `candidate_only`；
- Agent assessment 为 `advance_candidate_for_review`；
- 用户尚未选择不影响该状态，但选择记录必须另存。

首版真实账户预计大多数结果是 `partial + candidate_only`；这仍是有效的分析交付，不得升级成推荐。

### 8.3 Shadow 与执行

- `shadow_eligible` 继续由现有 Shadow 权威检查计算，不存成可被前端篡改的布尔事实。
- 新情景进入 Shadow 前需要一个显式适配步骤，把“保持现状 + 被选候选”冻结成现有 Shadow 可消费结构。
- 首版不实现该适配；页面按钮只允许“选择进入后续验证”。
- `execution_locked=true` 不受任何分析、选择或 Shadow 状态影响。

## 9. API

新增以下内部 API，并通过现有 BFF 暴露给本地页面：

### 9.1 Mandate

`POST /internal/v1/portfolio-mandates`

- 新建下一不可变版本。
- 要求 Idempotency-Key。
- 同一请求哈希返回原记录；相同 key 不同请求返回冲突。

`GET /internal/v1/portfolio-mandates/current?portfolio_id={id}`

- 返回最高版本。
- 不存在时返回 404，不生成默认 Mandate。

### 9.2 情景

`POST /internal/v1/portfolio-scenarios/evaluate`

- 纯评估，不保存情景。
- 请求不接受权重。
- 返回完整候选给应用，不作为 Agent 工具的直接模型投影。

`POST /internal/v1/portfolio-decisions/run`

- 创建一个持久化组合级 Agent 运行和 `portfolio.decision` Job，返回 HTTP 202、`run_id`、`status=prepared` 及 Location。
- Job 使用 `RetryPolicy.NEVER`、`max_attempts=1` 和 120 秒上限。
- Worker 完成后重新计算并原子保存情景和 intent。
- 相同 Idempotency-Key 和请求哈希返回原 `run_id`，不创建第二个 Job。

`GET /internal/v1/portfolio-decisions/{run_id}`

- 返回持久化运行状态；完成时包含 `scenario_id`，失败时只返回稳定错误码。
- 运行未知或客户端断流时调用此接口恢复状态，不重放模型。

`GET /internal/v1/portfolio-scenarios/{scenario_id}`

- 返回冻结结果、状态、证据和用户选择状态。
- 读取不得触发行情、模型或重新计算。

`POST /internal/v1/portfolio-scenarios/{scenario_id}/select`

- 只保存本地人工选择 `selected_for_validation` 或 `rejected`。
- 不启动 Shadow，不创建订单。
- 需要版本号做乐观并发控制。

对应 Web BFF 必须增加同名 `/api/v1/...` 路由，包括 decision run 的 POST 和 GET，并同步更新 `app/lib/engine/client.ts` 的封闭路径模板、HMAC 规范化以及 `app/lib/engine/errors.ts` 的运行时响应校验；页面不得绕过 BFF 直连 Engine。

### 9.3 稳定错误码

- `MANDATE_NOT_FOUND`
- `MANDATE_BINDING_INVALID`
- `SCENARIO_BASELINE_NOT_READY`
- `SCENARIO_DIAGNOSTIC_MISMATCH`
- `SCENARIO_OBJECTIVE_UNSUPPORTED`
- `SCENARIO_TURNOVER_BUDGET_EXHAUSTED`
- `SCENARIO_INFEASIBLE`
- `SCENARIO_RESULT_HASH_MISMATCH`
- `SCENARIO_STALE`
- `SCENARIO_VERSION_CONFLICT`
- `PORTFOLIO_DECISION_ALREADY_RUNNING`

业务上的 `improved_not_met` 和 `infeasible` 是成功响应中的状态，不使用 500。

## 10. 页面

现有三方案 API 与页面保持可读，不删除。新增组合决策区：

1. 当前组合及冻结时间。
2. 当前 Mandate：目标、期限、最大换手、最低现金。
3. Agent 识别的问题及证据。
4. 当前组合和一个候选情景的指标对比。
5. unknown 指标和阻止决策/Shadow 的原因。
6. `选择进入后续验证`、`拒绝`、`继续观察`。

文案规则：

- `candidate_only` 使用“候选情景”，不使用“推荐方案”。
- `decision_ready` 可使用“建议进入人工评审”，仍不称“可执行”。
- 不提供“接受并执行”按钮。
- 数值最多显示三位小数，但底层字符串精度不改变。
- security-only 集中度必须明确分母。

## 11. 兼容与回滚

- 迁移只新增表，`downgrade` 按依赖逆序删除新表，不触碰旧方案数据。
- 旧三方案的模型、API、页面和 Shadow 流程在本阶段不改变。
- 新页面读取不到 Mandate 或情景时显示明确空状态，不回退生成固定三方案。
- 功能可通过不展示新入口回滚；新表中的不可变审计记录保留。
- 不修改 `.openai/hosting.json`、券商配置或访问权限。

## 12. 验收案例

实现必须覆盖以下代表性案例：

1. 高集中、预算充足：返回 `met`，只减仓、现金增加、换手未超限。
2. 高集中、预算不足：返回 `improved_not_met`，不越过预算。
3. 已满足目标：返回 `unchanged`，不为了产生方案强制交易。
4. 单一证券且 security-only 阈值不可满足：返回 `infeasible`，不清仓伪装零风险。
5. Mandate 最低现金低于系统值：有效最低现金仍取系统值。
6. 缺行业或成本：可以 `partial` 展示，但不能 `decision_ready` 或 Shadow。
7. 结果哈希不一致：不保存情景。
8. Agent 输出权重字段：schema 校验失败。
9. 快照刷新后读取旧情景：显示 stale，不重算、不回填。
10. 页面刷新：只读已保存数据，不触发行情或模型。

## 13. 完成定义

本设计完成的用户可见结果是：用户能够设置一个明确的组合约束，平台根据真实冻结持仓生成一个确定性的降低集中度候选，Agent 能解释是否值得进入人工评审，页面能显示调整前后结果和真实阻塞；任何 unknown、选择或页面状态都不能解除执行锁。

仅当实现计划的全部阶段验收通过，才允许讨论 Shadow 适配。实盘和券商模拟盘仍不在本设计范围内。
