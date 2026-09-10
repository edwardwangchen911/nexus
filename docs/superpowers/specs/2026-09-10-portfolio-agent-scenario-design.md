# NEXUS 组合决策 Agent 与情景工具技术设计

**状态：** 独立审查通过（97/100）  
**阶段：** B（Agent 可调用）  
**前置：** 阶段 A 真实冻结组合已通过 Engine、BFF 和页面验收  
**目标：** 让一个组合级 Agent 在受限的持久化运行中调用一次确定性情景工具，生成可审计的研究意图，但不保存候选、不选择方案、不解锁 Shadow 或交易。

## 1. 结论与范围

阶段 B 采用独立的“组合决策运行”，不扩展现有单标的 `AgentResearchRunRecord`。现有运行强制绑定 `instrument_id` 和单标的证据权威；将组合决策塞入该模型会产生虚假的证券身份、错误的幂等语义和难以审计的恢复路径。

本阶段只交付：

- 组合级 durable run 及一个通用 Job；
- 一个专用只读工具 `evaluate_adjustment_scenario`；
- 一个严格的 `PortfolioResearchIntentV1` 模型输出；
- 内部 Engine API、BFF 读取路由和当前组合页的只读展示；
- 单次工具调用、费用上限、幂等、断流恢复和失败关闭。

本阶段明确不交付：

- `PortfolioMandate` 或 Mandate CRUD；
- 组合情景、候选仓位或用户选择的业务持久化；
- 接受、拒绝、继续观察等状态机；
- Shadow、模拟盘、实盘、调度或自动重跑；
- 通用求解器、多目标 Pareto、新 Agent 委员会或新外部数据源；
- 修改旧三方案的数据合同或历史记录。

## 2. 用户流程

1. 页面继续通过现有路径读取冻结组合、诊断和阶段 A 情景结果。
2. 用户在已有情景结果上明确点击“让 Agent 解读”；页面加载、刷新或切换组合不会创建运行。
3. Engine 以服务端权威重新加载组合快照、诊断和阶段 A 请求约束，对输入做时点与哈希绑定。
4. Engine 创建 `prepared` 组合决策运行、预留最坏模型成本，并入队一个 `agent.portfolio_decision` Job。
5. Worker 启动一个模型运行。模型必须且只能调用一次 `evaluate_adjustment_scenario`。
6. 工具执行器忽略模型对 ID、时间、换手和现金约束的任何表达；这些值仅从 run 的冻结输入加载。
7. 工具复用阶段 A 纯引擎，但仅将聚合风险变化、unknown、理由、证据和 `result_hash` 投影给模型；不返回任何逐标的目标权重。
8. 模型输出结构化研究意图。确定性校验通过后追加输出记录并将 run 置为 `completed`；否则置为 `abstained` 或 `failed`。
9. 页面轮询持久化状态并展示 Agent 关注的问题、结论、证据、失效条件和限制；逐标的权重继续直接来自阶段 A 确定性结果。

## 3. 运行与持久化

### 3.1 `PortfolioDecisionRunRecord`

新表 `portfolio_decision_runs`：

```python
class PortfolioDecisionRunRecord(ImmutableCreatedAt, Base):
    id: UUID
    portfolio_id: UUID
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    prompt_version_id: UUID
    model_config_id: UUID
    actor_id: str
    idempotency_key: str
    request_hash: str                  # 原始 POST bytes 的 SHA-256
    status: Literal["prepared", "running", "completed", "abstained", "failed"]
    retry_count: int                    # 固定 0，本阶段不自动重试
    decision_time: datetime
    input_hash: str
    schema_hash: str
    frozen_input: dict[str, object]     # PortfolioDecisionInputV1 的规范 JSON
    prompt_hash: str
    model_hash: str
    tool_hash: str
    evidence_hash: str
    evidence_manifest: list[dict[str, object]]
    tool_result_hash: str | None
    tool_projection: dict[str, object] | None
    provider_phase: Literal["none", "first_requested", "tool_recorded", "second_requested"]
    reserved_cost_usd: Decimal
    settled_cost_usd: Decimal | None
    pricing_version: str
    input_tokens: int | None
    output_tokens: int | None
```

`input_tokens/output_tokens` 允许在 checkpoint 2 写入首轮已确认的累计 usage，最终成功时再更新为两轮累计 usage；`settled_cost_usd` 只在终态写入。

约束：

- `(actor_id, idempotency_key)` 唯一；先按原始 POST bytes 的 `request_hash` 抢占或重放现有 `IdempotencyRecord`，相同 key 只在原始请求哈希不同时返回 409。
- `frozen_input` 必须是 object，其规范哈希等于 `input_hash`。
- `(portfolio_snapshot_id, portfolio_id)` 使用组合外键；`(diagnostic_report_id, portfolio_snapshot_id)` 使用组合外键，为此迁移向诊断表增加 `(id, portfolio_snapshot_id)` 唯一约束。
- `schema_hash` 绑定 `PortfolioResearchIntentV1` 的规范 JSON schema；`tool_hash` 绑定工具输入 schema、投影 schema、工具/投影实现、情景 canonical 合同、`scenario_engine.py` 内容哈希和政策版本。任一权威语义变化都拒绝旧 run，不尝试在新代码下重放。
- `evidence_manifest` 最多 100 条，必须在 `decision_time` 前可用且规范哈希等于 `evidence_hash`。
- `tool_result_hash` 和 `tool_projection` 要么同时为 null，要么同时存在且投影内的 `result_hash` 与前者相等。
- `request_hash` 只处理 HTTP 幂等；`input_hash` 在完成服务端权威冻结后生成，并用于 Job 唯一身份。权威后来变化不得将已存在的同 key 重放误判为冲突。
- `provider_phase` 是崩溃窗口权威；每次 provider 请求发送前必须先在独立短事务中更新并提交。
- `status` 只允许 `prepared -> running -> completed|abstained|failed`，以及尚未发送任何 provider 请求但 Job 已终止时的 `prepared -> failed`。
- `retry_count=0`；Job 的 `max_attempts=1` 且 `retry_policy=never`。
- 预留成本与现有 Agent 共享每日总预算，不创建第二套预算配置。

### 3.2 `PortfolioDecisionOutputRecord`

新表 `portfolio_decision_outputs`：

```python
class PortfolioDecisionOutputRecord(ImmutableCreatedAt, Base):
    id: UUID
    run_id: UUID                       # unique
    status: Literal["completed", "abstained", "failed"]
    content_hash: str
    scenario_result_hash: str | None
    intent: dict[str, object] | None    # 仅 completed 时存在
    details: dict[str, object]          # 稳定错误码，不存原始 provider 响应
```

分离 run 和 output 是为了保持输出追加不可变，并允许断流后只读检查是否已有终态输出。本阶段每个 run 最多一个 output，不引入通用事件流。

### 3.3 `PortfolioDecisionBudgetReservationRecord`

新表 `portfolio_decision_budget_reservations` 保留现有 Agent 预算语义，不修改 `agent_daily_budget_reservations` 的外键：

```python
class PortfolioDecisionBudgetReservationRecord(ImmutableCreatedAt, Base):
    id: UUID
    business_date: date
    run_id: UUID                       # unique, FK portfolio_decision_runs.id
    pricing_version: str
    reserved_usd: Decimal
    settled_usd: Decimal | None
    state: Literal["reserved", "settled", "released"]
```

两种预算预留使用同一个 `agent-budget:<UTC date>` PostgreSQL advisory transaction lock。新的共享汇总 helper 在锁内对两张 reservation 表的 `reserved` 金额和 `settled` 实际金额求和；原单标研究预留也改为调用该 helper，避免两类 run 同时超卖每日总预算。只改共享计数逻辑，不改原研究的保留、结算和释放状态机。

### 3.4 Job

向 `JobType` 增加 `AGENT_PORTFOLIO_DECISION = "agent.portfolio_decision"`，使用 `general` 队列：

- `schema_version="portfolio_decision_job_v1"`
- payload 仅含 `run_id`
- `scope_kind="portfolio"`，`scope_id=<portfolio_id>`
- `timeout_seconds=45`
- `retry_policy=never`
- `max_attempts=1`

不启动新 Worker 进程、队列或调度器。

## 4. 冻结输入合同

```python
class PortfolioDecisionInputV1(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["portfolio_decision_input_v1"]
    portfolio_id: UUID
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    decision_time: datetime
    objective: Literal["reduce_security_concentration"]
    max_turnover: DecimalString
    minimum_cash: DecimalString
    universe_policy: Literal["current_holdings_only"]
    allow_new_positions: Literal[False]
    scenario_request_hash: Hash64
    expected_scenario_result_hash: Hash64
    diagnostic_input_hash: Hash64
    policy_version: str
    evidence_hash: Hash64
```

创建 run 前必须重新验证：

- snapshot 属于 portfolio，是冻结且非未来的快照；
- diagnostic 绑定同一 snapshot、同一估值时点且哈希匹配；
- 情景请求只有阶段 A 允许的目标和约束，不含权重、证券列表或额外字段；
- `scenario_request_hash` 由规范化的阶段 A 请求生成；
- 准入时用当前代码和冻结权威重算阶段 A 结果，只有其 `result_hash` 等于用户所见的 `expected_scenario_result_hash` 才创建 run；
- `policy_version` 必须同时匹配 diagnostic 和当前可用的确定性政策权威；
- 服务端 `evidence_manifest` 中的 refs 必须在决策时点可用并覆盖 snapshot、diagnostic 和情景请求的引用权威，其规范哈希必须等于冻结输入的 `evidence_hash`；
- 当前 `execution_locked` 必须为 true。

## 5. Agent 工具边界

新建独立的 `PortfolioDecisionTools`，不修改现有单标的 `EXPECTED_READ_ONLY_TOOL_NAMES`。这避免八个单标的委员会角色获得无关的组合调整能力。

### 5.1 模型可见输入

```python
class EvaluateAdjustmentScenarioToolInputV1(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    objective: Literal["reduce_security_concentration"]
```

工具执行前确定性校验 `objective == frozen_input.objective`。模型不可提交 snapshot ID、diagnostic ID、时间、换手、现金、证券、权重或数量。

### 5.2 模型可见输出

```python
class AgentScenarioProjectionV1(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["agent_scenario_projection_v1"]
    objective: Literal["reduce_security_concentration"]
    objective_status: Literal["met", "improved_not_met", "unchanged", "infeasible"]
    analysis_status: Literal["partial"]
    baseline: AgentScenarioMetricSetV1
    candidate: AgentScenarioMetricSetV1
    unknown_metrics: tuple[str, ...]
    reasons: tuple[str, ...]
    changed_position_count: int
    result_hash: Hash64
    citation_ids: tuple[Hash64, ...]
    execution_locked: Literal[True]
```

`citation_id = sha256(run_id + ":" + manifest_index + ":" + canonical_json(EvidenceRef))`，其中 manifest 已按完整证据身份规范排序；索引与完整规范身份共同保证 ID 可唯一反解到本 run 的一条证据，并由唯一性测试覆盖相同 content hash 的不同来源。模型仅看到不透明 ID。完整 `EvidenceRef` 只保存在服务端 `evidence_manifest`，并在 GET 响应投影给用户时解析；不将可能含 symbol、UUID、URL 或本地路径的 `object_id/location` 传给模型。

`AgentScenarioMetricSetV1` 仅含 Top1、Top3、HHI、现金、单边换手和估算成本的 typed metric。投影中禁止出现 `positions`、`instrument_id`、symbol、`current_weight`、`target_weight`、quantity、order 或任何执行准入字段。`cash_weight` 是明确允许的组合聚合指标，不代表单个证券的目标。

投影完成后必须再次对其规范 JSON 扫描：只允许上述封闭 schema 的精确键，并额外拒绝 `positions`、`instrument_id`、symbol、`current_weight`、`target_weight`、quantity、amount、order 和执行准入字段。不直接复用现有会拒绝任何 `weight` token 的单标 ResearchIntent 扫描器，而是实现组合投影专用的精确 allowlist。

### 5.3 模型初始上下文

模型不接收 run 的完整 `frozen_input` 或阶段 A 的完整结果。第一个 provider turn 只接收 `PortfolioDecisionModelContextV1`：

```python
class PortfolioDecisionModelContextV1(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["portfolio_decision_model_context_v1"]
    run_id: UUID
    portfolio_id: UUID
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    decision_time: datetime
    objective: Literal["reduce_security_concentration"]
    constraints: AdjustmentScenarioConstraintsV1
    current_diagnostics: tuple[PortfolioDiagnosticSummaryV1, ...]
    citation_ids: tuple[Hash64, ...]
    execution_locked: Literal[True]
```

`PortfolioDiagnosticSummaryV1` 只允许诊断 code 精确属于 `CONCENTRATION_TOP1|CONCENTRATION_TOP3|CONCENTRATION_HHI`，并只包含 severity、typed value、unit 和不透明 citation IDs；任何其他 code 均拒绝而不是启发式判断。若三项没有形成可验证的组合集中度上下文，准入失败。模型必须调用工具才能看到候选后的聚合变化；不在首轮直接喂入工具结果来伪装“Agent 可调用”。

## 6. 模型输出合同

```python
class PortfolioResearchClaimV1(BaseModel):
    text: str                         # 1..500
    citation_ids: tuple[Hash64, ...]  # 1..5

class PortfolioInvalidationConditionV1(BaseModel):
    signal: Literal[
        "portfolio.security_top1",
        "portfolio.cash_weight",
        "portfolio.scenario_objective_status",
        "portfolio.estimated_cost_status",
    ]
    operator: Literal["lt", "lte", "eq", "gte", "gt"]
    threshold: DecimalString | Literal["met", "improved_not_met", "unchanged", "infeasible", "known", "unknown"]
    unit: Literal["ratio", "state"]
    citation_ids: tuple[Hash64, ...]

class PortfolioLimitationV1(BaseModel):
    code: Literal["estimated_cost"]
    text: str                         # 1..300

class PortfolioResearchIntentV1(BaseModel):
    schema_version: Literal["portfolio_research_intent_v1"]
    run_id: UUID
    portfolio_id: UUID
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    decision_time: datetime
    assessment: Literal["explore_adjustment", "observe"]
    objective: Literal["reduce_security_concentration"]
    horizon: Literal["short_term", "medium_term", "long_term"]
    confidence: Literal["low", "medium", "high"]
    rationale: tuple[PortfolioResearchClaimV1, ...]  # 1..4
    invalidation_conditions: tuple[PortfolioInvalidationConditionV1, ...]  # 1..5
    limitations: tuple[PortfolioLimitationV1, ...]  # 0..8
    scenario_result_hash: Hash64
    citation_ids: tuple[Hash64, ...]
    execution_locked: Literal[True]
```

关联校验：

- 输出 ID、时间和目标必须等于 run 冻结输入；
- `scenario_result_hash` 必须等于本次工具执行结果；
- 所有 claim 和 condition 的 citation IDs 必须是总 `citation_ids` 的子集，且总引用必须可由 run 的服务端 `evidence_manifest` 唯一解析；
- `met|improved_not_met` 时可输出 `explore_adjustment`；`unchanged|infeasible` 只能输出 `observe`；
- `limitations[*].code` 的集合必须与工具 `unknown_metrics` 完全相等，每项必须有非空说明，禁止用一段自由文本模糊覆盖多个 unknown；
- invalidation condition 使用封闭矩阵：`security_top1|cash_weight` 只能搭配数值比较操作符、DecimalString 和 `ratio`；`scenario_objective_status|estimated_cost_status` 只能搭配 `eq`、对应状态枚举和 `state`。
- 输出 JSON 通过与现有 research intent 同等的安全扫描，禁止 weight、quantity、amount、order、URL、SQL、shell 和 path 字段或危险文本；
- 不将 `confidence` 解释为统计概率。

## 7. 模型调用与恢复

组合决策使用单独的 `portfolio-decision-v1` prompt asset，在现有 `agent_prompt_versions` 中以新的 `role="portfolio_decision"` 冻结。迁移扩展 `role_allowed` check constraint，但不将该 role 加入旧单标委员会的 `ROLE_ORDER`；旧 `load_complete_prompt_authority()` 不会看到它，从而不会替换旧 `portfolio-v1` prompt。新准入器按 role 和 version 精确选择专用 prompt。模型权威仍来自 `agent_model_configs`，但准入时要求 `max_tool_calls >= 1`；运行时无论配置上限多大都将本 run 夹紧为 1。第一轮使用 `tool_choice="required"`，第二轮使用 `tool_choice="none"` 并启用严格 `PortfolioResearchIntentV1` JSON schema。

一个 run 是一次逻辑模型运行，但 Responses 自定义工具协议需要两个 provider transport turn：

1. turn 1：模型返回唯一工具调用；
2. 本地执行工具；
3. turn 2：将工具结果回送模型，获得严格 JSON schema 输出。

因此“单次调用”的精确含义是：

- 一个 durable run；
- 一次工具执行；
- 最多两个 provider transport turn；
- 不允许第二个工具调用；
- 不自动重试 provider 请求，不在超时后重放 run。

状态和恢复规则：

- 不将该 Job 放入现有“handler 与 Job 终结共用一个长事务”的 `run_job_worker_once()` 执行路径。新建窄化的 `run_portfolio_decision_worker_once()`，在同一 general Worker 进程中只 claim `agent.portfolio_decision`；现有通用 registry 不注册该 Job type，避免双重 claim。
- 新增通用但最小的 `checkpoint_job(session, claim, now, apply)` 仓储原语：锁定并验证当前 lease/fence，执行一个短业务 `apply`，必要时延长 lease，然后在该独立事务中提交；不改通用 handler 协议。
- checkpoint 1：claim 成功后，将 run 从 `prepared` 改为 `running`、`provider_phase="first_requested"` 并提交，然后在数据库事务外发送首个 provider turn。
- checkpoint 2：首轮响应经校验且本地工具执行完成后，将 `provider_phase="tool_recorded"`、`tool_result_hash` 和安全聚合投影写入 run 并提交，不保存逐标权重副本。
- checkpoint 3：将 `provider_phase="second_requested"` 写入并提交后，才在事务外发送第二个 provider turn。
- completed 与 abstained 使用现有 fenced `finalize_job()`；活动 lease 下的 failed 使用支持 `outcome="failed"` 的 `execute_and_finalize_job()`。两条路径都在同一事务写入 output、run 终态、预算结算和 Job 终态。
- 两个 provider turn 共享一个 `RunUsageAllowance`、一个 45 秒绝对 deadline 和一份模型配置的聚合 token 上限；第二轮只能使用第一轮后的剩余额度，因此预留值仍是该配置的聚合最坏成本，不按 transport turn 翻倍。
- 在任何 provider 请求前失败时释放预留；已获得可校验 usage 时结算已持久化的实际成本；只有 `first_requested|second_requested` 这类请求可能已发送但 usage 不可确定的状态才保守结算全部预留金额，不释放后再重放。
- API 断流或页面轮询失败不影响 Worker；客户端使用 run ID 恢复读取。
- 每次轮询在 claim 前先调用现有 `reclaim_expired_jobs()`，然后运行 `reconcile_orphaned_portfolio_decisions()`：只处理 Job 已 `failed|dead_letter` 但 run 仍非终态的记录，并在一个短事务内追加失败 output、终结 run 和结算预算。
- reconcile 按已提交 checkpoint 决定费用：`prepared/provider_phase=none` 释放预留；`tool_recorded` 按首轮已持久化 usage 结算；`first_requested|second_requested` 因存在请求已发送但 usage 未知的窗口而保守结算全部预留。任何分支都不得再次调用 provider。
- 所有 checkpoint、正常终结和孤儿恢复统一使用 `job -> run -> output -> reservation` 的锁顺序；孤儿恢复虽然面对终态 Job，也必须先锁 Job 再触碰业务行，禁止反向锁定。
- 已有终态 output 时只校正尚未终结的 Job 投影，不得调用模型；Job 已因超时或租约回收终结时，reconcile 只终结业务行和预算，不再调用需要活动 lease 的 fenced finalize。
- 由于 provider 响应是 `store=false`，任何 provider 请求发送后但输出未持久化的崩溃窗口都失败关闭，不自动重放。

## 8. API 和页面

### 8.1 Engine

```text
POST /internal/v1/portfolio-decision-runs
GET  /internal/v1/portfolio-decision-runs/{run_id}
```

POST 请求：

```json
{
  "schema_version": "portfolio_decision_create_v1",
  "portfolio_snapshot_id": "uuid",
  "diagnostic_report_id": "uuid",
  "expected_scenario_result_hash": "64-char lowercase sha256",
  "scenario_request": {
    "schema_version": "portfolio_scenario_request_v1",
    "portfolio_snapshot_id": "uuid",
    "diagnostic_report_id": "uuid",
    "as_of": "UTC timestamp",
    "constraints": {
      "schema_version": "adjustment_scenario_constraints_v1",
      "objective": "reduce_security_concentration",
      "max_turnover": "0.050000",
      "minimum_cash": "0.100000",
      "universe_policy": "current_holdings_only",
      "allow_new_positions": false
    }
  }
}
```

POST 需要 `Idempotency-Key`，返回 202 和 `Location`。GET 返回 run 状态及已校验 output；不返回原始模型响应、推理内容或 provider 错误原文。

Engine GET 仅在当前经 HMAC 验证的 `actor_id == run.actor_id` 时返回记录，其他情况统一返回 404，不泄露 run 是否存在。POST 重放也必须同时匹配 actor、route、idempotency key 和 raw request hash。

### 8.2 BFF

```text
POST /api/v1/portfolio-decision-runs
GET  /api/v1/portfolio-decision-runs/{runId}
```

BFF 仅做严格 schema 验证、HMAC 转发、稳定错误 allowlist、错误映射和响应再验证；GET 响应的 run ID、snapshot ID、diagnostic ID 和 expected scenario result hash 必须匹配页面当前作用域。BFF 不计算风险、不修改约束、不生成意图。

### 8.3 页面

在现有情景区块下增加一个紧凑的 Agent 解读区：

- 只有当阶段 A 结果存在、快照/诊断未变更且未过期时，按钮可用；
- 点击一次创建 run，运行中按钮禁用；
- 使用有上限的轮询读取持久化状态，页面关闭后重开可通过已保存的 run ID 继续读取；
- 点击后先在 `localStorage` 保存随机幂等 key，POST 成功后再保存 run ID；记录键由 portfolio/snapshot/diagnostic/scenario result hash 组成，值只含幂等 key 和 run ID，不保存持仓或模型输出。网络结果不明时复用同一 key，不新建 run；
- 重新打开页面时只对完整匹配当前作用域的 run ID 发起 GET，不自动 POST；
- 快照、诊断或情景结果变更时立即清除旧 Agent 结果和 run ID；
- 展示“研究解读，不是投资建议”、证据、unknown 和 `execution_locked=true`；
- 不出现“接受”、“执行”、“进入 Shadow”或类似按钮。

## 9. 稳定失败语义

| 错误码 | 结果 | 语义 |
|---|---|---|
| `PORTFOLIO_DECISION_AUTHORITY_MISMATCH` | 409 / 不创建 run | snapshot、diagnostic、时间或哈希不匹配 |
| `PORTFOLIO_DECISION_INPUT_UNSAFE` | 422 / 不创建 run | 请求含权重、证券或额外字段 |
| `PORTFOLIO_DECISION_BUDGET_EXHAUSTED` | 409 / 不创建 run | 日预算无法预留 |
| `PORTFOLIO_DECISION_TOOL_REQUIRED` | abstained | 模型未调用工具 |
| `PORTFOLIO_DECISION_TOOL_LIMIT_EXCEEDED` | failed | 模型请求第二次工具调用 |
| `PORTFOLIO_DECISION_TOOL_RESULT_MISMATCH` | abstained | 输出未绑定本次 result hash |
| `PORTFOLIO_DECISION_OUTPUT_INVALID` | abstained | schema、证据或语义校验失败 |
| `PORTFOLIO_DECISION_MODEL_FAILED` | failed | provider 显式失败或超时 |
| `PORTFOLIO_DECISION_PERSISTENCE_UNCERTAIN` | failed | provider 可能已成功但持久化不确定，禁止重放 |

所有失败都保持 `execution_locked=true`，不创建旧方案、Shadow、order 或 fill。

## 10. 验收标准

1. 合格真实冻结组合通过正常产品入口创建一个 run，Agent 恰好调用一次情景工具并输出可读意图。
2. 工具输出不含逐标的 ID、symbol、权重、数量或订单字段。
3. 模型提交权重、证券、数量、额外字段或第二次工具调用时失败关闭。
4. `met|improved_not_met` 才可生成 `explore_adjustment`；`unchanged|infeasible` 只生成 `observe`。
5. 成本未知时意图可完成，但 `limitations` 必须展示该未知，且交易锁不变。
6. 相同用户、幂等 key 和输入只创建一个 run/Job；不同输入返回 409。
7. 断流或页面关闭后通过 GET 恢复已持久化结果；已有 output 时不再调用模型。
8. provider 成功但持久化状态不确定时不重放。
9. 对首次请求前、首次请求后、工具投影提交前后、第二次请求前后和最终提交前后的每个崩溃窗口执行 PostgreSQL 回归；租约回收后 run/output/reservation 均进入确定终态且模型调用不重放。
10. 页面加载、刷新、组合切换和快照更新不触发模型 POST；只有明确点击创建一次 run。
11. 组合快照、诊断或情景哈希变更后，页面不展示旧 Agent 意图。
12. 正常完成、abstained 和 failed 都精确结算或释放预留预算。
13. 相同 raw POST 与幂等 key 在服务端权威变化后仍重放既有 run；只有 raw POST 改变才返回 409。
14. 非 run 所有者 GET 统一返回 404；BFF 拒绝与当前页面作用域不匹配的响应。
15. 每个 unknown 都有同 code 的结构化 limitation；所有无意义的 signal/operator/unit/threshold 组合被拒绝。
16. 真实验收前后，旧 proposal group、Shadow、paper order 和 paper fill 均为零增量。
17. 不调用 Provider、券商刷新、外部数据源、Shadow 或交易入口。
18. 新迁移可从 0047 升级到新 head，可在满足回滚前置条件后降级回 0047；历史研究和旧三方案仍可读。

## 11. 文件边界

新建：

- `engine/src/nexus/agents/portfolio_decision_contracts.py`
- `engine/src/nexus/agents/portfolio_decision_tools.py`
- `engine/src/nexus/agents/portfolio_decision_model.py`
- `engine/src/nexus/agents/portfolio_decision_persistence.py`
- `engine/src/nexus/agents/portfolio_decision_execution.py`
- `engine/src/nexus/agents/portfolio_decision_budget.py`
- `engine/src/nexus/agents/portfolio_decision_worker.py`
- `engine/src/nexus/agents/prompts/portfolio-decision-v1.md`
- `engine/src/nexus/api/routes/portfolio_decisions.py`
- `engine/src/nexus/api/schemas/portfolio_decisions.py`
- `engine/alembic/versions/0048_portfolio_decision_runs.py`
- `app/api/v1/portfolio-decision-runs/route.ts`
- `app/api/v1/portfolio-decision-runs/[runId]/route.ts`
- `app/components/portfolio-agent-interpretation.tsx`
- `app/lib/portfolio-decisions/bff.ts`
- `engine/tests/unit/agents/test_portfolio_decision_contracts.py`
- `engine/tests/unit/agents/test_portfolio_decision_tools.py`
- `engine/tests/unit/agents/test_portfolio_decision_model.py`
- `engine/tests/integration/test_portfolio_decision_persistence_pg.py`
- `engine/tests/integration/test_portfolio_decision_worker_pg.py`
- `engine/tests/integration/test_portfolio_decision_api.py`
- `tests/unit/engine/portfolio-decision-routes.test.ts`
- `tests/unit/portfolio/portfolio-agent-interpretation.test.tsx`
- `tests/e2e/portfolio-agent-interpretation.spec.ts`

修改：

- `engine/src/nexus/persistence/models/agents.py`
- `engine/src/nexus/agents/persistence.py`
- `engine/src/nexus/persistence/models/__init__.py`
- `engine/src/nexus/jobs/models.py`
- `engine/src/nexus/jobs/repository.py`
- `engine/src/nexus/persistence/models/diagnostics.py`
- `engine/src/nexus/persistence/migrations.py`
- `engine/tests/helpers/migration_schema_expectations.py`
- `engine/src/nexus/worker.py`
- `engine/src/nexus/main.py`
- `engine/src/nexus/ops/openai_local.py`
- `app/lib/engine/client.ts`
- `app/lib/engine/errors.ts`
- `app/lib/optimization/bff.ts`
- `app/components/portfolio-scenario.tsx`
- `contracts/openapi.json`
- `app/lib/generated/schema.d.ts`
- `app/lib/generated/runtime-contract.ts`

不修改：

- `engine/src/nexus/optimizer/scenario_engine.py` 的算法语义；
- 旧方案、Shadow、paper 和 broker 模块；
- `.openai/hosting.json`、凭据、数据套餐和外部权限。

## 12. 兼容、回滚与停止条件

- 迁移添加三张表和相关索引，扩展 prompt role check，并为诊断表添加组合外键所需的唯一约束；不改旧业务行。代码侧添加一个 Job type。日预算查询同时统计原 Agent 和组合决策的预留/已结算金额。
- 回滚前先停止创建新 run，执行租约回收与孤儿 reconcile，并要求所有组合决策 run 终态、没有 `reserved` 预算。若当天存在已结算组合决策费用，则等到下一个 UTC 预算日或保留表，不得直接降级造成当日预算少计。
- downgrade 先拒绝任何非终态 run 或开放 reservation，再删除三张新表；随后删除 `role="portfolio_decision"` 的专用 prompt 行、恢复旧 prompt role check，最后删除诊断组合唯一约束。完成数据库降级后才回滚应用代码到 0047 兼容版本。
- 如果无法在不向模型暴露权重的前提下生成有用解读，停在阶段 B 评审，不扩展工具输出。
- 如果运行恢复需要 provider 自动重放或 `store=true`，停止实施并重新评审数据与费用边界。
- 如果页面需要“接受”或 Shadow 才能被认为可用，该需求属于阶段 C，不在本阶段内暗含实现。
