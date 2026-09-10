# Portfolio Agent Scenario Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 交付一个组合级 durable Agent run，使模型只能调用一次确定性集中度情景工具并输出可审计、永久锁定的研究解读。

**Architecture:** 使用独立于单标的研究的组合决策 run/output/budget 表，并在现有 general Worker 进程内运行一个窄化的短事务 worker。模型只看到组合聚合诊断、不透明 citation ID 和无逐标权重的工具投影；阶段 A 情景引擎仍是权重与风险计算的唯一权威。

**Tech Stack:** Python 3.12、FastAPI、Pydantic v2、SQLAlchemy async、PostgreSQL/Alembic、OpenAI Responses custom functions、TypeScript、React/vinext、Vitest、Playwright。

**Spec:** `docs/superpowers/specs/2026-09-10-portfolio-agent-scenario-design.md`

## Global Constraints

- 只支持 `objective="reduce_security_concentration"`、`universe_policy="current_holdings_only"` 和 `allow_new_positions=false`。
- 一个 durable run 恰好执行一个工具调用，最多两个 provider transport turn，绝不自动重试。
- 模型不可看到或输出逐标的 ID、symbol、当前/目标权重、数量、金额、订单或执行准入。
- `execution_locked=true` 在所有成功、abstained 和 failed 结果中保持不变。
- 不创建 Mandate、scenario、selection 或 Shadow 业务表，不修改旧三方案和交易状态机。
- 所有 provider 调用位于数据库事务外；checkpoint 和终结统一锁序 `job -> run -> output -> reservation`。
- 页面加载、刷新和恢复只能 GET；只有用户明确点击才能 POST 创建 run。

---

## File map

- `portfolio_decision_contracts.py`：冻结输入、模型上下文、工具投影、研究意图及语义校验。
- `portfolio_decision_tools.py`：单工具 registry、服务端 authority binding、情景结果安全投影。
- `portfolio_decision_model.py`：两轮 Responses 适配器、共享额度/deadline、严格输出解析。
- `portfolio_decision_persistence.py`：准入、HTTP 幂等、权威冻结、Job 入队和只读加载。
- `portfolio_decision_budget.py`：跨单标/组合 run 的同日共享预算汇总、预留、结算和释放。
- `portfolio_decision_execution.py`：checkpoint 业务 apply、终态校验和 output 写入。
- `portfolio_decision_worker.py`：专用 claim、事务外 provider 调用、租约回收和孤儿 reconcile。
- `portfolio_decisions.py`：Engine POST/GET。
- `app/lib/portfolio-decisions/bff.ts`：BFF 请求/响应校验和作用域绑定。
- `portfolio-agent-interpretation.tsx`：显式触发、只读恢复、轮询和展示。

---

### Task 1: 闭合合同、持久化模型与迁移

**Files:**
- Create: `engine/src/nexus/agents/portfolio_decision_contracts.py`
- Create: `engine/src/nexus/agents/prompts/portfolio-decision-v1.md`
- Create: `engine/alembic/versions/0048_portfolio_decision_runs.py`
- Modify: `engine/src/nexus/persistence/models/agents.py`
- Modify: `engine/src/nexus/persistence/models/diagnostics.py`
- Modify: `engine/src/nexus/persistence/models/__init__.py`
- Modify: `engine/src/nexus/persistence/migrations.py`
- Modify: `engine/tests/helpers/migration_schema_expectations.py`
- Test: `engine/tests/unit/agents/test_portfolio_decision_contracts.py`
- Test: `engine/tests/integration/test_portfolio_decision_migration.py`

**Interfaces:**
- Produces: `PortfolioDecisionInputV1`、`PortfolioDecisionModelContextV1`、`AgentScenarioProjectionV1`、`PortfolioResearchIntentV1`。
- Produces: `PortfolioDecisionRunRecord`、`PortfolioDecisionOutputRecord`、`PortfolioDecisionBudgetReservationRecord`。
- Produces: migration head `0048_portfolio_decision_runs`。

- [ ] **Step 1: 写失败的合同测试**

覆盖二进制 float、extra fields、逐标字段、无意义 invalidation 组合、unknown/limitation 不等、scenario status/assessment 不一致、citation 子集错误和非 UTC 时间。核心断言：

```python
def test_intent_requires_one_limitation_per_unknown() -> None:
    payload = valid_intent()
    payload["limitations"] = []
    with pytest.raises(ValidationError):
        PortfolioResearchIntentV1.model_validate(
            payload,
            context={"unknown_metrics": ("estimated_cost",), "citation_ids": payload["citation_ids"]},
        )

def test_state_condition_rejects_numeric_operator() -> None:
    payload = valid_condition(signal="portfolio.scenario_objective_status")
    payload.update(operator="gt", threshold="met", unit="state")
    with pytest.raises(ValidationError):
        PortfolioInvalidationConditionV1.model_validate(payload)
```

- [ ] **Step 2: 运行合同测试确认失败**

Run:

```bash
cd engine
uv run pytest tests/unit/agents/test_portfolio_decision_contracts.py -q --import-mode=importlib
```

Expected: FAIL，因为合同模块尚不存在。

- [ ] **Step 3: 实现封闭合同**

实现精确类型和 `model_validator`。提供以下稳定入口：

```python
def canonical_portfolio_decision_hash(value: BaseModel | Mapping[str, object]) -> str: ...

def validate_portfolio_intent(
    intent: PortfolioResearchIntentV1,
    *,
    frozen: PortfolioDecisionInputV1,
    projection: AgentScenarioProjectionV1,
) -> PortfolioResearchIntentV1: ...
```

`validate_portfolio_intent` 必须校验 ID/时间/目标/result hash、citation 子集、assessment/status、unknown/limitation 和 `execution_locked`。

- [ ] **Step 4: 写失败的迁移测试**

测试从 0047 upgrade 到 0048 后：

- 三张新表、唯一约束、组合外键、JSONB/check constraints 存在；
- prompt role check 接受 `portfolio_decision`，旧 role 仍接受，任意 role 拒绝；
- 诊断表存在 `(id, portfolio_snapshot_id)` 唯一约束；
- downgrade 在存在非终态 run 或 reserved 预算时拒绝；
- 清空阻塞并跨 UTC 预算日后可降级到 0047。

- [ ] **Step 5: 实现 ORM、prompt 和迁移**

`PortfolioDecisionRunRecord` 必须包含 `request_hash/input_hash/schema_hash/model_hash/prompt_hash/tool_hash/evidence_hash/provider_phase`；run 与 snapshot/diagnostic 使用组合外键。迁移只插入 schema，不自动插入 prompt 内容；`openai_local` 在 Task 3 按内容哈希幂等导入 prompt。

- [ ] **Step 6: 验证 Task 1**

Run:

```bash
cd engine
uv run pytest tests/unit/agents/test_portfolio_decision_contracts.py tests/integration/test_portfolio_decision_migration.py -q --import-mode=importlib
uv run ruff check src/nexus/agents/portfolio_decision_contracts.py src/nexus/persistence/models/agents.py src/nexus/persistence/models/diagnostics.py tests/unit/agents/test_portfolio_decision_contracts.py
uv run mypy src/nexus/agents/portfolio_decision_contracts.py
uv run alembic heads
```

Expected: tests PASS；唯一 head 为 `0048_portfolio_decision_runs`。

- [ ] **Step 7: 提交 Task 1**

```bash
git add engine/src/nexus/agents/portfolio_decision_contracts.py engine/src/nexus/agents/prompts/portfolio-decision-v1.md engine/src/nexus/persistence/models engine/src/nexus/persistence/migrations.py engine/alembic/versions/0048_portfolio_decision_runs.py engine/tests/helpers/migration_schema_expectations.py engine/tests/unit/agents/test_portfolio_decision_contracts.py engine/tests/integration/test_portfolio_decision_migration.py
git commit -m "feat: define durable portfolio decision contracts"
```

### Task 2: 单工具 registry 与安全投影

**Files:**
- Create: `engine/src/nexus/agents/portfolio_decision_tools.py`
- Test: `engine/tests/unit/agents/test_portfolio_decision_tools.py`

**Interfaces:**
- Consumes: Task 1 contracts；现有 `evaluate_adjustment_scenario(...)`。
- Produces: `PortfolioDecisionToolRegistry.execute(name, payload)`。
- Produces: `build_portfolio_decision_tools(session, run) -> PortfolioDecisionToolRegistry`。

- [ ] **Step 1: 写失败的工具测试**

覆盖：registry 只有 `evaluate_adjustment_scenario`；输入只能有 objective；authority 从 run 而不是 payload 加载；重算结果必须匹配 expected hash；投影不含 positions、instrument ID、symbol、current/target weight；相同 content hash 的两条证据获得不同且可反解 citation ID。

```python
async def test_projection_never_exposes_position_identity() -> None:
    tools = build_fake_tools(evaluation=fixture_evaluation())
    result = await tools.execute(
        "evaluate_adjustment_scenario",
        {"objective": "reduce_security_concentration"},
    )
    wire = json.dumps(result, sort_keys=True)
    for forbidden in ("positions", "instrument_id", "current_weight", "target_weight", "AAPL"):
        assert forbidden not in wire
```

另写独立负例传入错误 objective，断言在 executor 调用前抛出 `ToolInputError`。

- [ ] **Step 2: 运行测试确认失败**

```bash
cd engine
uv run pytest tests/unit/agents/test_portfolio_decision_tools.py -q --import-mode=importlib
```

- [ ] **Step 3: 实现 registry、authority loader 和投影**

公开签名：

```python
class PortfolioDecisionToolRegistry(Protocol):
    names: tuple[str, ...]
    async def execute(self, name: str, payload: Mapping[str, object]) -> Mapping[str, object]: ...

async def build_portfolio_decision_tools(
    session: AsyncSession,
    run: PortfolioDecisionRunRecord,
) -> PortfolioDecisionToolRegistry: ...

def project_scenario_for_agent(
    evaluation: PortfolioScenarioEvaluationV2,
    *,
    citation_ids: tuple[str, ...],
) -> AgentScenarioProjectionV1: ...
```

使用精确 key allowlist 二次扫描序列化投影；`cash_weight` 允许，逐标 current/target weight 禁止。

- [ ] **Step 4: 验证并提交 Task 2**

```bash
cd engine
uv run pytest tests/unit/agents/test_portfolio_decision_tools.py tests/unit/optimizer/test_scenario_engine.py -q --import-mode=importlib
uv run ruff check src/nexus/agents/portfolio_decision_tools.py tests/unit/agents/test_portfolio_decision_tools.py
uv run mypy src/nexus/agents/portfolio_decision_tools.py
git add src/nexus/agents/portfolio_decision_tools.py tests/unit/agents/test_portfolio_decision_tools.py
git commit -m "feat: expose bounded portfolio scenario tool"
```

### Task 3: 准入、幂等与共享预算

**Files:**
- Create: `engine/src/nexus/agents/portfolio_decision_budget.py`
- Create: `engine/src/nexus/agents/portfolio_decision_persistence.py`
- Modify: `engine/src/nexus/agents/persistence.py`
- Modify: `engine/src/nexus/jobs/models.py`
- Modify: `engine/src/nexus/ops/openai_local.py`
- Test: `engine/tests/integration/test_portfolio_decision_persistence_pg.py`

**Interfaces:**
- Produces: `admit_portfolio_decision_run(...) -> PortfolioDecisionAdmission`。
- Produces: `reserve/settle/release_portfolio_decision_budget(...)`。
- Produces: `JobType.AGENT_PORTFOLIO_DECISION`。

- [ ] **Step 1: 写 PostgreSQL 失败测试**

覆盖：

- raw request hash 相同则重放既有 run，即使数据库中新建了更晚诊断；
- 同 key、不同 raw body 返回 `IdempotencyConflict`；
- expected scenario result hash 不匹配时不创建 run/Job/reservation；
- 单标研究预留与组合决策预留在同一 advisory lock 下共享总额，并发只能一个成功；
- prompt 必须是 `role=portfolio_decision/version=portfolio-decision-v1`；
- model `max_tool_calls=0` 时拒绝；
- frozen input、evidence、schema/model/prompt/tool hash 可重算。

- [ ] **Step 2: 实现共享预算汇总**

保持旧 reservation 表不变，新增：

```python
async def used_agent_budget_for_day(session: AsyncSession, business_date: date) -> Decimal: ...

async def reserve_portfolio_decision_budget(
    session: AsyncSession,
    *,
    run_id: UUID,
    now: datetime,
    daily_limit_usd: Decimal,
    worst_case_usd: Decimal,
    pricing_version: str,
) -> PortfolioDecisionBudgetReservationRecord: ...
```

修改旧 `reserve_daily_budget`，使其在同一 `agent-budget:<date>` 锁内调用共享汇总；不改变旧结算/释放接口。

- [ ] **Step 3: 实现准入**

顺序固定为：

1. 用 actor/route/key/raw request hash 调用 `claim_or_lock`；
2. 非 owner 直接加载既有 run；
3. owner 才加载 snapshot/diagnostic/policy/identities/evidence；
4. 重算阶段 A 结果并比较 expected hash；
5. 精确选择 prompt/model 并计算 frozen `input_hash` 与 Job key；
6. 插入 run、Job、reservation 和 idempotency response。

- [ ] **Step 4: 导入专用 prompt**

扩展 `openai_local.py` 的幂等 prompt 导入，使文件内容哈希与数据库记录一致；不修改旧 `ROLE_ORDER`。

- [ ] **Step 5: 验证并提交 Task 3**

```bash
cd engine
uv run pytest tests/integration/test_portfolio_decision_persistence_pg.py tests/integration/test_agent_persistence_pg.py -q --import-mode=importlib
uv run ruff check src/nexus/agents/portfolio_decision_budget.py src/nexus/agents/portfolio_decision_persistence.py src/nexus/agents/persistence.py src/nexus/ops/openai_local.py
uv run mypy src/nexus/agents/portfolio_decision_budget.py src/nexus/agents/portfolio_decision_persistence.py
git add src/nexus/agents/portfolio_decision_budget.py src/nexus/agents/portfolio_decision_persistence.py src/nexus/agents/persistence.py src/nexus/jobs/models.py src/nexus/ops/openai_local.py tests/integration/test_portfolio_decision_persistence_pg.py
git commit -m "feat: admit bounded portfolio decision runs"
```

### Task 4: 两轮模型适配器与短事务 Worker

**Files:**
- Create: `engine/src/nexus/agents/portfolio_decision_model.py`
- Create: `engine/src/nexus/agents/portfolio_decision_execution.py`
- Create: `engine/src/nexus/agents/portfolio_decision_worker.py`
- Modify: `engine/src/nexus/jobs/repository.py`
- Modify: `engine/src/nexus/worker.py`
- Test: `engine/tests/unit/agents/test_portfolio_decision_model.py`
- Test: `engine/tests/integration/test_portfolio_decision_worker_pg.py`

**Interfaces:**
- Produces: `checkpoint_job(session, claim, now, apply, extend_seconds=30)`。
- Produces: `PortfolioDecisionResponsesModel.run(context, tools) -> PortfolioDecisionModelResult`。
- Produces: `run_portfolio_decision_worker_once(factory, ...) -> bool`。
- Produces: `reconcile_orphaned_portfolio_decisions(session, now) -> int`。

- [x] **Step 1: 写模型适配器失败测试**

使用 injected transport 覆盖：

- 第一轮必须恰好一个函数调用；
- 零次调用为 `TOOL_REQUIRED`；
- 多个调用或第二轮调用为 `TOOL_LIMIT_EXCEEDED`；
- 两轮共享总 token allowance 与 45 秒 deadline；
- 第二轮 `tool_choice=none` 且严格 schema；
- result hash、citation 与 unknown 语义无效时 abstained；
- provider 超时不重试。

- [x] **Step 2: 实现模型适配器**

```python
@dataclass(frozen=True)
class PortfolioDecisionModelResult:
    intent: PortfolioResearchIntentV1
    projection: AgentScenarioProjectionV1
    input_tokens: int
    output_tokens: int
    cost_usd: Decimal

class PortfolioDecisionResponsesModel:
    async def run(
        self,
        context: PortfolioDecisionModelContextV1,
        tools: PortfolioDecisionToolRegistry,
    ) -> PortfolioDecisionModelResult: ...
```

provider transport 调用之间不得持有 `AsyncSession` 或数据库事务。

- [x] **Step 3: 写 checkpoint 与崩溃窗口失败测试**

在以下注入点抛出异常并提交真实 PostgreSQL 状态：

- claim 后、checkpoint 1 前；
- checkpoint 1 后、第一轮发送前/后；
- 工具执行后、checkpoint 2 前/后；
- checkpoint 3 后、第二轮发送前/后；
- output 写入前。

每个用例先令 lease 过期并执行 reclaim/reconcile，再断言唯一 output、run/Job 终态、预算状态和 transport 调用次数不增加。

- [x] **Step 4: 实现 fenced checkpoint**

在 `jobs/repository.py` 新增：

```python
async def checkpoint_job(
    session: AsyncSession,
    *,
    claim: ClaimedJob,
    now: datetime,
    apply: JobApply,
    extend_seconds: int = 30,
) -> None:
    job, lease, _ = await _lock_active_claim(session, claim=claim, now=now)
    await apply(session)
    lease.heartbeat_at = now
    lease.expires_at = max(lease.expires_at, now + timedelta(seconds=extend_seconds))
    job.version += 1
    await session.flush()
```

`apply` 内统一按 `run -> output -> reservation` 顺序；由于 helper 已先锁 Job，总顺序保持一致。

- [x] **Step 5: 实现专用 Worker 与 reconcile**

`run_worker` 每轮让专用组合 worker 与现有 general registry 各处理至多一个 Job，避免任一 backlog 饿死另一类任务。现有 registry 不注册新 Job type。

终结规则：

- completed/abstained：`finalize_job`；
- 活动 lease failed：`execute_and_finalize_job(outcome="failed")`；
- terminal Job orphan：锁 Job 后直接终结业务行；
- `provider_phase=none` release；
- `tool_recorded` settle 已知首轮 usage；
- `first_requested|second_requested` settle full reservation。

- [x] **Step 6: 验证并提交 Task 4**

```bash
cd engine
uv run pytest tests/unit/agents/test_portfolio_decision_model.py tests/integration/test_portfolio_decision_worker_pg.py tests/integration/test_daily_research_recovery.py -q --import-mode=importlib
uv run ruff check src/nexus/agents/portfolio_decision_model.py src/nexus/agents/portfolio_decision_execution.py src/nexus/agents/portfolio_decision_worker.py src/nexus/jobs/repository.py src/nexus/worker.py
uv run mypy src/nexus/agents/portfolio_decision_model.py src/nexus/agents/portfolio_decision_execution.py src/nexus/agents/portfolio_decision_worker.py
git add src/nexus/agents/portfolio_decision_model.py src/nexus/agents/portfolio_decision_execution.py src/nexus/agents/portfolio_decision_worker.py src/nexus/jobs/repository.py src/nexus/worker.py tests/unit/agents/test_portfolio_decision_model.py tests/integration/test_portfolio_decision_worker_pg.py
git commit -m "feat: execute recoverable portfolio decision runs"
```

### Task 5: Engine API、OpenAPI 与 BFF

**Files:**
- Create: `engine/src/nexus/api/schemas/portfolio_decisions.py`
- Create: `engine/src/nexus/api/routes/portfolio_decisions.py`
- Create: `engine/tests/integration/test_portfolio_decision_api.py`
- Create: `app/lib/portfolio-decisions/bff.ts`
- Create: `app/api/v1/portfolio-decision-runs/route.ts`
- Create: `app/api/v1/portfolio-decision-runs/[runId]/route.ts`
- Create: `tests/unit/engine/portfolio-decision-routes.test.ts`
- Modify: `engine/src/nexus/main.py`
- Modify: `app/lib/engine/client.ts`
- Modify: `app/lib/engine/errors.ts`
- Modify: `contracts/openapi.json`
- Modify: `app/lib/generated/schema.d.ts`
- Modify: `app/lib/generated/runtime-contract.ts`

**Interfaces:**
- Produces: `POST /internal/v1/portfolio-decision-runs`、`GET /internal/v1/portfolio-decision-runs/{run_id}`。
- Produces: matching BFF routes.

- [x] **Step 1: 写 Engine API 失败测试**

覆盖 202/Location、幂等 replay、raw-body 冲突、strict 422、authority 409、budget 409、actor mismatch 404、GET output citation resolution，以及旧 proposal/Shadow/order/fill 零增量。

- [x] **Step 2: 实现 Engine schemas/routes**

POST 直接读取 bounded raw body 后再 Pydantic 校验，以 raw bytes 生成 `request_hash`。GET 查询必须带 `run.id == run_id` 与 `run.actor_id == actor.actor_id`；不存在统一 404。

- [x] **Step 3: 写并实现 BFF 测试**

Vitest 必须断言：

- extra/weight/security fields 在 Engine 调用前拒绝；
- HMAC 路径模板允许新 POST/GET；
- 只转发稳定错误码；
- GET body 的 run/snapshot/diagnostic/expected-result hash 与请求作用域不一致时返回安全错误；
- 不透传 provider 原始错误。

- [x] **Step 4: 生成并核验合同**

```bash
npm run generate:client
npm run check:generated
```

该脚本通过当前 `create_engine_app(EngineSettings.for_test()).openapi()` 生成 OpenAPI、TypeScript schema 和 runtime contract。

- [x] **Step 5: 验证并提交 Task 5**

```bash
cd engine
uv run pytest tests/integration/test_portfolio_decision_api.py -q --import-mode=importlib
cd ..
npm test -- tests/unit/engine/portfolio-decision-routes.test.ts tests/unit/engine/client-security.test.ts
npm run check:generated
git add engine/src/nexus/api engine/src/nexus/main.py engine/tests/integration/test_portfolio_decision_api.py app/api/v1/portfolio-decision-runs app/lib/portfolio-decisions app/lib/engine contracts/openapi.json app/lib/generated tests/unit/engine/portfolio-decision-routes.test.ts
git commit -m "feat: expose portfolio decision runs"
```

### Task 6: 组合页面 Agent 解读

**Files:**
- Create: `app/components/portfolio-agent-interpretation.tsx`
- Create: `tests/unit/portfolio/portfolio-agent-interpretation.test.tsx`
- Create: `tests/e2e/portfolio-agent-interpretation.spec.ts`
- Modify: `app/components/portfolio-scenario.tsx`
- Modify: `app/styles/tokens.css`

**Interfaces:**
- Consumes: 阶段 A `PortfolioScenarioResponse` 和 Task 5 BFF。
- Produces: explicit-trigger Agent interpretation panel.

- [x] **Step 1: 写组件失败测试**

覆盖：

- 无阶段 A 结果时不显示可用按钮；
- stale/snapshot/diagnostic/result hash 改变时清空旧结果；
- render/refresh 不 POST；
- 点击只 POST 一次，运行中禁用；
- localStorage 只保存 key/run ID，恢复只 GET；
- late response 的作用域不匹配时丢弃；
- 页面不出现接受、执行、Shadow 文案；
- unknown、citation、失效条件和锁定状态可见。

- [x] **Step 2: 实现组件**

组件 props 必须包含完整作用域：

```typescript
type PortfolioAgentInterpretationProps = {
  portfolioId: string;
  snapshotId: string;
  diagnosticReportId: string;
  scenarioRequest: components["schemas"]["PortfolioScenarioRequest"];
  scenarioResult: components["schemas"]["PortfolioScenarioResponse"];
  stale: boolean;
};
```

轮询最多 45 秒，退避间隔限制在 500ms 到 3000ms；超时只停止观察并保留 run ID，不把 run 标为失败。

- [x] **Step 3: 写 Playwright 副作用测试**

拦截所有请求并断言：

- 初始加载、reload 和 localStorage 恢复没有 decision POST；
- 明确点击只产生一次 decision POST；
- 没有 Provider、broker refresh、Shadow、paper order/fill 请求；
- GET 返回旧 result hash 时页面不展示该输出。

- [x] **Step 4: 验证并提交 Task 6**

```bash
npm test -- tests/unit/portfolio/portfolio-agent-interpretation.test.tsx tests/unit/portfolio/portfolio-scenario.test.tsx tests/unit/portfolio/page.test.tsx
npm run typecheck
npm run lint
npm run test:e2e -- tests/e2e/portfolio-agent-interpretation.spec.ts --workers=1
git add app/components/portfolio-agent-interpretation.tsx app/components/portfolio-scenario.tsx app/styles/tokens.css tests/unit/portfolio/portfolio-agent-interpretation.test.tsx tests/e2e/portfolio-agent-interpretation.spec.ts
git commit -m "feat: show locked portfolio agent interpretation"
```

### Task 7: 集成回归、受控迁移与真实只读验收

**Files:**
- Create: `engine/tests/integration/test_portfolio_decision_e2e.py`
- Modify: `docs/plans/2026-09-02-core-evidence-checkpoint.md`
- Modify: `docs/superpowers/reviews/2026-09-10-portfolio-agent-scenario-review.md`

**Interfaces:**
- Consumes: Tasks 1-6 complete system.
- Produces: one real frozen portfolio acceptance record; no execution eligibility.

- [x] **Step 1: 写跨层集成测试**

通过正常 Engine admission 创建 run，injected deterministic Responses transport 发出一次工具调用和一次严格 intent。断言：

- exactly two provider turns / one tool execution；
- persisted run/output/citation/budget hashes match；
- Agent never receives positions or symbols；
- GET/BFF/UI show `explore_adjustment` 或 `observe` with limitations；
- `execution_locked=true`；
- proposal group、Shadow、paper order/fill 均零增量。

- [x] **Step 2: 运行聚焦后端回归**

```bash
cd engine
uv run pytest tests/unit/agents/test_portfolio_decision_contracts.py tests/unit/agents/test_portfolio_decision_tools.py tests/unit/agents/test_portfolio_decision_model.py tests/integration/test_portfolio_decision_migration.py tests/integration/test_portfolio_decision_persistence_pg.py tests/integration/test_portfolio_decision_worker_pg.py tests/integration/test_portfolio_decision_api.py tests/integration/test_portfolio_decision_e2e.py -q --import-mode=importlib
uv run ruff check src/nexus/agents src/nexus/jobs src/nexus/api tests/unit/agents/test_portfolio_decision_contracts.py tests/unit/agents/test_portfolio_decision_tools.py tests/unit/agents/test_portfolio_decision_model.py
uv run mypy src/nexus/agents/portfolio_decision_contracts.py src/nexus/agents/portfolio_decision_tools.py src/nexus/agents/portfolio_decision_model.py src/nexus/agents/portfolio_decision_persistence.py src/nexus/agents/portfolio_decision_execution.py src/nexus/agents/portfolio_decision_worker.py
```

- [x] **Step 3: 运行聚焦前端回归**

```bash
npm test -- tests/unit/engine/portfolio-decision-routes.test.ts tests/unit/portfolio/portfolio-agent-interpretation.test.tsx tests/unit/portfolio/portfolio-scenario.test.tsx tests/unit/portfolio/page.test.tsx
npm run check:generated
npm run typecheck
npm run lint
npm run build
```

- [x] **Step 4: 执行迁移预检与共享服务更新**

先确认当前 head、活动 Worker 版本和无未终态组合 run，再 upgrade 到当前唯一 head（现为 `0049_pd_terminal_phases`）。只重建需要新 Engine 代码的 API/general Worker；不重启 Provider、Fosun Gateway 或 broker 服务，不触发数据刷新或模型调用。

2026-09-10 实际执行时，`main` 快进到 `a2ff1e21`；共享库在受保护备份并验证可读后由
`0047_alpaca_market_stream` 事务升级到 `0049_pd_terminal_phases`。只重建并替换 API，数据库和
MinIO 未重启；当前 general Worker 原本未运行且配置为 `fixture`，因此没有为凑齐服务而启动它。
新 API 已显示组合决策路由；带完整 scope 的 BFF GET 经 HMAC 到达 Engine 并对不存在资源返回稳定
`404 NOT_FOUND`。三张新表及 Shadow、模拟订单、成交均为零写入。

- [ ] **Step 5: 执行一个真实只读验收**

使用当前真实冻结组合与已验收的阶段 A request/result hash，明确创建一个组合决策 run。模型调用属于现有套餐内单次研究调用；若凭据或额度不可用，保留 injected transport 的跨层验收并如实记录外部阻塞，不循环重试。

核验：

- run/Job/output/reservation 持久化；
- 页面实际可见研究解读、unknown、citations 和锁；
- 页面加载/刷新无 POST；
- Provider、broker refresh、Shadow、orders、fills 无调用；
- DB 中 proposal group、Shadow、paper order/fill 验收前后零增量。

- [x] **Step 6: 更新证据并最终审查**

在现有 checkpoint 和 review 文件中记录用户可见结果、运行版本、验证命令、真实阻塞和回滚条件。执行一次代码审查，任何资金、预算、fence、幂等或隐私 Important 必须修复并定向复验。

- [x] **Step 7: 提交 Task 7**

```bash
git add engine/src/nexus/agents/portfolio_decision_tools.py engine/tests/unit/agents/test_portfolio_decision_tools.py engine/tests/integration/test_portfolio_decision_e2e.py docs/plans/2026-09-02-core-evidence-checkpoint.md docs/superpowers/reviews/2026-09-10-portfolio-agent-scenario-review.md
git commit -m "fix: verify portfolio agent scenario loop"
```

## Stop conditions

- 任一测试证明模型能看到逐标身份或权重：停止，不扩大投影。
- 短事务 checkpoint 无法在现有 lease/fence 下安全实现：停止，不退回长事务或自动重试。
- 需要 `store=true`、provider 自动重放或新外部权限：停止并重新评审。
- 真实运行缺模型凭据/额度：只报告外部阻塞，不修改安全边界、不重复调用。
- 任何实现提出 Mandate、选择、Shadow 或交易入口：移出本计划，留给阶段 C。

## Self-review checklist

- Spec coverage：7 个任务覆盖合同、迁移、工具投影、预算/幂等、两轮模型、崩溃恢复、API/BFF、UI 和真实验收。
- 完整性扫描：每个实现步骤都给出精确文件、接口、验证命令和预期结果。
- Type consistency：所有任务统一使用 `diagnostic_report_id`、`diagnostic_input_hash`、`expected_scenario_result_hash`、`explore_adjustment` 和 `citation_ids`。
- Scope：没有 Mandate、scenario persistence、selection、Shadow、自动调度或交易。
