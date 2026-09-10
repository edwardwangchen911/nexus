# NEXUS 阶段 A 组合调整情景 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 通过正常 Engine/BFF/组合页路径，为一个合格的真实冻结组合确定性生成单个“当前持仓内降低单名集中度”候选，不持久化、不调用 Agent、不触发 Shadow 或交易。

**Architecture:** 请求只携带冻结快照、冻结诊断、时间和请求级约束；Engine 从数据库加载权威输入，调用纯 `Decimal` 引擎并返回临时结果。页面仅在用户明确点击后请求计算，结果只保存在组件内存中；现有固定三方案链路保持只读兼容。

**Tech Stack:** Python 3.12、Pydantic、FastAPI、SQLAlchemy 只读查询、vinext/React 19、TypeScript、Vitest、Pytest、Playwright。

**Spec:** `docs/superpowers/specs/2026-09-10-portfolio-scenario-decision-design.md`

## Global Constraints

- 首轮只实现阶段 A；不得实现 Mandate 表、候选表、Agent、Job、选择或 Shadow 适配。
- 不新增依赖，不新增数据库迁移，不修改 `.openai/hosting.json`。
- `execution_locked=true`，请求不得接受权重、证券列表、数量、金额或订单。
- 只允许当前正持仓减仓或保持，差额进入现金；不得新增、增持、做空或杠杆。
- 估值域固定为 `currency:USD`；只纳入身份权威确认的 USD 标准美股，`00656.HK` 不换算、不调整。集中度是该域 security-only 分母；现金腿计入单边换手。
- 全部权重用 `Decimal`，API 用十进制字符串，调整步长固定为 `0.0001`。
- 页面加载和刷新只读；只有明确点击才调用本地计算 POST，且不得调用 Provider、模型、Shadow 或交易入口。
- unknown 保持 unknown；成本未知仍可分析，但不得显示为可执行或推荐。

## 文件映射

| 文件 | 责任 |
|---|---|
| `engine/src/nexus/optimizer/scenario_contracts.py` | 封闭的阶段 A 请求约束、指标、仓位和结果模型 |
| `engine/src/nexus/optimizer/scenario_engine.py` | 无 I/O、无 UUID、无时钟读取的确定性算法 |
| `engine/src/nexus/api/schemas/optimization.py` | API 请求与响应 envelope |
| `engine/src/nexus/api/routes/optimization.py` | 边界校验、权威输入加载、纯引擎调用和审计摘要 |
| `app/api/v1/portfolio-scenarios/evaluate/route.ts` | BFF 严格校验和 Engine 转发 |
| `app/lib/optimization/bff.ts` | 复用现有 exact JSON 边界并新增请求断言 |
| `app/lib/engine/client.ts`、`app/lib/engine/errors.ts` | 封闭路径与响应合同验证 |
| `app/components/portfolio-scenario.tsx` | 请求级约束输入与单候选比较 |
| `app/components/portfolio-diagnostic.tsx` | 将已校验的最新冻结诊断交给同页情景区块 |
| `app/(protected)/portfolio/page.tsx` | 将当前快照、最新诊断和显式操作接入组件 |

---

### Task 1: 冻结阶段 A 合同与口径

**Files:**
- Create: `engine/src/nexus/optimizer/scenario_contracts.py`
- Create: `engine/tests/unit/optimizer/test_scenario_contracts.py`

**Interfaces:**
- Consumes: `nexus.optimizer.compare.TypedMetric`、`nexus.domain.time.require_utc`。
- Produces: `AdjustmentScenarioConstraintsV1`、`PortfolioScenarioRequestV1`、`ScenarioPositionV1`、`ScenarioMetricSetV1`、`PortfolioScenarioEvaluationV1`。

- [ ] **Step 1: 写拒绝浮点和额外字段的失败测试**

测试必须证明：`max_turnover=0.1`、额外 `positions`、`allow_new_positions=true`、非 UTC `as_of` 和越界比例均被拒绝。

```python
def test_constraints_reject_binary_float_and_weight_payload() -> None:
    with pytest.raises((TypeError, ValidationError)):
        AdjustmentScenarioConstraintsV1(
            schema_version="adjustment_scenario_constraints_v1",
            objective="reduce_security_concentration",
            max_turnover=0.1,
            minimum_cash="0.10",
            universe_policy="current_holdings_only",
            allow_new_positions=False,
        )
```

- [ ] **Step 2: 写权重平衡和动作一致性的失败测试**

覆盖目标权重大于当前权重、持仓集合重复、权重不等于 1、`reduce` 与权重方向不一致、`execution_locked=false`。

- [ ] **Step 3: 运行测试确认模块缺失**

```bash
uv run pytest engine/tests/unit/optimizer/test_scenario_contracts.py -q --import-mode=importlib
```

Expected: FAIL，因为 `scenario_contracts` 尚不存在。

- [ ] **Step 4: 实现最小封闭模型**

沿用 `optimizer.models._StrictModel` 的递归浮点拒绝语义，但不要为了复用而修改旧模型。对所有比例增加 `ge=0, le=1`；对元组增加唯一 instrument 校验；结果模型校验当前与目标两组权重各自精确为 1。

- [ ] **Step 5: 实现规范化哈希 helper**

```python
def canonical_scenario_hash(payload: BaseModel | Mapping[str, object]) -> str:
    """Return sha256 over sorted compact JSON with Decimal serialized as strings."""
```

结果哈希计算时排除 `result_hash` 自身；不得包含运行时 UUID 或当前时钟。

- [ ] **Step 6: 运行聚焦验证**

```bash
uv run pytest engine/tests/unit/optimizer/test_scenario_contracts.py -q --import-mode=importlib
uv run ruff check engine/src/nexus/optimizer/scenario_contracts.py engine/tests/unit/optimizer/test_scenario_contracts.py
uv run mypy engine/src/nexus/optimizer/scenario_contracts.py
```

- [ ] **Step 7: 提交合同切片**

```bash
git add engine/src/nexus/optimizer/scenario_contracts.py engine/tests/unit/optimizer/test_scenario_contracts.py
git commit -m "feat: define portfolio scenario contracts"
```

### Task 2: 实现纯确定性情景引擎

**Files:**
- Create: `engine/src/nexus/optimizer/scenario_engine.py`
- Create: `engine/tests/unit/optimizer/test_scenario_engine.py`

**Interfaces:**
- Consumes: Task 1 合同、`PortfolioSnapshot`、`PortfolioDiagnosticReport`、`InstrumentIdentity` 映射、`RealAdvisoryPolicy`、权威 `diagnostic_report_id` 与 `diagnostic_input_hash`。
- Produces: `evaluate_adjustment_scenario(...) -> PortfolioScenarioEvaluationV1`。

- [ ] **Step 1: 写预算充足和预算不足的失败测试**

固定 UUID 和 UTC 时间构造高集中组合。断言预算充足返回 `met`；预算不足返回 `improved_not_met`；两者均满足只减仓、现金增加、单边换手不超限。

- [ ] **Step 2: 写不交易和不可行测试**

覆盖已满足目标返回 `unchanged`、单证券返回 `infeasible`、一个基点无法推进、请求现金下限低于系统值时采用系统值，以及不可出售数量形成的目标权重下限。

- [ ] **Step 3: 写确定性与口径测试**

断言：

- 同一输入连续两次得到相同目标权重和 `result_hash`；
- 输入持仓顺序变化不改变结果；
- security-only Top1/Top3/HHI 不把现金计入分母；
- 出售 5% 并转现金得到 5% 单边换手；
- 舍入余数只进入现金；
- 估值域固定返回 `currency:USD`，港股只读持仓不进入权重和候选；
- USD 持仓缺少标准美股身份时失败关闭；
- 成本为 `unknown/COST_MODEL_NOT_AVAILABLE`，`execution_locked=true`。

- [ ] **Step 4: 运行测试确认入口缺失**

```bash
uv run pytest engine/tests/unit/optimizer/test_scenario_engine.py -q --import-mode=importlib
```

- [ ] **Step 5: 实现权威输入校验和指标函数**

私有 helper 仅负责：快照/诊断/诊断哈希绑定、标准美股身份过滤、USD 域含现金权重、冻结 USD Top1 指标复核、可出售下限、security-only 指标、现金腿换手和稳定候选排序。禁止数据库、时钟、随机数、网络或日志副作用。

- [ ] **Step 6: 实现一个基点循环**

每轮重新计算排名和指标；先检查预算，再移动一个基点；保留至少一个正证券仓位。停止状态严格按设计第 5 节映射。

- [ ] **Step 7: 实现结果哈希并运行聚焦验证**

```bash
uv run pytest engine/tests/unit/optimizer/test_scenario_contracts.py engine/tests/unit/optimizer/test_scenario_engine.py -q --import-mode=importlib
uv run ruff check engine/src/nexus/optimizer/scenario_contracts.py engine/src/nexus/optimizer/scenario_engine.py engine/tests/unit/optimizer/test_scenario_engine.py
uv run mypy engine/src/nexus/optimizer/scenario_contracts.py engine/src/nexus/optimizer/scenario_engine.py
```

- [ ] **Step 8: 提交引擎切片**

```bash
git add engine/src/nexus/optimizer/scenario_engine.py engine/tests/unit/optimizer/test_scenario_engine.py
git commit -m "feat: evaluate concentration adjustment scenarios"
```

### Task 3: 接通唯一 Engine API 与 BFF

**Files:**
- Modify: `engine/src/nexus/api/schemas/optimization.py`
- Modify: `engine/src/nexus/api/routes/optimization.py`
- Create: `engine/tests/integration/test_portfolio_scenario_api.py`
- Modify: `engine/tests/integration/test_what_if_body_boundary.py`
- Create: `app/api/v1/portfolio-scenarios/evaluate/route.ts`
- Modify: `app/lib/optimization/bff.ts`
- Modify: `app/lib/engine/client.ts`
- Modify: `app/lib/engine/errors.ts`
- Create: `tests/unit/engine/portfolio-scenario-routes.test.ts`
- Modify: `contracts/openapi.json`
- Modify: `app/lib/generated/schema.d.ts`
- Modify: `app/lib/generated/runtime-contract.ts`

**Interfaces:**
- Consumes: Task 2 引擎、现有快照和诊断记录加载模式、现有 `what-if/evaluate` 的 bounded body/HMAC/idempotency/BFF 模式。
- Produces: `POST /internal/v1/portfolio-scenarios/evaluate` 与 `POST /api/v1/portfolio-scenarios/evaluate`。

- [ ] **Step 1: 写 Engine 成功与状态响应测试**

用已持久化 READY 快照和匹配诊断调用正常路由，分别断言 `met`、`improved_not_met`、`unchanged`、`infeasible` 都是 200，且响应绑定请求中的快照、诊断、时间和 `execution_locked=true`。

- [ ] **Step 2: 写 Engine fail-closed 测试**

覆盖：快照不是优化基线、诊断不存在、跨快照诊断、`as_of` 不一致、额外权重/标的字段、非 JSON、超过 64 KiB、相同幂等键不同请求。

- [ ] **Step 3: 写 BFF 转发与响应绑定测试**

断言 BFF 只转发 exact JSON bytes、身份和 Idempotency-Key；Engine 返回的快照或诊断 ID 与请求不一致时，BFF 返回稳定内部错误；不得允许通配 Engine 路径。

- [ ] **Step 4: 运行测试确认路由缺失**

```bash
uv run pytest engine/tests/integration/test_portfolio_scenario_api.py engine/tests/integration/test_what_if_body_boundary.py -q --import-mode=importlib
npm test -- tests/unit/engine/portfolio-scenario-routes.test.ts
```

- [ ] **Step 5: 增加 API schema 和 envelope**

API 模型直接包裹 Task 1 合同，Decimal 继续经 `DecimalString` 输出。不要增加 GET、Mandate、decision run 或 selection schema。

- [ ] **Step 6: 实现 Engine 路由**

从数据库按 ID 加载快照和诊断，加载 `RealAdvisoryPolicy.v1` 的数据库权威版本，完成绑定校验后调用纯函数。复用现有 body bound 和 idempotency claim 模式；审计 payload 只含哈希、政策版本、标的数量和两个状态。

- [ ] **Step 7: 实现 BFF 和封闭路径验证**

在 `bff.ts` 增加 `requirePortfolioScenarioRequest`，明确拒绝任何权重或证券字段。在 `client.ts` 增加精确路径模板，在 `errors.ts` 校验 envelope、绑定 ID 和永久执行锁。

- [ ] **Step 8: 生成客户端并运行跨层验证**

```bash
npm run generate:client
npm run check:generated
uv run pytest engine/tests/unit/optimizer/test_scenario_contracts.py engine/tests/unit/optimizer/test_scenario_engine.py engine/tests/integration/test_portfolio_scenario_api.py engine/tests/integration/test_what_if_body_boundary.py -q --import-mode=importlib
uv run ruff check engine/src/nexus/optimizer/scenario_contracts.py engine/src/nexus/optimizer/scenario_engine.py engine/src/nexus/api/schemas/optimization.py engine/src/nexus/api/routes/optimization.py
uv run mypy engine/src/nexus/optimizer engine/src/nexus/api
npm test -- tests/unit/engine/portfolio-scenario-routes.test.ts tests/unit/engine/client-security.test.ts
npm run typecheck
```

- [ ] **Step 9: 提交 API/BFF 切片**

```bash
git add engine/src/nexus/api/schemas/optimization.py engine/src/nexus/api/routes/optimization.py engine/tests/integration/test_portfolio_scenario_api.py engine/tests/integration/test_what_if_body_boundary.py app/api/v1/portfolio-scenarios/evaluate/route.ts app/lib/optimization/bff.ts app/lib/engine/client.ts app/lib/engine/errors.ts tests/unit/engine/portfolio-scenario-routes.test.ts contracts/openapi.json app/lib/generated/schema.d.ts app/lib/generated/runtime-contract.ts
git commit -m "feat: expose portfolio scenario evaluation"
```

### Task 4: 交付单候选页面与真实只读验收

**Files:**
- Create: `app/components/portfolio-scenario.tsx`
- Create: `tests/unit/portfolio/portfolio-scenario.test.tsx`
- Modify: `app/components/portfolio-diagnostic.tsx`
- Modify: `tests/unit/portfolio/diagnostic.test.tsx`
- Modify: `app/(protected)/portfolio/page.tsx`
- Modify: `tests/unit/portfolio/page.test.tsx`
- Modify: `app/lib/display-labels.ts`
- Modify: `app/styles/ui.css`
- Create: `tests/e2e/portfolio-scenario.spec.ts`
- Create: `engine/tests/integration/test_portfolio_scenario_e2e.py`
- Modify: `docs/plans/2026-09-02-core-evidence-checkpoint.md`

**Interfaces:**
- Consumes: Task 3 generated request/response类型，组合页当前快照与最新冻结诊断读取路径。
- Produces: 明确触发、仅内存保存的当前组合 vs 单候选视图，以及阶段 A 验收证据。

- [ ] **Step 1: 写组件状态和文案测试**

覆盖 idle、evaluating、success、partial、infeasible、stale 和 error。断言页面写明 security-only 分母、成本未知、仅分析不保存、交易锁定；不存在“推荐”“接受”“Shadow”或“执行”。

- [ ] **Step 2: 写请求行为测试**

断言页面加载、普通刷新和组合切换没有 scenario POST；只有点击按钮发起一次 POST；重复点击运行中禁用；切换组合或快照后清除旧结果；请求不包含权重或证券列表。

- [ ] **Step 3: 写浏览器副作用测试**

Playwright 拦截并断言：页面加载和刷新不向 scenario、Provider、模型、Shadow、订单或成交路径 POST；明确点击仅命中一次 `/api/v1/portfolio-scenarios/evaluate`。

- [ ] **Step 4: 运行前端测试确认组件缺失**

```bash
npm test -- tests/unit/portfolio/portfolio-scenario.test.tsx tests/unit/portfolio/page.test.tsx
```

- [ ] **Step 5: 实现组件和组合页接线**

组件固定目标和 universe，只让用户输入最大换手、最低现金。使用 generated schema、现有格式化 helper 和设计 token；候选只存在 React state。给现有 `PortfolioDiagnostic` 增加可选 `onReport(report | null)` 回调：仅在其已完成快照绑定校验后把报告交给父页面，卸载、失败或快照变化时传回 `null`。不得为情景区块重复请求诊断。诊断尚未加载、快照不合格或请求运行中时按钮禁用并显示真实原因。

- [ ] **Step 6: 写跨层 fixture 验收**

`test_portfolio_scenario_e2e.py` 通过正常 Engine 入口创建匹配的持久化 fixture，调用 POST，验证精确哈希、权重、unknown 和审计摘要，并确认旧方案、Shadow、订单和成交记录均为零增量；阶段 A 不应存在 scenario 业务表。

- [ ] **Step 7: 运行阶段 A 全量聚焦验证**

```bash
uv run pytest \
  engine/tests/unit/optimizer/test_scenario_contracts.py \
  engine/tests/unit/optimizer/test_scenario_engine.py \
  engine/tests/integration/test_portfolio_scenario_api.py \
  engine/tests/integration/test_portfolio_scenario_e2e.py \
  engine/tests/integration/test_what_if_body_boundary.py \
  -q --import-mode=importlib
uv run ruff check engine/src/nexus/optimizer engine/src/nexus/api engine/tests/unit/optimizer engine/tests/integration/test_portfolio_scenario_api.py engine/tests/integration/test_portfolio_scenario_e2e.py
uv run mypy engine/src/nexus/optimizer engine/src/nexus/api
npm run lint
npm run typecheck
npm test -- tests/unit/portfolio/portfolio-scenario.test.tsx tests/unit/portfolio/diagnostic.test.tsx tests/unit/portfolio/page.test.tsx tests/unit/engine/portfolio-scenario-routes.test.ts
npm run test:e2e -- tests/e2e/portfolio-scenario.spec.ts --workers=1
npm run check:generated
npm run build
```

- [ ] **Step 8: 运行一次真实本地只读验收**

使用正常产品配置和现有持久化数据，不刷新券商、不调用 Provider 或模型。记录：

- 快照和诊断身份只写入受保护本地证据，不写聊天或公共测试；
- 用户可见目标状态、分析状态和 unknown；
- `execution_locked=true`；
- 旧方案/Shadow/order/fill 持久化零增量，且没有新增 scenario 业务表；
- 页面展示与同次 API 结果一致。

如果当前真实快照不满足基线，预期结果是准确的 `SCENARIO_BASELINE_NOT_READY`，并把“获得成功候选”保留为真实数据阻塞，不用 fixture 冒充真实验收。

- [ ] **Step 9: 更新既有证据检查点并提交**

只追加实际结果、代码基线、验证命令、真实数据阻塞和下一允许切片，不记录私人持仓、金额、凭据或原始响应。

```bash
git add app/components/portfolio-scenario.tsx tests/unit/portfolio/portfolio-scenario.test.tsx app/components/portfolio-diagnostic.tsx tests/unit/portfolio/diagnostic.test.tsx app/'(protected)'/portfolio/page.tsx tests/unit/portfolio/page.test.tsx app/lib/display-labels.ts app/styles/ui.css tests/e2e/portfolio-scenario.spec.ts engine/tests/integration/test_portfolio_scenario_e2e.py docs/plans/2026-09-02-core-evidence-checkpoint.md
git commit -m "feat: deliver read-only portfolio scenario loop"
```

## Implementation stop point

Task 4 验收后停止本计划。不要顺手增加数据库迁移、Mandate CRUD、Agent tool、模型 Job、候选选择、Shadow 适配、通用优化器或新标的支持。

阶段 B 的准入条件只有三个：阶段 A fixture 全链路通过；至少一个真实只读案例得到候选或确认真实基线阻塞；产品确认需要 Agent 解释而不是继续改确定性算法。阶段 B 必须使用新的设计评审和实施计划。
