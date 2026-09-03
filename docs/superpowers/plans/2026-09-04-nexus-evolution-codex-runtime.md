# NEXUS evolution Codex 单次运行适配 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. root 负责运行时/接线，B 负责 Task 2 的用量持久化，A 串行集成；不新建用户任务。

**Goal:** 为既有策略研发服务增加可验证的单次 Codex 运行时，同时保留证据、用量、队列重放和执行权限边界。

**Architecture:** 复用 `invoke_codex(payload, output_model=...)` 的隔离传输；由服务端预先读取既有 `StrategyRNDTools` 的冻结输出，模型无工具能力。输出仍是 `StrategyExperimentProposalV1`，不复用八角色委员会、不创建新策略搜索系统。运行时契约先独立交付，再接入现有 executor；真实家族验收仍等待合格价格/基线/用途 authority。

**Tech Stack:** Python 3.12、Pydantic、现有 Codex SDK、pytest/pytest-asyncio、SQLAlchemy/PostgreSQL。

**Spec:** `docs/superpowers/plans/2026-09-03-nexus-team-delivery.md` 的 E1；仓库 `AGENTS.md`。

## Global Constraints

- 基线为主线 `c10abb7`；root 在 `.worktrees/nexus-team-plan` / `edward/nexus-evolution-codex-runtime` 开发，A 独占主线合并。
- `local_read_only` 不包含自动调度、账户写入、模拟或实盘下单。
- 未知结果不重放；不因接通模型放宽调整价格、OOS、promotion、rollback 或人工选择门禁。
- 模型 ID 复用 SDK 的固定 `MODEL_ID`；64,000 输入 / 8,192 输出 token 是验收阈值，已检查本机 SDK 的 run/turn 没有服务端输出上限参数，不能声称实际消耗被此值限制。0 个模型工具、单次 provider call。
- 本切片没有真实模型/券商调用，无新依赖、凭据、套餐或外部写入。Task 2 的最小迁移仅允许研发 run 的 input/output token 和 tool_call_count 三项用量计数同时为 null，不改旧行。
- 订阅额度不当作 API 美元花费：持久化结算费用保持 `null`，不能写成“免费”；已知 token 保留实际值，未知为 null，不写成零或伪造的上界。
- 历史研究、策略、模型配置、请求哈希和结果不回填；当前 Responses/fixture 行为保持兼容。

## Task 1: 可注入的单次策略生成运行时

**Files:**
- Create: `engine/src/nexus/evolution/codex.py`
- Modify: `engine/src/nexus/evolution/agent.py`（仅提取两个既有纯校验供两种运行时复用）
- Modify: `engine/src/nexus/agents/codex_sdk.py`（失败携带 SDK 已报告的用量，不透传原始响应）
- Test: `engine/tests/unit/evolution/test_agent.py`
- Test: `engine/tests/unit/agents/test_codex_sdk.py`

**Interfaces:**
- Consumes: `StrategyRNDRequestV1`、`StrategyRNDTools.execute(name, payload)`、`invoke_codex(payload, output_model=StrategyExperimentProposalV1)`。
- Produces: `CodexStrategyRNDRuntime(config: ResponsesModelConfig)`，提供 `configuration`、`usage: UsageSnapshot | None`、`generate(request, tools) -> StrategyExperimentProposalV1`。未外呼是已知零；已发出但无有效用量为 None。Task 2 才扩展既有 runtime Protocol 并接业务，不能交给尚不支持 None 的旧 service。
- 配置版本：`CODEX_RND_CONFIG_VERSION = "codex-sdk-strategy-rnd-v1"`；此版本只标识新运行时，不修改旧模型记录。

- [x] **Step 1: 添加失败回归。** 用合成 request/proposal 和真实 `StrategyRNDTools`，只 mock 最外层 SDK 调用。覆盖冻结 parent/search/training 输出进入实际 payload、可用 sealed summaries 全部按冻结 context 读取、无当前 family/OOS 数据、同一请求仅调用一次。

  ```python
  result = await runtime.generate(request, tools)
  assert result == expected_proposal
  assert invoke.await_count == 1
  assert runtime.usage.provider_calls == 1
  with pytest.raises(ValueError, match="ALREADY_ATTEMPTED"):
      await runtime.generate(request, tools)
  ```

- [x] **Step 2: 跑红。** `PYTHONPATH=engine/src /Users/wangchen/Documents/ChatGPT/投资/.venv/bin/pytest engine/tests/unit/evolution/test_agent.py -q`；19 个新增回归因新运行时不存在失败，5 个原有测试通过。另补本地读取也受 deadline 约束的测试，先出现未抛超时的产品失败，再修复。
- [x] **Step 3: 最小实现。** 预取三项必要工具输出及最多 32 个已准入 sealed summary；本地预取不赋予模型工具能力，`tool_calls=0`。所有读取经既有 typed tool validator，失败即退出。完整 prompt/request/frozen outputs 组成一个 payload，不传账户或文件路径。

  ```python
  # 必须在 invoke 之前：上下文校验、配置身份校验、字节预算校验。
  estimated_input = len(json.dumps(payload, ensure_ascii=False).encode()) + 8192
  if estimated_input > config.max_input_tokens:
      raise ValueError("CODEX_RND_INPUT_TOO_LARGE")
  # 绝对 deadline 也要在同步工作后显式检查；外呼后未知用量为 None。
  if asyncio.get_running_loop().time() >= deadline:
      raise TimeoutError("CODEX_RND_DEADLINE_EXCEEDED")
  async with asyncio.timeout_at(deadline):
      raw, usage = await invoke_codex(payload, output_model=StrategyExperimentProposalV1)
  ```

  配置要求固定 SDK model、上述新 version、0 美元 API 费率、0 模型工具、SDK token 验收阈值、`0 < timeout <= 30` 秒。成功响应先保存真实 usage，再校验 token 范围、provider_calls=1、tool_calls=0、严格 proposal schema 与冻结身份/引用。失败的 `CodexInvocationError` 仅携带安全码与已知 usage（可空），不丢弃 incomplete/tool/size 拒绝结果中已报告的数字。沿用 SDK 的关闭与取消清理，不加重试。
- [x] **Step 4: 补边界并跑绿。** 错配置/错 context/坏工具输出/过大输入均零外呼；错误 schema、身份/引用漂移、用量超限、超时均最多一次调用且不能产生 proposal。相关 56 测试通过；全 evolution unit 使用 `--import-mode=importlib` 避免既有同名 test_models 模块冲突，145 通过。Ruff/format、两个源文件 Mypy、diff-check 通过。
- [x] **Step 5: 质量与交付。** 跑相关 pytest、Ruff、Mypy、diff-check；一次独立审查，修复后定向复核；提交并交 A。此时只宣告运行时离线契约完成，不宣告普通入口或真实家族可用。

  首轮审查发现同步工作跨过 asyncio timeout，以及将 SDK 验收阈值误作已实施用量上界两项 P2。已补 8 个先红回归，改为显式单调 deadline、可空未知用量、传输失败保留已报告用量；合并 evolution/SDK/旧 stage trace 回归 208 passed，Ruff/format、3 源文件 Mypy、diff-check 通过。同一审查者定向复核 72 passed，两项 P2 关闭，同意合入 `410ab0a` + `bd253e9`；尚不接业务 executor。

## Task 2: 现有 executor 与一次性运行边界

**Ownership:** root 独占 agent Protocol、executor、authority、worker 和对应 unit/executor PG；B 独占 service、持久化模型、迁移 head、新 0036 迁移及 migration/admission PG。两者在 Task 1 合入主线后同步基线再改，A 统一串行集成。不得交叉编辑对方文件。

**Files:**
- Modify: `engine/src/nexus/evolution/executor.py`
- Modify: `engine/src/nexus/evolution/codex.py`（保留 worker 绝对截止，不能在 service 预检后重新计时）
- Modify: `engine/src/nexus/evolution/authority.py`
- Modify: `engine/src/nexus/evolution/service.py`
- Modify: `engine/src/nexus/evolution/worker.py`
- Modify: `engine/src/nexus/ops/openai_local.py`（仅增加显式 `--strategy-rnd` setup 注册，不调用模型或入队）
- Modify: `engine/src/nexus/evolution/agent.py`（Protocol 接受可空 usage）
- Modify: `engine/src/nexus/persistence/models/evolution.py`
- Modify: `engine/src/nexus/persistence/migrations.py`
- Create: `engine/alembic/versions/0036_strategy_rnd_usage_unknown.py`（从当前 0035 head 前进，实施前重新确认没有并行迁移）
- Test: `engine/tests/unit/evolution/test_executor_capabilities.py`
- Test: `engine/tests/integration/test_evolution_executor_pg.py`
- Test: `engine/tests/integration/test_evolution_admission_pg.py`
- Test: `engine/tests/integration/test_evolution_migration.py`
- Test: `engine/tests/unit/ops/test_openai_local.py`
- Test: `engine/tests/integration/test_openai_local.py`

**Interfaces:**
- `_generation_runtime(authority)` 显式选择新运行时，不 fallback fixture。
- `load_generation(..., model_id=None, model_version=None)` 增加可选精确版本过滤，原调用默认不变。
- `run_strategy_rnd_worker(..., run_forever=False)` 复用既有 run-once；Codex 禁止 `run_forever=True`，CLI 保留先于数据库/队列拒绝长驻运行的边界。
- `nexus-openai-local setup --backend codex_sdk --strategy-rnd --skip-key` 只幂等注册现有 `strategy_rnd_v1` prompt 与固定 Codex R&D config；与 `--staged` 互斥，不创建研究、行情、作业或模型调用。

- [x] **Step 1: 先锁定拒绝与身份测试。** Codex builder 不需要 Responses API key；只接新模型版本；Responses/fixture 不消费新 Codex 配置；未知后端仍拒绝。生成操作 `context.claim.attempt_no != 1` 时返回失败且零模型调用，不能因既有队列重领自动重放外呼。
- [x] **Step 2: 跑红后实现精确适配。** executor 对 Codex 传固定 model ID/version 给 resolver；将 worker 剩余 deadline 留出至少 5 秒结算余量，并传递同一绝对 monotonic deadline，不能在 service 预检后从调用开始重新计时而越过 worker 的 45 秒上限。普通 setup 增加显式策略研发选项并注册 exact prompt/config；不复用委员会的 prompt version，不改默认 setup，也不顺带入队或外呼。
- [x] **Step 3: 订阅用量持久化回归。** 新 Codex version 的 `settled_cost_usd=None`；实际 token（包括超阈值但已知的数字）原样持久化，未知调用三项计数均 null 且失败。新迁移放开 input_tokens/output_tokens/tool_call_count 的 NOT NULL，并约束三项同时已知或同时未知；旧数值/旧行不更新。降级仅在无 null 记录时允许，已有未知记录则明确拒绝，不填零来降级。隔离 PG 覆盖升级、合法/非法插入、历史数值保持和两种降级路径；原 Responses 美元计费和 fixture 回归不变。新版本仅经明确 setup/受控注册存在，不把旧 v1/v2 委员会记录重标成策略研发配置。
- [x] **Step 4: 隔离 PG 接线。** synthetic authority 经 executor→execute_strategy_rnd→数据库验证：成功只创建一个严格输出；错误、超时、重领不创建 family；无合格调整价格时在外呼前停止；原 OOS/promotion/rollback 测试保持。
- [ ] **Step 5: 审查并集成。** 集中审查明确区分本地运行时/入口/真实家族，A 合入后复核主线包含变更。真实 family 仍不启动，直到同一合法数据 authority 和普通准备入口都已具备；不代用户选择真实策略动作。

  root 基线为 `0872916`（含 B `ac693f3`），运行时接线提交 `9ad78b6`；setup 与依赖它的重领 PG 分成第二个小提交，避免一片超过 800 行。167 项相关 unit、11 项 executor/worker/setup PG、6 源文件 Mypy、Ruff/diff-check通过。独立审查唯一 P2 为 service 预检后的相对 deadline 重起算，先红回归后改为固定绝对截止，59 项定向复核及受影响 3 项生成/重领 PG 再验通过。A 另以 `1d66446` 修复通用研究默认选择/显式准入/worker 对新 RND 配置的隔离，11 PG、独立17 unit通过。以上均无真实模型/券商调用；主线集成及业务运行仍分别记录。

## 后续验收边界

运行时、接线及正式 setup 是 E1 的代码部分，不代替合格历史基线、有效策略 registry 和真实单家族验收。不能以导入成功或 synthetic family 通过声称 E1 全部完成。真实长期晋级协议与主计划的 S1/S2/S3 依赖保持独立。
