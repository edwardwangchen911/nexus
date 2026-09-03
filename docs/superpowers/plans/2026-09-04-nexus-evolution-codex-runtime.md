# NEXUS evolution Codex 单次运行适配 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. root 独占本切片，A 串行集成；不新建用户任务。

**Goal:** 为既有策略研发服务增加可验证的单次 Codex 运行时，同时保留证据、用量、队列重放和执行权限边界。

**Architecture:** 复用 `invoke_codex(payload, output_model=...)` 的隔离传输；由服务端预先读取既有 `StrategyRNDTools` 的冻结输出，模型无工具能力。输出仍是 `StrategyExperimentProposalV1`，不复用八角色委员会、不创建新策略搜索系统。运行时契约先独立交付，再接入现有 executor；真实家族验收仍等待合格价格/基线/用途 authority。

**Tech Stack:** Python 3.12、Pydantic、现有 Codex SDK、pytest/pytest-asyncio、SQLAlchemy/PostgreSQL。

**Spec:** `docs/superpowers/plans/2026-09-03-nexus-team-delivery.md` 的 E1；仓库 `AGENTS.md`。

## Global Constraints

- 基线为主线 `c10abb7`；root 在 `.worktrees/nexus-team-plan` / `edward/nexus-evolution-codex-runtime` 开发，A 独占主线合并。
- `local_read_only` 不包含自动调度、账户写入、模拟或实盘下单。
- 未知结果不重放；不因接通模型放宽调整价格、OOS、promotion、rollback 或人工选择门禁。
- 模型 ID 复用 SDK 的固定 `MODEL_ID`；沿用 64,000 输入 / 8,192 输出 token 上限、0 个模型工具、单次 provider call。
- 本切片没有真实模型/券商调用，无新依赖、迁移、凭据、套餐或外部写入。
- 订阅额度不当作 API 美元花费：持久化结算费用保持 `null`，不能写成“免费”；记录已知 token 或失败时保守上界，不将未知用量写成零。
- 历史研究、策略、模型配置、请求哈希和结果不回填；当前 Responses/fixture 行为保持兼容。

## Task 1: 可注入的单次策略生成运行时

**Files:**
- Create: `engine/src/nexus/evolution/codex.py`
- Modify: `engine/src/nexus/evolution/agent.py`（仅提取两个既有纯校验供两种运行时复用）
- Test: `engine/tests/unit/evolution/test_agent.py`

**Interfaces:**
- Consumes: `StrategyRNDRequestV1`、`StrategyRNDTools.execute(name, payload)`、`invoke_codex(payload, output_model=StrategyExperimentProposalV1)`。
- Produces: `CodexStrategyRNDRuntime(config: ResponsesModelConfig)`，实现 `configuration`、`usage`、`generate(request, tools) -> StrategyExperimentProposalV1`。
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
  # 标记一次尝试；超时/传输错误保持非零保守用量上界，禁止再次调用。
  async with asyncio.timeout(float(config.absolute_timeout_seconds)):
      raw, usage = await invoke_codex(payload, output_model=StrategyExperimentProposalV1)
  ```

  配置要求固定 SDK model、上述新 version、0 美元 API 费率、0 模型工具、SDK token 上限、`0 < timeout <= 30` 秒。成功响应先保存真实 usage，再校验 token 范围、provider_calls=1、tool_calls=0、严格 proposal schema 与冻结身份/引用。沿用 SDK 的关闭与取消清理，不加重试。
- [x] **Step 4: 补边界并跑绿。** 错配置/错 context/坏工具输出/过大输入均零外呼；错误 schema、身份/引用漂移、用量超限、超时均最多一次调用且不能产生 proposal。相关 56 测试通过；全 evolution unit 使用 `--import-mode=importlib` 避免既有同名 test_models 模块冲突，145 通过。Ruff/format、两个源文件 Mypy、diff-check 通过。
- [ ] **Step 5: 质量与交付。** 跑相关 pytest、Ruff、Mypy、diff-check；一次独立审查，修复后定向复核；提交并交 A。此时只宣告运行时离线契约完成，不宣告普通入口或真实家族可用。

## Task 2: 现有 executor 与一次性运行边界

**Files:**
- Modify: `engine/src/nexus/evolution/executor.py`
- Modify: `engine/src/nexus/evolution/authority.py`
- Modify: `engine/src/nexus/evolution/service.py`
- Modify: `engine/src/nexus/evolution/worker.py`
- Test: `engine/tests/unit/evolution/test_executor_capabilities.py`
- Test: `engine/tests/integration/test_evolution_executor_pg.py`
- Test: `engine/tests/integration/test_evolution_admission_pg.py`

**Interfaces:**
- `_generation_runtime(authority)` 显式选择新运行时，不 fallback fixture。
- `load_generation(..., model_id=None, model_version=None)` 增加可选精确版本过滤，原调用默认不变。
- `run_strategy_rnd_worker(..., run_forever=False)` 复用既有 run-once；Codex 禁止 `run_forever=True`，CLI 保留先于数据库/队列拒绝长驻运行的边界。

- [ ] **Step 1: 先锁定拒绝与身份测试。** Codex builder 不需要 Responses API key；只接新模型版本；Responses/fixture 不消费新 Codex 配置；未知后端仍拒绝。生成操作 `context.claim.attempt_no != 1` 时返回失败且零模型调用，不能因既有队列重领自动重放外呼。
- [ ] **Step 2: 跑红后实现精确适配。** executor 对 Codex 传固定 model ID/version 给 resolver；将 worker 剩余 deadline 留出至少 5 秒结算余量，不能仅从调用开始重新计 30 秒而越过 worker 的 45 秒上限。
- [ ] **Step 3: 订阅用量持久化回归。** 新 Codex version 的 `settled_cost_usd=None`；实际 token 持久化，未知调用失败保守计上界；原 Responses 美元计费和 fixture 回归不变。新版本仅经明确 setup/受控注册存在，不把旧 v1/v2 委员会记录重标成策略研发配置。
- [ ] **Step 4: 隔离 PG 接线。** synthetic authority 经 executor→execute_strategy_rnd→数据库验证：成功只创建一个严格输出；错误、超时、重领不创建 family；无合格调整价格时在外呼前停止；原 OOS/promotion/rollback 测试保持。
- [ ] **Step 5: 审查并集成。** 集中审查明确区分本地运行时/入口/真实家族，A 合入后复核主线包含变更。真实 family 仍不启动，直到同一合法数据 authority 和普通准备入口都已具备；不代用户选择真实策略动作。

## 后续验收边界

运行时及注入接线是 E1 的代码部分，不代替正式 setup 入口、合格历史基线、有效策略 registry 和真实单家族验收。继续任务时先查现有注册入口，复用已有注册/提示资产流程；不能以导入成功或 synthetic family 通过声称 E1 全部完成。真实长期晋级协议与主计划的 S1/S2/S3 依赖保持独立。
