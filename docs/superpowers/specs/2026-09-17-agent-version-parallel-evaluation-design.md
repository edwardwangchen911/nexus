# Agent 版本并行模拟与样本外评价设计

**日期：** 2026-09-17
**状态：** 已确认设计范围，待实施计划
**对应路线图：** `docs/plans/2026-09-12-nexus-simulation-evolution-roadmap.md` 第三阶段

## 1. 目标

让一个冻结的新 Agent 版本在 AAPL 固定范围内，与当前冻结版本使用相同决策时点、相同市场与研究证据、相同初始资金和持仓，分别走现有 Agent Paper 的确定性仓位、硬风控、内部模拟订单、成交、账本及绩效链。训练观察与样本外评价必须预先分离；证据不足、结果不可比或没有可靠改善时继续使用旧版本。

本设计只解决 Agent prompt/model 版本的并行内部模拟与版本选择，不建设新的通用 Shadow 框架，不扩张股票池或数据源，不接入券商模拟盘，不改变实盘锁。

## 2. 已核实现状与缺口

- `agent_research_runs` 已冻结完整八角色 `prompt_manifest`、`prompt_hash`、`model_hash`、数据快照和 evidence manifest，可以作为一个实际运行版本的内容寻址事实。
- 当前 Agent Paper admission 使用 `local-agent-paper-cycle` actor 的最新 run 来复用版本，适合第二阶段的单一冻结版本，但不能显式选择 challenger，也不能表达训练/样本外归属。
- `agent_paper_cycles` 已强制 `execution_locked=true`，并已经接通研究输出、确定性 sizing、硬风控、内部模拟订单/成交、账本和绩效。
- 当前业务库有三个历史 prompt/model 身份，但它们按时间串行产生，不能作为同起点、同证据的 champion/challenger 结果。
- `strategy_shadow_*` 强绑定 `strategy_definition`、63 个交易日、5/21/63 检查点及确定性策略状态；当前相关表为空。把 Agent 版本塞入该体系会混淆策略定义与 Agent prompt/model 权威，并重新扩大已经暂停的 Shadow 子系统。
- 现有 `strategy_decision_agent_outputs` 数据库守卫只验证 completed v4 output、标的唯一和绑定数量；它本身不表达版本试验角色。因此版本试验必须有显式 lineage，不能靠 actor 字符串或页面标签推断。

## 3. 方案比较

### 方案 A：复用 `strategy_shadow_*`

优点是已有双腿和绩效检查点。缺点是需要把 prompt/model 伪装成 `strategy_definition`，继承 63 日策略机制，并重写大量输入、状态和晋升合同。方向错误，拒绝。

### 方案 B：同一周期额外运行 challenger research，但不走模拟执行

优点是改动小。缺点是只能比较文本、意图或后验价格方向，不能验证 sizing、风险、订单、成交、账本和成本后的实际差异，不满足“新版本先并行模拟”。拒绝。

### 方案 C：复用 Agent Paper，增加薄的版本评价权威

incumbent 与 challenger 各使用一个隔离的内部评价账户，现有第二阶段账户保持不变。两侧周期复用相同产品链，只增加冻结评价、周期配对和最终判定三类不可变事实。该方案最大限度复用已验收能力，同时把版本 lineage、训练/OOS 边界和禁止未经 PASS 的隐式激活写成确定性合同。采用此方案。

## 4. 范围与不变量

首个评价固定为：

- 标的仅 AAPL；每个周期恰好一个候选标的。
- 一次只允许一个 challenger 评价处于开放状态。
- incumbent 与 challenger 的 decision slot、交易日历、价格政策、sizing policy、风险政策和费用假设完全一致。
- 两侧研究 authority 的市场、公司、宏观和价格证据内容哈希一致；账户上下文可以使用不同记录 ID，但其现金、持仓、成本基础和 NAV 必须内容等价。
- 两个评价账户都只具有 `simulation_write_internal`；不得存在 broker binding、broker command 或外部账户标识。两者从相同 USD 全现金初始资本开始，且只运行评价中冻结的 slot；不复制真实账户或现有内部账户持仓。
- 所有 Agent Paper 周期继续保持 `execution_locked=true`。这里的 lock 表示没有券商/实盘执行权限，不阻止现有内部 paper ledger 模拟。
- challenger prompt/model 至少有一项与 incumbent 不同，且整个评价期间固定。
- 训练 slot 和 OOS slot 在第一次 challenger admission 前冻结，互不重叠；不能依据结果移动窗口、修改指标或阈值。
- `PASS` 只改变后续内部 Agent Paper 的版本选择权威；不授予券商模拟盘或实盘权限。

## 5. 持久化权威

新增三个小型 append-only 事实表；不修改旧 Shadow 表，不把试验字段塞入 `agent_paper_cycle_events.fact_ids`。

### 5.1 `agent_version_evaluations`

一行冻结一次评价：

- `id`
- `incumbent_account_id`：隔离的 incumbent 内部评价账户
- `challenger_account_id`：隔离的 challenger 内部评价账户
- `instrument_id`：AAPL
- `incumbent_version_source_run_id`
- `challenger_prompt_version_id`：完整 prompt authority 的 orchestrator 入口
- `challenger_model_config_id`
- 冻结的 incumbent/challenger `prompt_hash`、`model_hash`
- `train_slots`、`oos_slots`：有界且互不重叠的 UTC slot 数组
- `policy_version = agent_version_evaluation_v1`
- `policy_hash`、`authority_hash`
- `created_at`

注册时加载并校验完整八角色 prompt authority，计算 challenger manifest hash；这里只冻结配置，不创建研究 run、Job 或模型调用。

### 5.2 `agent_version_cycle_pairs`

每个 slot 一行配对事实：

- `evaluation_id`
- `phase`：`train` 或 `oos`
- `decision_slot_at`
- `incumbent_cycle_id`
- `challenger_cycle_id`
- `incumbent_run_id`
- `challenger_run_id`
- `market_evidence_hash`
- `starting_projection_hash`
- `pair_hash`
- `created_at`

唯一约束保证一个评价、一个 slot 只有一对；两侧 cycle/run 不能重复跨评价使用。插入守卫验证同一 AAPL、同一 slot、同一政策、相同非账户研究证据及等价起点。任一条件不满足时整对拒绝，不生成“部分可比”记录。

### 5.3 `agent_version_evaluation_decisions`

评价终态一行：

- `evaluation_id`（唯一）
- `status`：`PASS`、`FAIL` 或 `UNKNOWN`
- `metric_report`、`metric_hash`
- `selected_version_source_run_id`
- `reason_code`
- `decision_hash`
- `created_at`

`PASS` 时 selected version 必须是已完成 OOS 配对中按 slot 排序最早的 challenger run；`FAIL/UNKNOWN` 必须继续选择评价注册时的 incumbent source run。结果是不可变事实，不更新评价或历史周期。

## 6. 数据流

### 6.1 注册

正常产品 CLI/API 的本地管理入口接收 incumbent source run、challenger prompt/model 和预先确定的 train/OOS slots。服务只验证并写入 `agent_version_evaluations`，不调用模型。重放相同 authority hash 返回原评价；同一 idempotency key 不同内容 fail closed。

### 6.2 配对周期

到达冻结 slot 后：

1. 版本评价入口先查两个评价账户的 `cycle -> job -> outputs`，已有事实只读回或恢复。
2. 为两个隔离评价账户创建同 slot cycle；两侧候选范围、初始资本和政策相同。
3. incumbent admission 显式使用评价冻结的 incumbent source run，不读取 actor 的可变最新 run。
4. challenger admission 显式传入评价中冻结的 prompt/model authority，actor 使用独立固定 namespace；禁止使用“该 actor 最新版本”推断配置。
5. 两侧都通过原生 `AGENT_PAPER_RESEARCH` Job 和原生 resume 流程。challenger 也经过确定性 sizing、风险、内部订单、成交、账本与绩效；两侧事实分别写入各自的隔离账户。
6. 两侧 run 可恢复且 authority 等价后插入 cycle pair。已有 run/job/output 只读取或恢复，不因流断开重放模型。

### 6.3 训练与 OOS

- train 只用于检查候选是否可运行、错误模式及预先冻结评估政策；不得根据 train 结果修改候选后仍沿用同一 evaluation。
- prompt/model 有任何变化都必须注册新的 evaluation 和新 OOS 窗口。
- OOS 只读取预先冻结的 slots；缺失或不可比 slot 保留为缺口，不顺延挑选有利样本。

### 6.4 评价与选择

评价服务复用现有 `PaperPerformanceSnapshotRecord`、订单、成交和 ledger 事实，按同 slot 计算：完成率、合理 abstain/unavailable、净值变化、最大回撤、换手、费用、风险拒绝、订单/成交完整性，以及相对 `hold_current`、`baseline_v1` 和 SPY 的同口径差异。

确定性判定规则：

- authority、起点、窗口、成本或对照不完整：`UNKNOWN`。
- 任一硬风控、账本守恒、重复事实或不可恢复失败：`FAIL`。
- 没有足够 OOS 样本，或两侧始终等价/no-trade，无法证明改善：`UNKNOWN`。
- 只有所有预设门禁满足且 challenger 在 OOS 的预设主指标上可靠优于 incumbent，才可 `PASS`。

首版实现必须把具体最小样本数和数值阈值写入 `agent_version_evaluation_v1` 并测试，不能在读取结果后修改。真实运行尚未积累到门槛时，页面显示 `continue_observing`，但不阻塞第四、第五阶段的接入就绪开发。

`agent_version_evaluation_v1` 固定采用以下窗口和 PASS 门禁：

- train 恰好 4 个 slot，覆盖至少 2 个不同交易日；只验证可运行性，不能贡献 PASS 指标。
- OOS 恰好 20 个 slot，覆盖至少 10 个不同交易日，每日最多 2 个 slot，且全部晚于 train。
- 20 个 OOS pair 必须全部形成可比终态；缺一个、补换一个或 authority 不一致均为 `UNKNOWN`。
- 两侧不得有硬风控违规、账本不守恒、重复订单/成交或不可恢复失败。
- challenger 与 incumbent 至少 3 个 OOS slot 的最终 sizing action 不同，且 challenger 至少产生 1 个由现有内部模拟链完成的 fill；否则结果信息不足，为 `UNKNOWN`。
- challenger 相对 incumbent 的 OOS 税费后时间加权累计收益至少高 `0.005`，10 个交易日中至少 7 日的当日净超额为正。
- challenger 最大回撤相对 incumbent 的恶化不得超过 `0.0025`；所有费用必须来自同一冻结 cost policy，不能用零费用替代缺失费用。
- 同时满足以上全部条件才是 `PASS`；出现硬安全或账本失败为 `FAIL`；其余情况为 `UNKNOWN`。

这些阈值只用于首个 AAPL Agent 版本评价，不宣称具有跨标的统计普适性。修改任何窗口或阈值都必须发布新的 policy version，并创建新的 evaluation，不能重判旧结果。

### 6.5 后续内部版本选择

普通 Agent Paper admission 的版本来源优先读取最新有效 `PASS` 评价；没有 PASS 时兼容现有冻结版本。它仍按完整 manifest/model hash 载入，不读取可变“最新 prompt”。任何 `FAIL/UNKNOWN` 都不会切换版本。

该选择只影响后续内部 Agent Paper 研究。券商模拟和实盘仍需要各自独立 authority，且全局 `execution_locked=true` 保持不变。

## 7. 失败与恢复

- 注册失败：不写 evaluation，不创建 Job。
- 一侧 cycle 已存在：读取原 cycle/run/job/output 并恢复；禁止为补齐配对重复 admission。
- 一侧暂未完成：配对保持未形成，读取端显示 awaiting peer；不把观察超时当失败。
- Job succeeded 但 cycle 未 resume：使用原 output 恢复同一周期。
- 两侧 evidence/起点冲突：记录确定性不可比结果，不继续执行该 slot。
- 任一评价账户出现 broker binding、外部 command、非评价 slot 或 `execution_locked=false`：立即 fail closed，评价不得 PASS。
- 评价 finalization 幂等；重复请求返回原 decision，冲突请求拒绝。

## 8. 产品读回

扩展现有 `/paper` 读模型和页面，不新增独立控制台：

- 当前 incumbent 与 challenger 的完整 prompt/model hash、生命周期状态。
- train/OOS 预设窗口及已完成/缺失/不可比 slot 数。
- 两侧同口径绩效、风险、订单/成交和基线比较。
- `PASS/FAIL/UNKNOWN`、reason code 以及当前内部选中版本。
- 明确显示 `execution_locked=true`、`broker_write=false`、`live_write=false`。

页面没有“立即晋升”“发送订单”或“解锁”按钮。

## 9. 验证

### 数据库与迁移

- 迁移从当前 head 单步升级与降级；表、FK、唯一约束、哈希和阶段约束准确。
- 非 AAPL、重叠窗口、相同版本、账户相同、cycle/run 重用、证据不等价和起点不等价均被拒绝。
- 两个评价账户都不能存在 broker authority；只能存在冻结评价 slot 的 cycle，且必须 `execution_locked=true`。

### 服务与恢复

- 注册不创建 Job 或模型调用。
- 同一 slot 的重复调度只读回原周期、run 和 Job。
- succeeded output 后恢复不会再次调用模型。
- challenger 使用显式冻结版本，不随 actor 的其他历史 run 漂移。
- train 改版本必须新建 evaluation；OOS slot 不可补换。

### 完整内部模拟链

- fixture 覆盖 no-trade、下单/全成、部分成交、风险拒绝、不可用与结果未知。
- 两侧分别核对 decision -> batch -> order -> fill -> ledger -> performance，同账户内守恒且互不污染。
- 评价对不完整 authority 给 UNKNOWN，对硬风险/账本错误给 FAIL；只有满足冻结门槛才 PASS。

### API/BFF/页面

- DB -> API -> BFF -> `/paper` 同一 evaluation readback。
- 空状态、观察中、UNKNOWN、FAIL 和 PASS 均有显式展示。
- 版本历史仍只有一个 incumbent；challenger 不被误标为当前版本。
- 页面、API 与数据库都显示 execution lock，且不存在外部订单事实。

## 10. 分阶段实施与回滚

1. 先实现迁移、模型、注册服务和只读 projection；此时绝不 admission challenger。
2. 再实现显式版本 pin、隔离账户周期配对及 fixture 端到端验证。
3. 再实现冻结评价政策、finalization 与 `/paper` 读回。
4. 最后才用一个有界 AAPL 评价做真实模型调用；先查持久化状态，费用受既有 daily budget 约束。

代码回滚可删除尚未被运行入口引用的新读写路径；数据库 downgrade 只允许在新表为空时执行。已有 evaluation/pair/decision 事实不得通过改写历史迁移删除。任何真实观察所需市场时间通过一次性定时补证，不阻塞第四、第五阶段的接入就绪工作。

## 11. 明确不做

- 不恢复或扩建 `strategy_shadow_*`。
- 不新增 Universe 轮换、多标的候选或新行情源。
- 不复制真实券商持仓，不接触交易密钥。
- 不发送券商模拟盘或实盘订单。
- 不让 LLM 修改 sizing、硬风控、费用、评价阈值或版本选择结果。
- 不因 no-trade、短窗口或页面可用宣称 challenger 已优于 incumbent。
