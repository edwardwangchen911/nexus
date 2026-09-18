# 主动建仓与组合管理第一切片技术设计

**日期：** 2026-09-18

**状态：** 架构已确认；政策参数待确认；待实施计划

**产品依据：**

- `docs/design/nexus-portfolio-management-product-contract-v0.1.md`
- `docs/design/nexus-portfolio-management-first-slice-design-handoff.md`

## 1. 目标

把当前固定 AAPL 的逐标的 Agent Paper 验证链，扩展成一个有界的组合级内部模拟闭环：显式选择 1–5 个已批准股票池成员，分别完成证据受控的单股研究，再进行一次组合级取舍汇总，由确定性配置规则形成完整目标提案；满足准入和资源占用约束时生成不可变生效目标，并复用既有硬风控、内部模拟订单、成交、预留、账本和绩效链。

第一切片必须能够区分并追溯：单股研究、组合级 Agent 取舍、确定性完整提案、生效目标、调整计划、订单/成交以及实际组合。任何一层都不得冒充后一层。

本设计不建设自动全市场扫描、完整政策编辑器、通用组合优化器、新交易系统或外部交易能力。真实账户保持只读，内部模拟周期继续 `execution_locked=true`。

## 2. 已核实现状与真实缺口

### 2.1 已有能力

- `AgentPaperCycleRecord` 已保存决策时点、股票池快照、候选 manifest、价格政策、sizing policy 和不可解除的执行锁。
- Agent Paper 已能为 manifest 中的多个候选逐一建立研究 Job；研究预算、证据绑定、结果恢复和不可重复模型调用已有基础。
- `StrategyDecisionRecord`、`StrategyDecisionTargetRecord` 和 `cash_weight` 已能保存一份完整、不可变的目标配置提案。
- `create_paper_order_batch` 已接通确定性数量差额、硬风控、订单、预留、成交、账本与绩效。
- 订单取消已有本地幂等键和 `PaperRequestIdempotencyRecord`，但现有取消入口在服务端未找到幂等记录时会真正执行取消。
- Web 已有 `/decisions`、`/paper`、`/workspace`、`/portfolio`，无需新增一级页面。

### 2.2 不能靠接线掩盖的缺口

1. 正常 Schwab Agent Paper 入口从风险证据采集、行情冻结、候选准备到 manifest 校验都写死 AAPL；仅放宽 `CandidateManifestEntry.symbol` 不会形成真实多候选能力。
2. 当前研究输出按标的独立产生，`_resume_research` 只是把多个方向交给固定 sizing；不存在“为什么选 A 而不是 B、为什么以 A 替换 C”的组合级判断。
3. 当前 sizing 从实际持仓权重开始，未研究持仓会隐式继承实际状态，违反“从生效目标继承、实际只用于计算执行差额”的合同。
4. 周期在发现未结订单时直接停止研究，无法形成等待中的新提案；阻断位置过早。
5. 没有独立、不可变的生效目标身份，无法区分完整提案、目标生效和实际持仓。
6. `PaperOrderBatchRecord` 只关联策略决策，不能稳定表达历史计划归属哪个生效目标，也不能表达同一目标在以后经重新校验产生的顺序计划。
7. 当前取消重放入口不是纯读取；直接用于“核对原操作结果”可能产生新的取消副作用。
8. 组合配置、初始目标和调仓参数尚未获用户批准；当前 `agent_paper_sizing_v1` 不能被静默升级为正式组合政策。

## 3. 方案选择

### 3.1 目标与计划：采用薄目标激活层

继续使用 `StrategyDecisionRecord` 及其 targets/cash 作为完整提案；新增一个很薄的不可变目标版本事实；继续使用 `PaperOrderBatchRecord` 作为调整计划。拒绝从“最新决策”推导当前目标，也拒绝另建完整 proposal/target/plan 子系统。

### 3.2 多候选：采用显式有界候选批次

正常运行只接受显式的 1–5 个候选，不从观察列表、全市场或“最新可用标的”自动扩展。候选批次绑定账户、股票池快照和规范化标的身份；正常 runner 必须收到其 ID，不保留隐式 AAPL fallback。

### 3.3 组合判断：采用一次有界组合级汇总

逐标的研究完成后追加一次组合级 Agent 汇总。它只能输出选择、暂缓、替换关系、现金理由、仓位职责、反方证据和失效条件；不能输出权重、金额、股数、订单或风控结果。每周期最多一次，使用冻结输入、独立 Job、受现有日预算约束，结果不完整时不形成完整提案。

不能用确定性代码机械拼接单股方向来冒充 Agent 组合取舍，也不新增多 Agent 讨论、自动辩论或通用优化器。

## 4. 核心不变量

- 单一内部模型组合，同一时刻最多一个生效目标。
- 同一时刻最多一个占用现金或持仓的调整计划；“计划已对账”不自动等于资源已释放。
- 新提案不等于目标生效；目标生效不等于计划建立；计划建立不等于订单或成交。
- 未研究持仓从当前生效目标原样继承，不隐式重新分配、不更新研究有效期。
- 实际持仓、现金和预留只用于执行差额与核对，不自动成为首次目标或下一目标。
- 相同完整目标内容不创建新目标版本；研究证据更新可以形成新判断，但不能仅因此制造目标版本。
- 同一目标以后可以在重新校验后建立新的顺序计划，但不得重复已成交或未结订单覆盖的需求。
- 风控缩减或拒绝不回写目标，也不自动变成以后补单。
- 所有历史提案、目标、计划、订单和成交保持原归属，不随新目标重标。
- 页面刷新和读取接口只读持久化事实，不触发行情、模型、取消、订单或目标生效。

## 5. 持久化设计

### 5.1 `agent_paper_candidate_batches`

新增一张小型不可变表，表达一次明确的候选范围：

- `id`
- `account_id`
- `universe_snapshot_id`
- `candidate_instrument_ids`：规范化、去重、按 UUID 稳定排序，长度 1–5
- `source`：首版固定 `explicit_research_scope`
- `batch_hash`：覆盖账户、股票池快照、候选和 schema version
- `created_at`

约束：

- 候选必须都是同一已批准股票池快照中的有效 USD 美股/ETF；身份、别名和成员资格在创建时与触发时都重新验证。
- 相同内容重放返回同一批次；相同幂等键不同内容拒绝。
- 批次不是交易资格、研究结论或目标提案；它只定义允许进入本轮证据链的范围。
- `AgentPaperCycleRecord` 新增可空 `candidate_batch_id`。新正常周期必须有该关联；历史周期保持空值，不回填猜测事实。

首版提供本地产品 CLI/service 创建批次；现有研究入口以后可以调用同一 service。第一切片不新增候选管理页面。

### 5.2 组合级汇总 run/output

新增与现有单股研究隔离的窄事实：

`agent_portfolio_synthesis_runs`

- `id`、`cycle_id`（唯一）
- 冻结的 candidate batch、单股 research output IDs、当前生效目标 ID、paper projection hash
- prompt/model authority hash、input hash、idempotency key
- `status`：`prepared/running/completed/abstained/failed`
- 定价版本、已知 token/成本和时间事实

`agent_portfolio_synthesis_outputs`

- `id`、`run_id`（唯一）
- `status`
- 严格 schema 的组合取舍 intent
- `content_hash`
- 完整 evidence/output binding hash
- `created_at`

`agent_portfolio_synthesis_budget_reservations`

- `id`、`business_date`、`run_id`（唯一）
- `pricing_version`、`reserved_usd`、`settled_usd`
- `state`：`reserved/settled/released`

预算预留使用与现有 Agent 日预算相同的账户级上限和定价函数，但保留独立的 synthesis reservation 事实，不能借单股 Job 的预算记录重复结算。实现复用现有 worker、Codex 调用和 usage accounting 代码，不复用只支持 `current_holdings_only` 的 `PortfolioDecisionRunRecord` 合同。

组合 intent 至少表达：

- 本轮实际比较的候选和已有持仓范围；
- 拟纳入、增加、降低、退出、维持或暂缓；
- 候选之间的优先与替换关系；
- 主动现金理由、依赖、反方证据和失效条件；
- 核心/战术职责（如适用）。

它不得包含目标权重、金额、股数、订单动作或修改硬风控的指令。所有引用必须来自冻结的单股输出、当前目标和模拟组合上下文。

`StrategyDecisionRecord` 新增可空 `portfolio_synthesis_output_id`。历史决策及单标的版本评价保持空值；新组合 proposal 必须指向一个 completed synthesis output。新 proposal 的 decision hash 同时覆盖 synthesis intent hash、确定性 allocation policy hash 和完整 targets/cash，防止替换组合判断后沿用旧配置身份。

### 5.3 `paper_target_versions`

新增不可变目标激活事实：

- `id`
- `account_id`
- `portfolio_id`
- `version_seq`：账户内单调递增
- `strategy_decision_id`：形成该目标内容的完整提案
- `previous_target_version_id`：首次目标为空
- `target_content_hash`：仅覆盖完整证券权重、现金权重和配置语义
- `activated_at`
- `activation_hash`
- `created_at`

数据库约束保证账户/组合一致、账户内版本唯一、一个决策最多激活一次、previous 关系不跨账户且不分叉。当前生效目标是该账户最高 `version_seq`，不增加可变 `active` 布尔值。

目标表不重复保存 strategy/policy version 或 parameters hash；这些继续由关联的 `StrategyDecisionRecord` 提供。目标内容由该 decision 的 targets 和 cash 读取，历史记录不可回写。

### 5.4 调整计划关联

`PaperOrderBatchRecord` 新增可空 `target_version_id`：

- 新组合管理计划必须关联目标版本。
- 历史 batch 保持空值，读取端显示“历史记录/未关联目标”，不伪造版本。
- `strategy_decision_id` 表达本次形成或重新校验计划的判断，`target_version_id` 表达计划要实现的目标；两者可以不同。
- 目标发生变化时，两者通常来自同一周期；目标不变但实际偏差以后需要处理时，可以由新 decision 为既有 target 创建新的顺序 batch。
- 现有 `(account_id, strategy_decision_id)` 幂等约束继续保证同一判断只创建一个 batch。

## 6. 候选与证据链

### 6.1 正常入口

`AgentPaperTriggerRequest` 和常驻 runner 显式携带 `candidate_batch_id`。runner 从受保护的本地运行配置读取该 ID；配置缺失、批次不存在、账户不匹配、股票池快照失效或候选超界时停止为 `CANDIDATE_BATCH_REQUIRED/INVALID`，不退回 AAPL。

触发 reservation 的 canonical request hash 必须包含 candidate batch ID/hash，避免同一时间槽以不同候选重放。

### 6.2 有界外部证据

现有 provider 机制已经支持多 symbol 底层订阅，首版只泛化固定 AAPL 包装：

1. 按候选批次解析稳定的 symbol→instrument binding。
2. 单次 Schwab Level One capture 订阅全部 1–5 个 symbol，要求每个候选都有合格 quote；同一 capture artifact 可以绑定多个候选。
3. Nasdaq listing/halt 原始来源每轮各采集一次，再按候选分别解析；Alpaca corporate action 按标的有界采集。
4. 每个候选分别持久化并验证 risk authority；任一候选证据不完整时，该候选不能进入 manifest。首版要求请求批次全体合格才启动组合研究，避免“输入集合悄然缩水”改变比较含义。
5. `prepare_candidates` 改为接收明确候选 IDs，不再自行查询 AAPL；`validate_candidate_manifest` 接受 1–5 个唯一候选并逐项回读精确证据。
6. `CandidateManifestEntry.symbol` 改为受校验的普通 symbol 字符串；历史 AAPL manifest 仍可读取。

### 6.3 版本评价兼容

现有 Agent 版本并行评价仍严格限定 AAPL。其候选来自 `agent_version_evaluations.instrument_id` 的冻结事实，显式传入通用 candidate preparation；不读取正常 runner 的候选批次，也不因通用 manifest 支持多标的而扩大评价范围。

## 7. 决策、提案与目标激活流程

### 7.1 逐标的研究

沿用现有 manifest→research Job→output admission。多个 Job 共享同一冻结 paper projection 和候选证据时点。连接中断先查 run→job→output；已存在 terminal output 时只恢复，不重复模型调用。

### 7.2 组合级汇总

所有单股输出通过准入后，创建或恢复唯一 synthesis run：

1. 冻结单股 output IDs/hashes、当前生效目标、实际组合只读快照、候选批次和适用政策身份。
2. 调用一次组合汇总；失败、超预算、引用越界或 schema 不合格时周期进入明确 unavailable，不继续配置。
3. 持久化原始组合取舍及绑定，随后才允许确定性配置。

旧计划占用不阻止逐标的研究或组合汇总；它只阻止目标替代和新的占用计划。

### 7.3 确定性完整提案

新的 allocation service 接受：当前生效目标、合格组合 intent、已确认政策和证据 authority。

- 有生效目标：从其完整 targets/cash 原样开始，仅修改 intent 明确覆盖且政策允许的项目。
- 无生效目标：必须有用户确认的初始目标来源和 allocation policy；不得复制实际持仓。缺失时记录 `POLICY_UNCONFIRMED`，不创建 `StrategyDecisionRecord` 完整提案。
- 未研究持仓保持原目标；候选遗漏不代表退出。
- Agent 只决定投资取舍，确定性代码计算权重和现金并生成完整、总和为 1 的 proposal。
- proposal 继续保存到 `StrategyDecisionRecord`、targets 和 Agent/synthesis bindings。

当前 `agent_paper_sizing_v1` 的固定 2 个百分点步长和 10% 单周期预算只作为历史 Agent Paper 参数存在，不能未经确认成为新 allocation policy。

新组合管理周期不原地改变 `AgentPaperDecision` v1 的含义。实现新增版本化的 portfolio proposal contract，并调整数据库守卫同时接受：历史/版本评价使用的 `agent_paper_decision_v1 + agent_paper_sizing_v1`，以及候选批次非空的新组合 proposal schema + 已批准 allocation policy hash。旧记录继续按原 validator 读取，新旧 policy 不相互推断或静默转换。

### 7.4 激活与计划协调器

新增聚焦的 `paper/target_service.py`，在账户级 advisory transaction lock 下完成：

1. 重读当前生效目标、实际 projection、未结订单、预留、未知结果和已有计划。
2. 验证 proposal、政策、证据时效及完整目标 hash。
3. 若目标内容变化且资源可切换，插入下一 `paper_target_versions`。
4. 若目标内容不变，沿用当前版本，不插入新版本。
5. 基于将要生效或继续生效的目标计算差额，扣除已成交和未结订单已覆盖需求。
6. 只有政策要求调整、没有占用计划且差额未被覆盖时，调用现有 `create_paper_order_batch` 并绑定 target version。
7. 目标、计划、事件在同一事务提交；计划创建发生异常时不得留下孤立的新目标。

当旧计划仍占用资源或结果未知时，proposal 和 synthesis 事实可以保存，但原目标继续生效，不创建新 target/batch。前序结清后不能直接激活等待 proposal；新的周期必须按最新实际状态、政策和证据重新研究/校验。

“目标内容相同”不自动等于“无需交易”。同目标的新计划仅可在旧计划完全结清、适用再平衡政策明确要求、证据重新校验且差额未被订单覆盖时建立。风控上一轮未允许的差额不自动重试。

### 7.5 周期状态映射

第一切片不另建一套 proposal 状态机。现有 cycle 状态继续表达运行进度，细分业务结果使用不可变 event `result_code` 和关联事实：

- 单股研究或 synthesis Job 尚未结束：保持 `researching`，resume 只读取或推进原 Job。
- synthesis 完成且完整 proposal 已保存：进入 `decision_recorded`。
- 政策未确认、synthesis 不合格或输入不足：终止为 `unavailable`，使用 `POLICY_UNCONFIRMED`、`PORTFOLIO_SYNTHESIS_UNAVAILABLE` 等明确 result code。
- proposal 因前序计划占用而未激活：周期可正常终止为 `completed`，result code 为 `TARGET_ACTIVATION_BLOCKED`；这不表示目标或计划完成。
- target 已激活但无需订单：`completed/NO_ORDER_REQUIRED`，目标生效事实仍独立存在。
- 建立 batch 后：沿用现有 `risk_evaluated`、`awaiting_market`、`partially_filled` 和 `completed` 路径。

为持久化 synthesis Job identity，可以增加一条不改变业务阶段的 cycle event，或由 synthesis run 的唯一 `cycle_id` 关联读取；实现优先采用后者，避免为了一个内部步骤扩充 cycle 状态枚举。

## 8. 资源占用判定

首版不新增可变 plan status 表。占用由现有事实投影得到：

- 存在未结订单；
- 存在未释放现金或持仓预留；
- 取消、成交或账本结果仍未知；
- 订单、成交、账本与 projection 尚未达到可确认一致。

只有所有相关订单进入已确认终态、预留释放或结清、账本/持仓/现金核对一致且没有未知结果时，计划才不再占用资源。服务停止、观察结束、取消请求发送或一次对账动作完成均不充分。

## 9. 原取消结果的纯读取核对

新增：

`GET /internal/v1/paper/orders/{order_id}/cancel-result`，原取消尝试键通过现有风格的 `Idempotency-Key` header 传入，不放在 URL/query 中。

该入口只能查询：

- `PaperRequestIdempotencyRecord`
- 原订单及事件链
- 已确认成交
- 当前现金/持仓预留
- 关联 batch 和 target version

它不得调用 `cancel_internal_paper_order`，不得写订单事件、释放预留、修改账本、创建目标/计划或触发重新校验。

返回必须区分：

- `COMMITTED`：服务端存在匹配且已提交的取消幂等事实；
- `NOT_RECORDED`：服务端没有该幂等事实，只能同时返回当前权威订单/预留状态；
- `CONFLICT`：键、订单或请求哈希不匹配；
- `UNKNOWN`：持久化事实自身冲突或无法安全核对。

客户端生成的尝试键不能显示成服务端已确认的“取消请求 ID”。若网络超时后读取为 `NOT_RECORDED`，页面只说明服务端未记录该取消结果，不自动重发取消。现有显式取消能力继续存在，但与该读取按钮完全分离。

这项语义会在实现时同步修正设计交付说明中“原取消请求已发出”的过强表述。

## 10. 读取 API 与页面

### 10.1 API/read model

- 扩展 `GET /internal/v1/paper/cycles/{cycle_id}`：候选批次、逐标的研究、组合 synthesis、完整 proposal、当前 active target、激活结论、阻断计划及 plan 摘要。
- 新增 `GET /internal/v1/paper/plans/{batch_id}`：精确读取目标归属、决策依据、风控、订单/成交、预留、实际差额及占用投影。
- 扩展 paper portfolio/readiness response：当前目标摘要、当前占用计划、最新 proposal/判断摘要；workspace 和 portfolio 只消费该摘要并跳转详情。
- 历史缺少 target/candidate batch 关联时显式返回 legacy/unassociated，不猜测补链。
- 所有目标、金额、权重、现金和状态由服务端投影，前端不自行计算业务事实。

### 10.2 页面范围

- `/decisions`：组合级取舍、确定性完整提案、当前生效目标、激活/阻断结论和关联计划。
- `/paper`：计划归属、时间线、风控、订单/成交、预留、目标/实际差异和纯读取取消核对。
- `/workspace`、`/portfolio`：只增加最小摘要和详情入口。
- `/market` 与一级导航不变。

页面必须分别标注 Agent 原始判断、确定性配置、生效目标、风险结果和实际事实；不得用一个总状态合并。刷新只重新读取持久化数据。

## 11. 失败、恢复与幂等

- 候选批次无效：provider 和模型都不调用。
- 部分 provider 证据缺失：不悄然缩小候选集合，不 admission 研究。
- 单股 Job 或 synthesis Job 已完成：恢复原输出，不重新调用模型。
- synthesis 不完整：不生成确定性 proposal。
- 政策未确认：保留研究与 synthesis 事实，显示具体缺项，不猜默认值。
- 同目标 hash：返回当前 target；只有明确的同目标再平衡条件才可能建立新 batch。
- 并发激活：账户 advisory lock、version unique 和 previous-chain constraints 保证最多一个新版本。
- batch 创建失败：同事务回滚新 target 和关联事件。
- 旧计划占用：proposal 保存但不激活；不得自动轮询后直接启用。
- 读取取消结果超时：保持最后确认事实和占用，不重发任何写操作。

## 12. 迁移与兼容性

迁移只做加法：

- 新增 candidate batch、synthesis run/output、target version 表及必要预算事实；
- `agent_paper_cycles.candidate_batch_id` 可空；
- `strategy_decisions.portfolio_synthesis_output_id` 可空；
- `paper_order_batches.target_version_id` 可空；
- 泛化 manifest symbol/数量的应用校验，不改写历史 JSON；
- 保留旧 AAPL cycle、decision、batch、order、fill 和 ledger 的读取。

升级后旧入口在没有显式 candidate batch 时 fail closed，不再隐式选择 AAPL；版本评价使用自己的冻结 AAPL authority，不受影响。

代码回滚可以关闭新入口并恢复旧记录只读；已有新 target、proposal、plan 和 synthesis 事实不得删除或重标。数据库 downgrade 只允许在新增表为空且新增 FK 未被使用的隔离测试环境执行，不能作为已运行环境的常规回滚方案。

## 13. 验证策略

### 13.1 候选与证据

- 1、5 个候选成功；0、6、重复、跨账户、跨股票池、无效身份拒绝。
- Schwab 单次 capture 完整覆盖请求集合；少一个 quote 则全批次不 admission。
- Nasdaq 公共原始证据每轮只采一次，逐标的解析和 risk authority 完整。
- 正常 runner 缺 batch 不回退 AAPL；版本评价仍只允许显式 AAPL。

### 13.2 组合汇总与政策

- 多个单股结果只产生一个 synthesis run/output；恢复不重复模型调用。
- 输出含权重、金额、股数或订单时拒绝。
- 引用不在冻结输入、候选越界、portfolio projection 不匹配时拒绝。
- 预算不足安全停止，不占用交易资源。
- `POLICY_UNCONFIRMED` 不产生完整 proposal、target 或 batch。

### 13.3 目标与计划

- 首次目标必须有明确来源，不复制已有实际持仓。
- 未研究持仓继承 active target，不继承实际权重、不更新研究有效期。
- 同 target hash 不创建版本；合法变化只创建一个下一版本。
- 并发激活只提交一个版本和至多一个 batch。
- 旧计划占用时允许研究/proposal，但 target/batch 不变化。
- 前序结清后旧 proposal 不自动激活，必须新周期重新校验。
- 同目标偏差在政策允许时可以建立顺序计划；已成交、未结订单和上轮风险缩减不重复生成需求。
- target、batch、order、fill、ledger 和 projection 跨层归属一致。

### 13.4 只读核对

- `COMMITTED/NOT_RECORDED/CONFLICT/UNKNOWN` 四类结果准确。
- 首次和重复 GET 都不新增或修改订单事件、幂等记录、预留、账本、target 或 batch。
- 迟到响应不能覆盖当前选择；无关联时不猜测其他订单。

### 13.5 API 与 UI

- DB→API→生成客户端→BFF→页面使用同一 proposal/target/plan identity。
- 首次目标、无变化、部分完成、未知、取消已确认但预留未释放、政策未确认均有明确展示。
- desktop/mobile 数值、币种、分母和 as-of 一致；关键状态可键盘和读屏访问。
- 页面刷新不产生 provider、模型或交易写入。

自然运行验收仍要求至少一次由真实合格证据自然产生的可执行建仓意图，并贯穿 proposal→target→plan→risk→order→fill→ledger→performance；只有 no-trade 或 fixture 通过不算自然建仓闭环完成，也不得为验收强迫交易。

## 14. 实施切片边界

后续实施计划应按依赖拆成四个可独立验证的切片：

1. **候选与证据底座：** candidate batch、正常 runner 显式输入、多 symbol provider/risk manifest；不改目标和页面。
2. **组合判断与目标激活：** synthesis、确定性 allocation 接口、target version、plan 关联和占用协调；政策未确认分支先可验证。
3. **只读合同：** cycle/plan/portfolio projections、纯读取取消结果、生成客户端。
4. **产品读回与完整验收：** decisions/paper 主区域、workspace/portfolio 摘要、移动端及跨层真实案例。

共享 schema 和状态合同先完成再并行；同一 worktree 仅一个写入者。每个切片都必须保留历史兼容和 `execution_locked=true`，不能用后续页面或 fixture 替代前一切片的服务验收。

## 15. 实施前政策门槛

技术实现可以先交付 `POLICY_UNCONFIRMED` 和所有安全阻断，但在允许生成首个生效组合目标前，用户必须明确批准本切片实际使用的最小政策：

- 复用哪个内部模拟账户、初始目标来源和模拟本金口径；
- 候选可投资边界及允许证券类型；
- 配置档位或标准仓位、候选优先级如何转换为资金；
- 最低/目标现金、单标的集中度及换手上限；
- 目标偏离容忍、最小调整要求及同目标再平衡条件；
- 研究/提案有效期和运行频率。

这些参数批准后应发布为一个内容寻址、代码可重放的 allocation policy version，并由 `StrategyDecisionRecord.strategy_parameters_hash` 绑定。第一切片不建设完整政策编辑器，也不把本文或概念图中的演示比例当作默认值。

## 16. 明确不做

- 不做全市场扫描、观察列表自动准入、事件驱动机会漏斗或自动候选轮换。
- 不做多个组合、多策略资金竞争、复杂优化器或 Agent 自行给权重。
- 不复用受限的真实持仓组合诊断合同来伪装主动建仓。
- 不重建订单、成交、预留、账本或绩效系统。
- 不新增手动“批准执行”“一键建仓”“立即生效”按钮。
- 不接入券商模拟写入或实盘，不修改凭据、部署或 `execution_locked=true`。
- 不因文档、测试、页面可见或 no-trade 结果宣称主动建仓已经自然验收完成。
