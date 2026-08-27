# NEXUS Phase 2 策略进化实验室设计规格

- **状态：** 已授权自主实施，整体规格自审通过后直接进入编码计划
- **日期：** 2026-08-27
- **基线：** `codex/nexus-phase-0-1@c6d20b0`
- **范围：** 美股、受限策略候选、确定性回测、Strategy Shadow、内部模拟冠军晋级与回滚
- **明确排除：** 真实账户写入、复星模拟/实盘订单、部署、密钥或权限变更、Prompt/模型/角色自进化

## 1. 目标与成功标准

Phase 2 在已有时点正确数据、受限策略 DSL、确定性 `baseline_v1` 回测、Proposal Shadow、内部模拟账本、硬风控、Job/lease/outbox 和审计链之上，建立一个可追溯的策略进化闭环：

```text
冻结证据
  -> strategy_rnd Agent 提出实验假设与搜索轴
  -> 确定性候选生成与静态准入
  -> 不可变实验与 Walk-forward 回测
  -> 冠军/挑战者确定性比较
  -> 独立 Strategy Shadow
  -> 交接预检
  -> 自动晋级内部模拟冠军
  -> 下一安全交易边界生效
  -> 可审计一键回滚
```

完成时必须同时成立：

- Agent 不能生成或执行任意代码，不能写数据库、入队 Job、修改硬风控、访问券商或交易密钥。
- 所有候选、实验尝试、回测证据、Shadow 结果、晋级决定、冠军切换和回滚均可重放、可解释、不可改写。
- 历史评估不含 LLM 逐 Bar 执行或 Agent overlay；同一比较使用完全一致的 authority、成本、日期和执行模型。
- 只有版本化 `promotion_policy_v1` 可以决定内部模拟冠军晋级，Agent 和前端均无此权限。
- 自动晋级只影响未来内部模拟决策；不会创建、取消或修改真实订单，也不会启用任何券商 capability。
- 回滚不改写历史账本、不撤销历史成交，只追加事件并在下一安全交易边界切换未来策略。

## 2. 产品边界与核心不变量

### 2.1 Phase 2A 包含

- 一个固定、版本化、只读的 `strategy_rnd` Agent 角色。
- Agent 提出的经济假设、证据引用、失效条件和最多三个已登记搜索轴。
- 由确定性生成器创建的参数与已登记因子候选。
- 至少七年数据、3 年训练 + 1 年验证 + 6 个月 OOS、按季度滚动的 Walk-forward。
- SPY、同版本等权白名单和当前内部模拟冠军的同口径比较。
- 63 个评价 session、5 个运营宽限 session、至少 20 笔完全成交订单的独立 Strategy Shadow。
- 自动晋级内部模拟冠军、暂停/否决、冠军历史和一键回滚。
- `/strategies` 中的策略进化工作台和完整错误/空/过期状态。

### 2.2 Phase 2A 冻结

- 市场固定为现有版本化美股池；`00656.HK` 不进入候选或交易。
- 再平衡固定为每周最后一个交易 session，执行固定为下一交易日开盘模型。
- 不增加日内、高频、卖空、杠杆、期权、期货、外汇或加密资产。
- Agent Prompt、模型配置和角色结构可以版本化发布，但不能由 Agent 自动修改；自进化属于后续 Phase 2B。
- 新因子实现只能通过开发、审查和测试加入特征注册表，Agent 不能发明实现。
- 复星 OpenAPI、真实账户、模拟券商账户和实盘路由完全不参与本阶段。

### 2.3 权限根

| 权限 | Agent | 确定性 evolution 服务 | 用户 Web 操作 | broker/live 域 |
|---|---:|---:|---:|---:|
| 读取冻结研究证据 | 是，受限工具 | 是 | 仅摘要 | 否 |
| 提出实验假设/搜索轴 | 是 | 可验证/持久化 | 可触发生成 | 否 |
| 生成候选策略 | 否 | 是 | 否 | 否 |
| 执行回测/Shadow | 否 | 是 | 可触发安全 Job | 否 |
| 修改硬风控 | 否 | 否 | 否 | 否 |
| 晋级内部模拟冠军 | 否 | 仅政策通过后 | 可暂停/否决/回滚 | 否 |
| 创建内部模拟订单 | 否 | 否；只发布冠军 | 否 | 否 |
| 创建真实/券商订单 | 否 | 否 | 否 | 本阶段未加载 |

## 3. 领域架构

新增 `nexus.evolution`，不把进化状态塞入 `agents`、`backtest`、`shadow` 或 `paper`：

- `proposal`：固定 Agent 契约、证据清单和实验建议验证。
- `candidate`：搜索空间政策、确定性生成、规范化、去重和 DSL 编译。
- `experiments`：lineage/family/experiment/event/attempt 和可重建状态投影。
- `evaluation`：候选筛选、OOS、冠军配对比较和结构化门禁。
- `strategy_shadow`：独立的冠军/挑战者虚拟账本、同源 authority、检查点和最终门禁。
- `registry`：内部模拟冠军事件、当前 head、暂停、晋级、回滚和并发生效。
- `handover`：从实际内部模拟组合到新冠军下一目标的成本与风险预检。
- `api`：只暴露受限命令和证据读模型，不接受任意 DSL。

允许复用的纯组件：

- `nexus.strategy.dsl` 的完整文档编译、哈希和反前视检查。
- `nexus.simulation` 的成本、订单和成交计算。
- `nexus.backtest` 的 authority、指标、归因和冻结结果。
- `nexus.shadow.authority` 中与 Proposal 无关的纯 point-in-time 选择逻辑；持久化和状态机不复用。
- `nexus.risk` 的硬风控评估。
- `nexus.jobs`、`nexus.audit` 和事务 outbox。

禁止依赖：

- `evolution` 不得 import broker provider、gateway 或任何写 capability。
- `paper` 不得调用 Agent；它只读取已生效的 registry head 和已编译策略。
- Agent 运行时不获得 SQLAlchemy session、Job repository、文件系统、网络、代码解释器或 provider client。

## 4. 候选与 Agent 契约

### 4.1 两级候选契约

Agent 只输出 `StrategyExperimentProposalV1`：

```python
class StrategyExperimentProposalV1(BaseModel):
    schema_version: Literal["strategy_experiment_proposal_v1"]
    parent_strategy_definition_id: UUID
    champion_generation: int
    research_lineage_id: UUID
    hypothesis: str
    expected_mechanism: str
    invalidation_conditions: tuple[str, ...]
    evidence_refs: tuple[UUID, ...]
    mutation_axes: tuple[RegisteredMutationAxisV1, ...]  # 1..3
```

自由文本仅用于解释和审计，长度、字符和字段均受严格 Schema 限制；从不进入编译器、SQL、Job payload 或交易逻辑。

确定性生成器输出 `StrategyCandidateDeltaV1`：

```python
class StrategyCandidateDeltaV1(BaseModel):
    schema_version: Literal["strategy_candidate_delta_v1"]
    parent_expression_hash: Hash64
    ordinal: int
    mutations: tuple[RegisteredMutationV1, ...]
    candidate_hash: Hash64
```

- Agent 不能指定候选 ID、版本、参数上下限、OOS 入选顺序或因子实现。
- 生成器按 `candidate_budget_policy_v1` 做稳定笛卡尔枚举；超过 24 个时按 canonical hash 升序截取，不使用随机数。
- 每个 family 最多 24 个候选、最多 3 个搜索轴、最多 3 个候选进入 OOS。
- `expression_hash` 全局去重；重复候选关联既有证据，不产生新的独立成功试验。

### 4.2 Phase 2A 允许的搜索轴

只允许注册在 `evolution_search_space_v1` 的轴：

- `min_adv20_usd`：现有安全范围内的离散阈值。
- `min_close`：现有安全范围内的离散阈值。
- `target_weight`：策略目标而非硬风控；候选必须通过硬风控兼容检查。
- 排名特征只能在兼容类别 `momentum_rank` 的已登记特征间替换，首版为 `momentum_126_skip5` 与 `return_126`。
- 已有布尔过滤模板只能从注册表启用/禁用，不能提交任意 AST patch。

以下字段不可变：universe、fill timing、rebalance schedule、hard risk exit、数据源、成本政策、行业分类、审批和券商设置。候选不能自定义 JSON Path；服务端从 mutation axis 映射到固定字段。

### 4.3 特征注册表

每个可进化特征必须登记：

```text
name, version, compatibility_class, inputs, unit, adjustment,
minimum_history_sessions, availability_delay, missing_policy,
minimum, maximum, implementation_hash
```

注册表只指向本地纯函数。运行时禁止动态 import、`eval/exec`、表达式字符串和网络读取。引用特征必须满足 `available_at <= decision_time`，候选 patch 的 offset 固定为 0。

### 4.4 Agent 工具

新增独立固定只读目录：

- `get_strategy_parent`
- `get_evolution_search_space`
- `get_training_validation_summary`
- `get_sealed_experiment_summary`

所有工具只接受内部 UUID、冻结 snapshot 和 `as_of`；只返回聚合、去标识的研究/绩效证据。当前 family 生成期间不返回 OOS。Agent 输出由 Responses strict structured output 验证，服务端再做第二次 Pydantic、证据引用、父冠军 generation 和工具目录哈希验证。

`strategy_rnd` 是固定 Prompt 角色。Phase 2A 只允许开发流程发布 Prompt v2；Agent 不能提出或启用 Prompt/模型/角色变更。

## 5. 反过拟合和评估协议

### 5.1 Lineage 与不可重置试验数

- `research_lineage` 是适应性研究根。Agent 看过任何 OOS 汇总后创建的新 family 仍属于同一 lineage。
- 改名、调整 Prompt、改变说明文字或重新排列参数均不能清零累计试验数。
- 只有人工开发发布全新经济假设类别和新政策版本时才能创建新 lineage；Phase 2A Agent 无此权限。
- 同一 lineage 同时最多一个非终态 family。
- 所有真正接触 OOS 的不同 `expression_hash` 计入 `oos_trial_count`，包括最终失败者。

### 5.2 Walk-forward

- 默认至少七年历史。
- 每个窗口为 3 年训练、1 年验证、随后 6 个月完全 OOS；每季度前滚。
- 少于 8 个有效 OOS 窗口只能展示，不能晋级。
- family 冻结同一 authority bundle、代码版本、策略父版本、成本政策、基准、随机种子 0 和评价协议。
- 候选先在训练/验证区间确定性比较，按预注册验证排序选择最多 3 个进入 OOS。
- Agent 在 family 封存前不能读取 OOS；封存后只能读取汇总，用于同 lineage 的下一 family。

### 5.3 回测内核兼容

现有 `run_backtest()` 保持 `baseline_v1` 专用和 Phase 0–1 语义不变。Phase 2 先抽取共享纯 `simulation_core_v1`，再新增 `run_challenger_backtest_v1()`。

抽取门禁：先从抽取前的现有内核生成冻结 golden fixture；同一输入下，`baseline_v1` 的逐日 NAV、订单、成交、窗口、指标、归因、比较和其他业务语义载荷在排除 `code_hash`、`result_hash` 后必须字节级相同。任何业务语义差异阻止迁移和发布。

`code_hash` 和 `result_hash` 是实现身份，不是兼容性伪装层。只要哈希覆盖的源码发生变化，新运行必须生成新的 `code_hash` 和 `result_hash`；已持久化的旧运行保持不可变，并继续绑定其原始代码身份，禁止把新内核的结果冒充为历史运行。

新入口只接受已验证 `CompiledStrategy`，不接受 Mapping、JSON 或 Agent 输出。历史逐 Bar 循环只调用确定性策略解释器。

### 5.4 冠军比较

每个 OOS 候选与同一 authority 下重新运行的当前内部模拟冠军绑定为 `ChampionComparisonRunV1`。绑定内容至少包含：

```text
challenger_definition_id, champion_definition_id, champion_generation,
challenger_backtest_run_id, champion_backtest_run_id,
authority_bundle_id, protocol_hash, cost_policy_id,
research_lineage_id, cumulative_oos_trial_count, comparison_hash
```

`backtest_runs.promotable` 和旧 `phase01_promotion_locked` 永远保持 false；Phase 2 资格只存在于新的确定性 `strategy_promotion_decisions`。

### 5.5 历史晋级门禁

`promotion_policy_v1` 逐项输出 `PASS|FAIL|UNKNOWN`：

- 有效 OOS 窗口不少于 8。
- Deflated Sharpe 置信度不低于 95%，并使用 lineage 累计 OOS 试验数。
- 成本后 Sharpe 至少高于冠军 0.15，或年化收益至少高 1 个百分点。
- 最大回撤不比冠军差超过 2 个百分点。
- 双倍成本下挑战者相对冠军的成本后超额收益仍为正。
- 策略数据、所有基准、authority、成本和参数敏感性完整。

任何 `UNKNOWN` 等价于不合格；阈值不能由 Agent、Web、环境变量或调用方覆盖。

## 6. 不可变持久化与状态机

### 6.1 迁移

新增单头前向迁移 `0022_strategy_evolution`，父节点为 `0021_manual_job_retry_allowance`。不编辑任何已发布迁移。

迁移建立以下表组：

- Agent：`strategy_rnd_runs`、`strategy_rnd_outputs`。
- 政策：`strategy_evolution_policies`、`strategy_promotion_policies`。
- 实验：`strategy_research_lineages`、`strategy_experiment_families`、`strategy_experiments`、`strategy_experiment_events`、`strategy_experiment_attempts`、`strategy_experiment_state_projections`。
- 比较：`strategy_champion_comparisons`、`strategy_promotion_decisions`。
- Shadow：`strategy_shadow_groups`、`strategy_shadow_legs`、`strategy_shadow_advance_attempts`、`strategy_shadow_session_authorities`、`strategy_shadow_session_price_inputs`、`strategy_shadow_leg_states`、`strategy_shadow_orders`、`strategy_shadow_fills`、`strategy_shadow_checkpoints`、`strategy_shadow_checkpoint_metrics`、`strategy_shadow_finalizations`。
- 注册表：`strategy_registry_events`、`strategy_registry_heads`。

不可变权威表由数据库触发器拒绝 UPDATE/DELETE。projection/head 是可重建协调状态，只允许受约束服务更新。

### 6.2 通用事件字段

实验和注册表事件包含：

```text
schema_version, sequence, event_type, idempotency_key,
actor_kind, occurred_at, payload, previous_hash, event_hash
```

Hash 使用现有 canonical JSON SHA-256 约定。`(root_id, sequence)`、`idempotency_key` 和 `event_hash` 唯一；sequence 必须连续。

Registry 的 `event_sequence` 与 `champion_generation` 分离：GENESIS 建立 generation 1；只有生效的 PROMOTE/ROLLBACK 创建 `generation + 1`，PAUSE/RESUME/VETO 只增加事件 sequence。回滚到旧 definition 也会创建一个全新的单调 generation，不复用历史 generation。这样任何 Forward 决策都能唯一指向当时的冠军事实。

### 6.3 实验状态

```text
PROPOSED
  -> REJECTED
  -> ADMITTED
       -> BACKTEST_RUNNING
       -> BACKTEST_FAILED
       -> BACKTEST_PASSED
            -> SHADOW_RUNNING
            -> SHADOW_FAILED | EXPIRED | VETOED
            -> PROMOTION_ELIGIBLE
                 -> PROMOTED | STALE_CHAMPION_BASELINE
```

- 状态由追加事件派生；projection 只用于查询和领取。
- 失败重试追加 attempt，不覆盖旧结果。
- 只有 candidate、protocol、code 和 authority hash 全部相同才能重试同一 experiment；否则新建 experiment。
- 冠军 generation 在运行期间变化时转为 `STALE_CHAMPION_BASELINE`，重新与新冠军完成历史比较和新 Strategy Shadow，不能直接晋级。

### 6.4 策略定义与生命周期

继续复用内容寻址 `strategy_definitions`。新候选以 `lifecycle='challenger'` 出生且永不 UPDATE；旧 `lifecycle` 只表示发布时角色，不再作为当前冠军权威。当前冠军唯一权威为 `strategy_registry_events + strategy_registry_heads`。

迁移为 `baseline_v1` 创建 genesis registry event 和 `internal_paper` head。新 registry 读路径验证该 definition 与现有编译结果一致后才 backfill。

## 7. Strategy Shadow

### 7.1 与 Proposal Shadow 隔离

Proposal Shadow 比较 `hold_current/minimum_change/strategy_target`，并绑定真实只读组合提案；Strategy Shadow 比较冠军策略与挑战者策略，权限根和生命周期不同。二者只复用纯 authority、成本和 simulation 函数，不复用表、状态或 API。

### 7.2 起点与 authority

- 从内部模拟盘最新 PASS、已封存且账本平衡的 portfolio snapshot 克隆两个隔离虚拟账本。
- champion/challenger 起点现金、持仓、价格、成本、硬风控和 NAV 完全一致；指标归一化到 NAV=100。
- 固定 63 个评价 session 和 5 个运营宽限 session。
- 两条腿共享同一个 session authority；一个 session 只能整体成功或整体失败。
- 策略主动观望是有效结果；数据缺失、过期、来源冲突或 authority 不匹配不是观望。
- 宽限期只允许处理任务延迟和最终化，不能用后来可用的数据替换决策时点缺失的事实。

### 7.3 执行与检查点

- 使用与内部模拟盘相同的确定性订单、成交、成本、整股和 Decimal 账本内核，但不写 `paper_*` 账户事实。
- 5、21、63 session 生成不可变检查点；5/21 仅观察。
- 完成订单只统计最终状态为完全成交的订单，不计拒绝、取消、过期或部分成交。
- 最终必须有 63 个完整 session、挑战者至少 20 笔完全成交订单、零硬风控违规、成本后相对冠军收益非负、最大回撤不差超过 2 个百分点。
- 最终 authority hash 覆盖所有 session、订单、成交和检查点 hash。

### 7.4 实施约束决定

- `DataSnapshot.schema_version` 的数据库上限为 32 字符，Strategy Shadow session 证据使用 `strategy_shadow_evidence_v1`；完整语义继续由 Pydantic payload 类型和内容哈希约束，不能依赖超长显示名。
- 初始同起点不伪造 ordinal 0 session authority。两条腿分别保存同一个 `initial_state_hash`，该哈希绑定 baseline snapshot、position projection hashes 与 NAV=100 归一化状态；首个真实推进从 ordinal 1 开始。
- `strategy_shadow_groups` 是不可变创建根，其 `status='running'` 表示创建时状态，不作可变投影。终态唯一由不可变 `strategy_shadow_finalizations` 与 family state projection 推导，API/UI 不得把 group.status 当作当前状态。
- 现有 baseline-only Forward 在完整 registry 解释器接入前，仅允许 registry 当前 definition 的 `strategy_version` 与 baseline decision 完全一致；不一致时 fail closed，禁止把 baseline targets 误标成 challenger 决策。

## 8. 晋级、交接与回滚

### 8.1 结构化解释

每次 `strategy_promotion_decision` 保存有序 gate report：

```python
class PromotionGateResultV1(BaseModel):
    gate_code: str
    status: Literal["PASS", "FAIL", "UNKNOWN"]
    observed_value: str | None
    champion_value: str | None
    operator: Literal["gte", "gt", "lte", "lt", "eq"]
    threshold: str
    unit: str
    evidence_refs: tuple[UUID, ...]
    reason_code: str
```

人类可读说明由这些字段的固定模板生成；Agent 不能撰写或覆盖晋级结论。

### 8.2 交接预检

历史和 Shadow 门禁通过后，使用最新合格内部模拟快照计算从当前冠军持仓到挑战者下一目标的预计换手和成本，并再次运行现金、集中度、行业、流动性和全部硬风控。

必须满足：

```text
shadow_excess_return - estimated_handover_cost_ratio >= 0
```

数据、成本或风险未知时进入 `PROMOTION_WAITING_HANDOVER`，不创建订单。交接预检只保存证据；正式订单仍由下一次 Forward 决策产生。

实现时不得信任调用方声称的当前组合。持久化服务必须按交接决策时点重新加载最新封存、无未终结订单的内部模拟快照，归一化后逐项核对 snapshot ID、NAV、现金权重和所有正持仓权重；“真实 snapshot ID + 伪造权重”按 `HANDOVER_AUTHORITY_INVALID` 拒绝。source、cost、risk policy 必须由仓库固定 loader 重新验证，预计成本率必须等于已发布成本政策。交接 instrument authority 还固定 market、currency、asset type、价格、ADV20、可用时间和 blocked 状态；目标级预检覆盖支持范围、交易阻断、流动性、drawdown lock、最小买入额、现金、单名和行业约束。任何晚到或不完整 authority 均为 UNKNOWN，而不是乐观补值。

### 8.3 并发晋级

- `strategy_registry_heads` 只有 `channel='internal_paper'` 一行。
- 晋级事务 `SELECT ... FOR UPDATE`，校验 `expected_generation`、当前 champion definition 和 promotion decision 绑定一致。
- 同一事务追加 `PROMOTE` 事件、更新 head/pending projection、写 audit 和 outbox。
- 两个候选同时通过时只有一个提交；另一个标记 `STALE_CHAMPION_BASELINE`，不能覆盖新冠军。
- 自动晋级可由用户通过 registry control event 暂停；暂停不改变冠军或硬风控。

`strategy_registry_heads` 是可重建投影，精确保存 `active_definition_id`、`active_generation`、`last_event_id`、`auto_promotion_paused`、`pending_event_id`、`pending_effective_session` 和 CAS `version`。存在 pending promote/rollback 时不得提交第二个切换；PAUSE 允许提交，但只阻止新的自动 PROMOTE，不取消已通过人工回滚命令建立的 pending rollback。

幂等键同时绑定操作类型、目标 definition、effective session 和固定 reason；同一键跨 PAUSE/PROMOTE/ROLLBACK 或改变参数时必须返回 `REGISTRY_IDEMPOTENCY_CONFLICT`。pending 期间追加 PAUSE/RESUME 后，激活结果和 Forward 绑定仍引用原 PROMOTE/ROLLBACK 激活事件，不得误用最新控制事件。

### 8.4 安全生效

- 晋级只在下一交易决策边界生效。
- 若当前冠军仍有未终结订单，状态为 `PROMOTION_PENDING_ORDERS`；系统不自动撤单。
- 新 Forward 决策绑定 `strategy_definition_id + registry_event_id + champion_generation + authority_hash`。
- 每个内部模拟账户和 decision session 最多一个策略决策，防止冠军切换产生重复订单。
- 新策略决策仍必须通过现有硬风控才可产生内部模拟订单。

### 8.5 一键回滚

回滚命令只接受 `expected_generation`、幂等键和固定 reason code。服务端从 registry 历史选择上一兼容、已验证冠军：

- 重新验证 DSL 编译器、策略 definition hash 和硬风控兼容性。
- 有未终结订单时进入 `ROLLBACK_PENDING_ORDERS`；不自动撤单。
- 回滚作为安全动作显示预计成本，但不以经济收益门槛阻止。
- 生效后只影响未来决策，不重写历史订单、成交、现金、持仓或 NAV。
- 找不到兼容冠军时 fail closed，并保留当前冠军。

## 9. Forward 与内部模拟盘兼容

`0022` 为现有 `strategy_decisions` 增加：

```text
strategy_definition_id, registry_event_id, champion_generation
```

迁移用 genesis baseline registry event 回填旧行，再设 NOT NULL/FK。现有唯一约束收紧为每个 `(account_id, decision_session)` 唯一；旧 `(account_id, strategy_version, decision_session)` 不再是并发权威。

首版仍固定周频，因此保留 `WEEKLY_REBALANCE`、下一 session 和既有 Paper 执行时序。`ForwardStrategyDecision` 从 literal baseline 扩展为内容寻址 definition，但构造函数不接受调用方 targets；它只能执行确定性策略解释器输出。

`paper` 在每个 decision session 开始时读取已生效 registry head，验证 event/hash/generation/definition，然后把该绑定冻结进决策。即使随后冠军切换，当日决策和订单仍引用原 generation。

新决策使用 `forward_strategy_decision_sha256_v2`，将 definition、激活 event 和 generation 纳入 canonical hash。`0022` 之前已经存在的不可变 `baseline_v1` 行不能重写下游 hash，因此只读加载器可为 genesis generation 1 重算并接受旧 v1 hash；所有新写入只能使用 v2。PAUSE/RESUME 不是冠军激活 authority，不能成为 Forward 决策的 `registry_event_id`。

## 10. Job 编排与可观测性

### 10.1 `strategy.rnd` Job

继续使用隔离 `strategy_rnd` queue。一个严格 payload 承载一个有界操作：

```python
class StrategyRNDJobPayloadV1(BaseModel):
    schema_version: Literal["strategy_rnd_job_v1"]
    operation: Literal[
        "generate_family", "evaluate_candidate", "advance_shadow", "finalize_promotion"
    ]
    family_id: UUID | None
    experiment_id: UUID | None
    expected_champion_generation: int
    input_hash: Hash64
```

Job payload 不含 Prompt、代码、URL、路径、SQL、provider 方法或策略 JSON。handler 根据数据库中的不可变 ID 加载事实。每个 candidate 单独评估，避免一个 24 候选任务超时；结果通过 idempotency key 合并。

### 10.2 自动节奏

- 同一 lineage 无活动 family、registry 未暂停、最新质量 PASS 且距上一 family 至少一个自然季度时，daily pipeline 可入队 `generate_family`。
- 用户手动“启动实验”调用同一 admission，不可绕过季度冷却、单活动 family、预算或数据门禁。
- family admission 后为每个 candidate 入队 `evaluate_candidate`。
- 历史门禁通过后创建 Strategy Shadow；daily pipeline 每个合格 session 入队 `advance_shadow`。
- 63 日最终化后入队 `finalize_promotion`；重复 outbox/Job 只返回同一事实。

### 10.3 Audit 与指标

必须追加安全 audit：proposal created/rejected、candidate admitted/rejected、backtest completed/failed、shadow started/advanced/finalized、promotion gated/promoted、registry paused/resumed、rollback requested/effective/failed。

审计 payload 只含内部类型、版本、hash、gate code 和安全 reason；不含策略全文、Agent 原始文本、精确持仓/金额、账户 ID、工具响应或凭据。

进程指标至少包含 active family 数、candidate 状态数、strategy queue age、Shadow session/订单进度、promotion gate 结果数、registry generation 和 pending transition 状态。指标不能包含 symbol、持仓、金额或 Agent 内容。

## 11. API 契约

保留现有 `GET /internal/v1/strategies` 兼容 Phase 0–1 摘要；新增独立资源：

| 方法 | 路径 | 语义 |
|---|---|---|
| GET | `/internal/v1/strategy-evolution` | 当前冠军、控制状态、活动 family、最近实验、Shadow、门禁和历史摘要 |
| GET | `/internal/v1/strategy-experiments/{experiment_id}` | 单个不可变实验、证据和状态历史 |
| POST | `/internal/v1/strategy-experiments` | 触发受限 `generate_family`；不接受 DSL 或搜索空间正文 |
| POST | `/internal/v1/strategy-experiments/{experiment_id}/veto` | 晋级生效前追加否决事件 |
| POST | `/internal/v1/strategy-registry/control` | `pause_auto_promotion` 或 `resume_auto_promotion` |
| POST | `/internal/v1/strategy-registry/rollback` | 回滚至上一兼容已验证冠军 |

写命令均要求 HMAC 服务身份、同源 BFF、`Idempotency-Key` 和 `If-Match` generation/version。响应：

- 已完成且幂等重放：200。
- 已入队或等待订单/交接：202 + `Location`。
- generation/version 冲突：409。
- 状态不允许、预算/冷却/门禁失败：422。
- authority、质量或依赖不可用：503，并使用安全 reason code。

读接口默认返回活动 family、最近 20 个 experiment 和最近 20 个 registry event；更多历史使用现有签名 cursor 和稳定 `(created_at,id)` 排序，每页上限 100。响应中的 Decimal 全部使用字符串，时间使用 UTC ISO 8601，交易 session 使用 `YYYY-MM-DD`。

浏览器永远不能提交完整 DSL、mutation path、政策阈值、Prompt、模型、数据源、账户或订单参数。BFF 只转发严格 allowlist 字段，不记录响应正文。

## 12. `/strategies` 页面

页面从 Phase 0–1 锁定说明升级为单页策略进化工作台，继续使用现有 NEXUS token/AppShell，不增加图表依赖：

1. **冠军状态栏：** 内部模拟冠军、generation、生效 session、自动晋级暂停状态；醒目固定“实盘关闭”。
2. **活动实验：** Agent 假设、已登记搜索轴、候选/验证/OOS 数量、当前状态和下一步；不展示原始 Prompt 或工具 payload。
3. **证据比较：** 挑战者、冠军、SPY、等权白名单的绝对和相对指标；同屏显示成本、回撤、数据模式和 authority hash。
4. **Strategy Shadow：** 5/21/63 进度、完全成交订单数、成本后超额、回撤差、风险违规和数据完整性。
5. **晋级解释：** 每个 gate 的 observed/champion/threshold/status/reason；不能只显示单一收益排名。
6. **冠军历史：** Promote/Rollback 时间线、目标版本、生效状态；上一兼容冠军提供回滚操作。

交互规则：

- “启动实验”不接受参数；显示冷却、预算和当前冠军影响。
- 暂停/恢复自动晋级使用清晰影响说明，不影响已经运行的回测/Shadow。
- 回滚按钮显示目标冠军、只影响未来内部模拟决策、不会撤销历史成交；使用可访问确认对话框，无需输入文字。
- stale/partial/forbidden 状态禁止所有写动作，保留最后已知只读证据。
- 统一 `loading/empty/ready/partial/stale/error/forbidden`；错误摘要可聚焦，成功状态原位播报。
- 375/390/768/1024/1440 和 390×844 横屏无页面级横向溢出；44px 点击区、可见焦点、reduced motion、Axe critical/serious 为 0。

## 13. 失败语义

固定安全 reason code 至少包含：

```text
EVOLUTION_PAUSED
ACTIVE_FAMILY_EXISTS
EXPERIMENT_COOLDOWN_ACTIVE
EVIDENCE_INCOMPLETE
PROPOSAL_SCHEMA_INVALID
UNREGISTERED_MUTATION_AXIS
CANDIDATE_BUDGET_EXCEEDED
CANDIDATE_DUPLICATE
DSL_COMPILATION_FAILED
LOOKAHEAD_FORBIDDEN
BACKTEST_AUTHORITY_INCOMPLETE
OOS_WINDOW_INSUFFICIENT
PROMOTION_GATE_FAILED
SHADOW_DATA_GAP
SHADOW_ORDER_COUNT_INSUFFICIENT
STALE_CHAMPION_BASELINE
HANDOVER_AUTHORITY_INCOMPLETE
OPEN_ORDERS_PENDING
REGISTRY_GENERATION_CONFLICT
ROLLBACK_TARGET_INCOMPATIBLE
LIVE_EXECUTION_DISABLED
```

- 缺失/过期/冲突为失败关闭，不用 0 或最后值代替。
- 未知执行结果使用相同幂等键重新读取；不得创建第二个实验、事件或切换。
- Agent 超时/预算耗尽记录失败或 abstain，不生成默认候选。
- Shadow 数据缺口永久留痕，运营宽限不能变成回填窗口。
- 任一 registry hash/sequence/projection 不一致时停止所有晋级和回滚，但硬风控退出继续工作。

## 14. 测试与审查门禁

### 14.1 Unit / property

- Proposal/Delta strict Schema、未知字段、任意代码、路径、URL、binary float 和越权字段反例。
- 候选枚举稳定、24/3/3 预算、hash 去重、mutation path 封闭。
- 特征可用时间、offset=0、缺失策略和 no-lookahead 属性测试。
- `baseline_v1` 旧/新内核业务语义载荷字节级等价；实现身份按源码变化生成新 hash，历史身份不可复用。
- lineage 累计试验数不能通过新 family、Prompt 或重命名清零。
- promotion 每个 gate 的边界值、UNKNOWN、双倍成本相对冠军语义。
- Shadow 同起点、同 authority、账本平衡、订单计数和数据缺口属性。
- registry 状态机、pause/veto、generation CAS、pending orders 和 rollback compatibility。

### 14.2 PostgreSQL integration

- `0022` 单头、upgrade/downgrade、本地 backfill 和旧 baseline 决策绑定。
- 权威表 UPDATE/DELETE 拒绝，projection/head 只能通过允许路径更新。
- 同 lineage 单活动 family、event sequence/hash、idempotency 和唯一 candidate。
- 并发 candidate completion、并发 Shadow advance、双晋级竞争只有一个冠军。
- 晋级事务、audit、outbox 和 head 原子；崩溃重放不重复。
- 未终结订单阻止生效；订单终结后同事件生效。
- 每账户/session 仅一个 Forward 决策，绑定 definition/event/generation。
- rollback 不修改历史 paper ledger，且不触发 broker 表或 provider。

### 14.3 Agent/security

- `strategy_rnd` 只读工具目录固定，动态工具、代码、网络和 broker 请求拒绝。
- Prompt injection、越权调风控、要求下单、未来数据和伪造 evidence 的 eval cases。
- 静态 import 测试证明 `nexus.evolution` 不依赖 broker gateway/write capability。
- API/BFF 不接受或回显 DSL、Prompt、工具 payload、账户、精确金额或密钥。
- `live_execution_enabled` 在所有 Phase 2 响应始终为 false。

### 14.4 Web/E2E

- 七种数据状态、活动实验、历史/Shadow/门禁/无实验状态。
- 启动实验、暂停/恢复、否决、回滚的版本冲突、幂等重放、未知结果恢复和焦点管理。
- 不可用数据时写按钮禁用；实盘关闭文案始终可见。
- 断点、键盘、对话框、aria-live、Axe 和页面横向溢出测试。

### 14.5 完成证据

- Engine 全量 PostgreSQL 测试、Ruff、Mypy、迁移单头和 contracts 生成通过。
- Web 全量测试、typecheck、lint、build 和 Playwright 通过。
- 独立代码审查和安全边界审查未解决 Critical/Important 为 0。
- Git 工作区干净，所有实现和审查修复均有本地提交。

## 15. 实施切片

按以下顺序实施，每个切片先失败测试、再最小实现、再局部回归并提交：

1. Phase 2 contracts、政策、候选生成和通用策略解释器等价门禁。
2. `0022` 核心实验/registry/Agent persistence 与数据库不变量。
3. `strategy_rnd` Agent、只读工具、family admission、候选和 Job 编排。
4. 挑战者回测、冠军配对比较和历史 promotion gate。
5. 独立 Strategy Shadow、检查点和最终 gate。
6. registry 晋级、交接预检、Forward 绑定、pending orders 和 rollback。
7. Engine API、BFF、OpenAPI/contracts。
8. `/strategies` 工作台、全状态和交互。
9. 自动调度、审计、可观测性、E2E 和完整门禁。

## 16. 回滚与运维

- 功能级回滚优先追加 registry `ROLLBACK`；不回退数据库或重写策略历史。
- 代码回滚前暂停自动晋级，等待 Strategy RND/Shadow Job 到安全终态，备份数据库和审计链，再发布前向修复。
- `0022` downgrade 只用于没有 Phase 2 运行事实的本地环境；存在 experiment、Shadow、promotion 或新 Forward decision 时必须 fail closed，不能静默删除历史。
- Phase 2 不要求 Docker Desktop；只要求现有 Docker-compatible CLI/Compose 和 PostgreSQL 测试环境。
- 不修改 `.openai/hosting.json`，不部署，不启用 broker worker，不读取或轮换任何密钥。

## 17. 设计决定记录

- 选择独立 `evolution` 域，而不是扩展 Agent/Proposal Shadow 状态，避免权限根混淆。
- 选择 Agent 提实验搜索轴、确定性服务生成候选，而不是让 Agent 直接提交完整 DSL。
- 选择保留 `phase01_promotion_locked`，另建 promotion decision，避免可晋级布尔值受调用方控制。
- 选择 Phase 2A 固定周频，避免同时改写调度/执行时序；月频和 Prompt 自进化留到后续独立规格。
- 选择内部模拟自动晋级 + 可暂停/否决/回滚，既保留自进化能力，又不触及实盘。
- 选择安全边界生效和 pending orders，而不是自动取消已有订单。
- 选择单一前向迁移 `0022_strategy_evolution`，不修改已发布迁移。
