# NEXUS Phase 2 独立审查修正规格

## 1. 目的与结论

本规格修复 Phase 2 独立审查发现的“组件存在但闭环不成立”问题，不扩大 NEXUS 的产品范围。修复后的最小可验收闭环为：受限 Agent 生成 family，所有候选只运行训练/验证评价，确定性代码在全部验证结果终态后选择最多 3 个候选，先登记不可重置的 OOS 暴露，再运行同权威冠军/挑战者 OOS 比较，通过固定历史门禁后进入 Strategy Shadow，最后只切换内部模拟冠军并可回滚。

Agent 仍不能生成或执行任意代码、修改硬风控、访问 Broker、账户或交易密钥、创建订单、决定 Top-3、覆盖门禁或启用实盘。实盘默认关闭，所有真实订单继续要求逐笔人工确认。本修复不部署、不激活付费模型调用，也不执行任何券商写操作。

## 2. 已证实的根因

1. `strategy_rnd` worker 只有 executor 注入接口，没有仓库内生产组合根和 CLI；排队成功不代表工作流可执行。
2. family 只有一行状态投影，候选记录又是不可变的，因此 24 个候选无法独立完成验证、Top-3、OOS 和终态。
3. `select_oos_candidates()` 没有生产调用方；当前调度会直接为每个 `ADMITTED` 候选创建同类评估 Job。
4. OOS 累计次数由调用方传入比较服务，lineage 根上的计数没有生产更新路径；同一语义表达式跨 family 也没有 lineage 级去重和证据关联。
5. family 状态投影的链验证并未被所有写服务统一使用，数据库只阻止计数倒退，不能证明投影等于事件链。
6. PROMOTE/ROLLBACK 请求与边界生效之间缺少不可变 activation 事实，因此注册表 head 不能仅由权威事实重建。
7. 读服务只验证 JSON schema，未统一核对比较、门禁和 Shadow 事实的记录 ID、哈希与权威绑定；前端还容许缺失 ETag，并把 5 个宽限 session 混入 63-session 评价进度。

## 3. 方案选择

### 3.1 采用方案：前向补充权威事实与唯一组合根

新增候选级状态投影、验证结果、lineage OOS 暴露账本、注册表 activation 事实和一个数据库受限的 Engine 组合根。保留现有 family 投影作为聚合视图，保留四种 Job operation 和现有 HTTP/BFF 命令形状。

这一方案比“让 family 状态代表当前候选”更可重放，比“把候选状态写回不可变 experiment 行”更符合既有 append-only 原则，也比重做整个 Phase 2 API 更小。

### 3.2 不采用的方案

- 不把 Top-3、试验次数或门禁交给 Agent 或前端。
- 不在第一次候选 Job 中同时登记 OOS 暴露并运行回测；同一事务回滚会让崩溃后的暴露消失。
- 不仅加强 Python 校验而保留可任意更新的数据库 head；PostgreSQL 仍是最终权威。
- 不回改已提交的 `0022_strategy_evolution`。使用单头前向迁移 `0023_evolution_authority`，父节点为 `0022_strategy_evolution`；revision ID 保持在现有 `alembic_version.version_num varchar(32)` 上限内。

## 4. 持久化权威

### 4.1 候选状态与事件

`strategy_experiment_events` 增加 nullable `experiment_id`。family 事件链仍以 `(family_id, sequence)` 全局排序和哈希；候选事件必须绑定本 family 的 experiment。

新增 `strategy_experiment_candidate_projections`：

- 主键 `experiment_id`，并保存 `family_id`、`current_state`、`event_sequence`、`version`、`updated_at`。
- 状态集合固定为 `ADMITTED | VALIDATION_FAILED | VALIDATION_PASSED | OOS_NOT_SELECTED | OOS_SELECTED | OOS_RESERVED | BACKTEST_FAILED | BACKTEST_PASSED | SHADOW_RUNNING | SHADOW_FAILED | EXPIRED | VETOED | PROMOTION_ELIGIBLE | PROMOTED | STALE_CHAMPION_BASELINE`。
- 合法转换由服务端常量定义；数据库 trigger 要求 version 恰好加一、event sequence 前进，且该 sequence 对应同 experiment 的最新事件。
- admission 为每个候选追加独立 `ADMITTED` 事件并建立一行候选投影。

现有 `strategy_experiment_state_projections` 继续作为 family 聚合投影。每次候选转换后，确定性 `aggregate_family_state()` 从同 family 全部候选投影计算 family 状态，并将 family 投影绑定到本次最新事件。family 状态不再冒充每个候选状态。

聚合优先级固定为：`PROMOTED`；`PROMOTION_ELIGIBLE`；`SHADOW_RUNNING`；`BACKTEST_PASSED`；仍有待验证/待 OOS 候选时为 `ADMITTED` 或 `BACKTEST_RUNNING`；全部终态后按 `STALE_CHAMPION_BASELINE`、`SHADOW_FAILED`、`EXPIRED`、`VETOED`、`BACKTEST_FAILED` 的可解释规则收敛。任一服务在写入前必须使用统一 verified loader 重放 family 链、候选投影和聚合投影。

### 4.2 验证结果与 attempt

新增 append-only `strategy_candidate_validation_results`，每个 experiment 最多一行，保存 `validation_sharpe`、`validation_max_drawdown`、`validation_cost`、有效窗口数、完整性、冻结 authority/protocol/code hash、结果 hash 和对应 attempt ID。

`strategy_experiment_attempts` 只追加终态 attempt，不把 `running` 行更新成终态：

- `attempt_kind=backtest`，阶段在 input hash 中固定为 `validation` 或 `oos`。
- 成功追加 `completed` 和 output hash；确定性不可恢复失败追加 `failed` 与安全 reason code。
- 进程崩溃和 lease 丢失由通用 Job attempt 记录，不能伪造一个已完成的策略 attempt。

所有候选验证终态后，`finalize_validation_shortlist()` 从数据库加载完整结果并调用唯一排序：`validation_sharpe desc, validation_max_drawdown desc, validation_cost asc, expression_hash asc`。最多 3 个进入 `OOS_SELECTED`，其余进入终态 `OOS_NOT_SELECTED`。选择结果不能由调用方传入。

### 4.3 Lineage OOS 暴露与不可重置计数

新增：

- `strategy_research_lineage_projections(lineage_id, cumulative_oos_trial_count, version, updated_at)`；lineage 根保持不可变，计数只存在于受控投影。
- `strategy_oos_exposures(id, lineage_id, expression_hash, ordinal, first_experiment_id, exposed_at, exposure_hash)`；`(lineage_id, expression_hash)` 与 `(lineage_id, ordinal)` 唯一。
- `strategy_experiment_oos_exposure_links(experiment_id, exposure_id, link_hash, created_at)`；一个 experiment 最多绑定一个暴露。
- `strategy_oos_evidence_links(experiment_id, exposure_id, comparison_id, champion_backtest_run_id, challenger_backtest_run_id, authority_hash, evidence_hash, created_at)`；把重复语义在不同冻结 authority 下的比较证据关联回同一暴露。

`reserve_oos_exposure()` 是独立 Job 阶段：在事务中锁 lineage 投影，按 expression hash 查找或创建暴露，追加 experiment link，更新累计计数和候选状态为 `OOS_RESERVED`，然后 Job 成功提交。下一次 Job 才能运行 OOS。这样进程在真实 OOS 计算期间崩溃，暴露仍不会消失。

跨 family 的同一 `expression_hash` 复用同一 exposure ordinal，累计计数不增加，但新的冻结 authority 结果会生成新的 evidence link。并发首次暴露依靠唯一约束和 lineage 锁只能获得一个 ordinal。

`persist_champion_comparison()` 不再接收 `champion_generation` 或 `cumulative_oos_trial_count`。它从锁定的 family、registry head、OOS exposure、lineage 投影和 backtest protocol 中派生并交叉核对这些值；比较和 Deflated Sharpe 只使用数据库权威计数。

## 5. 注册表完整重放

新增 append-only `strategy_registry_activations`，每个 PROMOTE/ROLLBACK event 最多一行，保存 transition event、decision session、激活时间、from/to definition、generation 与 activation hash。

- PROMOTE/ROLLBACK event 仍代表“已请求、等待未来内部模拟边界”。
- `activate_pending_transition()` 必须先追加 activation，再更新 head；存在非终态内部模拟订单时不追加 activation。
- `rebuild_registry_head(events, activations)` 从 GENESIS 开始按序处理 PROMOTE、ROLLBACK、PAUSE、RESUME、VETO 及 activation，重建 active definition/generation、pause、pending event/effective session、last event、event sequence 和 version。
- version 定义为 `registry event 数 + activation 数`；GENESIS 后为 1。
- 所有 registry 读写先比较重建结果和持久化 head。数据库 head trigger 也计算同一语义，拒绝没有对应 event/activation 的 active definition 交换、pause 翻转、pending 绑定或 version 变化。

## 6. 受限生产执行闭环

新增 `nexus.evolution.executor` 作为唯一 `StrategyRNDOperationExecutor` 组合根，并新增 `nexus-engine-strategy-rnd-worker` CLI。它只导入 evolution、backtest、Strategy Shadow、内部模拟风险/注册表和只读数据库 authority loader；静态测试禁止导入 broker、Fosun、订单创建、账户或 live-write 能力。

四个 operation 的确定性语义：

1. `generate_family`：解析当前 registry、active lineage、冻结 baseline backtest authority、Prompt/model authority 与四个只读 Agent tools；development/test 使用固定 fixture runtime，production 只允许显式 `responses` runtime 且缺少模型 authority 时失败关闭。执行 `execute_strategy_rnd()` 后在同一事务 admission；abstain 不创建 family。
2. `evaluate_candidate`：payload 绑定候选投影 state/version。`ADMITTED` 运行验证评价并持久化结果；最后一个验证结果触发数据库 Top-3。`OOS_SELECTED` 只登记暴露；`OOS_RESERVED` 才重建冻结 point-in-time dataset、运行挑战者和同权威冠军回测、持久化比较与历史门禁；PASS 启动 Strategy Shadow。
3. `advance_shadow`：从数据库解析 group 和唯一 expected session，调用现有确定性 Shadow 服务，不接受外部价格正文或 session 覆盖。
4. `finalize_promotion`：先完成 Shadow final gate；仅 PASS 时完成内部模拟 handover、请求 promotion，并在无未完成模拟订单且达到边界后激活。它不触碰真实账户或 Broker。

Job payload 增加 server-owned `candidate_state` 和 `candidate_version`，并纳入 input hash；generate/shadow operation 必须为 null。调度只把当前候选状态的下一阶段排队，并只把 `queued|running` 同版本 Job 视为重复。终态旧 Job 不阻止后续阶段。

冻结 family 必须明确绑定一个已 seal、complete、`promotable=false` 的 baseline champion backtest run；其 authority bundle、data snapshot、protocol、cost/source policy、code hash 和 generation 构成候选所有后续评价的唯一 authority。新增 point-in-time dataset loader 从该 bundle 的 exact-set member 表重建 `PointInTimeDataset`，并复用持久化校验验证内容 hash、集合完整性、available-at 和 publication mark。

验证评价不读取最后保留 OOS 区间。固定 validation report 由冻结 walk-forward 边界的 validation 子区间计算，聚合定义为：有效窗口 Sharpe 的算术平均、最差最大回撤、成本总和；窗口 authority 或数据不完整时 `authority_complete=false` 并失败关闭。只有 shortlist 后的候选能进入完整 OOS backtest。

## 7. 读模型与前端

### 7.1 读模型

- 比较使用公开 strict validator 校验 record IDs、metric report IDs、authority/protocol/cost hash 和 comparison hash。
- historical/shadow/handover decision 使用各自 strict validator 校验 stage、policy、comparison/finalization/session authority 和 decision hash。
- Shadow finalization status 决定 UI 状态：PASS=`finalized`，FAIL=`failed`，STALE=`stale`；不可变 group 的创建状态不能当当前状态。
- overview/detail 任一 identity/hash 不一致都返回 partial 或 stale，`write_enabled=false`。
- family 链校验一次批量加载 projections、candidate projections 和 events 后分组重放，禁止逐 family 查询。注册表一次读取 events 与 activations后重放。长历史测试至少覆盖 500 families/24 candidates 和 2,000 registry facts，并限定固定查询数量。

### 7.2 前端

- overview 和 detail 都必须返回非空且完全相同的 ETag；缺失或不一致时清除 detail、进入 partial 并关闭写操作。手动选择 experiment 同样执行比较。
- Shadow 评价进度分母使用服务端返回的 `evaluation_sessions=63`；`grace_sessions=5` 和已过宽限 session 独立展示。
- rollback 使用真正 modal dialog：打开后焦点进入确认按钮，Tab/Shift+Tab 留在 dialog，Escape/取消/确认/异步动作完成都恢复到触发按钮。
- 页面仍不出现真实下单、Broker、账户或实盘开关。

## 8. 迁移与回滚

`0023_evolution_authority` 只新增表、列、索引和 guards，并回填：

- 每条 lineage 建一行 projection，初始计数取现有非负计数最大值。
- 仅当既有 Phase 2 family 仍处于纯 `ADMITTED`、没有 comparison/Shadow/promotion 事实时，按 experiment 回填 candidate projection；存在无法唯一归属的进阶事实则 upgrade fail closed，不猜测候选。
- registry activation 无历史回填；如果 head 显示已生效的非 GENESIS transition 而没有可证明 activation，upgrade fail closed。

downgrade 只允许没有 `0023` 新事实且所有 head/lineage/family 仍能表示为 `0022` 语义的本地环境。存在 exposure、validation、activation 或候选进阶状态时拒绝降级，优先前向修复。

## 9. 验收门禁

必须同时满足：

1. 24 个候选并发验证后恰好最多 3 个 OOS_SELECTED；独立失败和重试不改变排序或其他候选终态。
2. OOS 暴露在运行前已提交；崩溃、重放、同语义跨 family 和两个 worker 并发均不会漏记或重复计数。
3. 比较服务拒绝 caller trial count，持久化计数与 backtest protocol、lineage projection 和 exposure ordinal 一致。
4. 直接篡改 candidate/family projection 或 registry head 会被服务和 PostgreSQL 双重拒绝。
5. API enqueue 后由仓库内 CLI worker 完成 family 至内部模拟冠军/回滚的真实 PostgreSQL 闭环测试；测试可使用固定 Agent 和 point-in-time fixture，但不得用 fake operation executor。
6. evolution 生产组合根静态扫描为零 Broker/Fosun/account/live-write/order-create 引用；paper order 数和 broker snapshot 数在闭环前后保持不变。
7. 读模型身份/哈希反例均关闭写；500-family 与 2,000-registry-fact 基准不出现 N+1。
8. Web 单测、类型检查、lint、build、63+5 Shadow 展示、严格 ETag、modal keyboard E2E 全部通过。
9. `0022 -> 0023 -> 0022 -> 0023` 往返迁移在允许条件下通过；有新事实时 downgrade fail closed。
10. Engine 全量、PostgreSQL integration、Web 全量和 Playwright 全量重新在最终提交树运行，并由独立只读审查确认 Critical/Important 为零。
