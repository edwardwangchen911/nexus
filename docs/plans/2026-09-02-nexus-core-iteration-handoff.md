# NEXUS 主线迭代与新主会话交接

> 2026-10-14 v10正式接线本地实现与隔离验收完成（未提交/未合入）：新setup-only formal-flexible-stage-budget保留原isolated flag；精确hash、完整池/Worker/输出资格、replay、管理read/目标baseline、budget保护和未调用cancel均补齐。迁移草案0078增加正式V10hash，旧0077不改，V10binding存在时拒绝降级；旧镜像与新登记模型不能直接假定兼容。89定向unit、220SDK/trace回归、40集中PG通过；新增4输出资格断言因旧固定读时点早于真实Worker finished而失败，改用DBclock后4定向通过，未改生产时点策略；3/5买入/no-trade四完整场景及1个下一轮baseline定向通过。Ruff/Mypy13source/generated/diff与集中审查通过，无剩余P1/P2。证据 .secrets/formal-flexible-stage-qualification/verification.json。业务未迁移/登记/选择/部署/启动新周期，main服务仍29/schema0077/locktrue；正式180/195/210和token/calls不变。已批准单次真实耗时诊断另已完成三调用：research24.530/critique49.988/synthesis22.773秒、70880/3217token，模型完整返回，仅私有回执无业务写；该次不等于自然周期成功，原失败事实不改。下一步需单独批准本地提交/合入及部署等状态变更，首轮research尾延迟仍未解决。

> 2026-10-14 完整输入耗时诊断入口本地实现完成：独立 full-input-timing-diagnostic-v1 私有身份，固定300秒含来源加载，committee295，最多3调用/96k输入8192输出/wire100479不变。完整历史池复用原manifest/hash/事件链、正式mode及同模型身份、成员终态和冻结context验证；全部来源只读，不创建Run/Job/周期或订单。规范source-run私有叶子在来源读取前写attempt，已有叶子/未知/失败禁重放；仅私有计时及用量下界回执。初始3RED、入口2RED、来源5RED；定向193项、入口8项、轨迹/工作负载54项通过，Ruff/Mypy4source/generated/diff通过，集中审查两P2修正并定向复核无剩余P1/P2。今天3候选真实源只读编译通过，模型0/业务写0；证据本worktree .secrets/full-input-timing-implementation/verification.json。未真实诊断、未提交/合入/部署；实际服务仍29/schema0077/locktrue。新观察入口不改变正式180合同，不证明可用或已解决超时。

> 2026-10-14 柔性阶段预算本地修复与隔离验收完成（未提交）：base29，新增冻结v10，仅critique在父180内扣close7、后续synthesis45+7、已有terminal5后用余量，research60/synthesis45、handler195/lease210、3calls/96k/8192/wire100479不变。旧v7/v8/v9 hash与分配保持。正常setup必须显式flexible-stage-budget及受保护diagnostic URL；configured/实际库都隔离，重复登记匹配新hash，默认/正式资格保持关闭。完整池同配置，隔离native准入和exact Worker载入v10。SDK父截止检查与保守deadline clamp；新增flexible_critique_profile计时身份仅v10critique写，<=116000，旧<=108000且serializer省略None保持JSON。初始RED5、计时RED1；216 SDK关联单测、36真实PG全回归（含v9与未知用量/未执行sibling seal）、最终6个v10 native/readback回归、49最终timing/trace单测通过。Ruff/Mypy15source/diff及generated check通过，独立定向审查无剩余P1/P2。Native PG期间定位并修复registry名单/daily准入/重复登记/旧timing108拒绝等真实接线，未放宽时间或断言。计时/trace正常reader已覆盖，SDK仅替身无额外真实模型。证据本worktree .secrets/flexible-stage-budget/verification.json及logs。main/实际宿主APIWorker仍29、schema0077、locktrue且heartbeat fresh；无迁移/业务登记选择/部署重启/真实新请求/回放/推送，本轮不提交。新profile仅可隔离研究资格，不具有正式输出或激活权限，自然模型是否能在预算内完成仍未知；今晚验收对象保持29。

> 当前自主模拟与策略进化的最终主线说明见
> `docs/plans/2026-09-12-nexus-simulation-evolution-roadmap.md`。该文档优先于本文下方
> 历史 Shadow 阶段和本轮未提交的过程性核查记录；后续开发应从其“下一份技术方案的唯一范围”开始。

## 2026-09-19 主线与组合分支集成交接

- 主线已收口 Agent Paper 订单跨执行窗口的宿主幂等与恢复：新路径以
  `order_id + market_window_id` 识别一次推进，在 `paper_once` 的同一事务中绑定
  精确窗口、执行请求和结果，并在读取旧 fixed-key 前取得与执行服务相同的
  账户事务锁；同窗口的宿主时钟变化或重启只读回原结果，
  不重复成交、账本或执行 authority。旧 fixed-key 只在原请求 hash、成交、
  执行 authority 与当前/前置窗口唯一匹配时恢复；无法证明的旧无成交结果
  保持未知。后续窗口即使已经提前落库，在前置窗口尚未消费时也会于任何执行或
  取消写入前被拒绝；运行恢复与停机确认共用同一份完整结果、原请求 hash、精确
  窗口及唯一请求证据，协调记录损坏或结果归属有歧义时不回退宽松查询。订单一旦
  已提交 v2 窗口请求，迟到的无窗口新请求即使在共同账户锁上等待到 v2 提交之后，
  也会在任何执行写入前被拒绝；原 legacy 同 key/同 hash 结果仍可精确读回。Alpaca
  允许 partial 后通过真实 `inspect → planner` 进入下一窗口；
  Schwab 仍严格保持单窗口，partial 后在采集前确定性停止。评价通道的确认
  使用实际 `action_account_id`；`cancelled/terminal/no_data/no_capacity` 也按原请求
  精确区分和读回。聚焦验证为 254 个相关单测、75 个 PostgreSQL 用例、
  Ruff 和 Mypy 通过；未放宽 `execution_locked=true`，未变更 Schwab 多窗口政策。
- 主线 `2a7120d6` 已修复评价账户在首槽真实成交后无法进入下一槽的问题：初始化
  完整性只核对唯一原始注资、两条初始分录和初始锁事件，同时允许后续合法交易与
  回撤事件。本次增补恢复了 `transaction_type='funding'` 与初始锁事件
  `event_seq=1` 的显式断言；真实 PostgreSQL 回归还覆盖重复进入不重复注资/Job、
  损坏原始 hash、后续事实复用初始化语义及初始事件序号篡改均 fail-closed。
  两个相关 PostgreSQL 模块共 70 项通过，Ruff、Mypy 通过。完整 Engine 回归另有
  9 项既有失败，已分类为 Agent execution fixture、历史 migration-head 断言及
  current ETF identity；不能据此写成全仓全绿。
- 共享 Agent Paper 运行逻辑由主线任务统一负责，冻结同步点为 `main@2fc4aca7`；
  组合管理第一切片仍由任务 `01a09987-af26-7692-95ec-a9e9483a13d2` 负责，
  实现分支为 `edward/portfolio-management-first-slice-implementation@f4fc10d5`，
  共同基线为 `576eba79`。组合代码未验收，不整体合回主线。
- 组合分支扩大 `agent_paper_loop.py`、`agent_paper_once.py`、`agent_version_runner.py`
  或对应测试前，先语义适配主线 `54e4ecd9`、`83cdf303`、`2560ecdc`、
  `e5fdd25b`、`05c7e0b9`、`ec29c1a6`、`a332fbba`、`2fc4aca7`；不整文件覆盖
  组合分支已有的收盘唤醒、synthesis exactly-once/orphan 恢复与结果未知处理。
- 必须保留的主线语义：版本评价共用一次冻结市场事实并用持久化准备声明防重；
  宿主不因单个 terminal cycle 退出；同槽 `failed/unavailable` 不直接停宿主；
  仅在精确同槽的 Schwab 终态失败已持久化时等待下一槽，未知或字段不成对仍 fail-closed；
  评价快照在 pending、researching、completed 及成交阶段始终携带冻结 calendar authority。
- 加速演练覆盖：跨槽继续、研究失败后原 Job 恢复、未知运行 Job 停止且不重放、
  宿主重启只领取原 Job、收盘仅发布一次并跨到下一交易日、双版本不完整 pair 关闭且不创建替代事实。
  `main@2fc4aca7` 已通过 203 个循环单测、38 个 PostgreSQL 生命周期集成测试、Ruff 和 Mypy。
- 组合分支已完成本次语义适配，当前为 `fd596169`：`8b226f9f` 同步主线宿主生命周期，
  `1c985d41` 保留跨重启的策略批准等待，`fd596169` 分离策略等待、收盘发布和分配。
  实现方验证为 199 个 PostgreSQL 测试和 471 个单测/边界测试通过；独立复核为
  8 个 PostgreSQL 测试和 254 个单测通过，Ruff、Mypy 及 diff 均干净。分支仍未合回主线；
  下一个组合切片只处理批准时点绑定与替换/依赖强制两个已知 proposal 问题。
- 主线 `01a0b599-a36e-7f1d-a332-7b264b1ac2be` 已按 `PAIR_LANE_TERMINAL_FAILURE` 安全终结，
  后续真实 4+20 评价为 `01a0b5a9-38ef-7131-8ff4-56ae3b09e32d`。
  本交接不授权 push、部署、迁移或交易。

## 当前迭代方向：Agent 模拟交易与策略进化（2026-09-13）

本节优先于下方历史阶段的“人工接受候选后开发手动 Shadow”路线。状态为产品迭代计划，
不是技术方案开发准入，不授权交易、调度启用、迁移或实盘。后续只展开最近一轮技术方案。

### 目标和边界

- 主目标：Agent 在明确范围内自主作出模拟决策，经确定性风控、模拟执行、账本和对账，
  与基线及实际市场表现比较，依据新样本验证策略改进；最终是否进入实盘由用户决定。
- 人工研究选择不是内部模拟研发或每次模拟决策的前置条件。现有审批代码不能因此直接绕过；
  技术方案须区分内部模拟与外部写入，修改既有行为需独立回归。
- Shadow 是对照评估能力，不是必经的独立子系统。冻结候选五日研究分支及复权评价版本路线
  暂停、不合并；不删除工作区、不改写历史，不因已有投入继续扩建。
- 首选复用内部模拟。券商模拟用于后续执行联调，不同时建设两条执行路径。外部写入的账户、
  操作范围和风险限额需明确授权；持续运行也须明确频率及停止条件，不隐含创建定时任务。
- 实盘权限不由模拟成绩自动授予。Agent 不修改硬风控、交易密钥或审批规则。

### 已核实基础与未证实部分

主线已有多源研究、候选分析和历史页面验收；复星、Alpaca、嘉信、SEC、FRED/ALFRED、FMP、
SSGA 与交易日历均有不同程度的真实持久化或 Agent 消费证据。已有 `nexus.paper` 订单、成交、
账本、绩效与 `nexus.ops.paper_once` 单次入口，以及 `nexus.evolution` 研发/验证代码。
代码存在不代表真实闭环完成。`docs/runbooks/nexus-paper-once.md` 记录了复权用途准入、
200 日特征覆盖及模拟成交证据限制；应核对当前数据和实际拒绝点，不能直接改政策开关通关。
复星模拟运行手册仍记录 fixture-only 接入，不可视为已激活券商模拟盘。

2026-09-12 本轮系列只读核查（不是持续实时状态）：本地 API 所连业务库迁移头为 0055，
没有研究 Shadow 表。有 1 个内部模拟账户、1 个策略定义、1 条注册事件、1 笔账本事务；
已有绩效快照的现金和净值等于初始资金，盈亏和费用为零。策略决策、模拟订单/成交、排名
checkpoint、研发运行/输出、回测、策略比较和晋升记录均为零，不能宣称实际闭环已运行。

具体断点及证据位置：

- `strategy/snapshot_reader.py`：forward 仅接受旧 v2 政策且复权资格关闭；底层 `_bars`
  还明确只接受 fixture 来源，真实来源成交量单位适配未完成。不能只改布尔开关。
- 当前存在 Alpaca all 日线 30 份，其中 29 份至少 200 行；并非完全没有复权数据。
  六份股票池合计 22 个不同标的，其中 15 个有至少 200 行 all 清单、21 个有同长度 raw
  清单。该统计不是单一策略股票池，也不证明时点、连续性和用途合格，不能作为验收覆盖率。
- `sector_exposures` 当前为空，但 `ops/sector_exposure_local.py` 已有审核资料导入入口；
  优先核对已有资料及分类口径，不再建设行业框架。
- `paper/execution_service.py` 要求精确模拟成交窗口；当前仅见 16 份各一行的 Alpaca raw
  分钟清单，合格模拟成交窗口记录为零。展示行情与日线不能自动获得成交用途。
- `strategy/rank_checkpoint.py` 读取注册策略，当前仅在跨周交易日条件计算排名决策；
  `evolution/registry.py` 已有候选验证后的策略晋升/激活逻辑。不能把周频规则运行称为
  每日 Agent 自由交易，也不必先新建策略注册系统。

梳理结论：研究 Agent 已消费多类真实证据，不重建研究系统；`baseline_v1` 只作为对照和运行
探针，不再作为唯一模拟决策者，也不在 Agent 失败时自动接管交易。优先补“Agent 组合级交易
意图 → 确定性仓位与硬风控 → 现有模拟订单、成交和账本”的连接，并为一条已有行情路径建立
最小模拟用途 authority。详细来源数量、用途和运行状态以顶部路线图为准。

### 迭代顺序与完成标准

| 轮次 | 唯一交付结果 | 完成标准与边界 |
| --- | --- | --- |
| 0 第一轮技术方案及可达性门禁 | 对一个明确的内部模拟账户和有界候选范围给出 Agent 驱动的最小可开发方案 | 明确 Agent 意图合同、确定性仓位转换、真实来源模拟用途、成本假设、正常入口和必要文件变更；若数据或授权不支持则明确拒绝，不以固定拒绝冒充闭环。 |
| 1 单次模拟闭环 | Agent 用真实已持久化证据形成意图，并走确定性风控、执行和账本入口 | Agent 判断、数量转换、订单、成交或合理不交易可追溯；账本守恒、重复请求不重复成交；现有 API/页面核对同一记录。受控测试覆盖成交路径，真实案例不强迫交易。 |
| 2 持续模拟与对照 | 同一 Agent 版本按固定节奏持续运行并每日复盘 | 先核实运行频率、资金、候选范围、限额和停机条件；比较 `hold_current`、`baseline_v1`、市场基准及 Agent 结果；断线或未知先对账，不盲目重发。 |
| 3 有证据的 Agent 改进 | Agent 产生一个可追溯版本改进并验证是否优于旧 Agent | 保留旧版本和固定比较口径；训练与样本外分离；新版本先并行模拟；无可靠改善则不替换，硬风控不由 Agent 修改。 |
| 4 券商模拟执行验证 | 验证目标券商的订单生命周期与对账 | 先证明模拟账户隔离和适配可达性并取得外部写入授权，再做有界联调；比较本地模拟与券商回报差异。不是实盘授权，也不是重新建设策略研发系统。 |

对照优先采用同起点、同资金和持仓的持有不动基线。真实账户为辅助比较，须处理出入金、
人工操作、币种和时间差异；不把账户余额差当策略超额收益。缺公司行动或费用证据时明确
不可比；成本假设须标注为模拟假设，不冒称真实账户收费，也不静默用零成本。

若模拟账户从全现金起步，持有不动基线就是现金，必须明确标注；另复用已有市场基准比较，
不能仅凭跑赢现金就宣称策略优秀。不得为了构造对照而擅自复制真实账户持仓。
第一轮技术方案须预先约定正常、缺数据、合理不交易、部分成交、结果未知五类验收行为；
策略合理不交易不是系统故障，但不能替代成交路径的独立验证。第二轮启动前约定观察周期，
第三轮验证前冻结比较指标与窗口，避免根据结果临时改判定标准。

### 最近一轮技术方案应回答的问题

先做只读可达性核查，不新建框架：

1. 选定已有内部模拟账户和 Agent 候选范围，核对初始资金、持仓、数据身份及其运行权限；无现成账户
   时列出最小初始化方案，不自动复制真实账户或虚构资金。
2. 沿 `paper_once.forward/admit/advance`、`nexus.paper.service` 和 `execution_service`
   核对生产者与消费者：价格用途、raw/all 配对、特征/股票池、日历、费用与公司行动。
   按“数据缺失 / 接线错误 / 现行合同不支持 / 外部依赖”列出实际拒绝点。
3. 核对单股 `ResearchIntent`、Portfolio Agent 和现有 `ForwardStrategyDecision` 的断点；设计最小
   组合级 Agent 意图，并让确定性代码负责仓位数量和硬风控。`baseline_v1` 只作对照和运行探针。
4. 只为已证实缺口指定修改文件、接口、测试和回滚；默认不新增表、依赖、政策框架或独立页面。
   若不能复用，先说明原因和最小替代，不开始大规模开发。
5. 第一轮验收使用正常产品入口和真实已落库来源，不注入未来行情或临时放宽资格；隔离测试
   不冒充真实验收。先查已有运行/订单/回执，不重放不明结果。

### 方案审查与执行纪律

#### 历史前置核验（2026-09-12，已被 2026-09-13 完整梳理取代）

以下记录保留用于解释当时为何没有进入开发，不再定义当前技术方案范围。当前范围、数据源矩阵、
Agent 职责和准入标准以顶部路线图为准。

本轮进一步直接核对注册头绑定的策略 document：当前冠军为 `baseline_v1`，其 universe.key
是 `us_liquid`，不是六份股票池的并集。业务库只有一份同 key 快照，其中一个成员；该成员
有至少 200 行的 raw 日线清单，没有至少 200 行的 all 清单。尚未验证对应时间及行情权威。
不得把此前 22 个标的的汇总统计作为该策略覆盖，也不得自动替换为账户持仓池。

已发布 `execution_model_v1` 使用 `1Min` bars、5 分钟窗口、close-volume VWAP、1% 成交量
参与率，有报价/无报价分别 10/25 bps 模拟滑点；commission_bps=0、regulatory_fee_policy=zero_v1。
这些是已存模拟假设，不是实际券商费用。不能静默覆盖模型或宣称当前绩效已包含真实费用。

第一轮完整技术方案暂不评高分、不进入产品开发。尚需确定：现有单成员池是否是预期策略
运行范围；若不是，明确目标股票池来源和范围。需在用户可见的成本假设下比较结果，若要
发布不同成本模型，先核对现有固定 v1 约束和兼容方式，不为规划先建版本框架。

可直接复用的实现边界：`ops/paper_once.py` 的 forward/admit/advance、
`strategy/snapshot_reader.py` 的 FrozenStrategyReferences 和读取器、`paper/execution_service.py`
的执行窗口及账本事务、`ops/sector_exposure_local.py` 的审核输入导入。当前不修改这些文件。
下一步先核对单成员池的来源/发布目的与策略初始化记录，形成一个明确的范围选择，再展开
数据读取适配和成交入口的精确变更及回归用例；不预先宣称只需改一个门禁。

产品顺序确认后才写第一轮技术方案；技术方案审查通过后才开发。审查必须逐项证明目标贴合、
来源可达、范围最小、资金/时间/幂等安全及正常入口验收可执行。用户要求的 95 分以上必须有
对应证据且无未解决的关键问题；未证实的数据可达性不能靠打分通过。此前 Shadow 97/96 分
不作为此路线准入依据。每轮完成一个可见闭环即收尾，后续轮次不提前展开为代码。

## 恢复任务入口（2026-09-09 更新）

- **目标**：一个真实组合问题完成可信研究、方案比较或可核实的不调整理由，并在页面可用；执行继续受人工选择与既有风控约束。
- **当前规则**：以仓库 `AGENTS.md` 和用户最新指令为准。下文的日期、基线、负责人、调用额度及验收结果属于历史记录，不恢复旧审批，也不当作当前运行事实。
- **进展入口**：[现有证据记录](2026-09-02-core-evidence-checkpoint.md)。先搜索标题，再读取相关段落；区分实现、集成、运行与真实验收，不整篇加载。该记录由多个任务追加，不能仅凭最后一段推断全部主线状态。
- **恢复步骤**：核对自己的路径、分支、差异及相关活跃负责人；定位当前问题已有实现和验收证据；只补缺失环节。历史 `f883988` 及下文计数不得替代当前核查。
- **最新恢复点（2026-09-11）**：`main@20745806` 已具备“当前组合 → 正式诊断 → 一个确定性调整情景 → Agent 解读 → 人工研究选择”的页面闭环；旧三方案只读保留。三种选择都只绑定当前精确情景，不能启动 Shadow、创建订单或解除 `execution_locked=true`。Alpaca IEX 多标的 WebSocket 批次导入和复星 broker-only 新快照入口均已落地；北京时间 2026-09-11 21:32 的一次性验收仍未发生，当前真实情景点差覆盖仍是 0/12，不能提前宣称通过。先查该验收的持久化结果，禁止重复采集或重复刷新。佣金继续缺当前账户适用费率，滑点和税务也保持 unknown；不要从现金流水字段或公开活动费率反推。若开盘验收成功，下一产品阶段才评估“已人工选择且具备资格的候选 → 手动 Shadow”，不新增通用求解器、自动执行或更多 Agent 角色。
- **交付格式**：可用结果｜提交/路径｜实际验证｜精确依赖｜下一步。交付时在现有证据记录中更新；纯等待不追加进度文档，不反复请求对方状态。
- **成本核对**：需要比较优化效果时，复用已有用量日志，区分普通输入、缓存输入和输出，并对照耗时、返工与验收；不累加重复累计值，不把 token 总量直接当账单。不新增遥测或自动模型调用。

### 当前迭代摘要（核对基线 main@0148488，非实时状态）

| 用户结果/事项 | 已有证据与下一步 |
| --- | --- |
| 当前趋势进入研究页面 | 已有一次正常入口到研究结果页面的真实验收；恢复时定位证据记录“当前趋势真实导入、页面与一次新研究闭环”，不重跑已完成案例。 |
| 真实基线与候选缺口可解释 | 多维显示与无方案说明已集成；继续核对真实基线、缺失原因及草稿正常入口。正式方案还受准入政策影响，先查代码拒绝点，不把补数据或再跑研究当成必然解法。 |
| 独立历史回测/股票池 | 预检已定位身份前置缺口；下一步验证正常身份来源与发布路径。该依赖只限制自身，不暂停当前研究和方案展示。 |
| 已集成但待真实运行确认 | 0148488 身份刷新修复已有隔离回归及已保存身份只读核验；新鲜券商刷新尚未验收，随下一次必要正常调用验证，不为更新状态额外采集。 |

推进归属：最近确认的主线集成负责人为核心任务 `01a061bf-12c7-7cb2-b94b-0b696c5ab6db`；协调任务 `01a062a5-269b-7060-ab5f-b294c536a588` 跟踪独立实现与依赖。这是恢复线索，不按会话 cwd 推断写入所有权，也不据此重派进行中任务。集成负责人仅在交付或依赖变化时替换本摘要，链接详细证据，不持续追加状态流水。

### 下一个切片的执行方式

在现有任务上下文用一小段写清“用户可见结果、现有拒绝点、正常入口、适用异常状态和验收证据”，随即实施；不另建规格或批准环节。先定位政策/数据/接线边界，再用同一代表性案例贯穿接口和页面。验收调用产品入口并复用其配置解析；检查失败先判定产品故障还是验收脚本问题，结果未知时核实已提交状态，不重放动作。切片负责人负责到集成验收，主线合并仍由唯一集成人串行执行。

以下保留原计划及历史执行记录。原核查时间：2026-09-02 16:57（北京时间），代码基线：`main@f883988`。

## 1. 用户要求与工作方式

本计划承接用户明确要求：基于项目现状确认进展、制定详细迭代计划，并新开主会话执行后续迭代；非必要不请求审批，不默认使用 Superpowers，不做过度设计，保证核心任务落地。

- 本计划是执行输入，不再为每个常规切片重复写规格、召开设计确认或询问“是否继续”。直接完成范围内的本地代码、测试、文档、审查和本地提交。
- 已有相关技能只有确实必要时才用；不要仅因接到任务就套用 Superpowers 全流程。必要的安全、测试和证据检查仍要做。
- 外部账户/券商写入、模拟或实盘订单、部署、购买或新增付费授权、扩大权限、破坏性操作、Git 历史改写、实质安全边界改变，才需要明确确认。
- 正常只读与现有套餐研究遵循当前 `AGENTS.md`，合理管理限流、超时和用量，不按端点、分页或调用次数重新审批；新增付费、显著增费、访问范围扩大等仍按当前边界确认，失败不自动重试。
- 数据库结构变化先说明影响与回滚，复用既有迁移流程；不为普通应用实现新增审批门槛。若迁移会破坏现存事实或扩大安全边界则停止。
- 不请求用户通过聊天提供凭据，不输出认证文件、完整环境变量、真实账户标识、资产金额或仓位明细。

## 2. 已核实基线

本轮直接读 Git、本地 PostgreSQL 聚合状态和本机 CLI 状态；未刷新券商、未调用模型推理、未重跑全量测试或浏览器验收。

| 项目 | 2026-09-02 16:57 核查结果 |
| --- | --- |
| 主线 | `f883988`，核查时干净 |
| 真实账户快照 | 12 条持仓、3 条现金币种记录、2 条未终结订单记录；快照截至 11:33，非实时重新探测 |
| 准入状态 | `DISPLAY_READY`、`DECISION_READY`、`cash_flow_quality=PASS`；执行仍锁定 |
| 组合诊断 | 1 份、6 项，`RESEARCH_REQUIRED`；历史市场证据 `MARKET_EVIDENCE_UNAVAILABLE` |
| SEC 披露 | 13 份：3 份 10-K、8 份 10-Q、1 份 6-K、1 份 497K |
| 结构化财务事实 | 338 条，涉及 11 个标的；不能把 6-K/497K 文本视为公司财务指标或当前基金成分 |
| 真实研究 | 12 次 completed、覆盖 12 个标的；全部 `partial / watch`，共同缺口 `Price authority is unavailable.` |
| 模型接入 | 本机 `codex_sdk` 登录检查通过，套餐剩余额度 unknown；不是 API Key 阻塞 |
| 方案 / Shadow | 正式方案组 0，Shadow 组 0 |
| 内部模拟 | 当前库订单、成交、绩效快照均为 0 |
| 后续模块 | 回测、三方案、Shadow、硬风控、模拟账本、策略进化已有代码和历史离线验收，不从零重写 |

最近关键提交：

- `7e04e50`：冻结组合上下文接入研究工具。
- `14dc23f`：官方 Codex Python SDK 本地研究入口。
- `6b3f196`：研究结果接回组合持仓页。
- `50c59c7`：6-K 披露正文进入研究。
- `a27ea44`：基金 497K 披露进入研究。
- `f883988`：复星 K 线显式请求 `right=noward`（不复权）；这是代码修正，不证明历史数据已重新获取或已满足研究准入。

README 顶部“研究运行仍为 0”等旧数字已过时。更新文档时给出核查时间，不用静态数字冒充持续实时状态。

## 3. 产品目标与不变量

目标闭环：真实资产 → 确定性诊断 → 组合问题驱动研究 → 程序构建和比较方案 → 人工选择 → 影子验证与归因。

- 首期是个人 Mac 本地资产研究和决策工作台，不是无人值守交易机器人。
- 复星继续是账户和已授权行情的主要来源；补充源只处理明确缺口，不替换账户事实源。
- `00656.HK` 只读。模型不能直接下单、改硬风控、访问券商密钥或批准自己的方案。
- 保留 `ResearchIntent` 无订单数量/权重字段的边界，目标权重仍由确定性 optimizer 生成。
- 价格显示可用、研究证据合格、正式诊断准入、方案合格和交易批准是不同状态，不互相冒充。
- 允许保持现状、观察和有理由的阻塞；不以“变成买入建议”作为验收目标。

## 4. 所有权与新会话启动

旧核心会话：`01a05c77-1dec-7972-91ea-7afacee97bf2`，名称 `【排障】定位复星诊断启动阻塞｜排查中`；本轮核查为 idle。旧 UI 会话 `01a05d5c-6d6e-7f81-9975-d8ef2fed9d15` 为 notLoaded。不要仅依据标题里的阶段词判断真实状态。

- 新主会话为后续核心实施与合并负责人；启动后再次检查活跃会话、Git 和 worktree，避免时间差。
- 新主会话默认独立 worktree；本地服务和既有配置位于 `/Users/wangchen/Documents/ChatGPT/投资`。不要把 worktree 缺少未跟踪配置误诊为用户没有配置。
- 不复制、输出或提交 `.secrets`、认证文件、生产/真实账户配置。需要查本机现状时使用旧根目录已有的脱敏只读 CLI；测试使用隔离数据库和端口。
- 同一 worktree 只有一个写入者。不要未经协调修改旧根目录；经验证的切片由新主会话串行集成，集成前确认旧核心仍无写入、main 干净，不覆盖未提交内容。
- 默认至多两条独立实现线：价格证据线和研究流程线；合同未确定时先串行。需要辅助任务优先复用既有会话，使用独立 worktree；不为形式上的并发继续新开一堆会话。
- 重型集成测试错峰；每个切片一轮集中审查，不机械套用多轮独立审查流程。

## 5. 迭代 I1：历史价格证据与正式诊断闭环（立即优先）

### 工作

1. 只读追踪一条既有日 K：Gateway 请求/响应语义 → Provider 规范化 → 数据库和对象存储 → series manifest → 研究价格冻结 → 诊断读取。明确每一个拒绝原因，不先改门禁。
2. 检查历史长度、缺失交易日、源/feed、价格/复权口径、公司行动、可用时间、原始证据与 manifest 绑定，区分“数据不够”和“已有合格数据没接上”。
3. `right=noward` 只证明后续显式请求的参数意图；不得把参数曾省略的历史数据直接标成已验证的不复权数据。不能仅为通过测试把 display 数据改为 research authority。
4. 优先复用合格历史材料。确需刷新时按当前授权复用本地入口；先核实访问条件，正常只读自主推进，只有触及当前授权边界才确认，不扩建 Probe 系统。
5. 按指标声明最低数据要求。原始收盘价能支持的分析与需要公司行动处理的收益/回撤计算分开；不足项保持 unavailable，不凭空宣称总收益或跨币种合计。
6. 形成新的研究快照和诊断版本，保留旧记录。在有价值的单案例上验证后再扩到持仓，不重复跑 12 份模型报告来制造进度。
7. 页面展示具体证据缺口、数据截止与已可用指标，修正 README 过期状态。

### 主要位置

- `integrations/fosun_gateway/src/nexus_fosun_gateway/adapter/{sdk,probe_sdk}.py`
- `engine/src/nexus/ops/fosun_local_refresh.py`、`engine/src/nexus/providers/`、`engine/src/nexus/market/`
- `engine/src/nexus/agents/price_authority.py`、`snapshots.py`
- `engine/src/nexus/diagnostics/series_authority.py`、`market_risk.py`
- 既有 portfolio/research API、BFF 和相应页面，仅按需要改动。

### 验收

- 一个真实案例的行情来源、口径和时点可追溯；能算的风险项有真实结果，不能算的有精确原因。
- 更新后的诊断通过数据库/API/页面呈现，不降低 fail-closed 规则。
- 未来数据、混合复权/来源、缺交易日、重复刷新、旧快照误用均有反例测试。
- 本轮结束时交付页面/API/数据库结果，或可复核的精确外部数据缺口；“新增 helper/manifest”不是独立完成标准。

## 6. 迭代 I2：最小组合问题研究（可在 I1 合同明确后并行）

### 工作

1. 在既有单标的 `ResearchRequest` 之上增加最小组合问题上下文：问题、诊断来源、相关标的、约束版本、快照和研究截止；优先兼容扩展，不先建通用任务平台。
2. 保留当前 SDK 批量八角色流程为 v1 基线。v2 只做“研究 → 反方检查 → 综合”；后一步输入前一步的结构化论点和引用，不能仍然各说各话。
3. 继续由宿主程序提供最小、脱敏、冻结证据，保持 SDK 无工具/无文件/无网络能力。主研究流程不能获得数据库和券商凭据。
4. 区分人读报告与程序读 `ResearchIntent`：报告解释问题、假设、证据、反证、不确定性与失效条件；研究意图保持受限非执行合同。
5. 不把机械票数、固定有效期或角色齐全当作研究质量。综合要说明接受或未解决的异议；有效期由明确政策或证据类型约束，不由模型任意延长。
6. 每阶段保存输入版本、输出、用量和终止原因。围绕当前 job/lease 做最小恢复；外部调用结果不明要记录费用不确定性，不盲目自动重试，也不承诺外部 exactly-once。
7. 首版沿用 SDK 与现有运行器。只有实际步骤恢复/分支复杂度产生重复实现时才评估 LangGraph，且只替换研究编排，不能产生第二套资产或审批事实源。

### 主要位置

- `engine/src/nexus/agents/{models,committee,codex_sdk,execution,synthesis,persistence}.py`
- `engine/src/nexus/agents/{portfolio_context,tools,database_tools}.py`
- `engine/src/nexus/worker.py`、`engine/src/nexus/jobs/`：仅处理相关运行边界。
- `engine/src/nexus/api/{routes/research,schemas/agents}.py`、既有研究组件。

### 验收

- 一项真实组合问题在页面呈现研究、具体反证和综合；能说明为何需要/不需要调整。
- 对比 v1 与 v2 的同源案例，检查引用支持度、反方有效性、耗时和用量；不只比较输出长度。
- 注入缺价、矛盾证据、错误引用和中途失败，结果必须可解释且不会越权。
- 不要求新增依赖、新迁移或新角色才算进度；必要结构变化说明影响与回滚。

## 7. 迭代 I3：正式方案比较与选择（依赖 I1/I2 必要输入）

### 工作

1. 复用 `optimizer` 已有 `hold_current/minimum_change/strategy_target`、目标权重、成本和权威校验；不得让 Agent 直接填权重或绕过 selection authority。
2. 对齐研究截止、组合估值时点和方案基线的语义；分别记录，不为了满足比较运算而伪造同一时间或倒填时间。
3. 接通 `get_proposal_comparison`，只读已经生成并绑定本次权限的方案结果，不让读工具暗中创建方案。
4. 先让一个合格案例完成数据库 → API/BFF → 页面比较和人工选择；保存选择理由和使用的版本。
5. 诊断不满足优化准入时仍给出明确的保持现状/缺口结果；不得强制制造三套不同方案。

主要位置：`engine/src/nexus/optimizer/{production,models,compare,admission,lifecycle}.py`、`agents/database_tools.py`、既有方案 API/页面。

验收：现金与权重守恒；约束及来源时间正确；同一基线比较；不合格数据明确拒绝；重复提交幂等；人工选择不能由模型代替。没有满足条件的真实输入时先完成隔离测试与只读展示，不伪造正式准入。

## 8. 迭代 I4：影子验证与复盘（依赖合格方案及用户选择）

- 复用现有 `shadow`、`backtest`、`paper/performance`，不新造回测和账本。
- 先实现一次手动推进和对账，展示保持现状与选定方案的成本、风险、表现及缺口。
- 测试缺失交易日、重复推进、公司行动、失败恢复、基准与归因口径。
- 长期稳定性观察建议覆盖至少 5 个交易日，仅为运行验收，不是盈利有效性证明；不能阻塞前面可用切片的交付。
- 当前 `local_read_only` 不含自动调度与下单。需要真实启动 Shadow、内部模拟执行或调度时，核对已有针对该动作的选择/授权，不把新主会话授权当作交易批准。
- 需要日后持续运行时使用产品提供的定时机制；未实际创建监控前不得声称会持续盯着。

## 9. 测试与交付规则

- 首先阅读实际目录下的 AGENTS.md、现有测试和迁移机制；主库是 Python/SQLAlchemy/Alembic，不根据过时脚手架说明另建 Drizzle 事实源。
- Python 优先相关单测和隔离 PG 集成测试；行情相关覆盖 source/as_of/adjustment/duplicate/gaps，研究相关覆盖角色上下文/引用/预算/恢复，方案相关覆盖现金权重/准入/时间/幂等。
- 前端相关改动运行 `npm run typecheck`、`npm run test:unit -- <相关测试文件>` 和必要的 lint/build；API 合同使用既有生成器更新，不手工漂移生成文件。
- Gateway 验证使用现有离线 socket guard。禁止测试连接真实券商、共享真实账户数据库或消耗真实模型额度。
- 每轮一份简明交付：用户可见结果、提交、实际运行的测试、剩余缺口、下一步；本地 merge、push、部署、历史验收和本轮验证分开报告。
- 常规切片完成后继续下一项安全任务，不把“本轮结束”当成必须等用户批准。外部卡住时推进无依赖本地任务，但不要转去堆工程基础设施。

## 10. 明确暂缓

不整体迁移 TradingAgents/FinRobot/LEAN；不同时引入多个 Agent 框架；不重做 UI；不扩张数据源或完整估值库；不增加通用 Agent 平台、复杂消息基础设施和发布签名；不深化策略自动进化；不启用券商模拟/实盘写入。

第一阶段停止条件：一个真实组合问题有可信研究、可计算的方案比较或可核实的不调整理由，页面可用，后续 Shadow 入口受人工选择控制。达成后再评估下一阶段，不无限架构优化。

## 11. 原计划的首次启动步骤（历史，恢复任务使用顶部入口）

1. 核对主线是否晚于本计划基线、旧核心是否仍 idle、自己 worktree 是否包含计划；先读现有进展，不重复实现已交付部分。
2. 在安全本地入口读取聚合状态；明确 12/12 研究已完成而全部缺价格权威，不再问用户补 API Key。
3. 选择一个现有持仓的日 K，在本地追踪到价格权威/诊断拒绝点，给出具体原因并实施最小范围本地修复。
4. 完成聚焦验证与页面/API/数据库交付；串行集成前重新核对 main 无其他写入。
5. 连续推进 I1 与依赖清楚的 I2，不先索要逐步批准，不先跑框架选型工程。

## 12. 新主会话首轮执行记录（2026-09-02）

执行会话：`01a0618a-7805-7140-988d-4bfce47309b9`；原核心已停止写入。

- I1：`a17f049` 增加本地持仓价格证据聚合状态。12 个持仓均有日 K，最新各序列
  38–277 行，但交付模式均为 unknown、未复权，已登记价格权威为 0；最新序列晚于账户
  快照可用，不能倒填到旧快照。没有将展示可用冒充研究/优化可用，也未重新请求券商。
- I2：`6a0d603` 落地 research → critique → synthesis，复用冻结证据、既有角色输出和
  SDK，无新服务或迁移。单次真实验证首阶段校验失败，12,107 输入 / 904 输出 tokens，
  未生成有效新报告、未自动重试；不能宣称 I2 已通过真实验收。
- 随后收紧阶段 schema、验证具体回应关系、接受等价角色排列、记录安全错误分类。
  默认准入排除实验 v2，只有显式模型配置 ID 可登记验证；不改写失败历史或既有配置。
  60 项研究单测、11 项聚焦隔离 PostgreSQL 集成测试通过，相关 Ruff 和 mypy 通过。
- 独立 UI 切片 `e0f7fe5` 已由本会话串行集成为 `4f186f7`，加入研究页基础组件和证据抽屉。
  主线验证 75 项前端测试、类型检查、lint、构建和生成合同一致性通过。主目录浏览器
  回归受已有 3300 服务的单实例锁阻挡，未停止用户服务；UI 独立目录已报告浏览器验证。
- 后续仍以 I1 合格价格及 I2 一个真实组合研究闭环为优先；I3/I4 未因这些代码提交而
  自动达成。下一次模型验证须单独限定调用次数/额度，不扩展到全部持仓，不启动自动调度。
- 本轮仅本地提交和集成，未推送或部署，未创建订单、修改券商账户或启用后台监控。

## 2026-10-15 v10运行迁移标识最小修复

199c754b仅将CURRENT_MIGRATION_HEAD从0077改为0078，并补充迁移链/运行标识一致性、真实正常Worker首次轮询写心跳、新schema调度准入和旧schema拒绝回归。修复前1项unit及2项PG准确失败；修复后18项定向通过，Ruff/Mypy/diff通过。候选镜像nexus-engine-formal-v10:runtime-head-96200045ab19读回658文件一致，镜像内Worker标识0078；main应用检查通过。私有证据.secrets/v10-runtime-head-fix/verification.json。业务服务未更新，宿主保持停止；遵循上轮失败停止边界，已请求批准合入/更新APIWorker并恢复原宿主，无新迁移/凭据/周期/封存/推送。

### 2026-10-15 same-day recovery candidate (local qualification only)

7617eded adds explicit --same-day while preserving the default prior-UTC-day guard. Only terminal failed research with expired lease, no occupied orders, quiescent processes and verified original frozen input/request/terminal metadata is eligible. Unknown usage and its reservation remain unchanged; no old invocation is replayed. New audit schema paper_same_day_close_v1 preserves old v1 audit semantics, verifies later reads at the original audit time and rejects changed facts. A second process check fences commit; late failure rolls back. 129 targeted tests, Ruff, Mypy and review passed; image candidate-4b540cf09743 matched 658 source files. Real failed input was checked read-only, not disposed. Main remains 199c754b/schema0078. No deployment, real seal or new cycle has run. Before rollback, stop new manual requests and retain all new facts; old consumers are not assumed compatible with new audit receipts. Private evidence: .secrets/same-day-close-qualification/verification.json.


### 2026-10-15 manual preparation diagnostics candidate

Candidate 92276fa3 fixes primary trigger diagnostics lost by manual dispatch, including secondary failure while recording unknown or disposing resources. Only fixed trigger_request/trigger_reservation/trigger_provider phases, allowlisted error codes and fixed exception kinds are emitted; original response text remains hidden. Unknown or preparing requests remain non-replayable. No migration, retry, admission, usage, budget or execution permission changes.

368 focused unit tests, 89 isolated PG regressions, 5 post-review targeted PG checks, Ruff, Mypy and independent review passed. Qualification covers same-day failed research disposition through a new v10 request claim/reservation, and normal three-stock v10 HTTP/CLI/host -> trigger_once -> native cycle/research Job persistence with controlled sources, not a live model or simulation acceptance. Candidate image matches 643 Python/migration/project files. Current real batch/model hashes and synthesis prompt read back valid; the old masked preparation cause remains unknown and must not be inferred from tests. Main/runtime and the existing unknown request were not changed. Evidence: .secrets/manual-preparation-diagnostic-qualification/verification.json.


### 2026-10-15 未建周期的未知准备收尾：本地隔离候选

新增显式 `close-unstarted-preparation --request-id`，仅对已审查原生产者、完整原请求/事件、原生认领及周期绑定均不存在、宿主及研究进程退出且账户无占用的情况追加独立审计。原 unknown 事件、预算与预留保持不变；旧请求不能重放。准入与宿主队列验证审计后才跳过已收尾未知记录，后续独立请求仍需新鲜宿主和原有全部准入。复用正常服务生命周期锁并持有至数据库提交；证据变化、重复/模糊生产者、未完成原生认领及迟到占用均拒绝。无迁移。

39 项隔离 PostgreSQL 验证、18 项 CLI 定向单测及 Ruff/format/Mypy 通过；此前 370 项聚焦单测通过。修正旧迁移测试：失败降级后应保持实际降级前 head，而非写死 0075。两个历史 trigger 源码哈希已对 Git 原件读回。未构建或部署本候选。

真实适用性仍拒绝：旧请求最后心跳比 preparing 早 0.482 秒，原事件无生产者 ID 绑定；不能仅凭认领/周期为零证明无外部副作用。正常宿主只读状态 inactive，最新研究进程静默检查通过，但不补足历史身份缺口。旧异常已被屏蔽的具体原因仍未知。没有真实收尾、新周期、凭据修改或宿主启动；不能宣称产品真实闭环验收完成。当前批准范围仅本地实现与隔离验收。私有结果 `.secrets/unstarted-preparation-close-qualification/verification.json`。

#### 原生产者归档核对（2026-10-15，只读）

启动归档确认 7617eded 部署；故障日志只有 execute/TRIGGER_MANUAL 结果未知及时间，无 request/producer 身份和原异常。只读业务事务确认 queued→preparing→unknown、无周期绑定、原生认领0、匹配请求审计0；事件表没有 producer 身份字段。最近历史心跳早于 preparing 481.946ms，其他历史版本心跳更早。部署身份不代替逐请求执行身份，现有候选仍不可用于真实旧记录；不能靠等下一周期补历史证据，也不重放或回填。原准备失败原因仍未知。证据 `.secrets/unstarted-preparation-close-qualification/origin-archive-diagnostic.json`。如后续讨论人工明确接受准备结果未知的处置，须与已证明无副作用的收尾分开，保留未知事实并保守覆盖可能消耗，另行评审合同；本轮未实施或授权该替代。
