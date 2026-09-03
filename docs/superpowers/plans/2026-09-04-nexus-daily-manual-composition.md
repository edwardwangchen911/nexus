# NEXUS 显式手动日流程 Implementation Plan

> REQUIRED SUB-SKILL: superpowers:executing-plans。root 实现、A 串行集成；B 的风险 authority 工作独立进行。

**Goal:** 让本地使用者针对一个明确的已持久化账户快照，执行有边界、可恢复、不碰其他任务的日诊断入口；随后逐片接入真实研究与方案，不自动启用调度、模型或模拟交易。

**Architecture:** 复用现有 Job、lease、dependency 和诊断服务，不另造执行器或运行表。当前日阶段和原生研究/优化 Job 同名但 schema 不同，不能直接注册到同一个 registry。首片用独立的手动 root 类型与既有 quality/diagnostics 阶段，后续原生任务只能在其前置成功后正式准入。

**Tech Stack:** Python 3.12、SQLAlchemy/PostgreSQL、现有 Pydantic/pytest。无新依赖，无数据库迁移。

## 约束与 ownership

- root worktree `.worktrees/nexus-team-plan`，分支 `edward/nexus-daily-manual-composition`；实现基线 `fc88e1d`。
- root 独占 `jobs/models.py`、`jobs/repository.py`、`jobs/worker.py`、新 `jobs/daily_manual.py`、新 `ops/daily_once.py`、专属测试/手册。A 确认范围无冲突。
- 不改 `nexus/worker.py` 的长驻执行或旧 daily payload/schema；不修改历史 root、研究、快照与时间。
- 手动实际截止独立于旧流程的纽约 18:00 计划截止；诊断内容仍严格使用所选快照原来的 `as_of`，不得借手动执行补入后来事实。
- 首片只读取已有账户事实并持久化本地 Job/诊断，不采集券商、不调用模型、不生成订单、不创建账户资金或 QualityReport PASS。
- 拆为小提交；每片先红回归、定向验证、一次独立审查，A 合入后核对内容。

## Task 1: 通用 worker 的精确作业范围

**Files:** `jobs/repository.py`、`jobs/worker.py`、新 `tests/integration/test_job_scope.py`。

- [x] 给 `claim_next_job`、`reclaim_expired_jobs`、`run_job_worker_once` 增加可选 `job_ids: tuple[UUID, ...] | None`。None 保持既有队列行为；空 tuple 不领任何任务，也不回收任何租约。
- [x] claim 的 ID 条件与 queue、job_types、可用时间、依赖、重试额度取交集，不绕过原控制；reclaim 在 SQL 选择与加锁前限制同一 ID 集。
- [x] worker 在 claim 与 reclaim 传递同一冻结范围；heartbeat 保留现有自己的 worker liveness，不修改其他 worker。
- [x] PG 回归：外部更高优先级 queued Job 不被领取；外部过期 running Job/lease/attempt 不变；范围内过期可回收；空集合零 claim/reclaim；未指定范围行为不变；ID 不绕过队列、类型、依赖和未来可用时间。
- [x] 运行原 job lease/worker 集成回归与新测试、Ruff/Mypy/diff-check，审查后交 A。首批6项因缺少新参数跑红；实现后新旧28 PG通过，补空worker/外部前置依赖后8项新PG再验通过。两源 Mypy、Ruff、改动格式/diff-check通过，独立审查无P1/P2。尚待主线集成，不表示整个日流程可用。

## Task 2: 冻结手动 root 与实际诊断

**Files:** 新 `jobs/daily_manual.py`、`jobs/models.py`、专属 unit/PG。

- [x] 新手动 root 类型区分旧 `daily.pipeline`，不改变旧根 schema 或既有同名原生研究任务。Job 表已有字符串类型，无迁移。
- [x] 根请求显式绑定 portfolio snapshot ID、真实 UTC 截止及已有 XNYS calendar/session。冻结数据不晚于截止，不选择运行时 latest。幂等身份由此完整冻结输入决定；相同输入不重复建根/诊断，不改变旧根。
- [x] 三节点 root → quality validation → diagnostics。root 仅构建两子节点；quality 检查既有快照 READY、对应 sync 的 cash-flow PASS、真实时间/日历关系；不生成质量报告或改变账户快照。
- [x] diagnostics 校验 root/target 一致，调用既有 `diagnose_and_persist`，保留 snapshot.as_of 与来源边界。诊断不合格则明确失败/跳过，不伪造“无需动作”。
- [x] 保留原 Job lease/fencing/事务/依赖语义，重跑复用同一诊断。失败和等待不扩散到其他 root。
- [x] PG 贯穿根入队→原 worker→校验→真实诊断持久化，覆盖晚到快照/日历、未对账快照、重复输入、依赖失败、越界子节点；只用合成账户数据。9项新PG与20项job model unit通过，审查发现BLOCKED也被阶段标记成功，补第10项红回归后修成skipped；10项新PG+22项旧daily PG共32通过。artifact reader显式必填，不用缺能力替身冒充业务可用。独立定向复核ready，Ruff/两源Mypy/diff-check通过。普通CLI与业务真实案例尚待Task3，不声称日研究/方案全链路完成。

## Task 3: 显式单次本地入口

**Files:** 新 `ops/daily_once.py`、专属 unit/PG、现有 runbooks 下手册。

- [x] 显式 admit 与 advance；读取现有 EngineSettings，不创建配置或凭据。只允许现有本地 PostgreSQL 入口，复用既有校验；固定安全错误码，不输出账户事实或原始异常。
- [x] advance 只处理所选 root 及其两个经验证归属的子节点，最多三次原 worker tick。遇未来可用时间或未就绪依赖立即返回，不 sleep/poll、不主动重放外部调用。
- [x] 新命令无自动调度与后台 worker，不自动运行研究/方案/paper。输出仅作业阶段/安全状态；完整数据仍在受保护业务存储。
- [x] 入口→PG 实际调用链验收；同时布置其他根的高优先级及过期 Job，证明完全未被处理。说明兼容性与回滚：停用新入口即可；不删历史作业/诊断，无迁移回退。

  首批6单测及2入口PG通过。审查发现开发默认对象库会掩盖真实缺配置，先红确认会创建reader，再以局部 `DailyOnceSettings` 在父类填fixture前捕获五项显式非空输入；`advance`缺项先拒绝，不能用外部布尔标记绕过，全局fixture设置不变。14单测、2入口PG通过（标准 `PYTHONPATH=engine/src` + importlib），Ruff/格式/Mypy/diff-check通过，独立定向复核ready。Task1/2已由A合入主线`3f009ad`，root逐文件比较确认一致；Task3待A集成及真实业务快照验收。

## 后续 L1 切片（不能以首片完成替代）

### Task 4a：研究快照显式绑定日根持仓快照

**Owner/Files:** root；`agents/portfolio_context.py`、`agents/snapshots.py`，专属PG。基线同步主线`3f009ad`，独立分支`edward/nexus-daily-research-binding`。A独占Gateway来源字段留存，B独占rank continuity，无文件重叠。

- [x] `freeze_portfolio_context(..., *, portfolio_snapshot_id: UUID | None = None)`：None原样选择唯一账户最新可见快照；显式ID只读取这一已知快照，保留原active/source/time/holding/ready检查，不退回另一个READY或latest。
- [x] `prepare_research_snapshot(..., portfolio_snapshot_id=None)`转交显式选择；存在SEC/issuer证据但指定context无法冻结时明确拒绝，不静默生成不含该组合上下文的通用研究。没有研究证据仍按现有合同返回None、不创建run/Job。
- [x] 不新增冻结schema字段；所选snapshot ID已经在既有context及规范hash里。相同旧快照、相同时点与旧默认选择结果严格同hash；不改旧研究行。
- [x] PG覆盖两个可见快照中精确选择较早指定者、无参数仍选择最新、缺失/晚到/不持有目标证券不回退、非READY显式上下文不伪装READY、历史context/hash不变。
- [x] TDD、Ruff/Mypy/相关PG、独立审查后交A。5项新PG因缺参先红；实现后与原准备/组合上下文共22项PG通过，标准PYTHONPATH仅engine/src+importlib。为复用原synthetic filing并修相关测试跨目录导入，将原seed原样提取至helpers/research_snapshots.py，无fixture业务数据变化。两源Mypy/Ruff/diff-check、独立审查ready。

### Task 4b：原生研究准入接线（接4a后实施）

日根诊断成功后才能准入原生研究，不在等待依赖时创建research run或预算预留。后续研究使用显式的实际研究冻结时刻（不早于诊断实际生成时间），并绑定日根原snapshot；不能用日根早先cutoff假装当时已有后来生成的诊断。原快照as_of、日根截止、研究冻结时刻分别保留。具体native Job关联与单次运行边界按既有admit/worker完善，不新建同名daily agent handler或第二个模型executor。

**Owner/Files:** root，A确认无冲突。新`jobs/daily_research.py`，扩`ops/daily_once.py`，`nexus/worker.py`仅单次Agent函数转发可选exact job_ids；专属测试与现有手册。无迁移。

- [x] `admit_daily_research(session, root_id, instrument_id, research_as_of, model_config_id, daily_limit_usd, now)`：校验日根及两子节点身份完整且诊断成功，诊断实际完成/报告创建不晚于显式研究时刻；指定model config复用既有模型/提示词/预算准入，不自动选择模型。CLI非fixture运行配置校验在下一片完成。
- [x] 在调用者事务内用savepoint整体完成绑定快照准备→原生admit→既有JobDependency连接诊断。请求hash/幂等键覆盖root、diagnostic job、instrument、research cutoff、model config；只有新准入Job能添加这条边，旧replay必须验证已具备相同边，不给历史作业补归属。任一步失败不遗留新snapshot/run/预算/Job。
- [x] native Job保留`agent_research_job_v1`和`run_id`原payload；日诊断的三节点selector不扩展为跨业务遍历。新精确resolver以root+run检查该依赖边、研究快照context绑定及原生Job身份后给单个ID。
- [x] `run_agent_worker_once(...,job_ids=None,preclaim_guard=None)`转发同一范围及已有schema guard至通用worker实际领取事务，默认不变。新`research-admit`只准入不运行；`research-run`显式调用此既有单次入口，仅接受已配置非fixture研究runtime、匹配模型及已显式配置对象库。最多一个Job，不重试/调度，不运行方案或paper。
- [x] 原模型正常预算取`settings.agent_daily_budget_usd`，不新增额度配置、不将订阅额度写成花费为零；固定安全状态/错误，不输出账户事实。
- [x] PG检查前置未完成/blocked零run零预算、成功准入恰好一个native job与已有诊断边、相同请求幂等、模型准入失败整体回滚、错root/run拒绝；单次实际worker隔离外部queued/expired job，旧非scoped调用回归。真实模型是否需要再调用由新证据与验收缺口决定，不能为测编排重复消耗旧case。

Task4b1准入验证：新增8项PG，与既有显式绑定、准备及组合上下文共30项PG通过；标准`PYTHONPATH=engine/src`与importlib。首次成功路径红例捕获request hash与native durable hash混淆，按原生冻结字段分别校验后修复，独立定向复核ready；额外真实native准入无依赖的replay回归确认不补写旧edge且原run/预算不变。Ruff/Mypy/diff-check通过。无模型调用、无迁移；后续CLI/worker与研究证据驱动方案仍未完成。

Task4b2入口验证：先红缺命令/参数，再新增两个显式命令、既有once的exact job_ids转发。37项新旧unit、26项相关PG通过；审查P2发现schema预检查不覆盖领取事务，补guard转发红例并修复，4项聚焦PG复验通过，含实际预检查成功→claim前拒绝→零新heartbeat/attempt、未来availability立即pending、其他queued/expired不动、模型不匹配零run/预算。两源Mypy/Ruff/diff-check及独立定向复核ready。真实PG通过原生worker（仅外部模型leaf替为不可用证据）验证failed/blocked不冒充完成；没有重复真实模型调用。本片交A后才做运行版本/数据库匹配验收，真实研究证据驱动方案仍是下一必要切片。

- 诊断成功后按明确 frozen portfolio context 准备研究快照，再用原生 `admit_research_run` 准入；不能提前创建会被依赖跳过且预算悬挂的 run。
- 原生研究完成后实际消费其合格证据构造方案；当前优化路径 `research=()` 的事实必须先修复，不能仅添 dependency 就称消费研究。
- 未知/不充分结论保持原安全语义。paper 仍单独显式入口；自动日程仍是独立产品选择，不在此片偷开。

## Task 5：研究输出真实消费（不能仅连接依赖）

**设计核对：** `optimizer.models` 已有合格ResearchIntent约束确定性selection的consumer，但要求研究data snapshot与selection相同、decision_time不晚于baseline valuation；`optimizer.authority`也要求data在旧估值时已可用。当前日根研究实际产生在诊断之后，不能改intent ID/抬高旧持仓as_of以塞入旧formal路径。现有v2仅watch/none/partial，不获得排名资格。分两条必要交付，前者不替代后者。

### Task5a：输出只读冻结与新草稿上下文

Owner=root，基线同步main `70c9e82`，独立分支`edward/nexus-daily-research-consumption`；A仍唯一运行/集成，B独占forward reader。无新表/依赖/调度。

- [x] 新窄`optimizer/research_context.py`：显式root/run/context_as_of，复用精确resolver；读取终态最终output，核原内容hash、run/instrument/data/decision time、原始证据/最终引用绑定；job.finished_at与output.created_at均不晚于实际context截止。输出保留root/run/output/sequence/hash、研究输入ID/时点、真实输出可用时刻和intent或固定不可用原因。
- [x] 严格类型区分`context_only`，无执行/排名许可。v2语义保护复用已有函数；failed/abstained/partial不升级。不读取latest研究替换已冻结ID；相同截止和ID得到相同内容。
- [x] 在已有`create_initial_draft`中可选接入显式冻结context，写现有DataSnapshot JSONB的新输入schema版本并参与digest；不重写旧v1输入/Job。原valuation保持t0，research/context cutoff为t1，不能把晚生成context放入要求早于valuation的comparison evidence。
- [ ] 复用现有草稿同步入口新增显式本地调用，草稿目标仍由已有确定性诊断产生；研究作为可读依据及缺口说明，targets/partial/selection/execution flags保持原样。API投影及实际页面读取需验证同一冻结context，不得仅存入不可见JSON宣称交付。
- [ ] TDD覆盖错root/run、输出/证据hash不符、晚生成、无intent、v2 partial、冻结后新增输出不改变旧引用、相同请求幂等；真实PG→构造器→API可读且原targets及门禁不变。reader、构造/入口、消费者可分小提交，每片<800行。

5a-reader验证：先红缺reader；6项新PG覆盖原生worker实际输出的completed partial/failed（含v2配置）、新追加坏hash/缺输出引用拒绝及原截止内容不变。与原显式快照/准入/单次入口共23项PG通过；显式诊断ID与run/job终态对应收紧后4项终态聚焦PG复验通过，Ruff/Mypy/diff-check及独立审查ready。测试修正共享PG fixture固定created_at导致的“晚追加”假设，仅给新测试行显式实际时间，不改生产时间行为。此片纯只读，无迁移/模型外呼；持久化草稿/读API/页面尚待下一片，不能称方案已消费研究。

5a-backend：可选请求只接收root/run/UTC截止，服务端冻结可信输出后写`optimization_draft_inputs_v2`全文身份，GET读取原保存值并验证schema/hash/内外ID/时序；无context旧v1不变。15项unit通过；新旧草稿12项PG通过，补充错root/run/未来或早截止零草稿写入、晚追加坏output不改变旧GET、旧formal入口拒绝v2且Job数不变及原read/symbol路径8项PG通过。Ruff/Mypy4及独立集中审查无P1/P2。合成诊断叶子测试不代表真实源端到端验收；本地组合入口、页面及同一真实案例尚待后片。

5a-page：复用现有方案页增加冻结研究依据，分开原估值/研究截止/输出实际可用/引用截止，原生展开显示原论点/未知项/证据定位及版本，无最新查询或模型调用。BFF保持微秒时序与intent时间身份、组合/诊断身份及不可执行边界；23 BFF、6新组件和21旧组件测试通过。独立375/1440浏览器测试通过，含键盘展开、长hash、无外写及局部严重可访问性检查；发现旧comparison/actions隐式grid min-content溢出，以两处minmax修复，不裁剪内容。该浏览器案例是合成响应；真实数据库/BFF/DOM同案例仍待A成组加载验收。

5a-local：`daily_once proposal-build` 只接root/run/UTC引用截止/稳定请求键，由精确研究上下文取得原baseline/diagnostic并复用原同步草稿构造器。显式本地数据库/五项归档配置、schema就绪校验，无Job/采集/模型；只输出安全ID及context-only执行锁。25项ops单测、Mypy2及新9项PG通过，包含原生研究→单次草稿→存储重读幂等且Job数不变。新增v3综合论点/反证审查的全部引用绑定，两个伪造摘要PG先红后绿，另有合法结构化正文通过。独立集中审查ready；不启用定时任务，也不宣称正式优化准入完成。

### Task5b：正式研究约束目标（仍待实现与条件）

- [ ] 研究输出存在之后的新真实baseline/diagnostic/selection，必须按现有source/quality/cost/风险条件冻结，并显式绑定已验证研究数据身份；不能复制旧持仓抬as_of、改intent或放宽原gate。
- [ ] 新准入请求/持久化身份需要包含准确run/output/hash/实际可用时刻；复用现有consumer，修复正常formal Job硬编码`research=()`，不默认选择当前latest或给旧作业补研究。具体版本兼容按5a后现有路径细化。
- [ ] 新完整证据且真正内容合格时才验证研究对确定性target的限制；v2 partial与来源不足仍不能排名。不把只读context完成等同这一条完成。

## 验收记录

- 设计检查：旧 root 纽约 18:00 截止与手动晚到采集冲突；旧阶段与原生 agent/optimizer 同名 schema 冲突；通用 once 现会回收整个队列。三点分别由新手动根、原生任务后置准入、精确 claim/reclaim 范围解决，不扩展为多 schema 框架。
