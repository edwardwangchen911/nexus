# 每日研究与条件触发增量研究实施及验收计划

> 实施者使用 `superpowers:executing-plans` 按切片执行；当前主线是唯一写入者。

**Goal：** 每日完整研究产生逐股立场及可追溯失效条件，盘中每五分钟由确定性代码检查；无触发保持安静，触发去重通知并只研究受影响标的，新的研究与监控版本安全接替旧版本。

**Architecture：** 复用现有研究冻结、Job、预算、租约、outbox 和正常宿主。新增明确的监控计划、检查事实及触发到增量研究的关联；不把普通条件字符串或旧自然研究槽当成已实现监控。

**Tech Stack：** Python 3.12、Pydantic、SQLAlchemy/PostgreSQL、现有 Worker/Bark、vinext BFF/Web。

**Spec：** 用户 2026-10-24 在协调会话明确提出的每日研究与五分钟触发链；既有 `docs/design/nexus-portfolio-management-product-contract-v0.1.md` 的权限、目标与执行约束继续有效。独立 Chat 设计评审 `nexus-daily-monitor-design-20261024-01` 已完整取回，结论有条件通过本地实施；下列三项 P1、两项 P2 已纳入合同，仍须代码与回归证明，评审不证明运行生效。

## 全局约束与验收目标修正

- `execution_locked=true`；正式时效、价格、预算、风控、审计否决和未知占用保护不变。
- 研究完成不以订单存在为条件。`hold` 和内容校验通过的 `complete/watch` 可正常完成研究；`partial` 保留缺口。`watch` 的组合动作仍只能 `defer`，不解释为持有或交易许可。
- 分别报告研究质量、立场、监控覆盖、执行许可，不能由其中一项推断另外三项。
- 无触发必须零模型、零业务通知；缺失、过期、未来、来源不明的数据为 `unknown`，不是未触发。
- 旧 Run/Output/快照不可追改；旧失败和未知请求不重放，不能换键试探。新的模型研究也不能重复探测同一缺失证据。
- 通知写入、Bark 接受、设备实际收到是不同证据层。发送沿用现有获准目的地；不新增外部渠道或敏感载荷。
- 持久化 Goal 由原生接口核对。2026-10-24 本轮最新 `get_goal` 返回 `null`，随后按用户授权创建完整新 Goal，回执 `active`。此前 blocked 陈述已被本次回执取代，不编辑应用内部数据库。

## 已核验的现状（main d449fdef）

| 环节 | 实际代码/落盘事实 | 缺口或可复用部分 |
|---|---|---|
| 条件生产 | `agents/synthesis.py` 固定三个 `committee.*` 条件和七天有效期；`strategy/intents.py` 仅校验信号格式、单位及布尔/数值对应 | 没有由论点生成并校验的具体可执行价格/事件条件；不能将通用占位符注册成实测信号 |
| 持久化与展示 | ResearchIntent 随 Output 冻结，research API、BFF、页面展示原条件 | 可复用原件链；缺独立监控版本及覆盖事实 |
| 五分钟消费 | `choose_loop_action` 在每日自然研究槽五分钟窗口以外等待下个交易时段 | 这是日槽准入，不是全日条件扫描；心跳也不等于扫描 |
| 通知 | 现有 Paper producer 事务性冻结 outbox，Bark worker 仅登记五类 Paper 事件 | 缺研究触发生产者及业务唯一键；generic outbox 是至少一次投递，不提供业务恰好一次 |
| 单股 Job | `admit_research_run` 有 instrument scope、精确幂等、冻结输入、预算与租约；SDK 一次尝试 | 无 parent thesis/trigger evidence 关联；`version_source_run_id` 仅继承 prompt/model，不能替代父研究 |
| 正式入口 | `admit_agent_paper_research` 强绑定 cycle，formal 禁止自定义幂等和版本来源 | 不能把触发塞进旧 cycle 或通过通用 admission 绕过正式保护 |
| 收尾 | 已知用量结算、unknown 保留预算、worker fence、旧未知接受围栏可复用 | 新增量结果的版本接替、partial/unknown 保留范围、待处理证据收敛尚未实现 |

本轮冻结三股复核：AAPL、NVDA 为 `complete/watch`、审计 complete、复核 sufficient，当前 `actionable_intent_shape_valid` 为真；AVGO 为 `partial/watch`、审计 partial、复核 limited，仅符合安全暂缓形状。三股条件都是上述通用占位符，不能称监控已覆盖。旧文件逐字节保持不变；本复核没有模型或数据库调用。

## 评审后最小行为合同

1. 每日研究沿用现有权威交易日历和正式日槽，不因启动或重启补跑旧槽。未来研究输出版本化监控计划，精确绑定证券、Run/Output、快照和支持引用；旧输出继续读取并显示未覆盖，不能猜测自然语言或回填。
2. 注册信号限定单位、币种、来源、观察时点、可用时点及最大陈旧度。第一条完整链覆盖具体价格阈值和已可靠分类的结构事件；无法可靠分类的事件明确未覆盖。有效期届满为复核到期，不自动推翻观点或清仓。
3. 同一正常宿主在合法交易时段执行五分钟检查；检查持久化输入，资料生产仍用正常入口。事件时间与取得时间分别记录。条件持续为真只形成同一 episode；不能把每个五分钟 slot 当新触发身份。
4. 一次事务保存检查、触发以及 outbox 关系；研究准入失败仍留下触发与拒绝原因。通知失败只重试投递，不重新创建研究。相同证券并发条件合并，已有工作时聚合后续证据，未知占用时保持等待保护。
5. 新增量通过专用正常入口复用冻结、预算、Job 与 worker；输入含父论点、原审计和新事件证据，不是重新跑三股。成功结果追加历史并核验后接替监控版本；旧 worker 无权替代新版本。partial、unknown、失效和到期分别处理，不能默认延用不再有效的旧条件。

### 必须落实的评审修正

- P1 条件编译：新版本研究提出条件，反方/审计明确复核，确定性代码闭集接纳。价格来源、口径、单位/币种、用途、开始/复核/终止时点均绑定；语法合法不等于审计通过。首版仅具体价格复核条件，不由比较器推导经营恶化或卖出。
- P1 跨版本 episode：去重不能仅含 monitor_version。已处理触发/证据截止随新版本接管，持续越界、unknown 间隔及版本更新不重开 episode；只有合格 false 后再 true 才再触发。首次 true 明确为初次满足而不是已观察穿越。日研究只处理实际冻结覆盖的触发，不能吞掉截止之后的新事件。
- P1 原子边界：检查＋episode＋唯一 outbox＋研究请求状态持久化，Job/预算准入失败不回滚触发。使用 savepoint 或分离正常准入事务，不生成无预算可执行 Job；迟到结果可保留但不能接管非预期 generation。partial 后仅仍有效且依赖未失效的旧条件可继续，不自动续期。
- P2 时间：复核到期、monitor 到期、条件命中、观点失效和提案过期分开；每日研究已覆盖的复核到期不再生增量。漏 tick 明确遗漏，不用当前报价填造历史检查。
- P2 持续检查：tick 独立 DB session、有界读取，不能等待完整研究模型结束；不额外提高模型并发。单一宿主可以同时管理确定性 tick 与在途工作，不额外启动第二宿主。

现有 SDK 配置价格为零，USD 日预算与每 Job token/turn 上限不能单独限制全天反复触发的累计调用。需在上线自动增量前落实有限的会话/日总量并保障每日完整研究；先实现显式配置与耗尽回归，不能默认无限或把未确认额度用于 live 调用。既有用户已授权套餐内合理管理调用次数；是否需额外选择在具体方案、现有可复用限制核验后一次说明，不阻止离线实现。

## 实施切片与测试

**首个可交付版本必须是一种价格条件贯穿以下 1—3 的完整链，并完成 4 的受控正常入口/页面验收。下面按代码责任分组，不把 DTO、比较器或某一层作为已交付切片。结构事件在可靠 producer 未证实前显式 coverage gap，不阻塞价格链开发，也不宣称全部覆盖。**

### 1. 从真实研究输出到可读取监控计划

涉及 `agents/codex_staged.py`、版本化提示、`agents/committee.py`、`agents/synthesis.py`、独立 monitoring contracts、持久化模型/迁移、research API、BFF 与实际研究页。

- [x] 先红后绿：新输出绑定具体注册条件、来源/单位/时间及引用；错误来源、币种、非法阈值、未来引用拒绝。旧输出/hash 精确兼容且不可自动升级。仅完成生产与编译，不代表监控已激活。
- [ ] 生产者正常研究完成后保存计划；`hold`、完整 `watch`、`partial` 的质量与覆盖独立，旧通用条件显示无法执行。
- [ ] 正常 API/BFF/实际页面读回同一计划，不触发研究或采集，不改变十秒截止。
- [ ] 迁移保留 0086 采纳链读取；明确有新计划/引用后回滚的版本限制，不回滚业务历史。

### 2. 计划到五分钟检查、触发和去重通知

涉及 normal host planner/executor、monitoring persistence/service、新事务触发 producer、outbox registry、notifications worker/Bark 安全链接及只读页面。

- [ ] 真实隔离 PG：正常入口加载上述计划；无触发多 tick 零 Job、零 outbox；stale/unknown 不当 false。
- [ ] 跨阈值、持续同侧、回落后重越界、初次已越界、并发、重启、日切及休市案例。精确 observation/episode 身份去重，稳定通知 ID，出站确认丢失不重复业务研究。
- [ ] 可靠结构事件实际 producer → 原件 → 检查器，含迟到、修订、重复及不可靠分类拒绝；不使用手工布尔冒充上游已可靠分类。

### 3. 触发到单股增量及安全接替

涉及专用 monitoring admission、`agents/persistence.py`、输入投影/worker、预算/有效租约、监控版本关联。

- [ ] 正常入口生成一个受影响标的 Job；冻结 parent 论点和新证据，另两股不新增 Run；准确复用正式账户与未知占用保护。
- [x] 同股多触发有界合并；预算不足/未知/有效租约时保留触发等待事实。通知重投不重复 admission；未知调用不重放。此项为隔离正常入口验证，尚非部署或自然运行。
- [ ] 新结果落盘、审计、已知用量结算和版本接替原子一致；迟到旧结果不能恢复旧计划；partial/失败/unknown 收尾和下一轮正常准入分别验证。

### 4. 完整周末受控验收及交付

- [ ] 正常 CLI/API → 多角色 producer → PG → 五分钟 consumer → outbox → 单股 Job → 新结果 → BFF/实际页面，全链同一身份核对；替身模型的证据明确标为受控测试。
- [ ] 完整矩阵：hold、complete/watch、partial、无触发、价格/事件、重复/并发/重启、stale/unknown、休市/日切、预算不足/未知占用、迟到写入、下一轮准入；订单数量不是成功判据。
- [ ] 独立终审，相关本地验证，精确最终提交全部必需 CI，最终镜像与安全部署，版本/健康/页面回读与兼容回滚。
- [ ] 自然交易时段独立观察：正常每日研究、实际五分钟输入、自然触发及单股增量（没有触发便安静，不能强造事件或重放）。周末受控通过不得冒称自然运行已通过。

## 本轮证据与剩余工作

- 已通过 458 项现有通知、宿主、研究租约/停止、审计和组合语义单测（9.97 秒），只证明基线。
- 已通过 41 项真实隔离 PostgreSQL 基线（28.39 秒）：事务 outbox/重投、Bark worker、Job 租约、研究冻结/预算/幂等和 daily recovery。只运行新建独立测试容器及随机测试库，没有访问业务库；仍不证明新增监控链。
- 私有冻结原件及 hash 复核保存于 writer `.secrets/daily-research-monitoring/frozen-baseline.json`；基线日志同目录。无个人账户事实进入本计划。
- 只读 Docker 状态显示既有 Bark 通知 worker/server 处于退出状态。产品接线完成后还须核对现有获准通知目的地及正常服务恢复；本回合没有启动它们，不能把源码投递测试当成当前用户已能收到通知。
- 新链正在本地实施，尚未跑 GitHub CI、未发布。设计评审已取回；按以上完整价格链实施，完成后携实际 diff 定向终审，不重复设计派发。

### 2026-10-24 本地实现检查点

- 新 v8 由 market_state 提出闭集 Schwab L1 双边中间价复核条件，原始 bear/evidence_audit 审核 proposal hash，portfolio 只可精确采纳；审计否决保留。旧 v7 六组 wire hash、原 schema hash 和请求重建继续锁定。
- 正常 `setup --price-monitoring` / `prepare_database(price_monitoring=True)` 为显式入口，只接受完整 formal bounded 配置；默认版本不变。已用隔离数据库正常登记→正式三股准入→真实 Worker→三阶段模型替身→Output/Intent/用量结算验证，没有替换准入或输出持久化。
- 新 0087 保存计划、检查、跨版本语义 episode 和触发/outbox；当前仅是受控服务原语。相同 tick 并发只保存一笔；持续 true、unknown 间隔和相同条件新版本不重开；false 后 true 才重开。预算拒绝在单独事务保留触发和审计，unknown 终态不能重开，迟到同 tick 返回原件。尚未从已完成研究自动安装计划。
- 当前 DB loader 只消费已落盘 Schwab quote，不取 trade 代替。独立审查发现原 LIMIT 2 会漏掉同刻第三条冲突，已由三条 A/A/B 报价回归先红后绿修复为 EXISTS 查全同刻异价；冲突为 unknown。
- 容量预检原先没有计入新增监控上下文，已补合法最大提案及两份审核的保守上界，超限零 SDK 调用拒绝。原 100479 字节上限不变。纯合成近边界案例证明 v8 完整上下文仍可能超界；不是已测真实 AVGO，也不能以预检修好宣称 v8 已具备实际容量。后续需要有据压缩或报告该输入受限。
- 1331 项 Agent 单测通过、6 项原有跳过；最终 33 项集中回归通过（真实隔离 PG/迁移/正常 v8 三股 Worker/旧 formal v9 路径/注册参数），62 项前序聚焦验证通过。Ruff、13 源文件 Mypy、diff 检查通过；证据在 `.secrets/daily-research-monitoring/`，均非自然市场验收。
- 空 0087 升降级往返通过；有任何监控历史时拒绝降回 0086。0086 文件未改。部署前仍须准备保留 0087 的兼容回滚版本，不能在有历史后删除新表或拿只认识 0086 的旧程序冒充可用回滚。
- 下一步仍是完整价格链：严验原研究/账户/快照后安装与 CAS 接替计划；同一宿主独立五分钟 tick；outbox 登记及正常通知；冻结 parent 与触发证据的单股 admission、有限日总量与预算/unknown/租约保护；BFF/实际页及完整周末矩阵。未启动业务宿主/通知服务，无真实模型、数据源调用，无旧请求重放、未知接受或原结果变更。


### 2026-10-24 14:05 北京时间：正常计划安装、同宿主检查与通知消费

- 新受保护 `nexus-engine-research-monitor install/inspect/scan` 入口只使用既有配置账户和数据库时钟，不接受调用者覆盖账户或检查时间。安装核对已结算原 Run、正式冻结身份、原八角色与双审 hash，逐字段重建终态 intent；原审计否决拒绝安装，完整 watch 可安装而正式执行仍 defer。已由正常三股登记→正式准入→Worker→原输出→安装/读回/扫描的隔离 PG 案例核验。
- 计划追加并使用前代精确比较、递增版本和源研究时点防止迟到覆盖；同源并发只安装一份，旧回执不重激活。A→B→A 只消费新研究原冻结报价已覆盖的状态，更晚检查不被吞掉。扫描与安装使用同一证券锁，扫描拿锁后重读版本，尚未生效时不写旧代检查；真实并发回归通过。
- 原正常宿主在心跳后启动独立五分钟当前桶检查，8 秒总上限，不阻塞模型/心跳、不重叠、不补历史桶；只读取已持久化行情，不发起外部采集/研究。每次扫描重读受保护配置，停用及账户/日历/数据库变化拒绝进入；正常退出和取消收拢自身任务。休市零检查，陈旧/冲突行情 unknown，未知不解锁 episode。
- 正常 outbox registry/通知 worker 消费固定七字段事件，稳定 trigger 通知身份与既有受保护研究链接；只描述“触发时研究待准入”，不宣称增量已执行。没有启动 Bark 或发送实际通知。
- 独立代码审查发现扫描失败只留内存的 P2，已先红后绿补成每个完成失败桶一条固定安全码/启动桶记录，退出也收取；忙时跳过新桶不能改写失败身份。定向复核已收口，无新增阻塞；不把扫描失败改成研究 unknown，不重试。
- 本切片聚焦 329 项宿主/安装/协调器单测、21 项不重复 PG/迁移/原件/安装/扫描竞态/通知案例通过；Ruff、10 个源文件 Mypy、diff 检查通过。旧完整 Agent 单测结果继续有效；没有声称 GitHub CI 完成或自然运行通过。
- 仍未交付：每日正常研究自动安装及 partial 覆盖状态、冻结父论点和新触发证据的单股增量准入、有限日总调用额、与未知预算/租约联动的收尾接替、BFF/实际页、容量压缩及完整矩阵。第一条价格链完成、终审及最终提交必需 CI 通过后才合入部署；保留完整目标，不将当前原语与受控接线当成成品。

- 额外全量 `full --collect-only` 发现六个既有集成测试的同目录短名 import 无法在 importlib 模式收集；这些文件与 base d449fdef 无差异。本切片未改其断言或过滤它们，作为最终 CI/全量回归阶段待修项记录；必需分组新增选择器另行核验，不能声称全量已通过。

### 2026-10-24 14:42 北京时间：单股增量准入、Worker 与收尾

- 新正常 `admit/finish` 入口从正式应用配置解析账户、批次、模型、预算及日历，停用时拒绝支出；固定数据库时间，不接收调用者的时间或账户覆盖。原触发、check/outbox 在准入失败时保留，只回滚失败准入写入。
- 0088 追加不可改的增量成员、原生 Run/Job、快照与调用预留身份；0089 只为同账户/证券/计划/父输出/原触发绑定的专用证据增加数据库归属分支。新快照冻结完整父论点、原触发报价、本次新鲜报价、当时账户投影及正式候选，原结果不追改。父八角色版本通过原冻结清单加载，不把独立版本误当同版本提示包。
- 正常 Worker 对明确增量 purpose 使用专用 authority/capacity 与单股上下文，沿原三阶段/审计合同输出；不会冒充普通正式周期或直接提供组合执行许可。正式消费者明确拒绝增量身份；complete/watch 的正式执行限制不变。
- 每笔预留 3 次 SDK 调用，默认每日每账户 9 次，同日不超过已冻结上限，美元费用为零仍有限额；unknown/失败不返还。手动、定时与增量共用账户占用保护，有效租约只在原生完整消费证明匹配时视为已结束。
- `finish` 只登记原 Run/Job 的退出与用量事实，已知结算完成、未知继续占用，终态不可改；重复回执返回原实际状态。两会话均预读 pending 的回归先 RED（旧缓存误报 busy），取账户锁后刷新修复 GREEN，同一 trigger 只出现一份 Run/Job 和 3 次预留。
- 隔离正常三股父研究→单股准入→Worker→收尾回归覆盖完整已知三阶段与第二阶段超时 unknown；另两股不增加研究，重复消费不调用模型。覆盖零美元有限调用额、原 outbox 保留、错误证券/账户/触发证据拒绝、unknown 终态迟到修改拒绝，以及有新证据时禁止降级。模拟调用明确使用替身，不是本机真实模型验收。
- 独立定向审查确认配置绑定、统一锁顺序、0089 归属、父提示版本及收尾保护；剩余并发旧缓存 P2 已按上述回归修正。159 监控单测及 77 相邻准入/预算/选择器单测通过，21 项相邻监控 PG/迁移通过，15 源文件 Mypy、Ruff 和差异检查通过；新增增量及正常准入 PG 最终 51 项通过（191.45 秒），日志为 `increment-final-admission-pg.log`；本轮不重复合计 72 项 PG。pg-b 350 项收集通过，GitHub CI 尚未运行。
- 本切片尚非整项交付。下一步：每日研究自动安装与 partial 覆盖、同宿主自动增量派发、有界待处理触发合并、新结果审核及监控代际接替，然后 BFF/实际页、容量压缩、完整矩阵、终审与最终 CI/部署。无真实模型/行情/通知调用，无业务宿主启动、未知接受或保护解除。新 schema 尚未部署；有监控历史后回滚必须保留 0087–0089 及读取支持，不得降级删除历史。


### 2026-10-24：自动登记、partial 覆盖及安全版本接替

- 新 `load_qualified_source` 只验证监控原件，不授予正式执行权限。日研究保留原正式资格证明；增量使用自身冻结 manifest/context，统一重验原输出/hash/证据绑定/八角色阶段轨迹/完整 intent 重建、预算结算和原生 Job 租约消费证明。`load_approved_source` 再要求原 proposal、bear/evidence_audit 两审核和 portfolio 精确采纳。
- 增量来源只允许冻结的 `plan_id` 作为预期前代，当前 CAS 与源时点仍检查；完整与独立合格的 partial 条件均可安装，partial 原质量保留。旧正式消费仍明确拒绝增量身份，watch 执行继续 defer。既有跨版本 episode、A→B→A 和冻结后检查保护复用。
- 新 0090 不可改覆盖回执区分 installed/retained/gap/unknown/superseded/unverified。未验证回执只记录原件验证失败，不声称原件已合格或授予任何权限。正常 `reconcile` 和宿主 `scan` 扫描前自动处理当前候选批次的终态；单股失败独立 savepoint，阻断该股旧覆盖，其他股票继续检查。
- 原条件保留同时要求：此前仍有效覆盖该计划、未到期、除上下文外全部冻结事实精确未变化、无新审核否决。已撤覆盖不能被后来的 failed/unknown 复活；恢复必须新批准条件。原件修订/新证据不由代码推断为无害，条件不足明示 gap。
- 审查三问题已 RED→GREEN：撤覆盖复活、单股异常使全账户停止、历史范围误算当前五股。另补新完成结果尚未登记时阻止旧触发继续准入，防绕过潜在新否决。只读 inspect 绑定覆盖事实，分列质量与终态；没有计划的当前证券计入覆盖缺口，不冒充无触发。
- 38 项不重复隔离 PG 通过：最终新链 10 项（99.70 秒）+相邻 28 项（113.11 秒）；177 单测、15 源 Mypy、Ruff/diff 通过，pg-b 365 项收集通过。新链含正常三股自动安装/审核否决/单股坏原件/换候选范围、complete/partial/否决/未采纳/unknown、先前撤覆盖后 unknown、原件不可改与禁止降级、幂等读回与同条件不重复事件。全部模型为替身，无自然模型或业务服务变更。
- 当前切片已闭合但完整 Goal 未完成：同宿主自动增量派发、有界 pending 合并和冻结 cutoff 消费；BFF/实际页面；容量压缩；hold 与可靠事件等完整目标矩阵；终审、精确最终 CI、镜像与兼容回滚、安全部署；自然交易时段观察仍待完成。新 schema 未部署，不以阶段验证替代最终业务验收。


## 2026-10-24 自动增量派发切片（接续 23f99921）

- 同一宿主在既有前台队列消费原 trigger→Run→Job，后台五分钟扫描和心跳保持独立。当前阶段未运行业务宿主、真实模型、资料或通知传输。
- Ruling: 新增量只在正常等待期间启动；优先准备/每日/手动/到期评估，并保留 195+315+90 秒日研究余量。复用 Worker 原有限时常量，不延长 deadline。prepared 事实随本次前台 action 传递，不能仅因 reinspection 未携内存事实漏掉已准备后的等待期。
- Ruling: 未启动研究的资格拒绝按五分钟桶登记一次只读资格复核；已派发原 Job 先追加不可变审计，认领前中断、未认领和未知不自动重调。正常重启只消费尚无派发事实、未尝试的原 Job，不换 Run/key。
- 审查指出的退出确认已补原增量绑定、native lease/attempt退出、已知预算/用量和覆盖回执的只读验证。已知收尾可确认退出，未知仍不确认；没有放宽普通周期或正式执行门禁。
- 增量容量明确超界的已知零模型调用收口独立补齐；必须原 identity、原生取消证明、完整审计，普通失败/未知不可走此路径。三次调用 hold 不返还。
- 正常宿主执行入口→隔离 PostgreSQL→原 Worker→finish/reconcile 已覆盖已知结算及第二阶段 unknown；其余两股无新增 Run。另覆盖 preclaim中断/未认领、资格限次和日研究窗口优先，模型为替身。
- 尚未完成有界 pending 合并、可靠事件 producer、BFF/实际页面与完整验收矩阵、Chat 终审及精确最终 CI/镜像/部署。自然交易窗口观察独立待验。当前切片不冒充完整目标。

- 最后定向审查修正宿主配置重载身份：启动数据库及首次正式账户/日历/批次/模型固定，先比对再替换闭包配置。正常 `run_local` 两次 inspect 与 heartbeat 回归覆盖五种范围变化，旧无正式模型宿主保留原路由。
- 最终本地验证：26 PG（6 正常派发 + 8 确定拒绝 + 2 原增量 + 10 覆盖接替）通过，419 相关 unit/policy 通过，Mypy 19 源、Ruff/format/diff 通过；pg-b 379 收集通过。证据 `dispatch-final-path.log`、`dispatch-unit-final.log`、`dispatch-mypy-final.log`、`dispatch-review.md`。不是 GitHub CI，也未部署。


### 2026-10-24 同股冻结集合与收尾检查点

- 准入现在读取整股全部 pending/blocked，最多32条一起冻结；33条明确拒绝，不截断历史、不提前创建研究。并发从不同成员进入只返回一个主触发/Run/Job和一份3次调用预留。
- 原始检查、报价和outbox不变；完整冻结成员由不可变上下文hash绑定，收尾按同一native结果统一更新，迟到成员保持pending。正常扫描已用隔离合成行情实测false→true产生第二episode，再由正常入口合并；原Worker三阶段已知/未知及容量明确拒绝整组收尾均有回归。
- 同条件旧触发只沿每代连续同条件前代链进入当前有效父论点。正常Worker接替后的late触发可绑定新parent，变更条件或A→B→A链不能冒充同一条件延续。每日较新研究尚不等于已消费旧触发；精确覆盖衔接仍是下一独立事项。
- 0091追加数据库提交时完整组约束及读取支持：拒绝额外/漏掉成员、不同scope、状态分裂、同冻结快照第二Run；不改0086–0090。空库可以回滚，多成员或跨plan历史不能降至0090，后续发布须准备兼容0091的回滚版本。
- 每股每五分钟桶最多一次未建研究的资格复核，新到成员不能绕过抑制；其他证券仍可轮转。原账户、有效租约、预算、未知用量、审计否决和watch只能defer均不放宽。
- 定向独立审查已修复主触发回执、逐跳条件一致及重复Run三处问题；最后复核无剩余确定P1/P2。数据库“缺成员”负例补回故障producer注入，保留原拒绝断言，正常入口不替换绑定验证。
- 最终50项隔离PG通过（48项核心组448.49秒、正常扫描1项、迁移往返1项）；492项相关unit/policy通过，Mypy17、Ruff/format/diff通过，pg-b402项收集通过。最终验证证据在writer `.secrets/daily-research-monitoring/pending-*.log` 与 `pending-review.md`。GitHub CI、发布、实际BFF/页面和自然运行仍未完成；本轮没有业务模型、资料、通知调用，没有启动业务宿主或解除未知保护。

### 2026-10-24 每日研究精确消费冻结触发（本地候选）

- v9 显式监控提示把截止点可见的 pending 原件嵌入每日正常研究；原父论点、检查与观察均随快照冻结。晚到触发在集合之外保留，不阻断旧可见项，也不追加旧快照。默认 v7 与已有 v8 wire/hash 保持。
- 原 orchestrator 逐项 addressed/unresolved，bear 与 evidence_audit 分别审核原提案 hash，portfolio 精确采纳；已知原生 Job 结算后，正常 reconcile 重新核验原件及三个阶段，只追加已处理项的消费回执。审核否决、未解决和unknown均不登记覆盖。
- 0092 独立约束原八角色审核/采纳、正常周期/Job及已知结算、回执不可变和increment归属双向互斥；正常队列排除已覆盖项，旧选择 admit/dispatch 仅返回covered与原每日Run/Output。旧触发、通知、快照及未知预算不变。已有此类冻结快照或回执后须保留0092及读取支持。
- 修正正常第二轮读取正式目标漏传formal_stage_capacity；未放宽风险或结果资格。集中只读复核发现每日截止竞态，已有先红后绿回归并定向关闭。
- 20项每日/DB/迁移组合及随后加强的7项正常Worker回归通过（二者覆盖重叠）；141项相关unit、43项Web兼容回归、Mypy10源、Ruff/format/diff、生成客户端检查通过；pg-b收集421项。都是合成事实/模型替身的隔离正常入口证据，未调用真实模型、行情、通知或业务宿主。最终fast为6589 passed/10 skipped/1 deselected（234.75s），邻域PG 58 passed（558.47s）；与前述组合共78项不重叠PG，7项增强用例属于复验不重复计数。
- 本段不勾选完整产品闭环：可靠结构事件、BFF/实际页面完整矩阵、终审、精确最终head CI、镜像/部署/回滚与自然交易窗口仍待后续。

### 2026-10-24 可靠 SEC 索引来源（本地候选，事件派发尚未接线）

- 原 refresh-disclosure 每类只选最新一份，普通8-K reader不重读原件，XBRL collector也不是完整披露feed。新增窄来源复用既有collect_identity_sources的固定directory/submissions采集与原receipt，再通过正常collect/import/inspect保存、重验完整索引。
- 原件、receipt、既有发行人基础身份、canonical hash及实际本地入库时点逐层绑定；归档验证结束后重取数据库时钟登记可见时间。两种固定receipt精确接收，不改写原receipt。同件导入保持首次本地可见时间；同字节新HTTP收件产生新capture，逻辑event ID始终为CIK+accession。
- 只描述新增收录的8-K/8-K/A，不解释正文、重要性或会计适用性。首次baseline不触发全部历史；未来baseline拒绝。未读取历史分片时无新增保持unknown；明确新项可true，同时保留partial覆盖。stale/conflict仍unknown，来源原件全部保留。超过32项的事实/容量分离见下方最新修正。
- 真实公开归档证实AAPL recent有1002行，输入界限按事实调整为10000行/8MiB、目标类别1000项，均不截断。AAPL/AVGO/NVDA原bytes及原receipt零网络解析通过；不冒称真实数据库、模型、通知或当日采集验收。
- 正常collect到隔离PG再只读inspect覆盖重收、新capture、明确新增、冲突、partial历史、篡改、同哈希旧对象损坏、未来及过期门禁共8项；连同SEC exhibit/amendment相关PG共18项通过。94项相关unit、Mypy3源、Ruff/format/diff及正式注册CLI帮助入口通过；最终日志保存在sec-index-*。
- 集中复核两项P2均先红后绿：partial history误报false、全局同哈希旧URI未读回。未改变旧来源、价格条件、预算、租约或正式门禁。无新增依赖或迁移；旧代码回滚会忽略而不删除新来源快照。新测试已入默认CI组，远端CI尚未运行。
- 下一步仍须接有类型的事件条件、同槽多事件与价格互不吞没的存储合同、稳定事件outbox去重及单股冻结接替；现有价格表/observation/latch不能冒充SEC事件链。随后继续BFF/实际页面及完整交付，不将本来源切片当作全目标完成。


### 2026-10-24 SEC 双类型身份与价格旧版兼容（本地候选，完整事件链继续）

已返回的独立产品评审通过固定 `price` / `disclosure` 两类方案，并要求以下合同：

- SEC 信号仅证明相对于研究时点冻结 baseline 的本地新增收录，不证明刚发布、重要性或财务影响。网络新鲜度依据原 HTTP receipt，不能使用新导入时间；T0 冻结至 T2 激活之间的 T1 事件不可吞没。baseline、事件登记、核验后的研究消费是三个独立事实。
- 索引事件可以通知；判断影响前必须通过正常入口绑定精确 accession 的正文及必要附件。缺正文是明确 evidence gap，不能重复模型探测，也不能靠 addressed 标志清除。
- 同股一次 tick 先完成价格/事件评估并封口，再统一准入。同一封口批次的价格与两份披露应为三触发、一份聚合通知、一股一 Job / 三次 SDK 预留；冻结后 late 不追加原工作。消费与各 kind 接替须在归属、成员、前驱、审计、已知结算均通过后同短事务提交。
- 每份事件保留底层事实，通知按每股批次聚合，正文有界并链接本地详情。超过32项明确阻塞自动增量容量，保留全部事实；不能截断、改真值、重复通知或默认拆分新增模型调用，也不能永久阻断正常每日研究。
- 已核对 default9 只计增量 SDK 预留，未包含每日三股九次调用；共享美元预算、账户与未知占用保护仍同时生效。不提高限额、不扩大权限。

本片实施 `0093_monitor_kinds`：旧行默认 price；计划代次/同输出/coverage 按 kind 唯一，check 含 kind 与 event_key，SEC trigger 按账户+股票+CIK+accession 跨版本唯一。SEC 没有价格 episode；price episode 的非空/正数要求显式保留。基线引用和截止按 kind 校验；disclosure 截止必须等于源研究冻结时点，引用的 SEC 索引来源/证券和四个本地时间必须在截止前；前驱、check→plan、trigger→check、coverage→plan 以复合外键约束同账户、股票及 kind。

旧扫描、安装、准入、消费、接替、CLI 和研究详情仍严格处理 price。旧 v1 context/hash/shape 不增加缺省 kind；新迁移精确修改旧 DB 函数接缝，结构漂移拒绝迁移，不改已应用的0091/0092。共享预算/unknown守卫不按kind过滤。价格旧版不得接收 SEC，即使 SEC 行携带价格形状的 JSON。

正常 SEC collect/import/inspect 现将超过32项已证实新增返回 true 和完整事件集合，另列 `automatic_review_capacity=blocked`、`impact_review=not_performed`；容量不是资料真值。尚未启用 SEC 计划安装、扫描、聚合 outbox、v10 提示或 v2 混合冻结/消费，不把本片当成完整事件链。trigger 的既有独立 outbox 约束仍保留，后续批次切片必须完成后才启用新生产者。

迁移只在隔离数据库验证，未部署。无 disclosure 历史时可回0092且保留价格历史；一旦任何 disclosure plan/check/trigger/coverage 已落盘，正常降级会拒绝，必须保留0093及相应读取支持。此约束不等于已有验证可用的部署回滚镜像。


本片离线验证：完整fast为6636 passed / 10 skipped / 1 deselected（228.43秒）；价格完整组首次91 passed、2 failed（753.82秒），失败分别为运行中补强未发布0093导致旧template读新downgrade、旧unknown测试绕过正常增量绑定。保留原失败日志，未加DROP IF EXISTS或放宽断言；以fresh template及正常准入/Worker受控超时/finish修正夹具后，新增/邻域和两项失败共16项通过（64.73秒），更强的相反报价及unknown不可重开再1项通过（17.41秒）。最终类型/基线/SQL拒收、来源overflow、API只读和迁移均有新鲜结果；前序91项价格回归代码及条件未变，复用对应证据，不冒称存在一次最终全组全绿运行。

Ruff/format/diff、23源Mypy、生成合同检查、pg-b 434项收集、迁移offline SQL通过。集中审查的负例假阳性已修，基线T0冻结与跨kind资格抑制均另有RED→GREEN；未发现剩余确定P1/P2。当前仅隔离事实及模型替身，无真实来源/模型/通知/券商请求，不改旧结果、不解除unknown/预算、不启动业务宿主。

交付分层：SEC真值/闭集信号与旧unknown夹具修正可独立提交；kind迁移、所有旧消费者隔离及DB负例必须一起审阅，不能只落数据库唯一键就启用SEC写入。该核心diff体积主要来自显式DDL与跨表约束回归，未拆掉必要断言来减行。下一切片仍须实现研究时点实际冻结baseline、v10独立审核/采纳、封口批次聚合通知、精确accession内容准入、v2混合单股消费与双kind原子接替，然后继续完整页面矩阵、终审、最终同head CI和部署/兼容回滚，自然时段验收另列。完整Goal保持active。


### 2026-10-24 正常 v10 研究生产者冻结 SEC T0 基线（本地候选）

可靠 `DisclosureMonitoringContext` 已接入正常纸研究准备与精确读回。ready 绑定实际原件/receipt/canonical/index/issuer、四个本地可见时间及T0；旧baseline不因五分钟过期而失效。最新可见来源损坏或store失败是invalid gap，不回退旧件；missing/invalid保持当时冻结事实，后来到货不重选、不回填旧快照。只有原current观测需五分钟网络新鲜度，本文不将baseline当披露已研究证明。

显式 `--disclosure-monitoring` 依赖既有price及formal bounded setup，注册新v10提示，不改变旧v8/v9资产。market_state可独立提出SEC/价格条件，bear和evidence_audit各审原hash，portfolio只采纳精确hash；global audit非support仍同时否决。SEC提案必须引用paper context并绑定baseline id/hash/issuer/T0；无ready baseline不能伪造提案或继续模型探测。新wire/intent schema静态资产独立，旧字段None仍省略；修复paper context wrap serializer丢闭集schema属性但不改历史字节。

容量上界新增SEC proposal与两审，保持原8192输出、96k累计输入、100479 wire和截止；无新增SDK调用。快照冻结与准入共用prior/pinned/frozen/explicit/latest的版本解析，pinned v9/active v10不会增加SEC依赖，注册竞态导致两者scope不一致时整批nested transaction回滚，保护Job和预算。相邻回归另修受保护宿主初始身份：只读载入已封存配置后固定初始scope，后续数据库/账户/日历/batch/model/runtime漂移仍拒绝。

正常三股CLI/setup→准备→Worker三阶段→Output/Run/已知用量→受保护API GET通过。合成原件/真实持久化fixture为AAPL ready、AVGO/NVDA明确missing gap，各三阶段各一次，原watch/partial正式defer不变。API核对原baseline hash和verified stage trace；无真实来源、模型、通知或业务宿主调用，不将这份受控链当自然行情或设备送达证明。

验证：来源module13 PG+3unit（agent原16项，根串行集成）；根正常/邻域28 PG通过185.63秒。v10/旧hash/闭集schema/伪造及上下文50/52项定向unit均通过；最终正常producer2+真实protected-binding6共8 PG通过38.70秒，最终producer+loop344 unit通过9.09秒。整fast在最终authority修正前6645 passed/10skip/1deselected（224.81秒），后续采用上述定向复验，不冒称最终head全量fast或GitHub CI已完成。最终Mypy11源、Ruff/format/diff、生成客户端检查、Web typecheck通过。审查的版本选择P2已RED→GREEN，最终定向复核无新增确定P1/P2；所有源失败日志保留。

该片仅交付正常冻结与审核后的条件生产/持久化，SEC plan安装、封口观测批次、聚合outbox、精确accession内容准入、v2混合单股消费及双kind原子接替仍待下一片；不启用半成品SEC扫描或把索引状态冒充正文影响复核。未来实际v10落盘后回滚需保留v10 schema/prompt/读取支持，不删除新事实。完整Goal继续active，最终CI/镜像/部署/回滚和自然交易验收尚未完成。


### 2026-10-24 正常 SEC 安装、封口批次与聚合通知（本地候选）

本片以原 v10 冻结来源为输入，经正常 `install --kind disclosure` 安装审核后条件；价格与SEC独立。
可靠 current reader 精确复验原件/receipt/hash/issuer/四个可见时间，要求真实网络收取≤300秒，
partial 无新增为unknown，合格正例含partial新增及33项新增均保留true与完整集合；坏件不fallback。
正常 scan 对同股封口形成batch；价格+两份披露=三触发/一个outbox，重复扫描不重复通知，
迟到新capture只生成新成员批次，不改原batch。34项触发全部保留，自动复核容量明确blocked。
旧v1增量/每日冻结/派发入口及DB绑定围栏排除batch成员，显式准入返回typed context required；
尚未交付v2内容冻结/单股模型/消费/双kind接替，不冒称完整监控Goal完成。

0094新增不可变batch、精确成员及outbox payload范围校验，旧price-only outbox唯一性保留。
集中审查发现并修复两项P2：legacy复用batch outbox及并发不可见（双向校验先锁同一outbox）；
后继v8/v9研究被SEC current筛选漏掉（纳入全部监控版本，未证明保留前停止旧计划）。
定向静态复核无剩余确认阻塞，PG验证实际锁阻塞后拒绝晚到legacy成员。
正常CLI扫描覆盖三股独立通知：目标股混合一份，其余两股各自价格通知，不将全账户消息数误当单股数。
正常Bark worker以MockTransport验证并发、503后稳定message_id及回执；全部不创建Run/Job。
初版通知测试误以全账户只有一份通知导致等待，已查原隔离outbox并按三股事实修正测试，不改生产断言或旧outbox。

验证证据在writer `.secrets/daily-research-monitoring/`。完整fast 6716通过/10skip/1deselected
（214.72秒，早于最后source-current/SQL锁修正）；最终定向74项unit、Mypy全642源及增量、
Ruff全engine通过。来源/批次/迁移/旧类型/旧通知合并23 PG通过182.61秒；正常CLI→worker
2 PG通过30.92秒；最终并发迟到围栏及v8/v9后继3 PG通过37.30秒。最终批次显式旧入口拒绝及无新增Run/Job的2 PG通过27.56秒；空pending读取显式使用allow_empty，保留默认拒绝空集合的契约。
所有外部传输为合成/Mock，无真实模型、来源、Bark、业务库写入、宿主启动或未知占用变更。
新迁移仅隔离数据库应用；空表可回滚，有新批次拒绝降级，部署回滚镜像需兼容0094。

下一片继续精确accession正文准入、v2混合冻结/消费、双kind原子接替及页面矩阵，随后完整终审、
同head必需CI、最终镜像/部署及自然交易验收。本片未push/PR/远端CI/合入部署，完整Goal保持active。
