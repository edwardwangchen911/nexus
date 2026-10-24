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
- [x] 隔离正常研究完成后保存计划；`hold`、完整 `watch`、`partial` 的质量与覆盖独立，旧通用条件不升级。部署及自然运行仍另验。
- [x] 隔离正常 API/BFF/实际页面读回同一计划，不触发研究或采集，不改变十秒截止。
- [ ] 迁移保留 0086 采纳链读取；明确有新计划/引用后回滚的版本限制，不回滚业务历史。

### 2. 计划到五分钟检查、触发和去重通知

涉及 normal host planner/executor、monitoring persistence/service、新事务触发 producer、outbox registry、notifications worker/Bark 安全链接及只读页面。

- [ ] 真实隔离 PG：正常入口加载上述计划；无触发多 tick 零 Job、零 outbox；stale/unknown 不当 false。
- [ ] 跨阈值、持续同侧、回落后重越界、初次已越界、并发、重启、日切及休市案例。精确 observation/episode 身份去重，稳定通知 ID，出站确认丢失不重复业务研究。
- [ ] 可靠结构事件实际 producer → 原件 → 检查器，含迟到、修订、重复及不可靠分类拒绝；不使用手工布尔冒充上游已可靠分类。

### 3. 触发到单股增量及安全接替

涉及专用 monitoring admission、`agents/persistence.py`、输入投影/worker、预算/有效租约、监控版本关联。

- [x] 隔离正常入口生成一个受影响标的 Job；冻结 parent 论点和新证据，另两股不新增 Run；复用正式账户与未知占用保护。
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

### 2026-10-24 精确披露正文资格与封存材料准备（本地候选）

本片核查确认：旧refresh-disclosure按form选最新，不能兑现封存accession；旧普通SEC正文允许截断，
强8-K归档仅证明primary+99.1，不能证明没有99.2，8-K/A亦明确附件未收集。因而不能只把这些
引用塞入v2并让模型试探。新增固定8-K/8-K/A文档清单原件及全部声明文本文件的正常本地导入，
沿原SEC索引精确绑定CIK/accession/form/accepted/filed/primary及四个本地可见时点。

`research-monitor import-content`要求精确index snapshot、accession和已归档目录；每个文件有
原bytes/HTTP200 receipt/hash/size/URL，inventory引用目录与实际集合必须完全相同，包含99.2等
所有声明附件。仅支持文本html/htm/txt与primary/EX类型；其他类型、清单残缺、截断、缺件、
receipt早于提交或未来可见均明确拒绝。非文本/其他类型是当前支持缺口，不伪称附件完整。
解析器最多17份声明文档、每件8MiB及32k完整可见字符；prepared package整体256KiB上界，
超限保留原件并阻断准备，不截取或增加模型调用。该上界不是正式模型wire/token预算承诺。

原件、canonical及独立source `nexus_sec_monitor_filing` 保存到既有Artifact/Snapshot/EvidenceRef，
不登记或替换FinancialFact/Filing会计来源。available/created取实际对象归档后的数据库时间；
重复导入幂等，读回逐件重验原件及索引、canonical和时点。8-K/A关系not_assessed、会计修订
unverified、impact_review not_performed，不能绕过原EPS/TTM采纳或审计否决。

`prepare-batch`对完整sealed batch核验原始price quote/条件、每份SEC check及精确正文，冻结两kind
父论点、原角色审计、三条触发及每件文本附件为独立 `monitor_batch_inputs_v1`。仅索引则明确
missing_content，缺件不创建半包。相同批次并发准备只生成一个包/一次审计；迟到成员不入旧包。
历史读回仅重建原cutoff事实，与当前计划/Run终态资格分开；后来冲突正文或新计划不能改旧包，
正常再次准备仍重新检查当前资格并拒绝冲突/待接替。缺失对象和坏件均失效，不fallback旧版本。

本片不创建Run/Job/lease/预算hold，不消费触发，不修改未知费用/预算，不启动业务宿主。
prepared只表示材料已冻结，admission=not_checked；价格/身份/交易时段/账户/预算/租约与模型
容量仍须正式v2准入逐项重验。新材料source没有研究snapshot evidence authority，不能借普通
研究入口授权调用。仍缺精确accession正常网络采集、混合v2单股Job/3-call hold、模型输入消费、
按kind审核/消费/原子接替及API/BFF/实际页面矩阵；旧v1拒收batch保护保持。完整Goal继续active。

集中审查确认1P1/4P2并修复：末行未闭合清单遗漏附件（明确拒绝）；实际归档发布时间；清单
receipt时间下界；CLI固定缺口码；历史cutoff受后来plan/终态污染。对应缺陷回归均实际RED→GREEN，
补充原件存储失败、晚到冲突、并发、材料包不能获研究授权及无新增Run/Job/hold。
没有新依赖或数据库迁移，schema仍0094；所有新存储不可删除/回填旧事实，回滚保留0094及
原件，旧版本可关闭新入口但不得把不支持的材料读为研究已完成。


本片验证：完整fast6728 passed/10skip/1deselected（238.61秒，最后price原件复验/缺口清单/存储
错误增量以前）；来源/批次/旧SEC原件链35PG通过235.24秒。最后正常材料入口综合PG通过
22.75秒，覆盖两份精确正文/99.2、并发只一包、一审计、迟到冲突/新plan、旧cutoff、存储失败、
普通研究准入拒绝且Run/Job/预算预留数量不变、归档实际完成前不可见、精确missing_event_ids。
最终33定向unit、Mypy645源、Ruff全engine/format/diff通过；正常入口测试已加入CI组。
全部原件、模型、传输均为合成或隔离替身，本片没有真实来源/模型/Bark/业务写入或宿主启动。
尚未push/PR、远端CI、最终Chat终审或部署，不把这份材料资格证明当完整研究消费闭环。

### 2026-10-24 混合批次单股原生任务及三阶段消费（本地候选）

承接4aa287ef材料包：新增显式 `nexus_monitor_increment_v2` / `monitor_increment_v2`，旧price-v1
序列化与入口继续隔离，不以缺省kind接收SEC。正常 `admit-batch`冻结原材料ID/hash/全部成员、
两kind父论点/原审计、新决策报价/候选/账户投影及新T0 SEC baseline或明确gap；一批只创建一个
证券的Run/原生Job/一次USD hold和3次SDK call hold。重复入口返回原身份与原状态，未知不重放。

0095新增Increment nullable unique batch_id，旧行NULL；显式v2 evidence ownership及四表deferred
接缝检验完整batch/material/parents/typed成员、唯一Run和整组状态，原v1函数保留独立分支。
空schema可降0094，存在v2事实禁止降级；未来回滚镜像必须保留0095及其读取/绑定能力。
DB结构性独立3PG验证只是关系围栏，正常CLI/Worker原件链另验，不互相替代。

新版本通过正常readonly tools及正式capacity pool送入SDK三阶段。初次真实编译触发原wire
上界：不是上游缺正文，而是同一父论点/八角色审计被两个kind重复传输。确定性model_view
按原output身份只传一次父论点/原审计，保留完整文档text/哈希和精确source引用；完整receipt/
artifact绑定仍在不可变材料包逐次验证。没有提高100479字节、96k累计输入、8192输出或截止。

新增正常精确内容采集 `collect-content`：指定旧index/accession，固定官方目录清单及全部声明
文本原件，严格HTTP200/无重试/最多清单+17份正文/总60秒。使用已保护的本地配置与SEC_CONTACT；
账户/candidate scope在短只读事务内检查，网络在事务之外。失败保留已收bytes/receipts及固定
stage/请求数回执，不覆盖失败目录、不换accession、不退回“最新一条”。本片只隔离Mock验证，
未真实调用SEC。采集者源a56d85cf、DB源1c8a7ade已由唯一主writer串行整合。

集中只读审查确认并补齐：SEC-first typed成员不能被宿主/零调用封口硬拒绝，v2须用batch
幂等键；已知模型完成不能冒充逐事件impact审查。成功typed触发保留原归属/原生completed状态，
固定reason MONITOR_EVENT_REVIEW_UNRESOLVED；finish和batch报告显式impact_review=not_verified、
coverage=unresolved及未解决数，unknown保留原unknown/预算hold。绝不回退pending或解绑以重跑。
旧reconciler对v2明确awaiting_mixed_review，不装入旧price-only消费/接替路径，也不伪记source
unverified；新的mixed后继会让旧SEC计划等待正式接替，而不是忽略其可能否决。

本片尚未完成逐事件addressed+反方/审计+精确采纳、按kind消费及原子接替；正常宿主自动选择
待处理typed批次、coverage依赖的宿主退出确认和新页面也仍待下片。原v1自动pending过滤继续
排除typed，防止半片意外激活模型。账户已知结算可用与完整宿主闭环分别报告。
完整Goal保持active，后续仍需API/BFF/实际页面、完整diff Chat终审、最终同head全部必需CI、
最终镜像/兼容回滚/部署与真实交易时段观察；本片不以Job或CI代替全产品验收。


本片最终离线证据：根三种正常材料→admit-batch→原生Worker/零调用封口PG通过64.09秒，
含成功3阶段、critique未知2阶段不重试、纯SEC首成员的零调用容量拒绝；已知账户准入passed，
unknown过租约后仍拒绝，预留3calls不退。根21项新旧混合/DB/原price增量/原preclaim/collector
PG通过185.33秒；完整fast6749通过/10skip/1deselected（221.56秒）。最终Mypy648源、Ruff及
format/diff通过，末尾CLI安全码与定向回归日志单列；正常采集函数另有71unit/5PG独立证据。
以上均合成原件/HTTP与模型替身、真实隔离持久化，零真实模型/SEC/Bark/业务宿主或账户写入。
没有释放业务未知预留、重放旧请求或放宽正式门禁；当前尚无远端最终CI、部署或真实交易时段证明。

### 2026-10-25 逐事件双审、两类原子接替及只读收尾（本地候选）

本片承接354a205d的同批单Job与结算证明。原三阶段输出新增可空aux字段：market_state逐项
proposal、bear/evidence_audit原proposal hash审核、portfolio精确采纳hash。只对显式v2启用新
wire；不增加调用、限额或截止，旧v10 wire、ResearchIntent及None字段序列化保持原字节。
完整成员、kind/event_key和SEC原件snapshot_id逐一匹配冻结材料，正文已取得不等于已复核。

正常reconcile只在原生终结、已知结算及原件/输出资格通过后记录逐事件消费证明。新增0096
append-only事件回执，精确绑定批次/触发/Run/Output/原材料/context/proposal/双审hash。
price/disclosure两类coverage必须同Run、同观察时点一起落盘；新plan还必须具备本类全部
事件的合格回执。直接SQL成对伪造coverage、未知用量、未采纳或外来引用均不能装新plan。
公开单类install拒绝v2绕过，只有完整编译后的内部原子接替路径可安装。

某类有unresolved、无冻结前代或无可批准新条件时，该类明确gap，不推进baseline、不续期
旧条件。真实前代冲突使全批superseded且不消费事件。第二类安装失败回滚全部新plan及消费，
再保存两类固定gap；已知原生终态在savepoint回滚后重新封口，不重派Job。审计否决不能
被synthesis或采纳hash覆盖；unknown不写消费/coverage，不释放USD或3call预留。

只读资格入口复制完整批次校验，仅去掉outbox行锁；原写入guard保持原锁。宿主检查、零调用
证明和退出继续完整校验冻结身份，不能以released预算本身当零调用证据。新v2终态dispatch
读取原请求、完成reconcile并重新读回metadata，skipped必须先通过原生零调用证明。空批次
不标verified；批次与整股未解决计数分开。当前review原因与历史native_reason分别返回，旧
Trigger/Output/快照不追改。0096有新回执或typed coverage后禁止降级；回滚镜像须兼容0096。

本片最终验证结果见本段末尾及私有checkpoint；所有资料、模型和网络替身均隔离合成。
本片不包含自动typed pending选择/正常采集准备、未解决旧事件后续研究编排、新API/BFF/页面。
它们仍是同一完整Goal的剩余工作，不能将本片视为全监控闭环或自然时段验收完成。随后仍须
实际diff Chat终审、最终同head必需CI、镜像与兼容回滚、合入部署及自然业务观察。

本片最终离线证明：正常CLI/Worker/双类reconcile/只读宿主收尾7项通过（245.45秒），覆盖
正常3阶段、critique超时未知2阶段、零调用容量拒绝、逐项未解决、审计否决、第二类失败原子
回滚及纯披露首成员；旧dispatch/reconciliation/migration/mixedDB共20项通过（230.10秒）。
完整fast6785通过/10skip/1deselected（259.41秒），最终91项定向unit、Mypy651源、Ruff与
format/diff通过。0096合并后的全组PG结果保存在event-db-integrated-final.log，并在私有
checkpoint登记；集中只读复核无新增P1/P2。未启动真实模型/SEC/Bark/业务宿主，未改旧结果
或接受、解除任何业务未知占用。以上是本地候选证明，不能代替最终远端CI或部署事实。

### 2026-10-25 宿主自动接手已登记混合批次并准备、派发（本地候选）

普通宿主优先跟踪原Run；无原Run时按完整封口集合选择，兼容SEC首成员。五分钟检查按整股
跨kind及旧price-v1互相围栏，不以换成员或旧入口绕过。Daily/manual/evaluation和已有未知
优先级保留；Daily PREPARE在新pending查询前返回。既有Run/Job不换键、不创建第二尝试。

`prepare-pending --batch-id`是正常正文准备入口。短事务先保存精确accession的声明，再离开
事务取一份缺件；每次一个accession、总60秒，所有冻结成员原件齐备后才prepared。并发、
取消、重启与失败不重取；历史未知保持未知，合格已有材料零HTTP复用。私有目录固定owner
0700且不跟随symlink。没有模型任务或预算hold副作用。

完整材料就绪后复用正常prepare_schwab_decision_authority，仅原候选股、原日历/universe，
不扩执行scope，不重新跑每日filings/macro/trends。已有合格fresh authority零外呼复用，
stale才以单批一次声明调用正常单股风险证明和行情producer。新包装仅设300秒有限总包络，
原HTTP/Worker/模型和BFF十秒截止不变；超时、取消、无结果声明均unknown且不重放。用量/
unknown/有效租约、protected_demand及future预算保护在IO前后复核。宿主为准备额外留420秒
启动余量，不能挤掉原每日/evaluation槽；无Run退出须同时有正文和市场两个确定准备证明。

Ruling：原SEC评审§5不允许隐式拆批自动消化overflow，故按整股未准入pending总数>32阻断，
不是只限制单批。shared count与报告既有口径一致，用于selector各kind、正文/市场准备及新旧加锁
准入；两批17+17或再附旧price触发都不能绕过。其他股与既定每日槽不受此股容量阻断。
新的typed准入失败在savepoint回滚后保留整批blocked与安全审计，不产生部分Run/Job/预留。

完整fast6791通过/10skip/1deselected（225.25秒）；最终Mypy653源检查，59定向
unit通过。正常宿主/正文/市场组合24PG通过（261.97秒），最后两项跨kind/overflow gate
定向PG通过；最后旧price-v1派发/增量8PG通过（90.93秒），所有准确耗时见私有checkpoint与host-*日志。
独立集中只读终审未发现剩余确认P1/P2，测试均隔离合成，没有真实网络/模型或业务宿主调用。

仍未完成五分钟current行情与SEC捕获producer的正常周期接线（现scan读取落盘事实，行情
准备发生在已登记trigger之后），未解决旧typed事件在未来研究中的精确消费、API/BFF及实际
页面；随后仍需完整diff Chat终审、最终同head全部必需CI、兼容0096回滚/镜像/合入部署和
自然交易时段观察。本片不冒充完整自动检测或真实模型验收，未知业务占用保持。

### 2026-10-25 五分钟正常 current 观测生产者接线（本地候选）

现有宿主在每个实际观察到的 UTC 五分钟桶先调用正常`observe --calendar-authority-id`来源
入口，再执行独立只读D扫描；不另开宿主，不从页面/GET暗触采集。旧profile、禁用、休市、
无有效对应计划或已有合格fresh事实均零外呼。只读扫描仍保持原8秒截止；新的前置采集总
包络240秒，SEC和L1分别90秒，不改变已有HTTP/Worker/模型/BFF十秒约束。采集失败仍检查
落盘事实，过期/缺失是unknown，不是false；取消则不继续D扫描，关闭收集自有任务。

SEC只取原候选/原baseline绑定的issuer和identity，保留真实目录/回执/bytes验证。来源事实
import不授计划资格，D仍重验当前source_current、issuer和原baseline，不换baseline吞pending。
正常collector根因一并修复：204/206在body读取前拒绝，不再固定伪记200，原单次尝试/容量
和HTTP超时不变。L1仅合并需要的原候选股一次正常socket capture→原件解析/导入，不调用
整轮risk/corporate-actions/Nasdaq准备，也不扩订阅scope；metadata与最终scope/回执同事务
承认，失败回滚元数据保留对象原件。observed/available/received各自保留，不拿DB写入时间
刷新旧证据。请求数不能确认时为None，不与模型未知费用混为一笔结算。

source tick先在短事务声明固定account/证券/feed/current桶身份，网络不持account/security/
schema锁，同桶并发/重启/失败/unknown不重放，错过桶不补跑。下一桶是独立的新current
观察，不重试旧桶或旧研究。已有Model未知预算/租约不解除，不创建Run/Job/hold。QUOTE
取消收尾原shield可能留下后台writer，事件控制RED后改为当前task直接有界await并传播新的
CancelledError；仅审计收尾5秒，未完成claim保留围栏，无后续scan/悬空任务。

最终根正常CLI→实际原件导入→D/outbox 3场景通过（42.98秒）：价格+两披露=3trigger/1合并
outbox，重复同桶零新增；price-only无触发零Model/通知；双源失败读过期事实为unknown且
false=0。根组合22PG通过（129.98秒），包含并发、取消/超时、fresh复用、原件身份漂移、
跨桶、正常L1归档/导入及取消收尾。完整fast6813通过/10skip/1deselected（227.89秒），最终
65定向unit（3.71秒）、Mypy655源、Ruff/format/diff通过。集中只读终审无未修P1/P2；非Chat
终审。测试全部隔离合成HTTP/socket/模型，业务库/宿主/未知占用未动。

仍需旧typed unresolved在独立新正常研究中的精确消费、API/BFF及实际页面矩阵，然后全diff
指定Chat终审、最终同head全部必需CI、镜像/兼容0096回滚/合入部署及自然交易时段证据。
本片证明正常来源→确定性检查，不把outbox写入当设备送达，不把模拟socket当真实网络或
Model调用，也不以绿CI/健康替代全产品闭环。

### 2026-10-25 正常日研究的附带旧价格容量缺口（本地候选）

确认的拒绝点是正常daily准备`agent_paper_context`时，旧price-v1待处理集合超过32项，
`pending_stock`的容量异常被正式入口转为整轮`RESEARCH_ADMISSION_UNAVAILABLE`。既有
SEC评审§5要求附加pending不能永久阻断既定daily；这不豁免daily自身正式准入。

现在只有专用`PendingOverflow`可转为冻结`daily_monitor_gap_v1`：scope明确是尚未分配的
price-v1集合，记录截取前完整count、原T0和固定原因，不附部分父论点/成员。正常三股daily
继续走原身份、行情、账户、预算及租约；增量仍拒绝>32，不拆批、不清事件。其他身份/原件
异常继续走原正式拒绝。gap与pending_monitoring互斥，新字段None时完全省略，保留旧字节。

三阶段只对确有冻结pending的集合生成消费提案。gap明确提示尚未复核/消费，SDK拒绝孤立
采纳hash，原无上下文proposal/review拒绝和消费函数返回0保持。新baseline或monitor plan
不代表旧事件被消费，late第34项不回填T0的33项gap。合成超时依旧保留新Run未知费用和
reserved预算，不重派Job。研究质量、监控覆盖和正式执行资格仍分开。

此片不新增依赖、表或迁移；0096不变。新非空gap快照要求读取版本支持该字段，后续镜像
回滚必须同时保留0096和新输入读取能力，不能删字段、重写快照/hash或以旧镜像能启动代替
兼容核验。所有业务宿主、旧请求、未知接受及实际来源/模型/通知调用保持未动。

指定Chat定向设计评审`nexus-unresolved-daily-consumption-20261025-01`已关联完整新请求、
completed回复与idle，结论有条件通过。它是后续typed daily消费设计判断，不是代码终审或
运行通过。本地已核对其P1/P2：旧已分配事件须精确属于原冻结并由原终态Output标为
unresolved，known settled/无活动未知占用；不顺带接纳known-failed。历史来源读取与当前
各kind接替generation分开；新增独立proof，双writer/DBguard按同trigger锁跨表排他，损坏
证明拒绝而非当未消费重派；消费与当前coverage同事务，晚材料只入新冻结，原Run不改。

下一片仍需完成上述typed daily v2独立消费证明及正常受控全链，然后API/BFF/实际页面
矩阵、全diff指定Chat终审、最终同head全部必需CI、最终镜像/兼容回滚/合入部署和自然市场
证据。本片不宣称旧typed unresolved已可消费，也不把本地测试当完整产品或远端CI完成。

本片最终隔离PG32项通过（308.31秒）：32正常冻结、33项gap、迟到第34项不回填、原生
Worker已知/未知结算及无消费、非法原件拒绝、旧消费回执/增量准入；完整fast6820通过/
10skip/1deselected（218.31秒），最终43定向unit（4.61秒）、Mypy655源、Ruff/8format/diff
通过。与父提交13ccfd53实际读取代码对比的旧上下文字节/hash完全一致。两次独立只读
复核无新增P1/P2。初始新拒绝测试误假定内部异常透传及cycle有可变status，已改为核对正常
unavailable回执和append-only事件；失败日志保留，生产拒绝及原有断言未放宽。全部模型/
来源/通知为替身，未接业务数据库或启动业务宿主；完整Goal继续保持未完成。

### 2026-10-25 独立日研究消费旧typed未解决事件（本地集成）

正常三股准备可冻结daily_monitor_context_v2。原assigned成员须精确对应原批次/Run/Job/
Output/正文、known settled且原market_state明确unresolved；不接known-failed，不把已接受
unknown当known。历史依据按原prepared_at/Job.finished_at核验，新T0另冻当前两kind前代。
缺件、损坏、完整count>32或总材料>256KiB转gap，不截集合，不豁免日研究自身门禁。

SDK复用原daily proposal/双审/采纳schema，完整context hash绑定精确typed成员与正文。
模型视图只移除重复父论点和归档bytes副本，完整正文、原论点与原审计仍在；原Snapshot不改，
frozen_context_hash绑定原完整材料。原wire/token/时效/执行门禁及调用数保持。

0097独立append-only event receipt与旧0092/0096跨表锁同trigger排他；新两类coverage/
合法plan/addressed proof同事务。前代冲突两类superseded且零proof；第二类安装失败回滚
所有新plan/proof再落双gap。unknown无proof、不释放费用预算。historical reader不因其余
未消费成员后来正常准入而改变原冻结，当前被消费项自身仍精确核live归属。pending/API统一
读新proof；损坏proof拒绝且presence继续围栏，API用量回归原Run，不自动重派旧请求。

材料6PG+9unit（127.47秒）、历史后分配RED→GREEN；最终typed DB21项通过（286.34秒）。
根正常旧3unresolved→新三股九阶段→3proof+两类实际installed+API读回最终强正例通过
（261.10秒）；新超时未知与第二类失败整体回滚2项通过（389.39秒）。旧Run/Output/hash/
触发归属/快照保持。最初完整wire超限在模型前真实拒绝，去重复后原cap通过，不改业务断言。
完整fast6826通过/10skip/1deselected，但旧固定head断言失败；已同步实际0097authority，
57定向及最后12项unit通过，659源Mypy、Ruff/format/diff通过。非远端CI或真实模型验收。

尚需收口：≤256KiB材料仍可能超实际wire/token，须在首次调用前整组退gap，不能放宽限额
或让可选包永久挡日研究；部分覆盖后其余准入/损坏proof身份用量定向PG、旧plan过期换代、
veto/unresolved完整native矩阵亦待补。之后API/BFF/实际页面、全diff指定Chat终审、最终
同head全部必需CI、最终镜像/兼容0097及新输入回滚/合入部署、自然市场证据。业务schema
未迁移，真实模型/来源/通知/业务宿主未调用，旧未知保护保持，完整Goal未完成。

### 2026-10-25 日研究可选事件整包容量（本地集成）

正式正常入口在新 Run flush 后、Job 发布前，复用真实冻结模型/提示与数据库只读工具，
核对各阶段 wire 上界和 research 精确输入 allowance。仅完整可选 daily v2 超额时，专用
错误触发整个未发布候选保存点回滚，以同一全候选集合重建并给该股冻结
MONITOR_CONTENT_CAPACITY/完整成员数；不截成员、不改旧快照、不重派已发布 Job。
基础研究/整包 gap 仍超额、普通异常均保持原拒绝；Worker 完整候选预检保持原样。

预览的首份 v2 材料激活原195秒绝对截止，后续候选和有界整池重建沿用该时点。真实异步
超时收回本任务，不启动后台 waiter；退出的最后时钟核验仍在保存点内，避免返回拒绝后
残留 Run/Job/预算/认领。普通上游 TimeoutError 与外部取消不伪装成容量缺口。

四项 legacy+typed 混合原件先完整冻结，保留原全部成员、正文和历史验证断言；其真实
wire 超限后最终整包 gap/count=4，三股仍完整发布且旧事件未消费。容量变化发生在模型
输入选择层，不把原始冻结能力删成截断集合。无依赖/配置/迁移变化，业务schema仍未迁移。
新非空gap及0097已有输入读取兼容要求保持；后续回滚镜像不能删除已落盘新事实。

本片仍不等于完整目标交付。后续原定剩余proof/准入及native矩阵、API/BFF/实际页面、
指定Chat完整diff终审、最终head必需CI、镜像及兼容回滚/合入部署、自然市场证据继续保留。

本片15项正常入口/真实PG整组通过（342.84秒），覆盖实际wire、基础研究仍超额、晚成员
已暂存两Job/两hold的整体回滚、普通错误和已发布幂等返回。终审补齐“继承deadline但
配置重验原在timer外”的P2：过期scope在创建timer/ContextVar前拒绝；仅重建配置重验移入
保存点内的继承scope，初次原配置错误语义保持。两项精确RED→GREEN（48.94秒）证明
过期时不进第二次profile/快照，profile阻塞被同一原截止取消，零新占用/SDK/后台waiter。
独立定向复核该P2关闭，无新增P1/P2。

稳定代码全fast6858通过/10skip/1deselected（257.55秒）；之后最终窄P2增量31unit和
最后42定向通过，660源Mypy、全Ruff/5format/diff通过。首次全fast收集临时去重实验后
撤回实验，导致1项旧测试在新代码上失败；保留失败日志并固定代码重新全量验证，实验
源码和测试均未进入最终候选。首次并行Mypy共享缓存内部异常已以独立缓存重新全量通过。
这些本地证据不等于最终head远端必需CI；未push/PR/合入/部署。

### 2026-10-25 历史消费、接替与下一轮准入边界（隔离验证）

本片继续完整目标的既定验收缺口，不新增运行授权。模型、资料和通知仍为隔离替身；
实际证据层为正常 start/resume 服务、原 Worker、真实随机 PostgreSQL 和 API reader。
它们不是 CLI 子进程、HTTP/BFF/浏览器、真实模型或自然市场证明。

审计否决与全部未解决两种 native 场景新增断言后通过（2项，518.37秒）：重复 reconcile
不新增 plan/coverage/proof/Job/outbox/audit；两kind均gap、active_plan_id为空，固定原因
分别为 REVIEW_UNVERIFIED 与 KIND_EVENTS_UNRESOLVED；原三事件、Output/hash/归属和
冻结不改，预算已知结算，API仍读原Run与known用量。这里“零新增”不扩大成所有既有行
零UPDATE。新增断言前的2项523.88秒基线另留，不用于代替最后验证。

历史过期/换代2项PG通过（322.42秒）：旧混合包的disclosure父计划已被正常generation2
接替，另一个参数使旧price条件由最初合成SDK入库前合法设置600秒有效期，在新T0自然
过期。新日研究仍精确冻结原合法材料及未解决price，冻结当前两kind前代，经九阶段后
原子安装price下一代/disclosure下一代及唯一新daily proof；旧2mixed proof、材料正文、
原Output/hash和事件归属保持。没有修改不可变原件，也未单独证明“消费前未解决事件
自身kind已被其他Run接替”的更窄场景；不借用该描述扩大这两个案例的证据范围。

部分事件消费的真实正常收尾用例最初被 MANUAL_ACCOUNT_BUSY_OR_UNKNOWN 正确拒绝：
三研究Job成功不等于组合周期完成。测试现沿原 resume、组合Worker、已知结算、正常
收尾再进入独立新三股准备，不删除Job/hold或手工补周期终态。只保留未消费两公告，旧
price proof与API读回仍有效；损坏证明时仅在隔离测试库临时关闭本表UPDATE guard并恢复，
把证明指到另一股prepared Run，以not_started/known的区别核对身份与用量不会串用，
再用正常 research-monitor parser/run 确认明确拒绝且零新增工作。

剩余原范围不变：API/BFF/实际页面及完整安全矩阵、全diff指定Chat终审、最终head必需CI、
镜像和兼容0097回滚、合入部署及自然市场。静态已定位新v2 coverage固定原因尚未全部
映射到API CoverageReason/页面说明，后续展示切片必须实测补齐，不能把有效gap显示成
合格覆盖；既有执行、审计、时效价格、预算及有效租约门禁保持。

收尾分类已进一步收紧：本合成case三份研究均complete/watch，组合选择均defer；既有正常
完整研究路径的固定结果为NO_ORDER_REQUIRED，不是研究quality partial的暂缓分支。
相同全现金目标应复用旧active target，验收须核新decision→组合Output→三个原研究Output
的绑定及精确activation幂等回执（target_created=false、无batch），不能错误要求新target。
最初同时接受两个收尾码的宽断言已移除；两次收紧时发现的错误期望均保留日志，不改生产
政策或夹具质量，不据本case回填旧partial、未知用量或自然结果。最终精确整链1项通过（371.12秒）；坏proof拒绝后十类ID集合零新增，原快照/hash/归属及旧预算值保持。

### 2026-10-25 双类监控正常API/BFF/页面（本地候选）

新增可空 disclosure_monitoring 读模型，旧研究没有声明SEC监控时保持null；价格与SEC各有
独立计划状态/覆盖原因/检查时点。每项披露分别显示原批次、申报号、原/接替Run、用量与
通知状态，以及 addressed/unresolved/unverified/not_performed，不从Job completed推导已复核。
最近五项只是页面有界预览，完整总数与原批次不变；不把索引发现说成经营影响或设备送达。

三类固定gap原因接入生成合同、BFF闭合校验与两卡片文案：仍有事件未解决、完整复核尚未
通过、计划接替未完成。不猜是资料缺口、审核否决或研究质量不足。GET只读，不reconcile、
采集、调用模型或派发Job；交易锁、原正式门禁及十秒BFF上限不改。无新依赖/配置/迁移。
同源mixed资格仅在一次reader作用域按精确Run/Output复用，逐事件hash/review/member和
双kind coverage仍独验。坏daily proof不借用新Run用量；SEC额外核未来Job终态、子Run及
通知时点，未来数据退未知。旧price历史asof相对SEC仍有静态差异，列入后续完整时点矩阵，
不以公开GET固定current read_at为由假称任意历史时点均已验证。

后端7项真实隔离PG通过（604.23秒），覆盖正常受保护ASGI双SEC同批/price兼容/旧时点、
正常mixed及两类部分未解决、审核/接替拒绝、合法新daily proof与单坏proof原Run/known回退。
新6unit通过；完整引擎fast6865通过/10skip/1deselected（243.16秒）、661源Mypy及Ruff通过。
正常generate、check:generated、TypeScript、ESLint和Vinext build通过。Web标准全套1344通过/
1skip（37.54秒）；先前并发时既有工作台摘要等待1fail保留，单文件26及限并发全套通过，
未改该断言/超时/生产代码，不把资源假设当已确认根因。独立只读复核无新增P1/P2。

实际loopback TCP API→正常BFF→Chrome页面已覆盖待处理与三种gap，手机390px无横溢，
截图已人工核看；pending后的503/403由浏览器受控传输注入，分别保留stale旧内容/清空两pane。
三种gap来自明确合成coverage投影，不冒充自然模型结论；正常生产者语义由上述native PG证明。
未知/已复核及精确事件研究链接的最后严格2项通过（91.81秒）；前四类同代码实际页已过，
合计六类真实HTTP/BFF/浏览器证据齐备，不能称存在一次最终六项同命令全绿。所有资料/SDK/通知为替身，
业务库/宿主、旧请求、未知接受及预算保护未动；不宣称CLI进程、自然行情、真实模型或送达。

未知混合研究即使租约自然到期，也因费用未知停在awaiting_mixed_review，不获覆盖采纳；
API应显示awaiting_reconciliation，逐项用量unknown且budget reserved。测试先错误要求
usage_unknown coverage，现按原consumer已知结算门禁严格核对“无覆盖行+待复核计数”，
不写假unknown覆盖、不接受未知影响或放宽断言集合。十秒耗时用真实单调时钟；合成数据库
与worker使用显式固定业务读时点，HMAC/实际网络耗时不改。端口探测按正常SO_REUSEADDR
语义排除TIME_WAIT，不复用其他存活服务。

仍需剩余hold/安全时点及完整矩阵、指定Chat全diff终审、最终head全部必需CI、镜像/兼容
0086采纳+0097/v2/gap回滚、合入部署和自然市场；本轮尚未push/创建PR/合入/部署，完整目标
保持未完成。最终CI分组耗时须按实际同head全部job日志聚合，不以扩业务期限/删测试掩盖。

### 2026-10-25 价格监控的研究、结算与通知读取时点

正常认证GET的五个新增反例先在旧实现全部失败：未来child的UUID提前可见、Job终结前
提前显示known、后来published/dead_letter提前可见，以及未到observed_at的v1 daily回执
把另一Run的known用量借给仍pending的原触发。另对created_at单独移除过滤，观察新增
反例失败后恢复；两个独立回执时间边界均在JOIN/COALESCE之前过滤，不只修改covered标签。

最小修复保留原触发及坏v2 proof回原Run路径；研究链接与用量统一取同一可见child，未来
子Run隐藏链接，缺少已到读取时点的Job终结证明时状态与用量保持unknown。通知终态还须
published_at/dead_lettered_at可见；不从当前可变摘要反推过去，不修改旧数据、采纳或结算。
读取前后比对扩至12张表完整行hash，新增租约、通知尝试及审计，预算、调度和正式门禁未改。

首轮正常API七项通过；最终八项时点/原unknown与SEC、混合研究、daily及坏证明邻域共21项
通过（703.36秒），结果见writer私有price-asof-api-final.log。十二表完整行hash保持，
既有unknown保护及合法/损坏v2 proof归属回归通过；不以Job completed推导事件已addressed。
完整引擎fast6865通过/10skip/1deselected（245.64秒），661源Mypy、Ruff、格式、TS及e2e lint通过。
正常TCP API→BFF→Chrome的未知用量/已复核两项再次通过（108.72秒），展开价格触发详情
核对预算保留、known及精确研究链接，390px/桌面无横溢或页面异常；API全GET，最慢2.423秒。
十秒截止不变，资料/SDK/通知仍是替身；不把本片算作真实模型、自然市场或通知设备送达。

这一片只推进旧price读时点缺口，未新增依赖、配置、迁移或业务运行；仍需hold与剩余完整
CLI/安全矩阵、指定Chat全diff终审、最终head全部必需CI、镜像及兼容回滚、合入部署与自然
运行。此前部署和真实验收不被本地回归取代，完整目标未完成。

### 2026-10-25 已持仓的正常研究、监控和增量闭环（本地候选）

新完整场景先沿正常正式三股研究、组合、确定性风控、内部模拟订单与成交服务建立两笔
持仓，核账本借贷平衡、现金、数量、无未结订单；不手插position或只修改stance冒充持有。
随后新正常周期冻结正持仓权重，九阶段形成两份complete/hold与一份complete/watch，组合
对持仓maintain、对watch defer，正常收尾NO_ORDER_REQUIRED，不新增订单或改变五类账本原件。

两个五分钟桶的新鲜未命中观测零新增模型/Job/outbox；随后只有一股命中，同桶重复检查
保留唯一trigger与outbox。正常投递器使用通知替身，仅一次published，不声称设备已收到。
原账户的专用准入冻结精确parent Run/计划所绑定Output及hash、hold/maintain原论点、
原触发报价与另一份当前报价，再由原Worker三个阶段完成同股增量，其他两股无新增Run。
已知用量结算、generation2接替与同条件episode继承通过；重复reconcile不新增工作，旧
Output/hash和原模拟订单、成交、账本、持仓调整原件不改。下一轮正常三股准备真实准入
在回滚事务中通过，未启动下一轮模型或留下prepared工作。

本例定位了真实接线缺陷：监控错误套用手动入口的原始历史终态，拒绝已有完整模拟成交
证明的自然周期。价格与typed/mixed入口现仅显式复用既有allow_scheduled_completion资格；
仍要求非手动周期、完整PAPER_CYCLE_COMPLETED投影、全部Job身份、已知结算和租约证明，
之后照常检查未知claim、请求历史、占用、预算和当前输入。手动入口默认False及其拒绝
保持；不会将原awaiting_market历史改为completed。缺少typed材料依旧拒绝，不创建Job或预算。
价格与混合两条真实拒绝均先RED，修复后正常持仓流程GREEN。

页面原先把hold误标为“保持观察”；两个DOM反例先RED后统一为“持有”，watch仍“仅观察”。
已保存结果和完整性、审计及交易锁说明不改，无新增交易动作。正常TCP API→BFF→Chrome
核父/子精确Run、已知结算与通知处理、计划接替、桌面/390px、零写/零page error通过；
最慢API0.515秒，十秒截止不变。完整持仓/CLI/页面场景105.70秒是整个隔离测试耗时，
不是实际模型研究性能。独立临时安装本候选注册CLI并走正常配置入口，只读同一计划；
CLI采用真实DB当前时钟，正确显示该合成历史计划expired，不用API的测试读钟伪造新鲜。
共享venv、业务宿主/数据库及旧unknown费用预算未改。

最终完整fast6865过/10skip/1deselected（255.72秒），661源Mypy、Ruff、格式、TS/lint/build绿；
Web标准默认全套1345过/1skip（32.63秒）。中途一个既有what-if异步显示等待失败保留，
该文件5项及标准全套复验通过，未修改该文件、断言或超时，不宣称已经确认环境根因。
历史资格12项增强回归覆盖unknown费用、queued/running、错Job/重复缺漏、有效/迟到租约
等；两项已知/unknown增量与先前同组14项通过，12项增强复验有重叠不重复计数。旧页面
helper默认路径的实际BFF/页面1项兼容通过。typed已知/未知正常Worker最后邻域2项通过
（64.21秒），unknown仍保留用量/预算占用；证据为hold-mixed-known-unknown.log。

本片的模型、行情、资料及通知全为明确替身，数据库/Job/Worker/账本/API/BFF/浏览器为
真实隔离运行。不得把它当作新真实模型、自然市场、设备送达或已部署证据。剩余工作先
按完整矩阵核对已具备的证据与实际缺口，再做指定Chat全diff终审、最终head全部必需CI、
镜像/0086采纳及0097读取兼容回滚、安全合入部署和自然交易时段观察；不重放旧请求。
