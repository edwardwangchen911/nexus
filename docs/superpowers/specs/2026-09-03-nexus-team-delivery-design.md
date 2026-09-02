# NEXUS 模块团队交付技术方案

核查日期：2026-09-03（北京时间）。基线 `main@dce390b`；运行 API 版本 `fea44d7`。
执行计划：[团队迭代计划](../plans/2026-09-03-nexus-team-delivery.md)。

## 1. 目标、范围与事实来源

按用户本次要求，把剩余工作变成可直接实施的模块切片：独立会话、独立分支/worktree、单元测试、定向集成测试、集中审查、小步主线合并、下一轮先同步主线。

当前产品仍是单用户 Mac 本地研究决策工作台。交付目标是：真实资产与时点明确的数据 → 诊断 → 有证据与反证的研究 → 可计算方案比较或证据充分的不调整结论 → 人工选择后的影子验证。行情时间展示和公开公告证据并行补齐。新闻覆盖先做官方公告，不冒充全市场实时新闻。

依据原始产品设计、2026-09-02 主线交接、最新证据记录、实际源代码及在执行任务的回报；旧阶段未勾选项不等于没有实现，旧 README 数字不等于当前运行事实。最新根 `AGENTS.md` 高于旧方案的逐次只读审批、WIP=1、production 等表述。

已有并复用：Gateway/Provider、PostgreSQL/对象存储、FastAPI/BFF/vinext、SEC 财报披露、Codex SDK 三阶段研究、optimizer、backtest、shadow、paper、evolution、既有队列和审计。不要新建 Agent 框架、行情采集框架、新闻平台、审批系统、项目管理数据库或第二套账本。

不包含自动交易、付费订阅购买、外部模拟盘写入、实盘、推送发布部署、无限后台调度。普通现有权限只读与现有套餐研究不逐接口/请求/token 重新审批。用户正在考虑的真实库迁移仍等该具体决定。

## 2. 当前状态，不混淆交付层次

- 真实只读刷新、账户对账、组合与市场页面已有运行证据；这不是连续实时行情。
- 新日线原始响应保留了上游延迟布尔值；报价源时间未获证实时保持未知，接收时间不能当交易所时间。
- A 分支 `49535dd` 已完成 capability 版本唯一键及归档重识别测试；未合主线、未应用真实库。
- B 分支已验证 `202 API → general worker_once → 分析方案组`，以及加权历史风险/行业读取；回报测试不等于主线合入或真实方案合格。
- 三阶段研究已有真实运行，但证据和结论质量仍待单案例合格验收。
- 回测、Shadow、内部 paper、evolution、Phase 3A 有代码和历史离线验收；不是待从零开发。
- `quality/source_policy.py` 当前用途 selector 对调整价格、新 proposal Shadow、paper 成交/绩效等拒绝准入。新用途需真实材料与新的不可变 authority 版本，禁止修改旧 v2 或直接把 False 改为 True。
- 实时模块已有 FosunSnapshotProvider 和盘中展示路径，也有 Alpaca 历史实现；复星报价目前 receipt-only/缺源时间，不能因类存在就声称实时。
- SEC 有 Filing/ResearchEvidenceRef/DataSnapshot、6-K/497K 正文，未发现完整实时新闻源。SEC 公告扩展不是新闻全面覆盖。

## 3. 团队与模块所有权

采用四名开发角色加一个协调/复核角色。这是 AI 会话分工，不代表雇佣人数或四倍性能保证。重型测试同一时刻一组，轻量单测和编码可并行。

### A：数据证据与唯一集成人

会话 `01a061bf-12c7-7cb2-b94b-0b696c5ab6db`。现有 worktree `/Users/wangchen/.codex/worktrees/593c/投资`，分支 `edward/nexus-core-evidence-review`。

拥有 Gateway/Fosun Provider、`ops/fosun_local_refresh.py`、`market/`、`quality/source_policy.py`、诊断价格 authority；独占公共模型/schema migration、`worker.py`/公共 registry、共享服务、主线合并。B 拥有 `diagnostics/repository.py`，A 不同时修改它。

### B：研究与方案计算

会话 `01a061cf-face-77b3-ab79-6c1c6fb4919c`。现有 worktree `.worktrees/nexus-display-decimals`，分支 `edward/nexus-display-decimals`；名字虽旧，当前明确归 B，禁止为改名清理已有工作。

拥有 `optimizer/`、`api/routes/optimization.py`、`diagnostics/repository.py` 及相关测试。首轮后接手 `agents/` 中研究质量、comparison 只读工具、以及 `backtest/`/`shadow/`/`paper/` 的局部用途适配；跨模块开始前与 A 明确当前文件，不能同时抢修价格 authority。

### C：前端时间、证据与决策展示

复用会话 `01a05d5c-6d6e-7f81-9975-d8ef2fed9d15`。现有独立 `.worktrees/nexus-ui-refresh`；开始前确认分支、工作区干净并 merge 最新 main。

拥有 `app/(protected)/market/`、研究/方案相关页面、展示组件、`tests/unit/market/`、`tests/unit/optimize/`。复用已合入的统一北京时间和数字格式，不重做视觉设计。后端 market 字段由 A 提供，BFF 仅转发既有受保护接口，不代调用券商或模型。

### D：公开公告与缺口资料证据

复用原计划会话 `01a05d93-2692-73b0-827c-85f32cc2f888`，创建自己的 `.worktrees/nexus-public-evidence` / `edward/nexus-public-evidence`，不在其 app 默认根目录开发。

拥有 `ops/sec_local.py`、`research/sec_disclosures.py`、`research/sec_funds.py`、SEC provider/ingestion 相关路径及测试；`ops/openai_local.py` 的 SEC 子命令由 D 单独维护，B 如需改同文件，先完成交接。不改通用 `agents/snapshots.py`、源政策、数据模型、迁移或私自将 sector/corporate-action 原始事实标为已审核。

### 协调与交付复核

本会话 `01a062a5-269b-7060-ab5f-b294c536a588`：计划、依赖、派发、独立复核和状态汇总。文档 worktree `.worktrees/nexus-team-plan` / `edward/nexus-team-plan`。不再成为第二个 main 集成人。

## 4. 共享合同：先约定最小含义，不先造通用框架

### 4.1 行情时间与新鲜度

保持现有 `MarketItemResponse.as_of` 原义，不通过改名或回填让历史数据看似新鲜。`meta.as_of` 是查询快照时点/数据读取截止：存在cursor时保持游标冻结的decision_time，并非本次HTTP请求时间，更不是所有行情的源时间。真正的页面刷新成功时间若需要，由C独立记录本地成功接收时刻并明确不代表市场事件。现有 `MarketBarResponse.session/observed_at/available_at` 和盘中 `trade_as_of/quote_as_of/bar_as_of` 继续各自展示。

A 如确认有准确存储事实，给 `MarketItemResponse` 兼容增加可空字段：`source_observed_at: datetime | None = None`、`received_at: datetime | None = None`、`persisted_at: datetime | None = None`。只能映射经逐层验证的来源：上游事件时间、原始响应接收、数据库写入。没有该语义的存储值时保持 null，不以 nearby timestamp 代替。先通过合同测试再生成客户端；C 在新字段未合入前用现有字段实施可独立合入的诚实标签。

`delivery_mode=unknown|real_time|delayed` 保持不变；延迟 true 不推断分钟数。源时间缺失显示“源时间未提供”；新鲜度依据实际 quote/bar 时刻和既有交易时段政策判断，不能按网页刷新时间判定。轮询只是 GET 已持久化内容；配置 15 秒不保证数据源 15 秒更新。

### 4.2 方案生成与状态

B 保留现有请求/响应、202 异步语义及 Job ID。输出 `build_optimization_registry(*, artifact_reader=None).specs`，A 在现有 general registry 注册，不新建 worker。对象读取复用 `app.state.diagnostic_artifact_reader` 的只读 transport，不能把对象库密钥放 Job payload。

分析方案组不等于合格排名、用户选择或执行许可。所有候选绑定同一组合、估值基线、研究快照、价格/行业/FX 版本与可用时点。既有 `agents/proposal_context.py` 已允许同case的分析草案以 `partial/non_executable` 冻结并由研究工具只读消费；保留此能力，不为本计划删掉或重复实现。正式历史排名仍仅纳入合格比较；跨组合、未来证据、草案/分析组不得冒充正式结果。

成本“未知”和“确为零”分开。当前 `candidate.cost <= hold.cost=0` 不能作为经济上不调整的充分理由。没有有来源的适用费用、比较目标与预算时，只显示各维度/缺口，不宣称持有更优，不擅自新增阈值或风险容忍度。

### 4.3 公告证据

复用 `SecDisclosure`、`persist_disclosure()`、`Filing`、`ResearchEvidenceRef`、`DataSnapshot` 和 SEC 白名单 transport。首个开发切片扩展现有正文链到 8-K，不建 news/RSS 表和通用抓取器。保留 accession、CIK/证券身份、form、accepted_at、filed_at、原始 hash、received_at、截断标记；重修订追加新版本，不能覆盖旧研究冻结输入。

D → A/B 的交付是已有证据 ID/类型/来源/原始哈希/业务日期或时间/实际接收时间/适用限制。原始公告、费用网页或公司行动文字不自动变成结构化可计算事实。基金招募书不当作当前成分；官方披露不当作实时新闻推送。D 完成事实核实与具体映射建议，A 决定来源用途验证，B 决定能计算的研究/比较维度。

SEC 的 `accepted_at` 不是首次网页公开时间；官方说明没有首次可用时间戳。新8-K链保存实际本地 `received_at`，新研究冻结不得把较晚取得的资料倒填至旧截止；已有记录保持原样，不借本任务批量重写旧数据。[SEC时间戳说明](https://www.sec.gov/about/webmaster-frequently-asked-questions)。

### 4.4 新用途与回放兼容

调整历史、公司行动、Shadow、paper 使用新的不可变来源/用途版本；记录价格类型、时点与有效范围。旧 v1/v2、旧研究、旧 Shadow 仍按其原绑定读取。只为当前用例增加最小适配，不为所有未来市场建统一抽象层。历史价格收益、总收益、公司行动处理后的收益名称严格区分。

## 5. 分支、同步与合并协议

1. 开工读根目录最新 AGENTS、自己的子目录规则、此方案及执行计划；检查实际 cwd、branch、dirty diff、其他任务。旧 worktree 不会自动获得根目录未提交 AGENTS，必须显式读主项目那份。
2. 每轮在自己的 clean/checkpointed 分支执行 `git merge main`，不盲目 `git pull`，不 rebase 已共享提交，不 stash/reset 他人的修改。此项目的“拉最新主干”先指本地 main；远端 fetch/pull 必须先确认 remote/upstream，不以远端陈旧分支覆盖本地。
3. 当前切片未完成且 dirty 时先收敛自己可提交的检查点，再同步；不能为同步删除修改。每轮记录 main 基线和自身 HEAD。
4. 每个切片先写/执行能复现缺口的单测，再最小实现、绿测、定向跨层测试、一轮审查。单测不能证明“绝无影响”；合并前还要验证共享合同及相关消费者。
5. 向 A 交付：切片 ID、commit、base main、变更文件、真实测试命令/结果、schema/依赖/配置影响、回滚、未完成验收。等待集成时开发下一个无依赖切片，不能操作共享服务。
6. A 串行集成。优先在提交方吸收最新 main、定向复测后 fast-forward；必要的 merge/cherry-pick由 A 唯一执行并检查补丁是否已存在。冲突由文件 owner 解释解决，禁止 ours/theirs 粗暴覆盖。
7. A 验证 main 包含该补丁，发 main SHA；“已发送”不等于已合入。其他角色下一轮再 merge main。交付状态按“分支实现→分支验证→主线集成→运行生效→真实案例验收”分别记录。
8. 根 AGENTS 的既有未提交改动保留，A 单独审查提交；不把它与业务迁移捆绑。A 当前待确认的 migration 提交不得被合并其他分支顺带应用/视为批准。

## 6. 资源与验证约束

- 每个 worktree 独立 node 构建缓存、Python 测试临时目录；测试只能访问自己的合成库，禁止测试真实账户库。
- 现有共享 3302 服务由现负责人维护；C 新预览选择未占用端口并告知 A，不能重启别人进程。端口/PID/数据库名写在任务上下文，不固化为长期规则。
- 一次只跑一组重型 PG/浏览器/全量构建；由 A 协调时间窗，不禁止其他编码和轻量单测。只读样本采集同样避免两个任务重复读取相同缺口。
- API 生产者 owner 修改自己的 schema；生成产物 `contracts/openapi.json`、`app/lib/generated/*` 由 A 在集成时统一生成核验。C 不手改生成文件；可在本分支运行生成命令验证，但不覆盖他人的合同。
- `engine/src/nexus/main.py` 的依赖注入也由 A 独占；C不碰后端snapshot/diagnostic时点，D不注册并行组合根。需要新字段/路由时先交A集成小补丁。
- 关键测试必须覆盖：未来/错时点、未知与零、单位/分母、重复/幂等、旧版本兼容、越权拒绝、无自动外呼、错误与进程退出。资金/风控用 Decimal 及独立回归。
- 无真实数据时报告具体缺口，不能将隔离样本/测试数/页面 200 宣称真实验收。

## 7. 排期原则与必要决策

执行计划给出估算，不承诺自动按日期上线。首轮结束以实际周期修正排期，不拿测试数估百分比。外部资料覆盖、真实库迁移决定、比较目标/费用预算、人工选择和足够交易日是显式依赖。

待决定事项不阻塞所有开发：真实库 migration 由用户已收到的具体请求处理；经济成本比较标准先形成选项和证据，不擅自替用户选择；持续采集/定时调度、付费数据、外部模拟或实盘另行确认。常规只读和本地实现不再询问。
