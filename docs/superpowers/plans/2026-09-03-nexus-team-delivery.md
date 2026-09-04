# NEXUS 全项目剩余工作与团队迭代 Implementation Plan

> **For agentic workers:** 按 `superpowers:executing-plans` 执行分配给自己的任务；用户已选择多会话团队执行，不重新询问执行方式，不自行再开同模块实现人。必要的独立复核可使用 `superpowers:subagent-driven-development`，但不机械增加多轮审查。

**Goal:** 分模块完成 Mac 本地真实决策闭环及实时信息的最小可信补齐，小切片测试后持续集成主线。

**Architecture:** 保留现有 Gateway → Provider → 数据库/对象存储 → FastAPI/BFF → Web；研究、优化、回测、Shadow 使用已存在模块；共享合同先对齐、四个开发会话在独立 worktree 实现、A 串行集成。

**Tech Stack:** Python 3.12、SQLAlchemy/Alembic/PostgreSQL、FastAPI/Pydantic、Codex SDK、TypeScript/React/vinext、pytest/Vitest/Playwright。

**Spec:** [模块团队技术方案](../specs/2026-09-03-nexus-team-delivery-design.md)。本文件是其执行拆解，2026-09-03 初始排期。

## Global Constraints

- 单用户 Mac 本地应用，`offline_fixture` / `local_read_only`，不自动调度或交易。
- `00656.HK` 只读，`execution_locked=true`；分析/研究/展示不等于排名/选择/执行授权。
- 真实未知不变已知、旧证据不回填、不为验收调松风控。
- 一个 worktree 一个写入者；一个共享文件一个 owner；A 是唯一 main 集成人。
- 每轮先同步最新本地 main；每个功能切片包含回归测试及定向验证。
- 无新 Agent/采集/治理框架；默认不增加依赖、服务或数据库表。
- 普通已有权限只读/套餐研究无需逐次审批；本地迁移按实际影响遵循 AGENTS，付费/权限/外部写入等保持具体边界。

## 1. 工作总账：范围完整不等于本轮全部启用

状态缩写：进行中=分支实现/验证；待做=当前目标的代码或验收；待依赖=可开发但真实启用条件不足；后续门禁=列入剩余范围、不擅自激活。

下表保留初始排期口径；接续执行以第 9.3 节的依赖分类及对应后续专题交付记录为准，不根据初始“待做”重复实现。T1 的正常组包/日历见 9.14–9.15，S1 查询生产者及 B3 正常研究链见 9.13；仍须区分代码交付与真实来源验收。

| ID | 剩余交付 | Owner | 状态与依赖 | 估算开发人日 |
| --- | --- | --- | --- | --- |
| A1 | capability 版本、已有归档重识别、时点正确的新诊断 | A | 已完成：0034、12归档、11合格序列，新18项诊断DB/API/页面核验；A2缺口保留 | 0.5–1.5 |
| A2 | 调整价格/公司行动/行业来源适用性与新 authority | A，D供证据 | 待做；不修改旧 v2 | 1.5–3 |
| A3 | 现有实时路径的源时间/延迟/一次刷新验证 | A，C展示 | 待做；真实源可用字段制约 | 0.5–1.5 |
| B1 | 202→Worker→分析group、风险重算、历史比较接线 | B，A注册 | 分支已测；待审查和集成 | 0.5–1 |
| B2 | 适用成本、合法比较与有充分证据的不调整 | B，D供证据 | 缺真实成本及经济判据，不能凭零成本排序 | 1–2 |
| B3 | 单案例研究质量、comparison工具与正式结果 | B | 依赖A1/A2、B1/B2必要输入 | 0.5–1.5 |
| C1 | 网页刷新/接收/源时间分离、未知和过期提示 | C，A供字段 | 可立即独立开发；不重复格式美化 | 0.5–1 |
| C2 | 方案/研究/公告证据状态与人工选择页面接线 | C | API合同稳定后并行，不等待全后端 | 0.5–1.5 |
| D1 | SEC 8-K正文最小公告链及旧披露兼容 | D | 可立即开发，复用已有数据模型 | 0.5–1.5 |
| D2 | 公司行动/行业/ETF与费用适用性原始资料 | D | 只补明确缺口，资料不足给结论 | 1–2 |
| V1 | 同一真实案例页面/API/DB联合验收 | A牵头，B/C/D | 依赖前述必要输入；不是重写E2E框架 | 0.5–1 |
| T1 | baseline真实回测一次性入口与数据集 | B，A/D供证据 | 引擎已有，普通首次运行接线待补 | 3–5 |
| S1 | 新用途authority下Shadow单次手动闭环 | B，A供authority | 现有模块适配；依赖新authority与人工选择 | 3–5 |
| S2 | 内部paper admission/执行一次性入口 | B，A | 服务已有，正常调用入口与成交authority缺失 | 4–7 |
| S3 | 内部paper绩效/归因真实案例 | B，C展示 | 依赖S2；不另建绩效引擎 | 1–2 |
| E1 | evolution真实模型运行适配与最小家族 | B，A | 后置；缺调整价格/运行时兼容，非新策略框架 | 3–5 |
| L1 | 既有每日DAG按需接线与显式opt-in | A，B | 缺诊断/研究/方案等正常组合根注入 | 3–5 |
| O1 | 本地启动/版本/业务备份恢复与手册收口 | A，协调复核 | 现有恢复仅synthetic；真实恢复只到隔离目标 | 2–4 |
| R1 | 持续采集与故障/恢复观察 | A，C | 自动调度启用待明确；人工单次可先验 | 0.5–1，加观察时长 |
| X1 | 全面新闻源/预期/舆情 | D候选 | 来源覆盖/合法留存/成本尚需验证 | 先0.5–1调研，开发不虚估 |
| X2 | 正式券商模拟真实adapter | A牵头，D核对协议 | Phase3A复用；具体产品阶段确认后开发 | 5–8，条件估算 |
| X3 | 正式模拟单批外部验收 | A，C | 依赖X2、账户/批次/写入批准 | 2–3，加外部等待 |
| X4 | 实盘/部署等未来激活 | 不启动 | 不属于当前实施授权，实盘逐笔确认 | 不作无依据估算 |

表中为每包粗估，不直接相加当作承诺：A2与后续用途适配、O1/R1存在共享工作，首轮后去重重估。AI 会话并不等于人类人日：该数用于切片大小/容量比较，不作为收费或精确耗时承诺。当前核心闭环+时间/公告增量优先，后续真实回测、paper、evolution和外部模拟单列容量，不能承诺整个平台7天全部完成。

## 2. 排期与人员容量

D0=2026-09-03，D+n 为有效开发日而非自动承诺的自然日；首轮完成后按实测重估。

| 迭代 | 计划窗口 | A | B | C | D | 可交付结果 |
| --- | --- | --- | --- | --- | --- | --- |
| W0 | D0，立即 | A1收尾/共享合同 | B1审查提交 | C1现有字段标签与测试 | D1身份/8-K合同与测试 | 四线开工，第一批独立小提交 |
| W1 | D0–D2 | A1/A3 + 集成 | B1/B2成本缺口分类 | C1/研究方案读取状态 | D1/D2优先阻塞材料 | 最新主线页面诚实显示时间/缺口，方案入口实际可跑 |
| W2 | D2–D4 | A2与新诊断 | B2/B3研究比较闭环 | C2真实页面验收 | D2供证据/修订兼容 | 条件满足时一个真实案例闭环；否则列精确外部依赖 |
| W3 | D4–D7 | 核心闭环验收/集成 | 核心缺口修复 | 同案例浏览器验收 | 核心资料缺口修复 | 当前真实决策闭环、行情时间与官方公告最小交付 |
| W4 | 核心完成后约1–2周有效开发 | T1/S1用途authority | T1与S1按就绪顺序排，不并排给同一人 | 结果/归因页面 | 历史数据/公司行动证据 | baseline真实回测及人工选择后Shadow单次闭环 |
| W5 | W4后约2–3周有效开发 | S2/L1/O1分批 | S2→S3；之后E1 | paper/演化/运维可见状态 | 必要证据修订 | 内部paper与绩效；每日流程/演化按准入启用 |
| 外部模拟 | 用户决定进入该阶段后约1–2周开发及验收窗口 | X2→X3 | 相关合同复核 | 模拟状态界面 | 官方模拟协议核查 | 正式模拟账户闭环，不等于实盘 |
| 运行观察 | 对应模块启用后 | R1/故障核查 | 归因结果复核 | 时间/失效展示 | 必要资料修订 | 至少5实际交易日稳定性观察；长期晋级协议另计 |

默认四名开发角色同时存在，A 留约三分之一容量处理合同、集成与真实验收；本任务承担协调和文档/交付复核。不要把同一集成人排成全天开发又全天合并。没有待审切片时不单开常驻 QA 会话；每个 owner 自带单测，集成复核对准风险。

当前真正可能要求用户输入：B2 没有现有政策可复用时的经济比较目标/成本预算、S1 人工选择、R1 自动调度及 X1 激活。A1 曾误加的一概迁移审批已溯源撤回，不作为后续执行条件。

## 3. 每个切片的固定执行步骤

以下步骤适用于每一任务，不能以任务旧代码存在跳过新增行为的测试。

- [ ] 记录实际 worktree、branch、HEAD、main 基线、owned paths；先读主项目最新 AGENTS。
- [ ] 检查自己的未提交修改；完成自有检查点后 `git merge main`；不 reset/rebase 他人历史。
- [ ] 添加本切片下列具体失败场景测试，先运行观察真实失败；若既有测试已覆盖，增加真实断点的回归。
- [ ] 实施满足测试的最小修改；不添加未使用抽象、占位零值或新框架。
- [ ] 运行下述聚焦命令，修正失败；涉及数据库/API/页面时补真实调用路径验证。
- [ ] 一轮集中审查、定向修复、`git diff --check`，仅暂存自己文件后提交。
- [ ] 向 A 交付提交+基线+测试证据+影响/回滚+未完成项，合入后验证 main 包含，下一轮重新同步。

常用命令（在各自 worktree；无需每个切片全量跑）：

```sh
git status --short --branch
git rev-parse HEAD main
git merge main
uv run --package nexus-engine pytest engine/tests/unit/optimizer -q --import-mode=importlib
npm run test:unit -- tests/unit/market/page.test.tsx
npm run typecheck
npm run check:generated
git diff --check
```

上述 optimizer/market 命令仅是对应模块的实际入口；各任务以下指定自己路径。PG 测试必须由 owner 配置独立合成数据库后执行，不能复制真实配置连接测试。Gateway 使用 README 的 offline socket guard。完整 lint/build/PG集由 A 在需要的集成批次运行一次，不四人同时全量执行。

## 4. A 数据与集成：可开发细节

### A1：版本化 capability 与新证据时点

**Files:** A 现有 `engine/alembic/versions/0034_provider_capability_versions.py`、`persistence/models/data.py`、`market/fosun_delivery.py`、`ops/fosun_local_refresh.py`；对应 `engine/tests/unit/ops/test_fosun_local_refresh.py` 与 `engine/tests/integration/test_fosun_local_refresh.py`，新增测试沿 A 分支实际路径。

**Consumes:** 已保存原始/规范化对象及哈希、现有 capability 和账户快照。**Produces:** 追加的新 capability/manifest/evidence 引用，时点真实的新诊断；不重标旧证据。

**实现:** 以 A 已审查的 `49535dd` 为起点，不再重做。版本唯一键含 `created_at`；读取选当前请求时点之前最新有效版本。重识别验证 raw/normalized hash、证券、OHLCV、上游 delay 原字段。用已存材料，不能只因旧诊断截止不适用而重复取数；若必须新快照只走原入口。

**测试:** 旧unknown与新delayed共存；跨证券/哈希/OHLCV错配拒绝；未来版本不被旧时点选中；第二次重识别不重复；旧binding原样可读。升级无旧行修改；没有历史版本时可降级，有新增版本时事务拒绝降级且不丢数据。

**验收/回滚:** 先检查实际数据与权限影响，完成隔离验证及本地备份，再按既有实施范围迁移、读回与案例核对；实际涉及权限、破坏性操作或目标扩展时按 AGENTS 确认。应用可回退，已追加事实保留，不能删历史来强制 schema downgrade。

### A2：按用途合格的历史与数据输入

**Files:** `quality/source_policy.py`、`agents/price_authority.py`、`diagnostics/series_authority.py`、`diagnostics/market_risk.py`、`market/price_bar_authority.py` 及对应现有单元/PG测试。

**Consumes:** D2 的原始证据/映射建议、A1版本、现有价格序列。**Produces:** 可追溯的研究/诊断输入；后续用途按新不可变policy而非复用display资格。

**实现:** 先明确本案例指标需要 raw价格、调整价格、公司行动还是总收益；确认原始来源能证明哪些字段，再加入用途版本和已有读取器适配。行业需身份/有效时间/证据一致，ETF穿透缺失不得用发行商行业替代。每个用途一个实际调用路径，禁止泛化所有市场。

**测试/验收:** 公司行动日前后收益、有效时间、混源/混复权拒绝、缺交易日/样本不足、旧v1/v2只读兼容。命令 `uv run --package nexus-engine pytest engine/tests/unit/diagnostics engine/tests/unit/market -q --import-mode=importlib`；另按新增用途跑对应PG测试。同一案例风险可计算项有结果，未知项保留原因。

### A3：盘中真实能力与最小时间合同

**Files:** `providers/market/fosun.py`、`market/intraday_ingestion.py`、`market/service.py`、`api/schemas/market.py`；对应 provider/market/schema 测试。

**实现:** 逐层核对 FosunSnapshotProvider 到存储及 API 的源时间、receipt-only 标记、delivery_mode；按方案4.1仅添加能可靠映射的可空字段。复用现有单次采集入口与盘中模块；不新建常驻服务，不默认激活15秒外部采集。

**测试:** 无上游时间时source_observed_at=null，页面读取不触发Provider；上游delay布尔值不转换成臆造分钟数；未来/乱序时刻拒绝；新查询的读取截止可推进但行情原时刻不变，带cursor的查询快照仍冻结；历史响应缺新字段兼容。

**验收:** 一次真实只读样本经持久化/受保护API/页面对齐；若上游没有源时间，交付明确限制，不能承诺真实行情时延。C可先按旧字段标签独立合入。

## 5. B 研究与方案：可开发细节

### B1：实际分析构建路径与按方案权重计算

**Files:** `optimizer/{construction,jobs,initial_draft,production,compare}.py`、`api/routes/optimization.py`、`diagnostics/repository.py`；B现有 `engine/tests/integration/test_proposal_construction_path.py`、`engine/tests/unit/optimizer/test_scenario_risk.py` 与既有方案回归。

**Consumes/Produces:** 保留202 + Job ID；实现 `build_optimization_registry(*, artifact_reader=None).specs`，由A集成 `worker._general_registry`。不要修改同一registry。

**实现:** 收尾现有分支，不重复写 builder。复用 `diagnostics.metrics`，按各方案权重重新计算波动/回撤/相关性和有效行业暴露；对象库读取走同一只读 artifact_reader。诊断history只纳入已有严格合格compare，分析组和草案不升级资格。

**测试:** 草案入口→202→通用worker_once→group（不手插admission）；减仓改变风险/行业；未来行业、未准入行业仍unknown；跨组合、错时点、hash错配拒绝；重复job不重复group；分析产生后执行锁/排名边界不变；原正式方案历史兼容。

**验收:** A注册后在隔离PG通过真实通用Worker调用；不能仅测试直接调helper。`uv run --package nexus-engine pytest engine/tests/unit/optimizer engine/tests/integration/test_proposal_construction_path.py -q --import-mode=importlib`。

### B2：成本事实与比较语义

**Files:** `optimizer/cost_policy.py`、`optimizer/compare.py`、`optimizer/models.py`、既有成本/比较测试；公共policy持久化改动由A处理。

**实现:** 按D证据区分commission、spread、slippage、tax及适用范围，未知不得以0填充；历史backtest的5/10bps是假设不是当前实际收费。首先在输出/原因中明确“经济比较依据不足”，避免形式排除变成有证据的不调整。存在已批准比较policy才落地对应判断；没有时提交一页具体决策选项，不自行设置预算或删掉风控。

**测试向量:** hold cost=0、candidate cost>0且风险改善，缺经济判据→不可声称hold最优；缺费用→cost unknown；真实免佣只代表commission=0，slippage/tax仍独立；已知成本超预算拒绝；成本币种/分母错配拒绝。测试以现有 ComparableProposal/TypedMetric factory 写入现有比较测试，不发明新业务对象。

**验收:** 页面能说明风险变化及已知/未知成本，不再把无法比较当成经济上更优。最终排名等待明确policy和数据，不让此决策阻塞B1/C1/D1。

### B3：一个真实研究与比较消费

**Files:** `agents/{committee,synthesis,database_tools,execution,proposal_context,portfolio_context}.py` 与既有阶段/证据测试；若需改SEC CLI，先向D交接该文件。

**实现:** `proposal_context`→`portfolio_context`冻结→`database_tools.proposal_comparison`已实现并有PG回归，不从零接tool。保留同case分析草案以partial/non_executable作为研究上下文的现有能力，与严格合格正式排名分开。用A新快照的一项组合问题，冻结同批证据；检查反证确实回应论点、综合解释未解决矛盾，以及compare的研究/证据/hash/时间绑定。常规模型验证复用现有套餐，不反复对同一缺失材料烧额度。

**测试/验收:** 错引用、时间漂移、跨研究compare、矛盾未回应、阶段失败和未知用量、超时后进程清理；读工具无隐式创建。真实验收报告必须能回答原问题，不要求买入/调整；缺材料不是证据充分不调整。

### B3 v3：结构化最终审查（2026-09-04，B负责）

**目标/边界：** 新增`codex-sdk-chatgpt-v3`，复用三阶段、八角色、native run与
`ResearchIntent v1`。只替换最终synthesis响应合同，不新增执行器、审批或模型调用。
A统一进行真实调用与运行验收。root的optimizer/research_context/UI不在本片范围。

**第一片（core，<800行）：**
- `strategy/intents.py`新增`SynthesisClaimReview`、`SynthesisAssessment`：stance沿用
  buy_candidate/reduce_candidate/hold/watch；逐条review含claim_id、accepted/rejected/
  unresolved、理由、冻结evidence_refs、显式resolved、missing_evidence和prevented_conclusion。
  accepted不是resolved；未解决必须说清缺什么及阻止什么结论；sufficient不得含未解决项。
- `CommitteeRoleOutput`及`ResearchIntent`可选`synthesis_assessment`，None从序列化中
  仅省略该新字段，旧payload/hash不变，不回填历史。
- `agents/codex_staged.py`新增结构化committee子类复用现有生命周期；前两阶段不变，
  最后一阶段schema使用`synthesis_assessment`，reviews按实际全部bear/evidence_audit
  claim IDs精确覆盖（不只每角色1条），所有引用绑定原冻结evidence索引。
  本地解析必须调用本次请求实际`output_model.model_validate_json`；保留既有安全失败分类。
- `agents/synthesis.py`保留综合正文、逐条评估与反证，exposure_action只由确定性映射
  产生。模型自报审查充分不替代现有价格/财务等确定性准入；当前没有完整复用准入，
  v3仍watch/none/partial，保留评估内容和明确不充分原因，不以新版本解锁结论。
- 红测：遗漏第二条反证、重复/未知review ID、越界引用、accepted但未解决、
  sufficient与未解决项矛盾、实际动态schema本地解析、v2 None序列化原样、
  模型buy/hold表态不能覆盖价格缺口及目前完整准入缺失。聚焦unit后集中独立审查。

**第二片（版本/运行接线，<800行）：** 第一片交A后再做版本配置、worker选择、持久化
与trace；worker先同A确认，daily_once所需最小版本识别diff交root。无真实模型重跑，
由A统一决定一次新输入验收。每片报告代码/测试/集成/运行区别；不修改v2历史读写语义。
- 接线文件：`agents/{codex_sdk,persistence}.py`、`ops/openai_local.py`、`worker.py`、
  `api/{research_stages,routes/research,schemas/agents}.py`；不写A的issuer输入或root的daily_once。
- 通过显式`--structured-synthesis`注册v3，原`--staged`仍登记v2，默认admission排除v3。
  非SDK runtime拒绝订阅版配置，Job最多一次尝试，deadline沿用三阶段绝对截止。
- native worker→隔离PG→受保护API回归覆盖成功、critique超时、非法引用、旧v1/v2；
  trace逐条重验冻结引用与全量review，API保留评估正文；无新表或迁移。


## 6. C 前端：立即可开工的增量

### C1：行情时间与新鲜度

**Files:** `app/(protected)/market/page.tsx`、相关既有市场组件/格式函数、`tests/unit/market/{page,intraday-monitor,market-boundary}.test.tsx`、`tests/unit/engine/market-routes.test.ts`。

**第一小切片:** 不等待A新schema。复用现有 item.as_of、meta.as_of、盘中quote/bar时间，将“查询快照时点/数据读取截止”与行情记录时间分别标注；cursor存在时meta.as_of冻结，不能标成最新HTTP读取时间。确需显示本次页面刷新成功时刻，独立记录本地成功接收并注明非行情时间。源时间不明给精确说明；复用既有实时/延迟标签、轮询和北京时间格式，不再实现一套时间格式库。

**第二小切片:** A新可空字段合同合入后，按source/received/persisted分别展示，null不显示假时间；过期判断复用既有quote/bar与交易时段政策，不在周末/盘外仅凭墙钟制造误报。旧字段原含义不变。

**测试向量:** 无cursor新查询meta推进而行情不变；带cursor快照冻结但本次请求完成时间推进；delivery unknown；源时间null；历史响应缺字段；休市/过期/未来时刻；切换标的不显示上个标的证据；隐藏页轮询；GET未触发券商/模型。先增加失败断言，再改标签/mapper。

**验证:** `npm run test:unit -- tests/unit/market tests/unit/engine/market-routes.test.ts`、`npm run typecheck`、目标文件lint；使用自己端口实际浏览器验证，指出哪些后端字段尚未部署。

### C2：研究/方案/公告状态

**Files:** 既有研究页与组件、`app/(protected)/optimize/`、`tests/unit/optimize/proposal-comparison.test.tsx`；不新增一套独立新闻站点。

**实现:** 分开分析组/正式比较/人工选择/执行锁；展示风险计算时点、费用未知原因、对应证据；SEC8-K复用研究正文/来源抽屉。合同未增加时先做既有字段的可测试映射，不手工修改generated文件。

**测试/验收:** 202轮询job后的分析状态；不可排名不能显示推荐；空历史和旧版本兼容；人工选择确认不可由读取触发；公告文本作为文本展示不执行HTML/提示注入。浏览器→BFF→DB同案例核对。

2026-09-04 展示语义补验：全局 integration `as_of` 来自最近成功核验，非行情/账户快照截止；fixture 单列“样例时间”。证据抽屉明确研究摘录不等于来源原文；诊断 `NO_ACTION` 只表示未触发优化条件，不推断经济上保持现状更优或历史是否已做比较。策略 gate 按生产者的具体字段区分收益/回撤水平（百分比）与差值（百分点），保留夏普等无量纲数值。新固定码 `model_stage_output_invalid` 仅标明内容校验未通过，不推断JSON/引用故障。先红回归后修复，132项聚焦单测、TypeScript、目标lint通过；4项独立端口浏览器回归通过，含390/1440研究抽屉与严重可访问性检查，审查后NO_ACTION文案再次红绿及定向浏览器通过。此轮浏览器使用合成响应，不替代真实数据库同案例验收；无合同、数据精度或交易权限变化。

发行人分类/持仓读侧下一片（root，`edward/nexus-fund-observation-readside`）：已入库0038材料目前仅研究工具读取，复用`read_fund_observations`为`ResearchInputsSummary`添加默认空的可选`issuer_fund_observations`集合（最多一条）。只从指定snapshot/instrument精确匹配全部Frozen引用字段、证据可用与创建时间，再调用canonical reader验证内容hash/原件receipt；不发现latest替代、不外呼、不改变旧研究。分类最多50项、持仓最多100项，保留原次序、完整计数及独立truncated；权重是原始percent数值字符串，非ratio，不归一化或计算现金余量。持仓摘要只投影名称/ticker/原分类/权重/原币种；完整资料由两份官方原件定位。附原观察日期、分别收到时间、实际可用时间、两原件hash和来源URL；发布时刻/有效期/分母/余项分类保持null，两项用途资格保持false。非法可选资料不遮挡合法SEC/分红。

接线文件：`api/research_inputs.py`、`api/schemas/agents.py`、生成客户端、既有`ResearchInputs`及研究空态文案；独立窄组件复用当前表格/展开样式。先PG/API红例验证新旧混合、无SEC仅发行人、晚到/错instrument/错冻结引用/篡改hash排除、旧快照不被补入；界面覆盖0/负值/小非零不显示0、百分数不再乘100、null与截断、恶意文本纯文本、旧字段缺省、移动端键盘/溢出。合入后A用已入库同一真实冻结集合验DB/BFF/DOM，无需模型重跑。无迁移或业务写入；回退应用即可，原材料/旧研究不变。NAV仍由A独立核对，不混入此合同。

读侧验证：原4项PG和旧summary默认字段先红；新旧9 PG通过。独立审查发现canonical合法的极小Decimal超出40字符wire限制时会连带SEC失败，新增第5项PG先红，再在可选投影边界捕获预期转换异常；5项新PG复验通过，不放宽canonical或数值精度限制。2项API单测、92项前端/真实BFF边界单测、Mypy2、Ruff、TypeScript、eslint及生成合同检查通过。390/1440浏览器核验展开/负值与小非零/零写/严重可访问性/无页面溢出；手机实际键盘横滚已验证。原始长名称把桌面权重挤出视野，补精确可视断言先红，局部文本单元格换行修正，不裁切名称。API与UI独立集中审查及定向修复ready；真实业务同案例仍由A集成生效后验，不以合成浏览器替代。

NAV资料可见性接续（root，`edward/nexus-nav-history-readside`，已同步A/B的`aead83f`）：复用新`ssga_nav_storage.read_nav_histories`与`nav_tool_payload`，同一精确Frozen选择器；summary新增默认空`issuer_nav_histories`（最多一份），只展示同一规范视图最近32项观察及完整覆盖起止/总数/截断。保留观察日期（包括非交易日），NAV用USD/份，流通份额与基金净资产独立可空；Decimal转wire不改变值，超限可选资料局部不可用，不掩盖SEC。双原件URL/hash/receipt、actual available、币种来源均投影；发布/有效期/修订/拆分/再投资口径保持null，市场价格/总收益/历史回测资格false。UI复用既有展开与表格，明确非成交价、非总收益、不代表过去已知，不计算收益或生成交易。

该片Files限定已有API summary/reader/generated、独立NAV小组件、研究空态和测试；不改A的存储/0039/CLI/模型工具。TDD覆盖仅NAV/与SEC混合、冻结前后/错标的/旧快照不污染、原始hash篡改/超wire拒绝、32条截断与非交易日保留、科学计数精确转字符串、0与null区分、旧字段缺省；实测BFF闭合同、桌面/手机展开和数值单位。A先完成当前批运行与fundobs真实验收，再用正常准入冻结新NAV资料（不必调用模型）核对同一新snapshot DB/BFF/DOM。无新增依赖/表/迁移/外呼，回退读侧应用不改已存NAV。

NAV读侧切片验证：新增4项真实PG路径红测复现无NAV字段/仅NAV返回空，接线后与旧fund 5项共9项通过；API旧合同与NAV parser 33项、research UI/BFF 101项、typecheck、定向eslint、Ruff、Mypy两文件和generatedcheck通过。390/1440两项浏览器验证展开、键盘横向滚动、三位/小非零、无页面溢出、无业务POST与无严重可访问性问题；合成截图已目视检查。后端与UI独立审查均无实质P1/P2。此处只记录代码/隔离验收，运行与真实新snapshot验收由A串行执行后记入现有checkpoint，不能据此改变旧failed研究或正式优化资格。

## 7. D 公告与数据缺口：立即可开工

### D1：8-K官方公告正文复用

**Files:** `research/sec_disclosures.py`、`ops/sec_local.py`、`ops/openai_local.py`（仅SEC命令段）、SEC provider/ingestion相关文件；`engine/tests/unit/research/test_sec_disclosures.py` 和 `engine/tests/integration/test_research_snapshot_preparation.py`，补充局部CLI/parser测试。

**Contract:** `SecDisclosure.form` 兼容接受8-K/6-K/497K；原有 `SEC_RESEARCH_FORMS` 已有8-K，但正文模型不接受，必须贯通读取/冻结而非只改Literal。复用 `persist_disclosure()` 与当前SEC正文入口，不新建表。8-K的身份核验等同证券CIK绑定，不使用基金专用identity。

**实现:** 先通过现有transport获取或复用官方公开样本核对字段；扩展现有单标的正文入口选择/验证8-K，默认选择确定的最近accepted filing并保留原始form/accession。不把修订件写回原件；遇到8-K/A先明确与原件关系和现有枚举兼容，未支持时显式拒绝，不去掉后缀伪装8-K。

**测试向量:** 同一8-K重复幂等；错误CIK/accession/路径拒绝；accepted/received未来错配；隐藏HTML/脚本移除；正文过大/过短拒绝；6-K/497K身份原规则不变；旧冻结hash不变；读取页面不采集/调用模型。

最小真实函数回归示例（其余身份/时点沿现有SecDisclosure fixture扩展）：

```python
from nexus.research.sec_disclosures import disclosure_excerpt

def test_disclosure_script_is_not_research_text():
    text, truncated = disclosure_excerpt(
        b"<html><script>DO_NOT_EXECUTE</script><p>Public operating update for investors.</p></html>"
    )
    assert "DO_NOT_EXECUTE" not in text
    assert "Public operating update" in text
    assert truncated is False
```

该已有能力测试是兼容基线，不冒充8-K新功能测试；新功能必须另外让8-K冻结/读取全路径先失败后通过。新公告用途不将SEC接受时刻伪装首次公开时刻，新研究以实际取得证据为截止约束，旧资料不倒填。

**验收:** 一份真实官方8-K经原始归档→现有表→新冻结研究→受保护API能读取，C页面复用展示；旧6-K/497K回归通过。不输出私人账户事实到测试或聊天。

### D2：阻塞材料优先，新闻覆盖不虚报

**Priority:** 先公司行动/调整口径、行业及ETF身份/成分有效期、适用费用，再评估其他实时新闻来源。先查本地已有档案，缺时查官方公开源；不购买订阅，不传真实账户到第三方。

**Deliverable:** 对每一实际缺口交付来源URL/原始hash或既有证据ID、证券身份、时间、版本、精确适用范围和可提取字段。若原模型已能承载则复用；否则给A最小结构变更需求，不自建平行事实库。原始事实不能擅自改成reviewed或交易资格。

**测试:** 源解析的币种/单位/适用期、同名不同证券、ETF穿透缺失、费用零/未知、修订幂等；固定合成片段单测，真实资料只在已授权受保护本地存储中验收。

**后续范围:** 全市场突发新闻、分析师预期、社交舆情仍是X1，不以SEC公告完成冒充已实现；需先确定实际需求/合法来源/成本再给独立可估算切片。

## 8. 后续工作包：有入口、有触发条件，不提前重写

### V1 同案例集成验收

- 复用 `tests/e2e/`、`scripts/e2e/run-real-fixture.mjs` 和现有API/PG测试，不建新的验收框架。
- A提供同一case的快照/证据版本，B确认诊断/研究/方案基线，C核对页面，D核对原始来源；只在本地受保护证据中保存账号明细。
- 必验：source/delivery/时间、FX/分母、三阶段回应、风险重算、费用缺口、旧历史兼容、执行锁、所有GET无外呼。运行fixture E2E与真实只读验收分开报告。

### T1 baseline回测正常入口

- 已有 `run_backtest` / `persist_backtest_result` 的正常业务调用集中在evolution，不能认定普通baseline首次运行已接好。复用 `backtest/{engine,authority_loader,persistence,costs,metrics}.py`，A提供当前authority，B补现有job/CLI的一次性调用，不加第二回测引擎。
- 输入明确：策略版本、冻结universe、数据及政策版本、起止交易日、基准、成本假设。使用真实合格历史；没有退市/历史股票池证据时不把当前持仓名单当历史universe。
- 测试 `engine/tests/integration/test_backtest_authority_loader_pg.py`、`test_backtest_writer.py`，覆盖未来材料、混源、公司行动、重复run幂等及基准一致。输出持久化run与现有页面可读取结果。
- 完整晋级协议的多年历史/OOS窗口是后续资格，不因短期数据能画曲线就冒充达标，也不以其缺失阻塞当前组合诊断。

### S1 proposal Shadow

2026-09-04 接续核对：现有人工确认启动、单日推进服务及历史GET/BFF/页面均已存在；当前新用途价格判断关闭，reader仍限定旧policy，`shadow_source_query_v1`缺正常生产入口，不能只新增一个必败CLI或开启旧门禁。root下一最小修复是让方案GET的`shadow_eligibility`复用POST同一当前用途判断，返回既有固定`SHADOW_PRICE_AUTHORITY_UNAVAILABLE`并显示中文原因。真实PG测试完整历史compared组在当前条件下GET不可启动、POST拒绝且零新Shadow事实；隔离允许历史合同的合法启动、关闭后已存GET与成功请求replay保持。补实际组件/浏览器禁用回归，无新schema/迁移/权限，不标S1闭环完成。owner=root，独立`edward/nexus-shadow-eligibility`，A不改对应GET区域。

本片验证：新增两项真实API/PG先红，确证同组GET曾返回eligible=true而POST拒绝；修复后与原Shadow/API读取共10 PG通过，37项组件/标签单测、Mypy1、Ruff、TypeScript、eslint/diff-check通过。浏览器最初复用了“已有Shadow”fixture，导致页面正确优先显示旧观察状态；修为本例真实条件“尚未启动”后，固定原因可见、无启动按钮、无写请求通过。独立集中审查无P1/P2。无真实Shadow启动或重复外呼。

- 复用 `backtest/{engine,authority_loader,costs,metrics}.py` 和 `shadow/{authority,start,service,query,repository}.py`。
- A2的新用途authority就绪后，B补读取适配；人工选择真实方案前只在隔离合成环境建立session，不代用户选择真实组合动作。
- T1与S1不是全局先后依赖；S1自己的方案/价格用途/人工选择已满足时可先做，不因T1多年历史数据不足而等待。
- 测试 `engine/tests/integration/test_shadow_start_pg.py`、`test_shadow_advance_pg.py`、`engine/tests/property/test_shadow_no_lookahead.py`；补新旧policy兼容、重复推进、缺交易日/公司行动、成本基准与归因一致性。
- 验收是同方案单次手动推进→结果/归因→API/页面，不能以“历史测试绿”宣告当前源已可用。随后至少5实际交易日观察，不作为收益预测。

### S2/S3 内部paper与绩效

- 复用 `paper/{service,execution_service,performance}.py`。`create_paper_order_batch`目前未发现普通业务调用者，B需补策略/决策→admission→batch→现有execute的一次性入口，A接现有registry。
- A新增用途policy后B做回归：展示专用分钟数据不能成交；无合格价格不能给非零仓位估值；重复事件/费用/资金账本一致；旧策略/数据/政策回放相同。
- 顺序为次日允许的成交窗口→成交/手续费→复式账本→持仓投影→日终估值→绩效归因→现有paper页面。S3等待S2数据，C可先依据既有响应合同测试状态。
- 聚焦 `test_paper_order_batch_pg.py`、`test_paper_execution_pg.py`、`test_paper_atomicity_pg.py`、`test_paper_performance_pg.py`；资金/容量、部分成交、未知结果不重放、崩溃回滚、费用和现金守恒必须单独断言。
- 内部模拟执行和策略晋级在隔离测试中验证；真实账户关联的启动、自动调度和任何外部券商写入仍需各自选择/批准。

### E1 evolution真实运行与兼容

- 复用 `evolution/{executor,registry,worker}.py`。`_generation_runtime`目前仅Responses真模型分支，其余可能返回固定运行时；如使用Codex SDK必须显式适配，不能默默fallback fixture。
- 测试模型后端错配拒绝、同源lineage、输出/预算/首错停止、OOS/promotion/rollback。只跑一个有界家族证明链路，不展开新的策略搜索系统，不自动晋级。
- 模型接通不等于策略合格；原有长期Shadow/交易完成数门槛按原协议独立观察，不能用5个交易日的本地稳定性验收替代。

### L1 现有每日DAG接线

- 当前 `jobs/daily_local.py`仅组合paper执行/Shadow推进/绩效/审计及可选Provider，未完整注入质量冻结、诊断、研究、优化、paper admission和简报。复用现有Job/DAG/lease/outbox，不新调度器。
- A/B先实现一次显式手动日流程，再按用户选择启用的模块接handlers；每阶段绑定同一根和证据版本，缺证据跳过相应下游并说明，不重复外呼。
- Codex SDK现有长驻worker拒绝策略不悄悄解除；先保持run-once执行边界，再有具体调度需求才评估适配。
- 回归重复日根、租约过期、重启、跨队列权限、阶段失败/缺证据、外部结果未知禁止自动重放、开关关闭后无新调用。

### O1/R1 本地运行、恢复与观察

L1跨阶段恢复接续（root，`edward/nexus-daily-research-recovery`）：已有独立的daily CLI、native worker失败、旧context和proposal原子性回归，不新增入口。补一条隔离PG通过正常`daily_once`的admit→advance→research-admit→research-run；只在SDK传输边界注入调用后故障，不替换worker/诊断/冻结逻辑。失败终态已持久化后重建配置/会话，重入同root/run，核验SDK累计一次、终态不重领、预算与原快照不重复/不改写、其他队列不动；failed context保持partial/locked，本例下游proposal因输入不齐拒绝且无新group/data/job，不表示所有failed研究均禁止context-only草稿。该验证不是进程强制中断恢复。仅合成数据，无真实模型/网络/业务库；首跑1PG通过、独立审查ready，无生产代码修改。

- A复用 `ops/` 本地CLI、现有 backup/restore工具、`docs/runbooks/{local-development,backup-restore,intraday-market-monitor}.md`。只改本次证实过时的命令/状态/审批说明，不整库重写文档。
- 先读代码与synthetic恢复回归，真实备份若在已授权本地受保护目录中执行不上传；任何恢复只到验证过的空隔离目标，绝不覆盖业务库。需有校验/hash、表/对象对应与停止/回退说明。
- 故障测试：Provider首错停止、Job终态与进程退出分开、旧数据保留、页面正确标stale、未知调用结果不自动重放、应用版本确实加载。
- 连续采集/计划任务作为R1单独激活决定；未创建产品定时机制前不承诺自动长期运行。调度未授权不影响单次刷新和本地实现。

### X1–X4 后续产品门禁

- 全面新闻/舆情：尚无已选合法稳定来源，先来源与成本决策，不能现在编造完整开发排期。
- 正式券商模拟X2：Phase3A状态机复用，`integrations/fosun_gateway/src/nexus_fosun_gateway/simulation_main.py`当前只组fixture transport。用户决定该产品阶段后，核实官方模拟账号和固定操作协议，再写真实adapter与离线contract测试；不先假定Webhook或交易权限。账户类型、参数精度、未知结果不重发、密钥域隔离、主动对账须覆盖。
- X3仅在X2后按用户选定模拟账户/批次做一次外部验收，覆盖能安全验证的正常/拒绝/未知；不为制造异常随意下单，改单撤单必须在授权内，内部paper与券商模拟结果分开。
- 实盘：本期不开发激活；未来权限/资金/风控/逐笔人工审批是独立目标。云部署、多租户、替换Agent框架、一般性治理工程不属于本计划。

## 9. 初始交付状态与更新方法

2026-09-03建立此计划时：A1 `49535dd`已提交未合入/未应用；B1 `9177f32`与A接线已合main `7dd98d8`，不含A1迁移，运行生效另行验证；C1/D1已派发，四开发会话均active。根AGENTS仍有需保留的未提交修改。独立文档review已修正cursor冻结的meta.as_of语义；以上是时点记录，不是自动状态看板。

每个owner更新自己会话，不并发编辑这张总计划。协调人收到A确认的main SHA与验收证据后更新本节或已有 `docs/plans/2026-09-02-core-evidence-checkpoint.md`，避免新增重复状态系统。所有安全且无依赖的小切片继续推进；遇到真实外部阻塞只停受影响项。

### 9.1 持续执行基线（2026-09-04，main `8a58d15`）

用户已明确“除非必要，不要停，按照计划一直推进，直到项目所有任务都完成”。这不是新增交易、部署或付费授权；完成一片后接下一项已授权工作，必要选择只暂停受影响项。以下是该主线的核验快照，不是自动更新的任务状态。

| ID | 已有交付及当前证据 | 仍需推进或保留的边界 |
| --- | --- | --- |
| A1 | 0034 capability 版本及新归档重识别已合入、应用并验收 | 不再重复开发迁移或重标旧记录；新诊断的时点适用性逐例核对 |
| A2 | 原始日线当前可验证；发行人分红有独立归档、冻结及读取链 | 合格复权历史、公司行动/行业用途未闭合；ADV20 的时序、成交量单位须查明 |
| A3 | 源时间与接收时间分离；一次正常刷新已补至最新完成交易日 | 只有延迟原始日线，报价源时间未知；不等于连续实时行情 |
| B1 | 草案→202→general worker→分析组及独立风险窗口已合入、测试 | 合成跨层通过不等于本机真实方案已可完整计算 |
| B2 | 经济比较 API、冻结工具与页面已接线，未知费用不再默认零成本推荐保持 | 适用费用与经济取舍依据未齐，仍不能正式排名或判定持有最优 |
| B3 | SDK 保留证券证据、分开 raw close；真实三阶段已运行，正文发现 P2 后提示已修正 | `2220c83` 尚待合格真实综合输出检验；原 `abstained/conflicting_facts` 记录不改写 |
| C1 | 时间、新鲜度、统一三位小数及未知状态展示已合入并实页核验 | 后续新合同按实际消费者补验，不重做格式美化 |
| C2 | 已有分析/比较缺口、研究阶段、公告和放弃原因展示 | 完整方案选择及后续 Shadow/paper 页面需相应真实数据；不可把展示当执行许可 |
| D1 | 8-K 归档→冻结→SDK/API及修订保护已有回归 | 仅证明这一公告链，不等于全面实时新闻或附件覆盖 |
| D2 | 官方分红已消费；行业/持仓原始资料已有来源核对 | 行业资料未完整接入；复权、成交量/成交额单位、适用费用仍分别核实 |
| V1 | 多个同案例 DB→BFF→DOM 切片已有验收，旧行保持不变 | 总决策闭环及正文质量未合格，不能用局部验收累计成整体完成 |
| T1 | `nexus.ops.backtest_once` 正常入口及查询接线已实现、隔离回归通过 | 真实合格历史、股票池、公司行动、基准及相应冻结集合未齐；不重建入口 |
| S1 | 复用现有 Shadow 模块与离线测试 | 新用途 authority、真实方案人工选择、单次推进和实际交易日观察仍待完成 |
| S2 | 内部 paper 服务与执行状态机已有实现 | 按本计划补普通 admission→batch→execute 接线及回归；外部券商写入不随之启用 |
| S3 | 已有内部 paper 绩效/归因模块 | 等 S2 合格结果后验同案例，不另写绩效引擎 |
| E1 | 不支持模型运行时已明确拒绝，修复静默 fixture 回退 | Codex 真实生成适配、合格数据和单家族验证仍未完成；拒绝回退不算适配完成 |
| L1 | 既有 DAG/Job/lease/outbox 可复用 | 手动日流程的正常组合根仍需按启用模块接线；定时调度是独立选择 |
| O1 | 自包含 DB+引用对象备份及无源存储离线恢复已实际验收 | 覆盖既定本地恢复范围，不扩称权限、密钥、应用切换或异机灾备已验 |
| R1 | 一次刷新、首错停、进程归属清理和旧数据保留已实际核验 | 连续采集与故障/恢复观察未启用，不用单次成功替代持续运行 |
| X1 | SEC 公告和特定官方资料不等于全面新闻源 | 继续保留来源/留存/成本与产品选择依赖，不编造全面覆盖 |
| X2 | 既有 Phase 3A 状态机可复用 | 正式券商模拟适配仍需对应产品阶段、官方协议及账户条件；不直接激活 |
| X3 | 未进行正式模拟外部批次验收 | 等 X2 与用户选择账户/批次/外部写入权限，不以内部 paper 替代 |
| X4 | 原计划列明的未来门禁，未激活 | 实盘逐笔确认、部署等仍需明确要求，不因持续推进自动放开 |

依据：[核心证据与真实案例记录](../../plans/2026-09-02-core-evidence-checkpoint.md)、[baseline 入口](../../runbooks/nexus-baseline-backtest-once.md)、[备份恢复范围](../../runbooks/backup-restore.md)。保护目录中的账户事实不复制到本表。

当前接续顺序：

1. A 用新 raw 证据和已修正指令核验同案例研究；B 独立审正文。先核对真实请求再消耗一次有界调用，保留原限额、首错停止与旧历史。
2. B 核对比较数据实际消费和 ADV20 组装；D 查官方成交量/成交额单位。已有消费正确则不重复加工具；只对证实缺口做最小实现。
3. A/B 衔接研究→分析方案的真实可用部分；复权、费用、行业等缺口分别保留，不能伪造比较资格。
4. 对 T1/S1/S2/S3/E1/L1 按上表逐项核对现有实现与独立可开发部分；无依赖的正常入口与回归继续，不重复已完成 T1/O1，也不把资料等待扩大成全局停止。

### 9.2 第二轮正常入口与模型隔离（2026-09-04，main `fc88e1d`）

- E1 代码部分：Codex 专用运行时、可空真实用量、独立模型配置、显式 setup、一次性 worker、绝对截止及未知结果重领禁止重放已合入。通用研究与fixture/Responses均拒绝误用RND配置；旧v1/v2不被重标。root 已逐文件确认主线包含，具体测试与审查见[对应实施计划](2026-09-04-nexus-evolution-codex-runtime.md)。真实家族仍缺合格历史/用途authority；业务数据库与运行未随代码合入自动切换。
- S2a：B 的普通 `python -m nexus.ops.paper_once admit/advance` 入口已合为 `e630366`，只复用既有决策、风控、批次及执行服务；独立审查和A的24项定向unit/PG通过。无真实账户启动、资金初始化、政策放宽或外部订单。S2b仍缺前向冻结数据装配、首次风险摘要和跨周rank状态的可重放合同；S3仍依赖合格结果。
- 下一独立切片：B 处理现有风险authority首次摘要准备入口（不伪造已有decision/hash）；root 明确L1的手动组合根、同一冻结截止与native研究/方案Job合同；A保持唯一集成并核对B3引用schema修复后的首次有界真实复验。旧每日DAG与native Job同名不同payload，不能简单打开stage开关，也不能另建模型executor绕过生命周期。

其余条目及后续产品门禁仍按9.1保留，未因本轮代码/测试完成改为整体完成。原始价格、未知单位/费用、人工选择与实盘锁定边界不变。
5. 每次集成更新已有 checkpoint；最终逐项检查代码、测试、运行和真实验收，仍有未完成项时不宣告总目标完成。

### 9.3 已交付与剩余真实依赖（2026-09-04，NAV运行 `5b26c76` / DB `0039`）

本节取代9.1/9.2作为接续入口，旧节保留历史。A确认NAV新snapshot的canonical→BFF→DOM逐项一致，390/1440均通过；旧failed研究原cutoff不含新NAV、五类旧业务表hash完全一致、activejobs为0。此处没有把失败研究变为合格结论。具体运行证据继续记入唯一[checkpoint](../../plans/2026-09-02-core-evidence-checkpoint.md)，受保护真实身份/数据不抄入计划。

| 计划项 | 当前可用结果 | 剩余工作 / 启动条件 |
| --- | --- | --- |
| A1、A3、C1 | capability版本、旧归档重识别、单次刷新、实际源时间/收到时间分离已实现并验收；数值最多三位且未知可见 | 当前源仍不能证明实时行情时延；持续采集另属R1，不重复写刷新入口 |
| A2、D2 | 原始成交额等SDK字段留存；官方分红、行业/持仓、NAV原件→规范化→精确冻结→模型只读工具→页面已贯通 | NAV不是成交价/复权价格/总收益；成交量单位、公司行动完整性、PIT复权历史、行业风险映射与适用费用仍须有来源证据。D只做已有材料回顾或确有新来源的有界查证 |
| B1、B2 | 正常分析草稿→Job→worker→group、经济比较与未知费用提示已接线；研究context-only输入/页面/本地proposal-build已贯通 | 真实本例缺有证据减仓条件而拒绝建草稿；不能伪造目标。正式经济排名尚缺费用、价格与经济取舍政策 |
| B3 | v3结构化研究与输入/用量/安全错误保存已接线；一次真实调用失败状态、证据与锁定已验收 | 原始响应未留存，真实失败原因仍未知；不重复同一调用来美化结果。v3当前partial不自动成为complete，仍缺合格研究结论 |
| C2、D1、V1 | SEC公告、分红、行业/持仓、NAV和失败研究均有同案例DB/BFF/DOM验收；当前资料与已有研究不同快照时增加明确提示 | 页面可用不等于正式选择/执行就绪；完整真实决策闭环仍受B2/B3/A2制约 |
| T1 | 既有baseline回测一次性入口及隔离回归完成 | 真实合格历史、PIT股票池、公司行动、基准/冻结输入未齐；不新增另一个必败入口 |
| S1 | 原人工确认/启动/单日推进/历史GET已存在；GET eligibility与POST当前价格用途判断已统一 | 新价格用途authority、正常query producer及具体方案人工选择仍缺。人工选择之前不能自动开始Shadow；后续实际交易日观察不可用合成日期替代 |
| S2、S3 | risk-prepare、paper admit/advance、forward-once、rank连续性、原子checkpoint、冻结特征读取均完成；执行/费用/账本/绩效回归已覆盖 | 真实合格成交/估值价格与公司行动、内部模拟初始资金选择未齐。真实绩效等待真实合格内部模拟结果，不以零账户或人工填充结果验收 |
| E1 | Codex独立模型/用量/一次性worker/截止/未知结果不重放完成，不支持运行时拒绝误回退fixture | 合格历史、OOS、真实家族与长期观察/晋级证据未齐，不调用模型跑假输入证明真实运行 |
| L1 | 单次daily root→冻结→诊断、同root显式研究准入/执行、研究context和草稿入口完成；真实失败与拒绝已核验 | 正式研究到正式优化5b仍缺合法A/R/P绑定、正常selection/admission producer和完整证据；不能只把worker的空research参数替换。新增跨阶段PG证明失败已持久化后重建会话重入不重复调用、预算/快照不漂移、无关队列不动；不是进程强杀恢复证明 |
| O1、R1 | 自包含数据库+全部引用对象备份、离线隔离恢复、实际应用切换及旧事实不变已验收；最近备份174对象 | 不重复整组恢复；持续采集/调度仍需启用选择，对应至少5个实际交易日观察未完成，不称本机备份为异机灾备 |
| X1 | SEC及特定发行商官方资料已有来源和正文链 | 全面新闻/舆情覆盖、合法留存及成本尚未选定，不宣称全面新闻已完成 |
| X2、X3、X4 | 原状态机与门禁保留；本期未激活 | 正式券商模拟adapter/账户条件/外部写入批次待对应产品阶段决定；实盘逐笔确认、部署等不随持续开发自动授权 |

**5b设计约束（B只读核对，尚未实施新框架）：** 研究快照R、优化输入A与资产快照P不是同一对象。若要正式消费后来研究，应显式绑定各自ID/hash/实际可用时刻，区分资产估值时点与决策资料截止；原v1行为/hash不变，context-only v2不自动提升。正常selection必须来自既有baseline evaluator与合格输入，不能从模型文本、当前持仓或客户端布尔旗标构造。该变更还影响持久化/读取/触发器，不宜先局部放宽一个比较条件。P到决策截止的适用/连续性、研究能否跨P复用、研究达到complete的实际证据、成本换风险的经济判据，均须有明确产品政策；缺省继续拒绝，不擅自定数值。

**接续责任：** A唯一集成/运行验收；root维护本计划及小切片实现；B负责正式优化绑定设计；D核对合法现成数据来源。等待某项证据不阻塞独立实现，但也不为维持“有任务在跑”新增无消费者的schema、治理表或重复测试。后续记录必须分别注明代码、测试、集成、运行、真实验收，未满足上表条件的项目仍未完成。

### 9.4 恢复独立开发（2026-09-04）

缺少行情访问只阻塞对应真实价格验收，不阻塞以下现有目标的实现。不得据此将全项目停工或把局部完成算作总体完成。

- A：研究 SDK→domain 的失败证据增加固定安全诊断和响应摘要哈希，保留原始敏感内容不进入日志/聊天；先回归，再根据新增 NAV 与真实输入差异做一次正常套餐内新研究，不重放旧失败。A仍唯一负责主线集成与运行/业务迁移。
- root：独立 `nexus-team-plan` worktree，0040迁移、head常量和隔离PG测试。admission/group各增nullable `decision_cutoff_at`，旧NULL不回填；新schema严格绑定同一P的估值时间、证据截止与实际manifest生成时间；cutoff不可修改，新正式group禁止Shadow。旧数据不变时可降级；有新正式记录必须拒绝降级，不能丢弃证据。
- root子agent：复用B移交的干净 `nexus-formal-producer` worktree，拥有optimizer正式输入/normal producer/worker消费与ORM，B因本任务新用户只读检查不再写入。A=`optimization_formal_inputs_v2` / source=`nexus_optimization_formal`，P.id、估值、cutoff与materialized在顶层冻结；显式绑定R/run/output/hash/实际可用时间，只有精确同P且原研究有效期内可用。旧job payload/hash及context-only行为不变，非法新binding不回退旧草稿。
- selection来自既有baseline evaluator与合格冻结输入，不来自模型或调用者旗标；selection和manifest保留实际生成时间，不回填P时点。真实非空持仓缺rank/episode连续性时明确拒绝；正常流程正例使用完整合成cash-only P，另测不匹配/晚到/未知费用/幂等/原子回滚。费用、真实价格与经济政策缺口不被本片消除，不开放交易。

实施顺序：迁移与producer可独立开发，聚焦红绿回归和一轮独立审查后，由A串行集成；ORM/迁移/消费路径统一后再切换运行，不能仅合入无消费者schema就宣告5b完成。真实验收仍按9.3逐项保留。

正常入口核对后的最小补充：只有显式选择、READY且对账PASS、真实空持仓的组合，才允许为未持有标的冻结现金背景；使用标的实际币种与该币种现金，分母正值才能显示0/1，零/负/未知不伪装成权重。默认研究、不完整组合、非空组合缺该持仓仍保持旧拒绝行为，旧持仓context/hash不变。

新版“正式”仅指A/R/P来源身份被严格绑定，不等于经济判断就绪。正常现金组合诊断可能仍是RESEARCH_REQUIRED，因此只在新schema下保留原诊断并允许只读不完整比较，diagnostic_complete=False/partial=True传至group与全部comparison；NO_ACTION/BLOCKED不转为优化候选，旧v1前提不变。Fosun真实费用无证据时cost_complete=False、成本unknown、不排名、不执行，不拿0bps模拟方法标为真实费用。正例必须区分正常freeze/诊断/producer路径与纯存储合同fixture，不手插不存在的候选诊断来声称全流程可达。

本轮实现/验证交接：正式producer、worker消费、0040/ORM及`formal-proposal-admit/run`一次性入口已在独立worktree完成，尚待A集成与运行验收。集中审查发现并关闭跨请求键返回绑定不一致、晚到现金/诊断子记录越过截止、selection来源列表未精确复核三项问题；正式输入失败不回退旧路径。producer新旧聚焦回归104通过，另合法不同P复用研究反例通过；root联合迁移精确schema升级/降级/升级、0040数据/并发保护、真实CLI→producer→worker及入口单测19通过。全schema检查暴露0037表清单遗漏与新约束长名截断，已补实际不可变触发器校验并统一短名，未改历史迁移。合成正例研究输出是合法存储fixture，不是原生模型完成证明；费用、历史与真实合格研究仍未齐，不宣告5b真实闭环完成。

A的新一次原生研究已独立记录：第一阶段已保存，第二阶段在调用SDK之前由本地阶段预算守卫拒绝，不是等待授权、网络失败或账户额度耗尽；旧研究不重放。普通本地独立实现继续，真实经济/执行门槛不随代码接线而降低。

**研究输入体积修复接续：** 默认JSON空格与SDK紧凑wire计量不一致的小修复已合入，59单测及独立审查通过；真实输入仅省1726字节，仍超限，不能重跑来证明修好。A离线进一步验证现有工具结果的无损表示：仅在节省字节时将同键同键序对象数组改为columns/rows，并把所有工具都存在且JSON类型/值严格相等的六个wrapper字段（schema_version、instrument_id、data_snapshot_id、as_of、evidence_hash、portfolio_scope）提到common_scope。保留每工具status/reason/items、缺失/null区别、行序、单位/时点/来源和全部原始值；有异值或缺键不提取。含格式解释的同critique紧凑wire为35362字节，加原已用及8192余量为62817，仅证明这一阶段静态预检可行，不证明第三阶段/实际累计tokens合格。

由独立wire worktree owner实现staged v2/v3的临时模型视图，原工具输出、冻结事实及其hash、数据库不变；stage request hash覆盖实际投影和解释，额度、余量、调用数、超时与首错停止不变。编码标记碰撞或整体无收益时保留原完整输入，不加依赖、计数框架或新配置。以无损逆变换、类型/缺失/null/异构边界、实际SDK发送与hash、恰好预算及超1字节拒绝、旧v1路径作回归。root维护方案并审查，A负责集成和用生产函数复测真实输入；未经该核验不重复模型调用，旧失败记录不改写。

**本地输入计量接续（与上述无依赖投影分片区分）：** 投影已主线集成并用生产函数离线验证；后续阶段的累计预检仍混合实际tokens与保守字节量。独立隔离验证确认官方tiktoken 0.14.0对现有固定模型映射o200k_base；读取同份SHA-256已验公开词表、复制该固定官方编码配置并使用公开Encoding构造器可完全离线，与官方编码216个合成样本逐token一致。由同一wire owner新增这一项锁定引擎依赖及最小纯本地计量，不改共享虚拟环境、业务库或模型配置。固定模型/库版本不匹配、缺包或词表缺失/损坏时退字节守卫，不猜编码，不调用隐式下载loader或全局monkeypatch。

两处调用前检查共用同一量：实际payload文本tokens（不可用时UTF-8字节）＋实际严格输出schema字节＋实际显式base/developer指令字节＋原8192运行器余量。保留原独立wire字节上限、累计实际64000/8192阈值、最多三次调用、超时与首错停止；该量不宣称为整个SDK请求的精确token数或严格服务端消耗上界。用实际发送的序列化/schema/指令源生成量，不更改请求内容和hash。

公开词表通过现有本地CLI的独立prepare-tokenizer动作显式准备：固定URL、单次有界流式请求、大小/超时/重定向/SHA校验、已知公共缓存原子写入；有效缓存零网络。先于账户配置读取处理，无DB、模型或凭据动作；研究预检只读本地缓存，缺失不联网补齐。owner补离线黄金/特殊标记、失败回退与零联网、严格schema/指令计入、边界及v1/v2/v3/演化回归；root文档与集中审查，A统一装依赖、准备词表并用生产计量离线复核真实请求后才评估一次正常新研究。回滚应用与依赖锁即可恢复旧预检，公共缓存无需删除，旧研究/证据与数据库不变。

**D价格源收尾：** 既有Fosun/Alpaca/Tiingo/Alpha Vantage/Yahoo调查见checkpoint历史，不重试Yahoo限流或拿demo代替真实案例。2026-09-04有界核对[Alpaca官方FAQ](https://docs.alpaca.markets/us/docs/market-data-faq)确认，历史请求end早于现在至少15分钟时存在无订阅查询SIP的条件，不应说必须买实时套餐；[官方bars说明](https://docs.alpaca.markets/us/reference/stockbarsingle-1)明确各adjustment选项，但asof是证券映射日期，不是历史vintage，不能直接宣称历史PIT或精确总收益。日线VWAP和volume可能采用不同交易条件，不能把乘积当已证实精确成交额。项目已有固定主机/分页/归档/归一化adapter，缺的是有效访问和真实样本核验，不是全新框架。D所查项目声明配置与shell无非空Alpaca认证字段；A进一步核对实际API容器环境及正常`load_local_environment`启动入口，两认证字段也均无非空值，没有输出值或搜索其他账户目录。结论仅限本项目已知入口，不能推断用户没有其他账户。若既有有效访问可定位，下一步是一个已结束窗口raw样本及已知公司行动窗口对照，原件/实际单位/时段/调整用途分别验收；无既有访问则需用户决定是否接入第三方账户及接受条款。未注册、付费、扩权或静默切换现有source_policy。

### 9.5 接续基线（2026-09-04，main `062e434`）

本节更新9.3/9.4的当前代码与运行状态，未提及的真实依赖仍保留；详细证据仍只写既有checkpoint。

- 正式A/R/P绑定、正常selection producer、worker及`formal-proposal-admit/run`已主线集成；0040已备份、应用并实际加载，API代码为`85e3f9c`。旧数据不回填、五类历史表hash不变，真实失败研究被正常读取入口拒绝。5b不再缺代码接线，但仍缺合格研究、策略连续性及价格/费用等正式正例条件。
- 无损输入投影与离线token计量已主线集成，主机一次性研究环境已安装锁定依赖并准备SHA校验的公开词表；无需因此重启API。真实缓存下114项聚焦回归通过，另独立deadline与真实BPE两项通过。原50ms传输deadline测试仅隔离分词冷启动，生产截止和预算未放宽。
- 历史首阶段原request hash精确匹配，新保守预检为22698，已记录实际输入19263；同critique生产投影与计量的累计预检为39888/64000、余量24112，wire35283小于55808。仅证明调用前本地守卫可通过，不保证第三阶段、实际累计用量或研究内容合格。旧证据、冻结hash和业务行未改变。
- A继续一次新正常研究，使用同原P/问题与正常新截止，复用现有持久化资料，不重放旧run或重新采集。保留三调用、原用量阈值、超时与首错停；等待实际进程退出，核验正文及同案例DB/BFF/DOM。若输出不合格，保留真实终态并定位具体原因，不自动重试、不把预算修复等同于完整决策完成。
- 真实回测、Shadow、内部paper绩效、演化仍分别受合格历史/价格用途/人工方案或初始资金选择制约；连续采集、新闻新源、正式券商模拟和实盘/部署仍按原产品与外部影响边界处理。已交付入口不重复开发，局部资料缺口不阻塞本轮研究接续。

**综合合同接续（main `500d26c`）：** 新正常研究三阶段均已调用，实际输入51253/输出3540未超原阈值，七角色已保存；综合响应在assessment_binding/domain_validation被拒。旧记录与同案例页面已验收，具体响应未保留，不能倒推本次违反哪条规则。离线真实staged消费者已复现九种SDK严格schema允许、领域拒绝的组合，包括resolved=true却用空串而不是null表示无后果，以及未解决审查缺具体缺口/后果、空白理由、sufficient含未解决项。

下一片由wire owner唯一修改`codex_staged.py`、`committee.py`与unit：仅向实际v3请求和schema描述明确既有resolved/空数组/null/非空白/充分性规则，不改领域约束或引入新union形状；已有validation_kind增加四个固定值review_rationale_blank、review_resolution_inconsistent、review_evidence_incomplete、review_sufficiency_inconsistent，精确匹配固定校验消息，unsafe优先、未知仍domain_validation，不存原响应/错误输入/位置。新request hash覆盖新实际规则，旧v1/v2行为与历史读取不变。root独立补原生worker→PG终态→API的合成resolved矛盾回归，验证七角色保留、三调用用量、无intent/重试/私密文本；owner覆盖九种反例和合法对照。一轮审查、定向复验后A统一集成。该片使现有合同更明确并改善诊断，不保证模型成功，不借此再自动调用模型或重标旧失败。

**C2逐项审查可见性（main `32d30ce`）：** 综合合同修复已集成并通过新真实八角色研究，但正文将认可缺口误当解决，系统仍partial/watch/none，v3当前没有自动升级正式合格R的路径。DTO保存了逐项自评与理由，实际页面未渲染这些字段；不是页面把sufficient直接标为充分。root在独立worktree唯一实现现有DecisionCard综合角色内的只读展开区：仅消费已验证stage_trace的synthesis_assessment，逐项显示回应对象、模型态度/解决自评、原理由、缺失项和受阻结论，复用原证据抽屉。明确“模型自评，未独立核验，不改变系统证据状态或执行许可”，不用绿色就绪标识；空列表/null写为模型未列出，不当作没有缺口。保留原保存值，不根据不确定性重写resolved，不隐藏内容缺陷。

只改现有组件、必要局部样式与合成测试，不新增API/依赖/模型调用/审批或发布。旧无assessment及invalid trace不展示新段；测试真实DecisionCard路径的真假resolved、充分性、空值、静态文本安全、证据抽屉和原partial/locked，浏览器用隔离端口的375/390/1440视口与键盘展开、无写请求/横溢出核验。集中审查后A串行集成，用同一新run的真实DB/BFF/DOM核验四项原理由与自评、旧hash不变，不为本片重跑研究。界面依旧是本地工作台，hosting配置不改、无云发布。

### 9.6 数据访问与独立开发分离（2026-09-04）

用户已确认 Alpaca 免费行情接入，只读、不付费、不交易；注册/登录和本地凭据尚待完成，仅暂停该来源的真实样本验收，不再重复请求接入授权或检查凭据。其余原计划工作继续，禁止将可独立开发的接线缺口归为外部数据阻塞。

- T1入口修复：`backtest_once` 的 argparse 错误会回显原始输入，复用现有安全 Parser 返回固定错误码；独立分支先3红后12项通过，集中审查后已合 main `ff67c2a`。不改回测算法、冻结数据或资格，不等于真实回测完成。
- C2/V1历史简报回放：root拥有现有brief/research两页面，所有条目使用原有 `run_id` 精确链接，不再拿本次简报 `generated_at` 查询最新研究。复用现有受保护run GET，校验运行与证券，不存在/错配不回退；回放不读取新研究准入、不触发POST，普通as_of入口保持。路由身份切换清除旧状态，晚到响应不污染新运行。无需新API/表/模型调用；测试六类链接、同证券运行选择、404/错配、刷新/晚到响应、旧入口及手机/桌面真实浏览器路径；A集成后复用已存研究验DB/BFF/DOM，不重新采集或调用模型。
- L1仍有独立接线缺口：手动root目前的completed仅覆盖质量冻结/诊断，不能解释为完整日流程完成。现有简报GET不需新建生成服务；末阶段审计manifest已有实现但未接手动root，下一片先核对其事实范围和幂等语义，复用现有实现补正常手动接续，不新增调度/审计框架或自动执行下游。

C2/V1本轮交付：`0bd03c1` 已集成为 `068bac8`，main `a64920c` 已含真实验收记录。125项研究单测、TypeScript、定向lint/build与4项浏览器用例通过；375/390/1440真实旧失败运行均保持原身份、快照、时点与阶段hash，不跳到较新完成运行；刷新无准入/模型/写请求，404不回退。无需重启API，旧业务行hash保持不变。

S1只读复核（main `a64920c`）：`shadow/query.py` 是历史结果读取器，真正缺的是 `shadow_source_query_v1` 来源证明的正常生产者，现仅测试helper创建。当前source policy禁止新Shadow根/session，合法用途和完整查询/否定证据合同未就绪；不能将“未查到记录”填成“完整查询无事件”。不新增必败入口；来源合同成立后再接生产者到现有人工确认和单日推进，原人工选择与真实交易日要求不变。

#### L1 手动审计接续切片（开发合同）

- 入口：在既有 `daily_once` 增加 `audit --root-id UUID [--run-id UUID]`。只观察已保存的手动根、精确质量/诊断子阶段，以及显式选择且通过 `resolve_daily_research_job` 核验的研究；不发现或执行其他任务，不采集、不调用模型、不创建方案/订单。输出明确观测范围，未传研究不代表没有研究；方案、paper、绩效仍不在该切片的根绑定范围，不宣称完整 L1 完成。
- 服务：新增薄层 `jobs/daily_audit.py`，复用 `manual_daily_job_ids`、审计链写入与既有 manifest publisher。观察实际状态、版本、输入/输出绑定、冻结截止与实际观察时间；失败/等待可记录，不改阶段状态，不新增审计 Job，避免破坏旧根的两个子阶段合同。
- 幂等：相同所选阶段事实不重复追加事件，实际观察时间不进入事实指纹；串行化同范围快照写入，拒绝错误根/研究绑定，不从同证券或同快照推断关系。保护业务运行中的并发变化，快照必须来自一致的读取；不通过新的重复读快照破坏现有全局审计链锁语义。
- 封存：先提交观察事件，再单独处理清单；按事件序号核验既有清单是否真正覆盖。仅在实际当前 UTC 日期尚未封存且日期可递增时发布下一段；今日已封存且新事件未覆盖时返回 `pending`，不覆盖旧清单或借未来日期。存储/提交结果不明返回固定安全的未知状态，不能宣称失败已回滚或重跑业务。
- 安全：数据库和对象存储限定现有本地显式配置；事件细节仅在受保护本地业务存储，CLI 不输出账户事实、凭据、对象 URI 或上游错误；固定状态/范围和必要的不透明引用可返回。沿用脱敏导出，`execution_locked=true`。
- 文件所有权：实现者写新服务、`ops/daily_once.py` 与相关 unit/PG 测试；集中审查已用合成配置确认 boto3 会向本地请求应用环境代理，因此允许给既有 `audit/manifest.py` 增加显式关闭代理的可选参数，仅本入口启用，原调用者默认行为保持。不修改系统代理或全局配置。主任务负责本合同/运行手册及集中验收；A 串行集成并负责实际业务库。无迁移、依赖或调度变更。
- 验证：先红后绿；覆盖合法入口、坏参数不泄漏、精确根/研究绑定、失败/等待阶段、重复及并发调用不重复事件、状态改变产生新事件、已有未覆盖清单不误判封存、对象存储故障保留观察记录、不调用业务 worker。隔离 PG 覆盖正常 CLI/服务到真实审计链、清单与脱敏内容，不以 mock helper 替代；先证明局部完成，再交 A 做已保存案例验收。

分支实现为 `917bbad`，代理定向修复为 `fab1caa`：首次11项真实PG和43项相关CLI单测通过；代理4项实际botocore无网络回归先红后绿，修复后49项相关单测与1项正常CLI→服务PG通过。只读RR观察与新RC审计写入分离，不持业务行锁；真实PG证明4并发观察只追加1事件、root并发materialize不混快照、业务写锁持有期间仍可观察最后已提交事实。主任务吸收初版后24项入口回归及audit帮助入口通过。A已仅用现存日根和研究完成只读验收准备；仍待修复定向复核、主线集成及真实审计运行，不将分支通过写为已验收。

既有价格、费用、合格研究、人工方案与模拟初始资金等真实依赖仍保留；上述局部修复不重标任何历史或宣告总计划完成。

L1局部审计实际交付：A已将上述五提交串行集成为主线代码`4e9f4b7`，16项定向单测通过；现存精确root+run经正常CLI首次recorded/sealed，重复调用复用同一事件/清单。实际gzip/SHA/脱敏内容/序号coverage/链hash/时间含义核验通过，八表旧行不变，现有审计BFF与页面可读新脱敏摘要，active0且无模型/采集/订单/API重启。上方“仍待”是分支交接时历史状态，本次已完成该限定范围的真实验收；不包含方案、paper和绩效，详细证据见现有checkpoint与受保护daily-audit-20260904。

### 9.7 D2 股票费用来源接续核对（2026-09-04）

- [官网客户服务入口](https://www.fosunwealth.com/download/download)的搜索索引有收费菜单，但本机实际请求为302，转向[星财富App公告](https://www.fotechwealth.com.cn/)，未取得现行收费表正文。D的既有主站归档也记录同类302；不能将索引、推广渠道的免佣条件或本次无法访问，分别当作已验证费率、全账户适用或官网不存在收费表的证明。不注册账户、不改变网络出口、不据此请求用户再次批准普通读取。
- 新取得的[官方交易OpenAPI](https://openapi-docs.fosunxcz.com/specs/public/trade/openapi.yaml)文本与D既有原件逐字符一致，原始SHA-256仍为`87e4e6601b8da122a504239a1922d8742ecd4072e2d10c30c0d6a97f896fb808`。复用`nexus-public-evidence/outputs/acceptance/official-source-samples-20260903/trade.yaml`原件，不把临时文本副本的末尾换行差异报告为上游版本变化，也不更新SDK/合同准入。
- 核对`mapping.OrderListItem`、`mapping.CashFlowInfo`及其响应结构仍无股票费率查询或已分类手续费明细；`applyFeeAmount`、`filledFeeAmount`、`feeRate`位于理财订单/RWA票据结构，详情请求限定`productType=17`，不能移用于股票（`productType=5/6/7`）或期权（`15`）。`businessType`、`flowType`仍只有字段名称，未补充费种字典；本轮未调用账户API，也没有为验证缺口重复刷新流水。
- FINRA官方材料已有D归档，SEC页面此前403；不重复同一失败或把监管机构对会员的费率直接当作私人账户最终扣费。后续真正需要的是股票收费表原件及生效/渠道/账户适用性、结算费种与gross/net映射证据。在此之前不新增费率默认值、`cost_complete`或必败费用入口；与A负责的研究准入使用范围说明并行，互不修改代码。

同期A已完成并验收研究使用范围说明（main `57e27b4`）：现有v2/v3只读研究固定保持partial/watch/none，正式优化要求合格研究，当前没有自动晋级路径。真实手机/桌面页面明确该能力边界，不能再把这个拒绝单独解释为缺行情或等待授权；未修改准入合同、模型和数据。

### 9.8 A2 公司行动采集接续（2026-09-04，主线代码交付，真实样本待验）

- A确认本项无人重复领取，协调主任务负责provider合同与集成验收，独立worktree实现手动CLI，A仍为唯一主线集成人。Alpaca真实凭据只阻塞样本验收，不阻塞本地代码；不再次检查凭据或重复请求免费接入批准。
- 重新核对当前官方`/v1/corporate-actions`：事件分桶，无旧归一化器要求的`status/revision`；`data_quality=complete`不保证字段完整，日期窗口不能冒称ex-date窗口，币种空值不能默认USD。因此不把当前响应硬塞旧账本结构，也不先改迁移、source policy或历史authority。
- 此切片先补真正可运行的有界GET→逐页私有原件/receipt→终态manifest。复用固定源HTTP与精确Decimal JSON解析，单证券/包含端点的日期窗、所有事件类型、显式`region=us/data_quality=all`；原件先保存后解析。仅显式null游标表示分页结束，预算截断、循环、限流或坏响应保留已收页并停止，不自动重试。
- 无DB/对象服务/模型/订单/调度调用；不把分页耗尽写为事件完整或回测合格。命令与输出含义见[运行手册](../../runbooks/alpaca-corporate-actions-local.md)。测试覆盖实际HTTP路径/过滤条件、原件字节、未知字段与精度、分页与失败保留、凭据和错误脱敏；后续真实访问再验样本，不用离线fixture替代真实准入。

分支验证：provider `ac16e2b`、CLI `6eb7c5c`（协调分支集成为`1b0b69f`）完成；协调分支独立执行完整providers目录与新增CLI测试共384项通过，Ruff/Mypy与实际`python -m nexus.ops.alpaca_actions_local --help`通过。其中20项CLI测试经正常配置入口、真实固定源HTTP层、合成HTTP响应到私有目录原件/receipt/manifest，覆盖中断、120秒截止、8MiB上限、重定向、限流/游标停止及零敏感输出。没有真实账户调用或DB/页面验收。

扩展回归同时在未改动main复现一项旧测试失败：迁移正例仍硬编码0032，而当前0040。`3e191e3`仅将测试正例从本worktree实际Alembic目录独立取得，并拒绝旧0032、当前父迁移、空值和未知值；不从运行时常量生成预期，也不修改生产准入。

主线交付：集中独立审查Ready后，A按指定顺序串行集成六提交为`3d3c868`、`47be12f`、`52f9413`、`3cc70c3`、`82702ab`、`e48fc6c`，未合入协调分支的其他历史。A独立运行完整providers目录与新增CLI共384项离线测试全部通过，5文件Ruff、2个新模块Mypy、实际模块帮助入口及diff检查通过，主线已快进至`e48fc6c`。本次未读取凭据、访问账户/外网、改数据库或重启服务；交付的是可运行的本地采集入口，真实样本、公司行动完整性与历史回测准入尚未验收。回滚本次代码即可停用新入口，已保存原件应保留，无迁移或依赖变更。

### 9.9 A2 原件进入冻结研究工具（主线代码交付，运行迁移待联合更新）

本片交付正常本地导入→对象原件/数据库→研究快照→`get_security_snapshot`的实际消费者链；页面/API展示紧随其后，不能以本片测试代替真实样本或页面验收。复用既有表，不将当前响应写入要求旧status/revision的CorporateActionRecord，不改source policy、账本、行情或历史资格。

- canonical source=`alpaca_action_observation`，schema=`alpaca_action_observation_v1`。文档冻结instrument_id、symbol、query_start/query_end、available_at、逐页原件与receipt哈希/长度/实际接收时间、manifest哈希/长度、事件观测。固定`source_completeness="unknown"`、`historical_backtest_eligible=false`、`ledger_eligible=false`、`revision_semantics="local_content_hash"`；内容SHA仅是本地观测版本。available_at为各页receipt最大值，不是公告/历史可用时刻。provider_object_id由instrument与query窗口确定，revision为canonical SHA；重导同内容幂等，旧证据不改写。
- 归档验证只接受上一片正常collector的collected、分页耗尽、无error的完整归档；未完成原件仍保留在私有目录，但不伪装成完成查询。固定文件名page-NNN.json/receipt及manifest，拒绝符号链接/路径穿越、重复键、哈希/长度/源/路径/查询不一致、未来或逆序接收时间、token链断裂/循环/提前终止/多余页。最多100页、累计原件32MiB、10000事件；超过界限明确拒绝，不截掉数据后入库。没有网络、模型或账户读取。
- 事件保持来源类别、原id和page/index定位；只有原件`symbol`、`old_symbol`或`new_symbol`显式等于该本地证券symbol才进入已绑定观测，缺身份/错配计入`unbound_event_count`，不由请求参数补造身份。保留reported symbol字段及官方已提供的process/ex/record/payable/declaration日期、rate/old_rate/new_rate和currency；缺失保持null，数值用Decimal精确保存，未知字段仍可从原件定位。非法已提供字段拒绝，空币种不默认USD，不由比率生成拆股账本事件。无匹配事件不代表没有事件，报告总量/未绑定量及unknown完整性。
- Python合同位于`research/alpaca_actions.py`与`research/alpaca_actions_storage.py`：`AlpacaActionObservationDocument.canonical_bytes()`；`persist_action_observation(session, document, *, location)`；`read_action_observations(session, *, instrument_id, as_of, evidence_ids=None)`；`action_tool_payload(document)`。reader绑定同source/object/revision/hash/时间/证券，按实际created_at及receipt控制截止；坏的可选观测不得遮住有效SEC。工具最多展示32事件，保留总量、truncated、未绑定量和接收时间语义，不改变冻结全文hash。
- 本地入口为`python -m nexus.ops.alpaca_actions_import --repo-root ... --instrument-id ... --collection-dir ... --output ...`，复用正常配置和私有输出。仅本地DB/对象服务；核对当前Instrument ticker，保存manifest、receipt、page与canonical到既有内容寻址对象存储，并读回校验后绑定DataArtifact（source=`alpaca`；schema分别`alpaca_actions_manifest_v1`/`alpaca_actions_receipt_v1`/`alpaca_actions_raw_v1`）。最后prepare正常研究快照并核对新evidence确已冻结；结果仅私有输出不透明id/固定状态，失败固定安全错误。对象先于DB提交，孤立不可变对象允许保留，失败不宣称全部回滚。没有模型、订单、调度或共享服务重启。
- root拥有新增0041冻结ownership guard/head、snapshots与database_tools接线和隔离PG回归；子agent独占canonical/归档验证/storage/import入口及其测试。guard保留既有全部分支，校验同证券/版本、固定只读语义、manifest与每页raw/receipt实际DataArtifact绑定、长度和不晚于冻结时刻；降级若已有该类冻结evidence则拒绝，无数据时可降级/再升级。不重写0039/0040。A已确认无其他相关写入，仍唯一main与共享运行owner。
- 两个实现面以本合同并行；先红后绿、正常入口到真实隔离PG及工具路径、错证券/时间/篡改/缺原件/重复导入/历史截止反例，一轮集中审查后串行集成。原件解析样本全为明确合成fixture，真实访问未就绪仍待验；不重复查凭据或模型调用。回滚应用即可停用新读取者，迁移按上述保护回退，不删除已有原件。

独立B3复核：现有v2/v3 synthesis生成器固定partial/watch/none而正式消费者要求complete，因此当前真实formal R生产路径不可达。集成测试的complete研究是合法存储fixture，不是原生producer正例；另立下一小片的范围内事实充分性合同，保留旧版本和不执行语义，不靠删partial、相信resolved=true或再调用模型“解除”该缺口。

分支联合验证：原件归档/canonical/storage为`94014e9`（root吸收`96217be`），正常import/S3薄补充为`5a40fa7`（root吸收`a65cab9`），0041/head初版为`6d292f0`，后续消费者与观测时点校验一并验证。旧guard拒新source、缺canonical未拒、工具未冻结/未带正文先后红测，接线后正常collector→配置/CLI→实际S3 adapter（底层合成client）→隔离PG→冻结研究工具通过；重导只一份evidence和四份artifact，旧快照hash不变，不建账本/价格authority。原件篡改、错证券、最后canonical读回损坏均不留下部分DB证据，已写不可变对象保留。

集中审查前，308项相关单测通过，6项既有离线token词表用例因未提供显式fixture跳过（未为此联网或调用模型）；47项隔离PG通过，含18项0041 guard/降级保护、当前工具/坏可选观测、4项正常导入/回滚及既有SEC/issuer/完整schema升级降级回归。完整schema反射仍有两项既有SQLAlchemy dialect-option告警，不影响比较结果；7源Mypy、15文件Ruff、帮助入口和diff校验通过。新增本地导入没有依赖变更；仅显式本地S3客户端关闭环境代理，新增evidence前缀读回受原bucket/prefix/version约束。实际账户样本、页面/API展示与主线运行尚未因此完成，待一轮集中审查、定向修复及A串行集成。

主线接续：集中审查Spec通过、Code quality Ready、无P1/P2后，A仅按指定顺序集成`6d292f0→25da974`、`96217be→f8a859e`、`a65cab9→7242b76`、`b20da9d→0c191a8`。A定向复核79项新模块/对象存储单测与6项隔离PG全部通过；PG包含4项正常import/失败原子性、0041合法冻结后拒绝不安全降级、空数据升级降级往返。8文件Ruff、7源Mypy、实际import帮助入口及diff检查通过。主线代码已至`0c191a8`，未重复取子代理原提交或合并整条协调分支历史。

本轮仅代码与合成整链交付；业务库未应用0041、API未重启，运行仍沿用0040及原API版本。新的主机入口要求0041，联合更新前不能直接用于当前业务库；后续API/UI接线完成后由A按备份和兼容流程统一更新。真实样本、页面展示和历史回测准入仍未验收；不得从本轮测试推断已完成。

### 9.10 A2 冻结公司行动研究页（已集成并更新本地运行，真实源样本待验）

- 在既有 `ResearchInputsSummary` 增加可选默认空数组 `corporate_action_observations`（最多1项），不新建endpoint。只从精确快照绑定的source/object/revision/hash/时间/location引用调用既有canonical reader；新版本、错证券、截止之后或损坏的可选观测不进入旧快照，也不遮住有效SEC。准入和精确run回放共用现有读取路径，不采集、不调用模型、不改资格或历史。
- 新DTO `ResearchInputActionObservation`：evidence_ref_id、source=`alpaca_action_observation`、revision、content_hash、symbol、query_start/query_end、available_at、manifest_content_hash、page_count、total_event_count、bound_event_count、unbound_event_count、truncated、events（最多32）。保留固定pagination_exhausted=true、source_completeness=unknown、revision_semantics=local_content_hash、historical_backtest_eligible=false、ledger_eligible=false、execution_locked=true和available_at_semantics=maximum_page_receipt_not_historical_availability。
- 事件DTO `ResearchInputActionEvent` 保留category/provider_event_id/page_number/index/symbol/old_symbol/new_symbol、process_date/ex_date/record_date/payable_date/declaration_date、rate/old_rate/new_rate/currency。数值输出沿用DecimalString的40字符纯十进制合同；先按Decimal元组计算格式长度再转换，避免极大指数分配。超出合同的有效值只将相应展示字段置null，并在 `unavailable_numeric_fields` 列出rate/old_rate/new_rate；UI区别“原件数值超出展示范围”和“未知”，不修改canonical精度、不把未知币种补USD。
- 页面复用ResearchInputs/ResearchPanel和独立只读 `ResearchActionObservation` 组件、现有格式函数、原生details及表格容器。行动独有资料不得标成SEC/发行商财报；显示查询窗（不冒称除息覆盖）、分页与绑定/未绑定数量、实际接收时间语义、观测定位、日期和来源数值；不推断单位、拆股比率或入账。显式说明完整性未知及回测/账本不可用，长标识可展开且窄屏不撑宽页面。旧响应无字段继续可读。
- root独占API/schema/read-side辅助模块、生成合同与Python单测/PG回归；复用子agent在独立worktree独占ResearchInputs、ResearchPanel、新组件及其前端unit/e2e测试。A已确认无重叠写入并将隔离PG窗口交root，仍是唯一主线/共享服务owner。本片无迁移、依赖或运行配置变更；应用回滚即停用新展示，已有原件和冻结数据不删除。
- TDD覆盖正常归档→冻结PG→实际API、actions-only/SEC混合、空事件/未绑定、32条截断、零/微小/超大指数、错误版本和坏可选观测。界面测试覆盖旧响应、混合来源、精确历史回放、只读刷新和手机/桌面。一次集中审查后A串行集成、统一应用0041/更新运行并验证现有业务不受影响；真实Alpaca样本未就绪仍单列，不以合成样本冒充真实验收。

实施时核对出精确run GET原先并未返回inputs，不能假设它已有冻结原件消费者。root同时拥有 `api/routes/research.py`，为 `ResearchRunResponse` 增加默认null的inputs，并在既有GET hydrate中使用run本身的snapshot/instrument/decision_time读取；POST继续默认null，回放不查admission、不增加请求或回退最新资料。新旧观测双版本PG反例先复现字段缺失，再补接线；前端回放仅使用run.inputs与run.decision_time。A已确认该route独占分工。

分支接线已交付：`fb42dca`定义DTO/有界数值投影，`dead164`接精确API读取，`df40dd7`更新运行手册，前端`aa5ba35`吸收为`33c2528`。旧无字段响应继续可读，32条截断/空与未绑定保留语义。集中审查发现的零值大指数误标与通用ex_date过度解释由`98d1abb`修复，三条零值和未知类别日期用例先红后绿；定向复核Spec通过、Code quality Ready，无剩余P1/P2。

正常collector/归档/导入→隔离PG→真实API→真实BFF→浏览器同一合成案例验收完成，375/1440两宽度各测普通准入和精确run回放，共4项，无浏览器API路由mock。逐层evidence id、snapshot id和内容hash一致，显示精度0.123而API仍保留0.123456789012345678901；缺币种为未知。刷新仅GET，回放不查admission，8张相关业务/冻结表前后hash一致。HTTP来源和S3底层client明确为合成fixture，未调用真实账户/模型；隔离环境只确认数据库/迁移读取依赖ready，未启业务worker/外部S3服务，整体readiness仍unready，不把此次页面验收报告为完整运行就绪。临时API/Web已退出，临时库由现有隔离fixture清理；截图和逐层核对在协调worktree的`outputs/acceptance/actions-research-display-20260904/`。

该实际入口验收另外复现import私有结果as_of的`+00:00`被既有BFF的Z格式校验拒绝。`863cfa7`仅将生产者输出改为同一UTC时点的Z后缀，保留微秒，不改冻结值或放宽BFF；正常CLI先红后4项PG定向通过，实际import结果未转换即通过上述BFF/DOM，定向审查Ready。整片36项扩大PG（包含8新展示及正常CLI）通过，修正后4项import另行复测；前端147研究单测、Python76相关单测、TypeScript、定向lint/Ruff/Mypy、生成合同与build通过。主线/共享业务库0041迁移与API更新仍待A串行集成后执行，不把分支验收或合成源当真实Alpaca准入。

### 9.11 B3 新冻结集中度研究前提核验（已集成并更新本地运行）

本片解决现有 `_concentration_focus` 只核比例却信任诊断任意限额/政策字符串的问题。A已确认无人占用 `agents/portfolio_context.py` 与相关unit/PG测试；独立子agent拥有该范围，A仍唯一主线集成人。与A2展示分开提交，不更改共享API/schema、运行服务、数据库迁移或模型调用；PG窗口待协调。

- 只在生成新冻结context时独立核验既有TOP1建议预警：P估值时点同币种全部证券的quantity×current_price之和为分母，排除现金、不跨币折算；目标证券须为最大项，并列最大允许。缺价、异常数量、分母不全/非正、时点不合、P非READY/对账非PASS不能确证。维持现有最小脱敏输出，不向模型扩散其他持仓或账户事实。
- 必须绑定原P、D的id/input hash/估值时点和冻结引用，以及实际已发布canonical `real_advisory_policy_v1` 的id/version/内容/可用时间。复用现有policy loader和诊断计算口径；unit=ratio、policy_scope=currency:XXX、无unknown_reason，原指标、限额、severity必须匹配。既有政策严格w>0.30为critical，否则w>0.15为warning；计算不从页面三位数倒推，存储指标比对沿用12位量化口径。不得把任意0.35或非canonical policy fixture当正例，不发明阈值。
- 上述只证明“同币种证券范围的建议阈值偏离”，不是硬风控违例，也不证明减仓或维持更优。任何缺口保持focus不可确证，沿用现有可选输入降级，不删partial、不改intent、optimizer或formal准入。不新增完整性资格；狭窄事实充分不能升级成全局ResearchIntent.complete。
- 历史context/R按原冻结合同继续读取，不重算、回填、改hash或重标旧资料。本片不将新校验塞进旧读取路径。测试覆盖15%/30%等值及跨界、并列最大、现金改变不变分母、跨币隔离、坏/晚到事实、错P/D/hash、错政策/阈值/单位/等级；实际native输入读取确认核验后的focus来自同一冻结context，旧R/hash不变。先红后绿，集中审查和定向回归后单独交A串行集成。

正式R完整生产者仍未完成：现有complete方向会约束optimizer目标（reduce_candidate甚至置零），集中度事实本身不能证明该方向。后续仍须明确支持方向、反证与有效条件的充分证据合同；这一缺口不能用重复模型调用或放宽冻结门槛消除。

分支交付为`1765b96`，在已同步A2主线的协调分支吸收为`96a353f`，仅portfolio_context与两测试文件。新freeze调用独立核验helper，未更改旧read/_bytes/schema；同时实际NaN反例证明会抛InvalidOperation，已在现有cash/price/quantity入口补finite校验，坏估值降级而非隐藏整个冻结流程。纯计算6红→25绿、真实缺/错/晚引用4红→6绿，最终整文件21项PG通过；root独立44项研究/optimizer相关单测、Ruff/Mypy与diff检查通过。

集中审查`0e37cce..96a353f`结论Spec通过、Code quality Ready，无确认P1/P2。审查对照真实诊断evaluator/top_weights/_stored_decimal，核对完整同币证券分母、并列最大、未量化严格阈值与12位metric；canonical policy的身份/内容与不晚于P时点检查保留。PG正例实际走prepare→admit→database_read_only_tools，D为合法合成存储fixture，不能称为全诊断input_hash重演或正式R生产验收；旧focus 0.35原值及hash继续照原合同读取。本片只确定新研究前提，主线集成与运行交A，原正式研究方向/成本/来源数据缺口保留。

### 9.12 A 串行集成与实际运行更新（2026-09-04）

A2八增量按序集成为`e236ea5/beb23b2/27ff42f/4f93103/60191d9/afaaa08/de13833/757321c`；B3为`2c31750/6b1fe17`。A独立147项研究前端单测、64项定向Python单测、8项新API隔离PG、25项集中度单测、TypeScript与生成合同一致性检查通过，B3 Ruff/Mypy通过；不重复子代理原提交或合并协调分支历史。

更新前完整备份暴露此前daily audit新增对象未被完整备份支持：审计清单采用规范内容寻址路径且无versionId，旧备份只接受provider路径/版本，因此首对象前拒绝。另无网络复核证实本地S3请求会使用环境代理，这是独立潜在问题。`e7e5b81/ffe8472`分别修复局部代理与严格审计清单兼容；后者集中审查Ready且指纹一致，47相关回归及Ruff/Mypy通过。失败目录不覆盖、无完成标记；新的`pre-0041-20260904-complete`覆盖177个引用对象，并用不存在源配置路径实际离线恢复通过，schema/摘要/对象绑定一致、残留0。

实际运行已更新到API代码`55dedb910ef2f3871a0da3cb55b75672f8c04a14`、schema `0041_action_observations`。镜像与源码摘要匹配，原环境/端口不变，PG/MinIO未重启。九类历史表全部旧行摘要一致、活动Job为0、未注入公司行动测试证据。两个真实旧研究在375/1440共四组BFF/DOM通过：精确run/snapshot、原响应除新增inputs外完全相同，inputs按原冻结读取，公司行动列表为空，刷新无准入读取或写请求、执行锁保留、无横溢出。真实Alpaca公司行动样本仍未验收，不能把旧案例兼容验收等同新增资料的真实源正例。

受认证的readiness为HTTP503：database/migration/object_store ready，worker unavailable；live通过，不为改绿启动常驻worker。现存D/P只读新focus核验返回None，落在“非唯一匹配CONCENTRATION_TOP1”条件，不记录为合格正例、不重建旧研究。验收产物在受保护actions-0041-20260904。回退保留rollback-before-0041-20260904镜像；仅在0041降级保护允许时回到0040与旧镜像，不删除冻结证据或备份对象。

### 9.13 外部接口 mock 与正常生产链分离推进（2026-09-04）

分页切片主线交付：集中审查Ready后，A按序集成`39eb841→96719b5`、`15f5289→2ea0090`，独立13项HTTP/provider单测、3文件Ruff、1源Mypy及diff检查通过。root已验证的两项HTTP transport→collection handler隔离PG未重复运行，未占用正在进行的联合PG窗口。本轮无账户访问、迁移、数据修改或API重启；运行仍为API `55dedb9`、schema `0041_action_observations`。后续正式研究与S1候选提交尚待各自审查，不能把本次分页集成记为它们已完成。

用户明确允许尚未取得访问的外部 OpenAPI 先 mock 开发，申请后另验真实来源。这取代“必须先完成 Alpaca 配置才能恢复全部开发”的判断；不是允许 mock 业务准入、修改真实 source policy 或把合成资料标为真实。HTTP/对象客户端/模型传输可在隔离测试替换，内部归一化、冻结、Job、研究生成、正式读取及目标约束仍走实际实现。无真实账户调用、新调度、业务数据注入或交易。

**并行分工与顺序：** root 保持既有协调 worktree，负责 Alpaca 分页校验及本计划；独立价格 owner 负责 S1 正常查询生产者；独立研究 owner 负责正常研究到正式消费者的 PG 回归。A 已确认主线 `a35efa6` 无重叠写入，仍独占 main/共享服务。重型 PG 串行分配，各 owner 的纯单测和实现不互等。

#### A2/S1 分页边界修复

- `providers/market/alpaca.py` 不再通过 `payload.get` 把缺失 token 当查询结束；必须有显式 `next_page_token=null` 或非空续页字符串。缺字段/空串/错误类型返回固定不含源值的错误，不重试、不返回成功页。显式空结果与缺少结束证据是两种结果；这是一项保守完整性要求，不把缺字段本身解释成上游必然违反公开协议。
- unit 使用真实 `AlpacaHttpTransport→AlpacaMarketDataProvider`，只替换最底层字节请求，缺 token 与空串先红；显式 null/非空 token、精确原件和数值保留作为对照。真实 `collect.market` handler 的隔离 PG 必须证明缺 token 不能返回 `MARKET_NO_DATA`，不新增 artifact/manifest/bar，合法 null 空页保持原明确无数据结果。
- 不回填旧原件、不新增迁移。当前改动 `39eb841` 已有 20 项 provider 单测、Ruff/Mypy 通过；新增两项 PG 与集中审查完成后交 A 集成。

#### S1 价格查询正常生产者

普通 `collect.market` 固定过去 400 天与 `adjustment=all`，且不持久化空页，不能充当 Shadow 单日 raw 查询证明。新增专用 `shadow/source_queries.py`、`shadow/price_collection.py` 和 `ops/shadow_price_query.py` 单次入口，复用现有 provider、对象存储、`ingest_price_page`，不扩建调度系统。只接受明确已有 Shadow 根与交易日，证券集合来自冻结方案；不接收 `complete`、member IDs 或“分页耗尽”旗标。

- [官方单证券 bars 合同](https://docs.alpaca.markets/us/reference/stockbarsingle-1)的 start/end 均包含端点，短页不证明终止；[官方 FAQ](https://docs.alpaca.markets/us/docs/market-data-faq)说明日 bar 的 timestamp 是纽约日初，不是收盘时刻。专用请求固定 `1Day/raw/sort=asc/limit=10000`、明确 feed 和 `asof=session` 的证券映射；纽约当日 00:00 至次日 00:00 减一秒转 UTC，使用 zoneinfo 处理 DST。只处理已结束自然日；不从时间戳推断实时性或最终收盘资格，旧 transport 默认行为不变。
- 每页含空页保留实际路径/查询参数、证券与输入 token、原始 artifact ID/hash/字节数/实际接收时间、原件中输出 token、归一化 manifest/BarSet/成员。实际 producer 核对归档与数据库绑定、连续分页链、显式终止、日初时点/窗/证券一致，才能产生来源返回集合证明。缺页、循环、错误原件或成员、未知终止不得生成完整证明；晚于读取 cutoff 的证据不可采纳。已结束自然日之后收包可形成当时的返回集合证明，保持实际可用时间与 late_backfill，不回填旧 decision_at 或赋予历史价格用途资格。零条仅表示该来源在该窗口返回零条，不解释成停牌、退市或无公司行动。
- `shadow_source_query_v2` 哈希绑定请求窗口和 collection receipt；`shadow/authority.py`、`shadow/canonical.py` 新增对应读取/哈希分支，旧 v1 严格字段、读法及 hash 不变，不回填历史。重放先核对现存证明再零 HTTP；首错停止，保留原始归档，不覆盖已有冻结资料。不新增表/依赖或 source policy。
- TDD 单测覆盖终止/循环/DST/边界及无秘密输出；集中 PG 走底层 HTTP mock→实际 provider/持久化→producer→authority reader，覆盖多页、空页、错窗口/证券/hash、晚到、成员错配和重放。价格用途与新 Shadow 启动仍由原门禁决定。authority-reader 的合成事件对照须明确来自已有 fixture，不把当前完整性未知的 Alpaca actions 观测升级为完整事件集。

专用来源校验补充：[官方 BarsResponse schema](https://raw.githubusercontent.com/alpacahq/alpaca-docs/master/oas/data/openapi.yaml)包含响应 `symbol`，新 v2 路径要求其与固定请求/receipt 的证券一致，缺失或错配均拒绝，不改旧通用 provider。集中审查核实：外部 Alpaca HTTP 沿用现有本机环境代理，本地 S3 独立禁用代理；原件之外，每页请求/接收凭证使用既有 DataSnapshot 的独立 `shadow_price_receipt_v1` 保存，最终 v2 引用并重验。坏 JSON 或后续页失败时仍能定位原件对应的 root/证券/path/query/input cursor/实际收包时间；该凭证不是完整查询证明，输出 token 由原件解析，完整证明与价格归一化保持整轮原子性。无新表、迁移或通用任务框架。

同 key 的收包凭证还保存 idempotency key；已有该 key 的原件而没有 final proof 时，固定 `SHADOW_PRICE_INCOMPLETE_REPLAY`、零 HTTP，不能自动换 key 重放。成功 v2 须绑定该 key 的完整 receipt ID 集合，不能漏页或夹入别次采集。代码 `3288012` 已由 root 吸收为 `38dd8a4`，八个相关文件净差异为零；70 项单测（41 旧/29 新）、8 项新 PG（含真实 CLI）、5 项旧 v1/启动/推进/历史 replay PG、4 源 Mypy、8 文件 Ruff 和 diff 检查已通过。新 PG 明确验证失败原件与请求归属保留、同 key 失败重放与并发锁零 HTTP、提前 cutoff 非 complete、伪造成员/manifest 拒绝。正常结果仍 `price_use_authorized=false`、`execution_locked=true`，不调用 start/advance。最终定向审查确认两项 P2 及 key 闭合均已修复、无剩余阻断，Ready 交 A 集成；根任务已实际核对 CLI help 与运行手册参数一致，无外呼。命令见 `docs/runbooks/local-development.md` 的单次 Shadow 价格查询章节；此处不是实际来源或 S1 总闭环验收。

#### B3 正常研究生成到正式消费者的 mock 验收

主线接续：A按序集成`74f988a→06243cd`、`e0c43a9→4e8945f`、`177f996→1b05782`、`bb9905c→814882f`，独立11项formal inputs单测、4文件Ruff、2源Mypy及diff检查通过。复用下述已通过的6+15项隔离PG证据，不占S1正在使用的测试窗口。仅主线代码交付，未调用账户或模型、未构造业务正例；无迁移，API保持`55dedb9`、schema保持`0041_action_observations`，不为此单独重启服务。

新增 `engine/tests/integration/test_formal_native_research.py`，替代“直接插入最终 complete R 即证明研究生产”的证据缺口。测试通过正常 prepare/admit、研究 worker/execute/synthesize、最终 output，接 formal producer 和优化 worker；不 mock synthesis/quality/正式读取，不手填最终 R。已存在的离线委员会正例标为 legacy fixture，不能称作 v3/v4 原生模型充分性；v3 的 mocked SDK 自评 sufficient 仍必须经实际生成保持 partial 并被正式入口拒绝。实际完成时刻和 cutoff 不回填，保持原冻结引用、用量、Job 终态和时间约束。

正式 v4 仍需要生产实现，不能用该 legacy 正例替代：当前 `models.py` 对 complete 的 reduce_candidate 直接将目标置零，ResearchCondition 只核引用而不求值；现 formal producer 还严格只支持具备来源条件的 cash-only P。后续合同须分开“研究内容/证据核验”与“投资方向许可”，模型自评不能写入确定性通过结果，进入/退出/失效条件应由原冻结事实实际求值，未知保持拒绝。不能为了跨过门槛把 baseline 适用性单独命名为完整投资研究，也不能擅自选择减仓幅度或成本换风险阈值。本节新增测试是该实现的正常链路基线，不是完整 B3 或经济排名验收。

**正常链测试发现的时钟接线修复：** 实际研究 worker 已产生合法 legacy complete R，但 `formal_producer` 强制策略 `market.decision_time == request.decision_cutoff_at`；若将 refs 改为实际研究完成后，策略 reader 又正确拒绝与原日历不一致的时间。这不是外部行情缺失。root 独占 `optimizer/formal_inputs.py`、`formal_producer.py` 及已有 `test_formal_inputs.py`，修正已有 v2 冗余的同钟约束为 `P.valuation_time <= strategy.market.decision_time <= decision_cutoff_at <= materialized_at`。沿用已有分别冻结的字段，无新表/版本字段或迁移；旧合法等时 payload、引用与 hash 原样读取，不改 `strategy/snapshot.py` 的精确日历校验。价格、质量和 baseline 仍按原策略时点读取；后来 R 与实际 Job 完成按总 cutoff 检查，selection 仍记录实际计算时间。无成本/方向/执行资格提升，cash-only 与同 P/D 绑定不变。

独立研究测试使用明确合成的旧 P/原 calendar 与独立旧市场 quality snapshot，实际新研究与 worker 时钟不回填。先证明旧同钟校验阻断正常链，再验证正例到原正式 worker；坏边界覆盖 P 晚于策略时点、策略时点晚于 cutoff、篡改策略时间不匹配日历、cutoff 早于研究实际完成，以及 v3 自评充分仍拒绝。root unit 两个合法异钟例先红，并与原等时行为和越界拒绝共同回归；修复集中审查通过后交 A，不宣告这已完成 v4 研究充分性设计。

本片联合验证：正常研究 6 项 PG 已通过，另既有正式生产者 15 项 PG 通过；14 项 formal inputs/research intents 单测、Ruff/Mypy 两生产文件通过。集中审查 `15f5289..177f996` 为 Spec/质量 Ready，无可操作 P1/P2；首次联合验证中新增测试把 reservation 的 `state` 写作 `status`，已按真实 ORM 修正并重跑 6 项全绿，没有修改产品预算合同。legacy 正例确实通过 prepare→admit→研究 worker→synthesis→正式 producer→优化 worker，原生生成并保留 R/hash，三方案输出仍 partial、费用未知、不可排名/执行；v3 mocked SDK 三阶段自称 sufficient 仍 partial 并被拒绝。市场/质量/账户资料是明确合成存储 fixture，只有研究及方案是通过正常生产路径生成，不声称上游全采集、v4、真实模型或真实来源已经验收。

读取侧补验：同一个正常生成的 legacy 正例继续经过真实 HMAC 鉴权 GET `/internal/v1/optimization-proposals/{group_id}`，核对返回的新 group、冻结 snapshot、三方案及全部不可执行/不可排名、Shadow 不合格状态；追加后该完整正例隔离 PG 1 项通过（6.90s），Ruff/format/diff 通过。没有 mock API、另插 group 或修改生产代码；这是后端 API 读取证据，不替代 BFF/DOM、真实来源或 v4 验收。

### 9.14 T1 冻结数据包的正常装配接续（实现与验收合同）

当前 `ops/backtest_once.run_once → authority_loader.load_frozen_point_in_time_dataset → run_backtest → persist_backtest_result → /internal/v1/strategies` 已存在且有实际 CLI/PG/GET 回归，不重建入口或引擎。缺口在上游：生产代码没有 `BacktestAuthorityBundleRecord` 的正常写入者；原正例由 `test_backtest_writer._seed_data_snapshot` 直接插入最终 bundle、snapshot 及成员。A 已确认无重叠写入。由既有独立研究 owner 接续实现，root 维护合同/协调，A 串行集成；与 S1 收尾不共享写入或测试库。

- **范围与输入：** 新增小型 `backtest/bundle.py` 装配服务，复用现有 ORM、loader、policy、hash 和 writer。只接收已存 calendar、universe/sector/corporate_action collection、SPY alias、等权 benchmark universe snapshot 的 ID，以及既有 source/cost policy ID、baseline 区间、initial NAV 与有界 idempotency key。不接收 complete/PASS、caller 计算的 hash、source selection、收益或任意成员列表。benchmark 必须属于指定历史 universe collection，不能用当前名单代替。
- **集合与价格：** 从已有集合读取精确成员，核对种类/范围、完整性、hash、来源与时点；所需证券来自历史 universe 与 SPY，raw/all 候选从数据库按已发布 source policy、原日历和 publication marks 确定性读取。复用 `persistence._validate_source_selection_authorities` 的候选集合、历史收盘可用性与 `apply_source_policy_v1` 逻辑，不另写一套宽松选择器。必要时抽取共享内部函数，旧 writer 必须继续使用同一验证。
- **时间与并发：** 使用既有 `backtest-authority-global` 与 calendar/collection 锁顺序协调成员发布。新装配记录实际冻结时刻，不把新 bundle/snapshot 的 created/completed 时间改成旧数据日期；source selection 的 decision time 与 bundle freeze 保持原合同。新 CLI 装配后的读取时刻须不早于物化完成，不能重现研究同钟问题。旧已存 bundle 的 `--as-of` 读取行为不变。
- **已确认同钟接线矛盾：** 旧 `persistence._validate_bundle_authority` 要求 collection.completed_at 等于新包 cutoff，阻止旧完整集合正常组包；改为 collection.completed_at 不晚于 cutoff。bundle.completed_at、source-selection decision_time 与 cutoff 的原等时要求，以及成员/publication/历史时点/created 核验保留。旧等时包 hash 与 replay 不变；以真实旧集合完成时间和新包实际物化时间做正反例，不改写集合时钟。
- **构造与复核：** 在内存构造 `PointInTimeDataset`，复用 `_authority_bundle_hash`、`_audited_data_hash` 与 snapshot payload 规范；可以抽取输入 hash 版本的 payload helper，保留原 `expected_data_snapshot_payload(data, result)` 的输出和调用兼容，不伪造 BacktestResult 或为组包重复运行整个回测。新 snapshot、bundle、price/instrument members 在同事务入库，并通过真实 frozen loader 和共享数据库权威核验后提交；禁止先提交占位 complete 根再补事实。旧历史 hash/读取/回放保持。
- **幂等与失败：** 同 key 同输入先验证既有完整冻结包并复用，不重新选择后来数据；同 key 不同参数拒绝。不改写源集合、旧 snapshot/bundle 或 publication marks。非法集合、晚到/错绑定/成员不全/选择冲突时不留下新包或半成品 snapshot。合法组包可以独立存在；若随后回测失败，不声称整个 CLI 零写入，重入复用原包。
- **正常消费者：** 在既有 `ops/backtest_once.py` 增加明确的准备后运行模式，旧“给 bundle/snapshot ID 运行”参数与语义兼容；新旧参数互斥，准备模式不接受倒填物化时间。构造完成后调用原 `run_once`，沿用实际成本假设、历史年限/晋级限制、`execution_locked=true` 与原 GET。不增加 Job、迁移、调度、交易或外部采集，也不改变 source policy。
- **验证和交付：** 先写失败回归，隔离源 fixture 只构造已有 calendar/集合/价格等权威，不手插最终 bundle/snapshot、不 mock loader/policy/engine/writer。原 v1 policy 本身允许显式 fixture，无需放宽门禁。验证实际 CLI 准备→组包→原回测→持久化→鉴权 GET；覆盖重放/参数冲突、未知集合、晚到成员、错绑定/候选集、失败回滚及旧入口回放。聚焦 Ruff/Mypy/unit 与串行 PG，一轮集中 review 后定向复核，交 A 精确提交清单。

本片不生产历史源集合；现有完整集合的正常上游生产仍是后续缺口，真实历史 universe、行业、公司行动及价格适用性仍须来源证据。合成集合只能证明组包/运行接线，不证明真实历史完整、多年 OOS、收益或晋级已达标。

实现交付：共享时点/候选与输入验证 `398c956`（root `c199222`）、生产者/CLI/单测/运行手册 `4f051f5`（root `822fbd3`）、正常链与边界 PG `78c1ec4`（root `b29da5b`）。七个交付文件与原提交净差异为零。实际入口为既有 `nexus.ops.backtest_once --prepare-request-file`；普通 JSON 文件上限 1 MiB，拒绝符号链接、FIFO 与新旧模式混用，金额保持 Decimal。准备事务提交后调用原回测入口，失败后可保留合法冻结包，重入仍复用同包而不是重选来源。

新晚到成员 PG 还复现 `_load_collection` 原先只取成员 ID、丢失关联 created_at 的漏洞；现共享 reader 同时读取关联时间并拒绝整个不可见集合，不过滤晚到成员后冒充完整。旧候选集合错误优先级与旧 CLI 缺参数错误码均经原回归恢复兼容。源集合完成时间仅要求不晚于实际冻结 cutoff，包/选择的等时、原历史可用性与 hash 门禁不变。

验证：新 12 项 PG 加旧 CLI/writer/loader 38 项 PG，共 50 项通过（联合 49 项 164.03s，另两阶段失败 1 项 9.54s）；29 项单测、四源 Mypy、六文件 Ruff/format、help 与 diff 检查通过。实际 CLI→正常组包→原 run_once→持久化→鉴权 GET 已贯通；覆盖原包重放与后来数据不重选、并发单包、非法来源/未来集合或成员拒绝、准备回滚，以及策略尚不可用时一个合法包/零结果的两阶段失败。无 source policy/loader/engine/writer mock，无最终包或结果手工种入；只使用隔离的合成上游源集合与价格。

集中审查代码层无剩余 P1/P2；最终测试通过，四个生产文件 SHA256 与审查指纹完全一致，满足集成 Ready 条件，不重跑同环境同代码的完整 PG。没有迁移、依赖、外部接口/模型调用或共享服务变更；旧业务数据不回写，停用新准备模式即可回退入口。真实来源资格和完整历史仍独立待验，本片不能替代 T1 全部历史验收或正式投资结论。

### 9.15 T1 规范回测日历正常生产（实现与验收合同）

§9.14 已由 A 精确集成为 `116bbce/64aba5b/a4d1d23/4f8b8f8`，root 与 main 净差异为零。接续只补确实缺失的规范日历写入者：现有 Fosun 本地日历为 display 合同，不能重标为回测日历；内部 paper 的 forward/admit/advance 入口已齐，本轮不重复开发 S2。三类历史 source collection 的真实来源资格仍保持未知，不用分页结束或数据库 hash 假造 complete。

- 使用已安装 `exchange_calendars` 及现有 `ExchangeCalendarAuthority`、`exchange_calendar_content_hash`，新增小型 `backtest/calendar.py`。固定现有 XNYS 合同，只接收 start/end；要求有序、均为首末实际 session 且在库支持范围内，不悄悄截断或归一化端点。版本为实际包版本生成的 `exchange_calendars_<version>_XNYS`，query 为 exchange/package_version；session/open/close 均取真实安装库，decision 为 close+30 分钟，完整性只指该版本给定区间日历。
- 复用现有全局 backtest publication 锁，再按版本/区间查询或锁定已有根。自然唯一键 exchange/version/range 已存在；同输入精确重放，核验根字段/hash、完整 session 集、开闭市及决策时间，冲突拒绝，不修补旧根。不改旧 Fosun 日历、旧包、历史 hash 或 publication 机制；没有新表、依赖、政策或调度。
- 新根与全部 sessions 在单个事务/保存点写入，published_at 与 created_at 保持实际物化时间，使用现有 loader 在实际截止重读验证后提交；末条插入失败不残留 complete 根。生成日历不是将历史市场证据回填到过去，不能让缺失 universe/sector/actions 或价格证据变得合格。
- 复用 `nexus.ops.backtest_once` 增加显式 `--prepare-calendar --start ... --end ...` 模式，只允许这三个参数，与已有 bundle 准备/旧运行参数互斥。输出固定状态、calendar ID/hash/version/区间及只准备不执行语义；不接受用户 complete、hash、session 表或倒填时间。旧模式参数和错误码保持；错误不泄露请求正文/数据库异常。日历 ID 由正常 `--prepare-request-file` 消费，不建立第二套回测流程。
- Owner 复用独立 `nexus-codex-wire-budget` worktree，先安全合并最新 main；拥有新服务、现有 CLI/手册、新 unit/PG，root 只维护本节，A 唯一集成。单测先红后绿覆盖节假日、提前收市、DST、非法范围/hash/参数；隔离 PG 走实际日历 CLI→bundle CLI→原回测→鉴权 GET，不手插最终日历/包，不 mock loader/policy/engine/writer，其他来源明确 synthetic。覆盖幂等、并发、冲突、失败原子性和实际创建时点，并复用 §9.14 既有回归。
- 一轮集中审查与针对性复核后小提交交 A。无业务库/共享服务操作；回滚为停用日历准备入口，已经提交的不可变日历保留。该日历是当前安装库的规范输出，不宣称交易所原始官方归档或历史版本修订已独立验收。

实施验收：旧 CLI 对新模式报参数错误、日历模块缺失先红；完成后实际 calendar CLI→正常 bundle CLI→原回测→鉴权 GET 贯通，没有手插最终日历/数据包或 mock 内部校验。缺少 actions 集合仍拒绝。15 项新隔离 PG 与 2 项旧 CLI 入口 PG 通过（新增正例追加缺集合断言后另重跑），48 项新旧单测、两源 Mypy、四文件 Ruff/format、help/diff 通过。PG 覆盖实际物化时间、提前收市/DST 日历读取、同版本区间并发单根、错 hash/时点/session/开闭市/发布时间拒绝、插入及重读失败保存点回滚，以及旧 Fosun display 日历未改变。服务不采集、不准备其他源集合、不运行回测；实际消费者另使用其输出的 ID。

集中审查最终 Spec/Quality Ready，无剩余 P1/P2；代码审查后生产增量只有 exchange_calendars 的类型标注，最终指纹 calendar.py `9469b846`、backtest_once.py `2c24d0f2`。复核新增测试/文档后复用同代码验证，不重复整组测试。其他上游历史资料仍为明确 synthetic 测试输入，不能据此宣告真实历史、正式经济排名、Shadow/paper 激活或总目标完成。

精确交付 `4a2c8cc`，root 吸收为 `555360c`；五个交付文件与原提交净差异为零，root 实际 help 与运行手册参数一致、diff 检查通过。无新迁移、依赖或业务运行变更，A 仍按单提交及本合同串行集成，不合并开发分支的同步历史。

### 9.16 已配置 FMP/FRED/Alpaca 的正常真实接续

用户确认三源均已配置并要求持续推进。D 已用公开样本各一次 GET 验证 Alpaca bars、FRED observations、FMP stable/profile 非空，但未保存原件；不重复连接探测，也不把该摘要导入业务。正常入口须另行采集并保存原件/receipt。三源目前为免费套餐，不改变主行情来源、开启调度/模型/交易或扩大付费权限。root 本次正常 Alpaca actions collector 已采集公开样本，现有 archive reader 核验 1 页 1 个显式绑定事件通过；完整性仍 unknown、historical/ledger false。A 唯一负责已停机本地必要栈恢复与该真实包的 import→DB/API/BFF/DOM，受保护引用不写本文。

FMP 与 FRED 分别由既有独立 worktree 实现，root 单独负责共享研究快照/工具及 FMP 冻结 guard，A 串行主线集成。没有其他源调用或并行业务写入；重型隔离 PG 分时，普通单测可并行。

#### SDK 选择（2026-09-04 核验）

用户要求先评估提供方 SDK，避免自行维护兼容库。选择以当前端点和所需证据为单位；优先官方/官方推荐 SDK 的公开接口，不把“有 SDK”当作必须迁移，也不把未直接满足本片合同说成整个 SDK 不可用。业务归一化、时点和冻结证据校验仍由 NEXUS 负责；不复制 SDK 的通用端点体系。

- 复星：现有 gateway 已调用提供方 `fsopenapi`，固定版本和必要 TLS/日志/权限边界见 [既有说明](../../runbooks/fosun-sdk-admission.md)。不是重新实现其认证协议；本轮不升级或更换。
- Alpaca：[官方 SDK](https://docs.alpaca.markets/us/docs/sdks-and-tools) 是 `alpaca-py`。本次读取的 [CorporateActionsClient](https://github.com/alpacahq/alpaca-py/blob/master/alpaca/data/historical/corporate_actions.py) 和 [REST 源码](https://github.com/alpacahq/alpaca-py/blob/master/alpaca/common/rest.py)显示：`raw_data=True` 返回已解码/合并的字典而非每页原始响应；该客户端未公开 session/timeout/retry 注入，底层 `retry_attempts=0` 也未覆盖默认值。因此当前已验证的 bars/actions 原件采集继续使用既有窄 HTTP 边界，不通过修改 SDK 私有成员再造一层兼容。本结论只针对当前读取的源码和本片端点；不预判未来交易或其他 SDK 功能。
- FMP：[官方资源页](https://site.financialmodelingprep.com/developer/docs/resources)推荐社区维护的 `daxm/fmpsdk`，不是提供方自维护；已核 [20260824.0 对应源码](https://github.com/daxm/fmpsdk/blob/6505e711a917851ee25febe1e2b9381bc38c6833/fmpsdk/client.py)，确实支持 stable/profile、公开 Session 注入、超时与零重试，不能按过时索引称其废弃。公开 Session 可捕获原件，技术上能适配；但仍需新增有界 requests/重定向/原件捕获并再解析，当前唯一 profile GET 相比复用现有 httpx 没有减码收益。本片暂不加依赖，仅保留固定端点；采纳其官方支持的 `apikey` 请求头，避免 key 进入查询 URL。后续增加足够多已需要的端点时再衡量 SDK 的净维护收益，不预先实现通用库。
- FRED：[官方 API](https://fred.stlouisfed.org/docs/api/fred/overview.html)提供 REST/XML/JSON；本次未找到美联储自维护 Python SDK。[fredapi](https://github.com/mortada/fredapi/blob/master/fredapi/fred.py)、[FRB](https://github.com/avelkoski/FRB/blob/master/fred/helpers/__init__.py)、[zachwill/fred](https://github.com/zachwill/fred/blob/master/fred/core.py)是第三方候选。其公开接口未直接同时保留原件、显式 metadata vintage 和有界请求；部分还将值转换为 float/NaN 或默认缓存。复用现有 bytes 基础设施，仅接 series/observations 两端点，不另造 FRED SDK。`order_by` 仅在已核 observations 响应出现，不添加到未列明的请求参数。
- SEC：当前消费官方 [EDGAR JSON API](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)，既有 `SecHttpTransport` 仅映射所需固定路径。本轮没有发现必须替换为提供方 SDK 的证据，不为固定 JSON 文件读取增加第三方包。

本轮 SDK 核验无业务外呼、无依赖安装/升级、无凭据或权限变更。FMP/FRED 恢复按上述选择实施；保留既有原件、精度、首错停止和安全错误测试。不因 SDK 检查阻塞独立存储/读端开发，也不把连接成功当作 SDK/证据合同已通过。

#### FMP 公司背景观察

- 既有 `/stable/profile` 的单 symbol 原件与无 key receipt 先保存，再精确解析唯一匹配响应；1 MiB 上限、固定 origin、首错停止、无重试。正常 `load_local_environment` 读取 FMP_API_KEY，不保存认证 URL，不修改凭据。只绑定当前既有 Instrument/唯一 ticker，已有标识冲突拒绝，不新增/重标证券或别名。
- 独立 `fmp_company_profile` / `fmp_company_profile_v1`；`FmpCompanyProfile` 保存 instrument_id/symbol、可空 company_name/description/cik/isin/cusip/industry/sector、固定无密钥 source_url、received_at=available_at、raw/receipt 的 hash/长度。source_published_at/valid_until/sector_taxonomy 均 null，revision_semantics=local_content_hash，sector_risk_eligible/financial_fact_eligible/price_authority_eligible/historical_backtest_eligible=false，execution_locked=true。不投影价格、市值、beta，不进入 SEC facts、SectorExposure 或价格 authority。
- 原件/receipt/canonical 使用既有 DataArtifact（source=fmp，schema=fmp_profile_raw_v1/fmp_profile_receipt_v1/fmp_company_profile_v1）；DataSnapshot/EvidenceRef 的 object=`profile:{instrument_id}:{symbol}`，revision/content_hash 为 canonical_bytes SHA256，三个时间均为实际 receipt。正常本地 collect/import 入口及存储复用现有对象服务；对象先于 DB，失败可能留下不可变孤立对象，不宣称全局回滚。
- Python 合同：`read_company_profiles(session, *, instrument_id, as_of, evidence_ids=None)->dict[UUID,FmpCompanyProfile]`；`persist_company_profile(session,document,*,location)->ResearchEvidenceRef`。读端核原件/receipt/canonical 绑定、身份、时间和 false 资格。root 在已有 SEC/issuer/actions 基底非空时才附加 profile；profile-only 不创建研究快照，不绕过旧 price 或 v3 partial 门禁。正常精确 `get_security_snapshot` 增加 company_profile 项，旧 run 仅用原冻结 ID，不回退最新；坏 optional 不遮有效 SEC。
- root 新增 `0042_fmp_profiles`，只增量扩展现有冻结 source guard 与 head；不改历史迁移、不新增表。绑定精确 instrument/object/revision/hash/时间、原件/receipt/canonical Artifact 的 source/schema/长度，拒未来、缺原件与资格提升。有已冻结 FMP 证据时拒绝降级；无该证据可恢复原 guard，再升级不损数据。业务迁移由 A 在备份/兼容验证后执行。

#### FRED 单序列原件与宏观研究

- 现有批任务是 collect.sec_macro，并非独立宏观入口；旧强制 observation.revision、同 series/revision 跳过其他日期、误把 units 变换码当单位的问题已由代码核实。新增小型单序列采集/导入，复用 `research/fx.py` 的证据/日历与既有 research_macro_v1 表，不建设通用采集框架。
- `FredVintageRequest(series, observation_start, observation_end, vintage)` 限现有 8 序列及 31 天观察窗、显式历史 vintage；固定 HTTPS origin，读取 observations 完整分页和 series 元数据，max_attempts=1。原件先保存、无 key 的参数/分页/实际收包凭证完整；没有原件的 D 摘要不冒充来源。observations.units 必须为 lin 原值变换，实际单位来自匹配的 series metadata.units，不能从 fixture 或列名猜单位。
- 每个经济日期身份独立，revision=`local-{vintage}:{observed}:{canonical_sha}`、object=`{series}:{revision}`，本地 hash 不称作上游修订号。小数精度保留；`.` 缺值保存于原件/canonical missing_dates，不写零；全缺值不产生合格行。available_at 至少为全部原件 receipt、calendar adoption 发布时间、vintage 后下一 XNYS 开盘的最大值；新取得的历史 vintage 不提前暴露到旧研究。
- 合同为 `research/fred_observations.py` 的 request/parser、`research/fred_storage.py` 的 `import_fred_vintage(session, *, collection, store)->tuple[UUID,...]` 与 `read_fred_observations(session, *, as_of, evidence_ids=None)->dict[UUID,FredResearchObservation]`。值含 observation_id/calendar_evidence_ref_id、series/observed_on/value/unit/vintage/available_at 及 receipt/hash。读端核新 canonical/原件/metadata 与两份证据的精确可见性；旧 fx/legacy 数据保持原读法，不误认为新格式。
- root 仅在原研究基底非空时附加宏观上下文，每个 series 取最新可见日期/版本并同时冻结 macro/calendar 两个 ID；保留原 issuer/action/profile、price/context 预留和 100 引用上限，不只截取 macro ID。`get_macro_snapshot` 精确读取双绑定并保留旧 v1 回放。旧批采另补同 identity 不同经济期间的 fail-closed 回归，不能继续静默漏行；不借本片启动旧批任务或扩大采集。

#### 执行与验收顺序

- [x] 两 owner 按冻结合同先红后绿完成 adapter/原件/canonical/storage 的单位、时间、身份与安全单测；root 同时先用旧 guard 红测 FMP 合法绑定，再实现 0042/降级保护。
- [x] root 吸收合同稳定代码后接正常 prepare→精确工具，补 profile/macro-only 不启动研究、同 vintage 多日期、旧冻结不变、坏 optional/截断/未来/缺原件/重放与原子性 PG。HTTP/S3 只允许边界 fixture，不 mock guard/reader/normalizer 或手填最终研究。
- [x] 集中一轮审查、必要修复定向复核，逐个小提交交 A 串行集成；同代码环境合同的验证复用，增量记录在本节。
- [x] 用各源正常入口采集一份公开代表样本并保存原件，不重复早先连接探测；A 按已就绪依赖完成实际存储与精确消费者核验。页面/API展示若尚未接线须明确标为未完成，不能用工具测试声称 DOM 验收。

这些来源只补当前可追溯研究材料；FMP profile、FRED 宏观或 Alpaca 当期行动观测均不证明历史股票池、行业风控映射、复权完整性、费用或投资方向充分性。真实经济比较与历史回测资格保持原门禁。

增量验证：0042 合法绑定先在旧 guard 下失败，再以 18 项隔离 PG 核验新 guard/负例/有冻结数据拒绝降级/空库往返；该子面集中审查无 P1/P2。FMP 正常 archive→import→prepare→admission→精确工具先因未附加 profile 失败，接线后两项新 PG 连既有 Alpaca 研究共 6 passed；证明新研究可读取公司背景、旧引用不变、profile-only 不创建研究，不证明页面接线。共享两源码 Mypy、Ruff 通过；header/归档/CLI 共 38 unit 通过。共享消费者增量审查 Ready；整片仍待已发现的身份标识冲突 P2 导入/reader 修复及定向验收，未交 A 应用业务迁移。以上 PG 均使用隔离测试数据库，没有修改现有业务数据。

后续闭合：FMP 身份 P2 导入与 reader 均按原 receipt 时点修复，14 项 producer PG 覆盖及定向审查通过；A 已将完整片集成到 main，完成完整备份、0042 迁移与真实原件正常导入/DB/hash/新 prepare→精确工具验收，旧 Alpaca 冻结不改。详情沿用 `docs/plans/2026-09-02-core-evidence-checkpoint.md` 的 FMP 0042 验收记录；仍无 FMP 页面/API展示，不重复采集或导入。

FRED 增量：parser/collector/storage/CLI 使用现有 HTTP bytes、原件和 metadata 单位；正常实际采集公开 DGS10 历史 vintage 得到 metadata + 1 页、4 条观测，单位 Percent、显式缺值 0，已离线 load 校验，不推断缺少经济日期的原因或历史完整性。三项时钟/配置审查问题及内部 replay 的格式/authority 联动均修复：来源 receipt 不变，实际 DB 创建时间逐项限制可见性，adoption 按当前导入时刻核验，未来 available 的内部一致性重放不豁免其他合同，CLI 保留正常进程配置覆盖。producer 11 PG、后续 5 聚焦 PG、55 unit/静态检查通过；旧批采同 revision 跨日期静默丢行先 RED 后以 6 行拒绝检查修复，原正常重放和碰撞 2 PG 通过。

共享 FRED prepare→admission→tool 正常路径先因未附加宏观证据失败，再接入每 series 最新可见经济日期/vintage/receipt 与 calendar 双引用；同钟数值/单位矛盾不选，等价重复选择稳定引用，保留其他研究与两个预留槽。新格式同时按 canonical schema/hash 和 local revision 识别，坏新格式不得回退旧协议，严格结果还须匹配精确 observation ID。同引用异 series 的真实 PG 反例（不关闭 FK/guard）先错误读出额外行，修复后与 FMP/旧 FX 共 5 PG 通过；最新共享及 FRED 聚焦 34 unit、共享两源码 Mypy/Ruff 通过。集中审查与修复定向复核 Spec/Quality Ready。主线负责人可先并行集成已 Ready 的 producer/CLI 与真实导入，再接收共享读端提交；运行/真实消费者验收以接收方结果为准，不把发出交接当作完成。

实际闭合：A已将FRED五项交付串行合入，API源码/镜像6200c38，业务库仍0042；真实归档正常CLI一次导入4条，原件/canonical/calendar对象hash与DB reader一致。新prepare双引用→精确get_macro_snapshot通过，旧FMP/Alpaca冻结不回填；临时admit事务回滚，无持久研究/预算/Job/模型。详情见现有core-evidence-checkpoint的FRED真实验收段。FMP/FRED页面/API仍未接线，不能把本片当作整体产品目标完成。
