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

下表保留初始排期口径；接续执行先读第 9.1 节的核验进度及其证据，不根据初始“待做”重复实现。

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
