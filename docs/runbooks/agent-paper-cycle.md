# Agent 内部模拟周期

本手册说明组合周期在创建后的研究、恢复和模拟成交步骤。正常组合入口由
[组合管理第一切片](portfolio-management-first-slice.md) 负责：候选是显式冻结的 1–5 个标的，
每个 XNYS 交易日只允许纽约时间 10:00 的一个组合周期。历史 AAPL 五分钟周期只保留给隔离的
Agent-version evaluation，不能通过正常组合入口继续创建。周期只产生内部模拟
订单、成交和账本事实，不访问 Schwab 订单接口，不连接券商交易域，不解除
`execution_locked=true`，也不等于券商模拟盘或实盘。

页面和只读 API 只读取已持久化事实；刷新 `/workspace`、`/decisions` 或 `/paper` 不会触发行情、
模型或周期推进。功能开关
默认关闭，失败不自动重试。正常组合每个候选各有一个绑定研究 Job，每个 Job 至多执行一次；
1–5 个结果全部进入同一 synthesis。不得在 10:05 或当天其他五分钟槽创建第二个正常组合周期。

## 1. Preflight：先查状态，不先发请求

在等待 10:00 正式窗口时，可先运行只读候选预检：

```bash
uv run --project engine nexus-engine-agent-paper-readiness --repo-root "$PWD"
```

它从受保护的本地配置读取现有账户和候选批次，在数据库只读事务中核对下一适用
XNYS 交易日、批次与正式研究 Universe、组合合成提示词的有效登记、每只候选的研究快照、
已审核行业证据和前一完整交易日 Alpaca `all` 日线权威。`blocked` 会逐只列出候选缺口，
`synthesis_prompt_status` 单独显示提示词是否有效；
`research_inputs.current_trends` 另列出每只候选的前一交易日趋势证据是否齐全，及其是否已绑定
当前研究快照。它是补充研究输入的提前诊断，不改变 `ready_static` 的交易准入含义；
盘前尚未绑定时显示 `snapshot_status=missing`，不能将证据存在误写为已被 Agent 消费。
`research_inputs.macro_sources` 检验现已持久化的六项 FRED 原件能否在下一决策槽使用；
`projected_for_slot` 表示原件校验通过但当前尚不可见，`available_at` 是预计最早可用时刻，
`missing_series` 显示未通过校验的序列；`snapshot_bindings` 逐只核对当前研究快照是否同时绑定
每个序列的观察与日历证据。正式周期仍按真实决策时刻重新核验可见性与快照绑定，
不能将这一预测当作已发生的研究或订单准入。
`ready_static` 只表示上述已存证据齐备，返回中的
`window_evidence_status=not_evaluated` 表示同日 Schwab 行情、风险查询、
研究 Job、内部模拟执行和页面结果仍须分别验收。该命令不调用 Provider、模型，
不创建周期、订单或任何业务事实。`scheduled_trigger` 只读回 `decision_session`
对应的正常槽位，`current_session_cycles` 读回纽约当地日期已创建的组合周期；
盘后预检下一交易日时，前一交易日的失败触发不会显示成下一交易日的失败。
`normal_cycle_start_status=blocked_scheduled_claim/blocked_existing_cycle` 时不得换 key
启动新的正常周期。`requires_window_evidence` 也不等于可以直接触发，仍须核对
当日风险和行情证据。运行前还须核对 `nexus-local-stack status` 的
代码提交身份；宿主提交与当前 checkout 不一致时状态命令会报
`AGENT_PAPER_HEARTBEAT_RELEASE_MISMATCH`。

本机 Web 已在 `127.0.0.1:3000` 运行且 `nexus-local-stack status` 确认同版本后，可在当天反复运行只读页面整链核对：

```bash
NEXUS_LOCAL_PAPER_READONLY=1 ./node_modules/.bin/playwright test \
  --config playwright.local-paper.config.ts
```

该用例从正常 BFF 读取一条已完成的组合周期，核对决策、提案、当前目标在 API 中的身份一致，再打开 Workspace、Decisions 列表与详情、Paper、Portfolio；它拒绝浏览期间的写请求，并在最后复核业务身份未变。浏览器不保存截图、录屏或 trace。需要已有完成周期；通过只证明这些持久化事实的页面可读性，不替代真实行情、自然时间窗或成交验收。

只在当前代码、API、普通 Worker、PostgreSQL、MinIO 和 Web 来自同一 head，数据库位于当前
Alembic head，且本地 BFF readiness 可解释时继续。`/health/live` 或 Compose `Up` 不足以证明
业务就绪。

```bash
git rev-parse HEAD
uv run --project engine alembic -c engine/alembic.ini current
uv run --project engine alembic -c engine/alembic.ini heads
uv run --project engine nexus-openai-local status --backend codex_sdk
TZ=America/New_York date
```

通过受保护的本地数据库/API 工具按以下顺序读取，不把账户、金额、凭据或完整 payload 复制到
聊天和验收记录：

```text
cycle → cycle events → research Job/Run/Output → decision/bindings
→ batch/orders → raw artifact/manifest/window → fills/ledger/performance
→ Engine API/BFF/page
```

发现当天已有正常组合周期时停止新建，恢复已有 `CYCLE_ID`；错过 10:00 窗口不补跑。
发现 queued/running Job 时先读取其状态；断流、观察超时、worker 退出或终端输出丢失都不是换 key
重放的许可。存在非终态订单时先对账，不删除事实、不重建周期。`unavailable` 是保留的诚实终态，
不恢复、不删除；到下一合法槽后可以新建周期。

当天 10:00 行情采集失败且未创建周期时，唯一例外是用户明确要求的
[同日一次手动恢复](portfolio-management-first-slice.md#同日一次手动恢复)。它有独立入口和持久化
认领，不改变上述正常组合时间窗，也不重放失败槽位。
该手动周期若因随后发现的合成提示词缺失而在成功研究后终止，只能按
[合成配置缺失后的独立恢复](portfolio-management-first-slice.md#合成配置缺失后的独立恢复)
审查一次新的周期；原终态不可重开，第二次仍失败则当天停止。

运行前还必须满足：

- 1–5 个候选都来自同一个已发布的正式研究 Universe（历史 `us_liquid/us_liquid_v1` 或
  `agent_paper_research/reviewed_v1_*`）批次，且各自有当前研究快照、前一完整交易日
  Alpaca SIP delayed `all` 日线、审核行业 authority、完整 XNYS 日历和三类风险查询证据；
- 宏观工具只读取当前时点可见的 `DFF`、`DGS2`、`DGS10`、`CPIAUCSL`、`UNRATE`
  和 `VIXCLS` 严格 FRED vintage；宏观证据不是价格、风控或下单权威；
- 已幂等登记 actionable v4 模型/提示词配置，模型预算可用；
- `infra/.env.local` 保持受保护且未进入 Git；动态 Schwab OAuth token 只能位于主仓库
  `.secrets/nexus-schwab-local/tokens.json`，目录和文件权限分别为 `0700`、`0600`；
- `NEXUS_AGENT_PAPER_CYCLE_ENABLED=true` 仅在本轮手工窗口内启用。模板及默认值仍为 false；
- 当前纽约时间处于合法 XNYS 交易日的 `10:00–10:04:59` 正常组合窗口。

### 10:00 前行情边界

2026-09-15 约 05:45 ET 的一次 20 秒有界实测中，Schwab `LEVELONE_EQUITIES` 为 AAPL 和 NVDA
返回了 `delayed=false` 的双边报价与最近成交；这说明 07:00 前可以做只读行情观察。该归档没有
导入业务库。随后对 NVDA 前五个完整分钟仅请求一次 `pricehistory`，Provider 返回
`SCHWAB_PRICE_HISTORY_RESPONSE_INVALID`，未满足现有精确 5×1 分钟执行窗口合同，且按无重试规则
停止。

因此不要把“WebSocket 有盘前报价”推导为“完整订单闭环已可运行”。当前组合循环只在 10:00 ET
单次窗口开始：决策报价继续使用 Level One，订单成交权威继续使用通过合同校验的 Schwab pricehistory
分钟线。不要临时改用 WebSocket 成交事件拼 K 线，不新增 04:00–07:00 决策专用状态机；若未来
要提前正式窗口，应先用正常产品入口取得一次合格的精确五分钟 pricehistory 证据，再单独评审。

登记当前 actionable 研究版本时只登记配置，不创建 Job、不调用模型：

```bash
uv run --project engine nexus-openai-local setup \
  --backend codex_sdk \
  --structured-synthesis \
  --aggregate-input-tokens 96000 \
  --max-wire-bytes 64000 \
  --inline-citations \
  --actionable-paper \
  --skip-key
```

当前入口登记 `codex-sdk-chatgpt-v5-paper-scope`，预算仍为 96k 聚合输入和 64k wire，
不增加来源请求、模型轮数或执行权限。v5 明确单股研究使用冻结的候选仓位和现金比例；
完整组合分配与约束交由后续 `portfolio_decision` 和确定性风控处理。阶段分工不能消除
真实证据缺口或审计否决。`v4-actionable` 保留原输入说明，旧 Job 继续使用冻结版本；
已明确选定的 Agent 版本、旧 Job 和相同幂等请求继续使用冻结版本。
未显式选定版本的新正式周期可使用最新登记且合格的 v5 配置，同时沿用历史冻结角色提示词；
历史版本完整性校验仍然保留。登记 v5 后须核对正式入口的实际选择，不代表自然周期验收已通过。
无需迁移或改写旧 Run/Output；回滚旧代码前须确认没有运行中的 v5 Job，
旧代码继续选择其支持的 v4 配置，保留 v5 已落盘事实。

## 2. Trigger：冻结一次决策证据

`trigger` 不接受 symbol 或 decision time。数据库固定认领当日纽约时间 10:00 的唯一正常组合周期；
缺少现成风险查询证明时，按既有只读 Alpaca 入口补齐一次，然后对批次中的每个候选做一次有界 Schwab Level One
采集。Schwab 路径固定一次 LOGIN、一次订阅、最多 15 秒/100 帧/8 MiB，不重连、不重试。先按
[组合管理第一切片](portfolio-management-first-slice.md) 创建并核对候选批次；`trigger` 必须显式
绑定该批次，不能从 symbol 或环境默认值推断。

```bash
uv run --project engine nexus-engine-agent-paper-once trigger \
  --account-id "$PAPER_ACCOUNT_ID" \
  --candidate-batch-id "$CANDIDATE_BATCH_ID" \
  --idempotency-key "$TRIGGER_KEY"
```

只接受返回中冻结的 `CYCLE_ID`，并核对决策证据是 `schwab/l1/real_time`、`delayed=false`、
双边报价完整且 quote/artifact/hash 绑定一致。命令结果不明时读取持久状态；只可使用原
`TRIGGER_KEY` 恢复，不能换 key 再请求行情。

以下任一情况立即停止本轮：`AGENT_PAPER_OUTSIDE_WINDOW`、`RISK_INPUT_REQUIRED`、
`SCHWAB_L1_FRESHNESS_UNPROVEN`、`SCHWAB_L1_INCOMPLETE`、候选或证据绑定冲突。不得换数据源、
放宽时间/质量门槛或借用 Shadow 路径。

## 3. Run research：逐个消费该周期已持久化的 Job

使用 `trigger` 返回的 `CYCLE_ID`。入口从该周期的持久化事件解析唯一 Run，只允许领取与之绑定的
queued `agent.paper.research` Job。每次命令最多领取一个；只有前一 Job 已有明确持久终态时，才为同一
`CYCLE_ID` 再次运行以领取下一个候选 Job，直到 1–5 个候选都终结：

```bash
uv run --project engine nexus-engine-agent-paper-once run-research \
  --cycle-id "$CYCLE_ID"
```

该命令最多处理一个 Job 后立即退出，不扫描普通研究 Job，不执行 `resume`，也不创建新周期。
如果 Job 已在运行、已终态、绑定冲突、已有 Output 或调用结果不明，命令返回
`AGENT_PAPER_RESULT_UNKNOWN`；先查原 Cycle/Job/Run/Output，不再次运行 worker。只有持久化 Output 终态明确后才继续。

## 4. Resume：消费已持久化研究结果

```bash
uv run --project engine nexus-engine-agent-paper-once resume \
  --cycle-id "$CYCLE_ID" \
  --idempotency-key "$RESUME_KEY"
```

`resume` 不调用模型、不采集行情，只读取已持久化 Job/Run/Output，并生成确定性 sizing、硬风控、
批次和内部模拟订单。用同一个 `RESUME_KEY` 处理结果不明；不要轮询或换 key。合法结果包括：

- 首次 `resume` 在候选研究全部终态后登记唯一的 `agent.paper.portfolio_synthesis` Job，并返回
  `PORTFOLIO_SYNTHESIS_PENDING`。此时再次执行一次上节的 `run-research --cycle-id "$CYCLE_ID"`
  只领取该 synthesis Job；确认其 Output 已持久化后，再用原 `RESUME_KEY` 执行 `resume`。

- `NO_ORDER_REQUIRED`：maintain/none 是真实不交易结论，没有订单；
- `PAPER_ORDERS_AWAITING_MARKET`：已有风控通过订单，等待五分钟窗口；
- risk rejected：保留拒绝事件，不创建成交；
- research unavailable/failed：保留终态和固定错误码，不替换 Run。

## 5. Collect window：一次 Schwab 五分钟窗口

窗口由周期和 Output 时间确定，CLI 不接受 instrument 或 window-start。请求固定
`price_history/1Min/raw`、`needExtendedHoursData=true`、`[start,start+5m)`，只发一个 GET；响应
实时性未由 Schwab price history 声明，因此持久化为 `delivery_mode=unknown`，不得展示成实时。

```bash
uv run --project engine nexus-engine-agent-paper-once collect-window \
  --cycle-id "$CYCLE_ID"
```

有订单时窗口绑定该订单并用于 `paper_fill`；无订单时只保存一个 `acceptance_probe`，证明行情
合同但绝不制造订单或成交。`PAPER_MARKET_WINDOW_UNAVAILABLE` 表示本次窗口没有形成合格
authority：停止并保留 cycle/order 原状，不重试、不补造 bar、不用最新行情替换冻结窗口。

## 6. Advance：只推进已有内部模拟订单

`resume` 不执行订单。只有上一步已持久化与该订单精确绑定的五根连续分钟线时，才复用现有内部
模拟入口推进；`EVALUATED_AT` 使用已到达窗口的实际核对时间，不伪造未来时间。下述命令只用于尚未进入
受管 v2 窗口协议的历史订单及其原 key 结果核对；某订单一旦已产生 v2 窗口请求，
禁止再用该无窗口命令推进，新请求会在任何成交、撤单或幂等写入前被拒绝。
受管循环必须由正常 Agent Paper 宿主使用持久化的 cycle/window 恢复。

```bash
uv run --project engine python -m nexus.ops.paper_once advance \
  --account-id "$PAPER_ACCOUNT_ID" \
  --order-id "$ORDER_ID" \
  --evaluated-at "$EVALUATED_AT" \
  --idempotency-key "$ADVANCE_KEY"
```

该入口使用现有确定性 VWAP、容量、滑点、reservation、fill 和 ledger 逻辑。`no_data`、
`no_capacity` 或部分成交都是诚实结果；原 `ADVANCE_KEY` 可用于查明不确定结果，但不能通过换 key
重复成交。成交后无需再用 `resume` 推单，进入只读核对。

## 7. DB / API / BFF / page readback

依次核对同一 `CYCLE_ID`：cycle/event hash 与关联 ID；唯一 Job/Run/Output；decision 和风险结果；
订单最新事件；Schwab raw artifact/manifest/window；fill、reservation、借贷平衡 ledger 和绩效。
不得只看 CLI 文本、HTTP 200 或页面标签。

Engine `/internal/v1/paper/cycles/latest`、同源 BFF `/api/v1/paper/cycles/latest`、工作台
`/workspace`、决策详情 `/decisions/{cycleId}` 和 `/paper` 必须引用同一周期。页面应分别显示：

- `决策报价 · Schwab Level One · 实时 · <冻结报价时间>`；
- `执行分钟线 · Schwab pricehistory · 实时性未由响应声明`；
- `execution_locked=true`，且没有创建周期、再次请求行情或券商下单按钮。

工作台上“循环在线”只来自 30 秒内且版本兼容的 `agent_paper` heartbeat；“最近周期已完成”只来自
已保存周期，二者互不替代。heartbeat 过期、缺失或版本不兼容时必须显示离线或未知，即使数据库中
存在最近完成周期。刷新页面只重读已保存事实，不启动或重放 Agent。

只读浏览器核对顺序：

```text
/workspace
→ 一次点击最新决策
→ /decisions/{cycleId}
→ 对照 cycle、decision、order ID 与时间
→ /paper 核对账户、订单和同窗结果
```

没有活动订单时，“无需下单”或“当前没有活动订单”是有效状态；没有精确关联订单且结果不能证明时
必须显示“结果未知”，只允许进入核对原操作结果，不提供重试交易。

同 head 本地栈的浏览器只读验收入口：

```bash
NEXUS_REAL_PORTFOLIO_MANAGEMENT_ACCEPTANCE=1 npm run test:e2e -- \
  tests/e2e/portfolio-management-real.spec.ts
```

### 单次组合周期固定验收顺序

周期使用受保护的本地变量 `NEXUS_ACCEPTANCE_PAPER_ACCOUNT_ID` 和各步骤幂等键；值不得写入命令历史、
聊天、Git 或验收文档。每一个动作前都先按本节开头的事实链顺序读取持久状态，固定流程为：

```text
read current cycle/job/order state
→ trigger 10:00 portfolio cycle
→ run existing one-shot worker（仅当该 Job 为 queued）
→ resume to admit synthesis
→ run synthesis worker（仅当该 Job 为 queued）
→ resume with the original key
→ collect-window（仅当存在订单）并 advance
→ resume and read DB/API/BFF/page
→ cycle 与订单均终态后关闭本次手工窗口
→ read the cycle and disable the feature flag
```

同一交易日不会创建第二个组合决策周期。该周期允许合规 no-trade；但必须形成组合决策并经过确定性
硬风控。使用原幂等键回读不得增加 Job、订单、成交或账本。连接或命令结果未知时只
查原 `run → job → outputs` 和业务事实，不换 key、不重放。原始模型全文、token 和个人账户事实不
进入 Git 或验收记录。

## 8. 连续服务与前台诊断

手工单次组合周期验收完成后，宿主服务仅在下一交易日 10:00 ET 再创建新周期。执行分钟窗口只推进
当前周期已有订单，不代表新的决策槽；每个交易日最多一个组合 cycle，包含 1–5 个候选研究 Job 和
一个 synthesis Job。
正常启停统一使用本地后端入口；它先完成基础 Compose 步骤，仅在受保护配置明确启用时启动宿主机
Agent Paper，停止时则先确认 Agent Paper 已安全退出，再停止 Compose：

```bash
uv run --project engine nexus-local-stack up \
  --repo-root "$PWD" --env-file infra/.env.local --project-name nexus-fosun-live
uv run --project engine nexus-local-stack status \
  --repo-root "$PWD" --env-file infra/.env.local --project-name nexus-fosun-live
uv run --project engine nexus-local-stack down \
  --repo-root "$PWD" --env-file infra/.env.local --project-name nexus-fosun-live
```

`status` 将 Compose、精确归属的宿主机托管状态、数据库 heartbeat 和零外部请求 preflight 作为
四项独立事实报告。Agent Paper 离线不表示 Web/API 离线；页面仍只读取持久化 heartbeat、周期、
Job、Output、订单和窗口，刷新不会启动或重放任何调用。

以下前台命令仅用于开发诊断，不是另一套后台服务：

```bash
AGENT_RUNTIME_MODE=codex_sdk uv run --project engine nexus-engine-agent-paper-loop \
  --account-id "$PAPER_ACCOUNT_ID"
```

每一步都先读已持久化的 cycle、Job、Output、订单和窗口：同槽已完成则等待下一槽；queued Job 只按
该 cycle 的精确 Job ID 领取；running、缺失或结果不明立即停止，不换 key。订单窗口到达后再额外
等待 60 秒发布余量，只请求一次 Schwab pricehistory；窗口合格后才推进对应内部订单。

宿主在正式交易日 09:30 ET 开盘后先对已发布候选池做一次有界预准备：前一完整交易日日线权威
→ 六项 FRED 宏观证据及研究快照绑定 → 当前价格趋势。预准备只写研究输入，不创建 cycle、
订单或交易窗口；已知来源不可用时仍检查其他候选，未知结果立即停止。宿主随后等到 10:00 ET
的唯一正常组合槽；正式触发前仍逐项回读校验，缺失时按原有幂等入口准备，绝不因预准备成功
提前创建周期。服务重启后预准备可以再次回读，已有原件和失败尝试不重复请求 Provider。

FRED 宏观准备复用现有固定 origin 采集和严格导入合同，按 XNYS 交易日保留一个幂等
尝试目录。六项已完整且当前快照已绑定时不请求 Provider。缺失时只采集缺失序列；
已知配置或 Provider 失败使宏观工具保持 unavailable，Agent 仍可依其他已冻结证据作出诚实
决策；导入、数据库或回读结果不明则立即 fail closed。不删除失败尝试目录来重试，不切换
数据源，不用当天后来发布的 vintage 回填已有决策。采集失败时，私有尝试目录的
`failure.json` 只记录失败阶段、分页偏移和固定安全码；诊断时先读它，不读取或转发原始响应，
也不把失败目录作为可导入的完整归档。

普通组合槽出现 `unavailable` 或 `failed` 时，保留该终态事实，同槽不重复 Provider 或模型调用；
宿主保持心跳，先在已冻结收盘时点核对绩效，再等待下一个合法的纽约 10:00 组合决策槽。
已注册版本评价继续按其冻结槽位运行，不受普通组合候选批次配置缺失影响。
周期建立前的 Schwab 已知终态失败必须回读同账户、槽位、route 和 schema 的持久化失败声明；
仅回读一致的 `SCHWAB_L1_FRESHNESS_UNPROVEN` / `SCHWAB_L1_INCOMPLETE` 可等待后续槽。
未知结果仍停止，不能凭异常文本重放；综合决策未知、政策未确认和目标待生效保留各自安全边界。
日线、宏观或趋势补齐已有失败尝试时不会删目录或换 key 重放 Provider。已有订单继续使用其冻结窗口。
运行中将
`NEXUS_AGENT_PAPER_CYCLE_ENABLED=false` 或按 `Ctrl-C` 都会停止；历史事实不删除。

内部托管适配器在 Mac 宿主机使用 Codex SDK 登录，不复制凭据、不注入 OpenAI API key，也没有
Compose Agent Paper profile。适配器在 `.secrets/nexus-agent-paper` 维护临时注册，并将 0600 日志写入
`~/Library/Logs/nexus-agent-paper` 的 0700 私有目录；正常使用不直接调用该脚本。

预检只读取本地配置、受保护 token 文件、私有目录权限和 Codex SDK 本机登录状态；不刷新
ChatGPT 登录、不连接数据库、不请求券商、不调用模型；
它一次返回全部固定阻塞码，`status=ready` 才允许进入启动步骤。输出中的
`execution_locked=true` 不代表会解锁交易，`external_requests=0` 表示本次预检没有外部调用。
FRED 未配置只作为可选提示；Schwab access token 已过期时提示首次行情请求需要刷新，其他必需项
缺失均 fail closed。行情请求发现 token 已过期或将在五分钟内到期时，只调用一次既有 refresh
端点；成功后原子替换私密文件并继续，失败则保留旧文件、不重试且不请求 K 线。

先在 `infra/.env.local` 明确配置账户 ID，并保持功能开关为 true。预检
同时核对动态 Schwab token 目录可写；该目录只用于一次刷新后的原子替换。预检发现 token 挂载
只读时拒绝启动。
服务退出后先读取数据库终态和日志中的固定错误码，不直接重启。回滚只需将开关恢复为 false，并
运行统一 `down`；不会删除或改写 cycle、Job、Output、订单、成交、账本和心跳事实。

若普通 `down` 返回 `AGENT_PAPER_SERVICE_STOP_UNCONFIRMED`（退出码非零或缺失，75 是典型情况），
不得重复停止 Compose 或盲目重启。先只读核对原 `cycle → job → outputs → order/window`，
确认没有结果未知、待对账或仍需依赖服务的动作；只有完成该人工核对后，才可对同一
repo/env/project 使用内部恢复命令清理已退出注册。该命令只接受归属精确匹配、已确认
inactive 且未能证明正常退出的注册；running 或退出码 0 均拒绝。它只移除注册，不发信号、
不改业务事实：

```bash
.venv/bin/python scripts/macos/agent-paper-loop-service.py stop \
  --repo-root "$PWD" --env-file "$PWD/infra/.env.local" \
  --project-name nexus-fosun-live --acknowledge-unconfirmed
uv run --project engine nexus-local-stack down \
  --repo-root "$PWD" --env-file infra/.env.local --project-name nexus-fosun-live
```

## 9. Stop：默认关闭并保留现场

1. 将 `NEXUS_AGENT_PAPER_CYCLE_ENABLED=false`，再运行统一 `nexus-local-stack down`。它仍会检查并
   停止同一 repo/env/project 遗留的 Agent Paper 注册；功能开关不会掩盖实际托管状态。
2. 只读确认没有未解释的 queued/running Job，没有结果未知的模型调用，也没有未对账的
   `proposed`、`risk_approved`、`submitted`、`partially_filled` 订单。
3. 对 `prepared`、`researching`、`decision_recorded`、`risk_evaluated` 或 `awaiting_market` 周期，
   保留现场并记录精确阻塞；不得删记录来制造完成状态。
4. 不删除 cycle、event、Run/Job/Output、订单、raw、window、fill、ledger 或 performance。关闭开关
   只阻止新手工调用，不改写历史事实。

## 10. Rollback：应用回退与数据回退分开

首选应用回退：保持 `NEXUS_AGENT_PAPER_CYCLE_ENABLED=false`，回退应用版本但保留数据库 head 和
所有事实。FRED 自动准备没有新迁移或新表；回退应用时保留已导入的原件、宏观观察和
研究快照，不删数据来伪造“未运行”状态。出现数据合同问题时前向修复；Git 回退不是业务数据恢复。

迁移 `0058_agent_paper_schwab_realtime` 与 `0059_agent_paper_intraday_cycles` 的降级只允许在专用
空临时数据库演练。先按
[备份恢复手册](backup-restore.md)完成独立备份/恢复演练，再对临时库执行：

```bash
DATABASE_URL="$EMPTY_TEMP_DATABASE_URL" \
  uv run --project engine alembic -c engine/alembic.ini upgrade 0059_agent_paper_intraday_cycles
DATABASE_URL="$EMPTY_TEMP_DATABASE_URL" \
  uv run --project engine alembic -c engine/alembic.ini downgrade 0058_agent_paper_schwab_realtime
DATABASE_URL="$EMPTY_TEMP_DATABASE_URL" \
  uv run --project engine alembic -c engine/alembic.ini downgrade 0057_agent_paper_market_window
DATABASE_URL="$EMPTY_TEMP_DATABASE_URL" \
  uv run --project engine alembic -c engine/alembic.ini upgrade 0059_agent_paper_intraday_cycles
```

存在同账户同交易日的多 cycle、多 Agent decision 或同 scope 多版 risk proof 时，0059 downgrade
必须以 `AGENT_PAPER_INTRADAY_FACTS_EXIST` 失败；存在任一 Schwab Agent cycle、decision、authority
或 window 事实时，0058 downgrade 必须以 `SCHWAB_AGENT_PAPER_FACTS_EXIST` 失败。不得为降级删除
事实，不得在当前业务库试跑，不删除 MinIO bucket、volume、数据库或迁移历史。
