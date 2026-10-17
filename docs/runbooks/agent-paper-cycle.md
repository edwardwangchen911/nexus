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

### 已有研究请求的只读大小诊断

在调用或重放模型之前，可针对已有 Run 读取冻结输入并重建请求：

```bash
uv run --project engine nexus-engine-research-workload-inspection \
  --repo-root "$PWD" --run-id "$RUN_ID"
```

该入口支持正式 v8 和既有受支持版本，仅在只读事务中读取现有事实，不调用模型、
不领取 Job、不改变预算或执行锁。`research.wire_limit_status=exceeded` 表示当前代码
重建的研究请求超过其冻结配置的上限；`component_bytes` 仅包含固定输入类别的大小，
不输出内容。构造委员会所需的观察期限不是一次新的执行期限。

`original_request_hash_matched=true` 必须有完整且校验通过的原研究角色、冻结输入与
请求哈希匹配；原调用前已拒绝、没有原请求哈希时，仅报告 `current_code_reconstruction`。
critique 只使用它之前的五个研究角色，缺少原研究时明确为不可核验，不制造替代分析。
命令成功仅表示诊断完成，研究阶段未超限不证明未来 critique/synthesis 也未超限，
更不代表来源当前有效、业务准入或自然周期验收通过。诊断结果不允许重放失败周期。

候选财务表呈现可显式加 `--financial-table-candidate`：保留基线哈希及输入，另行报告
`candidate_presentation`。候选仅将全部财务事实按指标及来源/期间组织，保留单位、
原文标签、修订限制、空值、重复事实和原顺序；引用索引先走原生绑定，再编译候选。
完整 wire（包括解释说明）没有净节省时保留基线表示。该选项仅供只读观察，不登记
配置、不调用模型、不修改正式 v8 的默认编译或激活候选。仅存在校验通过的前序输出
时才编译 critique/synthesis；缺失时报告不可核验，不用短假 claims 宣称整链通过。

预算判读须按真实门禁：调用前是此前 SDK 报告的 input tokens 加下一次预检估算不超过
96k；调用后核验真实累计 input/output 不超过 96k/8192。SDK 单次入口另检查预检估算
不超过 64k。三次预检估算之和是诊断量，不是已消费用量，不能与结算混为一谈。

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

## 11. 显式新手动请求（隔离候选，尚未启用）

该入口是用户批准的独立新请求，支持 `research_only` / `research_and_paper`，不重放旧定时失败槽。
目前已完成本地请求、原生周期、受保护 API/CLI，以及只读唤醒、一次领取和显式收尾的隔离验收。
正常宿主已接通只读队列、首次领取 dispatcher 和显式收尾调度；未知启动不重复调用。
正常 trigger 的受控来源和隔离数据库成功创建已验证，手动来源冻结要求盘内；已确认且无占用的
失败终态可显式收尾；已证明完全未执行的研究可在宿主收尾中原生取消并释放预留，未知用量和
其他有效租约继续阻止收尾。宿主协议与正常消费者已通过受控隔离验收；Workspace 显式动作、BFF 和浏览器回执流程已接通。真实来源整链与自然周期验收仍待完成。
不在当前业务库执行下述 start，不据此启用服务。完成宿主与页面验收及获准更新服务后才可使用。

正常入口只读受保护配置中的 `NEXUS_AGENT_PAPER_ACCOUNT_ID`、
`NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID`、`NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID`，
客户端不能提交账户、缩减候选池、切换模型或指定业务时刻。
`NEXUS_AGENT_PAPER_MANUAL_ENABLED` 默认 false；它不解除 execution_locked。
新请求还要求 cycle 开关开启、唯一宿主心跳 30 秒内新鲜、schema 和 release 一致，以及宿主的
`nexus-manual-paper-v1` 领取协议与账户/批次/模型/预算保护/启用配置的 scope 哈希一致。
当前定时宿主不声明该协议，因此即使只改开关也会明确拒绝，不会静默排队到次日。

API：HMAC 验证的 `POST /internal/v1/paper/manual-requests`，必需 `Idempotency-Key`，
body 仅含 `mode`；`GET /internal/v1/paper/manual-requests/{request_id}` 仅读持久化回执。
所有业务时刻及 meta.as_of 使用数据库时钟。另有 `GET /internal/v1/paper/manual-requests`，以原 `Idempotency-Key` 仅查配置账户的持久化回执；查无记录返回 data=null，不能据此重放提交。Workspace 通过受认证 BFF 的同源写入校验、严格 mode body 和原请求键接线。页面先保存请求键，结果不明时只查询原键；刷新不提交。确认终态或固定安全拒绝后须显式选择新请求。

CLI 使用同一服务，不采集来源、不调用模型、不创建周期或订单：

```bash
uv run --project engine nexus-engine-manual-paper --repo-root "$PWD" \
  --env-file infra/.env.local start --mode research_only --idempotency-key "$NEW_REQUEST_KEY"
uv run --project engine nexus-engine-manual-paper --repo-root "$PWD" \
  --env-file infra/.env.local read --request-id "$MANUAL_REQUEST_ID"
```

同键先读原始回执：关闭入口、候选批次轮换、收盘或宿主停止后也不重做采集。
同键改变模式拒绝；新键不绕过旧周期、订单/预留或未知结果。当前 reader 只支持盘内同日新请求，
盘外明确返回 `MANUAL_SESSION_UNAVAILABLE`，不自动变成次日执行。
正式定时预算保护开启时返回 `MANUAL_BUDGET_PROTECTED`，不动定时专用分配，不提高额度。
GET、CLI read、刷新及重复 POST 均不能领取、恢复或重跑未知请求。

0075 迁移增加不可变请求及回执、仅研究原生终态分支和数据库执行门禁；只在隔离库验证。
有手动事实时不得直接降级删除，应用回退保留 head 与原始事实；尚无主库迁移或服务部署。
旧应用对新 head 的兼容性须单独验证；不能把保留 schema 等同于旧镜像可直接启动。

### 本地隔离的柔性 critique 预算候选

`codex-sdk-chatgpt-v10-flexible-stage-budget` 仅用于隔离验证。正常 `nexus-openai-local setup`
需显式加 `--flexible-stage-budget`、现有完整输入配置参数及 `--database-url-file`（受保护的
`nexus_diag_` 连接文件）；实际数据库也必须通过隔离检查。业务数据库拒绝登记，默认选择、
正式周期和正式输出资格保持不变，不通过注册启用任何业务周期。

新配置的 Job/handler/lease 仍为 180/195/210 秒。critique 仅使用父截止扣除自身关闭 7 秒、
synthesis 完整 45+7 秒后的余量；terminal 5 秒已从父截止扣除。research 60 和 synthesis 45
上限不变，准备与关闭计入原截止；无余量不调用 SDK。新计时记录显式绑定候选版本；旧
v7/v8/v9 的配置哈希、分配和计时 JSON 保持原样。没有重试、额外调用、token 或字节额度。

隔离回归使用正常准备/原生候选准入、完整候选池及精确 Worker，SDK 外部端由替身返回。
这证明接线与边界，不证明自然模型会在期限内完成。未应用迁移、未正式登记或选择候选。
撤回时仅撤回本地候选差量；不得用候选结果改写旧失败或实际运行证据。


## 完整输入耗时诊断（本地候选，非正式验收）

`full-input-timing-diagnostic-v1` 仅用于独立测量历史完整输入的三阶段耗时，
不是 v9/v10 正式模型配置，不登记、选择模型或创建 Run/Job/周期。
入口支持精确冻结身份的 v9/v10 来源，只读核对原正式周期的完整候选清单、事件哈希链、同模型同版本和冻结快照归属，
在全池容量检查通过后只观察指定来源的一份研究。输出标记为当前代码重建，
不声称还原旧调用字节或继续旧请求；完整前序结论仍由正常 staged 验证器传递。

正常入口（只有另行批准真实诊断调用后才执行）：

```bash
uv run --project engine python -m nexus.ops.full_input_timing_diagnostic \
  --repo-root "$PWD" --source-run-id <历史终态研究Run-ID>
```

固定总观察上限 300 秒包含来源检查、初始化、三个阶段及清理；committee 截止为
总截止减 5 秒，每个阶段另留自身 7 秒清理及后续各 7 秒清理，允许观察首阶段超过
旧上限。最多三次调用、96k 输入/8192 输出与 100479 字节限制保持；不能设置任意上限。
这只测量耗时，不证明正式 180 秒合同能完成，也不改变 handler/lease 或执行许可。

规范私有输出是 `.secrets/nexus-full-input-timing/<source-run-id>/`。
来源加载前持久化独立身份及不可重放 attempt；叶子存在一律拒绝，不能删除或换目录
重试未知结果。result 仅保存阶段计时、用量已知部分和安全状态，不打印模型内容或账户事实。
失败或进程中断保留 attempt；旧业务事实不变。撤回候选代码时保留该私有归档。
回执另记加载/验证/编译合计、阶段总墙钟、SDK 分阶段耗时、首推理/首回答及终态通知时间。
通知时间只代表本机收到消息的时刻；远端排队与推理无法进一步拆分。SDK 外的阶段余量包含输出解析、绑定验证和本地调用开销，不冒充纯模型执行时间。
2026-10-17（北京时间）已完成一次获批的 v10 AVGO 冻结输入独立观察：总耗时
88.680 秒，research/critique/synthesis 分别为 45.629/22.776/14.746 秒；
三次调用用量完整，业务表前后摘要一致。它只证明本次观察完成，不还原旧请求的
远端耗时，不证明正式自然周期或尾延迟可靠性；正式 180/195/210 秒合同未调整。
具体证据见主线交接文档当日记录，私有 attempt/result 必须保留且不可重放。


## v10 正式柔性阶段预算接线（本地候选，尚未启用）

正式 v10 沿用 `codex-sdk-chatgpt-v10-flexible-stage-budget` 的精确冻结 hash。
新增 `--formal-flexible-stage-budget` 仅接受显式 formal/full-input 的正常 setup；
原 `--flexible-stage-budget` 仍只用于隔离库，不可隐式改为业务登记。
setup 只登记待选择版本，不创建研究或自动改运行配置；默认研究仍排除这些专用版本。

正式入口必须证明原周期的完整候选清单、事件链、同一冻结模型和快照 context。
Worker 根据已登记的精确 profile 使用原180/195/210秒合同，正常记录和读取阶段轨迹。
输出、组合合成、管理历史读取、目标基线、预算保护及未调用任务作废复用原资格验证；
300秒观察身份始终无业务模型、Job 或输出绑定资格。

迁移草案 `0078_formal_v10_output_binding` 只向现有 SQL 正式分支增加精确 v10 hash，
不改写0077或历史事实。后续切换前须备份并核验代码/数据库兼容性，先部署受支持版本再
登记和选择新模型；本轮没有对业务库执行这些动作。出现 v10 正式输出绑定时直接降级会
被 `V10_OUTPUT_BINDINGS_EXIST` 拒绝。回退应先停用 v10 新任务、保留所有事实；即使没有
绑定，也不能假定旧镜像能正确处理新登记模型，须单独核验消费者兼容性，不删除事实通关。

三/五候选买入及不交易受控 PostgreSQL整链（替身模型）、内部成交/账本及 API/BFF
读回已经通过，活跃目标作为下一轮基线的读取亦通过。研究耗时波动和真实自然周期仍未
证明稳定；180秒预算没有扩大，本机运行版本及选择仍是旧版本。

### 2026-10-15 same-day recovery candidate (local qualification only)

7617eded adds explicit --same-day while preserving the default prior-UTC-day guard. Only terminal failed research with expired lease, no occupied orders, quiescent processes and verified original frozen input/request/terminal metadata is eligible. Unknown usage and its reservation remain unchanged; no old invocation is replayed. New audit schema paper_same_day_close_v1 preserves old v1 audit semantics, verifies later reads at the original audit time and rejects changed facts. A second process check fences commit; late failure rolls back. 129 targeted tests, Ruff, Mypy and review passed; image candidate-4b540cf09743 matched 658 source files. Real failed input was checked read-only, not disposed. Main remains 199c754b/schema0078. No deployment, real seal or new cycle has run. Before rollback, stop new manual requests and retain all new facts; old consumers are not assumed compatible with new audit receipts. Private evidence: .secrets/same-day-close-qualification/verification.json.


### 2026-10-15 manual preparation diagnostics candidate

Candidate 92276fa3 fixes primary trigger diagnostics lost by manual dispatch, including secondary failure while recording unknown or disposing resources. Only fixed trigger_request/trigger_reservation/trigger_provider phases, allowlisted error codes and fixed exception kinds are emitted; original response text remains hidden. Unknown or preparing requests remain non-replayable. No migration, retry, admission, usage, budget or execution permission changes.

368 focused unit tests, 89 isolated PG regressions, 5 post-review targeted PG checks, Ruff, Mypy and independent review passed. Qualification covers same-day failed research disposition through a new v10 request claim/reservation, and normal three-stock v10 HTTP/CLI/host -> trigger_once -> native cycle/research Job persistence with controlled sources, not a live model or simulation acceptance. Candidate image matches 643 Python/migration/project files. Current real batch/model hashes and synthesis prompt read back valid; the old masked preparation cause remains unknown and must not be inferred from tests. Main/runtime and the existing unknown request were not changed. Evidence: .secrets/manual-preparation-diagnostic-qualification/verification.json.


### 未建周期的未知准备收尾（本地候选，尚未部署）

候选入口 `nexus-engine-manual-paper close-unstarted-preparation --request-id <UUID>` 沿用主仓库与受保护环境配置。必须显式指定旧请求，不自动封存或自动创建下一请求。该入口追加独立审计，保持原 unknown 与原预算，不重放旧调用。需原生产者身份及已审查调用顺序、原生认领/周期绑定缺失、实际进程退出和账户无占用的完整证明；缺失或变化返回固定 `MANUAL_PREPARATION_CLOSE_BLOCKED`。维护与正常服务启动共用生命周期锁，持有至提交。

当前真实旧请求尚不符合该候选证据门槛：心跳早于 preparing，事件未绑定原生产者身份。隔离成功不能授权真实收尾、部署或新周期，也不能还原旧屏蔽异常。不得为通过该记录放宽身份规则，或通过重放采集/模型来补历史事实。


### 明确接受历史准备结果未知（本地候选，尚未部署）

`nexus-engine-manual-paper acknowledge-unknown-preparations --request-id <UUID> --claim-id <UUID> --claim-id <UUID> --claim-id <UUID> --claim-id <UUID> --accept-unknown-effects` 使用正常主仓库、受保护配置和数据库时钟。只支持一次明确绑定的一条 unknown 手动请求及四条空回执定时认领，不自动扫描并接受其他未知记录。此入口不证明旧生产者顺序、模型调用或费用为零：原请求、事件、认领及预算全部保留，只追加 `paper.preparation.unknown_acknowledged` 审计以解除这五项调度占用。

维护期间使用既有生命周期、schema、账户和预算日锁，持有至提交；生产者必须退出。读取者重新验证原事实及审计链，迟到周期/认领、事实变更、额外未知占用、有效 Run/Job/租约/订单均拒绝。旧请求和槽位仍不可重放，新任务仍须正常独立准入；仅允许现有本地 Codex 套餐及原额度，受保护经济预算或付费模型不适用。当前零内部定价不回推历史费用。历史正常完成回执按原 route/schema/槽位或请求绑定解码；旧恢复回执按精确前驱和历史周期前缀读取，后续合法周期不使旧回执失效。

本切片不新增迁移或配置，不部署、不处置真实记录、不启动宿主或新周期。候选可在无真实审计时撤回；未来若批准应用并产生审计或后续事实，回退必须先停用新入口并保留数据、核验消费者兼容性，不能直接删除审计恢复旧调度。隔离测试中正常 CLI/宿主 dispatcher/native 入口已可创建独立三股票周期和三个研究任务；来源受控、模型未调用，不代表真实研究或内部模拟验收通过。

#### Workspace 已处置 unknown 的只读恢复回执（本地候选）

正常配置账户的 GET/CLI 读回，在每次核验处置审计链及原事实绑定后，可附加 preparation_disposition=unknown_acknowledged。这不是新的业务状态、执行许可或费用结算；原 status/code/cycle 及预算不变。canonical reader 不调用投影，避免审计证明递归。未处置时该字段省略，保持旧七字段；非法组合、审计变化或迟到事实仍拒绝，不从请求参数接受该字段。

页面保留 unknown 和历史不确定说明，并提供“开始新的请求”。动作仅移除本地原请求引用，不请求维护、不重放旧 POST、不自动创建周期；用户随后显式开始才生成新 key，后端现有账户、预算、时段、来源和风控准入保持。GET 失败或回执不合格时撤销旧展示回执，保留原 key 并禁止复位。该说明不表示其他账户占用或市场准入已经通过。

本候选无迁移、依赖或配置变更。新版页面兼容旧七字段 API，后续获准部署时先更新兼容页面再更新 API；应用回退先回退 API 再回退页面，保留全部审计与历史事实。本轮仅本地和隔离验收，未更新实际 Web/API/Worker，也未启动真实周期。


### 当前生产者 preparing 中断恢复（本地候选，尚未部署）

正常宿主将 preparing 与 `paper.manual_preparation.execution_started` 一同提交，记录执行身份、生产者 revision/PID 和请求事件哈希；进入正常 trigger（包括刷新和来源准备）前，另行提交 `effects_possible`。执行身份同时进入原生认领哈希与周期首事件，账户锁内校验，旧身份不能在维护后继续创建周期。旧记录没有这些事实时，不推断“未调用”，继续使用其原恢复合同。

正常维护入口为 `uv run --project engine nexus-engine-manual-paper --repo-root <主仓库> --env-file infra/.env.local recover-preparation --request-id <UUID>`。该入口不停止服务；须先按既有流程确认生产者退出，且源码、schema、受保护账户匹配。生命周期锁、账户锁和相关 Job 行锁持有至审计提交；活动研究 Job、有效租约、订单投影占用或不完整证据均拒绝。Worker 用 `SKIP LOCKED` 认领；维护不会执行或取消已有研究 Job。

只有正向证明尚未进入外部操作、无原生认领/周期且其他准入无占用时，追加 unavailable / `MANUAL_PREPARATION_CANCELLED` 与独立审计。已经可能调用但结果未知时，仅追加/保留 unknown 及审计，费用、预留和原认领不变，不因此允许新请求；发现已存在周期时只核对并连接原周期，不创建第二个周期。审计失败整笔回滚；重复维护只读回同一结果。所有旧 key 均只读原记录，新的独立 key 仍走全部既有准入。

GET/CLI 每次验证原事件链、处置锚点、周期执行身份及哈希；未知认领变化或迟到事实使旧处置失效。Workspace 区分安全取消、未知保留和已找到原周期；读回失败即撤销旧处置展示与复位能力，刷新不发起业务写入。此候选不改研究时间/token/预算、不增加重试，不改变定时合同或执行锁。

无新迁移、依赖或配置。未来部署需协调兼容 Web/API/Worker/宿主；在产生新执行日志后，不应恢复不校验执行身份的旧生产者。回退先停新入口与生产者，保留全部请求、认领、审计及周期，核对消费者兼容性后再选择版本；不能删审计恢复调度。上述命令说明不是部署或真实处置授权。

#### 显式接受当前准备结果未知（本地候选，尚未部署）

处置前可通过正常配置入口检查当前资格（本地候选）：

```sh
uv run --project engine nexus-engine-manual-paper --repo-root <仓库> --env-file infra/.env.local check-preparation-acceptance --request-id <UUID>
```

该命令复用接受入口的源码/schema、执行证明、生产者退出、任务/租约、账户占用和历史封存检查，并持有相同的必要维护锁。它不调用审计写入函数，不改变请求、事件、Run、Job、预算或订单；这是带锁的无业务写入检查，不是 PostgreSQL 的 `READ ONLY` 事务。锁争用仍固定拒绝，命令退出后释放锁。返回 `eligible_at_check`（仅检查时满足条件）或 `already_acknowledged`（原处置证明有效），附请求 ID、数据库检查时间、`check_only=true` 和执行锁；不得把回执当作实际处置授权或新周期准入。已接受分支允许后续合法工作，仍验证原证据。实际接受不消费检查回执，必须独立重验全部门禁；迟到事实、版本或占用变化可能使其拒绝。检查不停止服务，不刷新凭据，不调用模型或来源。

仅在用户明确接受指定请求准备结果仍未知后，使用同一正常入口并增加 `--accept-unknown-effects`：

```sh
uv run --project engine nexus-engine-manual-paper --repo-root <主仓库> --env-file infra/.env.local recover-preparation --request-id <UUID> --accept-unknown-effects
```

默认不带此参数的恢复语义不变。该参数仅支持已有完整当前生产者 STARTED/EFFECTS 执行日志、没有认领或周期的 unknown 请求（包括已追加 unknown_preserved 恢复回执的请求）；不替代历史五项未知处置。首次接受须核验生产者退出、原证据、其他认领、研究任务/Run/有效租约及账户订单占用。既有生命周期/schema/账户锁持有至提交；全局审计锁及短时 Jobs/leases/Run 表锁采用非阻塞获取，竞争时整笔拒绝并回滚，不等待、重试或干预正常研究。

只追加 `paper.manual_preparation.unknown_accepted` 审计。原请求、unknown 事件、执行日志、未知费用及全部预留保持原样；正常 GET 复用 `preparation_disposition=unknown_acknowledged`，不增加业务终态或执行权限。后续合法新周期不改变旧审计；重复操作返回同一接受结果，无需再次停止新的合法生产者。新请求仍独立检查账户、任务、预算、数据与执行条件，不能通过本次接受直接判定账户可用。

执行占用检查不把所有历史 `prepared` 都当作活跃研究：仅当唯一研究 Job 的身份、输入哈希、scope 和时间精确绑定，单次自动尝试已经 `failed/dead_letter`，当前 attempt 已结束且与失效 lease 的 token/fence 对应、无重试路径时，才视为没有执行占用。无 Job、多 Job、身份或时点不符、有效 lease、其他非终态仍拒绝；真实进程退出检查独立保留。此判断不封存该 Run，也不结算或释放未知用量/预算；账户周期仍须通过原有完整准入及有效历史封存验证。

读回只验证该请求的原事实及审计，不绑定全局空闲状态或最新版本。迟到认领/周期、原事件或审计变更均使接受证明失效；即使证明失效，已经存在的接受审计仍永久阻止旧执行身份继续认领或建周期。旧 key 只读旧 unknown，不可重放。

无迁移、依赖或配置变更；当前授权仅本地实现和隔离验收，未处置真实记录、启动宿主或新周期。未来部署须同步兼容消费者与生产者；有接受审计后，回退先停新入口及生产者、保留全部事实并核验旧消费者兼容性，不可删除审计或直接恢复不认识该执行拦截的旧版本。

### 准备与维护失败诊断（本地候选，未部署）

正常宿主准备日线、宏观、趋势及身份时，异常保留原业务结果，附加固定的
`failure_phase`、`failure_point`、`failure_code`。子步骤区分范围检查、归档、采集、
导入、快照及读回；未细分的外层异常使用 `operation`，不冒充已知根因。
已有归档、采集不可用或验证拒绝仍返回 `unavailable`，另输出
`agent_paper_preparation_unavailable` 安全事件，不增加重采集或重试。
宏观 HTTP 拒绝仅可附经过范围校验的整数 `http_status`，不输出响应正文。

正常 manual-paper CLI 的失败回执保留原错误码，并附 `operation_stage`、
`operation_code` 和 `transaction_outcome`。阶段区分事务开启、业务处理、提交、
回滚、session 关闭、engine 清理与生命周期解锁；后续清理失败记录在
`secondary_failures`，不会替换最初失败。`committed` 只在提交正常返回后设置，
提交异常或取消均保持 `unconfirmed`，不能由非零退出推断未提交或再次执行。
先以正常只读入口核对持久化事实；这些字段不是新的恢复许可。

进程检查的 `HOST_IDENTITY_UNCONFIRMED` 保留原拒绝含义，并可附
`diagnostic_reason`：`ANCESTOR_MISSING`（祖先缺失）、`ANCESTOR_CYCLE`（进程链循环）、
`OWNER_MISMATCH`（属主不符）或 `EXECUTABLE_MISMATCH`（执行文件身份不符）。
封存 CLI 和准备检查封装均保留该固定原因；旧回执未带原因时保持未知。
字段不包含 PID、路径、进程参数或属主标识，不改变身份准入和失败后的处理权限。

诊断字段全部采用固定白名单，原异常、SQL、账户资料及凭据不进入输出。
新诊断无法追溯还原旧的被屏蔽异常；它也不保证上游一定提供足够信息确认根因。
本候选不改变预算、180 秒研究合同、执行锁、失败停止或旧请求不可重放边界。

## 三股票历史研究案例（本地隔离候选）

`nexus-engine-historical-pool` 复用已验证的完整 v9/v10 历史三股票冻结输入，
以当前已选且精确匹配的 v11 配置进行新的历史观察。它不继续旧 Run，不创建业务
Job/Run/Output、周期、目标或订单，也不取得正式输出资格。研究与组合复用现有
committee、完整池容量检查、组合 transport 和引用验证器；没有调用正式 Worker，
因此此入口及其替身测试不能替代正式 Worker、API 或四页面验收。

分开的正常命令如下；本轮仅授权 `prepare` / `read` 和模型替身隔离验证，
**没有授权对真实案例执行 `run`**：

```bash
uv run --project engine nexus-engine-historical-pool prepare \
  --repo-root <配置所在主仓库> --source-cycle-id <历史完整三股票周期ID>
uv run --project engine nexus-engine-historical-pool read \
  --case-dir <私有案例叶子目录>
# 只有取得独立真实观察授权后才可执行：
uv run --project engine nexus-engine-historical-pool run \
  --case-dir <同一私有案例叶子目录>
```

`prepare` 使用正常配置解析及只读事务，校验配置账户、来源终态及哈希链、完整池、
冻结证据、快照上下文、历史组合投影/目标、历史日历和当前 v11 身份。全池预检也在
保存前完成，不调用模型或重新采集来源。私有文件为主仓库
`.secrets/nexus-historical-three-stock/<source-cycle-id>-v11/case.sqlite3`，目录0700、
文件0600；同源同版本叶子已存在时拒绝覆盖，后续只读。不得复制或换目录重跑。

隔离库只有案例与诊断输出两张表，无业务连接。执行前先提交不可重试的未知认领；
进程中断、超时、无效输出、用量未知均停止，不继续下一只或组合，不自动重试。
完整池准备最多195秒，每只 v11 研究最多300秒、三次调用、96k输入/8192输出，
各自保留现有关闭和后续阶段余量；组合沿用现有单次 transport 的实际超时及
64k输入/8192输出，不提高现有上限。私有诊断记录已知用量与未决状态，不释放正式
未知预留、不写正式预算，也不宣称对共享业务预算取得了新的排他预留。

`evidence_as_of` 是历史证据时点，`compiled_at` 是本次编译时点，
`observation_started_at/finished_at` 是本次实际调用时间。组合的 decision_time
保留历史值，admitted_at/expires_at 仅描述本次诊断，不代表当时已经形成研究。
每只结果保存独立 output_id、完整角色结果与用量；组合保存完整输入、哈希和引用。
读回重新验证案例、输出哈希及组合绑定；终端只输出固定状态/诊断，不展示账户或研究正文。

10月16日案例没有匹配的后续执行行情归档，始终标记 `EXECUTION_WINDOW_MISSING`。
最小补证方案是取得同一批 Instrument 在历史决策时点之后、满足既有执行窗口/质量/
可用时间条件的真实行情原件并独立归档，验证标的绑定及严格解析。今天新产生的研究
不能冒充历史当时可得结论；若要使用这些原件做历史成交，仍需单独定义并验收隔离
反事实执行入口，不能导入正式历史周期。另一条已有路径是在未来新周期采集匹配
的新输入和执行行情，走原正式准入。当前候选没有实现或证明内部成交、账本及页面闭环。
