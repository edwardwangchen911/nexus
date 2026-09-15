# Agent 内部模拟周期

本手册用于单一私人用户在 Mac 本地手工运行一个 Agent 模拟周期。当前最小真实路径固定为
`AAPL + Schwab Level One 决策报价 + Schwab price history 五分钟执行窗口`。它只产生内部模拟
订单、成交和账本事实，不访问 Schwab 订单接口，不连接券商交易域，不解除
`execution_locked=true`，也不等于券商模拟盘或实盘。

页面和只读 API 只读取已持久化事实；刷新 `/workspace`、`/decisions` 或 `/paper` 不会触发行情、
模型或周期推进。功能开关
默认关闭，失败不自动重试。每个五分钟决策槽最多一个周期；每个周期只允许一个研究 Job 和至多
一次模型执行。同一交易日可以在前一周期及其订单都进入诚实终态后运行下一槽，不按“每天一次”限制。

## 1. Preflight：先查状态，不先发请求

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

发现当前五分钟槽已有周期时停止新建，恢复该槽已有 `CYCLE_ID`；同日较早周期不阻止下一槽。
发现 queued/running Job 时先读取其状态；断流、观察超时、worker 退出或终端输出丢失都不是换 key
重放的许可。存在非终态订单时先对账，不删除事实、不重建周期。`unavailable` 是保留的诚实终态，
不恢复、不删除；到下一合法槽后可以新建周期。

运行前还必须满足：

- 唯一候选是已发布 `us_liquid/us_liquid_v1` 中的 AAPL，且有当前研究快照、前一完整交易日
  Alpaca SIP delayed `all` 日线、审核行业 authority、完整 XNYS 日历和三类风险查询证据；
- 已幂等登记 actionable v4 模型/提示词配置，模型预算可用；
- `infra/.env.local` 保持受保护且未进入 Git；动态 Schwab OAuth token 只能位于主仓库
  `.secrets/nexus-schwab-local/tokens.json`，目录和文件权限分别为 `0700`、`0600`；
- `NEXUS_AGENT_PAPER_CYCLE_ENABLED=true` 仅在本轮手工窗口内启用。模板及默认值仍为 false；
- 当前纽约时间处于合法 XNYS 交易日的 `[07:00, 16:00)`。07:00–09:30 盘前可运行；不必等
  09:30 正式开盘。当前 MVP 不接受 07:00 前隔夜行情，也不接受 16:00 后盘后行情。

### 07:00 前盘前行情边界

2026-09-15 约 05:45 ET 的一次 20 秒有界实测中，Schwab `LEVELONE_EQUITIES` 为 AAPL 和 NVDA
返回了 `delayed=false` 的双边报价与最近成交；这说明 07:00 前可以做只读行情观察。该归档没有
导入业务库。随后对 NVDA 前五个完整分钟仅请求一次 `pricehistory`，Provider 返回
`SCHWAB_PRICE_HISTORY_RESPONSE_INVALID`，未满足现有精确 5×1 分钟执行窗口合同，且按无重试规则
停止。

因此不要把“WebSocket 有盘前报价”推导为“完整订单闭环已可运行”。当前循环仍从 07:00 ET
开始：决策报价继续使用 Level One，订单成交权威继续使用通过合同校验的 Schwab pricehistory
分钟线。不要临时改用 WebSocket 成交事件拼 K 线，不新增 04:00–07:00 决策专用状态机；若未来
要提前正式窗口，应先用正常产品入口取得一次合格的精确五分钟 pricehistory 证据，再单独评审。

首次使用 actionable v4 时只登记配置，不创建 Job、不调用模型：

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

## 2. Trigger：冻结一次决策证据

`trigger` 不接受 symbol 或 decision time。数据库按当前时间归入五分钟决策槽，并认领同账户、同槽
的唯一周期；缺少现成
风险查询证明时，按既有只读 Alpaca 入口补齐一次，然后对 AAPL 做一次有界 Schwab Level One
采集。Schwab 路径固定一次 LOGIN、一次订阅、最多 15 秒/100 帧/8 MiB，不重连、不重试。

```bash
uv run --project engine nexus-engine-agent-paper-once trigger \
  --account-id "$PAPER_ACCOUNT_ID" \
  --idempotency-key "$TRIGGER_KEY"
```

只接受返回中冻结的 `CYCLE_ID`，并核对决策证据是 `schwab/l1/real_time`、`delayed=false`、
双边报价完整且 quote/artifact/hash 绑定一致。命令结果不明时读取持久状态；只可使用原
`TRIGGER_KEY` 恢复，不能换 key 再请求行情。

以下任一情况立即停止本轮：`AGENT_PAPER_OUTSIDE_WINDOW`、`RISK_INPUT_REQUIRED`、
`SCHWAB_L1_FRESHNESS_UNPROVEN`、`SCHWAB_L1_INCOMPLETE`、候选或证据绑定冲突。不得换数据源、
放宽时间/质量门槛或借用 Shadow 路径。

## 3. Run existing worker once：最多消费一个 Job

先确认该周期恰有一个 queued 研究 Job，且没有相同 Run 的未知调用结果，再运行现有一次性
研究 worker：

```bash
uv run --project engine nexus-openai-local run-once --backend codex_sdk
```

该入口至多领取一个已登记 Job。`job_processed=false`、调用结果 unknown、预算不足或 worker
退出时先查原 Job/Run/Output，不再次运行 worker。只有持久化 Output 终态明确后才继续。

## 4. Resume：消费已持久化研究结果

```bash
uv run --project engine nexus-engine-agent-paper-once resume \
  --cycle-id "$CYCLE_ID" \
  --idempotency-key "$RESUME_KEY"
```

`resume` 不调用模型、不采集行情，只读取已持久化 Job/Run/Output，并生成确定性 sizing、硬风控、
批次和内部模拟订单。用同一个 `RESUME_KEY` 处理结果不明；不要轮询或换 key。合法结果包括：

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
模拟入口推进；`EVALUATED_AT` 使用已到达窗口的实际核对时间，不伪造未来时间。

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
NEXUS_REAL_AGENT_PAPER_ACCEPTANCE=1 npm run test:e2e -- \
  tests/e2e/agent-paper-cycle-real.spec.ts
```

### 同日两槽固定验收顺序

两个槽使用受保护的本地变量 `NEXUS_ACCEPTANCE_PAPER_ACCOUNT_ID`、
`NEXUS_ACCEPTANCE_SLOT_A_KEY` 和 `NEXUS_ACCEPTANCE_SLOT_B_KEY`；值不得写入命令历史、聊天、Git
或验收文档。每一个动作前都先按本节开头的事实链顺序读取持久状态，固定流程为：

```text
read current cycle/job/order state
→ trigger slot A
→ run existing one-shot worker（仅当该 Job 为 queued）
→ resume
→ collect-window（仅当存在订单）并 advance
→ resume and read DB/API/BFF/page
→ slot A 的 cycle 与订单均终态后等待 slot B
→ trigger slot B
→ run/resume/collect/advance as required
→ read both cycles and disable the feature flag
```

槽 B 必须获得不同的 `cycle_id`、Job/Run/Output 和 decision，并从槽 A 终态后的最新模拟现金、持仓、
当日换手、预算及未结订单事实重新投影。两槽都允许合规 no-trade；但都必须实际形成 Agent 决策并
经过确定性硬风控。同槽使用原幂等键回读不得增加 Job、订单、成交或账本。连接或命令结果未知时只
查原 `run → job → outputs` 和业务事实，不换 key、不重放。原始模型全文、token 和个人账户事实不
进入 Git 或验收记录。

## 8. Foreground loop：同一交易时段连续决策

手工双槽验收完成后，可在一个纽约交易时段内以前台进程按五分钟槽连续推进。该入口没有逐日
次数上限，但仍受现有美元日预算约束；每槽最多一个 cycle，每个 cycle 最多一个真实 Agent Job。
它不常驻后台、不注册 LaunchAgent，也不改变 API/Web 的只读行为：

```bash
uv run --project engine nexus-engine-agent-paper-loop \
  --account-id "$PAPER_ACCOUNT_ID"
```

每一步都先读已持久化的 cycle、Job、Output、订单和窗口：同槽已完成则等待下一槽；queued Job 只按
该 cycle 的精确 Job ID 领取；running、缺失或结果不明立即停止，不换 key。订单窗口到达后再额外
等待 60 秒发布余量，只请求一次 Schwab pricehistory；窗口合格后才推进对应内部订单。

只有 `completed` 允许进入同日下一槽。当前纽约交易日出现 `unavailable` 或 `failed` 时，本进程
熔断退出，不在后续槽重复 Provider 或模型调用；下一交易日可以重新启动。16:00 ET 后若没有活动
订单则正常退出，已有订单可完成其已冻结窗口后再退出。运行中将
`NEXUS_AGENT_PAPER_CYCLE_ENABLED=false` 或按 `Ctrl-C` 都会停止；历史事实不删除。

## 9. Stop：默认关闭并保留现场

1. 将 `NEXUS_AGENT_PAPER_CYCLE_ENABLED=false`。只停止本轮自己启动的进程；一次性 worker 正常
   返回即已停止。若另有自己启动的普通/Provider worker，只各按一次 `Ctrl-C`。
2. 只读确认没有未解释的 queued/running Job，没有结果未知的模型调用，也没有未对账的
   `proposed`、`risk_approved`、`submitted`、`partially_filled` 订单。
3. 对 `prepared`、`researching`、`decision_recorded`、`risk_evaluated` 或 `awaiting_market` 周期，
   保留现场并记录精确阻塞；不得删记录来制造完成状态。
4. 不删除 cycle、event、Run/Job/Output、订单、raw、window、fill、ledger 或 performance。关闭开关
   只阻止新手工调用，不改写历史事实。

## 10. Rollback：应用回退与数据回退分开

首选应用回退：保持 `NEXUS_AGENT_PAPER_CYCLE_ENABLED=false`，回退应用版本但保留数据库 head 和
所有事实。出现数据合同问题时前向修复；Git 回退不是业务数据恢复。

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
