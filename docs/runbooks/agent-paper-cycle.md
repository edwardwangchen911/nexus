# Agent 内部模拟周期

本手册用于单一私人用户在 Mac 本地手工运行一个 Agent 模拟周期。当前最小真实路径固定为
`AAPL + Schwab Level One 决策报价 + Schwab price history 五分钟执行窗口`。它只产生内部模拟
订单、成交和账本事实，不访问 Schwab 订单接口，不连接券商交易域，不解除
`execution_locked=true`，也不等于券商模拟盘或实盘。

页面和只读 API 只读取已持久化事实；刷新 `/paper` 不会触发行情、模型或周期推进。功能开关
默认关闭，失败不自动重试。一次周期只允许一个研究 Job 和至多一次模型执行。

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

发现同账户、同 XNYS session 的周期时停止 `trigger`，恢复已有 `CYCLE_ID`。发现 queued/running
Job 时先读取其状态；断流、观察超时、worker 退出或终端输出丢失都不是换 key 重放的许可。
存在非终态订单时先对账，不删除事实、不重建周期。

运行前还必须满足：

- 唯一候选是已发布 `us_liquid/us_liquid_v1` 中的 AAPL，且有当前研究快照、前一完整交易日
  Alpaca SIP delayed `all` 日线、审核行业 authority、完整 XNYS 日历和三类风险查询证据；
- 已幂等登记 actionable v4 模型/提示词配置，模型预算可用；
- `infra/.env.local` 保持受保护且未进入 Git；动态 Schwab OAuth token 只能位于主仓库
  `.secrets/nexus-schwab-local/tokens.json`，目录和文件权限分别为 `0700`、`0600`；
- `NEXUS_AGENT_PAPER_CYCLE_ENABLED=true` 仅在本轮手工窗口内启用。模板及默认值仍为 false；
- 当前纽约时间处于合法 XNYS 交易日的 `[07:00, 16:00)`。07:00–09:30 盘前可运行；不必等
  09:30 正式开盘。当前 MVP 不接受 07:00 前隔夜行情，也不接受 16:00 后盘后行情。

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

`trigger` 不接受 symbol 或 decision time。它先认领同账户、同 session 的唯一周期；缺少现成
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

Engine `/internal/v1/paper/cycles/latest`、同源 BFF `/api/v1/paper/cycles/latest` 和 `/paper` 必须显示
同一周期。页面应分别显示：

- `决策报价 · Schwab Level One · 实时 · <冻结报价时间>`；
- `执行分钟线 · Schwab pricehistory · 实时性未由响应声明`；
- `execution_locked=true`，且没有创建周期、再次请求行情或券商下单按钮。

同 head 本地栈的浏览器只读验收入口：

```bash
NEXUS_REAL_AGENT_PAPER_ACCEPTANCE=1 npm run test:e2e -- \
  tests/e2e/agent-paper-cycle-real.spec.ts
```

## 8. Stop：默认关闭并保留现场

1. 将 `NEXUS_AGENT_PAPER_CYCLE_ENABLED=false`。只停止本轮自己启动的进程；一次性 worker 正常
   返回即已停止。若另有自己启动的普通/Provider worker，只各按一次 `Ctrl-C`。
2. 只读确认没有未解释的 queued/running Job，没有结果未知的模型调用，也没有未对账的
   `proposed`、`risk_approved`、`submitted`、`partially_filled` 订单。
3. 对 `prepared`、`researching`、`decision_recorded`、`risk_evaluated` 或 `awaiting_market` 周期，
   保留现场并记录精确阻塞；不得删记录来制造完成状态。
4. 不删除 cycle、event、Run/Job/Output、订单、raw、window、fill、ledger 或 performance。关闭开关
   只阻止新手工调用，不改写历史事实。

## 9. Rollback：应用回退与数据回退分开

首选应用回退：保持 `NEXUS_AGENT_PAPER_CYCLE_ENABLED=false`，回退应用版本但保留数据库 head 和
所有事实。出现数据合同问题时前向修复；Git 回退不是业务数据恢复。

迁移 `0058_agent_paper_schwab_realtime` 的降级只允许在专用空临时数据库演练。先按
[备份恢复手册](backup-restore.md)完成独立备份/恢复演练，再对临时库执行：

```bash
DATABASE_URL="$EMPTY_TEMP_DATABASE_URL" \
  uv run --project engine alembic -c engine/alembic.ini upgrade 0058_agent_paper_schwab_realtime
DATABASE_URL="$EMPTY_TEMP_DATABASE_URL" \
  uv run --project engine alembic -c engine/alembic.ini downgrade 0057_agent_paper_market_window
DATABASE_URL="$EMPTY_TEMP_DATABASE_URL" \
  uv run --project engine alembic -c engine/alembic.ini upgrade 0058_agent_paper_schwab_realtime
```

存在任一 Schwab Agent cycle、decision、authority 或 window 事实时，downgrade 必须以
`SCHWAB_AGENT_PAPER_FACTS_EXIST` 失败。不得为降级删除事实，不得在当前业务库试跑，不删除 MinIO
bucket、volume、数据库或迁移历史。
