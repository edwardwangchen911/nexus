# Agent 内部模拟周期

本手册只用于单一私人用户在 Mac 本地运行一个显式 Agent 模拟周期。它会产生内部模拟业务事实，
但不会连接券商下单域、不会解除 `execution_locked=true`，也不等于券商正式模拟盘或实盘。
页面只读已持久化结果，刷新页面不会触发模型、行情或周期推进。

## 运行前停止门

只有以下条件同时满足才继续：当前代码、API、普通 Worker、Provider Worker、PostgreSQL、MinIO
和 Web 来自同一 head；数据库为当前迁移 head；已有当前 `ResearchSnapshot`、完整 XNYS 日历、
US-liquid universe、决策日调整后日线 authority、风险查询 authority；模型预算与 Alpaca IEX
只读权限已存在。任何凭据只放受保护的 `infra/.env.local`，不得粘贴进命令、聊天或日志。

Agent 候选严格取自已发布的 `us_liquid/us_liquid_v1` 成员；当前本地只读来源
`fosun_local_read_only` 可直接复用。Agent 路径不要求另造 `QualityReport`，但注册策略原有的
PASS QualityReport 门槛保持不变。候选为空或超过五只时停止，不自动扩大股票池。

先读取持久化状态，顺序固定为：

```text
cycle → cycle events → research Jobs/Runs/Outputs → decision/bindings
→ batch/orders → raw artifact/manifest/window → fills/ledger/performance → API/BFF/page
```

断流、观察超时或 worker 退出都不是重放许可。先用受保护的本地 API/数据库输出取得现有
`CYCLE_ID`、Run/Job/Output、订单和窗口 ID；只记录脱敏 ID、数量、UTC 时间、source/feed/
delivery、状态、hash 和固定 result code。

```bash
uv run --project engine alembic -c engine/alembic.ini current
```

若已存在相同账户和 decision session 的周期，必须用原 idempotency key 恢复，不得换 key 重建。
若有 queued/running Job、非终态周期或订单，先恢复对应 worker；不得重复调用模型或行情。

首次使用前通过现有本地配置入口幂等登记 actionable v4。该命令只登记冻结的模型与提示词配置，
不创建研究、Job 或订单，也不调用模型：

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

## 单次推进

在 `infra/.env.local` 中显式设置 `NEXUS_AGENT_PAPER_CYCLE_ENABLED=true`；默认模板为 false。
ID 和时间来自上述受保护输出，不在 shell 命令中包含任何密钥：

```bash
uv run --project engine nexus-engine-agent-paper-once run \
  --account-id "$PAPER_ACCOUNT_ID" \
  --decision-time "$DECISION_TIME" \
  --idempotency-key "$CYCLE_KEY"
```

命令只推进一个持久化状态，不轮询也不执行模型。若进入 `researching`，启动项目既有普通
Worker，让既有 Agent Job 正常完成；再用完全相同的三项参数推进。若状态已经是
`completed`、`unavailable`、`failed` 或 `awaiting_market`，先核对事实，不得再次造新周期。

## 真实 IEX 窗口与模拟成交

只在美股开市窗口进行一次、不重试。若周期产生订单，为该订单的标的采集一个精确五分钟窗口；
若无订单，仍为候选采集一个 `acceptance_probe`，证明行情合同但绝不制造成交：

```bash
uv run --project engine nexus-engine-agent-paper-once collect-window \
  --cycle-id "$CYCLE_ID" \
  --instrument-id "$INSTRUMENT_ID" \
  --window-start "$WINDOW_START"
```

有订单时，核对 raw artifact、manifest、`PriceBarSetAuthority` 和 Agent-window binding 的 hash、
source=`alpaca`、feed=`iex`、delivery=`real_time`、raw、1Min、连续五根后，才推进内部模拟订单：

```bash
uv run --project engine python -m nexus.ops.paper_once advance \
  --account-id "$PAPER_ACCOUNT_ID" \
  --order-id "$ORDER_ID" \
  --evaluated-at "$EVALUATED_AT" \
  --idempotency-key "$ADVANCE_KEY"
```

无交易且有合格 `acceptance_probe` 是有效验收；闭市抑制只是正确 no-op，不是行情验收。
Alpaca IEX entitlement、完整五分钟 bars、当前 ResearchSnapshot 或模型预算任一不可用时立即停止，
报告真实阻塞，不换数据源、不重试、不放宽门槛、不伪造交易，也不借用 Shadow 代码。

最后读取数据库 event chain、`/internal/v1/paper/cycles/latest`、同源
`/api/v1/paper/cycles/latest` 与 `/paper`：数据库核对精确 cycle/event hash 和关联 ID，两个 API
核对同一 cycle ID，浏览器核对相同 decision time、result code、订单/成交事实，且页面没有创建或
实盘下单动作。
手工浏览器验收可在已启动的同 head 本地栈上运行：

```bash
NEXUS_REAL_AGENT_PAPER_ACCEPTANCE=1 npm run test:e2e -- \
  tests/e2e/agent-paper-cycle-real.spec.ts
```

## 停止、回滚与保留

1. 将 `NEXUS_AGENT_PAPER_CYCLE_ENABLED=false`，只对本轮自己启动的 Agent/Provider worker 各按一次
   `Ctrl-C`；观察超时后不要重复中断或重放。
2. 只读确认没有 `prepared`、`researching`、`decision_recorded`、`risk_evaluated` 周期，也没有
   `proposed`、`risk_approved`、`submitted`、`partially_filled` 订单。若存在，保留现场并恢复，
   不得删除记录来通过验收。
3. 停止不会删除 cycle、event、Run/Job/Output、订单、raw、fill、ledger 或 performance；它们是
   审计事实。配置回滚只恢复默认关闭，不改变历史。
4. `0056_agent_paper_cycle` 与 `0057_agent_paper_market_window` 都有业务事实降级栅栏。有任一对应
   事实后不得降级；优先前向修复。仅在从未使用的数据库上，且已在临时数据库完成现有
   backup/restore drill 和 downgrade/upgrade round trip 后，才可按 Alembic 正常入口降级。
5. 不删除 MinIO bucket、volume、数据库或迁移历史。确需恢复时按
   [备份恢复手册](backup-restore.md)从独立备份演练，不以 Git 回滚代替业务数据恢复。
