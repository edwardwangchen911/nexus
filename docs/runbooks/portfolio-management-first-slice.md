# 组合管理第一切片

本手册只用于 Mac 本地的内部模拟组合管理。它不会访问券商下单接口，不启用券商模拟盘或实盘，
并始终保持 `execution_locked=true`。页面和只读接口只读取已持久化事实，不触发模型、行情采集或
订单推进。

## 1. 查看并确认首次组合基线

先从本地受保护配置取得内部模拟账户 ID，并选择一个已经完成绩效发布、无活动订单或未知占用的
UTC 时点：

```bash
uv run --project engine nexus-engine-agent-paper-once inspect-projection \
  --account-id "$PAPER_ACCOUNT_ID" \
  --as-of "$PROJECTION_AS_OF"
```

把输出中的 `as_of`、`projection_hash`、现金、持仓和未结订单完整展示给用户。只有用户明确批准
这一份精确投影后，才运行以下一次性写入；政策确认、页面浏览或此前对其他快照的同意都不能代替
本次批准：

```bash
uv run --project engine nexus-engine-agent-paper-once approve-initial-target \
  --account-id "$PAPER_ACCOUNT_ID" \
  --projection-as-of "$PROJECTION_AS_OF" \
  --projection-hash "$PROJECTION_HASH" \
  --approved-by local-user \
  --approved-at "$APPROVED_AT" \
  --idempotency-key "$INITIAL_APPROVAL_KEY"
```

结果不明时先读取持久化的 approval 与幂等记录，只能使用原 key 核对。不要更换 key 重复批准。

## 2. 创建唯一候选批次

正式多股票研究池先核对每只股票的唯一身份、已持久化研究快照和人工审核、非合成的行业证据。
行业资料的审核和导入见[本地受审核行业暴露导入](sector-exposure-local.md)。只读预检发布条件：

```bash
uv run --project engine nexus-engine-agent-paper-universe-local \
  --repo-root "$PWD" \
  --effective-from "$EFFECTIVE_SESSION" \
  --instrument-id "$INSTRUMENT_ID_1" \
  --instrument-id "$INSTRUMENT_ID_2"
```

返回 `publishable` 后，再以相同参数加 `--publish`，将 2–5 只普通美股股票发布为新的
不可变 `agent_paper_research/reviewed_v1_*` Universe。返回的 `universe_snapshot_id` 和版本
用于下面的候选批次；重跑相同证据会读回同一版本。新证据只能产生新版本，不回填历史周期。
该入口不采集资料、不调用模型、不创建订单；发布前须确认行业原件已人工核对，且只写本地业务库。
发布不等于周期准入，前一交易日行情、当日行情和风险查询仍由后续预检与周期入口核验。

从已批准 Universe 中确定 1–5 个互不重复的 instrument UUID，再运行：

```bash
uv run --project engine nexus-engine-agent-paper-once prepare-candidates \
  --account-id "$PAPER_ACCOUNT_ID" \
  --universe-snapshot-id "$UNIVERSE_SNAPSHOT_ID" \
  --instrument-id "$INSTRUMENT_ID_1" \
  --instrument-id "$INSTRUMENT_ID_2" \
  --idempotency-key "$CANDIDATE_BATCH_KEY"
```

核对输出的候选身份、Universe、`batch_id` 和 `batch_hash`。将该 `batch_id` 写入受保护且 Git 忽略的
`infra/.env.local`：

```text
NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID=<confirmed batch_id>
```

模板保持空值；不得把账户 ID、候选批次或凭据提交到 Git。

## 3. 运行一个有界周期

先查 cycle → Job → Output → order 的持久状态。正常组合只允许在 XNYS 交易日纽约时间
10:00–10:04:59 启动一次；当天已有组合周期、错过该窗口或结果不明时都不得换 key 补跑。
触发入口会在认领前核对已登记的组合合成提示词；若返回
`PORTFOLIO_SYNTHESIS_PROMPT_INVALID`，先用现有本地 `nexus-openai-local setup` 补齐配置，
不要运行行情采集或研究。该拒绝不创建触发认领。
合法窗口内没有周期时，使用同一个显式批次启动一次：

```bash
uv run --project engine nexus-engine-agent-paper-once trigger \
  --account-id "$PAPER_ACCOUNT_ID" \
  --candidate-batch-id "$CANDIDATE_BATCH_ID" \
  --idempotency-key "$TRIGGER_KEY"
```

随后严格复用 [Agent 内部模拟周期](agent-paper-cycle.md) 中的 `run-research`、`resume`、
`collect-window` 和受管宿主推进流程。失败或输出不明时先读持久状态，不换 key、不自动重试、
不放宽证据、时间窗或风控。

### 同日一次手动恢复

仅当当天 10:00 的定时认领已持久化为 `SCHWAB_L1_INCOMPLETE` 或
`SCHWAB_L1_FRESHNESS_UNPROVEN`、且当天没有组合周期时，可由用户明确要求在纽约时间
10:05–14:59 发起一次独立的手动恢复。它使用当前真实五分钟决策槽、单独的幂等路由和
当天唯一的手动认领；不重放、改写或伪造 10:00 的失败结果。原 `trigger` 命令仍只接受
10:00–10:04:59。组合分配参数沿用冻结的 v1，`decision_local_time=10:00` 仍是自动调度
时点；手动例外的实际决策时刻记录在 cycle 中。

先只读核对原认领、同日 cycle/Job/order、候选批次与行情准备状态。未知结果、已有周期、
研究或订单占用不允许手动恢复；缺失研究输入必须如实显示为缺失，不为通过准入而重标。
满足条件时，以新的固定 `TRIGGER_KEY` 调用一次：

```bash
uv run --project engine nexus-engine-agent-paper-once trigger-manual-recovery \
  --account-id "$PAPER_ACCOUNT_ID" \
  --candidate-batch-id "$CANDIDATE_BATCH_ID" \
  --idempotency-key "$TRIGGER_KEY"
```

超时先核对 `agent-paper-manual-trigger-v1` 的持久化认领和 cycle；只用原 key 读取结果，
不在新槽位换 key 重试。此入口仅写入本地内部模拟事实，保持 `execution_locked=true`。

### 合成配置缺失后的独立恢复

若上述手动周期的 AAPL 研究 Job 已成功，但周期因当时尚未登记组合合成提示词而以
`PORTFOLIO_SYNTHESIS_UNAVAILABLE` 终止，不改写或重开该终态。先通过现有本地 setup
登记并核对合成提示词；系统仅在提示词的真实登记时间晚于失败事件、原研究输出仍可校验、
原周期没有合成 Run 或决策、同日只有该一个组合周期时，允许用户明确要求再创建一次
独立周期。它有单独的 `agent-paper-synthesis-recovery-v1` 认领和当天唯一 key；会重新采集
有界行情并运行一次研究模型，不复用旧研究结果伪造新决策。仅限纽约时间 10:05–14:59，
且保持 `execution_locked=true`。原失败周期和它的 Job/Output 原样保留。

```bash
uv run --project engine nexus-engine-agent-paper-once trigger-synthesis-recovery \
  --account-id "$PAPER_ACCOUNT_ID" \
  --candidate-batch-id "$CANDIDATE_BATCH_ID" \
  --idempotency-key "$SYNTHESIS_RECOVERY_KEY"
```

结果未知时只按原 key、claim 和 cycle 读回，不能发起第三次周期或绕过证据与风控。

## 4. 只读核对同一事实链

Web 已在本机运行后，使用同一个 `CYCLE_ID`、`PLAN_ID` 和原取消尝试键读取：

```bash
curl -fsS "http://127.0.0.1:3000/api/v1/paper/management/cycles/$CYCLE_ID"
curl -fsS "http://127.0.0.1:3000/api/v1/paper/management/portfolio"
curl -fsS "http://127.0.0.1:3000/api/v1/paper/plans/$PLAN_ID"
curl -fsS \
  -H "Idempotency-Key: $CANCEL_IDEMPOTENCY_KEY" \
  "http://127.0.0.1:3000/api/v1/paper/orders/$ORDER_ID/cancel-result"
```

逐项核对 proposal、target、plan、order、fill、reservation、借贷平衡账本、最新绩效和页面身份一致；
重复 GET 不得新增业务事实。`cycle.execution_locked` 必须为 `true`。取消的客户端尝试键只有在服务端
持久化 `COMMITTED` 后才能称为已确认取消请求；超时或未知不得释放占用。

最后依次查看 `/workspace`、`/decisions/$CYCLE_ID`、`/portfolio` 和 `/paper`。外部券商模拟写入和
实盘交易在本切片中不可用，不能因内部模拟成交、页面可见或绩效发布而启用。

## 5. 开盘前受控页面验收

用独立的 PostgreSQL 测试实例运行以下用例。它会在临时数据库中生成两笔内部模拟成交，启动临时
Engine API 和 Web，核对订单、成交、持仓与上述四个页面，然后停止临时服务；不会读取或写入正式
模拟账户。测试实例的 `DATABASE_URL` 须指向测试库服务，不能指向本地业务库。

```bash
DATABASE_URL="$ISOLATED_TEST_POSTGRES_URL" \
PYTHONPATH=engine/src:engine/tests \
NEXUS_CONTROLLED_BROWSER=1 \
uv run pytest engine/tests/integration/test_portfolio_management_e2e.py -q
```

此项通过只证明冻结的两笔成交样例能贯通 PostgreSQL、Engine API、Web 接口和浏览器；真实行情、
正式多股票候选池、自然时间窗及用户账户的验收仍须分别核对。
