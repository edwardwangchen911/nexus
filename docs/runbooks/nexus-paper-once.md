# 内部模拟单次操作

`python -m nexus.ops.paper_once` 复用现有内部模拟服务。`forward` 从显式冻结资料准备决策和持仓状态，再调用已有风控准入；`admit` 和 `advance` 使用已存决策或订单。命令不创建账户或初始资金、不采集数据、不启用每日调度，也不发送券商订单。使用前须已有明确选择的内部模拟账户。

在仓库根目录使用项目 Python 环境，沿用已配置的 `DATABASE_URL`。数据库只接受 `postgresql+asyncpg` 和 `localhost`、`127.0.0.1` 或 `::1`；不支持通过 URL 查询参数覆盖连接地址。不要把连接串、账户事实或命令结果粘贴到聊天。

Agent 单次闭环还要求同一决策交易日的公司行动、停牌和退市查询证明。当前正式入口仅支持
`us_liquid_v1` 中唯一的 AAPL/Nasdaq 首环，不泛化到其他标的或交易所：

```sh
uv run --project engine nexus-engine-agent-risk-authority \
  --decision-session <纽约当日YYYY-MM-DD> \
  --decision-time <已存权威日历中的UTC决策时刻> \
  --output-dir <新的本机受保护目录>
```

入口先查数据库；若同一股票池和交易日已有三条完整证明，则直接核验并返回，不再次请求来源。
首次运行只读 Alpaca Corporate Actions、Nasdaq Trader 当前停牌 RSS 和 Nasdaq 当日上市目录，
把原始响应写入新目录后才追加数据库证明。停牌 RSS 超过两分钟、目录不是纽约当日、分页未穷尽、
响应异常或任何来源失败时均不写证明。首次采集只能在权威决策时刻前两分钟内启动；同日旧停牌
快照也不能复用，后续闭环保持 `RISK_INPUT_REQUIRED`。`complete` 仅表示指定查询和分页已完整执行，
不把 Alpaca 标注的来源完整性未知改写为全球事实完备。该命令不调用模型，
不创建周期、决策、订单、成交或账本记录；页面仍只读已持久化结果。

## Agent 版本并行评价：先查持久化状态

版本评价只允许固定 AAPL、两个冻结版本、独立内部账户以及预先登记的 4 个训练槽和 20 个
样本外槽。网页只读，不提供注册、推进、恢复、终结或晋升按钮。任何自动任务或人工恢复都必须
按以下顺序核对，不能因为命令超时、输出流断开或市场时间已到就重新注册或盲目推进：

1. 读取 `agent_version_evaluations` 和 `agent_version_evaluation_decisions`，先确认是否已有相同
   authority hash 的评价、是否仍为 open，以及是否已有 `PASS`、`FAIL` 或 `UNKNOWN` 终态；
2. 按 `evaluation_id + decision_slot_at` 核对 incumbent 和 challenger 两条 cycle，不能只看其中一条；
3. 对每条 cycle 依次核对原 `AgentResearchRun`、原生 `Job` 和唯一终态 Output；已有 queued、running
   或 succeeded Job 时不得创建替代 Job，也不得再次调用模型；
4. 核对 `agent_version_cycle_pairs` 和终态 decision，已配对或已终结的槽不得重放；
5. 核对当日模型预算和既有 usage；额度不足或账目不明时保持 unavailable/UNKNOWN；
6. 核对两个内部账户都没有 `broker_account_bindings`、`broker_execution_intents` 或外部账户标识，
   同时确认券商命令和实盘订单计数仍为零；
7. 核对关联 cycle、评价投影和页面均为 `execution_locked=true`，且 `broker_write=false`、
   `live_write=false`；
8. 只有某个登记槽的两条持久化事实都确实缺失，且冻结 authority 仍完全匹配时，才允许推进该
   精确槽；不得换槽、换幂等键、改冻结版本或补造 challenger。

先使用只读状态命令；它只读取本地 PostgreSQL，不调用模型或 Provider：

```sh
uv run --project engine nexus-engine-agent-version-evaluation status \
  --evaluation-id <已登记评价UUID>
```

如果某槽已经有原 Job 或 Output，但 pair 尚未落盘，只恢复同一槽：

```sh
uv run --project engine nexus-engine-agent-version-evaluation resume \
  --evaluation-id <已登记评价UUID> \
  --decision-slot <原UTC槽时刻>
```

只有上述检查证明两条 lane 的精确槽工作均未创建时，才可对该槽执行 `advance`。只有全部样本外
槽已过窗口且持久化事实齐备时才可执行 `finalize`。`UNKNOWN` 和 `FAIL` 都保留 incumbent；即使
`PASS` 也只影响未来内部模拟版本选择，不授予券商模拟或实盘权限。

如果业务库没有 open 评价，也没有一个明确冻结、与 incumbent 不同且用途合法的 challenger
prompt/model authority，则保持 `version_evaluation=null`，不要为了启动观察而复制旧提示、修改提示
或注册无意义评价。定时补证任务应保持安静，直到合法 authority 已存在；这不阻塞第四、第五阶段
的本地接入就绪工作。

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.paper_once --help
```

从冻结资料准备一次决策并准入：

```text
python -m nexus.ops.paper_once forward --request-file <本机受保护的JSON文件>
```

JSON字段均需显式填写，不包含凭据，最多64 Ki字符：

```json
{
  "action": "forward",
  "account_id": "<内部模拟账户UUID>",
  "idempotency_key": "<固定键>",
  "request_time": "<UTC时间>",
  "strategy_definition_id": "<预期已注册策略定义UUID>",
  "champion_generation": 1,
  "references": {
    "market": {
      "decision_session": "<YYYY-MM-DD>",
      "execution_session": "<权威日历下一交易日>",
      "decision_time": "<该日历确定的UTC截止时间>",
      "source_policy_id": "<已存来源政策UUID>",
      "calendar_authority_id": "<已存日历UUID>",
      "universe_snapshot_id": "<已存股票池UUID>",
      "quality_report_id": "<已存质量报告UUID>"
    },
    "raw_bar_set_ids": ["<已存raw BarSet UUID>"],
    "adjusted_bar_set_ids": ["<已存all BarSet UUID>"],
    "sector_exposure_ids": ["<已存行业证据UUID>"]
  }
}
```

以上是字段模板，尖括号内容须替换成已核验的本地引用；代际也必须与预期注册状态相符。
引用覆盖股票池及持仓，特征需要200个交易日。持仓计数由历史事实推导，不接受JSON填零。
完整引用随checkpoint保存并参与摘要；相同数值但不同来源不能静默复用。决策、checkpoint
和准入批次同事务提交，任一拒绝整体回滚；`forward` 不推进成交。

当前真实 v2 来源不支持复权用途，`forward` 会返回
`STRATEGY_SNAPSHOT_ADJUSTED_AUTHORITY_UNAVAILABLE`，不写入决策、checkpoint或订单。
fixture分层读取测试通过不改变这个准入结论；不能仅凭`all`标签或USD标的推断来源与成交量单位。

创建一次订单批次：

```text
python -m nexus.ops.paper_once admit
  --account-id <内部模拟账户UUID>
  --strategy-decision-id <已存策略决策UUID>
  --request-time <明确的UTC时间，例如2026-09-04T12:00:00Z>
  --idempotency-key <本次操作的固定键>
```

`batch_evaluated` 表示风控已评估批次，不保证所有订单获准或已成交。输出只包含批次和订单标识；详情读取现有 `/internal/v1/paper/orders` 页面/API。

推进一笔已有订单：

```text
python -m nexus.ops.paper_once advance
  --account-id <该订单所属内部模拟账户UUID>
  --order-id <已有订单UUID>
  --evaluated-at <已到达成交窗口的UTC时间>
  --idempotency-key <本次推进的固定键>
```

这些操作分别在单一数据库事务内执行，均拒绝未来时间。`advance` 只评估指定时刻，不等待下一个窗口、不循环推进。`no_data`、`no_capacity`、`not_due` 不代表成交；`partially_filled` 只代表部分成交。

当前不可变价格政策尚未授权新建单所需的复权价格或模拟成交用途。命令保留这些检查：新建单可能返回 `STRATEGY_DECISION_INVALID`，无合格成交窗口不会产生 fill，已有订单在截止时间可能按原规则撤销并释放预留。不得为试运行修改政策开关或重标历史事实。历史合成 PostgreSQL 测试不代表当前真实数据已取得用途授权。

确定拒绝时输出 `status=failed` 和固定错误码；未能确认事务结果时输出 `status=unknown`、`PAPER_ONCE_RESULT_UNKNOWN`，退出码为 1。先从现有只读页面/API核对；如需恢复，必须使用原账户、原决策/订单、原时间及原幂等键，不能换键或更新时间来重放。命令自身没有自动重试。

不执行迁移或服务重启。代码撤回可停用该命令；已写入的模拟账本/事件为不可变事实，不通过删除记录回滚，订单撤销继续使用已有取消入口。
