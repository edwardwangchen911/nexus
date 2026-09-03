# 内部模拟单次操作

`python -m nexus.ops.paper_once` 复用现有内部模拟服务。`forward` 从显式冻结资料准备决策和持仓状态，再调用已有风控准入；`admit` 和 `advance` 使用已存决策或订单。命令不创建账户或初始资金、不采集数据、不启用每日调度，也不发送券商订单。使用前须已有明确选择的内部模拟账户。

在仓库根目录使用项目 Python 环境，沿用已配置的 `DATABASE_URL`。数据库只接受 `postgresql+asyncpg` 和 `localhost`、`127.0.0.1` 或 `::1`；不支持通过 URL 查询参数覆盖连接地址。不要把连接串、账户事实或命令结果粘贴到聊天。

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
