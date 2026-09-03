# baseline 首次回测

入口复用现有点时数据加载、回测引擎和事务写入器。只使用数据库内已有的完整证据集合；不采集行情、不调用模型、不创建市场证据，不连接交易接口。

## 输入

在项目根目录使用 Python 3.12 虚拟环境，沿用已配置的 `DATABASE_URL`；不要把连接凭据写入命令或日志。

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.backtest_once \
  --strategy-version baseline_v1 \
  --authority-bundle-id '<已有证据集合 ID>' \
  --data-snapshot-id '<该集合的数据快照 ID>' \
  --source-policy-id '<该集合的来源政策 ID>' \
  --cost-policy-id '<该集合的历史成本政策 ID>' \
  --start '<集合开始交易日 YYYY-MM-DD>' \
  --end '<集合结束交易日 YYYY-MM-DD>' \
  --initial-nav '<正的初始模拟净值>' \
  --as-of '<本次读取截止时刻，ISO 8601，含 UTC 时区>'
```

ID 来自现有 `backtest_authority_bundles`，与其 `data_snapshot_id`、`source_policy_id`、`cost_policy_id`、`range_start`、`range_end` 一起核对。区间须与冻结集合一致；初始净值作为明确的模拟参数纳入协议哈希。不存在合格集合时，先补实际数据与证据；不能临时建一个占位集合或用当前持仓替代历史股票池。

基准固定沿用 SPY、同一冻结股票池等权和 baseline。成本取已发布的 `backtest_cost_policy_v1`：佣金 0、价差 5、滑点 10 基点；这是历史模拟假设，不代表当前券商实际费用，税务处理仍未知。不得把优化比较的零成本占位政策传入。

## 输出与核验

- 成功返回 `BACKTEST_COMPLETED`、`run_id` 和结果/数据/证据哈希。同一结果重复或并发运行返回同一记录，复用时为 `BACKTEST_REPLAYED`。
- 所有子记录在一个事务内写入。失败回滚，不留下部分回测；终端仅输出固定错误码，不输出原始数据库异常。
- 已有受保护查询 `GET /internal/v1/strategies`（Web BFF 为 `/api/v1/strategies`）返回 `latest_backtest`，可用 `result_hash` 对照命令行输出；来源模式由已持久化证据推导。
- 保存成功不表示多年历史/OOS 要求已满足；以 `history_requirement_met` 为准。历史结果保持 `promotable=false`、`execution_locked=true`，不自动晋级或创建订单。

## 常见缺口

| 错误码 | 含义 |
| --- | --- |
| `BACKTEST_DATABASE_UNCONFIGURED` | 当前入口未取得数据库配置 |
| `BACKTEST_CUTOFF_IN_FUTURE` | 读取截止时刻在未来 |
| `BACKTEST_FROZEN_AUTHORITY_MISSING` | 指定的冻结回测证据集合不存在 |
| `BACKTEST_AUTHORITY_BINDING_MISMATCH` | 集合与快照、来源或成本政策不一致 |
| `BACKTEST_COST_POLICY_MISMATCH` | 成本政策不是已有的历史回测政策 |
| `BACKTEST_STRATEGY_UNAVAILABLE` | 指定时刻尚无可用的策略定义 |
| `BACKTEST_AUTHORITY_OR_DATA_INCOMPLETE` | 时点、区间、数据哈希、公司行动、股票池、基准或来源核验未通过 |
| `BACKTEST_RUN_FAILED` | 参数、配置、连接或执行失败；先排查，不自动重试 |

本入口无调度或共享服务注册。停用可停止调用该模块；已提交结果保留为不可变证据，不通过删除结果或改写历史来回滚。
