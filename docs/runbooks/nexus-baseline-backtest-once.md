# baseline 首次回测

入口复用现有点时数据加载、回测引擎和事务写入器。只使用数据库内已有的完整源证据集合；不采集行情、不调用模型、不创建源市场证据，不连接交易接口。可以直接运行已有数据包，也可以先从完整源集合装配新数据包再运行。

## 从现有完整源集合准备后运行

准备模式与下方已有数据包模式互斥，不接受额外的旧模式参数。请求文件须为不超过 1 MiB 的普通文件（不接受符号链接），内容为闭合 JSON：

```json
{
  "strategy_version": "baseline_v1",
  "calendar_authority_id": "<已有日历 ID>",
  "universe_collection_id": "<完整历史股票池集合 ID>",
  "sector_collection_id": "<完整行业集合 ID>",
  "corporate_action_collection_id": "<完整公司行动集合 ID>",
  "spy_instrument_alias_id": "<原 SPY ticker 别名 ID>",
  "equal_weight_universe_snapshot_id": "<上述历史股票池集合中的基准快照 ID>",
  "source_policy_id": "<已发布来源政策 ID>",
  "cost_policy_id": "<历史回测成本政策 ID>",
  "start": "2021-01-08",
  "end": "2021-01-11",
  "initial_nav": "10000",
  "idempotency_key": "baseline-local-example"
}
```

示例日期及净值只是参数格式，不代表已有可用历史。填写实际已存在的源集合及相应区间；金额使用十进制字符串。key 为 1–100 个字母、数字、下划线、点、冒号或连字符。

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.backtest_once \
  --prepare-request-file '<本地请求 JSON 文件>'
```

程序从集合读取精确成员，按原日历及已发布政策选择 raw/all 价格候选，并独立核对股票池、行业、公司行动和基准。请求不能提供成员列表、选择结果、哈希或 complete/PASS 标记。不存在完整源集合时继续拒绝；空行动列表、分页耗尽或当前持仓名单不能替代历史完整性证明。

新数据包保存实际冻结时间，后续回测读取截止不早于该物化时间；准备模式不接受调用者倒填 `as_of`。同 key 同参数核验并复用原包，不重新挑选后来资料；同 key 不同参数拒绝。源集合和旧数据包均不改写。

准备阶段独立事务提交合法数据包，然后调用原回测入口。因此随后回测失败时，合法数据包可能已经存在：不能宣称整个命令零写入，也不应换 key 重复组包。排障后按原请求重入复用；准备阶段本身失败则回滚该次新包、成员与快照。所有终端失败仍只返回固定错误码，不打印请求正文或数据库异常。

本模式只补“已有完整源集合→冻结数据包”的生产接线，不生产历史源集合，不证明真实历史、多年 OOS 或晋级资格已齐备。

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

本入口无调度或共享服务注册。停用可停止调用该模块；已提交数据包及结果保留为不可变证据，不通过删除结果或改写历史来回滚。无新增迁移，旧参数模式和旧包哈希/回放语义保持。
