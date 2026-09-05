# baseline 首次回测

入口复用现有点时数据加载、回测引擎和事务写入器。回测只使用数据库内已有的完整源证据集合；不采集行情、不调用模型、不创建历史股票池、行业或公司行动证据，不连接交易接口。可以单独准备本地库的规范日历、直接运行已有数据包，或从完整源集合装配新数据包再运行。

## 只读预检未来内部股票池

沿用已配置的本地 `DATABASE_URL`，选择尚未开盘的有效 XNYS 交易日：

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.backtest_once \
  --preflight-universe --effective-session '<未来交易日 YYYY-MM-DD>'
```

该模式与其他模式互斥。固定定义随安装包保存在 `nexus/backtest/data/us_liquid_v1.csv`；49 个成员保持原定义，CSV 旧日期不作为发布时间。程序在同一 `REPEATABLE READ, READ ONLY` 事务中核对数据库时钟、已有 Instrument 与唯一有效 ticker alias。等于开盘时刻也拒绝，身份缺失、晚到、错配或歧义分别返回固定缺口分类。

输出仅包含 `status`、`member_count`、`gap_categories`。`BACKTEST_UNIVERSE_PREFLIGHT_READY` 表示本次身份和日期前置通过；`BACKTEST_UNIVERSE_PREFLIGHT_BLOCKED` 表示有明确缺口，两者都没有发布股票池、创建成员、生成回测或启动调度。预检结果不保留未来发布资格，实际发布必须重新核验。当前版本尚未实现发布动作，也不补成过去的 PIT 证据。

## 单独准备规范日历

使用当前 Python 环境安装的 `exchange_calendars` 生成 XNYS 日历。此模式只接收以下三个参数，与下方两种模式的其他参数互斥：

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.backtest_once \
  --prepare-calendar --start '2021-01-08' --end '2021-01-11'
```

开始和结束必须都是库支持范围内的实际交易日，且有序；不自动截断或将节假日移动到临近交易日。日期是格式示例；实际日历须覆盖拟用历史及策略预热区间，不仅是回测起止两日。所有 session、开闭市及收盘后 30 分钟决策时点均取当前安装库，节假日、提前收市及夏令时由该版本日历确定。

成功只返回 `BACKTEST_CALENDAR_PREPARED`、`calendar_authority_id`、`content_hash`、实际包版本组成的 `version`、`start/end`、`backtest_executed=false` 与 `execution_locked=true`。将此 ID 填入下方 `calendar_authority_id`；本命令不创建数据包、回测结果或订单，不调用外部接口或模型。

根和全部 sessions 在同一事务写入，创建/发布时间取实际物化时刻，不倒填到历史日期。同版本、同区间精确核验并复用原 ID；并发不会重复创建。发现已存在记录字段、hash、时点或 session 集不一致时拒绝，不修补或重标旧根。现有 Fosun display 日历保持原样，不能因同属 XNYS 而当作本日历复用。

这里的完整性只表示当前安装库对应区间的规范输出，不宣称交易所原始官方归档、历史版本修订或源市场数据已验收。缺失 universe/sector/actions、历史价格或来源资格时，下方组包仍拒绝；日历成功不代表 T1 历史输入已齐备。

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
| `BACKTEST_REQUEST_INVALID` | 缺少必要参数或混用互斥模式；不加载运行配置 |
| `BACKTEST_CALENDAR_RANGE_INVALID` | 日期无序、端点不是实际 session 或超出安装库支持范围 |
| `BACKTEST_CALENDAR_AUTHORITY_INVALID` | 同自然键的既有日历不完整、不一致或时点不可见；不修补历史 |
| `BACKTEST_CUTOFF_IN_FUTURE` | 读取截止时刻在未来 |
| `BACKTEST_FROZEN_AUTHORITY_MISSING` | 指定的冻结回测证据集合不存在 |
| `BACKTEST_AUTHORITY_BINDING_MISMATCH` | 集合与快照、来源或成本政策不一致 |
| `BACKTEST_COST_POLICY_MISMATCH` | 成本政策不是已有的历史回测政策 |
| `BACKTEST_STRATEGY_UNAVAILABLE` | 指定时刻尚无可用的策略定义 |
| `BACKTEST_AUTHORITY_OR_DATA_INCOMPLETE` | 时点、区间、数据哈希、公司行动、股票池、基准或来源核验未通过 |
| `BACKTEST_RUN_FAILED` | 参数、配置、连接或执行失败；先排查，不自动重试 |

本入口无调度或共享服务注册。停用相应准备/运行模式即可回退入口；已提交日历、数据包及结果保留为不可变证据，不通过删除记录或改写历史来回滚。无新增迁移或依赖，旧参数模式和旧包哈希/回放语义保持。
