# baseline_v1 同窗前向对照设计

状态：待用户复核；暂不开发

日期：2026-09-15

上位路线图：`docs/plans/2026-09-12-nexus-simulation-evolution-roadmap.md`

## 1. 结论

`baseline_v1` 应作为独立的前向模拟对照运行，但不建设第二套执行系统。最小方案是在现有
`accounts → portfolios → paper_accounts → decision/order/fill/ledger/performance` 模型中增加一个
隔离的 baseline 比较账户，继续复用现有策略、风控、成交、账本和绩效服务。

Agent 主账户和 baseline 比较账户从一个明确的共同起点使用相同初始资金和现金状态，此后分别维护
持仓与账本。`/paper` 只读取双方已持久化事实；任何一侧缺少共同窗口、冻结输入、成交价格或绩效
快照时，baseline 行保持 `UNAVAILABLE`，不得用历史回测或事后重放填充。

## 2. 已核实状态与断点

截至 2026-09-15 的本地业务库只读核查结果：

- 只有一个 `internal_usd_v1` 内部模拟账户，初始资金为 USD 10,000，当前 Agent 路径使用该账户；
- 已有 5 条 `agent_paper_decision_v1` 决策，没有 `baseline_v1` 前向决策、订单或成交；
- `strategy_rank_checkpoints`、`backtest_runs`、`backtest_authority_bundles` 和
  `backtest_authority_collections` 均为空；
- `us_liquid_v1` 当前只有一个成员；该成员仅有一根已发布的 Alpaca SIP all-adjusted 日线，尚不满足
  `baseline_v1` 的 200 个交易日特征要求；
- 业务库没有质量报告，只有一条已审核行业记录；现有事实不足以回填 2026-09-11 的前向决策；
- `PaperReadService.account()` 当前以“全库恰好一个内部模拟账户”为隐含合同。直接插入第二个账户会
  令现有 `/paper` 读取返回 409，必须先改为按稳定账户角色选择。

这些断点说明当前 `SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE` 是诚实结果，不是页面漏接。

## 3. 产品完成标准

在 `/paper` 的连续模拟总览中：

1. Agent、`hold_current`、`baseline_v1` 和 SPY 市场基准使用明确的共同比较窗口；
2. baseline 行只来自 baseline 比较账户的不可变决策、订单、成交、账本和绩效快照；
3. 显示 baseline 的净值、收益、最大回撤、换手和总成本；
4. baseline 尚未启动、输入不足、窗口不重叠、绩效未发布或对账失败时显示稳定原因码；
5. 刷新页面不触发行情、模型、策略、订单或绩效生产；
6. baseline 不替代 Agent、不跟随 Agent 五分钟频率、不调用研究模型，也不获得券商写权限；
7. `execution_locked=true` 保持不变。

## 4. 账户隔离与初始化

### 4.1 稳定角色

- Agent 主账户沿用代码 `internal_usd_v1`；
- baseline 比较账户使用代码 `baseline_v1_usd`；
- 两者的 `Account.provider` 均为 `internal`，不创建外部账户标识；
- baseline 使用独立 `Account`、`Portfolio`、`PaperAccountRecord` 和账本事实，避免持仓、现金、预留、
  回撤锁及幂等键互相污染。

现有 `PaperAccountRecord.code` 已有唯一约束，足以承担角色选择；本切片不增加表、列、枚举或迁移。

### 4.2 正常初始化入口

新增一个有界、本地、显式的一次性初始化入口。它在单个事务中：

1. 锁定并读取 `internal_usd_v1`；
2. 拒绝主账户不存在、不是全现金、存在未结订单或最新投影不可对账的状态；
3. 创建 baseline 的内部 Account、Portfolio、PaperAccount、标准账本科目和 USD 10,000 初始注资事实；
4. 把双方共同起点固定为首个同时拥有绩效快照的完整 XNYS 收盘；
5. 重复调用时按 `baseline_v1_usd` 读取并核对完整内容，一致则返回 replay，不一致则冲突退出。

入口不复制真实券商持仓，不接受客户端随意指定资金，不删除或覆盖已有事实。当前主账户仍为全现金，
因此首个实施版本只支持“相同现金起点”；未来若需要复制非空持仓，应另行设计，不能隐式扩张。

## 5. baseline 前向生产

baseline 继续使用已注册的 `baseline_v1` 定义和现有 `rank_checkpoint → paper order batch → execution
window → fill → ledger → performance` 链路：

```text
冻结的日历、股票池、raw/all 日线、质量报告和行业事实
  → baseline_v1 周频确定性决策
  → baseline 比较账户的 rank checkpoint
  → 现有 RiskPolicyV1
  → 现有内部模拟订单、成交或确定性拒绝
  → baseline 独立账本和收盘绩效快照
  → /paper 只读对照
```

- baseline 只按策略自身的周频门槛求值，不在每个 Agent 五分钟槽重复决策；
- 非周末决策日返回 `not_week_end` 且不写策略决策；
- 至少补齐 200 个交易日的 AAPL Alpaca SIP raw/all 日线、对应冻结股票池、质量报告和已审核行业事实后
  才能开始；使用现有有界采集及导入能力，不新增 provider 或自动 fallback；
- 新采集事实只能支持其真实 `available_at` 之后的前向决策，不回填 2026-09-11；
- 模拟成交继续使用既定的 Schwab 五分钟 raw 窗口合同；缺少合格窗口时保留订单状态并 fail closed；
- baseline 不触发研究 Agent 或模型调用。

首个共同窗口从 baseline 初始化后的下一个完整、双方均已发布绩效快照的 XNYS 收盘开始。此前的 Agent
历史继续保留，但不计入同窗收益。

## 6. API 与页面读取

### 6.1 主账户选择

将内部读取从“恰好一个 simulation account”改为显式读取 `internal_usd_v1`。出现零个、重复代码、
能力不符或 Portfolio 绑定错误时继续返回 `DATA_PARTIAL`。Agent cycle、订单、组合与运行状态接口都只
使用该主账户，不因 baseline 账户存在而改变。

### 6.2 对照读取

`PaperReadService` 为 `baseline_v1` 行加载 `baseline_v1_usd`，并验证：

- 两账户初始资金、币种、现金/long-only 模型一致；
- 公共窗口起止收盘均有双方不可变绩效快照；
- baseline 窗口内订单只绑定 `registered_strategy / baseline_v1` 决策；
- Agent 窗口内订单只绑定 `agent_research` 决策；
- 账本、成交和绩效能够按既有投影规则对账。

共同窗口收益以双方在窗口起点的 NAV 为分母，不再假设初始资本就是任意晚窗口的起始 NAV。换手和
成本也只累计共同窗口内事实。SPY 行同样只读取公共窗口内的交易日收盘价；`hold_current` 继续按相同
起点保持现金。现有 API 顶层 `window_start` 表示四方公共起点，不新增第二套响应模型。

### 6.3 稳定不可用原因

- `BASELINE_ACCOUNT_UNAVAILABLE`：比较账户未初始化或合同不匹配；
- `BASELINE_COMMON_WINDOW_UNAVAILABLE`：双方尚无共同起止绩效快照；
- `BASELINE_FORWARD_FACTS_UNAVAILABLE`：共同窗口内尚无合格 baseline 前向事实；
- `BASELINE_COMPARISON_CONTAMINATED`：账户中出现不属于预期 origin/version 的订单或成交；
- `BASELINE_PERFORMANCE_UNAVAILABLE`：绩效或对账事实缺失。

错误只影响 baseline 行，不把已可用的 Agent、现金持有和 SPY 行整体降级；资金、账本或身份污染除外，
这类错误继续 fail closed。

## 7. 明确不做

- 不合并或复制 Research Shadow 分支；
- 不新增比较表、虚拟持仓 JSON、事件总线、Redis、Kafka、调度平台或第二套执行服务；
- 不用历史 backtest、当前价格倒推或事后采集数据冒充前向事实；
- 不为形成交易强制改变 baseline 规则、股票池、风控、成本或成交模型；
- 不扩展到多 baseline、多资金方案、动态基准选择或券商正式模拟盘；
- 不解除执行锁，不发送券商订单。

## 8. 验收

### 8.1 受控测试

- 账户初始化首次成功、完全一致重放、内容冲突、非现金主账户和未结订单拒绝；
- 两账户读取隔离，新增 baseline 账户后所有现有 Agent `/paper` 接口不出现 409；
- baseline 的周末决策、非周末 no-write、买入、卖出、风控拒绝、部分成交和幂等；
- 公共窗口分母、净值、回撤、换手和成本计算；
- 缺账户、缺共同窗口、缺前向事实、origin 污染和缺绩效的稳定降级；
- GET 页面和 API 不产生任何写入或外部调用。

### 8.2 真实验收

1. 通过现有正常入口采集并导入 baseline 所需的真实冻结日线及辅助事实；
2. 用一次性正常入口初始化 `baseline_v1_usd`，核对两账户共同现金起点；
3. 在未来首个合格周频决策时点运行 baseline，接受订单、拒绝或合理无交易的真实结果；
4. 若有订单，通过正常 Schwab 窗口入口推进内部模拟；若无合格窗口则保留 no-fill；
5. 发布双方同一 XNYS 收盘的绩效快照；
6. 从 PostgreSQL、Engine API、BFF 和 `/paper` 回读同一组事实；
7. 核对没有模型调用、broker 写入、Shadow 数据或 execution unlock。

受控成交测试不能冒充真实 baseline 前向验收；真实无交易可以证明读取闭环，但不能证明真实成交路径。

## 9. 回滚与恢复

发布严格按以下顺序进行：先发布“按稳定代码选择主账户”的兼容读取，使单账户状态下 baseline 行继续
安全显示不可用；再运行一次性 baseline 初始化；最后才启用 baseline 前向生产。不得先插入第二账户再
部署读取修复。

- 初始化或单次生产在事务提交前失败：整体回滚，不留下半账户、半账本或半决策；
- 结果未知：先按稳定账户代码、决策会话和原幂等键查持久化状态，不换键重放；
- 运行异常：停止 baseline 生产入口，Agent 主循环继续独立运行；
- 展示异常：baseline 行恢复 `UNAVAILABLE`，不影响其他三行；
- 代码回退：按 `internal_usd_v1` 选择主账户的兼容修复必须与 baseline 初始化同时发布，旧版“恰好一个
  账户”代码不得在第二账户已存在时重新启用；
- 已产生的 baseline 决策、订单、成交、账本和绩效均保留为不可变审计事实，不删除、不改写历史；
- 本切片无数据库迁移，因此没有 downgrade。

## 10. 方案取舍与审查

未采用的方案：

1. **历史回测填充 baseline 行**：实现较快，但不是同窗前向事实，拒绝；
2. **在 Agent 账户内维护隐式虚拟持仓**：少一个账户，却需要新状态和特殊投影，且容易污染资金与风控，
   拒绝；
3. **独立比较子系统或 Shadow 合并**：能容纳更多未来策略，但当前没有对应需求，属于过度设计，拒绝。

推荐方案评分：

| 维度 | 满分 | 得分 | 依据 |
| --- | ---: | ---: | --- |
| 产品目标与事实真实性 | 20 | 20 | 独立前向事实，不回填、不冒充回测 |
| 最小复用与隔离 | 20 | 19 | 复用全部核心服务，只增加账户角色和初始化入口 |
| 数据、时间与比较口径 | 20 | 19 | 明确 available_at、周频和公共 NAV 分母 |
| 风控、权限与幂等 | 20 | 20 | 独立账本、现有硬风控、无 broker、保持执行锁 |
| 验收、恢复与回滚 | 20 | 19 | 覆盖真实链路和未知结果；真实成交仍取决于未来信号与窗口 |
| **总分** | **100** | **97** | **达到 95 分门槛，无 P0/P1 未决项** |

占位符扫描：无。范围扫描：不含新表、迁移、新 provider、自动 fallback、Shadow、券商写入或策略改写。
实现前置只有两项：补齐 baseline 真实冻结输入，以及先发布主账户按稳定代码选择的兼容改动。
