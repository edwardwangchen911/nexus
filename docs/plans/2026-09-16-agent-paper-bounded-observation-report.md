# Agent Paper 有界连续观察验收报告

日期：2026-09-16（XNYS）
状态：已完成
范围：固定 AAPL、内部模拟账户、`execution_locked=true`

## 1. 验收结论

本轮原定观察窗口包含 13:05、13:10、13:15、13:20、13:25 ET 五个决策槽。五个周期均已
形成完整的 `Cycle → Job → Run → Outputs → Decision → Batch` 事实链，终态均为
`completed / NO_ORDER_REQUIRED`。每个周期都有一条确定性 `NO_CHANGE` 风控事实，没有订单、
成交或订单相关账本事务。

13:25 ET 周期验证了中断恢复：Job 创建后未立即领取，服务停止期间保留了唯一 queued Job；
之后只领取原 Job 一次，Job 成功并持久化 10 条 Output，再由 `resume` 读取原 Output 完成同一周期。
没有更换幂等键、创建替代 Run 或重复调用模型。

本轮证明连续 no-trade 路径和中断恢复可用，但不证明真实订单、五分钟执行窗口、成交或账本变动路径。

## 2. 五槽事实

| 决策槽（ET） | Job 创建 | Job 开始 | Job 完成 | 终态 | Input tokens | Output tokens | 持久化成本（USD） |
| --- | --- | --- | --- | --- | ---: | ---: | ---: |
| 13:05 | 13:08:48 | 13:09:26 | 13:10:17 | `NO_ORDER_REQUIRED` | 77,590 | 2,672 | 0 |
| 13:10 | 13:10:19 | 13:10:58 | 13:11:59 | `NO_ORDER_REQUIRED` | 77,908 | 2,499 | 0 |
| 13:15 | 13:15:02 | 13:15:39 | 13:16:40 | `NO_ORDER_REQUIRED` | 77,211 | 2,821 | 0 |
| 13:20 | 13:20:03 | 13:20:41 | 13:21:40 | `NO_ORDER_REQUIRED` | 78,085 | 2,732 | 0 |
| 13:25 | 13:25:03 | 14:06:02 | 14:07:10 | `NO_ORDER_REQUIRED` | 77,470 | 2,972 | 0 |
| **合计** |  |  |  |  | **388,264** | **13,696** | **0** |

成本列是 NEXUS 持久化的 `settled_cost_usd`，不能据此反推 ChatGPT/Codex 套餐的实际账单。

## 3. 完整性与重复检查

- Cycle / Run / Job：`5 / 5 / 5`；全部 Run completed、Job succeeded。
- Output：每个 Run 10 条，共 50 条。
- Decision / Batch / NO_CHANGE：每槽各 1 条，共 `5 / 5 / 5`。
- 活动 Job：0；非成功 attempt：0。
- 订单 / 成交 / 订单相关账本事务：`0 / 0 / 0`。
- 重复 cycle、Run 引用、Job、Output 序列、Decision、Batch、Market Window：全部为 0。
- 五条 Job lease 均已过期，并分别匹配同 Job 的成功 attempt 与 fence token；它们是保留的审计事实，
  不是活动 Worker 租约，不删除。
- 本观察窗口没有产生新的 outbox 事件；无订单终态不制造 Bark 订单通知。

## 4. 服务停止与恢复边界

Agent Paper 宿主机服务已确认无进程，并按运行手册清理 inactive 托管注册，最终托管状态为
`stopped`。数据库 heartbeat 保留为 stale 历史事实，未删除或伪造新心跳。预检仍为 ready，
`execution_locked=true`，但本报告定稿前不再启动新决策周期。

## 5. 范围外事实

收口过程中曾错误启用后续 Universe 试验，额外产生一个 14:10 ET AAPL 周期。该周期随后完成为
`NO_ORDER_REQUIRED`，包含一个成功 Job、10 条 Output、一个 Decision 和一个 Batch，订单与成交均为 0。
它不属于原定五槽观察，不进入本报告的成本合计或 Agent/hold/SPY 对照样本，也不会被删除或改写。

轮换 Universe 提交已从 `main` 撤销，仅保留在独立分支供未来单独审查；当前基线继续固定 AAPL。

## 6. Agent、hold_current 与 SPY 同窗对照

正常 Agent Paper 服务入口已在数据库时间 2026-09-16 20:28 UTC 执行唯一待办动作
`publish_performance`，并发布 `as_of=2026-09-16 20:00 UTC` 的收盘绩效快照。执行前后 cycle 总数
保持 50，活动 Job、订单和成交均为 0；随后前台循环以 `stop_requested` 正常退出，没有创建新周期。

最终同窗结果如下：

| 对照 | 数据完整性 | 净值 | 累计收益 | 最大回撤 | 换手 | 累计成本 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Agent | 完整 | USD 10,000.00 | 0% | 0% | 0% | USD 0.00 |
| `hold_current` | 完整 | USD 10,000.00 | 0% | 0% | 0% | USD 0.00 |
| `baseline_v1` | 缺少同窗前向事实 | — | — | — | — | — |
| SPY | 缺少同窗前向事实 | — | — | — | — | — |

数据库、Engine/BFF 与真实 `/paper` 页面均读回同一份 20:00 UTC 事实。页面显示账户绩效、四方同窗
对照、空持仓和空订单；Agent 与 `hold_current` 完整，`baseline_v1` 与 SPY 均明确返回
`SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE`，没有用前一交易日快照或历史回测填充。页面返回 200，
浏览器控制台未见错误；刷新页面没有增加 cycle、Job、订单或成交。

收盘发布路径也已提前完成隔离验证：策略与执行层 28 项通过；在自动创建并删除的 `nexus_test_*`
PostgreSQL 数据库中，日历绑定、精确收盘发布、重放/顺序/不可变约束及绩效投影等 23 项通过。
隔离验证与本次真实发布结果一致；本次没有再次调用模型、Provider 或重新联调研究链路。

## 7. 当前边界与下一步

第二阶段现已确认“固定候选的多槽 no-trade 连续运行、中断恢复与收盘绩效发布”，不能宣称自然发生的
真实订单路径已经完成。`baseline_v1` 与 SPY 缺少同窗前向事实是本轮最终证据边界，不通过补数据源、
改窗口或重跑历史来制造完整结果，也不阻塞后续阶段的代码审计、隔离验证或不含外部写入的实施。
后续切片按路线图继续推进 Agent 复盘与版本进化；不扩张 Universe、Shadow、数据源或调度框架，
不强迫 Agent 为验收下单。券商模拟盘真正发送外部订单前仍需单独授权，`execution_locked=true`
保持不变。
