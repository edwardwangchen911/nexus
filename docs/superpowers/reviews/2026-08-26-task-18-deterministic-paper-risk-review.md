# Task 18 确定性硬风控与内部模拟账本验收复审

**日期：** 2026-08-26
**结论：** Task 18 在“单用户、USD 10,000 内部模拟账户、baseline_v1 前向决策、确定性硬风控、无模拟成交”边界内通过验收。实现没有新增或调用复星/券商写接口，没有真实账户访问、真实订单、部署、密钥或权限变更。

## 1. 范围与版本

- 规格：`docs/superpowers/specs/2026-08-26-task-18-deterministic-paper-risk-design.md`
- 实施计划：`docs/superpowers/plans/2026-08-26-task-18-deterministic-paper-risk.md`
- 规格至最终实现的包含式范围：`d04bcca^..20f0e01`
- 最终实现提交：`20f0e01`
- 前置 Task 17：`c9e4e60`
- 变更规模：64 个文件，16,208 行新增，47 行删除。

复审在实现和首轮全量测试之后，重新仅以已提交差异与设计规格为输入，检查时点、权威、Decimal、数据库绕过、并发、账本和交易边界。受当前不委派约束，这是**单执行者分离式复审**，不是第二人或第二模型背书。

## 2. 实现结果

- 新增独立 `internal_usd_v1` 模拟账户，固定 USD、cash、long-only、初始资金 10,000；不与复星只读组合或 Shadow NAV 合并。
- 持久化不可变 `baseline_v1` 前向决策及目标；旧决策在同账户/策略已有更晚会话且尚未建批次时 fail-closed，已存在批次仍可幂等重放。
- 风险输入只接受决策时点前可用的冻结日线 raw close、完整行业暴露、日历、质量、Universe 和公司行动/停牌/退市查询证明。
- 以 Decimal 纯函数计算 approved/reduced/rejected，顺序固定；最坏价格向上取整，风险阈值、现金和仓位由确定性代码控制，Agent/前端不能覆写。
- 订单、风险、状态事件、预留、双重记账、审计和 outbox 在同一事务原子追加；并发使用账户级 advisory lock、行锁和唯一约束收敛。
- 数据库组合外键和 deferred trigger 阻止跨账户、跨批次、跨订单、跨标的、跨 policy/authority 串接；原始 SQL 不能改写事实、伪造事件链、不平衡账本或错误预留。
- `income.realized_pnl.usd` 是唯一允许有符号余额的科目，以支持 Task 19 的实际亏损；现金、预留、持仓成本、投入资本、股息和费用仍不得为负。
- Task 19 的 fill、position adjustment、position snapshot、performance snapshot 表仅建立受保护契约，Task 18 验收中保持零行。

## 3. 验收证据

### Engine 与数据库

| 命令/检查 | 最终结果 |
| --- | --- |
| `pytest --import-mode=importlib engine/tests -q` | `1220 passed in 912.87s` |
| Task 18 风险/账本/并发/迁移相关域 | `170 passed in 85.88s` |
| 原始行哈希、迁移往返、数据库绕过最终子集 | `36 passed in 47.87s` |
| `ruff check engine` | exit 0，`All checks passed!` |
| `mypy engine/src` | exit 0，195 个源文件无问题 |
| `git diff --check` | exit 0 |

最初一次全量运行为 `1204 passed, 5 failed in 891.35s`；五个失败都是旧迁移测试仍断言 `0013_shadow` 为 head，且救援基线 glob 误包含新 `0014_paper`。`0d98b59` 更新 head 断言并把基线锁定到 `0001..0010`，随后首轮全量为 `1209 passed in 882.27s`。分离式复审修复后，本次从头重跑得到最终 `1220/1220`。

迁移测试在随机临时数据库完成 `upgrade head -> 0014_paper`、`downgrade 0013_shadow`、再 `upgrade head -> 0014_paper`。最终 PostgreSQL 残留检查：

```text
temporary_databases=0
temporary_roles=0
temporary_connections=0
```

一次性 rollback-only 验收从原始行重算并匹配：前向决策、组合投影、风险输入、单订单风险结果、整批结果、订单批次和每笔 ledger transaction 哈希；订单事件连续、预留精确、借贷相等。事务回滚后 `(batch, order, reservation, non-funding ledger, idempotency, audit, outbox)` 残留均为 `0`，Task 19 四类事实和 broker order 均为 `0`。

### Web

| 命令/检查 | 结果 |
| --- | --- |
| `npm test` | 32 个文件、347/347 单测通过 |
| `npm run typecheck` | exit 0 |
| `npm run lint` | exit 0 |
| `npm run check:generated` | exit 0 |
| `npm run build` | exit 0 |

首次 `check:generated` 正确发现 portfolio subtype 契约过期；生成并审查预期的 `internal_paper` discriminated-union 差异后由 `4ffab14` 提交，最终生成检查通过。Task 18 没有新增或改变渲染页面，页面与 E2E 属于 Task 19，因此本任务没有运行也没有声称运行页面 E2E。

## 4. 分离式架构与安全复审发现

| 级别 | 发现 | 红测/修正 | 状态 |
| --- | --- | --- | --- |
| Important | `reference_close × 1.01` 使用 half-even 量化时可在第 13 位小数向下取整，低估现金预留。 | 精确边界红测后，`df82313` 改为 `ROUND_CEILING`。 | 已解决 |
| Important | 若子表分别引用合法父行，原 schema 仍可把风险评估串到另一批次、现金预留串到另一订单、仓位/成交串到另一标的，且 Task 19 成交 authority 未强制存在。 | 原始 SQL 红测；`20f0e01` 增加组合父键、精确 root trigger、fill authority/notional 和调整事件绑定。 | 已解决 |
| Important | 所有科目统一禁止负 normal-side 余额会阻止亏损记入 `income.realized_pnl.usd`，使 Task 19 无法实现亏损成交。 | 纯函数与 PostgreSQL 红测；`20f0e01` 只允许 realized P&L 有符号，现金等资源科目继续非负。 | 已解决 |
| Important | 更晚策略决策已存在时，旧决策仍可按旧 `decision_time` 投影并追溯创建订单，忽略后续预留。 | 追溯决策红测；`20f0e01` 对尚无批次的 superseded decision 稳定返回冲突且零订单。 | 已解决 |
| Minor | query authority hash 接受非 UTC offset，并与其他 canonical hash 的 `Z` 序列化不一致。 | UTC 红测；`42d7704` 统一拒绝非 UTC 并固定六位微秒 `Z`。 | 已解决 |

**计数：** Critical `0`；Important `4` 且全部已解决；Minor `1` 且已解决；未解决问题 `0`。

复审还检查了：available_at/decision_time 前视边界、exact-set 权威和来源切换、15/30/10 阈值、8/6 回撤锁、pending buy/sell 投影、幂等/锁顺序/死锁、事件连续性、append-only、审计/outbox 回放、安全错误、凭据泄漏以及 Agent/broker/live/approval 隔离，未留下其他问题。

## 5. 回滚与后续边界

- 运行时失败由单事务回滚；重试使用原 decision 与 idempotency key，不删除历史事实。
- schema rollback 仅适用于尚无 Task 18 业务事实的环境；`0014 -> 0013` 会删除 Task 18 自有表/触发器并移除三个冗余父表唯一约束，不改写既有父表数据。
- 停止未来 Task 20 调度即可阻止新批次，已提交内部模拟订单保留审计事实。
- 下一执行单元是 Task 19：仅模拟选定来源的成交、内部账本/绩效和 paper 页面。它不是复星模拟盘或实盘交易。
- Task 20、Task 21、Stage 2 策略自进化、真实复星只读激活、任何真实交易、部署和完整 NEXUS 目标仍未完成。
