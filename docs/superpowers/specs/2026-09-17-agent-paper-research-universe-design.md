# Agent Paper 独立研究 Universe 设计

## 目标

把 Agent Paper 从单一 `AAPL` 候选扩展为独立、版本化、可审计的研究池。首版固定为
`AAPL / MSFT / JPM`，每个五分钟时槽只研究一个标的，不改变确定性组合风控、模拟执行、
账本或 `execution_locked=true`。

## 已确认决策

- 研究池与当前持仓、展示 watchlist 和通用 `us_liquid_v1` 的成员选择相互独立。
- 首版 `agent_paper_research_v1` 按 `AAPL → MSFT → JPM` 固定排序。
- 每个周期只冻结一个候选并形成独立 `watch / buy / sell` 决策；订单意图仍进入现有组合级
  资金、仓位和硬风控。
- 当前标的形成成功终态后才前进。进程或任务中断恢复同一周期，不创建重复周期。
- 候选级失败连续三次后，第三次落盘 `RESEARCH_BLOCKED`，该标的不生成订单并轮转到下一
  标的；下一圈允许重新尝试。账户、日历、身份、凭据、行情源和结果未知等系统级失败不计
  入候选失败，也不触发轮转。

## 数据合同

研究池是代码内的小型不可变定义：版本、顺序和允许的 ticker 一起发布。每个新周期的首条
`CYCLE_PREPARED` 事件在 `fact_ids` 中冻结：

```json
{
  "research_universe_version": "agent_paper_research_v1",
  "research_universe_ordinal": 0,
  "research_symbol": "AAPL",
  "research_attempt": 1
}
```

这样无需改写历史周期或复用持仓 Universe；事件哈希同时绑定轮转身份。旧周期首事件没有这些
字段，继续按旧合同读取，但不参与新版本游标计算。候选 manifest 仍保持单元素上限；symbol
只扩大到首版的三个 ticker，manifest 哈希继续绑定实际证据。

## 轮转状态机

轮转游标从同一账户、同一研究池版本的已落盘周期派生，不维护第二份可漂移的游标：

1. 没有新版本历史时选择 `AAPL`、attempt 1。
2. `completed` 后选择下一 ordinal、attempt 1。
3. 候选级 `unavailable/failed` 后保持 ordinal，attempt 加一。
4. attempt 3 的候选级失败写成 `RESEARCH_BLOCKED`；下一周期选择下一 ordinal、attempt 1。
5. 非候选级失败保持原目标且不增加 attempt；未知或非终态周期禁止启动新周期。

数据库唯一键 `(account_id, decision_slot_at)`、触发幂等 claim 和现有 advisory lock 继续负责
并发保护。轮转选择在触发预留时冻结，正式创建周期时再次验证，发生偏移则失败关闭。

## 端到端接线

前置日线、当前趋势、风险源证明、Schwab L1 捕获、候选准备与 manifest 回读均接收同一个
`ResearchTarget`，不再各自写死 `AAPL`。Schwab 捕获仍是一次、一个 ticker、有限时长、无重连；
不会把三只同时订阅而扩大调用。通用市场 Universe 仍作为身份与风险 authority 的外部边界，
所选 ticker 必须在该快照中且全部既有证据门禁通过。

## 失败与恢复

- 已存在的 `researching` 周期继续从 `job → run → output` 恢复，轮转逻辑不重放模型。
- 仅 `CANDIDATE_EVIDENCE_UNAVAILABLE`、`RESEARCH_OUTPUT_NOT_ACTIONABLE` 和明确的候选决策失败
  计入三次阈值；触发前失败没有形成周期，因此不伪造候选失败记录。
- 第三次失败的终态为 `unavailable / RESEARCH_BLOCKED`，页面可沿现有周期结果码展示；不创建
  订单、不放宽 freshness，也不改变执行锁。
- 历史 AAPL-only 周期保持可读、可验哈希、可恢复。

## 验收

- 单元测试证明固定成员、成功轮转、失败重试、第三次阻塞轮转、版本隔离和未知状态失败关闭。
- manifest 测试证明三个 ticker 可用、Universe 外 ticker 被拒绝、旧 manifest 哈希不变。
- 入口测试证明日线、趋势、风险证明和 Schwab capture 使用同一目标 symbol。
- PostgreSQL 集成测试证明周期首事件冻结版本/ordinal/attempt，连续终态得到正确下一目标，旧周期
  不被重写。
- Ruff、Mypy 与相关 Agent Paper 测试通过后，才进行正常 CLI 的持久化读回。真实模型或券商
  调用只恢复已有未完成周期，禁止为验收重复创建同一时槽工作。
