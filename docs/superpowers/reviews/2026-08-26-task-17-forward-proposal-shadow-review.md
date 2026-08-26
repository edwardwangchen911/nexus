# Task 17 Forward Proposal Shadow 验收与复审

**日期：** 2026-08-26
**结论：** Task 17 在“静态三方 Proposal Shadow”边界内通过验收。实现不创建 paper/broker/live 订单、成交、执行意图或交易审批，也不调用复星 API。

## 1. 范围与版本

- 批准规格：`docs/superpowers/specs/2026-08-26-task-17-forward-proposal-shadow-design.md`
- 实施计划：`docs/superpowers/plans/2026-08-26-task-17-forward-proposal-shadow.md`
- 规格到最终可验收实现：`f27cb9e..c9e4e60`
- 运行代码起点：`f8a2076`
- 最终代码/测试提交：`c9e4e60`
- 前置 Task 16 基线：`2db9c1b`

审查按实施后的独立阶段重新从权威、时点、数据库绕过、并发、BFF/UI 与下单边界进行对抗式复核。受当前不委派约束，这是**单执行者分离式复审**，不是第二人或第二模型的组织独立审查；该限制不得被误报为外部独立背书。

## 2. 运行时与迁移证据

- 已有 Docker 兼容运行时可用，不需要安装 Docker Desktop。
- `nexus-phase01-task2-postgres-1` 与 `nexus-phase01-task2-minio-1` 验收时均为 healthy。
- Compose 配置对齐时曾重建现有容器，PostgreSQL 原匿名 volume 被保留；主库在验收后仍为 `0001_runtime_and_data`，未在主库应用 Task 17 schema。
- 随机临时数据库完成 `upgrade head -> 0013_shadow`、`downgrade 0012_backtests`、再 `upgrade head -> 0013_shadow`；最终全量集成测试再次覆盖迁移往返、约束名、append-only trigger、deferred exact-set 与 lifecycle 恢复。
- 所有破坏性迁移测试只使用命名受限的临时角色/随机测试库；最终残留角色 `0`，残留 `nexus_test_*` 数据库 `0`。

## 3. 验收门禁

### Engine

| 命令/检查 | 结果 |
| --- | --- |
| `pytest engine/tests/unit/simulation engine/tests/unit/backtest engine/tests/unit/shadow engine/tests/property engine/tests/integration -q` | `649 passed in 727.16s` |
| `ruff check engine` | exit 0，`All checks passed!` |
| `mypy engine/src` | exit 0，179 个源文件无问题 |
| `git diff --check` | exit 0 |

最终全量前的一次运行为 `647 passed, 2 failed`；两个失败都是旧测试用“数据库时间/加一天”猜测 XNYS 决策时点，与新增的未到点门禁冲突。`c9e4e60` 改为从冻结 calendar authority 读取真实 `decision_at`；两个回归与未到点拒绝测试先聚焦 `3/3` 通过，随后从头完成上述 `649/649`。

### Web

| 命令/检查 | 结果 |
| --- | --- |
| `npm test` | 347/347 |
| `npm run typecheck` | exit 0 |
| `npm run lint` | exit 0 |
| `npm run check:generated` | exit 0 |
| `npm run build` | exit 0 |
| `npm run test:e2e` | 24/24 |

E2E 覆盖 375、390、768、1024、1440 px 与 844x390 横屏，包含键盘逐日导航、reduced motion、完整 NAV 表、无页面级水平溢出以及 axe serious/critical `0`。页面没有 order、trade、approval、promotion 或 self-evolution 操作。

### 确定性与边界验证

一次性验收脚本在临时库中得到：

```text
rollback_shadow_fact_rows=0
session_63_state_hash_match=true
checkpoint_63_metrics_hash_match=true
protected_row_delta=(0, 0, 0)
paper_live_execution_table_count=0
```

这证明测试回滚不留 Shadow 事实，第 63 会话状态和检查点指标可从 append-only 输入精确重算，已保护上游行无变化，且 Task 18/paper/live execution 表并未被引入。

## 4. 对抗复审发现

| 级别 | 发现 | 红测/修正 | 状态 |
| --- | --- | --- | --- |
| Important | 查询证明可声称一个数据库中不存在的 member；同一来源还可对同一标的同时声称“有 Bar”和“无 Bar”。 | `9194d88` 按每个准入来源校验数据库可见 member 精确集；`1fd09e2` 拒绝 present/negative 交集。 | 已解决 |
| Important | 上游若提前把 Bar/查询证明标为可用，Shadow 可在冻结 `decision_at` 之前推进；跳到宽限期末也可提前过期。 | `1fd09e2` 在 authority loader 与 grace-end 过期路径都使用冻结 calendar `decision_at` 关闭时点绕过。 | 已解决 |
| Important | 直接数据库写入可在宽限截止前提交 `SHADOW_DATA_INCOMPLETE` seal，且 seal reason/coverage 与实际 authority 覆盖的绑定不足。 | `1fd09e2` 增加 reason-code CHECK，deferred trigger 校验实际 authority count、初始执行缺失必须 63 会话、数据缺口 seal 不得早于 grace `decision_at`。 | 已解决 |

**计数：** Critical `0`；Important `3` 且全部已解决；未解决 Minor `0`。

复审还检查了前视偏差、会话缺口/禁止 carry-forward、Decimal 与费用单位、仅首日执行、幂等、proposal-first 锁顺序、start/reject/advance/seal 竞态、append-only 绕过、BFF GET/POST schema 分流、稳定错误与凭据泄漏、键盘/图表可访问性和下单边界，未留下其他 Critical/Important/Minor 发现。

## 5. 非本任务完成项

Task 17 的“验证”只是静态提案三组合的证据状态，不是买卖建议或下单授权。以下仍未完成：

- Task 18 硬风控、内部模拟账本与订单状态机；
- Task 19 模拟成交与绩效；
- Task 20 作业、outbox、对账与可观测性；
- Task 21 最终运维门禁；
- Stage 2 Agent/Strategy Evolution Shadow 与策略自进化；
- 复星账户/实盘交易、部署、密钥或任何外部写操作；
- 完整 NEXUS MVP。

下一执行单元是 Task 18，需在用户选择继续后单独启动。
