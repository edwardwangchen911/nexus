# Provider 故障 runbook

## 首要裁决

Provider 故障不等于 NEXUS 进程故障，也不允许用未授权来源、fixture 或陈旧数据静默代替。无法证明证据时必须保持现状，不生成新的策略意图或订单。

## 三类状态

| 类型 | 表示什么 | Provider 故障时 |
| --- | --- | --- |
| `/health/live` | API 事件循环存活 | 通常仍为 alive |
| `/health/ready` | DB、migration、object store、worker 等内部运行依赖 | Provider 状态故意不纳入，不能伪报 Provider 可用 |
| integration/data quality | 特定数据源与决策证据是否可用 | 进入 `PARTIAL`/`FAIL`/`STALE`，业务 fail closed |

readiness 为 `ready` 只说明 NEXUS 内部运行面可用，绝不能作为复星、行情、财报或任何外部证据已就绪的结论。

## 响应步骤

1. 记录脱敏的 provider、operation、时间窗、错误码和 correlation ID；不记录 endpoint、header、body、账户 ID 或密钥。
2. 确认影响是连接、限流、权限、分页/对账、时点过期还是来源冲突。
3. 将受影响 capability/quality 保持为失败或不完整；不手工设为 PASS/READY。
4. 允许界面显示已持久化的最后已知事实，但必须同时显示 `as_of`、STALE/PARTIAL 和“不可据此操作”。
5. 暂停依赖该证据的研究、诊断、优化、Shadow 评估和模拟订单生成；不影响与之无关的已持久化只读功能。
6. 仅对标记为 automatic-safe/manual-safe 的幂等任务重试；未知结果使用同一幂等键查询，不生成新请求。

## 禁止的“降级”

- 不将真实读取自动切到 fixture；
- 不临时加入未经 source policy 批准的数据源；
- 不用单一报价掩盖 K 线/财报/对账缺失；
- 不延长时效窗、删除 quality flag 或改写历史 `as_of`；
- 不启动 broker profile 来“试一下”真实账户。

## 恢复与回滚

恢复后先执行 capability test、完整分页和对账，再产生新的带时点 authority/quality report。旧的 STALE/PARTIAL 事实不修改；新事实通过追加方式证明恢复。若对账仍不一致，保持 `RECONCILIATION_REQUIRED` 并停在人工调查前。
