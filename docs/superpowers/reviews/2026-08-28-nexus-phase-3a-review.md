# NEXUS Phase 3A 离线券商模拟执行验收审查

**当前结论：** `Phase 3A offline_ready / Phase 3B external_activation_blocked`。离线 fixture 已覆盖“服务器生成意图 → 硬风控 → 人工批准事实 → 串行一次发送 → 未知结果锁定 → 主动对账”的完整状态机；任何复星模拟盘外部写入仍未实现激活、未启动、未调用。

## 审查范围与方法

- 范围：`57df305..1d8ab51` 与本轮 Task 10 验收差异；重点复核订单事实来源、审批/CAS/幂等、单 attempt、未知结果不重发、账户锁、Webhook 权威、对账解锁、队列隔离、Gateway 路由、BFF 输入、数据库降级和默认配置。
- 方法：实现完成后以提交差异、精确反例和新鲜回归做一次与编码步骤分离的二次审查；本轮没有启用子 Agent，不把结果描述为独立第二审查者背书。
- 边界：没有读取或调用复星凭据、账户、Provider SDK 或外部写接口；没有真实/模拟外部订单、Webhook 注册、部署或实盘能力变更。

## 已修复发现

| 级别 | 发现 | 修复与反例 |
| --- | --- | --- |
| Important | 新增 `0024` 后，旧迁移测试仍把 `0023` 当作最终 head，通用 schema 断言也未识别 11 张 broker-simulation 表；额外全目录诊断在 154 项通过后暴露 4 个失败。 | 更新所有“当前最终 head”断言，并把 11 张表作为同一可选迁移组；9 个直接受影响用例和全部 38 个迁移测试通过，历史迁移文件未修改。 |
| Minor | 新稳定错误码 `SIMULATION_WRITE_DISABLED` 未同步到完整枚举测试。 | 保留失败反例后更新稳定客户端分支集合；最终 1,016 项非集成回归通过。 |
| Minor | Broker workbench 在 tablet/mobile-landscape 与 200% 等效宽度下出现横向溢出。 | 收紧证据表格与执行链布局；6 档视口、200% reflow、键盘和 Axe E2E 均通过。 |

当前未解决发现：Critical `0`、Important `0`、Minor `0`。

## 核心安全证据

| 门禁 | 结果 |
| --- | --- |
| Phase 3 Engine/PostgreSQL | `65 passed in 76.53s`；覆盖 `0023 -> 0024 -> 0023 -> 0024`、有事实降级拒绝、约束、并发批准、串行发送、失败注入、未知结果、重启与对账。 |
| 全部迁移 | `38 passed in 80.81s`；Alembic 唯一 head 为 `0024_broker_simulation`，临时测试库残留 0。 |
| Gateway offline | socket guard 下 `157 passed in 1.62s`；外部 socket attempt 0。只读与 simulation composition root 不相交。 |
| 路由面 | 只读 Gateway 固定 7 个读取 operation；simulation Gateway 固定 `capabilities/create-order/modify-order/cancel-order/orders/portfolio/cash-flows` 7 个 operation，拒绝通用 URL/method/headers。 |
| HTTP 激活 | 未激活 admission 时 `HttpSimulationGatewayClient` 在构造 `httpx.AsyncClient` 前失败；外部 HTTP client construction 0。 |
| Worker/Agent 隔离 | broker job 全部 `RetryPolicy.NEVER / max_attempts=1`；Agent 与 research 模块对 broker-simulation/simulation-gateway import 命中 0。 |
| Web/BFF | 45 个文件、476 个单测及 85 个浏览器 E2E 通过；浏览器 body 不能提交账户、代码、价格、数量、Provider、URL、method 或 headers；`live-approvals` 仍锁定。 |
| Compose/密钥 | 5 个 Phase 3 policy 测试通过；app profile 无 simulation 服务/配置，静态 simulation profile 使用独立命名域且不能激活；tracked credential-shaped finding 0。 |
| 残留 | `nexus_test_%` 0；任务 Compose 容器与网络 0；没有清理其他 Compose project 或 volume。 |

## 产品与架构结论

- Agent 只能产出研究/策略意图；订单明细由服务器从已冻结提案、持仓、行情、白名单和风控事实生成。浏览器只能提交标识符、版本、sealed hash、幂等键与固定 reason。
- 批次批准不会触发外部 dispatch。离线 worker 只能通过注入式无 socket fixture 执行；普通 Web 页面始终展示“实盘关闭”和 `external_activation_blocked`，不呈现可用的 dispatch/modify/cancel 控件。
- 每个 create/modify/cancel 命令最多一个持久化 attempt；一旦结果不确定，状态只能进入 `result_unknown`/reconciling，锁定账户并主动读取对账，不能回到 sending。
- Webhook 仅是非权威提示；只有完整主动读取的 orders/portfolio/cash-flows 与冻结 expected hash 全匹配才可解锁。冲突和人工 acknowledge 不能绕过 match。

## Phase 3B 停止条件与回滚

Phase 3B 仍缺少并且必须另行冻结：Phase 1 连续五个美股交易日的真实只读证据、复星模拟账户隔离证明、精确写 SDK/契约/test vector、幂等和 Webhook 语义、独立 HMAC/mTLS/Provider 身份、生产 simulation adapter、追加式 admission，以及一次明确到“账户、UTC 窗口、订单数、notional、风控上限、允许 operation”的用户批准。

在这些条件齐备前，不启动 simulation profile、不加载/旋转凭据、不注册 Webhook、不发送 create/modify/cancel。回滚使用普通 `git revert` 或前向修复；存在 Phase 3 事实时不得破坏性 downgrade。任何未来真实订单仍必须逐笔人工确认。
