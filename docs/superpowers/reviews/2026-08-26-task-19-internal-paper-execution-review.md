# Task 19 内部模拟成交与绩效验收审查

**结论：** 通过本地验收。Task 19 在 `internal_paper` 美股现金账户范围内满足设计规格；审查发现的 Critical/Important/Minor 未解决项为 `0/0/0`。本次没有真实账户或券商写操作、真实订单、部署、密钥/权限变更或付费行为。

## 1. 审查范围与方法

- 规格、实现和纠正范围：`9fe4ba0^..6ae2592`。
- 审查对象：确定性成交、时间与价格权威、并发/幂等、预留与双重记账、持仓/绩效投影、Engine API、BFF、`/paper` 页面、迁移和回滚边界。
- 审查方式：实现完成后进行一次与编码步骤分离的单执行者对抗审查，逐项检查前视偏差、时点错配、重复事件、跨账户绑定、审批/能力绕过、响应泄密和账本不平。由于本次未启用委派，这不是第二位人员或第二模型审查。
- 实盘边界：执行服务没有 provider transport 或 broker capability，只读取 PostgreSQL 已冻结权威；API 只提供内部模拟盘读取与可取消订单的幂等取消。

## 2. 已修复发现

| 级别 | 发现 | 修复与反例 |
| --- | --- | --- |
| Important | 零容量窗口可在新幂等键下使用早于既有事件的 `evaluated_at`，造成执行时间倒退。 | `70d2204` 在应用与 `0017_paper_execution_time_order` 同时强制单调时间，并覆盖幂等重放例外。 |
| Important | `created_at > as_of` 的行业暴露可进入历史绩效归因，形成事后信息泄漏。 | `b4f838e` 限制只读取当时已创建的 exposure；迟到事实只能得到 `unclassified/PARTIAL`。 |
| Important | 可在较晚绩效快照后发布较早快照，破坏不可变峰值/回撤链。 | `f43038e` 在应用拒绝倒序发布，`0018_paper_performance_chain` 在数据库校验峰值和回撤链。 |
| Important | 直接 SQL 可插入未由账本、持仓、价格和归因根支持的绩效快照。 | `6ae2592` 增加 `0019_paper_performance_root` 延迟约束，逐标的对账并拒绝独立快照和额外零值伪造归因。 |
| Minor | API 在锁外预查取消幂等记录，并发重复取消可能都返回 `replayed=false`。 | `1d79bf3` 让事务锁内服务返回权威重放状态；竞争测试证明结果恰为一条首次执行和一条重放，且只有一个事件/幂等事实。 |

兼容性回归同时修复：`87dbd18` 补齐回测 session 边界，`be98383` 对齐成交元数据，`1f4507e` 补齐半日市 Shadow fixture，`5259014` 允许治理后的 1 分钟权威进入市场数据测试。它们不扩大 Task 19 产品能力。

## 3. 最终验证证据

| 门禁 | 最终结果 |
| --- | --- |
| Engine 全量 | `1313 passed in 1264.08s`；命令使用 `pytest --import-mode=importlib engine/tests -x -q` 和本地临时 PostgreSQL。 |
| Python 静态检查 | Ruff 全绿；Mypy `202 source files` 无问题。 |
| 绩效/执行/迁移定向 | 41 项通过；覆盖正常发布、迁移往返、独立伪造快照、额外伪造标的归因和迁移单头。 |
| Web 单测 | 36 个测试文件、385 项测试全部通过。 |
| Web 静态与构建 | ESLint、TypeScript、生成合约一致性和 vinext production build 全部退出 0。 |
| 浏览器 E2E | 31 项全部通过，覆盖桌面/移动 `/paper`、取消、PARTIAL/STALE/error、分页与跨页面回归。 |
| 可访问性与视觉 | `/paper` 桌面 1440 和移动 390 目视检查通过；Axe critical/serious 为 0；页面无整体横向溢出，宽表使用带标签的内部滚动。 |
| Compose 边界 | `app` 仅为 postgres/minio/worker/api；`broker` 才包含 fosun-gateway/broker-worker；两种 `config --quiet` 均退出 0。 |
| 密钥扫描 | 受控凭据/私钥模式命中文件数为 0。 |

构建仅报告已有的代理环境提示和 vinext 无法静态分类部分动态路由的提示；两者均未导致失败，也不改变运行时安全策略。

## 4. 架构验收结论

- 成交输入只来自订单既定 source/feed 的冻结 1 分钟权威；未来数据、缺失窗口、零容量、源切换和前收盘回退均 fail closed。
- 状态、预留、成交、持仓事件、双重记账、审计和 outbox 在同一事务内形成追加事实；数据库延迟约束在提交点验证完整根。
- 绩效只按收盘时点发布，现金来自账本，仓位来自调整事件，价格来自当时可见的完整日线权威；行业暴露保存时间可用性和完整性降级。
- Agent、前端和 broker 数据都不能覆盖成交模型、硬风控、账本或绩效事实；`real_read_only` 与 `fosun_simulation` 不具备 Task 19 执行能力。
- API/BFF 使用服务端 actor、HMAC、严格 UUID/请求头/响应 schema、签名游标、请求体上限和安全错误；浏览器不能直接调用 Engine 或接触签名密钥。

## 5. 回滚与后续边界

- 本次只有本地提交，没有发布或外部状态变更；停止本地 API/worker 即可停止 Task 19 行为。
- Alembic `0019 → 0014` 的 downgrade 会移除 Task 19 权威表/列，不应用于已有需保留的模拟成交事实。若将来部署后需要回滚，应先备份并校验账本/事实哈希，优先采用前向纠正迁移。
- Task 20 才负责定时任务、lease、恢复、ready health 和安全审计导出。Task 19 不包含常驻调度器。
- 真实复星账户、复星模拟盘/实盘写入、任何真实订单、凭据接入和部署仍保持关闭并需要单独确认。
