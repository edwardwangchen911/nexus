# NEXUS Phase 3 复星模拟盘与真实只读运行化设计

**状态：** 已完成设计自审与可编码实施 Plan
**基线：** `main@c6e3ff1`  
**范围：** 本地可重复运行、真实美股只读数据 transport、Phase 3A 离线券商模拟写闭环，以及未来单独批准的 Phase 1/Phase 3B 激活门禁  
**明确排除：** Phase 4 实盘、自动券商下单、Agent 交易权限、真实凭据入库、未经批准的 Provider 请求、部署

## 1. 目标与完成定义

本阶段把已经验收的离线研究、回测、内部模拟盘和策略进化能力推进到两个可验证的终点：

1. `main` 可以在纯 fixture 配置下重复启动 PostgreSQL、MinIO、Engine API、通用 Worker 和 Web，且 readiness、迁移、对象存储与页面冒烟测试通过。
2. Alpaca、SEC、FRED/ALFRED 具备生产可用但默认不激活的窄 HTTP transport 与组合根，所有真实读取都受来源、主机、路径、TLS、重定向、限流、时点、归档和质量策略约束。
3. Phase 3A 在不连接复星的条件下实现完整的模拟券商执行闭环：确定性执行意图、批次审批、账户类型绑定、风险复验、下单/改单/撤单命令、未知结果、Webhook 幂等、主动对账、审计、API 和 Web。
4. `SIMULATION_WRITE` 与 `LIVE_WRITE` 在仓库默认配置、fixture 运行和普通 app profile 中持续关闭；Phase 3A 的任何测试都不能产生外部请求。
5. Phase 1 真实只读和 Phase 3B 模拟账户写联调只有在外部门禁齐备并由用户明确批准后才可执行。Phase 4 不属于本目标。

“代码存在”“页面解锁”或“fixture 测试通过”均不等于 Phase 3B 已激活。Phase 3A 完成时，产品必须明确显示 `offline_ready / external_activation_blocked`。

## 2. 已有基础与缺口

### 2.1 可复用基础

- Phase 0–1 已有来源权威、时点数据、复星只读防腐层、组合快照、对账、硬风控、优化方案和审计。
- Task 18/19 已有内部模拟订单、事件链、预留、确定性成交、双重记账、幂等、绩效和 `/paper` 工作台。
- Task 20/21 已有 lease、outbox、重试、readiness、备份恢复、故障门禁和独立 broker profile。
- Phase 2 已有不可变策略候选、OOS、Shadow、晋级、回滚、CAS 和严格的浏览器写入边界。
- 复星 Gateway 已有 mTLS、HMAC、只读操作 allowlist、fixture adapter、SDK admission 与 sanitized activation artifact。

### 2.2 当前缺口

- 主工作树没有本地运行配置，旧 Phase 1 Compose 只运行 PostgreSQL/MinIO；API、Worker 和 Web 未常驻。
- Alpaca/SEC/FRED 已有领域 adapter，但生产 HTTP transport 和 composition wiring 尚未闭合。
- Phase 1 真实复星只读仍为 `fixture_with_next_action / COMPLETE_SDK_ADMISSION`。
- `/broker-simulation` 仍是锁定页；没有 Phase 3 数据模型、迁移、执行服务、Gateway 写 surface 或实施 Plan。
- 复星公开资料未证明模拟/实盘账户选择字段、券商级幂等键和 Webhook 消息认证。缺失证据必须阻止 Phase 3B，而不是由 NEXUS 猜测。

## 3. 交付分解与顺序

本目标分为四个串行门禁，避免把外部阻塞混入本地代码完成度：

### 3.1 Workstream A：本地运行化

在新 Phase 3 工作树中建立可重复的 fixture 启停与 doctor 流程。普通 app profile 只包含 PostgreSQL、MinIO、object-store-init、API 和通用 Worker；Web 由本地 Node 进程启动。broker profile 只做静态配置校验，不自动启动。

### 3.2 Workstream B：真实美股只读 transport

实现仅能访问固定官方主机和固定路径族的 Alpaca、SEC、FRED/ALFRED transport。真实 transport 默认无配置且 fail closed；fixture 模式永远不读真实密钥、不访问网络，也不静默退化为别的来源。

### 3.3 Workstream C：Phase 3A 离线闭环

使用脱敏、固定日期的复星模拟券商 fixture 实现完整状态机、数据库约束、执行服务、Gateway surface、Broker Worker、API、Web、Webhook 与轮询对账。所有外部 transport 使用 socket guard 禁止网络。

### 3.4 Workstream D：外部激活

先完成 Phase 1 真实只读 admission 与连续稳定证据，再冻结 Phase 3 写契约并请求一次明确的 Phase 3B 联调批准。此 Workstream 不因 A–C 完成而自动开始。

## 4. 方案选择

### 4.1 选择：独立 simulation gateway 组合根

采用同一 `nexus-fosun-gateway` 包、不同 composition root 与独立容器/密钥域：

- `read_only` 进程继续只编译七个已审查只读操作。
- `simulation_write` 进程只在 broker profile 和 admission artifact 同时允许时编译模拟账户读写 surface。
- Engine API、研究 Worker、Strategy R&D Worker 和浏览器永远不持有 Provider 凭据。
- Broker Worker 只能通过 mTLS + HMAC 调用 simulation gateway，不导入 SDK。

不选择在同一常驻只读进程上用普通布尔开关动态增加写路由。进程级隔离能让镜像、路由表、密钥挂载和网络策略成为独立证据，减少配置漂移导致的权限升级。

### 4.2 不采用的方案

- **直接复用内部模拟订单作为券商订单：** 会把回测/内部成交事实误当成外部授权，破坏账户与账本隔离。
- **Agent 生成并提交订单：** Agent 只提供研究假设，不能获得 execution intent、账户或 Gateway 工具。
- **Webhook 作为唯一权威：** 当前公开资料只表明 `orderUpdate`，未证明认证与完整覆盖；主动全量轮询必须是最终补偿机制。
- **未知结果自动重试下单：** 当前公开示例未证明券商级幂等键；网络未知时重试可能产生重复订单。

## 5. Capability 与激活状态机

能力按严格偏序单向提升：

```text
OFFLINE_FIXTURE
  -> READ_ONLY_ADMITTED
  -> READ_ONLY_ACTIVATED
  -> SIMULATION_CONTRACT_ADMITTED
  -> SIMULATION_WRITE_ACTIVATED
```

`LIVE_WRITE` 不在该状态机中。任何状态都不能由单一环境变量提升；至少同时需要：

- 版本化 admission manifest；
- 与 manifest subject 完全匹配的 sanitized artifact；
- 数据库内追加式 activation record；
- 部署 profile 与独立密钥域；
- 未过期的 capability window；
- 启动时与每次 Provider attempt 前的重算校验。

普通开发、测试和 app profile 的最大能力是 `OFFLINE_FIXTURE`。Phase 3A 页面显示离线证据，但所有发送按钮不可用。

## 6. Phase 1 与 Phase 3B 前置门禁

### 6.1 Phase 1 只读

必须满足现有 SDK admission runbook，并额外提供至少五个连续美股交易日的稳定证据：

- 每日账户/现金/持仓/订单/流水全域同步完成；
- 无未解决 `RECONCILIATION_REQUIRED`；
- capability、TLS、manifest、SDK 和 base URL 未漂移；
- 过期、限流和 Provider 故障都能 fail closed；
- 不使用此前在对话中暴露的旧 API Key，且已有吊销/轮换证据。

### 6.2 Phase 3 写契约

真实联调前必须冻结并审查：

- 模拟账户与真实账户的可验证分类字段；
- 下单、改单、撤单、订单查询、成交与拒单的完整字段/枚举/精度；
- 是否存在 Provider idempotency/client-order-id；
- HTTP 超时后如何唯一对账；
- `orderUpdate` Webhook 的认证材料、事件 ID、时间字段、重放语义和交付保证；
- 模拟账户权限不能触及真实账户的证据；
- 官方测试环境、脱敏 test vector 和最小权限说明。

若任何一项无法证明，Phase 3A 仍可完成，但 Phase 3B 保持 `external_activation_blocked`。

## 7. 数据与权限隔离

### 7.1 账户绑定

浏览器和 Engine 只使用内部 `broker_account_binding_id` 与脱敏标签。真实 `sub_account_id` 只存在于 simulation gateway 的受控映射/secret domain；不得写入浏览器响应、日志、审计文本或测试快照。

绑定记录保存：

- provider、environment、account type；
- 外部 ID 的不可逆 keyed digest；
- read-only snapshot identity/hash；
- capability subject 与有效期；
- 最后一次全量对账状态。

只有 `provider=fosun`、`environment=simulation`、`account_type=simulation` 三者均由冻结契约证明时才可准备订单。任一缺失或冲突都 fail closed。

### 7.2 密钥域

- Web/BFF：只有 Engine 服务 HMAC，不接触 Provider。
- Engine/Broker Worker：只有 Gateway mTLS 客户端身份和 Gateway HMAC，不接触 API Key 或 Provider 私钥。
- Simulation Gateway：独立挂载轮换后的 Provider API Key、客户端私钥、服务端公钥与 TLS trust。
- Read-only 与 Simulation Gateway 不复用 HMAC identity、mTLS identity 或进程内 session cache。

## 8. 执行意图与审批

### 8.1 独立权威

Phase 3 不复用 `OptimizationProposalGroup.status`、内部 `PaperOrderBatch` 或 Phase 2 registry event 表达交易授权。新增不可变 `BrokerExecutionIntent`，绑定：

- 选中的 proposal/group 与 strategy decision；
- 组合快照、市场数据、财务/宏观证据和来源哈希；
- 目标组合、当前模拟券商组合与确定性差异；
- 风控政策、执行模型和账户绑定；
- 规范订单集合与总 notional；
- 准备时间、审批截止时间和版本。

Intent 状态：

```text
draft -> prepared -> approved -> dispatching
draft|prepared -> rejected|expired
approved -> expired|dispatching
dispatching -> completed|partially_completed|failed|submission_unknown|reconciling
submission_unknown -> reconciled|reconciling
reconciling -> completed|partially_completed|failed
```

所有转换由追加事件和数据库约束验证；不更新或删除历史授权事实。

### 8.2 批次审批

审批只能来自已认证的单一用户，通过 BFF 固定命令完成。批准请求必须包含 intent ID、expected version、规范 body hash、ETag 与 Idempotency-Key。服务端重新读取全部权威事实，浏览器不能提交价格、数量、账户 ID、风险参数或订单列表。

审批有效期取以下最早值：

- intent 准备后五分钟；
- 任一报价/组合快照/capability 的失效时间；
- 当日常规交易时段边界。

数据变化、对账状态变化、账户绑定变化、订单集合变化或风险策略变化都会使审批失效，必须重新准备和人工确认。

## 9. 订单范围与硬风控

Phase 3 只允许：

- 美股普通股票和主流非杠杆 ETF；
- USD 现金账户、long-only；
- `DAY` 限价单；
- 正常交易时段；
- 整股、正数量、合法 tick；
- 白名单标的和确定性风控已通过的订单。

禁止：市价单、条件单、期权、碎股、盘前盘后、夜盘、卖空、融资、跨币种、通用 method/URL proxy、浏览器自由填写订单和 Agent 订单工具。

发送前 Broker Worker 必须重新验证：账户绑定、能力、审批有效期、最新只读全量快照、未解决对账差异、现金/持仓、白名单、单标的/行业/现金/回撤限制和 batch hash。复验失败不会调用 Gateway。

## 10. 外部命令与未知结果

每个订单按规范排序串行发送。第一条非明确成功结果后停止发送剩余订单；已经明确成功的订单不自动撤销，未发送订单保持可审计的 `not_sent`。

命令状态：

```text
prepared -> sending -> acknowledged
sending -> rejected|transport_failed|result_unknown
acknowledged -> working|partially_filled|filled|cancelled|rejected|expired
result_unknown -> reconciled|reconciliation_required
```

内部 Idempotency-Key 只能防止 NEXUS 重复 dispatch，不能伪造 Provider 幂等。若冻结契约没有可验证的 Provider idempotency/client-order-id：

- 超时或连接中断后绝不自动重发 create/modify/cancel；
- 将命令标记为 `result_unknown`；
- 使用 OrderList、Holdings、CashSummary 和 CashFlows 在有界窗口内主动对账；
- 无法唯一匹配时进入 `RECONCILIATION_REQUIRED` 并锁定该账户的新命令。

改单属于新的物质性订单意图，必须重新计算差异、重新风控并人工确认。撤单必须由用户显式点击；系统不以“补偿”为由自动发送外部撤单。

## 11. Webhook 与主动对账

### 11.1 Webhook

Phase 3A 实现禁用状态的 receiver、严格 schema、body 上限、认证 adapter、事件 ID 幂等、原始字节哈希、时间窗和乱序缓冲测试。只有冻结契约证明认证方式后才能把对应 adapter 标为 admitted。

Webhook 收到后只追加 receipt 与候选订单事件；它不能直接改变账本、解除账户锁或授权新订单。无签名/弱认证只能作为提示，必须由主动读取确认。

### 11.2 主动轮询

Broker Worker 对 dispatch 中、未知和 working 订单执行有界轮询，并定期进行账户/现金/持仓/订单/流水全量对账。轮询结果与 Webhook 冲突时：

- 保存两份来源事实；
- 状态进入 `reconciling`；
- 禁止新命令；
- 只有冻结冲突政策与完整全量快照才能恢复。

内部模拟账本和复星模拟账户永远是两个组合。系统只做差异比较，不把券商成交回写成内部模拟成交，也不宣称两者必然一致。

## 12. 持久化与迁移

从当前单一 head `0023_evolution_authority` 新增向前迁移 `0024_broker_simulation`，不修改历史迁移。最小表集：

1. `broker_account_bindings`：脱敏账户绑定、类型、能力 subject 与对账状态。
2. `broker_execution_intents`：不可变 intent 根和规范 hash。
3. `broker_execution_intent_orders`：服务端生成的订单集合。
4. `broker_batch_approvals`：审批 actor、版本、ETag、body hash、过期时间。
5. `broker_commands`：create/modify/cancel 的规范命令根。
6. `broker_command_attempts`：每次唯一外部 attempt 与 safe outcome。
7. `broker_order_events`：只追加 Provider/轮询/Webhook 状态链。
8. `broker_webhook_receipts`：原始字节 hash、认证结果、事件 ID 和去重事实，不保存敏感 body。
9. `broker_reconciliation_runs` 与 `broker_reconciliation_diffs`：全量对账与锁定/恢复证据。
10. `broker_simulation_admissions`：写契约 subject、artifact、能力窗口与撤销记录。

数据库延迟约束验证：账户类型、intent/order/approval hash、事件序列、命令与 attempt 一对多边界、同一外部订单的唯一绑定、未知结果不可重发、对账锁期间无新发送、终态不可逆和 live capability 永远不存在。

存在任一 Phase 3 事实时 downgrade 必须以固定 SQLSTATE fail closed；空事实库允许 `0024 -> 0023 -> 0024` 往返。

## 13. Gateway 与 Broker Worker

### 13.1 Phase 3A fixture Gateway

新增固定操作：

- `fosun_simulation_capabilities`
- `fosun_simulation_create_order`
- `fosun_simulation_modify_order`
- `fosun_simulation_cancel_order`
- `fosun_simulation_list_orders`
- `fosun_simulation_sync_portfolio`
- `fosun_simulation_sync_cash_flows`

操作名与 request schema 固定，不能接受 method、URL、任意 endpoint、任意 headers 或额外字段。fixture 覆盖成功、拒单、部分成功、超时前、发送后未知、分页、重复/乱序 Webhook 和对账冲突。

### 13.2 生产 adapter

生产 adapter 只在 `SIMULATION_CONTRACT_ADMITTED` 后进入构建；从冻结 SDK ref 调用精确方法。任何签名、枚举、字段、TLS surface 或 SDK commit 漂移都在网络前拒绝。日志只允许 operation、attempt、safe outcome、elapsed、内部 correlation；禁止 body、headers、URL、外部账户/订单 ID 和异常原文。

### 13.3 Worker

独立 `broker` queue 与 worker 只领取 broker simulation jobs。它不消费 research、paper 或 strategy_rnd queue；不构造 Agent。Worker 在领取、发送前、每次等待后和落库前重验 capability 与 expiry。

## 14. 真实美股只读 transport

### 14.1 网络策略

每个 transport 使用独立 `httpx.AsyncClient`，强制：

- TLS 验证开启，禁止重定向；
- 固定 scheme/host/port 与代码内路径模板；
- 禁止调用方提供 URL、host、headers 或 query 字符串；
- 绝对 deadline、有界连接池和响应体上限；
- 只对安全 GET 和明确可重试状态执行有界退避；
- 429 尊重有界 `Retry-After`；
- 日志不含凭据、完整 URL、响应 body 或用户数据。

### 14.2 来源

- Alpaca：美股报价与历史 bar，delivery mode 必须来自配置/响应契约，不能从 feed 名推测；Basic/IEX 与 delayed SIP 明确区分。
- SEC：`data.sec.gov` 固定路径，统一可联系 User-Agent，进程级不超过 5 req/s，保存原始文档和 acceptance-time 权威。
- FRED/ALFRED：固定八个 series，保存 vintage 与保守 available_at；API key 仅在 transport secret 域。

生产 composition 缺少必要配置时启动失败，不回退 fixture。fixture composition 使用完全不同的 transport，socket guard 证明无外网。

## 15. Agent 运行边界

Agent 保持现有两种显式模式：

- `fixture`：默认、本地可复现、不需要 OpenAI 配置。
- `responses`：只有明确配置 API key、提取模型、推理模型、预算和 allowed tools 才能启动。

无论模式如何，Agent 都不能读取账户绑定、execution intent、approval、broker command 或 Gateway 工具。Agent 输出不能改变订单数量、限价、账户、风控或 capability。模型费用和密钥配置属于单独审批项，不是 Phase 3A 验收前提。

## 16. API 与 BFF

Engine 内部 API：

- `GET /internal/v1/broker-simulation`
- `POST /internal/v1/broker-simulation/intents`
- `POST /internal/v1/broker-simulation/intents/{intent_id}/approve`
- `POST /internal/v1/broker-simulation/intents/{intent_id}/reject`
- `POST /internal/v1/broker-simulation/orders/{order_id}/modify`
- `POST /internal/v1/broker-simulation/orders/{order_id}/cancel`
- `POST /internal/v1/broker-simulation/reconciliation/{run_id}/acknowledge`

浏览器 BFF 使用对应 `/api/v1/broker-simulation/**` 路由，只接受固定空 body 或固定枚举/expected version；请求体不接受账户、订单参数、价格、数量和 Provider ID。所有写入要求同源、HMAC、Idempotency-Key、If-Match/expected version、64 KiB 上限和安全错误 envelope。

当 capability 未激活时，读 API仍可返回 Phase 3A fixture/离线验收事实，但所有外部命令返回固定 `SIMULATION_WRITE_DISABLED`，不得仅依赖前端隐藏按钮。

## 17. `/broker-simulation` 页面

页面从锁定占位升级为离线可审查工作台，但顶部永久显示：

- 当前能力：`offline_ready`、`read_only_activated` 或 `simulation_write_activated`；
- “实盘关闭”；
- 数据模式、来源、as-of、对账状态与 capability expiry。

信息顺序：

1. 能力与账户绑定摘要（仅脱敏标签）。
2. 选中方案与当前复星模拟组合差异。
3. 硬风控和数据证据清单。
4. 服务端生成的订单批次预览。
5. 审批/过期/发送状态。
6. 订单与只追加时间线。
7. 内部模拟盘 vs 复星模拟盘差异与对账。
8. Webhook/轮询健康和审计入口。

`offline_ready` 状态只允许查看 fixture 证据，不显示可用的发送/改单/撤单按钮。外部能力激活后，批准按钮仍需要可访问确认对话框；网络未知时页面锁定其他写操作，只允许刷新权威状态，不能用新幂等键重复发送。

页面覆盖 loading/empty/ready/partial/stale/error/forbidden/reconciling/unknown。键盘、焦点、aria-live、窄屏表格、200% reflow 和 Axe critical/serious=0 继续作为发布门禁。

## 18. 错误处理与恢复

- 数据缺失、过期、来源冲突、对账未完成、能力过期：不创建或不发送 intent。
- 明确 Provider 拒单：记录安全错误码，停止剩余发送，不自动修改或补单。
- 外部结果未知：不重发，进入主动对账；无法唯一确认则锁定账户。
- Webhook 失败：不影响主动轮询，记录 health 告警。
- 数据库提交失败：outbox/attempt 原子边界保证不会把未落库命令当成功；若外部已可能接受则按未知结果处理。
- Worker 重启：只恢复未终结 command；任何可能已出站的 attempt 都先对账，绝不直接重发。
- capability 撤销或过期：停止领取与发送，保留只读状态和审计。

## 19. 测试与验收

### 19.1 领域与属性测试

- intent/approval/command/order/reconciliation 状态机全转换与非法反例；
- 账户类型、币种、市场、产品、数量、tick、notional 和风险边界；
- 任意重试/重复/乱序下至多一个外部 attempt；
- 未知结果永不创建第二次发送；
- hash、ETag、版本、过期和 capability 漂移均 fail closed。

### 19.2 PostgreSQL

- `0023 -> 0024 -> 0023 -> 0024` 空库往返；
- 有 Phase 3 事实时拒绝 downgrade；
- 延迟约束、并发审批、dispatch lease、取消/成交竞争、Webhook/轮询冲突；
- 故障注入覆盖 outbox、attempt、事件与对账原子性；
- 测试数据库残留为 0。

### 19.3 Gateway 与安全

- offline socket guard 下完整 fixture suite；
- 路由表精确等于 admitted operation set；
- `READ_ONLY` 镜像没有写路由，普通 app profile 没有 simulation gateway/broker worker；
- 生产 adapter 在 admission 缺失、SDK 漂移、TLS surface 漂移和 capability 过期时均在网络前失败；
- secret/private-key/API-key/header/body/URL 日志扫描为 0。

### 19.4 Web

- Engine schema/OpenAPI/generated client/BFF 严格一致；
- 所有页面状态、审批过期、409、未知结果、对账锁、键盘和可访问性；
- 浏览器不能提交任何订单参数或外部 ID；
- Phase 3A E2E 证明写按钮不可用且 API 同样 fail closed。

### 19.5 全量门禁

- Engine/Gateway 全量 Pytest、Ruff、Mypy；
- Alembic single head 与迁移 round trip；
- Web generated check、Vitest、TypeScript、ESLint、production build、Playwright；
- Compose app/broker 静态组合、fixture 启停、readiness、对象存储与残留检查；
- 独立代码审查与安全审查无未解决 Critical/Important。

## 20. Phase 3B 有界联调

只有外部门禁完成后才向用户请求一次明确批准，批准内容必须列出：目标模拟账户脱敏标签、SDK/contract hash、时间窗口、允许操作、最大订单数、最大总 notional、停止条件、回滚负责人和是否允许 Webhook 注册。

首次联调默认只允许一个人工确认批次；任何异常立即停止，不扩大请求预算。联调成功也只证明复星模拟账户链路，不开启实盘、不改变 Phase 4 门禁。

## 21. 回滚与清理

- 本地：停止 Phase 3 工作树进程和精确 Compose project；只删除本任务创建且标签匹配的容器、网络和可重建 volume。
- 代码：普通 `git revert` 或前向修复，不改写历史。
- 数据库：有事实后不执行破坏性 downgrade，先备份并保留审计链。
- 外部：撤销 simulation capability、停止 Gateway/Broker Worker、吊销相应密钥、保留脱敏审计；不自动发送补偿订单。

## 22. 官方证据快照与限制

2026-08-28 刷新的公开证据：

- 复星 OpenAPI 文档说明账户、交易、行情、IP 白名单、权限和独立行情授权；这只证明公开能力，不证明本账户已授权。
- `openapi-python-sdk`：`main=7f0fb48f3e1ce04329b990cf16f391cf59078137`；当前 NEXUS admission 候选仍固定 `v1.2.0=891d95fb4967efaa682387c1b35966ceea38fb21`，不得静默升级。
- SDK README/example 公开 `create_order`、`order_modify`、`cancel_order`、OrderList、CashFlows 和 `orderUpdate` subscription；公开示例没有形成 NEXUS 所需的模拟账户分类、Provider 幂等与 Webhook 认证证据。
- `fw-trading`：`main=157c52fda8387c2ae658bb2010060df75d269f07`，仅作为交互与 skill 参考，不是运行时、凭据管理器或交易授权层。

因此 Phase 3A 使用严格 fixture 和契约占位，不把公开示例的缺失字段自行补成生产事实。

## 23. 设计自审

- **占位符：** 无 `TODO`、`TBD` 或待用户选择的隐式参数；所有外部未知均转换为明确激活门禁。
- **一致性：** Phase 3A 可完整离线验收，同时 `SIMULATION_WRITE` 默认关闭；Phase 3B 不会因本地代码完成而自动开启。
- **范围：** Phase 4、实盘、自动交易、部署、付费和真实密钥明确排除。
- **权限：** Agent、浏览器、API、Broker Worker 与 Gateway 的权限和密钥域分离。
- **失败安全：** 数据/能力/账户/契约/对账/网络未知任一不合格都阻止新命令；未知结果不重发。
- **可实施性：** 迁移、领域模型、服务、Gateway、Worker、API、Web 和测试均有明确文件级落点，可进入详细 Plan。
