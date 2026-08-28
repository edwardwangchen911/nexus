# 复星模拟盘 Phase 3B 激活手册

## 当前结论与停止边界

当前唯一可报告状态是 `Phase 3A offline_ready / Phase 3B external_activation_blocked`。
本手册是未来激活检查表，不是对复星账户、模拟写入、Webhook 注册、凭据加载或部署的授权。

仓库中的 `broker-simulation` Compose profile 只用于静态组合审查：

- 使用独立的 `fosun-simulation-gateway` 和 `broker-simulation-worker` 服务名；
- 宿主机配置仅接受 `FOSUN_SIMULATION_GATEWAY_*` 与 `NEXUS_SIMULATION_EXTERNAL_ID_*` 命名域；
- Gateway 只编译无 socket fixture 组合根，不映射 Provider API Key 或 Provider 私钥；
- Broker Worker 固定为 `offline_fixture`，又故意没有可从 Compose 注入的进程内 fixture client，因此启动会 fail closed；
- 读取 Gateway 与模拟 Gateway 不共享 HMAC、mTLS 身份或会话缓存。

只允许做静态校验，不启动该 profile：

```bash
docker compose --env-file infra/env.example -f infra/compose.yaml \
  --profile broker-simulation config --quiet
```

## 门禁 A：Phase 1 真实只读稳定证据

以下条件必须同时有可验证证据，缺少任一项立即停止：

1. 对话中曾暴露的旧 Key 已吊销；旋转后的只读凭据仅在批准的 secret manager 中，不出现在 Git、聊天、命令行、日志或测试快照。
2. `docs/runbooks/fosun-sdk-admission.md` 的书面许可、安全审查、官方 test vector、TLS 验证、精确 HTTPS base URL 和固定 SDK subject 全部通过。
3. SDK 与契约标识被冻结为可重算的 tag/commit、manifest SHA-256 和读取路由集，不允许漂移或通用代理。
4. 按 `docs/runbooks/fosun-read-only-activation.md` 完成一次单独批准、最多 24 次请求的只读探测，并生成不含账户或交易数据的脱敏 artifact。
5. 连续五个完整美股交易日都完成账户、现金、持仓、历史订单和流水的只读同步；每日记录 UTC 窗口、artifact hash、数据时点、页数、结果计数和安全结果码。
6. 五日内没有未解决的 `RECONCILIATION_REQUIRED`、能力过期、契约漂移、TLS 漂移、身份错配或无界重试；故障和限流演练均能在网络尝试前或已知安全边界内 fail closed。

本地预检结果 `fixture_with_next_action / COMPLETE_SDK_ADMISSION` 不是五日证据，也不能提升任何能力。

## 门禁 B：写契约与账户隔离

在请求 Phase 3B 批准前，必须把以下证据冻结到新的 simulation admission package：

1. Provider 可验证的“模拟账户”分类字段、枚举、错误语义以及它无法触达真实账户的权限证据。
2. 下单、改单、撤单、订单列表、成交、拒单的精确 SDK 方法、请求/响应 schema、整股与 tick 精度、时段、币种和状态转移。
3. Provider 级幂等键/client-order-id 的书面语义。如无法证明，必须保留“结果未知绝不重发，只全量对账”政策。
4. Webhook/orderUpdate 的认证方式、事件 ID、时间窗、重放和交付保证。证据不足时不注册 Webhook，仅用主动读取补偿。
5. 七个固定操作的脱敏 test vector、路由集、响应大小、超时和限流规则；禁止任意 URL、method、headers 和通用 SDK 调用。
6. 与只读 Gateway 完全不同的 HMAC current/previous identity、mTLS client/server identity、Provider 凭据、进程和 session cache。
7. 生产 simulation adapter 已实现并通过安全审查；当前 fixture-only composition root 不能被环境变量直接升级。
8. 数据库中有追加式 `SIMULATION_CONTRACT_ADMITTED` 记录，其 subject、artifact hash、有效窗口与启动时重算结果完全一致。

## 门禁 C：单次有界联调批准

门禁 A 和 B 通过后，向用户提交一份完整、可一次性判断的批准请求。请求必须同时写明：

- 目标只能是一个模拟账户的脱敏标签与内部 binding ID；
- 冻结的 SDK commit/tag、读取契约 hash、模拟写契约 hash、admission artifact hash 和将运行的代码 commit；
- 精确 UTC 开始/结束窗口，不允许自动延长；
- 允许的操作集：能力读取、有界订单/持仓/流水读取，以及最多一个人工确认批次的 create；modify/cancel 默认不在首次窗口内；
- 批次的最大订单数、最大总 notional、单标的/行业/现金比例上限。这些必须是用户在该次批准中看到并明确同意的数字，不从代码默认、当前持仓或 Agent 推断；
- 是否允许注册 Webhook；未明确写“允许”就视为禁止；
- 执行人、观察人、回滚负责人与审计 artifact 位置；
- 停止条件和回滚顺序。

用户批准窗口后，仍需对将发送的每一个具体批次做页面内人工确认。窗口批准不等于订单批准。

## 强制停止条件

出现以下任一情况时，停止领取新 job，不自动重发、改单、撤单或发送“补偿订单”：

- 账户类型、权限、SDK/manifest/contract hash、TLS/HMAC 身份或 capability 窗口与批准不符；
- 报价、组合快照、风控、审批或对账证据缺失、过期、部分或冲突；
- 任一 create/modify/cancel 返回结果未知，或 Webhook 与主动读取冲突；
- 订单数、notional、现金、单标的或行业上限将被突破；
- Provider 返回未知字段/枚举、重定向、TLS 失败、认证异常、权限扩张、限流异常或非模拟账户证据；
- 审计落库失败、数据库版本不匹配、worker lease 或唯一 attempt 约束无法证明。

## 回滚与恢复

1. 撤销数据库中的 simulation capability 并停止模拟 Gateway/Broker Worker；不删除订单、attempt、回执或对账审计链。
2. 在 secret manager 中吊销/旋转该模拟进程专用的 Provider、HMAC 和 mTLS 凭据，不改动只读 Gateway 凭据。
3. 对任何结果未知的命令只执行有界全量读取对账；无法唯一确认时保持账户锁定并人工处理。
4. 恢复 NEXUS 至 app fixture 配置，验证 `offline_ready / external_activation_blocked`、readiness、单一迁移 head 和审计可读。
5. 代码回滚使用普通 `git revert` 或前向修复，不改写 Git 历史；存在 Phase 3 事实时不执行破坏性数据库 downgrade。

## 当前不执行的动作

在本阶段结束时，不执行真实只读探测、不启动面向复星的 simulation Gateway、不加载/旋转凭据、不注册 Webhook，不发送 create/modify/cancel。Phase 3B 只能在上述证据齐备后，由用户对“精确窗口 + 精确上限 + 精确账户”再次明确授权。
