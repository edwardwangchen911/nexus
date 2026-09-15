# Bark 只读通知设计

## 目标

NEXUS 在关键 Agent 内部模拟事实已经提交到 PostgreSQL 后，经自托管 Bark Server 向用户的
iPhone 发送脱敏、端到端加密的只读通知。通知投递不参与业务事务，不改变任何业务状态，不触发
行情、模型、模拟盘或实盘写入，也永远不解除 `execution_locked=true`。

## 既有事实与选择

仓库已有通用 `outbox_events`、`outbox_delivery_attempts`、短租约领取、有限重试、退避和死信；
Paper 服务已在业务事务内写入真实事件。因此不新增依赖、迁移、表或调度框架。

采用独立 Bark worker。它是现有 Paper outbox 在第一阶段的唯一生产消费者，只领取明确登记的
事件。没有采用通用 worker 内嵌发送，因为那会扩大其密钥和网络权限；没有采用 API/业务请求内
同步发送，因为那会把外部通知故障耦合回业务提交。

## 组件与数据流

1. 现有 Paper 服务在同一数据库事务内提交业务事实、审计事件和 outbox event。
2. 独立 Bark worker 只领取 `created_at >= activated_at` 且事件类型/版本在白名单中的 queued event。
3. 纯函数把事件映射为固定中文标题和摘要，只使用事件种类，不复制账户、持仓、金额、证据正文、
   哈希、密钥或可执行 token。当前真实事件覆盖：决策已持久化、批次已评估、风控拒绝、订单已提交、
   部分成交、成交和取消。仓库没有独立的“订单失败” outbox event，第一版不虚构该状态。
4. publisher 在加密前加入稳定的 `id=<outbox event UUID>`。Bark v1.5.2+/server v2.2.5+ 会用相同
   ID 更新对应通知，从而收敛 outbox 的 at-least-once 重投。
5. publisher 以随机 16 字节 ASCII IV 和本机 `.env.local` 注入的 16 字节 key 执行 AES-128-CBC/PKCS7，
   调用 Bark V2 `POST /push`。外层只包含 `device_key`、`ciphertext`、`iv`。
6. 发送成功后 outbox 标为 published；超时、非 2xx 或 Bark 非成功响应只记录固定
   `PUBLISHER_FAILED`，按现有最多三次策略重试并最终 dead-letter。异常正文不落库、不打印。

## 配置与权限

- `NEXUS_BARK_ENABLED` 默认 false。
- server URL 必须是无认证信息、query 或 fragment 的 `http://127.0.0.1:<port>` 或
  `http://localhost:<port>`，固定调用 `/push`。
- device key 与加密 key 按项目既有本地运行约定从受保护且 Git 忽略的 `infra/.env.local` 注入；
  不接受 CLI 参数值，不写数据库、日志、测试快照或仓库，并在配置对象和错误中隐藏值。
- `activated_at` 必须是显式 UTC 时间，用于跳过启用前的历史 queued event。
- 可选页面链接只接受 HTTP(S) 且路径为 `/paper`；未配置时通知不带链接。链接只打开既有只读页。
- Compose 使用 Bark Server v2.3.5 的多架构镜像摘要
  `sha256:32d65b07fa835c99b31a396b77727a04ed058377fc2482da3e9dc7397167ffc4`。
  稳态不发布 Bark 端口；worker 共享 Bark 的网络命名空间并调用 `127.0.0.1`。

## iPhone 注册与网络边界

Bark App 必须先把 APNs device token 注册到自托管 server。稳态 localhost-only 服务无法由 iPhone
直接访问，因此运行手册把首次注册列为明确的人工步骤：只在受信任局域网内临时发布 Bark 端口，
完成 App 绑定及 `/register/<device-key>` 检查后立即停止临时配置，再以无端口发布的稳态 profile
启动。不得公网映射或配置公网反向代理。

## 验证与回滚

单元测试覆盖固定脱敏映射、AES 解密回读、外层字段白名单、稳定 ID、超时和非 2xx。PostgreSQL
集成测试覆盖 activated-at 过滤、重复事件收敛、有限失败记录，以及通知失败时既有 Paper 业务事实
保持已提交。Compose 用 `docker compose config` 验证。Ruff、Mypy 和相关 pytest 必须通过。

关闭只需把 `NEXUS_BARK_ENABLED=false` 并停止精确的 notification worker/Bark 服务；已提交的业务
事实不回滚、不删除。需要重新启用时设置新的 `activated_at`，先查 outbox 状态，不重放结果不明的
事件。Bark 数据卷可保留以保留注册；删除卷属于另行确认的破坏性操作。

## 官方协议依据

- Bark V2 push、`ciphertext`、`url`、`action`：<https://github.com/Finb/bark-server/blob/master/docs/API_V2.md>
- Bark AES-CBC 加密流程：<https://github.com/Finb/Bark/blob/master/docs/en-us/encryption.md>
- Bark 稳定 `id` 更新语义：<https://github.com/Finb/Bark/blob/master/docs/en-us/tutorial.md>
- Bark 注册端点：<https://github.com/Finb/bark-server/blob/master/route_register.go>
