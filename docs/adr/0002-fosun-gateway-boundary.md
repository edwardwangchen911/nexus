# ADR 0002：复星接入使用独立只读网关

- 状态：Accepted（真实 SDK admission 仍 Blocked）
- 日期：2026-08-27

## 背景

复星 SDK/OpenAPI 涉及 Provider 密钥、客户端私钥/服务端公钥、出站 IP、账户外部 ID 和经纪商语义。如果将 SDK 直接嵌入 Web、Agent 或通用 Engine，会扩大密钥域、网络权限和任意路由被滥用的范围。

## 决策

复星适配器运行为独立 `fosun-gateway` 进程。只有两个职责分离的调用方可以通过各自的 mTLS 客户端身份和 gateway HMAC v2 调用：`broker-worker` 消费账户域只读事实，`provider-worker` 消费美股行情域只读事实。Web/Agent/API 不持有 Provider 凭据、gateway server 证书、broker external-ID 密钥或任意网关路径选择能力。

网关编译期只注册七个 `POST /internal/v1/read-only/*` 操作：

1. `fosun_capabilities`；
2. `fosun_sync_accounts`；
3. `fosun_sync_portfolio`；
4. `fosun_list_orders`；
5. `fosun_list_cash_flows`；
6. `fosun_get_quotes`；
7. `fosun_get_bars`。

调用方不能传 URL、HTTP method、SDK method 名或任意 operation。不编入 generic proxy、订单创建/撤销/修改、模拟账户变更、订阅 CRUD 或 Webhook handler。Gateway 由 HMAC key ID 派生固定 principal，并在 transport 前执行 operation allowlist：

- `nexus-broker-worker` 只允许组合、订单列表和现金流三个 broker 读取操作；
- `nexus-provider-worker` 只允许 quotes 与 bars 两个 market 读取操作；
- capabilities 与 account discovery 不授予常驻 worker，未来需要时使用独立 bootstrap 身份。

越权操作返回统一 404，不能到达 Provider transport。Engine 为两个 principal 使用互不复用的固定路径 client；行情调用还受 Provider capability、source policy 与调用前后 authority 复验约束。

## 密钥与运行域

- Engine HMAC、broker gateway HMAC 与 market gateway HMAC 是三个独立轮换域；
- broker-worker 与 provider-worker 各持有不同的 mTLS client cert/key/CA 和 current/previous HMAC 身份，gateway 持有 server cert/key/client CA；
- HMAC v2 的签名覆盖 actor ID 与 request ID；actor 必须与 key ID 注册的 principal 完全一致，不能由请求头自报；
- broker external-ID AEAD/blind-index 只在 broker-worker；
- `FSOPENAPI_*` 与 provider manifest/admission 只在 gateway；
- `app` Compose profile 不含 broker/market gateway secret；`broker` 或 `provider` profile 必须显式选择；
- production 不得在 SDK/gateway 失败时回退到 fixture。
- `read_only` transport 只能在 production 且完整 mTLS 下启动；development/test 只能 fixture。

## 后果

优点是可以独立停止、撤销、审计和网络限制 Provider 能力，Agent 即使产生恶意/错误输出也无法构造任意 Provider 请求。代价是需要维护跨进程合同、mTLS/HMAC 轮换和更明确的对账状态机，这些复杂度是隔离真实账户所必需。

真实只读激活仍受 `docs/runbooks/fosun-sdk-admission.md` 约束。网关边界存在不代表 SDK 已获准，更不代表允许真实下单。

## 被拒绝的方案

- 在 Web/BFF 中直接调用复星：泄露服务端密钥域并绕开 Engine authority。
- 让 Agent 直接持有 broker tool：LLM 输出不能成为权限/风控边界。
- 单一通用 Provider proxy：请求面无法用编译期 allowlist 证明，将未来 SDK 新增写能力意外暴露给调用方。
