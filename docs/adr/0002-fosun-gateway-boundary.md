# ADR 0002：复星接入使用独立只读网关

- 状态：Accepted（真实 SDK admission 仍 Blocked）
- 日期：2026-08-27

## 背景

复星 SDK/OpenAPI 涉及 Provider 密钥、客户端私钥/服务端公钥、出站 IP、账户外部 ID 和经纪商语义。如果将 SDK 直接嵌入 Web、Agent 或通用 Engine，会扩大密钥域、网络权限和任意路由被滥用的范围。

## 决策

复星适配器运行为独立 `fosun-gateway` 进程，只有 `broker-worker` 可以通过独立 mTLS 客户端身份和 gateway HMAC 调用。Web/Agent/API 不持有 Provider 凭据、gateway server 证书、broker external-ID 密钥或任意网关路径选择能力。

网关编译期只注册七个 `POST /internal/v1/read-only/*` 操作：

1. `fosun_capabilities`；
2. `fosun_sync_accounts`；
3. `fosun_sync_portfolio`；
4. `fosun_list_orders`；
5. `fosun_list_cash_flows`；
6. `fosun_get_quotes`；
7. `fosun_get_bars`。

调用方不能传 URL、HTTP method、SDK method 名或任意 operation。不编入 generic proxy、订单创建/撤销/修改、模拟账户变更、订阅 CRUD 或 Webhook handler。当前 Engine gateway client 还进一步只消费组合、订单列表和现金流三个 broker 读取操作；行情操作由各自数据 authority 边界消费。

## 密钥与运行域

- Engine HMAC 与 gateway HMAC 是两组独立 current/previous 域；
- broker-worker 持有 mTLS client cert/key/CA，gateway 持有 server cert/key/client CA；
- broker external-ID AEAD/blind-index 只在 broker-worker；
- `FSOPENAPI_*` 与 provider manifest/admission 只在 gateway；
- `app` Compose profile 不含 broker 代码与 secret，`broker` 必须显式选择；
- production 不得在 SDK/gateway 失败时回退到 fixture。

## 后果

优点是可以独立停止、撤销、审计和网络限制 Provider 能力，Agent 即使产生恶意/错误输出也无法构造任意 Provider 请求。代价是需要维护跨进程合同、mTLS/HMAC 轮换和更明确的对账状态机，这些复杂度是隔离真实账户所必需。

真实只读激活仍受 `docs/runbooks/fosun-sdk-admission.md` 约束。网关边界存在不代表 SDK 已获准，更不代表允许真实下单。

## 被拒绝的方案

- 在 Web/BFF 中直接调用复星：泄露服务端密钥域并绕开 Engine authority。
- 让 Agent 直接持有 broker tool：LLM 输出不能成为权限/风控边界。
- 单一通用 Provider proxy：请求面无法用编译期 allowlist 证明，将未来 SDK 新增写能力意外暴露给调用方。
