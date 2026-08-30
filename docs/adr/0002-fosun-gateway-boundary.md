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

行情 `provider-worker` 是受信任的 normalization/writer TCB：它校验网关响应、保存 transport
evidence、规范化行情并写入 Provider 数据表。网关边界保护 Provider 凭据、出站网络和固定
operation 面，不提供抵御已攻陷 `provider-worker` 的响应来源证明。因而“不可变证据”表示
提交后不可更新/删除和可检测血缘漂移，不表示写入者无法自造事实。若 worker 完整性未知，必须
进入人工 incident stop，停止 provider profile、禁用集成并禁止继续依赖相关事实作出新决策。
现版本不声称能自动检测 compromise 或撤销此前冻结的事实。
将 hostile-worker 纳入威胁模型需要另立 ADR，引入 gateway-only 响应 attestation、独立验证/
authority writer 和独立密钥域，不能复用现有请求 HMAC 冒充响应证明。

Provider 数据库角色对 capability、integration status、已有 artifact 和既有 manifest 只有
`SELECT`，不会为了校验 authority 获得 `UPDATE`。数据库用与校验函数相同的事务级 advisory
lock 序列化这些输入的变更与新事实提交。两个最小 `SECURITY DEFINER` 由迁移
拥有者控制：`nexus_mark_provider_backtest_authority_publication()` 只能由两个无条件、
非延迟、无参数的 `AFTER INSERT FOR EACH ROW` trigger 为 `corporate_actions` 和
`price_bar_set_authorities` 追加回测发布标记；
`nexus_guard_provider_price_bar_frozen_insert()` 只能由 `price_bar_records` 的固定
`BEFORE INSERT FOR EACH ROW` trigger 读取回测冻结成员并拒绝追加。Provider 与 `PUBLIC`
都没有直接执行权，Provider 也不能读取回测成员表或直接写发布标记。worker 启动时
要求 credential 直接登录 Provider role，拒绝双向 role-membership edge，并核对所有非系统
schema 的 relation/column/sequence 权限、`PUBLIC` ACL、grant option 与 definer 精确集合，以及
helper 源码、owner、ACL、`pg_catalog` 优先的固定 `search_path`、RLS 内建函数 OID 绑定、三个
owner-rights trigger 和全部 60 个 Provider 可写表应用 trigger 的完整 topology/函数体摘要。
运行时还要求会话角色未经 `SET ROLE`、本身是 `LOGIN` 角色、对当前数据库没有
effective `CREATE` 能力；所有 Provider 可写 relation 必须精确为普通表 `relkind='r'`，
trigger 必须无 `UPDATE OF` 列集合（`tgattr` 为空）且不是分区 clone；
任一漂移均拒绝启动。这两个 owner-rights bridge 只解决既有审计/冻结
trigger 的最小需求，不扩大 Provider 的表权限或网络能力。

## 密钥与运行域

- Engine HMAC、broker gateway HMAC 与 market gateway HMAC 是三个独立轮换域；
- broker-worker 与 provider-worker 各持有不同的 mTLS client cert/key/CA 和 current/previous HMAC 身份，gateway 持有 server cert/key/client CA；
- HMAC v2 的签名覆盖 actor ID 与 request ID；actor 必须与 key ID 注册的 principal 完全一致，不能由请求头自报；
- Gateway 把 nonce 写入模式为 `0600` 的 SQLite replay journal，并在同一持久卷的
  lease 文件上持有非阻塞排他锁；第二进程无法获得 lease 则拒绝启动。Compose 与运行时
  仍固定一个副本，外部编排必须挂载同一个支持 POSIX lock 的持久卷；
- replay journal 跨正常重启保留已用 nonce；每次生产进程启动都重新进入 61 秒认证
  拒绝窗口，以覆盖 journal snapshot 回滚无法由卷内状态自证的边界。持久化 clock
  high-water mark 防止墙钟回拨后提前清理/重放；本地时间未追平时持续拒绝认证且不就绪。
  未来时间戳始终拒绝。Compose healthcheck 还要求 lifespan marker 的 PID 存活且 Gateway
  TCP listener 已建立；broker/provider worker 只在 Gateway healthy 后启动。
- broker external-ID AEAD/blind-index 只在 broker-worker；
- `FSOPENAPI_*` 与 provider manifest/admission 只在 gateway；
- Provider 对象存储使用独立于 app/general worker 的 IAM principal，运行时固定在
  `provider/market` 前缀；永久证据只允许 Put/Get，Delete 只允许临时
  `provider/market/temporary/market` 子树；
- `app` Compose profile 不含 broker/market gateway secret；`broker` 或 `provider` profile 必须显式选择；
- production 不得在 SDK/gateway 失败时回退到 fixture。
- `read_only` transport 只能在 production 且完整 mTLS 下启动；development/test 只能 fixture。

## 后果

优点是可以独立停止、撤销、审计和网络限制 Provider 能力，Agent 即使产生恶意/错误输出也无法构造任意 Provider 请求。代价是需要维护跨进程合同、mTLS/HMAC 轮换和更明确的对账状态机，这些复杂度是隔离真实账户所必需。

剩余风险是受信任 `provider-worker` 的 compromise 可以污染行情研究域。职责分离把其破坏半径
限制在 market 数据域：它没有 broker principal、账户 ID 密钥或订单写能力；本 ADR 不把该
残余风险描述为已经由哈希、create-only 存储或请求 HMAC 消除。

真实只读激活仍受 `docs/runbooks/fosun-sdk-admission.md` 约束。网关边界存在不代表 SDK 已获准，更不代表允许真实下单。

## 被拒绝的方案

- 在 Web/BFF 中直接调用复星：泄露服务端密钥域并绕开 Engine authority。
- 让 Agent 直接持有 broker tool：LLM 输出不能成为权限/风控边界。
- 单一通用 Provider proxy：请求面无法用编译期 allowlist 证明，将未来 SDK 新增写能力意外暴露给调用方。
