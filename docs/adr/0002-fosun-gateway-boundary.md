# ADR 0002：复星接入使用独立只读网关

- 状态：Accepted（真实 SDK admission 仍 Blocked）
- 日期：2026-08-27

## 背景

复星 SDK/OpenAPI 涉及 Provider 密钥、客户端私钥/服务端公钥、出站 IP、账户外部 ID 和经纪商语义。如果将 SDK 直接嵌入 Web、Agent 或通用 Engine，会扩大密钥域、网络权限和任意路由被滥用的范围。

## 决策

复星适配器运行为独立、单副本的 `fosun-gateway` 进程。该进程在同一应用、同一 replay journal 和同一真实 SDK 上启动两个固定 TLS listener：`8081` 仅信任 broker client CA，`8082` 仅信任 market client CA。只有两个职责分离的调用方可以通过各自的 mTLS 客户端身份和 gateway HMAC v2 调用：`broker-worker` 消费账户域只读事实，`provider-worker` 消费美股行情域只读事实。Web/Agent/API 不持有 Provider 凭据、gateway server 证书、broker external-ID 密钥或任意网关路径选择能力。

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

每个 listener 的本地 ASGI server port 映射出不可由客户端 header 覆盖的 expected principal。网关只在签名完整验证后比对 listener principal 和 HMAC principal，两者不一致统一返回 401；越权操作返回统一 404。两种失败都在 nonce 预留和 Provider transport 之前发生。Engine 为两个 principal 使用互不复用的固定路径 client；行情调用还受 Provider capability、source policy 与调用前后 authority 复验约束。

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
lock 序列化这些输入的变更与新事实提交。Provider 会话固定为 `READ COMMITTED`，启动角色
探针和每轮 schedule/preclaim/claim 都校验 `current_setting('transaction_isolation')`；这是为了让
加锁后的 `populate_existing` 重读取得新的 statement snapshot，其他隔离级别一律 fail closed。
应用锁顺序固定为：按 operation 排序的 capability scope、按 UUID 排序的 integration status、
按 UUID 排序的 raw `DataArtifact`，不得交换锁类或依赖 ORM identity map 旧值。两个最小
`SECURITY DEFINER` 由迁移
拥有者控制：`nexus_mark_provider_backtest_authority_publication()` 只能由两个无条件、
非延迟、无参数的 `AFTER INSERT FOR EACH ROW` trigger 为 `corporate_actions` 和
`price_bar_set_authorities` 追加回测发布标记；
`nexus_guard_provider_price_bar_frozen_insert()` 只能由 `price_bar_records` 的固定
`BEFORE INSERT FOR EACH ROW` trigger 读取回测冻结成员并拒绝追加。Provider 与 `PUBLIC`
都没有直接执行权，Provider 也不能读取回测成员表或直接写发布标记。worker 启动时
要求 credential 直接登录 Provider role，拒绝双向 role-membership edge，并核对所有非系统
schema 的 relation/column/sequence 权限、`PUBLIC` ACL、grant option 与 definer 精确集合，以及
helper 源码、owner、ACL、`pg_catalog` 优先的固定 `search_path`、RLS 内建函数 OID 绑定、三个
owner-rights trigger，以及 63 个 Provider 可写表 trigger 和 5 个 authority-input mutation
trigger（共 68 个）的完整 topology/函数体摘要。后 5 个包含 status 锁、capability scope 锁、
capability append-only 和两张输入表的 created-at 不可变边界；禁用、改名或源码漂移均拒绝启动。
运行时还要求会话角色未经 `SET ROLE`、本身是 `LOGIN` 角色、对当前数据库没有
effective `CREATE` 能力；所有 Provider 可写 relation 必须精确为普通表 `relkind='r'`，
trigger 必须无 `UPDATE OF` 列集合（`tgattr` 为空）且不是分区 clone；
任一漂移均拒绝启动。这两个 owner-rights bridge 只解决既有审计/冻结
trigger 的最小需求，不扩大 Provider 的表权限或网络能力。

## 密钥与运行域

- Engine HMAC、broker gateway HMAC 与 market gateway HMAC 是三个独立轮换域；
- broker-worker 与 provider-worker 各持有不同的 mTLS client cert/key 和 current/previous HMAC 身份，gateway 持有 server cert/key 及两个专用 client trust root；两个 CA 文件都必须恰好包含一张 `CA=true` 证书，且证书指纹和 CA 公钥都不同，否则生产进程拒绝启动；
- HMAC v2 的签名覆盖 actor ID 与 request ID；actor 必须与 key ID 注册的 principal 完全一致，不能由请求头自报；
- Gateway 把 nonce 写入模式为 `0600` 的 SQLite replay journal，并在同一持久卷的
  lease 文件上持有非阻塞排他锁；第二进程无法获得 lease 则拒绝启动。Compose 与运行时
  仍固定一个副本，外部编排必须挂载同一个支持 POSIX lock 的持久卷；
- replay 容量按 HMAC key ID 独立计数；key ID 又被强制在 broker/market 及 current/previous 间全局唯一，因此 Provider 无法消耗 broker 的 nonce 配额。持久 journal 之前还有两个 principal lane，行情积压最多让一个 Provider reservation 与 broker 争用共享 SQLite 事务，不能把其余 Provider 请求排到 broker 之前；
- replay journal 跨正常重启保留已用 nonce；每次生产进程启动都重新进入 61 秒认证
  拒绝窗口，以覆盖 journal snapshot 回滚无法由卷内状态自证的边界。持久化 clock
  high-water mark 防止墙钟回拨后提前清理/重放；本地时间未追平时持续拒绝认证且不就绪。
  未来时间戳始终拒绝。Compose healthcheck 还要求 lifespan marker 的 PID 存活且 Gateway
  `8081` 和 `8082` 两个 TCP listener 都已建立；broker/provider worker 只在 Gateway healthy 后启动。
- readiness marker 另外冻结当前获准 capability 的过期 epoch。SDK transport 从激活证据
  评估前采样的单调时钟建立不可延长的 deadline，并以墙钟或单调钟任一到期作为永久
  expiry latch；后台 watchdog 即使没有请求也会触发 fail-stop。HTTP readiness 与无密钥
  CLI/Compose readiness 在同一边界转为不健康。持久 replay clock floor 至少推进到
  `ceil(expiry)`，expiry tombstone 以原子替换和目录 `fsync` 持久化；墙钟回拨或进程重启
  都不能重新放行已经过期的能力。业务请求保留明确的 `ACTIVATION_EXPIRED` 并且不能到达
  SDK；续期必须提供新的受审激活证据并整进程重启，不支持就地延长 marker。崩溃遗留的
  ready marker 视为未清洁关闭并拒绝自动重启，必须先审查 journal、lease 和激活证据。
- SDK 调度在 broker 和 market 域各有一个独立入队 lane 和 token bucket（分别最大 6/min 与 24/min，合计不超过 30/min）；绝对 deadline 覆盖 lane、rate wait、共享执行槽与 SDK 调用。两域最终共享容量为一的 daemon SDK 线程槽，retry backoff 不占用它；rate wait 先释放该槽后再在 event loop 睡眠。最终 readiness/capability 检查和可能阻塞的日志都位于令牌线性化点之前；同一 daemon 线程在成功消费令牌后紧邻调用 SDK 方法，从而对 market 强制真实调用起点间隔至少一秒且任一 `(t-60s, t]` 最多 24 次。实际同步 SDK 调用超时或等待者被取消时，该 Gateway 进程的 transport 永久 poison：首个超时请求就返回 `PROVIDER_TRANSPORT_POISONED`，后续两个 lane 入口快速拒绝，HTTP readiness 和共享卷 marker/CLI/Compose health 同时转为不就绪。不在原进程重试、恢复或扩大 deadline；必须停机审查原因后再经批准重启整个 Gateway。底层线程无法在存活进程内强杀，但 daemon 语义不会阻止进程退出；晚到异常只被静默消费，不记录 Provider 错误文本。`fosun_get_bars` 是一次性 SDK 调用，任何失败都立即 fail-closed；quotes 与 broker 读操作仅在 transport 未 poison 时保留有界 transport retry，且每次尝试独立计入同一 QPS/RPM 预算。
- broker external-ID AEAD/blind-index 只在 broker-worker；
- `FSOPENAPI_*` 与 provider manifest/admission 只在 gateway；
- 一次性 probe 进程单独持有受保护的 Ed25519 发布私钥；长驻 gateway 只接收变更受控的
  公钥 pin。artifact 中声明的公钥不建立信任，只有 pin 匹配且 detached receipt 验签成功
  才能形成只读 capability；pin 轮换必须连同 artifact、receipt、contract hash 一起复审；
- Compose 将 artifact/receipt 分别作为 `create_host_path: false` 的只读文件挂载，将发布公钥
  与 contract hash 作为两个独立 file-backed config；Gateway 仅接受对应 `_FILE` 路径，不接收
  raw pin/hash 环境值，probe 进程不获得已晋级证据、contract config 或 replay 卷；
- provider manifest、probe artifact 与 receipt 在 SDK import 前通过 held descriptor、全路径
  no-follow、regular-file、owner/mode、大小和读前后稳定性校验，任何不确定性 fail closed；
- Provider 对象存储使用独立于 app/general worker 的 IAM principal，运行时固定在
  `provider/market` 前缀；只授予 PutObject、条件创建冲突复核用的 key-scoped GetObject
  和冻结证据重放用的 GetObjectVersion，不授予 ListBucket 或 DeleteObject。
  `DataArtifact.sha256` 只表示精确对象字节摘要；
  raw staging 另存对 id、URI、byte size、mime、schema、source、三个时点和 created-at 的
  canonical identity SHA-256，发布时在锁下重载并逐字段复核。raw/normalized URI 还必须精确
  绑定完整 content-addressed key；只在 query/fragment/substring 中声称 key 一律拒绝；
  S3 bucket versioning 必须精确为 `Enabled`，且 locator 必须携带解码后不为 `null` 的
  精确 `versionId`；缺失/`Suspended` 或 PUT、冲突复核、URI 准入、读回中的
  `VersionId=null` 均 fail closed；
- 日线聚合必须先完成全部 normalized object prepare，再以 PostgreSQL `clock_timestamp()`、
  Provider Worker 时钟和最新 staging 时点的最大值生成全批唯一 `provider_authorized_at`，
  并在持锁状态下完成最后一次 authority 复核；历史 as-of 在该时点前看不到批中任何标的；
- 所有来源的盘中分钟栏在 normalized 序列化前都把精确 raw SHA-256 写入 `revision`，
  持久化层再次校验该值与已锁定 raw artifact 一致；不同 raw 响应不能因 OHLCV 相同而归并；
- Fosun 盘中 Job 的单一 worker monotonic deadline 包含预检：acquisition 最多 24 秒，
  为已完成响应的独立 partial-raw 事务保留 5 秒，并向 worker 外层保留 1 秒；
  内部取消/guard 失败不 normalization、不发布、不重试，operator 取消原样传播；
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
