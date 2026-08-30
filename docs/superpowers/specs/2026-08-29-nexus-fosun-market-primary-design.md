# NEXUS 复星美股行情主源运行时设计规格

**日期：** 2026-08-29

**状态：** 已冻结，允许直接实施

**范围：** 复星美股 L1 行情通过隔离网关进入 Provider Worker、不可变证据、PIT/质量门禁、Agent 日线价格 authority v2、盘中监控和市场页面
**明确排除：** 真实凭据注入、真实 Provider/账户探测、复星或 Alpaca 外部调用、付费订阅、部署、券商写操作、模拟券商写入和实盘订单

## 1. 结论

NEXUS 的产品决策保持不变：美股价格数据优先使用复星，Alpaca 仅作为默认关闭的可选补充；SEC 与 FRED/ALFRED 是独立研究数据域，不随 Alpaca 市场数据开关变化。

本次不是把系统从 Alpaca 架构改成复星架构，而是修复实现与已发布来源政策不一致的问题。当前数据库政策已经声明 `fosun -> alpaca -> fixture`，但生产组合、盘中 Snapshot、Agent 日线 authority 和网关调用身份仍硬编码或仅接受 Alpaca。仅删除 Alpaca 配置会使系统进入 `unavailable/abstain`，不会自动获得复星数据。

冻结后的运行拓扑为：

```mermaid
flowchart LR
    A[观察列表/冻结 Universe] --> B[Provider Worker]
    B -->|market-only mTLS + HMAC v2| C[Fosun read-only gateway]
    C -->|admitted SDK, quotes/bars only| D[复星 OpenAPI]
    C --> E[脱敏 gateway transport evidence]
    E --> F[调用前后 authority 复验]
    F --> G[不可变行情事实 + authority v2]
    G --> H[Market API / 页面]
    G --> I[Agent 确定性价格工具]
    I --> J[研究 Agent]

    K[Alpaca adapter] -.显式批准后才可启用.-> B
    J -.禁止.-> C
    J -.禁止.-> L[订单/密钥/硬风控]
```

复星真实激活仍由现有 SDK admission 和 read-only activation runbook 单独控制。本地实现完成不等于真实接口已可用。

## 2. 复星与 Alpaca 的职责差异

| 维度 | 复星 | Alpaca | NEXUS 决策 |
| --- | --- | --- | --- |
| 账户关系 | 同时提供券商账户、持仓、订单及行情域 | 独立市场数据/交易平台 | 账户事实只来自复星；绝不回退 Alpaca |
| 美股行情 | 已公开 US L1、quote、K 线、分时、逐笔、盘口和推送能力 | Snapshot 可一次返回 trade、bid/ask、minute bar；IEX/SIP 权限清楚 | 首期复星主源；Alpaca adapter 保留但关闭 |
| 覆盖/费用 | 以实际 admission/probe 证据为准，不从文档宣传推断实时性或历史深度 | Basic 通常是 IEX；SIP/更高额度涉及相应权限或付费 | 未证明的复星延迟一律 `unknown`；不为补全字段自动购买 Alpaca |
| 安全边界 | SDK、API key、客户端私钥、服务端公钥和出站白名单集中在 gateway | Provider Worker 固定 HTTPS client | 两者都不能进入 Web/Agent；复星额外经过独立 gateway |
| 典型补充价值 | 同账户行情、券商事实一致性 | 独立交叉验证、SIP、已知历史深度、批量 Snapshot | 只在第 12 节触发条件成立并另行批准后启用 Alpaca |

已核对的公开来源：

- 复星官方 `fw-trading`：<https://github.com/fosunwealth/fw-trading>
- 复星 Market OpenAPI：<https://openapi-docs.fosunxcz.com/specs/public/market/market/openapi.yaml>
- 复星 Push OpenAPI：<https://openapi-docs.fosunxcz.com/specs/public/market/push/openapi.yaml>
- Alpaca Market Data 权限说明：<https://docs.alpaca.markets/us/docs/about-market-data-api>
- Alpaca IEX/SIP FAQ：<https://docs.alpaca.markets/us/docs/market-data-faq>

公开文档只能确定接口能力，不能替代 NEXUS 的 license、SDK、TLS、entitlement、延迟、请求预算或真实覆盖 probe。

## 3. 数据域政策

| 数据域 | 首选 | 次选 | 首选不可用时 |
| --- | --- | --- | --- |
| 复星账户、现金、持仓、订单、流水 | 复星 broker read-only | 无 | `BROKER_STALE`，不回退 |
| 美股日线、回测、Agent 价格、paper 参考价 | 复星 bars authority v2 | 经批准的 Alpaca | 无合格源时 abstain |
| 盘中显示 | 复星 quote + 完整 minute bars | 经批准的 Alpaca Snapshot | 显示 unavailable/partial，不生成信号 |
| 公司行动 | 独立 capability；现有 Alpaca adapter 不随价格开关自动启用 | 待单独设计 | unavailable |
| 财报 | SEC | 无自动回退 | unavailable/abstain |
| 宏观 | FRED/ALFRED | 无自动回退 | unavailable/abstain |

系统只选择完整来源序列或同一来源的事实集合，不跨来源拼接 bid/ask、last 或 bars。来源冲突阻断需要完整价格权威的后续动作；盘中显示只告警。

## 4. 信任域与最小权限

### 4.1 进程权限

- `fosun-gateway` 是唯一持有 `FSOPENAPI_*`、SDK admission、Provider base URL 和出站网络能力的进程。
- `provider-worker` 只持有 market-only 网关 mTLS 客户端证书、HMAC 身份、Provider 数据库角色，
  以及独立的 market-prefix 对象存储 principal；不持有账户 ID 加密密钥。对象键被运行时固定在
  `provider/market`，只授予 PutObject、用于条件创建冲突复核的 key-scoped GetObject，
  以及用于冻结证据重放的 GetObjectVersion；不授予 ListBucket 或 DeleteObject。
- Provider 数据库角色对 status/capability/已有 artifact/manifest 只有读取权；authority 校验与这些输入的变更使用同名事务级 advisory lock 串行，不以 `SELECT FOR SHARE` 换取隐含 `UPDATE` 权限。
- Provider 数据库会话固定为 `READ COMMITTED`；启动角色探针以及每轮 schedule/preclaim/claim
  均校验实际 transaction isolation。加锁后的 `populate_existing` 必须取得新的 statement
  snapshot，其他隔离级别一律 fail closed。应用锁序固定为按 operation 排序的 capability
  scope、按 UUID 排序的 integration status、按 UUID 排序的 raw `DataArtifact`。
- Provider 数据库凭据必须直接认证为独立 login role；`SET ROLE`、出入两个方向的 membership、任意非系统 schema 权限、Provider 或 `PUBLIC` 的 column/sequence ACL，以及 allowlist 外 table/partition/view/materialized-view/foreign-table 权限或 grant option 均使启动失败。RLS helper 固定 `search_path=pg_catalog, public, pg_temp`，bypass policy 还核对其绑定的 catalog `pg_has_role` OID。
- `broker-worker` 使用另一组 mTLS/HMAC 身份，只能调用组合、订单列表和现金流水读取；它不拥有行情采集队列。
- Web、BFF、API、general worker、Agent 和 Strategy R&D 均不持有任一网关客户端身份。
- 复星 gateway profile 可由 `provider` 或 `broker` 显式启动；普通 `app` profile 不启动它。

既有回测审计要求 authority/corporate-action INSERT 同事务写入 publication mark，而受限
Provider 不能直接写该标记表，也不得因 bar INSERT 获得回测成员表读权。迁移因此保留
两个 owner-controlled、trigger-only `SECURITY DEFINER` bridge，都固定
`search_path=pg_catalog, public, pg_temp`：publication bridge 只接受两张允许表的无条件、
非约束、非延迟、无参数 `AFTER INSERT FOR EACH ROW` trigger 并只追加 public-qualified
mark；price-bar guard 只接受 `price_bar_records` 的无条件 `BEFORE INSERT FOR EACH ROW`
trigger，并仅判断对应 manifest 是否已被回测冻结。Provider、`PUBLIC` 和其他非 owner 均无直接
`EXECUTE`；startup probe 对全部非系统 schema 的 definer 精确集合、精确源码/owner/ACL、三个
owner-rights trigger，以及 63 个 Provider 可写表 trigger 和 5 个 authority-input mutation
trigger（共 68 个）的启用状态、时机、函数归属、配置与函数体摘要任一漂移都 fail closed。
输入侧集合明确包含 integration-status Fosun 锁、capability scope 锁、capability append-only
以及两张表的 created-at guard。

### 4.2 HMAC v2

现有 HMAC v1 未把 `actor_id` 与 `request_id` 纳入签名，不能作为 principal 授权依据。由于真实 SDK 尚未激活，本次直接关闭 v1 并使用 v2：

nonce 保留期以签名时间戳的最晚有效时点计算，而不是首次到达时点。
服务端不接受未来时间戳。生产 nonce 写入持久 SQLite journal，正常重启继续拒绝
已用 nonce；同一持久卷的排他 lease 使第二个 Gateway 进程在启动时失败。每次生产
进程启动都进入新的 61 秒认证拒绝窗口，覆盖旧 snapshot 无法由卷内状态自证的问题；
持久化 clock high-water mark 使墙钟回拨期间持续拒绝认证。readiness/Compose health
还要求 lifespan owner PID 存活且 TCP listener 已建立。支持的部署固定一副本；任何
外部编排也必须让所有候选进程挂载同一个支持 POSIX lock 的 journal 卷，不得使用
独立 replay state 重叠运行。

```text
hmac-v2\n
POST\n
canonical_path_and_query\n
unix_timestamp\n
nonce_uuid7\n
actor_id\n
request_id_uuid7\n
sha256(body)
```

请求必须包含固定版本头、key ID、timestamp、nonce、actor ID、request ID、body hash 和 signature。服务端由 key ID 注册表派生 principal，并要求请求 actor ID 与注册 principal 完全相等；不能由调用方自报角色。

固定授权矩阵：

| principal | 允许 operation |
| --- | --- |
| `nexus-broker-worker` | `fosun_sync_portfolio`、`fosun_list_orders`、`fosun_list_cash_flows` |
| `nexus-provider-worker` | `fosun_get_quotes`、`fosun_get_bars` |

`fosun_capabilities` 与 `fosun_sync_accounts` 不授予常驻 worker；未来若需要，使用独立 bootstrap 身份。越权统一返回 404，transport 调用计数为零。broker/provider 的 current/previous key ID、secret 和客户端证书必须独立；所有配置 key ID/secret 跨角色唯一。

### 4.3 数据真实性 TCB 与非目标

`provider-worker` 是受信任的数据接入根（normalization/writer TCB），不是需要由
`fosun-gateway` 防御的敌对租户。它负责校验 gateway 合同、保存收到的字节、规范化事实并以
Provider 数据库角色发布 authority；因此其进程完整性、已审查镜像和运行身份是行情真实性的
前提。`fosun-gateway` 的隔离目标是把 Provider 凭据、出站网络与固定只读 operation 面从
Web、Agent、general worker 和 broker 域中剥离，不是对已攻陷的 `provider-worker` 提供
gateway-origin 的密码学证明。

本阶段的 `immutable` 只表示事实提交后不能更新或删除，血缘哈希可检测已提交数据的漂移；它
不表示能够阻止一个已获得 Provider 数据库写权限和 market-prefix 对象存储权限的恶意 worker
自造一套内部自洽的行情事实。该进程一旦完整性未知，运维必须进入人工 incident stop：停止
provider profile、禁用集成，并禁止平台继续作出依赖这些事实的新决策。现版本不能自动检测
worker compromise，也不能自动撤销此前冻结的事实；恢复前必须单独处置并重建信任。若未来把“抵御被攻陷的 Provider
Worker”纳入威胁模型，必须先单独设计 gateway-only 响应签名、独立验证/authority writer、
重放约束和新的密钥域；不能把现有 HMAC 请求签名或 create-only 存储宣称为 origin attestation。

### 4.4 TLS

- `read_only` transport 只允许 `NEXUS_ENV=production` 且 server mTLS 配置完整；development/test 只能 fixture。
- Gateway client 固定 HTTPS origin、证书/私钥/CA、`trust_env=False`、禁重定向、10 秒绝对 deadline 和响应字节上限。
- 绝对 deadline 覆盖入队、限速、共享 SDK 槽与真实调用。真实同步 SDK 超时/取消会永久 poison 当前 transport，撤销 HTTP 及共享 marker readiness，两个 principal 后续快速失败；只能停机审查后整进程重启。daemon 线程不阻塞进程退出，晚到 Provider 异常不得格式化或输出日志。
- mTLS 负责连接认证，HMAC principal 负责 operation 授权；任一成功都不能替代另一项。

## 5. Provider 运行配置

新增显式市场模式：

```text
NEXUS_MARKET_PROVIDER=fosun|alpaca
NEXUS_ALPACA_MARKET_MODE=disabled|enabled
PROVIDER_OBJECT_STORE_PREFIX=provider/market
```

默认 `fosun + disabled`。规则：

- fixture 模式不构造任何生产 client，也不读取 secret value；
- production + Fosun 只要求复星 market gateway、SEC 和 FRED 配置，不要求 Alpaca key；
- production + Alpaca 必须同时显式 `ALPACA_MARKET_MODE=enabled` 并满足现有 feed/delivery/credential 门禁；
- Provider 的 `PROVIDER_OBJECT_STORE_*` principal 必须与 app/general worker 的
  `OBJECT_STORE_*` 不同；bucket/versioning/prefix/IAM 由激活流程预置，Provider profile
  不运行通用 `object-store-init`；
- 禁止配置错误时回退 fixture 或另一 Provider；
- SEC/FRED 配置与 Alpaca 市场模式独立；
- `repr`、验证错误和日志不能包含 secret、证书路径内容、URL query 或原始 body。
固定来源 HTTP client 在构造和每次请求边界都封闭 `httpx`/`httpcore` 依赖日志，
只输出 NEXUS 自有的 provider/attempt/status-class/elapsed-bucket/result 脱敏事件。root 或
依赖 logger 被调到 DEBUG/INFO 不得使 FRED query key、授权 header、host/path 或 body 进入日志。

Fosun 首期固定：

- `feed=l1`；
- `delivery_mode=unknown`，除非 activation capability 明确证明 `real_time` 或 `delayed`；
- ticker 由 PIT instrument alias 解析成唯一 `us{SYMBOL}`，调用方不能传任意 Provider code；
- 每批 quote 1–10 个 symbol；不得超过 admission 记录的请求预算。

## 6. Gateway market 合同

### 6.1 请求

`fosun_get_quotes`：1–50 个唯一、规范排序且匹配 `^us[A-Z][A-Z0-9.-]{0,14}$` 的 code。

`fosun_get_bars`：一个合格 code、`day|minute`、成对出现且有时区的 start/end、`start <= end`、limit 1–500。不能传 URL、HTTP method、SDK method、headers 或未注册参数。

### 6.2 响应

Gateway 只返回脱敏、严格、gateway-owned JSON；Provider 原始账户或 transport 对象永不跨边界。Provider Worker 保存收到的精确 gateway HTTP JSON 字节，名称固定为 **gateway transport evidence**，不能误称为 Provider raw body。这里的 `gateway-owned` 描述响应合同的生产责任，不构成对敌对 Provider Worker 的独立密码学来源证明。

quotes 必须与请求 code 集合完全一致；缺价以 `price=null` 和 `PARTIAL` 表达，额外 code 拒绝整批。由于当前冻结 SDK 合同未提供可靠 Provider quote timestamp：

- `QuoteItem.as_of` 是 gateway receipt time；
- Engine 写入 `FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE`；
- 它不能被描述为 Provider observed time，也不能产生 `ready` 质量。

bars 必须匹配请求 code/window/limit，严格按 timestamp 升序且无重复；价格必须为有限正 Decimal、volume 非负、OHLC 关系有效。未来时间、窗口外行、数量超限或乱序拒绝整页业务事实。

Gateway 在 SDK 调用前与返回后都检查 activation expiry；调用跨过 expiry 时返回失败，不发布响应事实。
生产进程还从证据评估前的单调时钟锚点建立不可延长的 deadline，墙钟或单调钟任一到期
都会永久 latch 并由 watchdog 主动撤销 readiness。SQLite replay clock floor 至少推进到
`ceil(expiry)`，原子 expiry tombstone 跨重启保留；未清洁关闭遗留的 ready marker 必须阻止
启动并进入审查，不能由启动或 healthcheck 静默删除。

## 7. 冻结 Provider authority

每次复星外部读取必须在调用前和完整收包后，按**实际 UTC 时钟**调用 `load_fosun_market_admission`。该函数必须验证：

- `ProviderCapability.created_at <= check_time < expires_at`；
- provider=`fosun`、capability=`READ_ONLY`、schema=`1`；
- operation 与 market_scope=`US` 精确匹配；
- `IntegrationStatus.status=read_only`；
- status admitted hash 与 capability contract hash 相同；
- capability 未被当前 invalidation 边界覆盖。

Job payload 冻结 capability ID、created_at、expires_at、contract hash、status ID/version、source、operation、feed、delivery mode 和 source-policy version。调用前/后任一字段漂移时：

1. 精确 gateway transport bytes 仍以 create-only 方式归档；
2. 不写 LatestQuote、PriceSeriesManifest、bar authority 或 Agent evidence；
3. Job 以稳定的 authority-changed 结果结束；
4. 不重试到另一 Provider。

## 8. 数据库 authority v2

新增迁移，不修改历史迁移和 v1 事实。

### 8.1 事实绑定字段

`price_series_manifests` 与 `latest_quotes` 增加可空的：

- `provider_capability_id`；
- `provider_status_id`；
- `provider_status_version`；
- `provider_contract_hash`；
- `provider_authorized_at`。

Fosun 新事实必须全部非空，非 Fosun 必须全部为空。数据库 INSERT trigger 读取 capability/status 并验证 operation、market scope、版本、hash、时间和 read-only 状态。字段被现有不可变边界覆盖，旧事实不能被后来的新授权重新合法化。

`DataArtifact.sha256` 只覆盖精确对象字节。`raw_artifact_identity_sha256` 是独立的全身份摘要，
对 `id/uri/sha256/byte_size/mime_type/schema_version/source/observed_at/available_at/received_at/created_at`
的 canonical JSON 求 SHA-256。日线 fetched staging 冻结该值；聚合发布和盘中 ingestion 在事务中
按 UUID 加锁、`populate_existing` 重载并重新计算，任一字段漂移均整批零发布。

### 8.2 不可变政策 v2

新增而非修改以下记录：

- `source_policy_v2`：日线/Agent；
- `quote_source_policy_v2`：last/quote；
- `intraday_monitor_policy_v2`：盘中 bars + observations；
- `agent_price_authority_v2` 与对应 PostgreSQL canonical hash v2；
- `intraday_monitor_authority_v2` 与对应 canonical hash v2。

v2 canonical 文档把 Provider binding 五字段纳入摘要。v1 Alpaca/fixture authority 继续按原函数重放；v2 Fosun authority 只能由新的 v2 prepare/validate 函数创建。policy/version/hash-version 必须是受允许的完整元组，不能混搭。

Agent price v2 只接受：

- interval=`1Day`、source=`fosun`、feed=`l1`；
- delivery mode 已由 capability 证明为 `real_time|delayed`，不能是 `unknown`；
- adjustment=`raw`；
- 完整 PIT Provider binding；
- 所有 bars `quality_flags=[]`；
- 一个 session 恰好一条 bar，时间顺序、instrument、行数与 manifest 一致。

`adjustment=raw` 只能为 Agent 提供最新 `close` 的可追溯展示值。在公司行动与
adjusted-price authority 尚未完成前，`sma_20`/`sma_200`/`rsi_14`/`volatility_20`
等跨 session 派生特征必须返回
`adjusted_price_authority_unavailable`，不得把拆股、分红或其他公司行动解释为收益或波动。

Gateway bars 顶层 `quality=PARTIAL` 是因为网关不持有决策时点的 XNYS 日历 authority，
因而使用的保守 transport marker，不是对每根 bar 的质量通过证明。Engine 只有在重新校验
精确 instrument/区间/顺序，且实际 session 与冻结日历集合完全一致后，才为日线生成空
`quality_flags`；缺任一 session 稳定返回 `FOSUN_MARKET_PAGE_INCOMPLETE`、仅保留原始证据且不创建
manifest/bar/authority。因此这一转换是 Engine 基于独立冻结 authority 的确定性重评估，
不是将 Gateway 的 `PARTIAL` 盲目提升为 `PASS`。

因此首期以 `delivery_mode=unknown` 激活时可以安全展示/归档，但 Agent、回测、paper 与订单价格 authority 继续 abstain，直到真实 activation 证据证明交付口径。这是预期的 fail-closed 行为。当前 proposal-to-order 路由在读取 proposal 或写入任何 Broker intent 前就检查该 authority；不可用时稳定返回 `PRICE_AUTHORITY_UNAVAILABLE`，不得用仅供展示的 `LatestQuote` 定仓或持久化订单意图。

### 8.3 PIT 读取

读取端只使用事实自身冻结的 capability/status 版本和 `provider_authorized_at`，capability 生效/过期窗口也对该冻结时点校验，不以查询时刻或未来新授权补签旧事实。当前 Provider 被禁用后不再产生新事实；历史事实仍可审计，但超过 stale/数据窗口后不能支持当前决策。所有 `timestamptz` 到交易日的转换显式使用 UTC，不依赖数据库 session `TimeZone`。

## 9. 盘中采集

Job schema 按来源显式分流，保留 job type。每 15 秒全局只有一个持久化 v2 Job：

- 每个 bucket 一次批量 quote；
- Fosun 仅在 `bucket_start.second == 0` 时逐标的读取最近两根 minute bars，并只发布已完整结束的 bar；
- Fosun 最多 10 个 symbol，确保在 gateway 的 2 QPS 总上限和 market lane
  24 RPM 保守预算内；
- handler deadline 30 秒；Fosun acquisition 从 handler 入口起最多 24 秒（包含首次
  数据库重验），为 partial raw 独立事务保留 5 秒并为 worker 外层保留 1 秒；
  bucket 结束后 45 秒失效；
- Alpaca 显式启用时每个 bucket 始终只有一次最多 30 symbol 的批量 Snapshot，用于 trade/quote，
  不增加独立 bars 请求；非分钟边界设置 `include_minute_bars=false`，normalizer 与 handler 都会
  清除响应中多余的 `minuteBar`，不创建 normalized bar/authority。

该 Job 固定为 `retry_policy=never` / `max_attempts=1`，payload 的 `bucket_start` /
`bucket_end` 必须精确等于对齐的 `effective_at` /
`effective_at + 15s`。Scheduler 在分桶 advisory lock 内按 `effective_at` 复用第一个
已持久化 Job 及其冻结 payload，后续 watchlist、来源、feed 或 authority 漂移不得为
同 bucket 创建第二个逻辑任务。数据库 partial unique index 是最终并发防线；自动/手动
retry、`attempt_no > 1`、payload/claim bucket 不一致都必须在 Provider 调用前终止。Gateway 内部
quotes 的有界 transport retry 仍属于这一个签名逻辑操作，每次真实 SDK 尝试都单独消耗
24 RPM/1 QPS 预算，它不会创建第二个 Job 或 Job attempt；`fosun_get_bars` 始终只有一次 SDK
`kline` 尝试。

acquisition 截止点取消 provider 时，provider 必须把取消前已完整收到的
bars/quote 响应编码为确定性有界 partial envelope，handler 用独立
session/transaction 提交精确 raw bytes，然后以
`skipped / INTRADAY_SNAPSHOT_READ_CANCELLED` 终态化。该路径不 normalization、不发布事实、
不重试且不创建第二 attempt；无已完成响应时不伪造 artifact。已收响应后
guard/数据库异常以 `INTRADAY_AUTHORITY_REVALIDATION_FAILED` 走同一归档路径。归档失败或
超时继续 fail closed；operator shutdown 或 worker 外层取消必须原样传播，不得冒充
内部 acquisition timeout。
正常 raw 归档、异常 partial raw 归档和数据库 ingest 都受同一 handler-owned
总截止点约束，分别使用稳定的 `INTRADAY_RAW_ARCHIVE_DEADLINE_EXCEEDED` 与
`INTRADAY_INGEST_DEADLINE_EXCEEDED`；分钟栏直接 create-only 写入最终 digest key，所有对象
写成功前不进入数据库发布。

升级前必须停止旧 Provider Worker，并在旧版本内排空所有列或 payload schema 不兼容的
actionable intraday Job；`0031_intraday_schema_admission` 不承担旧 Job 转换。新迁移以数据库
trigger 原子拒绝任何非精确 v2 的新 INSERT 和 legacy terminal requeue；`source` 与 `feed`
没有默认值，持久 payload 必须显式冻结合法的 `fosun/l1` 或
`alpaca/{iex,sip,delayed_sip}` 组合。新 worker 在启动及每轮 poll 时直接校验唯一 Alembic
head，并同时检查列 schema、payload schema 和 source/feed；缺失或非法组合都按不兼容队列
处理；非法 bucket 时间文本也必须稳定归类为 `INTRADAY_V1_QUEUE_DRAIN_REQUIRED`，不得泄漏数据库 cast 异常。调度事务先取
共享 schema-admission advisory lock，0031 downgrade 先取匹配的独占锁，防止首轮调度与
降级 guard 交错通过；同一共享锁与 head/queue guard 还必须在 heartbeat/reclaim/claim 事务
最前执行并持有至 claim 提交。监控 disabled 时仍注册无网络 v2 drain handler，只将已有
queued v2 终态化为 `INTRADAY_MONITOR_DISABLED`，不创建新快照。任一不符都 fail closed，
不得将旧任务静默 dead-letter。Fosun 与
Alpaca 各自使用其固定
payload/normalizer/authority 路径，不进行
字段级拼接，也不互相自动 fallback。

Fosun last 使用 `event_kind=snapshot`，bid/ask 保持 null；绝不能设置 `bid=ask=last`。缺盘口时 `spread_bps=null`，质量原因包含 `INTRADAY_BID_ASK_UNAVAILABLE`。quote timestamp 缺失、delivery unknown、minute window 不完整或任何 quality flag 都使状态至少为 `partial`。

批次原始证据 schema 按来源区分：`fosun_gateway_market_snapshot_v1` 或 `alpaca_stock_snapshots_v1`。同一批多次 gateway 响应以有界、确定性的 envelope 保存每个 operation、request ID、body digest 和精确 body bytes。
raw 与 normalized 对象返回的 URI 必须用同一严格 matcher 绑定完整 content-addressed key；
locator/key 必须是 ASCII 且无控制字符或 fragment delimiter，`memory://` 必须精确等于 key，
`s3://` 必须具备非空 bucket、无 userinfo、未编码且无空/`.`/`..` 段的完整 path，以及唯一非空、
合法百分号编码的 `versionId`，且解码后不得等于 S3 哨兵值 `null`（包含
百分号编码等价形式）。bucket versioning 必须精确为 `Enabled`；缺失、`Suspended`、
PUT/冲突复核/冻结 URI/读回响应中的 `VersionId=null` 都 fail closed。只在 query 或其他
substring 中出现 key 一律拒绝。
日线聚合器读取已锁定 raw artifact 时必须传入该 artifact 的完整 URI；S3 实现还要将
bucket、固定 `provider/market` prefix、完整 key 和解码后的 `versionId` 同时绑定到
GetObjectVersion，并核对响应版本。只有当条件 create 返回 409/412 且尚无 versionId 时，
才允许对该精确 content-addressed key 做一次 current-version GetObject；必须比对完整字节、
取得非 `null` VersionId 并立即冻结 locator。该读取不构成普通证据重放。
对象 key 还必须符合 ASCII 安全字符白名单 `[A-Za-z0-9._/-]+`，拒绝控制字符、
`%?#`、反斜线和歧义路径段。SEC submissions/companyfacts 的 accession 必须是精确
`^[0-9]{10}-[0-9]{2}-[0-9]{6}$`；collector 在首个对象写入前完成全部 accession 与
accession-content 交叉验证，非规范值产生零对象、零财务事实。
所有 raw/normalized 对象直接以 create-only 方式写入最终 digest key，不存在
temporary/promote/delete 路径。并发 publisher 在首个实际完成的异常后立即停止新的
排队 I/O、取消并收割同伴任务，不能等待全部完成后再按输入顺序选错。
每次 Fosun intraday 外调前在 handler 事务中获取与 integration-status 变更 trigger 同名的
advisory lock，并在锁下重验冻结 authority；锁持有至外调返回和 handler 事务结束，
使撤权与真实 read 互斥。

## 10. 日线采集

`collect.market` 根据显式市场模式构造单一 Provider：

- Fosun 使用 `adjustment=raw`、limit 500、最多 400 天窗口；
- Alpaca 保持现有显式 feed/delivery；
- 复星 bars 请求前后冻结并比较 authority；
- Engine/Gateway 将日线 `end_time` 定义为包含边界；真实 SDK admission 必须证明返回集包含指定的已完成目标交易日。排他或含糊边界一律拒绝激活；未版本化新的 requested/returned-window lineage 与边界测试前，不得静默改为“下一日”。
- 每页 raw-first 归档，严格规范化后才创建 manifest/authority；
- 响应为空、部分、窗口外、contract mismatch 或 authority 漂移时不发布序列；
- 不自动调用 Alpaca 补洞。

typed 对象 I/O 停止码按 Job 重试权限映射：日线子 Job 与盘中 `never/1` 路径终态化；
日线聚合 publisher、SEC/FRED 和显式 Alpaca 日线路径保留精确码进入 automatic-safe
重试。在对象写发生于任何数据库变更之前的 helper 中，不得用无意义的 rollback
异常覆盖精确码。

同一 XNYS session、daily profile 和 market source 只有一个逻辑 root。Scheduler
先以该逻辑身份取 PostgreSQL transaction advisory lock，再从持久 Job payload
查找并严格复用已有 root；只有不存在时才读取当前 Universe/Provider authority
并首次冻结。Authority 在同一交易日内从 active 变为 expired/disabled 或替换
不得新建 root 和整套下游 DAG；未来重跑必须引入显式、受控的 generation，
不能借当前 authority 漂移隐式触发。

`0032_ticker_alias_authority` 把 ticker 的时点身份提升为数据库合同：同一时点一个
instrument 只能有一个 ticker，同一大小写不敏感 symbol 也只能属于一个 instrument；两个
`NOT DEFERRABLE` GiST exclusion constraint 在并发写入时同样生效。迁移前的歧义使升级原子
失败，Provider Worker 启动时精确证明约束名称、定义、验证和索引状态。日线 materializer
另外只接受 `^[A-Z][A-Z0-9.-]{0,14}$` 的规范美股 ticker，不做静默大写或字符替换；非规范、
多义或缺失 alias 统一生成 `FROZEN_INSTRUMENT_ALIAS_INVALID` terminal skip，不能让 payload
校验异常把 root 留在无审计的重试循环中。
0032 downgrade 还纳入 broker worker heartbeat 与 actionable
`broker.simulation.dispatch`；broker heartbeat/reclaim/claim 在同一 READ COMMITTED 事务中
持共享 admission lock 并验证 exact head，防止约束移除后模拟券商从歧义 alias 任选 symbol。

Fosun 的 bars 是单标的读请求，网关保守上限为 30 次/60 秒，因此日线 Universe 的
硬准入上限是 49 标的；第 50 个标的在创建任何外部读取子 Job 前以
`FROZEN_UNIVERSE_TOO_LARGE` 终止。合格 Universe 也不得在单个 45 秒 handler 中串行抓取。
调度器为每个标的创建最多一次
外部读的有界子 Job，按不高于 10 次/分钟的节奏错峰，为分配给 market lane 的
24 次/分钟上限中预留最多 14 次/分钟盘中读预算。
数据库全局 call gate 必须先完成 alias、Universe 和 Provider authority 的可能慢复核，再在
紧邻真实 Gateway 调用前提交 one-shot reservation；七秒节流以该 reservation/真实 call 时点
为准。复核失败不占用 reservation，已提交 reservation 则保证该 child 不会再次外调。
盘中快照 Job 优先级为 290，高于日线子 Job/聚合 Job 的 280、低于券商安全任务的 300；即使默认只有一个 Provider Worker，连续就绪的 49 个日线子 Job 也不得使 15 秒快照超过 45 秒有效窗口。
每个 45 秒、`never/1` 的日线子 Job 从 handler 入口建立最晚第 44 秒的内部总截止点，
为最终 staging 保留 3 秒、为独立 raw 归档保留 6 秒，Gateway fetch 本身最多 10 秒。
preflight、节流、authority 重验、reservation 与真实外调共享一次性 acquisition 预算；
已完整收到的响应在 acquisition 或 permit-exit 异常时仍先由独立 session 归档，再以稳定
deadline 码终态化，不进行第二次 Provider 读取。
每个 `fosun_daily_instrument_v2` 子 Job 和 `fosun_daily_publish_v2` 聚合输入还冻结 ticker alias ID、symbol 及其完整行身份摘要；网关请求摘要与归一化均使用该冻结 symbol，外调前和聚合前重验当日 ticker 权威，多义或身份漂移零发布。
子 Job 只归档 raw evidence 并写入不可变 staging；只有全部预期标的子 Job 成功、
Universe/日历/Provider authority 在聚合前后均与冻结输入一致时，聚合 Job 才能在
单一数据库事务中发布全部 manifest/bar/authority。任一部分失败、缺失、重复或
hash 不符都是整批零发布。发布事务的锁序是 capability scope、status、按 UUID 排序的所有
raw artifact；`price_series_manifests` 的 `BEFORE INSERT` 按名称先执行 binding guard
（status → 新 normalized artifact），再执行 `trg_price_series_manifests_z_fosun_transport`
（raw artifact → calendar）。该触发器名称顺序属于数据库合同。全部 normalized object prepare
完成后，聚合器取 PostgreSQL `clock_timestamp()`、Worker 时钟和最新 staging 时点的最大值，
在持锁状态下再次复核 authority，并把这个全批唯一时点写入所有 manifest 的
`provider_authorized_at`；任何更早的历史 as-of 都必须看不到该批中的任一标的。

## 11. API 与页面

现有 Market API 字段足以表达 `source/feed/delivery_mode/quality/quality_reasons`，不新增交易含义。页面必须：

- 复星显示“复星 L1（只读）”，不再显示泛化英文标签；
- `delivery_mode=unknown` 显示“实时性未获证明”；
- 无 bid/ask 时显示“盘口未提供”，而不是笼统的数值错误；
- 质量 reason 映射为可读中文，同时保留机器码用于审计；
- 显示 gateway evidence 的接收时点，不把它标成 Provider 成交时点；
- 历史证据和图表摘要显示复权口径；`raw` 常驻提示不得直接用于跨期收益、动量或回撤判断；
- 无合格主源且 Alpaca disabled 时显示 unavailable，不显示 fixture 或陈旧数据作为替代。

页面和 API 只读；盘中事实仍不进入硬风控或订单创建。Agent 只能消费通过 authority v2 冻结的日线集合。

## 12. Alpaca 客观触发矩阵

触发条件只生成 `ALPACA_ELIGIBLE` 运维建议，不能自动改配置、读取 secret、发请求或购买订阅。

| 场景 | 客观条件 | 允许动作 |
| --- | --- | --- |
| 复星 bars 能力不足 | 未准入、过期、权限缺失、历史深度不足或合同无效 | 评估显式启用 Alpaca 日线 |
| 复星盘中不可用 | 正常交易时段连续超出质量/SLA 窗口，且复星 capability/health 失败 | 评估有界 Alpaca Snapshot |
| 独立验证 | 复星 contract hash 变化、恢复或出现数据争议 | 开启有时限双源验证，不自动切主 |
| SIP 需求 | 用户明确需要全市场 SIP，IEX/复星 L1 不满足 | 先确认 entitlement、费用和预算 |
| 双源冲突 | 日线超过 `max(tick,20bps)` 或盘中超过 50bps | 阻断/告警，不以冲突本身自动切源 |
| 券商事实故障 | 任意账户/持仓/订单错误 | Alpaca 永不作为 fallback |
| SEC/FRED | 独立来源健康 | 与 Alpaca 市场开关无关 |

Alpaca 首次真实启用继续遵循既有有界 probe：1 symbol、最多 2 GET、最长 10 分钟，并要求 entitlement、费用、保留、RLS、停止与回滚证据。
此外，启用前必须为 Alpaca 盘中单次读取补齐与 Fosun 同等的 handler-owned
acquisition/raw/archive/ingest 分阶段截止证据，并通过精确停止码与取消回归；
当前默认关闭状态不构成该生产准入证据。

## 13. 失败语义

- 配置缺失：启动失败，零外部请求；
- Fosun admission 缺失/过期：不调度或安全跳过；
- pre/post authority 漂移：仅归档 transport evidence，零业务事实；
- unknown delivery：可展示 partial，不进入 Agent/订单 authority；
- quote 无 Provider timestamp：标记 gateway receipt-only，不计算 ready；
- bars/quote payload 无效：保留受控 evidence，整页业务事实拒绝；
- 复星失败且 Alpaca disabled：unavailable/abstain；
- 任何 Agent 输出：不能改变 source policy、capability、风控或订单状态。

## 14. 验收条件

### 14.1 安全与组合

- broker/provider current/previous key × operation 授权矩阵测试通过；越权 404 且 transport 零调用；
- actor/request ID 篡改导致 HMAC v2 失败；v1 请求拒绝；
- 未来时间戳、每次生产启动后 61 秒内请求、墙钟回拨、重放 nonce、第二个持锁进程和非单副本配置均失败；
- 非 production read_only gateway 拒绝启动；
- Provider Worker 不含 broker external-ID key，broker 不含 market-only key，app/general worker 两者都没有；
- Provider 角色无需 authority 输入的 `UPDATE` 权限，不能直接写 publication mark 或执行其最小 definer；startup probe 对 advisory-lock、`PUBLIC`/column/sequence ACL、definer 与全部 Provider 写入 trigger 边界的任何漂移 fail closed；
- startup probe 还必须拒绝 status/capability mutation lock、capability append-only 或其函数摘要的禁用、改名与漂移，并拒绝非 `READ COMMITTED` 会话；
- Fosun production 配置不要求或构造 Alpaca client；Alpaca disabled 零访问 secret。
- root/httpx/httpcore 即使被调到 DEBUG/INFO，真实 FixedOrigin FRED 请求的 key/query/URL/header 仍不得出现在日志；自有脱敏结果日志仍可见。
- Provider Worker 被明确纳入行情真实性 TCB；它的凭据域不能访问 broker/order 写面，疑似 compromise 时按运行手册人工停机，且不声称现版能自动检测或撤销已冻结事实。

### 14.2 合同与证据

- symbol、集合、bars 窗口/排序/数量/OHLC/volume 的正反例通过；
- gateway 精确响应字节被 content-addressed 保存，schema/source 正确；
- 对 DataArtifact 全身份的每一字段变异和只在 query/substring 声称对象 key 的 locator 均零事实发布；
- authority 在调用前创建时间、调用中失效、响应后 version/hash 漂移都 fail closed；
- 漂移路径只增加 raw artifact，LatestQuote/manifest/authority 为零；
- Fosun fact/manifest 缺任一 binding 或 hash 不符时，应用和直接 SQL 都失败；
- v1 authority 可重放，v2 Fosun authority 摘要覆盖完整 binding，迁移 upgrade/downgrade 测试通过。

### 14.3 数据与产品

- Fosun quote 仅产生 snapshot-last，bid/ask/spread 保持 null；
- receipt-only timestamp 与 unknown delivery 必须 partial；
- minute bar 只有完整结束后可见；Alpaca 非分钟 bucket 即使返回 bar 也必须丢弃；无前视、无跨源字段拼接；
- Fosun/Alpaca 分钟栏的 `revision` 都必须等于该批精确 raw SHA-256；不同 raw、相同 OHLCV 不得归并为同一 authority；
- 49 标的日线聚合的 `provider_authorized_at` 全部相同、不早于对象准备和 staging，发布前 as-of 返回空；
- 有效复星是第一来源；Alpaca disabled 时不出现自动 fallback；
- Agent 仅接受满足 v2 且 delivery 已证明的干净日线；否则 abstain；
- API、页面、OpenAPI、类型生成、可访问性和窄屏 E2E 通过。
- raw 日线的复权口径和跨期判断警示保持可见，不能只存在于 API 字段中。

### 14.4 完成证据

- Python offline suite、PostgreSQL integration suite、gateway suite、Web unit、lint、typecheck、build 和 E2E 全部通过；
- 独立代码审查与安全审查无未处理的 P0–P2，且无当前范围内 actionable finding；
- secret scan 不含真实 key、凭据内容或认证文件；
- 工作树只含目标改动并完成本地提交；
- 不执行真实 Provider 调用、部署或任何订单。

## 15. 回滚

- 运行回滚：保持 `NEXUS_INTRADAY_MONITOR_MODE=disabled`，停止 provider profile；普通 app 不受影响。
- 来源回滚：把 production market mode 置为 disabled/fixture 需要显式运维变更；不能自动切到 Alpaca。
- 数据库回滚：只允许按当前 migration head 逆序执行；0031 先取得与调度和 claim 事务共享的独占 advisory lock，再依序对 `worker_heartbeats`、`jobs` 取 `ACCESS EXCLUSIVE`，关闭绕过 advisory 协议的直接 SQL writer 竞态；在存在近期 Provider heartbeat 或 actionable v2 intraday Job（`queued`/`running`/`failed`）时以 `P3D01` 拒绝；不可重试的 `dead_letter` 是终态历史记录，不会永久锁死回滚，但回滚前仍必须先停止 Worker；0030 先对 staging 取 `ACCESS EXCLUSIVE`，在同一事务中检查数据/runtime 授权并保持到 DROP，任一存在都 fail closed；0029 在存在 transport lineage/freshness 引用时、0028 在存在 Fosun v2 事实或受限 Provider writer 时、0027 在存在 v2 Fosun 事实/authority/引用时分别 fail closed。历史不可变证据不删除，也不能跳过中间 guard。
- 代码回滚：revert 本次提交；不修改历史迁移或 Git 历史。

## 16. 实施裁决

1. **Gateway transport evidence 而非 Provider raw。** Provider raw 不跨隔离边界，代价是无法在 Engine 内独立重放 SDK 私有原始对象；gateway contract 和 fixture 承担反腐层验证。
2. **HMAC v2 直接替换未激活的 v1。** 代价是所有内部 client/fixture 必须同步升级；收益是 principal 与 request correlation 真正受签名保护。
3. **unknown 先展示、Agent 继续 abstain。** 代价是首次本地接线后不会立刻产生 Agent 可用价格；收益是不把未证明实时性误当作交易权威。
4. **不自动 fallback。** 代价是主源故障时页面可能 unavailable；收益是不会在未批准 entitlement、费用或数据合同下悄悄切换 Provider。
5. **Provider Worker 是数据真实性 TCB。** 当前隔离防止行情进程接触账户/订单能力，但不声称抵御已攻陷的行情写入进程；扩大到该威胁需要新的响应 attestation 与独立 writer 设计，不在本阶段暗中引入新密钥域。
