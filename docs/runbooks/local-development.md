# 本地开发与 Compose runbook

## 边界

本流程只启动 synthetic fixture 环境，用于研究、回测、Shadow、内部模拟盘和运维验证。它不安装复星 SDK，不读取真实账户，不启动任何交易写入路由。`app` profile 不包含 `fosun-gateway` 或 `broker-worker`；不要为了本地开发启用 `broker` profile。

Docker Desktop 不是必需品。任何提供 Docker API 与 Compose v2 的兼容运行时都可以，包括 Docker Desktop、OrbStack 或 Colima。

## 预检

```bash
docker version
docker compose version
uv --version
node --version
npm --version
```

前两条必须同时显示可用的 client 和 server/Compose。仅安装 `docker` CLI 而没有可达 daemon 不足以运行本项目。

## 真实本地 fixture E2E（不使用 Compose）

synthetic browser E2E 可在浏览器层替换 API 响应，以覆盖页面契约；真实本地 fixture E2E
不替换网络响应，启动 loopback Engine 与 Vinext 同源 BFF，并将状态写入调用者提供的专用空
PostgreSQL 数据库。它只验证本地软件链路，不读取或构造真实行情、账户、订单或 Provider 证据。

调用者必须先提供一个专用、空的 PostgreSQL 数据库，名称以 `nexus_e2e_` 开头。例如，使用
loopback 地址的 URL（占位符必须替换为仅用于该测试数据库的 synthetic 凭据）：

```bash
export NEXUS_REAL_E2E_DATABASE_URL='postgresql+asyncpg://<synthetic-user>:<synthetic-password>@127.0.0.1:<port>/nexus_e2e_local'
export NEXUS_E2E_BROWSER_CHANNEL=chromium
npm run test:e2e:real
```

运行器只会校验该 URL、迁移这个调用者提供的专用库并启动本地测试进程；它不会创建、删除、
清空或重置数据库。若初始状态并非空库所需的 fixture `version=0`，测试会失败而不会清理。
默认不运行 Provider、交易 Gateway、任何 worker 或 MinIO；不要把本命令视为复星 SDK admission、
复星激活、真实行情/财报、账户同步或订单可用性的证据。

## 本地环境文件

```bash
cp infra/env.example infra/.env.local
```

`infra/.env.local` 被 `.gitignore` 覆盖，仍只能填本地 fixture 值，不得填真实 Provider 密钥、账户资料或生产证书。必填域：

- 基础设施：`POSTGRES_*`、`MINIO_ROOT_*`；
- Engine：`NEXUS_ENV=development`、`DATABASE_URL`、全部 `OBJECT_STORE_*`、Engine HMAC current key ID/secret；
- Agent：`AGENT_RUNTIME_MODE=fixture`；默认不需要任何 `OPENAI_*`；
- 行情/财报：`NEXUS_PROVIDER_RUNTIME_MODE=fixture`；普通 `app` profile 不接收
  复星/Alpaca/FRED 密钥、任一网关客户端身份或 `NEXUS_INTRADAY_*` 配置，也不会发起外部数据请求；
- 端口：默认 5432/9000/9001/8000，被占用时用 `NEXUS_*_HOST_PORT` 改为其他本地端口。

`DATABASE_URL` 与 `OBJECT_STORE_ENDPOINT` 供容器使用，因此 host 分别应指向 Compose service 名 `postgres` 与 `minio`，而不是 `127.0.0.1`。

## 启动

先校验解析后的 Compose，不打印完整配置：

```bash
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app config --quiet
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile broker config --quiet
```

第二条只做静态组合校验，不启动 broker 服务。然后按依赖顺序启动 app：

```bash
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app up -d postgres minio
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app run --rm object-store-init
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app run --rm --no-deps api alembic -c engine/alembic.ini upgrade head
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app up -d api worker
```

`object-store-init` 只服务普通 `app` profile 并操作其配置的单一 bucket；development/test
缺失时创建，随后启用并复核对象版本控制，production 则要求 bucket 与版本控制均已预置。
重复执行应返回 `existing`。Provider profile 不依赖也不运行这个通用初始化器；它的独立
bucket/principal/policy 必须在单独批准的权限流程中预置。

## 可选的真实美股只读数据

真实美股行情、SEC 财报与 FRED/ALFRED 宏观数据属于独立的 Provider
profile，不属于默认本地启动流程。首选市场源固定为复星：
`NEXUS_MARKET_PROVIDER=fosun`、`NEXUS_ALPACA_MARKET_MODE=disabled`。启用前需要在批准的
密钥域注入 market-only 的复星网关 mTLS/HMAC 身份、`FRED_API_KEY` 与带联系方式的
`SEC_CONTACT`；网关自身是唯一接收 `FSOPENAPI_*` 的进程。不要把任何值写入仓库、
命令历史或日志。Alpaca 只作为另行批准的备选/交叉验证源；没有完成其 entitlement、
费用、留存和数据合同核验时，不得改为 `NEXUS_MARKET_PROVIDER=alpaca` 或启用
`NEXUS_ALPACA_MARKET_MODE=enabled`。

Compose 中 `provider` 是固定的复星密钥域，`provider-alpaca` 是固定的
Alpaca 密钥域；两者不得同时启动。切源时必须先停止旧 Provider Worker，
确认旧 lease 已释放，再让通用 worker 与新 Provider Worker 使用同一个
`NEXUS_MARKET_PROVIDER`。当前队列没有 source-bound claim，因此本拆分不声称支持并发
双源交叉验证；该能力需要未来独立的 Job/queue 协议。

先确保数据库迁移已完成，并为 Provider 创建独立的非
superuser、非 `BYPASSRLS` 数据库角色。`infra/postgres/provider-runtime-grants.sql`
是待审批的授权模板：它不创建角色或凭据，只授予固定表/列权限，并对共享 Job 表启用
强制 RLS。受控 policy 函数把父 daily root 的只读可见性与 Provider 子任务的写权限分开，
startup probe 会校验 policy 的实际 `USING/WITH CHECK` 谓词及函数定义，而不只检查名称。
Provider 对 integration status、capability、既有 artifact/manifest 等 authority 输入保持
`SELECT`-only；校验函数与输入变更 trigger 使用相同的 transaction advisory lock，而不是要求
Provider 取得 `UPDATE` 行锁权限。模板还撤销 Provider/`PUBLIC` 对两个最小 trigger-only
`SECURITY DEFINER` 的直接执行权；publication 函数只能由两个受控 `AFTER INSERT` trigger
追加回测发布标记，price-bar guard 只能由一个受控 `BEFORE INSERT` trigger 在不授予
Provider 回测表读权的前提下守住冻结事实。模板和 startup probe 还拒绝 Provider/`PUBLIC`
的意外 relation/column/sequence ACL；startup probe 会逐项校验精确函数集合、源码、owner、
ACL、精确 `search_path`，以及 Provider 可写表上 60 个非内部应用 trigger 的完整 topology
与函数体摘要。
执行该模板、创建角色及注入 `PROVIDER_DATABASE_URL` 都属于权限变更，不能
在普通本地启动中自动执行。

Provider 对象存储同样是独立权限域：`PROVIDER_OBJECT_STORE_*` 不能复用 app/general
worker 的 `OBJECT_STORE_*` principal。bucket 必须预先启用版本控制；运行时固定
`PROVIDER_OBJECT_STORE_PREFIX=provider/market`，IAM 必须拒绝 bucket 根/List 和其他前缀，
只允许该前缀内 PutObject、条件创建冲突复核用的 key-scoped GetObject 和冻结证据
重放用的 GetObjectVersion，不授予 ListBucket 或 DeleteObject。两个 Provider
profile 都不会自动运行通用 `object-store-init`；缺少上述权限证据时不得启动真实 Provider。

生产数据模式仍只保留一个由通用 worker 管理的 `daily.pipeline` 根。激活时，通用
worker 使用原 `DATABASE_URL` 并显式设置 `NEXUS_PROVIDER_JOBS_ENABLED=true`，只负责将
两个 Provider 子任务写入 `provider` 队列；Provider-only worker 使用独立
`PROVIDER_DATABASE_URL` 领取这些子任务。不能停止通用 worker，也不能让 Provider
worker创建第二个日任务根。以下仅是批准后的启动形态，不是本地默认命令：

```bash
NEXUS_MARKET_PROVIDER=fosun NEXUS_PROVIDER_JOBS_ENABLED=true \
  docker compose --env-file infra/.env.local \
  -f infra/compose.yaml --profile app --profile provider up -d worker fosun-gateway provider-worker
```

经单独批准的 Alpaca 替代形态不启动复星 Gateway，且只能在先停止
`provider-worker` 后使用：

```bash
NEXUS_MARKET_PROVIDER=alpaca NEXUS_PROVIDER_JOBS_ENABLED=true \
  docker compose --env-file infra/.env.local \
  -f infra/compose.yaml --profile app --profile provider-alpaca up -d \
  worker provider-worker-alpaca
```

`provider-worker` 强制 `NEXUS_PROVIDER_RUNTIME_MODE=production`。在默认配置中，美股日线和
盘中证据只通过复星 market-only 网关身份读取；SEC company facts/submissions 与
FRED/ALFRED 的 8 个白名单序列继续使用各自固定 HTTPS origin。Provider Worker 不接收
broker 网关身份、账户 ID 加密密钥或交易写能力，也不会构造或读取 Alpaca client；复星
Gateway 同时属于显式 `provider`/`broker` profile，但由不同的 HMAC principal 派生固定操作
权限。生产 Gateway 使用外部命名卷 `nexus-fosun-gateway-replay-v1` 保存 nonce journal
并持有 singleton lease；不得在普通 `down -v` 或清理脚本中删除该卷。在 Job 域，Provider 数据库角色只能读写
`provider` 队列及只读查看其 `daily.pipeline` 父任务；在 market 数据域，它仅能 INSERT
批准的 evidence、manifest、bar/quote、authority、filing/fact、corporate-action 与宏观事实表，
不能 UPDATE/DELETE authority 输入或直接写 publication mark。进程启动时会复核精确权限、
强制 RLS、策略实际谓词和受控函数定义。每日 Job 只读取调度时冻结并绑定哈希的
美股 universe（上限 100 个）；不存在、哈希不匹配、数据冲突或证据不完整时均跳过，
不会临时扩展股票池。

分钟级 Snapshot 是这个独立 profile 中另一项默认关闭的能力。跟踪模板固定为
`NEXUS_INTRADAY_MONITOR_MODE=disabled`、15 秒 bucket 和复星最多 10 个标的。页面把复星
delivery `unknown` 明确显示为“实时性未获证明”，缺失 bid/ask 时显示“盘口未提供”，不会
合成价差或交易信号。不要在普通本地开发中把 mode 改为 `production`。任何复星真实数据
激活、凭据注入或有界探测都先按[复星只读激活手册](fosun-read-only-activation.md)冻结证据并
获得精确批准；可选 Alpaca Snapshot 仍需另按[分钟级盘中市场监控激活手册](intraday-market-monitor.md)
完成独立 admission，且不会被自动 fallback 触发。

## 验证

```bash
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app ps -a
curl --fail --silent http://127.0.0.1:8000/health/live
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app run --rm object-store-init
```

如果在 `infra/.env.local` 改写了 `NEXUS_API_HOST_PORT`，上面 `curl` 也应使用同一个显式端口；`--env-file` 不会把变量注入当前 shell。

`/health/live` 只证明 API 进程存活。真正的 `/health/ready` 受 HMAC + nonce 保护，同时检查 PostgreSQL、唯一 Alembic head、bucket 和 30 秒内的通用 worker heartbeat。通过同源 Web BFF 的 `/api/v1/readiness` 检查，不要在命令行手工粘贴签名 secret。当前组件状态也可在本地“风险与审计”页面查看。

如果 readiness 不是 `ready`，按 `database -> migration -> object_store -> worker` 的顺序查询脱敏组件状态；不要通过关闭检查或伪造 heartbeat 绕过。

## 停止与精确清理

```bash
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app stop api worker
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app down
```

`down` 默认保留 volume。只有确认该 Compose project 中的 fixture 数据可删除时，才可针对该精确 project 另行执行 volume 清理；不得清理其他 NEXUS 容器、volume 或工作树。

## 常见失败

- 端口冲突：只改 `NEXUS_*_HOST_PORT`，容器内 service URL 不变。
- init 失败：检查六个必填值和 MinIO health；CLI 故意不打印 Provider 错误详情。
- migration `incompatible`：在当前代码镜像重跑 `upgrade head`，不手工改 `alembic_version`。
- worker `stale`：确认 worker 正在运行且使用同一数据库/迁移 head；不直接写 heartbeat 表。
- Provider Job `FROZEN_UNIVERSE_*`：先发布完整 universe，再等待下一次调度；不要手工改 Job payload。
- Provider evidence `*_CONFLICT`：保留现有版本与对象，停止自动重试并核对上游修订；不要覆盖同摘要对象。
