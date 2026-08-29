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

## 本地环境文件

```bash
cp infra/env.example infra/.env.local
```

`infra/.env.local` 被 `.gitignore` 覆盖，仍只能填本地 fixture 值，不得填真实 Provider 密钥、账户资料或生产证书。必填域：

- 基础设施：`POSTGRES_*`、`MINIO_ROOT_*`；
- Engine：`NEXUS_ENV=development`、`DATABASE_URL`、全部 `OBJECT_STORE_*`、Engine HMAC current key ID/secret；
- Agent：`AGENT_RUNTIME_MODE=fixture`；默认不需要任何 `OPENAI_*`；
- 行情/财报：`NEXUS_PROVIDER_RUNTIME_MODE=fixture`；普通 `app` profile 不接收
  Alpaca/FRED 密钥或 `NEXUS_INTRADAY_*` 配置，也不会发起外部数据请求；
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

`object-store-init` 只操作配置的单一 bucket；development/test 缺失时创建，随后启用并复核对象版本控制，production 则要求 bucket 与版本控制均已预置。重复执行应返回 `existing`。Provider 原始证据采用内容寻址键与 `versionId`，同一摘要只能复核，不能覆盖。

## 可选的真实美股只读数据

真实美股行情、SEC 财报与 FRED/ALFRED 宏观数据属于独立的 `provider`
profile，不属于默认本地启动流程。启用前需要在批准的密钥域注入
`ALPACA_API_KEY_ID`、`ALPACA_API_SECRET_KEY`、`FRED_API_KEY`，并配置
`ALPACA_DATA_FEED`、`ALPACA_DELIVERY_MODE` 与带联系方式的 `SEC_CONTACT`；
不要把值写入仓库、命令历史或日志。

先确保数据库迁移和 object-store init 已完成，并为 Provider 创建独立的非
superuser、非 `BYPASSRLS` 数据库角色。`infra/postgres/provider-runtime-grants.sql`
是待审批的授权模板：它不创建角色或凭据，只授予固定表/列权限，并对共享 Job 表启用
强制 RLS。受控 policy 函数把父 daily root 的只读可见性与 Provider 子任务的写权限分开，
startup probe 会校验 policy 的实际 `USING/WITH CHECK` 谓词及函数定义，而不只检查名称。
执行该模板、创建角色及注入 `PROVIDER_DATABASE_URL` 都属于权限变更，不能
在普通本地启动中自动执行。

生产数据模式仍只保留一个由通用 worker 管理的 `daily.pipeline` 根。激活时，通用
worker 使用原 `DATABASE_URL` 并显式设置 `NEXUS_PROVIDER_JOBS_ENABLED=true`，只负责将
两个 Provider 子任务写入 `provider` 队列；Provider-only worker 使用独立
`PROVIDER_DATABASE_URL` 领取这些子任务。不能停止通用 worker，也不能让 Provider
worker创建第二个日任务根。以下仅是批准后的启动形态，不是本地默认命令：

```bash
NEXUS_PROVIDER_JOBS_ENABLED=true docker compose --env-file infra/.env.local \
  -f infra/compose.yaml --profile app --profile provider up -d worker provider-worker
```

`provider-worker` 强制 `NEXUS_PROVIDER_RUNTIME_MODE=production`，只开放三个固定 HTTPS
源：Alpaca bars、SEC company facts/submissions、FRED/ALFRED 的 8 个白名单序列。
它不会加载复星交易网关或 broker 写能力。Provider 数据库角色只能读写
`provider` 队列及只读查看其 `daily.pipeline` 父任务，并在进程启动时复核精确权限、
强制 RLS、策略实际谓词和受控函数定义。每日 Job 只读取调度时冻结并绑定哈希的
美股 universe（上限 100 个）；不存在、哈希不匹配、数据冲突或证据不完整时均跳过，
不会临时扩展股票池。

分钟级 Snapshot 是这个独立 profile 中另一项默认关闭的能力。跟踪模板固定为
`NEXUS_INTRADAY_MONITOR_MODE=disabled`、15 秒 bucket 和最多 30 个标的；仅当
`ALPACA_INTRADAY_FEED` 与 `ALPACA_INTRADAY_DELIVERY_MODE` 构成合法 authority 时才可能通过
配置校验。不要在普通本地开发中把 mode 改为 `production`。任何真实数据激活、凭据注入
或有界探测都先按[分钟级盘中市场监控激活手册](intraday-market-monitor.md)冻结证据并获得精确批准。

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
