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

`object-store-init` 只对配置的单一 bucket 执行 `HEAD`；development/test 缺失时创建并再次验证，production 缺失时直接失败。重复执行应返回 `existing`。

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
