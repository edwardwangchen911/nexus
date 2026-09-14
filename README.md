# NEXUS

NEXUS 是面向单一私人用户的本地优先投资工作台，用于美股研究、证据管理、确定性回测、
方案比较和内部模拟。它不提供投资建议，也不承诺收益。

项目的核心原则是：研究结论必须能追溯到冻结证据，业务判断由确定性代码执行，读取可用、
研究合格与交易许可分别判定。页面显示正常或研究任务完成，都不会自动解除执行锁。

> 当前开发进度、真实验收结果和未闭合缺口，请查看
> [核心证据 checkpoint](docs/plans/2026-09-02-core-evidence-checkpoint.md)；提交状态以
> [GitHub Actions](../../actions) 为准。README 不复制易过期的账户数量、数据批次或单次运行结果。

## 能做什么

- 在 `offline_fixture` 下运行无外部网络依赖的研究、回测、Shadow 和内部模拟流程。
- 在 `local_read_only` 下读取并持久化已授权的账户、持仓、现金、行情和研究证据。
- 通过冻结快照、来源引用、可用时间和质量标记保留研究审计链。
- 通过 Python Engine 执行投资口径、风控、任务状态机和幂等控制。
- 通过同源 Web BFF 展示组合、市场、研究、策略、模拟和风险审计状态。

## 明确边界

- `00656.HK` 仅作只读持仓展示。
- `local_read_only` 不包含自动调度、账户写入、券商模拟盘或实盘下单。
- Agent 只能生成研究和策略意图，不能下单、修改硬风控、读取交易密钥或绕过人工审批。
- 缺失、过期、未对账或不可比较的数据保持未知，不以零值或推断结果代替。
- 所有实盘能力默认关闭；未来任何真实订单都必须逐笔人工确认。
- Web 页面和刷新只读取已持久化数据，不暗中调用券商、数据源或模型。

## 架构

```text
浏览器
  │ 同源请求
  ▼
Vinext Web / BFF ── HMAC ──► Python Engine ──► PostgreSQL
                                  │            └─► MinIO / S3 证据原件
                                  ├─► Agent / Provider Workers
                                  └─► 独立 Fosun Gateway（只读能力白名单）
```

浏览器不直接访问 Engine，也不持有服务签名密钥。Engine 是业务状态和确定性规则的所有者；
复星 Gateway 使用独立身份与密钥域，不能与 Web BFF 复用凭据。架构决策见
[PostgreSQL 所有权](docs/adr/0001-postgresql-owner.md)和
[复星 Gateway 边界](docs/adr/0002-fosun-gateway-boundary.md)。

## 目录

| 路径 | 用途 |
| --- | --- |
| `app/` | Vinext 页面、组件和同源 BFF |
| `engine/` | Python 3.12 Engine、领域规则、任务和迁移 |
| `integrations/fosun_gateway/` | 隔离的复星只读 Gateway |
| `contracts/` | OpenAPI 与生成客户端合同 |
| `infra/` | Compose、容器镜像和本地环境模板 |
| `tests/policy/` | 仓库、依赖和安全边界测试 |
| `docs/` | ADR、设计、阶段记录和运行手册 |

## 本地启动

### 前置条件

- Node.js `>=22.13.0`（CI 使用 22.19）与 npm
- Python `>=3.12,<3.13` 与 uv
- 提供 Docker API 和 Compose v2 的运行时，例如 Docker Desktop、OrbStack 或 Colima

先安装锁定依赖；仅在配置文件不存在时创建本地 fixture 配置，已有配置应保留：

```bash
npm ci
uv sync --frozen --all-packages
if [ ! -e infra/.env.local ]; then
  cp infra/env.example infra/.env.local
fi
```

不要在 `infra/.env.local` 中填写真实 Provider 密钥、账户资料或生产证书。按文件注释填写
fixture 配置后，校验并启动 PostgreSQL、MinIO、Engine API 和通用 worker：

```bash
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app config --quiet
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app up -d postgres minio
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app run --rm object-store-init
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app run --rm --no-deps api alembic -c engine/alembic.ini upgrade head
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app up -d api worker
```

Web 服务端需要与 Engine 相同的本地 BFF 身份配置；完成配置后启动：

```bash
npm run dev
```

完整配置字段、端口调整、readiness 检查、停止和故障处理见
[本地开发 runbook](docs/runbooks/local-development.md)。普通开发不要启用 `broker` profile。

已有本地 PostgreSQL、MinIO、复星日线配置及受保护的 Schwab 授权时，可只把盘中来源切到
Schwab。先通过上述 Compose 入口迁移到当前 head，再在 `infra/.env.local` 设置
`NEXUS_PROVIDER_RUNTIME_MODE=production`、`NEXUS_MARKET_PROVIDER=fosun`、
`NEXUS_INTRADAY_PROVIDER=schwab` 和 `NEXUS_INTRADAY_MONITOR_MODE=production`，然后启动：

```bash
cd engine
uv run nexus-engine-provider-worker-local --repo-root "$PWD/.."
```

该入口复用现有本地 PostgreSQL 与 MinIO，不要求额外身份或 Compose profile；按一次
`Ctrl-C` 即可停止。闭市启动只验收日历抑制，不代表 Schwab 行情采集成功。配置、迁移和
持久化 Job 核对命令见[盘中市场监控手册](docs/runbooks/intraday-market-monitor.md)。

内部 Agent 模拟周期是默认关闭的单次本地入口。当前最小真实路径使用 Schwab Level One 冻结
AAPL 决策报价，以前一交易日冻结日线执行确定性 sizing 和硬风控，再用 Schwab price history
五分钟窗口驱动内部模拟成交和账本；它不连接 Schwab 或其他券商下单域。盘前 07:00 ET 起可
手工验收，不必等正式开盘；
启用、持久状态恢复、单次模型 worker、只读核对、停止与回滚见
[Agent 内部模拟周期手册](docs/runbooks/agent-paper-cycle.md)。

## 验证

前端与合同：

```bash
npm run lint
npm run typecheck
npm test
npm run check:generated
npm run build
```

Python Engine：

```bash
uv run pytest engine/tests -q --import-mode=importlib
uv run ruff check engine
uv run mypy engine/src
```

复星 Gateway 的离线测试使用 [CI 手册的本地复现命令](docs/runbooks/ci-release-gates.md#本地复现)，
包含网络 guard 和仓库内受保护的 pytest 临时目录；不要替换为公共 `/tmp` 目录，
以免触发安全路径校验。静态检查：

```bash
uv run ruff check integrations/fosun_gateway scripts/offline_guard
uv run mypy integrations/fosun_gateway/src
```

浏览器合成 E2E 使用 `npm run test:e2e`。真实本地 fixture E2E 需要调用者提供专用的空
PostgreSQL 数据库，具体限制见本地开发 runbook；它不会读取真实账户或 Provider 数据。
CI 的完整门禁及发布含义见 [CI 与离线发布门禁](docs/runbooks/ci-release-gates.md)。

## 运行模式与外部接入

| 模式 | 网络与数据 | 允许范围 |
| --- | --- | --- |
| `offline_fixture` | 无真实外部数据 | 开发、测试、确定性回测与内部模拟 |
| `local_read_only` | 仅显式启用的只读来源 | 本地采集、持久化、研究与展示 |

真实数据源、账户同步、模型调用和模拟盘分别有独立准入，不因基础栈启动而自动启用：

- [复星只读激活](docs/runbooks/fosun-read-only-activation.md)
- [复星 SDK 准入](docs/runbooks/fosun-sdk-admission.md)
- [Alpaca 历史价格](docs/runbooks/alpaca-prices-local.md)
- [新闻本地采集](docs/runbooks/news-local.md)
- [盘中市场监控](docs/runbooks/intraday-market-monitor.md)
- [内部模拟单次运行](docs/runbooks/nexus-paper-once.md)
- [Agent 内部模拟周期](docs/runbooks/agent-paper-cycle.md)
- [券商模拟激活](docs/runbooks/broker-simulation-activation.md)

任何包含凭据、外部请求或业务写入的操作，都应从对应 runbook 的正常产品入口执行；不要把密钥
放进命令行、聊天、日志、测试快照或 Git。

## 开发约定

- 接口变更后运行 `npm run generate:client`，并提交生成结果；CI 用
  `npm run check:generated` 检查漂移。
- 数据库迁移位于 `engine/alembic/`，不得改写已应用的历史迁移。
- 风控、回测、交易状态机和财务口径变更必须有对应回归测试。
- `/health/live` 只证明 API 进程存活，不代表数据库、对象存储、worker 或业务数据就绪。
- `DISPLAY_READY`、`DECISION_READY` 与 `execution_locked` 是不同状态；前两者不会自动授权交易。
- 应提交源码、测试、迁移、依赖锁文件、生成客户端、无敏感信息的配置模板和通用文档；
  本地环境文件、凭据、账户原始数据、数据库备份、受保护验收材料、运行产物和缓存留在本地。
  `.gitignore` 不会自动移除已经跟踪的文件，提交前仍需检查暂存差异。
- 删除 worktree 或分支前，确认没有活跃写入、未提交改动及尚未合入或可靠备份的独有提交。
  GitHub 保存代码历史；业务数据库和证据对象按[备份恢复手册](docs/runbooks/backup-restore.md)单独备份。

更完整的产品边界和协作规则见 [AGENTS.md](AGENTS.md)，当前迭代恢复入口见
[核心迭代交接](docs/plans/2026-09-02-nexus-core-iteration-handoff.md)。
