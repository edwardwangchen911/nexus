# NEXUS

NEXUS 是面向单一私人用户的本地优先资产 Agent 管理平台。它把真实资产事实、市场与公司证据、
AI 研究、确定性风控、策略比较和内部模拟统一到一条可追溯的决策链中，帮助用户回答：现在持有
什么、证据是否足够、有哪些可比较选择，以及一个想法在承担真实风险前表现如何。

NEXUS 不提供投资建议或收益承诺，也不是无人值守交易机器人。研究、展示、模拟验证与执行许可
始终分开；实盘默认关闭，未来任何真实订单仍须逐笔人工确认。

> 当前迭代方向见 [Agent 模拟与策略进化路线图](docs/plans/2026-09-12-nexus-simulation-evolution-roadmap.md)，
> 已完成和待验收证据见 [核心证据 checkpoint](docs/plans/2026-09-02-core-evidence-checkpoint.md)。
> README 只描述稳定能力和边界，不复制容易过期的账户数量、运行批次或提交状态。

## 为什么自建

券商、行情终端和通用 AI 各自解决一部分问题，但不会天然形成属于个人的长期资产决策记录。
NEXUS 的价值不在于重复 TradingView 或券商交易界面，而在于连接它们之间缺失的部分：

- 将持仓、现金、订单、行情和研究证据保存在用户控制的本地环境中。
- 保留来源、实际可用时间、版本和质量标记，避免用今天的信息改写过去的判断。
- 让 AI 负责研究与解释，让确定性代码负责资金、仓位、风控、幂等和账本。
- 在内部模拟中持续比较 Agent、持有不动基线和市场基准，再决定是否值得进一步验证。
- 允许“保持现状”“等待证据”和“不可比较”成为正式结果，而不是强迫系统给出买卖建议。

## 产品闭环

```text
只读资产与市场来源
        │
        ▼
持久化事实与冻结证据 ──► 组合诊断与 Agent 研究
        │                         │
        │                         ▼
        │                 情景、策略与回测比较
        │                         │
        ▼                         ▼
确定性仓位与硬风控 ──► 内部模拟订单、成交、账本与绩效
        │                         │
        └──────────────► Web 只读页面 / Bark 脱敏通知
```

浏览页面只读取已经持久化的事实，不会暗中刷新券商、调用模型或创建订单。有界内部模拟可以在
固定策略、资金和风控范围内运行，但不会因此获得券商写入或实盘权限。

## 当前能力与成熟度

| 能力 | 当前状态 | 仍需注意 |
| --- | --- | --- |
| 资产与市场事实 | 已有本地只读接入、持久化、来源时间和质量标记 | 页面可见不等于数据新鲜或研究合格 |
| 组合诊断与 Agent 研究 | 已有可追溯任务、冻结输入、引用和结果页面 | Job 完成不自动等于 `DECISION_READY` |
| 情景、策略与回测 | 已有确定性比较、历史回测和只读展示 | 缺费用、公司行动或同口径数据时保持不可比 |
| 内部模拟 | 已有单次与有界连续周期、硬风控、订单/成交状态、账本和绩效读取 | 合理不交易是有效结果，但不能替代订单与成交路径验收 |
| 绩效对照 | 已有连续周期汇总和市场基准读取 | 短期跑赢现金或基准不证明策略有效 |
| Bark 通知 | 自托管、脱敏加密、默认关闭，只消费已提交的内部模拟事实 | 必须完成本地配置和真实 iPhone 收件后才算运行验收 |
| 券商模拟盘 | 保留独立适配和激活边界 | 尚不因内部模拟完成而自动启用，外部写入需另行授权 |
| 实盘交易 | 关闭 | `DISPLAY_READY`、`DECISION_READY` 或模拟成绩都不能解除执行锁 |

### 当前迭代位置

- **已真实验收：** Agent 基于已持久化证据完成合规 no-trade 闭环和同日多次独立决策，结果可从
  API、BFF 和 `/paper` 回读。
- **已实现、待持续验收：** 有界连续运行入口、周期恢复、绩效汇总，以及 `hold_current` 和 SPY
  对照；仍需覆盖完整运行时段和更多自然样本。
- **尚未完成：** 真实 Agent 自然产生可执行订单后的订单、部分成交、取消、账本变化和长期绩效
  证据。受控测试覆盖这些能力，但不能冒充真实运行。
- **后续阶段：** 在稳定样本上验证有证据的 Agent 版本改进，再单独评估券商模拟盘；实盘继续关闭。

当前主线不需要继续扩建新的 Agent 框架，而是把已有链路运行扎实、对照清楚、失败状态诚实落盘。

## 运行模式与安全边界

| 模式 | 数据与网络 | 允许范围 |
| --- | --- | --- |
| `offline_fixture` | 不读取真实外部数据 | 开发、测试、确定性回测和内部模拟 fixture |
| `local_read_only` | 仅显式启用的只读来源 | 本地采集、持久化、研究与展示 |

- `00656.HK` 仅作只读持仓展示。
- `local_read_only` 不包含自动调度、账户写入、券商模拟盘或实盘下单。
- Agent 不能修改硬风控、读取交易密钥、绕过审批或直接下单。
- 缺失、过期、未对账或不可比较的数据保持未知，不用零值或推断结果替代。
- `DISPLAY_READY`、`DECISION_READY` 与 `execution_locked` 是不同状态；前两者不会授权交易。
- Web 页面和刷新只读取持久化事实，不触发隐藏的 Provider、模型或交易调用。

## 本地启动

### 前置条件

- Node.js `>=22.13.0`（CI 使用 22.19）与 npm
- Python `>=3.12,<3.13` 与 uv
- 提供 Docker API 和 Compose v2 的运行时，例如 Docker Desktop、OrbStack 或 Colima

安装锁定依赖；仅在配置不存在时创建本地配置，已有配置必须保留：

```bash
npm ci
uv sync --frozen --all-packages
if [ ! -e infra/.env.local ]; then
  cp infra/env.example infra/.env.local
fi
```

`infra/.env.local` 只保存非秘密配置和受保护文件路径。Provider 密钥、账户资料、签名密钥和
证书不得进入 Git、聊天、命令参数或普通日志。

本机已有 NEXUS 数据时，必须先核对 Compose project 和挂载卷；当前个人环境使用
`nexus-fosun-live`。不要省略 `-p nexus-fosun-live` 后误启默认 `infra` project 和空数据卷。
确认配置后启动基础服务：

```bash
docker compose -p nexus-fosun-live --env-file infra/.env.local \
  -f infra/compose.yaml --profile app config --quiet
docker compose -p nexus-fosun-live --env-file infra/.env.local \
  -f infra/compose.yaml --profile app up -d postgres minio
docker compose -p nexus-fosun-live --env-file infra/.env.local \
  -f infra/compose.yaml --profile app run --rm object-store-init
docker compose -p nexus-fosun-live --env-file infra/.env.local \
  -f infra/compose.yaml --profile app run --rm --no-deps api \
  alembic -c engine/alembic.ini upgrade head
docker compose -p nexus-fosun-live --env-file infra/.env.local \
  -f infra/compose.yaml --profile app up -d api worker
```

Web 服务端需要与 Engine 相同的本地 BFF 身份配置；完成后运行：

```bash
npm run dev
```

启动后从 `http://127.0.0.1:3000/workspace` 进入交易工作台；根路径 `/` 也会转到该页面。日常查看顺序为：

```text
交易工作台 → 决策记录 → 账户与交易 → 复盘
```

“循环在线”来自 30 秒内兼容的 `agent_paper` heartbeat；“最近周期已完成”来自已保存周期，二者互不
替代。页面刷新只重读已保存事实，不启动或重放 Agent，也不采集行情、调用模型或创建订单。从工作台
可一次点击进入 `/decisions/{cycleId}`，按运行窗口、证据、决策、仓位计算、风控、订单、执行结果和
后续表现核对同一周期；缺少的阶段保持“未知”。

Web 只接受 `localhost`、`127.0.0.1` 或 `[::1]` 的本机请求，使用固定本地审计身份；不依赖
Sites 登录，也不对局域网或公网提供页面。如需远程访问，必须先单独设计本地身份验证和
网络边界，不得把开发服务直接绑定到 `0.0.0.0`。

Compose 显示 `Up` 或 `/health/live` 返回 200 只证明进程存活。实际恢复还应核对数据库、迁移、
对象存储、worker、BFF 和页面读取。完整流程见[本地开发手册](docs/runbooks/local-development.md)，
普通本地使用不要启用 `broker` profile。

## 主要使用入口

### 日常工作台

- `/workspace`：默认首页，分别显示循环存活状态、最近持久化周期、最新决策、活动订单、待处理事项和内部模拟账户摘要。
- `/decisions`：读取最近最多 20 条已保存周期；筛选仅作用于本次已加载记录。
- `/decisions/{cycleId}`：读取该周期及其精确关联订单，不从其他周期补填事实。
- `/portfolio`：账户与交易；当前保留既有真实只读账户和组合事实。
- `/paper#paper-comparison-title`：复盘入口；当前保留既有内部模拟与同窗对照能力。

`/overview`、`/market`、`/optimize`、`/strategies` 和 `/risk-audit` 仍可通过原地址或上下文链接访问，
只是退出一级导航；底层模块、API、迁移和历史数据没有删除。

### 数据与研究

- [复星只读激活](docs/runbooks/fosun-read-only-activation.md)
- [复星 SDK 准入](docs/runbooks/fosun-sdk-admission.md)
- [Alpaca 历史价格](docs/runbooks/alpaca-prices-local.md)
- [新闻本地采集](docs/runbooks/news-local.md)
- [盘中市场监控](docs/runbooks/intraday-market-monitor.md)

### 回测与内部模拟

- [回测单次运行](docs/runbooks/nexus-baseline-backtest-once.md)
- [内部模拟单次运行](docs/runbooks/nexus-paper-once.md)
- [Agent 内部模拟周期](docs/runbooks/agent-paper-cycle.md)
- [券商模拟激活](docs/runbooks/broker-simulation-activation.md)

### 通知

- [Bark 自托管只读通知](docs/runbooks/bark-read-only-notifications.md)

所有入口都应先读取持久化状态。断流、进程退出、观察超时或闭市抑制不代表动作失败，也不允许
据此重复触发可能已经提交的研究、采集或模拟操作。

## 自托管 Bark 通知

NEXUS 使用运行在本机 Docker 中的 Bark Server，不把通知交给 Bark 公共服务器。通知 worker
只消费已经提交的内部模拟 outbox 事实，并通过本机 `127.0.0.1` 发送脱敏加密内容；失败不会
回滚或重跑业务动作。最终 iPhone 系统投递仍经过 Apple APNs。

首次注册时，iPhone 需要在同一受信任局域网临时访问 Mac 的明确 LAN 地址。注册完成后 Bark
Server 恢复为仅本机访问，iPhone 此后可在蜂窝网络或其他 Wi-Fi 下正常收件，不需要继续留在
同一局域网。当前实现需要：

- Bark 注册产生的 `device key`，用于标识接收设备。
- 一段恰好 16 个 ASCII 字符的 AES-128-CBC 共享密钥；Mac 和 Bark App 各保存一份相同值。

两个值只能保存在 `.secrets/bark/` 的 `0600` 本地文件和 iPhone App 中，不进入 `.env.local`
或 Git。真实验收必须同时确认业务事实已提交、outbox 和 delivery attempt 已发布，以及 iPhone
实际显示解密后的通知。详细注册、健康检查、关闭和回滚步骤见
[Bark 运行手册](docs/runbooks/bark-read-only-notifications.md)。

## 架构与数据所有权

```text
浏览器
  │ 同源请求
  ▼
Vinext Web / BFF ── HMAC ──► Python Engine ──► PostgreSQL
                                  │            ├─► MinIO / S3 证据原件
                                  │            └─► Outbox ──► Bark（只读出口）
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

## 开发与验证

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

复星 Gateway 的离线验证使用
[CI 手册的本地复现命令](docs/runbooks/ci-release-gates.md#本地复现)，包含网络 guard 和受保护
pytest 临时目录。浏览器合成 E2E 使用 `npm run test:e2e`；真实 fixture E2E 必须使用专用空
PostgreSQL 数据库，不能读取真实账户或 Provider 数据。

接口变更后运行 `npm run generate:client` 并提交生成结果。数据库迁移必须新增正向 migration，
不得改写已应用历史。风控、回测、交易状态机和财务口径变更必须有对应回归。

更完整的产品边界和协作规则见 [AGENTS.md](AGENTS.md)，任务恢复入口见
[核心迭代交接](docs/plans/2026-09-02-nexus-core-iteration-handoff.md)。
