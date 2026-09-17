# 复星模拟盘 Phase 3B 激活手册

## 2026-09-17：独立模拟账户只读准备入口

本节覆盖后文历史记录中“模拟接入只有 fixture”的描述，但**不激活 Phase 3B 写入**。
新增 `nexus-fosun-simulation-readonly` 一次性 CLI；原模拟 Gateway、Broker Worker、
数据库、main 本地服务及现有 fixture 组合根不变。不创建或重置模拟账户、不注册 Webhook、
不发送下单/改单/撤单，不调用可能解锁 binding 的 reconciliation。输出恒为
`simulation_write_enabled=false`、`execution_locked=true`。

### 合同与实现范围

- 官方 [trade OpenAPI](https://openapi-docs.fosunxcz.com/specs/public/trade/openapi.yaml)
  的 `mapping.AccountInfo.subAccountType`：0 为实盘证券、1 为实盘期权、2 为模拟盘。
  读取前必须由本次 Accounts 原始响应唯一匹配选定账户，且严格为整数 2、启用状态整数 1。
  缺失、重复、字符串枚举、未知类型或停用都在资产/订单调用前拒绝；不凭配置声明模拟身份。
- 沿用锁定的官方 SDK commit `891d95fb4967efaa682387c1b35966ceea38fb21`，启动核对
  安装包 direct-url commit 和已有 SDK admission manifest；只暴露 Accounts、CashSummary、
  Holdings、OrderList、CashFlows 五个固定读取路径，低层请求也拒绝写路径、其他账户和代理 URL。
- 复用 `SdkFosunTransport`、`GatewayPage` 与账户/现金/持仓/订单/流水归一化模型、TLS 强制验证、
  关闭重定向/HTTP 重试和依赖日志脱敏。独立进程、独立 SDK Session；不复用真实账户的会话缓存。
- 订单省略状态过滤，明确给出日期范围；账户和资产读取当前状态，订单/流水读取指定日期窗口，
  **不称为原子快照或已对账快照**。逐页保留 request ID、读取时间、币种、质量与未知 provider 时间。
- 窗口最多 32 个自然日（含首尾）；持仓/订单各最多 3 页、每页 100 条。CashFlows SDK 没有分页
  入参，只调用一次；响应不能证明完整时拒绝，不用虚构游标重复调用。最多 9 次业务请求、
  1 次 Session 请求、120 秒物理调用准入窗口；失败不自动重试，读取实例不可重复执行。
- 2026-09-17 官方 trade YAML SHA-256 为
  `5bda8d6a25532ed414fdac283686a2dff51b6580b6a2d8b0e06d0c4f5764d7a0`，
  与既有只读 admission manifest 的历史合同 hash 不同。本次核对上述读取方法与模拟类型字段，
  不改写历史 manifest，不声称整个新版合同或任何写契约已获准入。

### 配置与正常入口

默认只检查 `~/.config/nexus/fosun-simulation/provider.env`，不读取 main 配置或回退到
环境中的 `FSOPENAPI_*`。文件必须位于 Git 工作树外、无符号链接、当前用户所有，目录无组/其他
用户权限、文件建议 `0600`。只使用既有模拟专用配置；缺失时停止，不自动复制真实账户凭据。
配置只接受以下键（真实值不得进入命令参数、聊天、Git 或日志）：

```dotenv
FOSUN_SIMULATION_API_KEY=<simulation-only credential>
FOSUN_SIMULATION_CLIENT_PRIVATE_KEY="<PEM; dotenv quoted multiline supported>"
FOSUN_SIMULATION_SERVER_PUBLIC_KEY="<PEM>"
FOSUN_SIMULATION_ACCOUNT_ID=<existing simulation subAccountId>
FOSUN_SIMULATION_WRITE_ENABLED=false
FOSUN_SIMULATION_EXECUTION_LOCKED=true
```

SDK construction 只在这个独立 CLI 进程中临时映射密钥变量，完成后立即恢复；不是验收脚本注入。
HTTPS 使用现有审核过的 loopback proxy `http://127.0.0.1:9090`，无代理回退；不启动或修改代理。

```bash
# 无网络配置检查；configuration_ready 只代表配置语法/权限合格。
uv run --package nexus-fosun-gateway nexus-fosun-simulation-readonly

# 只有既有模拟配置与权限就绪时，显式执行一次有界只读。
uv run --package nexus-fosun-gateway nexus-fosun-simulation-readonly \
  --read --date-from 2026-09-01 --date-to 2026-09-17
```

也可用 `--config /absolute/protected/path/provider.env` 选择独立配置。终端仅输出固定状态、
随机 run ID 和安全开关，不输出账户标识、余额、持仓或原始错误。正常读取的本地业务记录在
配置同级 `reads/<run_id>/`（0700）内，文件为 0600：`started.json`、调用前持久化的
`dispatching.json`、成功时的 `snapshot.json`、终态 `status.json`。
只有 dispatching 而无终态表示结果未知；先检查这些记录和进程，不因终端断开重放。
`read_completed` 只表示该窗口读取与落盘完成；页内 `PARTIAL` 不提升为 `PASS`，不解除执行锁。

### 本切片验证及外部阻塞

隔离测试覆盖实际固定 SDK 方法 → 请求白名单 → 账户类型校验 → 现有归一化 → 正常 CLI
私有落盘；使用合成密钥和数据，未访问券商。另覆盖类型/状态异常、分页截断、写接口拒绝、
身份/路径隔离、HTTP 预算、失败无重试和中断记录。本机默认模拟专用配置缺失，因此真实账户、
权限、Provider 返回与持久化验收仍未完成。没有创建/重置账户或发送任何模拟/实盘订单。

本变更无迁移、无新增依赖、不改变公共 API；可独立选择性集成。回滚为撤回本 CLI 入口及
新增模块；保留受保护的读取记录。后续外部写入仍按下方独立门禁处理。

本切片本地验证（2026-09-17）：启用 `NEXUS_OFFLINE_SOCKET_GUARD=1` 的完整 gateway
测试 1000 项通过；gateway Ruff、Mypy（40 个源文件）、新增文件格式检查和 `git diff --check`
通过。正常 CLI 默认检查实际返回 `configuration_blocked / network_attempted=false`。
首次全量命令因遗漏创建 pytest basetemp 父目录而产生 setup 错误，补齐 CI 的目录准备后通过；
测试配置使用 Git 工作树外的隔离临时目录，不放宽正式配置存储规则。独立只读审查已完成，
发现的中断标记问题已修复并补回归。以上是本地验证，不是 GitHub CI 或真实券商验收。

## 历史 Phase 3A 状态与 Phase 3B 写入停止边界

原 fixture 通道仍为 `Phase 3A offline_ready / Phase 3B external_activation_blocked`；
新增只读 CLI 状态见上节。下方是未来写入激活检查表，不授权模拟写入、Webhook 注册或部署。

仓库中的 `broker-simulation` Compose profile 只用于静态组合审查：

- 使用独立的 `fosun-simulation-gateway` 和 `broker-simulation-worker` 服务名；
- 宿主机配置仅接受 `FOSUN_SIMULATION_GATEWAY_*` 与 `NEXUS_SIMULATION_EXTERNAL_ID_*` 命名域；
- Gateway 只编译无 socket fixture 组合根，不映射 Provider API Key 或 Provider 私钥；
- Broker Worker 固定为 `offline_fixture`，又故意没有可从 Compose 注入的进程内 fixture client，因此启动会 fail closed；
- 读取 Gateway 与模拟 Gateway 不共享 HMAC、mTLS 身份或会话缓存。

只允许做静态校验，不启动该 profile：

```bash
docker compose --env-file infra/env.example -f infra/compose.yaml \
  --profile broker-simulation config --quiet
```

## 门禁 A：Phase 1 真实只读稳定证据

以下条件必须同时有可验证证据，缺少任一项立即停止：

1. 对话中曾暴露的旧 Key 已吊销；旋转后的只读凭据仅在批准的 secret manager 中，不出现在 Git、聊天、命令行、日志或测试快照。
2. `docs/runbooks/fosun-sdk-admission.md` 的书面许可、安全审查、官方 test vector、TLS 验证、精确 HTTPS base URL 和固定 SDK subject 全部通过。
3. SDK 与契约标识被冻结为可重算的 tag/commit、manifest SHA-256 和读取路由集，不允许漂移或通用代理。
4. 按 `docs/runbooks/fosun-read-only-activation.md` 完成一次单独批准、最多 24 次请求的只读探测，并生成不含账户或交易数据的脱敏 artifact。
5. 连续五个完整美股交易日都完成账户、现金、持仓、历史订单和流水的只读同步；每日记录 UTC 窗口、artifact hash、数据时点、页数、结果计数和安全结果码。
6. 五日内没有未解决的 `RECONCILIATION_REQUIRED`、能力过期、契约漂移、TLS 漂移、身份错配或无界重试；故障和限流演练均能在网络尝试前或已知安全边界内 fail closed。

本地预检结果 `fixture_with_next_action / COMPLETE_SDK_ADMISSION` 不是五日证据，也不能提升任何能力。

## 门禁 B：写契约与账户隔离

在请求 Phase 3B 批准前，必须把以下证据冻结到新的 simulation admission package：

1. Provider 可验证的“模拟账户”分类字段、枚举、错误语义以及它无法触达真实账户的权限证据。
2. 下单、改单、撤单、订单列表、成交、拒单的精确 SDK 方法、请求/响应 schema、整股与 tick 精度、时段、币种和状态转移。
3. Provider 级幂等键/client-order-id 的书面语义。如无法证明，必须保留“结果未知绝不重发，只全量对账”政策。
4. Webhook/orderUpdate 的认证方式、事件 ID、时间窗、重放和交付保证。证据不足时不注册 Webhook，仅用主动读取补偿。
5. 七个固定操作的脱敏 test vector、路由集、响应大小、超时和限流规则；禁止任意 URL、method、headers 和通用 SDK 调用。
6. 与只读 Gateway 完全不同的 HMAC current/previous identity、mTLS client/server identity、Provider 凭据、进程和 session cache。
7. 生产 simulation adapter 已实现并通过安全审查；当前 fixture-only composition root 不能被环境变量直接升级。
8. 数据库中有追加式 `SIMULATION_CONTRACT_ADMITTED` 记录，其 subject、artifact hash、有效窗口与启动时重算结果完全一致。

## 门禁 C：单次有界联调批准

门禁 A 和 B 通过后，向用户提交一份完整、可一次性判断的批准请求。请求必须同时写明：

- 目标只能是一个模拟账户的脱敏标签与内部 binding ID；
- 冻结的 SDK commit/tag、读取契约 hash、模拟写契约 hash、admission artifact hash 和将运行的代码 commit；
- 精确 UTC 开始/结束窗口，不允许自动延长；
- 允许的操作集：能力读取、有界订单/持仓/流水读取，以及最多一个人工确认批次的 create；modify/cancel 默认不在首次窗口内；
- 批次的最大订单数、最大总 notional、单标的/行业/现金比例上限。这些必须是用户在该次批准中看到并明确同意的数字，不从代码默认、当前持仓或 Agent 推断；
- 是否允许注册 Webhook；未明确写“允许”就视为禁止；
- 执行人、观察人、回滚负责人与审计 artifact 位置；
- 停止条件和回滚顺序。

用户批准窗口后，仍需对将发送的每一个具体批次做页面内人工确认。窗口批准不等于订单批准。

## 强制停止条件

出现以下任一情况时，停止领取新 job，不自动重发、改单、撤单或发送“补偿订单”：

- 账户类型、权限、SDK/manifest/contract hash、TLS/HMAC 身份或 capability 窗口与批准不符；
- 报价、组合快照、风控、审批或对账证据缺失、过期、部分或冲突；
- 任一 create/modify/cancel 返回结果未知，或 Webhook 与主动读取冲突；
- 订单数、notional、现金、单标的或行业上限将被突破；
- Provider 返回未知字段/枚举、重定向、TLS 失败、认证异常、权限扩张、限流异常或非模拟账户证据；
- 审计落库失败、数据库版本不匹配、worker lease 或唯一 attempt 约束无法证明。

## 回滚与恢复

1. 撤销数据库中的 simulation capability 并停止模拟 Gateway/Broker Worker；不删除订单、attempt、回执或对账审计链。
2. 在 secret manager 中吊销/旋转该模拟进程专用的 Provider、HMAC 和 mTLS 凭据，不改动只读 Gateway 凭据。
3. 对任何结果未知的命令只执行有界全量读取对账；无法唯一确认时保持账户锁定并人工处理。
4. 恢复 NEXUS 至 app fixture 配置，验证 `offline_ready / external_activation_blocked`、readiness、单一迁移 head 和审计可读。
5. 代码回滚使用普通 `git revert` 或前向修复，不改写 Git 历史；存在 Phase 3 事实时不执行破坏性数据库 downgrade。

## 当前不执行的动作

在本阶段结束时，不执行真实只读探测、不启动面向复星的 simulation Gateway、不加载/旋转凭据、不注册 Webhook，不发送 create/modify/cancel。Phase 3B 只能在上述证据齐备后，由用户对“精确窗口 + 精确上限 + 精确账户”再次明确授权。
