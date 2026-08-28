# NEXUS 可重复本地运行与美股只读数据运行时验收审查

**当前结论：** 可重复 fixture 本地栈达到 `offline_ready`；Alpaca bars、SEC company facts/submissions 与 FRED/ALFRED 固定序列的生产只读 transport 达到 `code_ready / external_activation_blocked`。本轮没有读取真实凭据、发起 Provider 请求、部署或激活付费数据。

## 审查范围与方法

- 范围：`d518bba..1f84ef6`、本轮验收修正及其运行手册；重点复核固定源、重定向、响应上限、超时/重试所有权、可用时间、原始证据、幂等 Job、配置域与本地栈清理。
- 方法：实现完成后以提交差异和新鲜运行证据做一次与实现步骤分离的二次审查。本轮未使用子 Agent，因此不是第二人或第二模型背书。
- 安全边界：仅使用 fixture、回环端口与任务专用 Compose project；没有访问复星账户或交易接口，没有真实订单、券商写操作、部署、密钥/权限变更或 Git 历史改写。

## 已修复发现

| 级别 | 发现 | 修复与反例 |
| --- | --- | --- |
| Important | Vinext 本地 worker 只注入认证变量，BFF 在运行域内看不到 Engine base URL 和 current HMAC 身份，真实本地 readiness 在发请求前返回脱敏 500。 | 新增 `local-binding-config` 红测试；本地 worker 只增加 `NEXUS_ENGINE_BASE_URL`、current key ID/secret 三项，不传 previous、Fosun、Alpaca 或 OpenAI 配置。独立栈的认证 BFF readiness 随后返回四组件 `ready`。 |
| Minor | 全仓 Ruff 会拒绝 15 个既有测试 helper 的导入分组，妨碍把完整静态门禁作为发布证据。 | 仅执行 Ruff 证明的机械导入排序；无逻辑变更，Ruff/Mypy 和 Python 回归随后全绿。 |

当前未解决发现：Critical `0`、Important `0`、Minor `0`。

## 验收证据

| 门禁 | 结果 |
| --- | --- |
| Provider/ops 离线套件 | `171 passed in 2.46s`；开启 loopback-only socket guard，外部 socket attempt 为 0。 |
| Provider PostgreSQL 集成 | `24 passed in 55.78s`；覆盖持久化采集、市场摄取与 Agent 证据读取。 |
| Engine 非集成全量 | `1016 passed in 54.16s`。 |
| Python 静态 | Ruff 全通过；Mypy 对 308 个源文件无问题。 |
| Web | generated client、TypeScript、ESLint、production build 全通过；45 个文件、476 个单测通过（13.47s）。 |
| 浏览器 | Chromium `85 passed (1.7m)`；包含 375/390/768/1024/1440、横屏、200% reflow、键盘/焦点与 Axe critical/serious=0。 |
| 独立 fixture smoke | `doctor` 5/5、`up` 6/6；对象存储重复初始化为 `existing`；Engine liveness 200；认证 Web BFF readiness 返回 database/migration/object_store/worker 全 `ready`。 |
| 清理与边界 | 运行中的 broker/Fosun 服务 0，`nexus_test_%` 残留库 0；`nexus-phase3-runtime-acceptance` 容器/网络 0/0，`nexus-phase3-test` 容器 0；未使用 `--volumes`。 |
| 配置/密钥 | app 与 provider Compose 静态策略通过；tracked Provider credential-shaped finding 0；临时 fixture env 被 Git ignore 并在验收后删除。 |

## 架构结论、外部门禁与回滚

- 普通 `app` profile 只能组合 fixture Provider；生产 Provider 必须显式选择 `provider` profile 并一次性满足完整配置，不会静默降级或混用来源。
- HTTP 读取只允许三个固定 HTTPS origin，拒绝重定向和动态 URL，最多 3 次尝试、统一绝对截止与 8 MiB 上限；SEC 固定为 1 次尝试。
- Provider Job 只消费冻结且绑定哈希的美股 universe，保存来源、时点、原始对象版本与质量结论；缺失、过期或冲突时不产生新的交易权威。
- 真实 Alpaca/SEC/FRED 连接仍需新的明确授权与受控 secret 注入。本轮不证明账号权限、feed entitlement、真实限流或外部服务可用性。
- 回滚使用普通 `git revert` 或前向修复；停止 provider worker 即停止新读取。不得通过放宽固定源、重试、证据或 freshness 门禁来恢复服务。
