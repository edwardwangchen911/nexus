# Agent Paper 服务生命周期设计

**状态：** 待用户审阅
**日期：** 2026-09-16
**范围：** Mac 本地 NEXUS 后端、Codex SDK、内部模拟；不包含券商写入或实盘

## 1. 结论

Agent Paper 需要一个独立运行的 Worker 进程，但不需要新建微服务、HTTP API、数据库、
任务队列或调度平台。它继续复用现有 Engine 代码、PostgreSQL、MinIO、Job/Run/Output、
风控、模拟订单、账本和心跳事实。

该进程是 NEXUS 后端的一等组件，不应由用户单独执行脚本。统一的本地后端入口负责
启动、状态核对和安全停止；Agent Paper 功能开关关闭时不启动该进程。

## 2. 为什么需要独立进程

1. 普通 Worker 是队列消费者，可以有多副本；Agent Paper 是按账户和交易时槽推进的单调度者。
2. 普通 Worker 的常驻模式明确禁止 Codex SDK；Agent Paper 只为当前周期的精确 Job ID
   执行一次 Codex SDK 任务。
3. Agent Paper 还负责交易日历、五分钟时槽、前置数据、Schwab 行情窗口、模拟订单推进和
   绩效发布，生命周期与普通 Job 轮询不同。
4. 结果未知时 Agent Paper 必须整个停止，但不应阻止普通 Worker 处理无关 Job。
5. 放入 API 会受重载、多进程和水平扩容影响，存在重复循环风险。

“独立”只表示独立进程和生命周期，不表示独立产品或数据边界。

## 3. 组件结构

```text
NEXUS 本地后端
├─ PostgreSQL / MinIO
├─ Engine API
├─ General Worker
└─ Agent Paper Worker
   ├─ 交易日历与五分钟时槽
   ├─ 持久化状态检查
   ├─ 前置数据准备
   ├─ 精确 Agent Job 执行
   ├─ 确定性风控与内部模拟
   └─ agent_paper 心跳
```

Agent Paper Worker 继续使用 `nexus-engine-agent-paper-loop` 入口。不改写循环内的投资、风控、
订单或幂等合同。本切片会修改循环状态机、协作停机、进程身份和统一入口，属于完整的
生命周期切片，不是简单的启动脚本接线。

## 4. 统一生命周期合同

### 4.1 启动

`nexus-local-stack serve` 是 MVP 唯一的完整本地后端入口，并保持前台运行。终端关闭导致
supervisor 退出时，产品语义是“后端已停止或降级”，即使遗留容器仍在也不算后端在线。
MVP 不承诺脱离终端、登录后自启或异常自动拉起。

`serve` 执行以下顺序：

1. 只读验证仓库、被 Git 忽略的本地配置、私有运行目录、owner、mode 和非符号链接。
2. 使用 `O_NOFOLLOW` 等价的安全打开方式取得 supervisor 生命周期锁；失败时不创建描述符、
   不启动任何组件。
3. 原子写入 supervisor 私有描述符。
4. 启动并验证 PostgreSQL、MinIO、迁移、API 和 General Worker。
5. 当 `NEXUS_AGENT_PAPER_CYCLE_ENABLED=true` 时，在宿主机运行零外部请求的 Agent Paper
   preflight。
6. preflight 为 `ready` 后启动唯一 Agent Paper Worker；否则 supervisor 继续前台运行，后端返回
   明确的部分就绪或阻断状态，不调用模型或行情。
7. 启动结果必须以新鲜 `agent_paper` 心跳确认，不以“子进程已创建”代替。

### 4.2 运行

- Worker 活着时由现有循环自行对齐五分钟时槽，不新增 cron 或每槽外部调度。
- 每个时槽先读 `cycle → job → outputs → order/window` 持久化事实，不确定时不重放。
- 交易时段外只等待和写心跳，不调用模型、Provider 或模拟订单。
- 每次等待不超过 15 秒，以便持续刷新心跳并及时响应停止请求。
- 日终不把“服务停止”作为必然结果；只要后端和功能开关仍在，Worker 等待下一个
  合法交易时段。
- 当前循环在收盘后退出的行为需调整为“等待下一时段”；这是服务化必要改动，不改变
  每个时槽的业务规则。
- 新增只读的服务级 calendar selector。它只使用数据库时钟和日历表，不依赖账户、Universe、
  当前是否在决策窗口或任何外部调用；按现有权威顺序
  `published_at DESC, id DESC` 选择一个在数据库时钟前已发布、`complete`、range 覆盖纽约
  当前日期且包含当前或下一 XNYS session 的 authority。
- `serve` 将选出的 `calendar_authority_id` 显式传给 Worker。Worker 在本次进程生命周期内固定
  使用该 ID；当前 session、下一 session、开盘和收盘必须来自同一 authority，不在运行中切换
  到“更新”日历。
- `load_quasi_realtime_cycle_authority` 增加必填的 expected calendar ID，查询时限定该 ID 并
  回读严格比较；它原有的账户、Universe、07:00–实际收盘决策窗口和时点规则全部保留。
  其他循环路径也只能使用 Worker 绑定的 ID，任何不一致都以
  `STOP_CALENDAR_AUTHORITY_MISMATCH` 停止。
- 该 XNYS 日历权威必须是 `complete`，其 range 和 session 记录覆盖数据库时钟对应日期到
  下一 session，且每个 session 的开收盘合法；这同时覆盖周末、假日、半日市和美东夏令时。
  决策窗口从 session 当日 07:00 ET 起，到该 authority 给出的实际收盘时刻止。
- 若日历缺失、过期或相互矛盾，Worker 以 `STOP_CALENDAR_UNAVAILABLE` 停止；不得用固定
  16:00、自然日或本机时区近似。
- 这里“过期”是指固定 authority 的 `range_end` 或 session 集合无法解析下一 session；
  “冲突”是指固定 ID 下缺行、重复语义或开收盘约束不一致。数据库中存在其他完整 authority
  不构成冲突，也不得据此静默换 ID；恢复时由新的显式 `serve`/preflight 重新选择。
- 已在当前周期持久化且到期的绩效任务可按现有价格权威规则继续发布；没有精确到期事实时，
  等待期间不得新建研究、行情、模型、订单或绩效调用。

### 4.3 失败与恢复

- 只有 `completed` 或合规 no-trade 才继续下一时槽。`unavailable` 或 `failed` 仍按现有政策
  停止整个 Worker，即使已有明确终态也不每五分钟重复消耗 Provider 或模型额度。
- 运行中 Job、模型请求、Provider 请求或订单结果未知时，Worker 整体停止，不自动重启。
- 进程意外退出不默认自动重启。状态显示 `offline` 并附最后退出原因，先核对持久化事实。
- 只有显式的后端启动/恢复动作才可重新运行 preflight；不为变绿自动连续重启。

### 4.4 停止

1. Supervisor 向 Worker 发送一次 `SIGINT`/`SIGTERM`；信号处理器只设置
   `shutdown_requested`，不直接取消活动任务。
2. 没有活动任务时，Worker 不再认领新时槽，写入 `STOP_REQUESTED` 后退出。
3. 已有活动任务时，Worker 继续心跳，并等待该任务自身已有的有界超时或终态；第二次信号
   也不强杀或取消任务。
4. 活动任务返回后，Worker 只回读一次精确的 `cycle → job → outputs → order/window`。
   已知终态则安全退出；缺失、冲突或结果不明则写入 `STOP_UNCONFIRMED` 后退出。
5. `down` 在有界时间内核对 Worker 进程、锁和退出描述符。结果不明或超时时不强杀、不删
   历史事实，也不继续停止其依赖的 API/存储；整体返回停止未确认，等待人工核对。
6. 只有 Agent Paper 已确认退出后，API、General Worker 和存储才按现有顺序停止。

## 5. 宿主与 Codex SDK 边界

Codex SDK 使用宿主机 ChatGPT 登录存储，不复制认证文件，不把 OpenAI API key 伪装成
Codex SDK，不把宿主认证目录盲目挂载到容器。因此 MVP 的 Agent Paper Worker 在宿主机运行，
但仍属于统一 NEXUS 后端生命周期。

统一入口以前台 supervisor 方式维持宿主 Worker。supervisor 接收 `SIGINT`/`SIGTERM` 时触发
上述安全停止。它向 Worker 传入只读的父进程存活管道；管道 EOF 只触发同一个
`shutdown_requested` 流程，避免 supervisor 意外退出后 Worker 无限孤儿运行。它或子进程
意外退出时，持久化心跳过期并使后端显示降级，不伪装为就绪。这不要求 launchd、完全磁盘
权限或容器内登录。

当真实运行证明需要登录后自启、脱离本地 supervisor 或异常自动恢复时，再单独设计安装在
非 Documents 保护路径的专用运行时。当前不授予通用 Python 完全磁盘权限。

## 6. 统一操作面

在现有 `nexus-local-stack` 上增加一个明确的长运行入口，不新建第二套 launcher：

- `serve`：唯一完整后端入口；准备并启动现有容器组件，再以受控子进程启动宿主 Agent Paper
  Worker，然后保持前台监督。
- `status`：保留容器状态，并同时读取 Agent Paper preflight、最新心跳和最后周期；三者分开表达。
- `down`：优先请求 supervisor 安全停止 Agent Paper，再停止 API/Worker/存储。
- `infra-up`：原 `up` 的兼容能力，只启动容器基础组件，明确标记为维护/诊断入口，不表示
  NEXUS 后端完整在线。

现有 `up` 先输出弃用提示并转到 `infra-up` 语义；文档、状态和验收不得再把 `up` 称为完整
后端启动。后续兼容移除另行处理。

Supervisor 不同时承担投资、风控或任务调度；它只负责进程生命周期、脱敏状态和信号转发。

私有运行目录使用两把职责独立的锁和两个 mode `0600` 描述符：

- Supervisor 从 `serve` 启动到退出始终持有 `serve.lock`，并记录 supervisor PID、进程启动
  时间、固定入口标识、版本和可证明的子进程观察结果。即使功能关闭或 preflight 阻断，
  第二个 `serve` 也不能启动。
- Worker 在获得 `agent-paper.lock` 后才可写心跳、检查周期或执行业务动作，并持锁到进程退出；
  它记录 Worker PID、进程启动时间、固定入口标识、版本、绑定的非敏感
  `calendar_authority_id`、固定退出原因码和退出时间。这样 supervisor 异常死亡后，第二个
  Worker 仍不能启动。

两个描述符都禁止账户 ID、证券、订单、提示词、原始异常、凭据或环境变量。它们只是本机
生命周期诊断，不替代数据库业务事实，也不直接进入页面/API。

第二个 `serve` 无法获得 `serve.lock` 时直接拒绝。`status`/`down` 对两套文件分别先拒绝
符号链接和非当前用户文件，再核对 mode、PID、启动时间和入口标识：

- 进程不存在且对应锁可获得：该进程确认为已停止，可清理陈旧描述符。
- 锁仍被占用且身份一致；对 Worker 还要求心跳一致：按已在线处理。
- 锁、身份、心跳任一冲突：对应状态为 `unconfirmed`，不发送信号、不启动重复进程。

`down` 只向全部匹配的 supervisor/Worker 发起协作停止，不按 PID 猜测进程。Supervisor 不做
异常自动重启；恢复必须重新执行显式 `serve` 和 preflight。

## 7. 状态与验收

服务启动不等于 Agent 可决策，不等于会交易。必须分开显示：

| 事实 | 权威来源 |
| --- | --- |
| 容器组件运行 | Compose 进程状态 |
| 完整后端入口在线 | supervisor 锁、描述符和进程身份 |
| Agent Paper 进程在线 | 30 秒内、版本兼容的 `agent_paper` 心跳 |
| Worker 实际绑定日历 | Worker 私有描述符中的 `calendar_authority_id` |
| 当前 preflight 候选日历 | 服务级 calendar selector 的只读结果 |
| 最近周期完成 | 持久化 cycle/Job/Output 终态 |
| 当前可运行 | preflight + 功能开关 + 时段与证据状态 |
| 本机最近退出原因 | 私有运行描述符中的固定原因码 |
| 执行许可 | 始终独立的 `execution_locked=true` |

容器仍运行但 supervisor 缺失，或功能开启后 Worker 锁/新鲜心跳缺失时，`status` 必须按下表
报告为 `degraded`、`stopped` 或 `unconfirmed`；功能主动关闭按
`ready_agent_disabled` 处理。不能只因 Compose 为 Up 就报告后端在线。

状态矩阵固定如下：

| 条件 | 完整后端状态 | 行为 |
| --- | --- | --- |
| supervisor 在线、功能主动关闭、基础组件就绪 | `ready_agent_disabled` | 不运行 preflight/Worker；不是故障 |
| supervisor 在线、功能开启、preflight 阻断 | `degraded_preflight_blocked` | supervisor 留在前台；不启动 Worker |
| supervisor 和 Worker 在线、心跳新鲜 | `ready` | 按时段状态决定是否可运行 |
| supervisor 在线、Worker 明确终态退出 | `degraded_worker_stopped` | 不自动重启，等待显式恢复 |
| supervisor 在线、Worker 异常消失或状态冲突 | `unconfirmed` | 不自动重启，不停止依赖 |
| supervisor 不在线、两把锁均释放且有确认的正常退出描述符 | `stopped` | 容器若遗留也不算完整后端在线 |
| supervisor 与 Worker 均不存在但缺少正常退出描述符 | `unconfirmed` | 报告运行时消失，不猜测原因 |
| supervisor/Worker 任一身份或锁无法确认 | `unconfirmed` | 不发信号、不清理、不启动重复进程 |

退出原因只表达可证明事实：Worker 正常退出时写固定业务/停机原因；supervisor `wait` 到非零
退出码时写 `WORKER_EXITED_UNCLEANLY`；Worker 消失、锁释放但无最终描述符时写
`WORKER_EXIT_REASON_UNKNOWN`；supervisor 与 Worker 同时消失时，后续 `status` 只能报告
`RUNTIME_ABSENT_UNCONFIRMED`。不得从旧心跳或原始异常猜测具体原因。

`status` 必须分开显示 Worker 绑定日历与当前 preflight 候选日历。两者不同不触发热切换；
若绑定 authority 仍覆盖下一 session，状态标记 `calendar_update_pending`，待显式恢复后采用；
若已无法覆盖下一 session，Worker 按 `STOP_CALENDAR_UNAVAILABLE` 停止，状态等待显式恢复。

最小真实验收：

1. `serve` 在功能开关开启时只启动一个 Agent Paper Worker。
2. 30 秒内心跳变为 online，页面刷新不产生业务写入。
3. 跨越两个五分钟槽，每槽最多一个 cycle 和一个精确 Agent Job。
4. 非交易时段只心跳和等待，外部请求为零。
5. 用冻结时钟和固定 `calendar_authority_id` 分别覆盖周末、假日、半日市和 DST；日历缺失、
   越界或冲突时 fail closed。真实双槽验收不替代这些确定性日历测试。
6. 夜间和周末启动 `serve` 时，服务级 selector 能绑定下一 session，但周期创建仍被原有决策
   窗口约束；绑定 ID 与 cycle loader 返回 ID 不同则停止且不产生业务写入。
7. 明确终态的 no-trade 继续下一槽；`unavailable`、`failed` 或结果未知时 Worker
   停止且状态可见。
8. 在活动任务中发出停止请求，证明任务未被取消、未重放，并在一次精确回读后得到
   `STOP_REQUESTED` 或 `STOP_UNCONFIRMED`。
9. 杀死 supervisor 后，Worker 经父进程管道进入相同安全停机流程；此时第二个 `serve`
   不能越过 Worker 持有的锁。
10. 功能关闭和 preflight 阻断时分别重复 `serve`/`down`，证明 supervisor 单例和安全识别仍成立。
11. 覆盖 Worker 单独异常退出、陈旧描述符、PID 复用、符号链接、错误 owner/mode；任何冲突
    均不得误发信号或启动第二个 Worker。
12. 证明 Worker 获得单例锁前不会写心跳、检查周期或执行任何业务动作。
13. 停止后心跳过期，历史 cycle、订单、成交、账本和绩效不改写。

## 8. 回滚

1. 将 `NEXUS_AGENT_PAPER_CYCLE_ENABLED=false`，统一入口不启动 Agent Paper Worker。
2. 恢复使用 `infra-up/status/down` 维护基础组件，不删除业务事实。
3. `nexus-engine-agent-paper-loop` 仍可作为有界诊断入口，但不作为用户正常启动流程。
4. 实施与真实验收期间保留当前 launchd 代码作为禁用的回退实现，不加载、不运行，也不修改
   macOS 权限；`serve` 通过完整验收后，再以独立清理提交移除 launchd 管理器和文档入口。

## 9. 非目标

- 不建立 Agent Paper HTTP 微服务。
- 不引入 Redis、Kafka、Celery、Kubernetes 或新的分布式锁。
- 不把 Codex SDK 登录或 OpenAI API key 复制到容器。
- 不为了“服务在线”放宽时点、数据、风控、幂等或停止条件。
- 不恢复 Shadow，不新增 Agent 角色，不接入券商写入。

## 10. 备选方案与取舍

### 放入 API

拒绝。多进程和重载会重复启动调度器，请求服务不应拥有交易时段生命周期。

### 并入 General Worker

拒绝。会破坏常驻 Codex SDK 禁止、多副本消费和未知结果停止边界。

### Compose 容器常驻

当前不采用。容器没有宿主 ChatGPT 登录；复制或盲挂认证存储会扩大凭据域。

### 临时 launchd

不作为当前正常入口。它只解决进程脱离当前 supervisor，且仓库位于 Documents 时会引入
TCC 权限问题。如以后证明需要登录后自启，应使用安装在非保护路径的专用运行时重新设计。

### 统一前台 supervisor

采用为 MVP。它不改变 Codex SDK 凭据边界，不需要 macOS 额外权限，并且直接表达“后端活着
时 Agent Paper Worker 也活着”的产品合同。这里的“后端活着”特指 `serve` supervisor、
Agent Paper Worker 与基础容器共同满足状态合同；只剩容器存活不算完整后端在线。
