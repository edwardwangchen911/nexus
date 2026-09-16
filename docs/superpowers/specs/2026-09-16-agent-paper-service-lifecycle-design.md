# Agent Paper 独立 Worker 服务设计

**状态：** 待用户审阅

**日期：** 2026-09-16

**范围：** Mac 本地 NEXUS、Codex SDK、内部模拟；不包含券商写入或实盘

## 1. 结论

Agent Paper 是与 Web、API、General Worker 并列的独立后台 Worker 服务。它不是新的 HTTP
微服务，也不拥有新的数据库、队列或调度平台；它继续复用现有 Engine、PostgreSQL、MinIO、
Job/Run/Output、模拟订单、账本、审计和心跳事实。

服务只负责持续运行现有五分钟循环。交易正确性仍由数据库事实、每槽幂等、确定性风控和
`execution_locked=true` 保证，不由操作系统进程状态保证。

## 2. 产品边界

```text
NEXUS local
├─ Web                         读取已持久化事实
├─ API                         查询与只读产品接口
├─ General Worker              通用 Job 消费者
└─ Agent Paper Worker          五分钟模拟决策循环
   └─ PostgreSQL / MinIO       与其他组件共享事实
```

- Agent Paper 离线时，Web、API 和 General Worker 仍可正常使用。
- 页面显示“平台状态”和“Agent Paper 状态”，不得把循环离线解释成整个平台离线。
- Agent Paper 不新增网络端口，不接受外部写请求，不接触实盘权限。
- 页面刷新只读数据库，不启动循环、不请求行情、不调用模型。

## 3. 部署和正常入口

Agent Paper 使用 Codex SDK 的宿主机 ChatGPT 登录，因此当前在 Mac 宿主机运行；不复制登录
文件、不注入 OpenAI API key，也不把认证目录挂载进容器。

用户只使用现有统一入口：

- `nexus-local-stack up`：启动 Compose 基础组件；功能开关开启时，再启动宿主机 Agent Paper
  服务。
- `nexus-local-stack status`：分别报告基础组件状态和 Agent Paper 状态。
- `nexus-local-stack down`：先请求 Agent Paper 安全停止，再停止基础组件。

现有 `scripts/macos/agent-paper-loop-service.py` 作为上述入口的内部 macOS 托管适配器，正常
产品文档不要求用户直接操作它。它只负责 start/status/stop，不判断交易结果。

`nexus-local-stack` 把本次明确的 `repo_root`、`env_file` 和 `project_name` 传给适配器；适配器和
Worker 不再自行回退到脚本所在目录或固定 `infra/.env.local`。每个 macOS 用户仍只有一个固定
Agent Paper 后台服务。start/status/stop 在操作已有注册前必须确认其 repo、配置文件和项目名与
本次请求完全一致；不一致时返回固定 ownership mismatch，既不发信号也不替换服务。这样开发
worktree 不能误停或接管主工作区服务，也不需要建设通用的多实例管理器。

`nexus-engine-agent-paper-loop` 保留为开发和故障诊断入口，不是第二套后台托管方案。
Compose 中的 `agent-paper-loop` 服务不能获得宿主机 Codex 登录，应从正常运行路径和文档中
移除，避免形成第三套入口。

## 4. 最小运行合同

### 4.1 单实例

Worker 在任何心跳、周期检查或业务动作前取得一把进程生命周期 `flock`，持有到退出。锁不可
用时返回固定错误码，且不产生业务写入。

锁只解决前台诊断入口与后台服务并发启动的问题。不新增运行描述符、Supervisor 锁、PID
启动时间校验、命令归一化、父子管道或运行状态数据库。

### 4.2 固定交易日历

Worker 启动时使用数据库时钟调用现有服务级日历选择器，只选择一个已发布、`complete`、覆盖
当前日期及下一 XNYS session 的 authority，并在本次进程生命周期固定其 ID。

当前 session、下一 session、07:00 ET 决策窗口、实际收盘、半日市、周末、假日和 DST 都从
该 authority 读取。缺失、越界或 ID 不一致时 fail closed，不使用固定 16:00、本机日期或其他
authority 回退。数据库出现更新的日历不会使运行中 Worker 热切换；下一次明确启动再选择。

### 4.3 五分钟循环

- 每轮先读持久化 `cycle → job → outputs → order/window`，结果未知时不重放。
- 决策窗口内按五分钟槽推进，每槽最多一个 cycle 和一个精确 Agent Job。
- 交易时段外等待同一 authority 的下一 session，仅更新现有数据库 heartbeat；已有精确冻结且
  到期的订单窗口仍按原合同完成，不把该恢复动作误报成新周期外调。
- 等待采用不超过 15 秒的分块，以便及时响应停止；等待期间不调用模型、行情或模拟订单。
- `completed` 和合规 no-trade 可进入下一槽；`failed`、`unavailable` 或结果未知使 Worker
  停止，等待显式恢复。
- 已冻结且精确到期的订单窗口或绩效动作按现有规则完成；不得为了保持服务在线而放宽数据、
  时点、预算或风控门槛。

### 4.4 心跳与状态

继续复用 `worker_heartbeats` 中现有 `agent_paper` 记录，不增加运行时表。

页面/API 只展示数据库能证明的事实：heartbeat 为 `online`、`offline` 或 `incompatible`，并分开
展示最近周期状态、固定停止原因和下一持久化交易窗口。现有 heartbeat 没有 phase 字段，因此
页面不猜测 `waiting`，也不把陈旧 heartbeat 猜成操作系统进程已停止。

`nexus-local-stack status` 分开输出托管器观察到的 `running`、`inactive`、`stopped` 或
`unconfirmed`，以及数据库 heartbeat 状态和零外部请求 preflight 结果。三类事实不合并成新的
总状态，也不写入新表。任何状态输出都不得包含账户、证券、订单、提示词、原始异常、命令行或
环境变量。

## 5. 安全停止和恢复

- `SIGINT`/`SIGTERM` 只设置停止请求，不直接取消正在执行的业务动作。
- 空闲时不再领取新槽，写最后一次 heartbeat 后退出。
- 活动动作继续使用自身已有的有界超时；完成后只回读一次精确持久化事实。
- 回读为已知终态时正常退出；回读失败或结果冲突时以固定 `STOP_UNCONFIRMED` 退出。
- 停止等待有界超时时，托管器报告停止未确认，不强杀、不删记录、不自动重启。
- `nexus-local-stack down` 遇到停止未确认时立即返回失败，不继续停止 API、PostgreSQL、MinIO
  或其他当前动作可能仍需使用的依赖；核对持久化事实后再显式重试 `down`。
- 进程意外退出时 heartbeat 自然过期；恢复前先检查原 `cycle → job → outputs`，禁止换 key
  或盲目重放。

不需要 Supervisor 死亡协议、parent fd 或新的两阶段资源控制器；上述顺序只是现有统一 `down`
命令的普通 fail-closed 步骤。Agent Paper 自行停止不阻止只读平台继续运行。

## 6. 配置和启停

- `NEXUS_AGENT_PAPER_CYCLE_ENABLED=false`：`up` 不启动 Agent Paper；`status` 单独显示配置为
  disabled，同时仍如实显示实际托管和 heartbeat 状态，不能用配置值覆盖仍在运行的进程。
- 开关为 true：`up` 先完成零外部请求 preflight，再调用宿主机托管适配器。
- preflight 阻断时基础组件仍保持在线，`status` 单独显示 preflight blocked；它不覆盖实际托管
  或 heartbeat 状态，也不请求 Provider 或模型。
- 重复 `up` 必须幂等，不启动第二个 Worker。
- `down` 即使 Agent Paper 已停止也可幂等执行；停止未确认时不谎报成功，也不继续关闭依赖。

## 7. 验收

### 离线验收

1. 功能关闭时 `up/status/down` 不启动 Agent Paper，基础组件行为不变。
2. 功能开启且 preflight ready 时只出现一个 Worker，并在 30 秒内写新鲜 heartbeat。
3. 第二个后台或前台入口无法越过单实例锁，锁失败前无业务副作用。
4. 冻结时钟覆盖正常日、周末、假日、半日市和 DST；日历缺失或 authority 不一致 fail closed。
5. 非交易时段且不存在已冻结待完成订单窗口时，只等待和 heartbeat，外部调用计数为零。
6. 停止发生在活动动作期间时，动作不被取消；结束后只做一次确认回读。
7. CLI 分开输出托管、heartbeat 和 preflight 事实；页面只展示数据库可证明的 online/offline、
   最近周期和下一窗口，不跨层猜测。
8. 从另一个 worktree 或配置调用 start/status/stop 时返回 ownership mismatch，且不影响已有服务。
9. 停止未确认时 `down` 返回失败，API、PostgreSQL 和 MinIO 保持当前状态。
10. Web/API 在 Agent Paper disabled、blocked、stopped 时仍正常只读。

### 真实验收

真实验收独立于服务安装，不因进程在线而自动通过：

1. 正常入口连续跨越两个合格五分钟槽。
2. 每槽最多一个 cycle 和一个精确 Agent Job；同槽重读不增加 Job、订单、成交或账本。
3. 每槽形成决策，并产生合规 no-trade 或内部模拟订单，再由 `/paper` 回读同一周期事实。
4. `execution_locked=true` 始终保持；不进行券商写入。

## 8. 回滚

1. 将 `NEXUS_AGENT_PAPER_CYCLE_ENABLED=false`。
2. 通过统一 `down` 或内部托管适配器请求安全停止。
3. 保留全部 cycle、Job、Output、订单、成交、账本、heartbeat 和审计事实。
4. 回滚 Agent Paper 接线不影响 Web、API、General Worker、PostgreSQL 或 MinIO。

## 9. 明确不做

- 不创建 `nexus-local-stack serve` 或新的 Supervisor。
- 不创建运行描述符、PID 身份系统、父子进程管道或多级运行状态机。
- 不引入 Redis、Kafka、Celery、Kubernetes、新数据库表或新网络服务。
- 不把 Agent Paper 离线升级为整个平台离线。
- 不同时维护 launchd、Compose 和自建 Supervisor 三套后台托管路径。
- 不恢复 Shadow，不新增 Agent 角色，不接入券商写入。
