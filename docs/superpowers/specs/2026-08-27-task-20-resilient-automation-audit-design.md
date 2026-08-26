# Task 20 韧性自动化、恢复与审计设计

**状态：** 可直接编码
**范围：** 本地数据库作业、通用 worker、内部模拟盘自动化、事务 outbox、readiness、脱敏审计与风险审计台
**明确排除：** 真实券商写入、真实订单、复星凭据接入、复星只读激活、部署、外部告警发送、策略自进化

## 1. 目标与强制不变量

Task 20 把 Task 1–19 已有的同步服务放入一个可恢复、可审计、不会越权的 PostgreSQL 协调层。它负责“何时运行、谁获得执行权、失败后能否重试、事务提交后怎样可靠派发内部事件”，但不能改变任何业务域的风控、幂等或来源权威。

必须始终成立：

- 作业 payload 只能是注册 handler 的严格 schema，不能包含 URL、SQL、路径、Python 代码、模型指令、凭据或任意 provider 方法名。
- `general` worker 不能领取 `broker` 队列；`broker` worker 不能由 `app` Compose profile 启动。Task 20 只建立隔离协议，不激活复星读取。
- 领取使用 `FOR UPDATE SKIP LOCKED`；每次领取生成不可猜测 `lease_token` 和单调 `fence_token`。过期 worker 在最终提交时必须因 token 不匹配而失败。
- handler 的数据库副作用、作业终态、审计和 outbox 在同一事务提交。外部派发只消费已提交 outbox，并按全局 `event_id` 至少一次投递。
- 自动重试只允许 `automatic_safe`；人工重试只允许 `manual_safe`。`never`、未知结果或缺少幂等消费契约的动作不得重试。
- 数据或依赖缺失默认 `skipped`/`failed`，不能伪造成 `succeeded`。18:00 ET 质量仍非 PASS 时，当日决策链终止为 `skipped_data_quality`，不创建新提案或订单。
- 审计链全局单调、追加写入；导出采用字段 allowlist，不返回账户 ID、portfolio ID、订单 ID、精确金额/仓位、请求正文、凭据、签名、Agent 原始机密输入或 provider 原始响应。
- `/health/live` 仍只证明进程存活；受 HMAC 保护的 `/health/ready` 才检查数据库、对象存储、Alembic 单头和通用 worker heartbeat。Provider 故障不改变 liveness/readiness，但继续阻断相应业务。

## 2. 作业身份、状态与队列

### 2.1 规范 JobKey

`JobKeyV1` 严格包含：

```python
class JobKeyV1(BaseModel):
    job_type: JobType
    scope_kind: Literal["global", "portfolio", "instrument", "provider"]
    scope_id: str
    effective_at: datetime
    input_hash: Hash64
```

- `effective_at` 必须是 UTC；session date 转成该 session 权威计划时点，不能只拼本地日期字符串。
- `scope_id` 是稳定内部标识或固定 `global`，1–100 个 ASCII 安全字符，禁止 `:`、空白和外部券商账号。
- key 精确为 `job_type:scope_kind:scope_id:YYYY-MM-DDTHH:MM:SS.ffffffZ:input_hash`。
- `input_hash` 是严格 payload、依赖 ID/版本、数据快照、policy/strategy/model 版本的 canonical JSON SHA-256；未使用字段省略。
- `jobs.job_key` 继续唯一。同一个 key 的并发入队返回同一 Job，不创建第二个业务意图。

### 2.2 状态机

数据库状态统一迁移为小写：

```text
queued -> running -> succeeded
                  -> skipped
                  -> failed -> queued       (仅安全且未耗尽的重试)
                            -> dead_letter
running --lease expired--> queued           (fenced reclaim)
```

- `queued/running/succeeded/failed/skipped/dead_letter` 是唯一合法状态。
- 终态 `succeeded/skipped` 不可重开；`failed/dead_letter` 只有版本化安全重试服务可回到 `queued`。
- 自动 attempt 上限 3；人工安全重试上限 2。handler 可声明更低值，不能声明更高值。
- `job_attempts` 每次 claim 新增一行；`attempt_no` 对 job 连续，`fence_token` 单调。attempt authority 固定后不可换 job/worker/token，终态后不可更新。
- `job_leases` 是每个 job 的当前可变 lease 投影；heartbeat 只允许同 token 延长，最长延长 60 秒，默认 30 秒，每 10 秒心跳。
- `worker_heartbeats` 以 `(queue, worker_id)` 唯一，保存版本、Alembic head、started/seen 时间；ready 只要求 `general` 队列在 30 秒内有兼容 heartbeat。

### 2.3 队列与优先级

固定队列：`general`、`broker`、`strategy_rnd`。固定优先级从高到低：

1. broker sync/reconciliation 300（只可由 broker worker，默认不激活）；
2. quality/source conflict 280；
3. diagnostics/drawdown/paper execution 260；
4. paper performance/brief 220；
5. Agent research/optimization 180；
6. strategy R&D 20。

同优先级按 `(available_at, created_at, id)` 稳定领取。通用 worker 只领取 `general`；strategy worker 可缺省，由通用 worker仅在无更高优先级 general job 时领取 `strategy_rnd`。

### 2.4 `0020` 精确持久化形状

- `jobs` 保留现有主键、`job_key/input_hash/schema_version/payload/created_at`，把 `job_key` 扩为 512 字符，并新增 `job_type(50)`、`queue`、`scope_kind`、`scope_id(100)`、`effective_at`、`priority`、`retry_policy`、`max_attempts`、`attempt_count`、`manual_retry_count`、`available_at`、`started_at`、`finished_at`、`last_error_code`、`result_code`、`version`。payload 必须是 JSON object，hash 为 64 位小写十六进制。
- `job_leases` 保留每 job 唯一行，新增 UUID `lease_token`、正整数 `fence_token`、`acquired_at/heartbeat_at`；`expires_at > heartbeat_at >= acquired_at`。
- `job_attempts`：`job_id/attempt_no/lease_token/fence_token/worker_id/started_at/finished_at/outcome/error_code`；`(job_id,attempt_no)` 和 lease token 唯一。outcome 为 `running/succeeded/skipped/failed/lost`。
- `job_dependencies`：`job_id/depends_on_job_id/required_outcome`，复合唯一、禁止 self edge；延迟触发器递归拒绝环。
- `worker_heartbeats`：`queue/worker_id/process_version/migration_head/started_at/seen_at`，`(queue,worker_id)` 唯一；只允许同 worker 更新 `seen_at/process_version/migration_head`。
- `outbox_events` 保留既有列并新增 `status/attempt_count/max_attempts/available_at/last_error_code/dead_lettered_at`；`published_at` 只有 published 状态非空。
- `outbox_delivery_attempts`：`outbox_event_id/attempt_no/worker_id/lease_token/started_at/finished_at/outcome/error_code`，attempt 连续且唯一。
- `audit_events` 增加第 6 节链字段；`audit_chain_manifests` 为 `session/first_event_seq/last_event_seq/head_hash/object_uri/object_sha256/byte_size/created_at`，session 和 head hash 各唯一。

Job/lease/outbox/heartbeat 是协调投影，因此允许受约束 UPDATE；attempt、dependency、audit、manifest 是权威事实，发布/终态后只追加或不可变。

## 3. Worker、fencing 与业务幂等

### 3.1 事务边界

1. 短事务 claim：锁定一批可运行 job，写 `running`、lease、attempt 后提交。
2. handler 可在事务外做只读、有限时、无副作用的 provider/object-store 读取；结果只保存在内存或内容寻址临时对象。
3. finalize 事务重新锁 job/lease，验证 `lease_token/fence_token` 仍为当前值，再执行 handler 的确定性数据库提交。
4. 同一事务写 job 终态、attempt 终态、链式 audit 和 outbox。
5. token 已过期/被替换时，旧 worker 不得写任何业务事实；临时对象按现有清理协议删除。

所有注册 handler 必须声明：payload model、queue、priority、timeout、retry policy、业务 idempotency key 和输出 schema。未知 job type 在不执行 payload 的前提下进入 `dead_letter/handler_unregistered`。

### 3.2 Agent worker 迁移

现有 `run_agent_worker_once` 改为 `agent.research` handler adapter：

- 继续复用冻结 request/evidence/model/budget；不重建输入。
- Job 终态映射：Agent `completed -> succeeded`，`abstained -> skipped`，`failed -> failed/dead_letter`。
- Responses 调用仍最多一次重试且属于 Agent 内部模型调用策略；Job attempt 重试必须复用同一 frozen run，不扩大模型预算。
- 生产 general worker 仍不能加载 broker secret；broker composition root 保持独立。

## 4. 事务 Outbox

### 4.1 数据模型与领取

`outbox_events` 向前增加 `status/attempt_count/max_attempts/available_at/last_error_code/dead_lettered_at`；既有 `published_at` 保留。

- 状态：`queued -> delivering -> published`，或 `delivering -> queued/dead_letter`。
- `outbox_delivery_attempts` 保存 event、attempt、worker、lease token、开始/结束、outcome 和安全错误码，不保存响应 body。
- dispatcher 用 `FOR UPDATE SKIP LOCKED` 领取已到期事件。publisher registry 只接受已登记 event type 和 schema。
- publisher 必须声明消费端按 `event_id` 幂等；否则 registry 拒绝注册。未知/unsafe publisher 不发送，直接记录不可重试错误。
- 退避为确定性 `min(2 ** attempt_count, 60)` 秒，不加随机数，便于重演；达到上限进入 dead letter。
- 崩溃发生在业务提交后、派发前：下一个 dispatcher 可见 queued event。崩溃发生在外部成功、`published` 提交前：允许同 event ID 重发，消费者必须去重。

保留 `dispatch_paper_outbox` 兼容包装，但实现委托给通用 dispatcher；不增加外部通知或 broker 下单 publisher。

## 5. 每日 Pipeline

### 5.1 调度权威

- scheduler 读取 `exchange_calendar_sessions.market_close_at`；`planned_at = market_close_at + 30m`，因此正常日约 16:30 ET，半日市自动相对收盘。
- `quality_cutoff_at` 固定为 session 当日 America/New_York 18:00 并转换为 UTC；DST 由 zoneinfo 处理。
- 空闲 worker 每次 tick 只调用一次 `schedule_due_daily_pipelines(now)`；唯一 JobKey 防止多实例重复创建。
- Task 20 不引入系统 cron 或云调度器，不修改部署配置。

### 5.2 有向无环阶段

`daily_pipeline` 根按以下依赖生成严格 schema 的 child jobs；`job_dependencies` 阻止依赖未终态时被领取：

1. `collect.broker_snapshot`（broker queue，未激活则 `skipped_capability`）、`collect.market`、`collect.sec_macro`、`paper.execute_due`、`shadow.advance_due`；
2. `quality.freeze` 等待全部 collect，检查来源冲突、对账和数据新鲜度；
3. `diagnostics.run` 只在质量 PASS；
4. `agent.research` 等待 diagnostics；
5. `optimization.propose` 等待 research；周最后 session 才可追加 `paper.admit_orders`，仍调用 Task 18 硬风控；
6. `paper.performance`、`daily.brief` 等待当日相关事实；
7. `audit.manifest` 最后保存链头清单。

Task 19 的次日 09:55 执行不是回写前一日 pipeline：根任务为每个已提交订单创建独立 `paper.execute_due` job，effective_at 来自下一 session 的 09:55 ET 权威。Task 19 自身仍决定可成交窗口和终态。

未实现或未配置的 provider handler 必须 `skipped_capability`；fixture 验收使用注入 registry。任何 quality 非 PASS 或 cutoff 到达后仍失败，所有 proposal/order-producing 后继 job 原子标为 `skipped/dependency_data_quality`。

## 6. 审计链与脱敏导出

### 6.1 链式写入

`audit_events` 向前增加：

- `event_seq bigint` 全局唯一且从 1 连续；
- `occurred_at timestamptz`；
- `previous_hash`（genesis 为 64 个 `0`）；
- `canonical_event text`、`event_hash`、`chain_version = audit_jcs_sha256_v1`。

`append_audit_event` 获取全局 advisory transaction lock，读取最后链头，严格校验 payload 后计算：

```text
event_hash = SHA256(previous_hash UTF-8 || RFC8785_JCS(chain_event_without_hashes))
```

允许的 JSON 子集是 null/bool/安全整数/UTF-8 字符串/数组/字符串键对象；禁止 float、NaN、Infinity、Decimal 和超出 JavaScript 安全整数的值，精确数值必须在业务边界先转字符串。数据库触发器验证 seq/previous hash 连续、`canonical_event::jsonb` 与行字段语义一致，并用 PostgreSQL 内置 `sha256(bytea)` 重算 event hash；同时拒绝 audit `UPDATE/DELETE`。迁移按 `(created_at,id)` 为历史 audit 行确定性回填。

所有现有 AuditEvent 生产点改用该服务，禁止生产代码直接构造链字段。每日 `audit.manifest` 以内容寻址键写入对象存储，并在 `audit_chain_manifests` 保存 session、first/last seq、head hash、对象 URI、SHA-256 和字节数；同内容重放幂等。

### 6.2 风险审计读模型与导出

`GET /internal/v1/risk-audit` 返回 allowlist 摘要，kind 固定为 `quality/risk/agent_cost/job/reconciliation/audit`，使用签名 cursor 和 `(occurred_at,id)` 稳定排序。响应不包含原始 audit payload。

`GET /internal/v1/risk-audit/export?from=&to=&kind=`：

- 日期范围最多 31 天、最多 100,000 行；超限 fail closed。
- 返回 `application/x-ndjson` + `Content-Encoding: gzip`，gzip `mtime=0` 保证同输入字节一致。
- 每行只含 seq/time/kind/action/outcome/safe reason/schema、previous/event hash；actor 只降级为 `user/system/service`。
- 保留源链 hash 便于验证连续关系，但不含可逆识别符或原始 payload。
- BFF 只代理受限二进制响应，验证 content type/encoding/长度，设置安全 filename；不得缓存或记录 body。

## 7. Readiness、重试与回撤确认 API

### 7.1 `/health/ready`

受现有 HMAC + nonce 保护，任何失败统一返回 503 和非敏感 component 状态：

- database：`SELECT 1`；
- migration：数据库 `alembic_version` 等于代码单一 head；
- object_store：只做 bucket/head 能力检查，不列举对象；
- worker：兼容版本的 general heartbeat 不超过 30 秒。

不返回 DSN、endpoint、bucket、worker ID、迁移历史、异常字符串或凭据存在性。Fosun/Alpaca/SEC/FRED/OpenAI 状态不属于 process readiness。

### 7.2 安全重试

`POST /internal/v1/jobs/{job_id}/retry` 要求 HMAC、同源 BFF、`Idempotency-Key`、`If-Match` Job version。只允许 `failed/dead_letter + manual_safe + manual_retry_count < 2`；重排队复用原 JobKey/input hash/payload，不接受新 payload。

### 7.3 内部模拟盘回撤确认

`POST /internal/v1/risk/drawdown-lock/acknowledge` 不接受 account ID；服务端锁定唯一 `simulation_write_internal` account。只有当前状态 `recovery_eligible`、最新冻结绩效 drawdown 低于 6%、expected event seq 匹配时，追加 `user_acknowledged=true` 的 `unlocked` 事件。请求要求 Idempotency-Key/If-Match，结果、audit 和 outbox 同事务。它不创建订单，也不修改硬阈值。

## 8. `/risk-audit` 页面

- 页面保持 NEXUS 深绿/米白 token 和现有 AppShell，不引入新视觉系统或图表依赖。
- 顶部显示 process readiness、数据质量、回撤锁和失败 job 数量；所有值来自 typed BFF。
- URL 保存 `kind/date/status` 筛选；事件使用语义列表/表格，移动端优先卡片，只有不可拆审计字段允许带名称的局部横向滚动。
- 统一 `loading/empty/ready/partial/stale/error/forbidden`；STALE 保留旧值但禁止 retry/ack。
- retry 与 ack 使用清晰影响说明、版本/幂等、未知结果同键确认、错误摘要焦点和原位成功状态；不提供通用“重跑全部”。
- 导出按钮只下载已选日期/kind 的脱敏 gzip NDJSON，不在页面展开原始 payload。
- 375/390/768/1024/1440 和 390×844 横屏无页面级横向溢出；44px 点击区、可见焦点、reduced motion 和 Axe critical/serious 0。

## 9. 迁移、兼容与回滚

- 主运行权威迁移为 `0020_resilient_automation_audit`，父节点 `0019_paper_performance_root`；不回改旧迁移。Slice 8 审查发现 `attempt_count <= max_attempts` 会使已耗尽的 dead-letter 在人工重排队后仍永远无法领取，因此追加单头前向迁移 `0021_manual_job_retry_allowance`（父节点 `0020`）。它仅将最多两次 `manual_retry_count` 作为额外领取额度，自动重试上限不变，且 DB trigger 禁止越过 API 同时修改尝试次数。
- 迁移先新增 nullable 列/新表，确定性 backfill Job 状态与 audit 链，再加 NOT NULL/check/unique/触发器。
- 现有 `QUEUED/RUNNING/COMPLETED/ABSTAINED/FAILED` 分别映射为 `queued/running/succeeded/skipped/failed`；无法映射的行使迁移失败，不能猜测。
- 当前 `paper.outbox` 调用保持兼容；Agent admission/worker 同步改成小写状态与新 repository。
- downgrade 会删除 Task 20 attempt/dependency/heartbeat/manifest 事实并移除链列；只适用于无须保留 Task 20 运行事实的本地环境。已有事实时优先前向修复并先备份。
- `0021` 如已产生超出原 `max_attempts` 的人工尝试事实，downgrade 会因旧 check 无法重建而 fail closed，不会静默删除或改写 attempt 历史。

## 10. 验收标准

- 并发 claim 无重复；expired lease reclaim 后旧 fence 无法提交；同一业务副作用最多一条。
- heartbeat 只有当前 token 可续；terminal attempt/Job 不可越权重开。
- outbox 在业务提交后崩溃可恢复；发布成功但 DB 未确认时只按相同 event ID 重发；安全错误重试、unsafe 拒绝和 dead letter 有反例。
- 正常日/半日市、DST、18:00 cutoff、依赖顺序、周末/非 session、低优先级 strategy queue 全部有确定性测试。
- audit Unicode/嵌套/整数 golden vector、并发 append、断链、修改/删除、导出脱敏、gzip 字节一致性全部通过。
- readiness 每个依赖独立失败为 503 且无敏感信息；provider outage 不影响 ready。
- job retry 与 drawdown ack 覆盖版本冲突、幂等并发、未知结果恢复、越权/错误状态拒绝。
- `/risk-audit` 全状态、响应式、键盘、焦点、Axe 和无敏感字段 E2E 通过。
- 最终运行 Engine/Web/Compose 全量门禁和独立安全审查，未解决 Critical/Important 为 0。
