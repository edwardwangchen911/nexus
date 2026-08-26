# Task 20 韧性自动化、恢复与审计实施计划

> 实施依据：`docs/superpowers/specs/2026-08-27-task-20-resilient-automation-audit-design.md`
> 方法：每个切片先写失败测试，再写最小实现，运行定向门禁并独立本地提交。全程不激活 broker、provider、部署或外部消息。

## Slice 1：规范 Job、重试与审计纯函数

- [x] 新增 `engine/tests/unit/jobs/test_models.py`，覆盖 JobKey UTC/字符边界、canonical key/hash、状态转换、队列优先级、retry policy 和 fencing 值对象。
- [x] 新增 `engine/tests/unit/audit/test_chain.py`，覆盖严格 JCS 子集、Unicode golden vector、全局 previous/event hash 和非法 float/Decimal/大整数拒绝。
- [x] 新增 `engine/tests/unit/audit/test_redaction.py`，覆盖 action 分类、actor 降级、字段 allowlist 和敏感键/精确金额不可导出。
- [x] 实现 `engine/src/nexus/jobs/models.py`、`engine/src/nexus/audit/canonical.py`、`engine/src/nexus/audit/redaction.py`。
- [x] 运行三组单测（37 passed）、Ruff、Mypy；提交 `feat(engine): define resilient job and audit domains`。

## Slice 2：0020 运行权威迁移

- [x] 新增 `engine/tests/integration/test_automation_migration.py`，先断言单头、历史 Job 状态映射、audit 链回填、新表/列/check/unique/触发器和 `0019 <-> 0020` 往返。
- [x] 新增 `engine/alembic/versions/0020_resilient_automation_audit.py`；扩展 jobs/job_leases/outbox/audit，新增 job_attempts/job_dependencies/worker_heartbeats/outbox_delivery_attempts/audit_chain_manifests。
- [x] 更新 `engine/src/nexus/persistence/models/runtime.py`；数据库反例覆盖非法状态、fence 倒退、终态篡改、audit 断链与 UPDATE/DELETE。
- [x] 更新所有迁移 head 断言为 `0020_resilient_automation_audit`。
- [x] 运行迁移往返和 PostgreSQL 守卫；迁移兼容同步接入现有 Job/Outbox/审计生产点，待本切片提交。

## Slice 3：Job repository、lease 与 fencing

- [x] 新增 `engine/tests/integration/test_job_leases.py`，覆盖幂等入队、优先级、依赖、并发 SKIP LOCKED、heartbeat、过期重领、token/fence 拒绝和终态。
- [x] 实现 `engine/src/nexus/jobs/repository.py`：enqueue/claim/heartbeat/finalize/fail/reclaim，所有时间由调用方注入 UTC。
- [x] 新增故障注入，证明 handler 业务事实与 Job/attempt/audit/outbox 同事务，旧 worker 不可在被 fence 后提交。
- [x] 运行 Job 定向集、Ruff、Mypy；待本切片提交。

## Slice 4：通用 worker 与现有 Agent 适配

- [x] 新增 `engine/tests/integration/test_job_worker.py`，覆盖严格 registry、未知 handler、timeout、automatic/manual/never retry、dead letter 和空闲 heartbeat。
- [x] 实现 `engine/src/nexus/jobs/registry.py`、`engine/src/nexus/jobs/worker.py`；handler 只能得到 typed payload、job/fence context 和 session factory。
- [x] 把 `engine/src/nexus/worker.py` 的 Agent 路径改为 `agent.research` adapter；admission 使用完整冻结权威哈希并跨幂等键复用同一 Job/Run，不改变模型预算。
- [x] 保持 broker composition root 与 general worker 隔离；Agent-only root 只领取已注册类型，未注册真实 broker handler。
- [x] 运行 Agent/worker 回归（106 passed）、Ruff、Mypy；待本切片提交。

## Slice 5：通用事务 Outbox

- [x] 新增 `engine/tests/integration/test_outbox.py`，覆盖并发领取、事件 schema registry、相同 event ID 重发、安全退避、unsafe 拒绝、dead letter 和 attempt 事实。
- [x] 实现 `engine/src/nexus/outbox/models.py`、`registry.py`、`dispatcher.py`；保留 `paper/outbox.py` 兼容委托与既有异常语义。
- [x] 添加“业务事务已提交、dispatcher 前进程退出”和“publisher 成功、published 提交前退出”故障测试。
- [x] 运行 Outbox/Paper 定向回归（6 passed）、Ruff、Mypy；待本切片提交。

## Slice 6：每日 Pipeline 与日历调度

- [ ] 新增 `engine/tests/unit/jobs/test_daily_schedule.py`，覆盖正常日、半日市、DST、非 session、close+30、18:00 cutoff 和次日 09:55。
- [ ] 新增 `engine/tests/integration/test_daily_pipeline.py`，覆盖严格 DAG、依赖领取、quality PASS、cutoff skip、周最后 session、低优先级 strategy R&D 和重复 scheduler tick。
- [ ] 实现 `engine/src/nexus/jobs/daily_pipeline.py` 与 typed stage payloads；fixture registry 运行完整顺序，未注册/未激活 provider 明确 skipped。
- [ ] 为 Task 17/19 安全本地 handler 接入 shadow advance、paper execute/performance；proposal/order 仍调用现有确定性 admission/硬风控。
- [ ] 运行 pipeline 与受影响域回归；提交 `feat(engine): schedule deterministic daily pipeline`。

## Slice 7：链式审计、manifest 与 gzip 导出

- [ ] 新增 `engine/tests/integration/test_audit_chain.py`，覆盖迁移后 append、并发全局序列、断链、不可变和所有现有业务生产点。
- [ ] 实现 `engine/src/nexus/audit/service.py`；逐一替换生产代码中的直接 `AuditEvent(...)`。
- [ ] 新增 `engine/tests/integration/test_audit_export.py`，覆盖 31 天/100k 上限、kind/date、脱敏、稳定排序、gzip mtime=0、字节一致和无敏感字段。
- [ ] 实现 `engine/src/nexus/audit/export.py` 与内容寻址 manifest writer protocol；测试只用内存 object store。
- [ ] 运行审计/既有事务回归；提交 `feat(engine): seal and export redacted audit chain`。

## Slice 8：Readiness、风险审计与动作 API

- [ ] 新增 `engine/tests/integration/test_health_ready.py`，覆盖 HMAC、DB/object store/migration/heartbeat 独立失败、503 安全响应和 provider outage 不影响 ready。
- [ ] 新增 `engine/tests/integration/test_risk_audit_api.py`，覆盖签名 cursor/kind/date、Job safe retry、If-Match/幂等并发和 unsafe/未知状态拒绝。
- [ ] 新增 `engine/tests/integration/test_drawdown_ack_api.py`，覆盖唯一内部账户、recovery threshold、append-only event、版本/幂等/并发和不创建订单。
- [ ] 实现 health/jobs/risk_audit routes/schemas/services，接入 `main.py` 并生成 OpenAPI；不返回 raw payload 或运行时配置。
- [ ] 运行 API/HMAC/生成合约定向集；提交 `feat(api): expose readiness and safe recovery`。

## Slice 9：BFF 与 `/risk-audit` 页面

- [ ] 新增 BFF boundary/runtime schema 单测，覆盖 JSON 列表、retry/ack 写保护和 gzip NDJSON 的 type/encoding/size/filename 验证。
- [ ] 实现四个 BFF route、Engine client allowlist 和生成 runtime schema；浏览器不接触 Engine HMAC。
- [ ] 使用已有 AppShell/DataState/token 实现 `/risk-audit`，覆盖 URL 筛选、readiness/quality/lock/job 摘要、事件、retry、ack、unknown recovery 和导出。
- [ ] 新增 UI 单测覆盖七状态、STALE 禁止动作、焦点/aria-live、无原始 payload/ID/精确金额。
- [ ] 运行 Web 单测、lint、typecheck、generated check；提交 `feat(web): build risk and audit console`。

## Slice 10：E2E、全量门禁与验收

- [ ] 新增 `tests/e2e/risk-audit.spec.ts`，覆盖 375/390/768/1024/1440/横屏、筛选、retry、ack、unknown、导出和失败状态。
- [ ] 运行 Axe 与视觉检查，要求 critical/serious 0、无页面级横向溢出、44px 目标和可见焦点。
- [ ] 运行 Engine 全量 Pytest/Ruff/Mypy；Web 全量 Vitest/ESLint/TypeScript/generated/build/E2E；Compose app/broker config。
- [ ] 运行秘密扫描与单执行者分离安全审查，重点检查 stale lease、重复副作用、unsafe retry、审计泄密/断链、readiness 泄密和 broker 队列越权。
- [ ] 修复所有 Critical/Important，重跑受影响门禁；新增 Task 20 review 文档。
- [ ] 更新主计划和本地进度账簿；提交 `docs: record Task 20 automation acceptance`。

## 中止条件

出现真实账户/券商写操作、真实订单、复星只读真实调用或凭据/权限、部署、外部消息发送、付费、破坏性操作、Git 历史改写或明显范围扩大时立即停止并请求确认。
