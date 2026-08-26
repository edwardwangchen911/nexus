# Task 20 韧性自动化、恢复与审计验收审查

**结论：** 通过本地验收。Task 20 在“单用户、美股研究、Shadow/内部模拟盘、实盘默认关闭”边界内满足设计规格；分离式对抗审查的 Critical/Important/Minor 未解决项为 `0/0/0`。本次没有访问真实账户，没有复星/券商写操作、真实订单、部署、密钥/权限变更、外部消息或付费行为。

## 1. 审查范围与方法

- 规格、实施和纠正范围：`10ed414..799e16f`；规格与计划为 `fb8d845`，审查纠正为 `486f96b`，验收覆盖为 `799e16f`。
- 审查对象：Job key/lease/fence/attempt、依赖 DAG、通用 worker、事务 outbox、每日调度、审计链/manifest/导出、readiness、人工安全恢复、Engine/BFF 边界和 `/risk-audit` 页面。
- 审查方式：实现后以已提交差异为输入进行一次单执行者分离式对抗审查，专项检查前视偏差、时点错配、重复副作用、stale lease、unsafe retry、队列越权、审计泄密、恢复绕过和密钥暴露。本次未启用委派，因此不是第二位人员或第二模型背书。
- 实盘边界：`general` worker 不加载 broker secret，`app` profile 不包含 broker worker/gateway；Task 20 没有激活复星读取或任何下单路径。

## 2. 已修复发现

| 级别 | 发现 | 修复与反例 |
| --- | --- | --- |
| Important | Node 服务端 `fetch` 会自动解压 gzip 响应体，但保留 `Content-Encoding: gzip`；BFF 因此把合法真实导出误判为损坏并返回 500。 | 真实本地 HTTP 输反例确认该行为；`486f96b` 同时支持原始 gzip 和已自动解压 NDJSON，两种路径都先验证行数、UTF-8 与闭合事件 schema，再保留/重建 gzip。 |
| Important | OpenAPI 对 `outcome` 只限制值类型，没有表达 Engine redactor 的键 allowlist；若上游异常返回 `account_id` 等键，原 BFF 仍会通过。 | `486f96b` 在分页与导出边界共用与 Python redactor 一致的闭合键集；嵌套 `account_id` 页面/导出反例均稳定返回 500。 |
| Important | 回撤解锁服务读取“最新绩效”时没有 `as_of <= now`，未来时点的低回撤快照可以错误授权当前解锁。 | 数据库/API 反例先证明原实现返回 200；`486f96b` 令解锁与风险摘要都只读取决策时点前的回撤事件/绩效，未来快照稳定 fail closed 为 `DATA_PARTIAL`。 |
| Important | manifest writer、内存 object-store 验收和 daily handler 都存在，但常驻 general worker 未注入对象存储，真实 `audit.manifest` 会永久 `SKIPPED_CAPABILITY`。 | `486f96b` 增加只创建的 S3/MinIO 内容寻址适配器并注入 composition root；单测覆盖 `If-None-Match`/冲突内容校验，一次性真实 MinIO 验证得到同 URI、单对象和匹配 SHA-256。 |
| Minor | 全量迁移往返测试的共享 schema 期望集未登记 Task 20 的五张新表及关键外键，导致真正完整的新 schema 被旧测试误报。 | `799e16f` 更新可选迁移组和外键权威；定向反例通过，随后从头全量得到 1400/1400。 |

## 3. 最终验证证据

| 门禁 | 最终结果 |
| --- | --- |
| Engine 全量 | 隔离 PostgreSQL 上 `1400 passed in 1322.03s (0:22:02)`；命令使用 `pytest --import-mode=importlib engine/tests -x -q`。 |
| Python 静态检查 | Ruff 全绿；Mypy `231 source files` 无问题。 |
| 复星 gateway 离线契约 | `104 passed in 1.72s`；未读取密钥、未发出真实 provider 请求。 |
| Web 单测 | 39 个测试文件、420 项全部通过。 |
| Web 静态与构建 | ESLint、TypeScript、生成合约一致性和 vinext production build 全部退出 0。 |
| 浏览器 E2E | 43 项全部通过；其中 `/risk-audit` 12 项覆盖六档视口、筛选、retry/ack、unknown 同键恢复、gzip 导出和失败状态。 |
| 可访问性与视觉 | 390/1440 截图目视通过；Axe critical/serious 0；375/390/768/1024/1440/横屏无页面级横向溢出，控件至少 44px。 |
| Compose 边界 | `app` 只包含 minio/postgres/api/worker；`broker` 才包含 fosun-gateway/broker-worker；两套 `config --quiet` 均退出 0。 |
| 对象存储契约 | 一次性本地 MinIO 首次写入/重放 URI 一致，对象数 1，下载后 SHA-256 匹配；容器随后已删除。 |
| 秘密/清理 | 受控 API key/私钥模式命中文件数 0；随机临时数据库/active connection 残留均为 0，一次性 PostgreSQL/MinIO 容器已删除。 |

`pytest engine/tests` 会因多个历史目录使用相同测试模块名而在默认 import mode 收集冲突；项目已有的权威全量命令使用 `--import-mode=importlib`。构建只报告已有代理环境提示和 vinext 无法静态分类部分动态路由的提示；两者都未导致失败或弱化运行时边界。

## 4. 架构验收结论

- Job payload 仅能通过注册的严格 Pydantic schema，任意 URL/SQL/路径/代码/prompt/凭据键和未注册 handler 均 fail closed。
- claim 使用 `FOR UPDATE SKIP LOCKED`，lease token 不可猜、fence token 单调；旧 worker 在过期/重领后不能提交任何业务事实。Job 终态、attempt、业务副作、audit 与 outbox 在同一事务收敛。
- Outbox 在业务提交后可恢复；publisher 成功但确认前崩溃只以相同 event ID 重发。未注册或不幂等 consumer 不会自动派发。
- 每日 DAG 使用冻结 XNYS 日历、半日市/DST/收盘+30 分钟/18:00 ET cutoff；未实现 provider 明确 `SKIPPED_CAPABILITY`，质量非 PASS 不会创建新提案或内部模拟订单。
- 审计事件全局单调、数据库重算 hash 且禁止 UPDATE/DELETE；manifest 按内容寻址创建，导出只包含闭合脱敏字段与源链 hash，BFF 不记录或展开原始 payload。
- Readiness 仅返回四类闭合状态，provider outage 不冒充进程故障。人工重试仅允许 `manual_safe`，回撤确认只能解锁唯一 `simulation_write_internal` 账户，不创建订单、不改风控阈值。
- `/risk-audit` 对 READY/PARTIAL/STALE/error/forbidden 等状态明确 fail closed；恢复操作使用同源、服务端 actor、HMAC、If-Match、Idempotency-Key 和未知结果同键确认，浏览器不接触 Engine 签名密钥。

## 5. 回滚与后续边界

- 本次只有本地提交，没有发布或外部状态变更；停止本地 worker/API 即停止新调度与恢复行为。
- `0021 -> 0019` downgrade 会删除 Task 20 attempt/dependency/heartbeat/manifest 事实并移除审计链列，不适用于已有需保留运行事实的环境。若已产生超过旧自动上限的人工尝试，`0021` downgrade 会 fail closed；应先备份并优先前向修正。
- manifest 上传在有界的 handler 事务内持有全局审计锁；内容寻址和超时使失败可安全重放。Task 21 的故障演练需测量对象存储慢/中断时的竞争、RTO 和孤儿内容清理，不应未测量就宣称生产值。
- Task 21 仍负责 CI、备份/恢复演练、运维 runbook、更广 E2E/故障注入与复星真实只读激活门禁。复星 API key 已在历史对话暴露，在轮换并通过出站 IP/客户端密钥门禁前不得使用。
- Stage 2 策略自进化、复星模拟盘/实盘写入、真实订单、部署和整体 NEXUS 长期目标仍未完成。
