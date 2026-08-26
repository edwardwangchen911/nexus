# Task 21 最终离线发布、运维与复星只读激活门禁设计

**状态：** 可直接编码  
**范围：** Phase 0 离线 fixture 闭环、CI/供应链门禁、本地容器启动、备份恢复演练、故障 runbook、复星真实只读激活判定  
**明确排除：** 部署、真实复星网络调用、凭据配置、复星模拟/实盘写入、真实订单、外部告警发送、Stage 2 策略自进化

## 1. 当前裁决

Task 21 完成后必须把两类结论分开：

1. `phase0_offline_ready`：离线研究、回测、Shadow、内部模拟盘、自动化与审计的代码/演练门禁全绿。
2. 复星真实只读激活结果：只能是 `activated_read_only` 或 `fixture_with_next_action`。

当前必须是 `fixture_with_next_action`，原因是：

- 历史对话中的 API key 已暴露，没有“已撤销+已在 secret manager 直接配置新 key”证据；
- SDK `v1.2.0@891d95fb4967efaa682387c1b35966ceea38fb21` 缺少标准许可证，且已审查代码存在 `verify=False`；
- provider manifest 仍为 `blocked`，书面使用许可、安全审查、TLS 修复和 provider test vector 证据均为 null；
- 未经用户对真实账户/凭据/网络探测的单独确认。

本任务可实现和验证激活门禁，但不得为追求 `activated_read_only` 绕过上述条件。

## 2. 强制不变量

- CI、E2E、演练与本地 Compose 默认只使用 synthetic fixture；任何 provider test 对非 loopback 连接 fail closed。
- 浏览器不接触 Engine/gateway HMAC、mTLS、provider 凭据或外部账户 ID。
- `/broker-simulation` 和 `/live-approvals` 必须没有 form、订单输入、批准/提交/下单按钮或隐藏写 API 请求。
- 数据缺失、PARTIAL、STALE、源冲突、对账失败、Agent 弃权或回撤锁定时，默认保持现状，不生成/提交订单。
- CI 不在仓库、日志、artifact 或 snapshot 中写入密钥、完整环境变量、真实账户数据或 provider 响应。
- 备份恢复演练只能操作自身创建且名称带随机 run ID 的数据库/对象前缀；拒绝默认库、工作库、根前缀和未解析目标。
- 实盘路由不得因“最终验收”而解锁；Phases 2–4 仍保持锁定。

## 3. 离线用户旅程与 UI 门禁

### 3.1 旅程集

`tests/e2e/overview.spec.ts`、`locked-routes.spec.ts`、`failure-modes.spec.ts` 必须用 Playwright route fixture 覆盖：

1. 无数据 -> 需研究 -> READY 组合 -> 诊断 -> 三方案 -> Shadow 启动；
2. Agent abstention；
3. 市场/证据源冲突；
4. stale broker snapshot；
5. reconciliation failure；
6. internal paper partial fill；
7. drawdown locked/recovery eligible 但未确认；
8. broker simulation/live approval 两条锁定路由。

每个失败旅程必须断言没有不当的 proposal/order/recovery 控件，不能只检查提示文案。

### 3.2 视口与可访问性

- 视口：375×812、390×844、768×1024、1024×768、1440×1000、844×390。
- 390 和 1440 保存视觉截图；所有视口页面级横向溢出为 0。
- Axe critical/serious 为 0；交互目标至少 44×44，相邻目标可点区间距至少 8px。
- 必测：skip link、路由切换后 `main` 焦点、纯键盘图表/表格、错误摘要焦点、局部滚动标签、sticky 状态焦点可见、200% zoom 和 `prefers-reduced-motion`。

## 4. 本地运行时与 Compose

- 需要 Docker API 与 Compose v2 兼容运行时，不强制 Docker Desktop；Docker Desktop、OrbStack 或 Colima 均可，以 `docker version` 和 `docker compose version` 为准。
- 新增只负责本地/test 的 `nexus-engine-object-store-init` 命令：对精确配置 bucket 执行 head/create；production 缺 bucket 必须失败，不自动创建。
- Compose `object-store-init` 使用 engine image，在 minio healthy 后创建单一 bucket；API/worker 只在 init 成功后启动。
- `app` 仍不得包含 fosun-gateway/broker-worker/broker secret；`broker` 是显式 profile，创建 Compose 配置不等于启动或真实读取。
- `.openai/hosting.json` 不修改，Task 21 不部署。

## 5. CI 与供应链

`.github/workflows/ci.yml` 使用固定大版本 actions，按权限最小化拆成：

1. `web`：`npm ci`、audit high+、lint、typecheck、unit、generated check、build；
2. `gateway-offline`：开启 loopback-only socket guard，运行 104+ gateway 单测/契约/安全测试；
3. `engine-postgres`：PostgreSQL 17 + MinIO，从空库迁移，运行 Engine 全量、Ruff/Mypy；
4. `e2e`：Chromium 全量，失败时上传 Playwright report/test-results；
5. `policy`：Compose 两 profile、禁止路由、密钥模式扫描、npm/Python 依赖和许可证清单、SDK 未经 admission 不在 lock 中。

Provider 测试的“离线”不能仅依赖无效 URL；测试进程对非 loopback TCP 连接必须显式拒绝并有自测反例。CI 安装依赖和 `npm audit` 可访问包仓库，但运行 provider test 时不得访问外网。

## 6. 备份/恢复演练

新增可测试的演练边界，仅对自创建 fixture 运行：

1. 创建 `nexus_drill_source_<run_id>` 与 `nexus_drill_restore_<run_id>`；
2. 从空库 upgrade 到唯一 Alembic head，写入确定性 fixture（内部模拟账户、平衡 ledger、最新绩效/READY 投影、audit manifest）；
3. 使用 `pg_dump --format=custom --no-owner --no-privileges`，在新库使用 `pg_restore --exit-on-error --single-transaction`；
4. 对原前缀、backup 前缀和 restore 前缀执行有界对象复制，逐对象校验 SHA-256/长度；
5. 比较迁移 head、关键权威表行数、ledger 借贷平衡、最新绩效/READY 根和 object hash；
6. 记录实测 backup/restore/verify 耗时和输入字节，只报告本次 fixture RPO/RTO，不外推生产值；
7. `finally` 删除两个精确随机库和三个精确前缀，残留计数必须为 0。

演练失败时保留脱敏计数/错误码，不输出 dump 内容、连接密码或对象内容。

## 7. 复星只读激活结果

### 7.1 结果合同

```python
class FosunActivationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    status: Literal["activated_read_only", "fixture_with_next_action"]
    contract_hash: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    checked_at: datetime
    capability_expires_at: datetime | None
    next_action: str | None
```

`contract_hash` 绑定：result schema/version、provider manifest SHA-256、SDK subject，以及若存在时的脱敏 probe artifact SHA-256。不包含账户 ID、持仓、金额、原响应或凭据。

### 7.2 离线 gate

`nexus-fosun-activation-gate` 只读 manifest/脱敏 probe artifact，本身没有 provider transport：

- manifest 非 admitted、evidence 不齐、SDK subject 不匹配、probe 缺失/过期/非闭合操作集，统一返回 `fixture_with_next_action` 和一个稳定、不含敏感数据的 next action；
- 只有所有书面证据齐全、已审计 probe 仅覆盖 Session/Accounts/CashSummary/Holdings/OrderList/CashFlows/quote/最小 K-line，且权限/分页/调整/限流/契约均通过时，才能返回 `activated_read_only`；
- 任何 write/simulation/subscription/Webhook 操作名使 gate 拒绝整个 artifact。

### 7.3 真实 probe 确认门

真实 probe 不属于自主本地实施。只有同时满足以下条件才可向用户请求一次明确确认：

- 已暴露 key 的 revoke 证据；
- 新 key、客户端私钥/服务端公钥只在 secret manager，不通过对话/命令行传递；
- 固定出站 IP allowlist 已配置；
- SDK admission 的许可、安全、TLS、test vector 四类证据已完成；
- 探测计划明确操作集、请求上限、无写路由和回滚。

## 8. Runbook 与 ADR

必须新增：

- `docs/runbooks/local-development.md`：运行时预检、环境变量分域、bucket init、迁移、启停、readiness 和无 Docker Desktop 替代方案；
- `backup-restore.md`：只允许 fixture drill，精确命名/清理、验证项与恢复失败处置；
- `provider-outage.md`：liveness/readiness/业务质量区分，禁止源回退和假 READY；
- `fosun-read-only-activation.md`：admission/revoke/allowlist/secret/probe/结果/回滚门禁；
- `key-rotation.md`：Engine HMAC、gateway HMAC、mTLS、external-ID AEAD/blind-index、provider key 分域轮换；
- `docs/adr/0001-postgresql-owner.md`：PostgreSQL 是事务权威，MySQL 并非不可用，但当前 advisory lock/JSONB/deferred trigger/`SKIP LOCKED`/迁移语义使切换无收益且风险高；
- `docs/adr/0002-fosun-gateway-boundary.md`：独立进程、mTLS/HMAC/密钥域、七条只读操作和无 generic proxy/write route。

## 9. 验收与回滚

- 必须先保留 Task 21 首个失败 E2E/CI/演练反例，再修改生产代码或配置。
- 最终全量：Web lint/typecheck/unit/E2E/build/generated；Engine/Gateway Pytest；Ruff/Mypy；Compose app/broker；路由/密钥/依赖/许可证门禁；备份恢复演练。
- 清洁工作树、临时库/连接/对象前缀残留 0，且未解决 Critical/Important 为 0 后，才能声明 `phase0_offline_ready`。
- 当前 Fosun 结果必须为 `fixture_with_next_action`；这不会阻止 Phase 0 离线代码就绪，但会阻止 Phase 1 真实激活。
- 代码回滚只需停止本地容器/移除 CI 提交；演练数据库/前缀由工具 `finally` 精确清理。不改写已有 Git 历史、不部署、不推送。
