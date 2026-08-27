# Task 21 最终离线发布、运维与复星只读激活门禁实施计划

> 实施依据：`docs/superpowers/specs/2026-08-27-task-21-final-operational-gates-design.md`  
> 执行原则：TDD、离线 fixture、最小权限、精确清理。真实复星调用、凭据/权限、部署、外部消息和任何下单仍需单独确认。

## Slice 1：最终边界与失败 E2E

- [x] 新增 `tests/e2e/locked-routes.spec.ts`，先证明 broker simulation/live approval 页无 form、无写按钮、无写 API 请求；覆盖移动/桌面和键盘。
- [x] 新增 `failure-modes.spec.ts`，覆盖 Agent abstention、source conflict、stale broker、reconciliation failure、partial fill、drawdown lock，并断言 fail-closed 操作面。
- [x] 新增 `overview.spec.ts`，覆盖无数据 -> 研究 -> READY -> 诊断 -> 三方案 -> Shadow 启动的离线跨页旅程。
- [x] 保存首个失败证据；只修复必需的焦点、文案、语义或容器问题，不重设已验收页面。
- [x] 运行三组定向 E2E 与既有 43 项回归；提交 `test(e2e): close offline investment journeys`。

验收记录（2026-08-27）：首个红灯是概览 empty 状态缺少“研究美股证据”入口；修复后定向 E2E `11 passed`，全量 E2E 在 3 workers 下 `54 passed`，Web 单测 `422 passed`，lint/typecheck/generated/build 通过。首次 4 workers 全量运行有 1 项在 `page` 初始化阶段超时，该市场测试单独重跑 `2 passed`，降为 3 workers 后全量稳定通过。

## Slice 2：全局可访问性与视觉门禁

- [x] 建立共用 Playwright 可访问性 helper，覆盖六档视口、Axe critical/serious、页面溢出、44px/8px、200% zoom 和 reduced motion。
- [x] 测试 skip link、路由切换 main 焦点、表格/图表键盘、局部滚动标签、sticky 焦点可见和错误摘要焦点。
- [x] 仅修复失败的通用 AppShell/token/组件，重跑所有 Web 单测和 E2E；保存 390/1440 视觉快照。
- [x] 提交 `fix(web): enforce final accessibility gates`。

验收记录（2026-08-27）：首个红灯暴露路由链接整页导航后 `main` 不聚焦，且 error `DataState` 无可聚焦 `alert`。改为受控的同源导航，并为 `main:focus` 和 error summary 提供可见焦点。定向可访问性 E2E `10 passed`，全量 E2E `64 passed`，Web 单测 `424 passed`，lint/typecheck/generated/build 通过；390/1440 截图由 Playwright 保存于本地 `test-results`。

## Slice 3：本地 Compose 启动与运维文档

- [x] 为 `nexus-engine-object-store-init` 先写 development/create、existing replay、production missing fail-closed 单测，再实现最小 boto3 bucket init CLI。
- [x] 修改 `infra/compose.yaml` 增加 `object-store-init`，API/worker 依赖其成功；更新 `infra/env.example` 的分域和非空验证说明。
- [x] 新增 local-development/provider-outage/key-rotation 三份 runbook 和 PostgreSQL/Fosun gateway 两份 ADR；更新 README 入口。
- [x] 对一次性本地 Compose 执行 config、启动、bucket/readiness 验证和精确停止；提交 `feat(ops): make local runtime reproducible`。

验收记录（2026-08-27）：对象存储初始实现前首先保留了 missing module 红灯，空值/非绝对 endpoint、离线 worker 默认值、可覆盖回环端口和镜像迁移资产也分别以红灯锁定。定向单测 `16 passed`；Engine 单测 `686 passed`，readiness/outbox PostgreSQL 回归 `9 passed`，Ruff 全量和 Mypy `232 source files` 通过。一次全量预跑在 `436 passed` 后暴露 paper outbox 测试使用已过期的固定 dispatch 时间；最小复现稳定失败后，改为从持久化 `available_at` 推导测试时钟，单项回归通过，未修改生产 dispatcher。Docker 29.7.2/Compose v5.4.0 下 app/broker config 均通过。独立 `nexus-task21-slice3` project 实测 bucket `created -> existing`、空库迁移至 `0021_manual_job_retry_allowance`。首次 readiness 发现并发 heartbeat 与请求起始时间竞态，先增加失败单测，改为选出 heartbeat 后取生产观察时间；重建后 `/health/ready` 返回 200 且四组件全部 `ready`。精确 down 后 container/volume/network 残留为 0，所有 `nexus-task21-*` 临时容器残留为 0，其他现有 NEXUS 容器未受影响。

## Slice 4：备份/恢复演练

- [x] 新增操作测试，先断言精确随机库/前缀、`pg_dump/pg_restore` 参数、校验集和任意失败的 finally 清理。
- [x] 实现 fixture-only drill runner，备份/恢复 PostgreSQL 和 MinIO 前缀，校验迁移 head、行数、ledger 平衡、最新绩效/READY 根和 object hash。
- [x] 在一次性本地 PostgreSQL/MinIO 执行真实演练，记录本次 fixture 的 backup/restore/verify 耗时、字节数、RPO/RTO 和残留 0 证据。
- [x] 新增 `docs/runbooks/backup-restore.md`；提交 `test(ops): prove backup and restore recovery`。

验收记录（2026-08-27）：先保留 missing module 红灯，最终 14 项安全测试覆盖随机目标、精确命令、动态回环端口、`noexec,nosuid` tmpfs、8 表校验集、数据库 manifest 与对象的精确绑定、六阶段任意失败 cleanup、主失败与 cleanup 双错误保留、production 早期拒绝和 CLI 错误脱敏。首次真实演练暴露 ORM 无 relationship 时复合 FK 父子 flush 顺序，保留数据库约束并改为先 flush 父根。收紧隔离后重跑：PostgreSQL custom dump 736,456 bytes，1 个对象/244 bytes；backup 0.237s、restore 0.428s、verify 0.091s，本次 fixture RPO 0.132s/RTO 0.520s。migration head、8 表计数、平衡账本、最新绩效/READY/审计根、数据库 manifest 绑定和三前缀对象哈希全部一致；演练前后随机容器数均为 0，database/object/container residue 均为 0。这些数字只是本次 fixture 实测，不代表生产承诺。

## Slice 5：CI、离线网络和供应链门禁

- [x] 新增 gateway loopback-only socket guard 与自测；在该 guard 下运行全部 gateway 测试。
- [x] 新增禁止路由/密钥模式/依赖与许可证清单/SDK lock admission 策略脚本及测试。
- [x] 创建 `.github/workflows/ci.yml`，拆分 web/gateway-offline/engine-postgres/e2e/policy，最小 `contents: read`，失败时仅上传脱敏 Playwright artifact。
- [x] 本地通过 workflow 语法、所有策略脚本和全量静态门禁；提交 `ci: add offline release gates`。

验收记录（2026-08-27）：首个策略红灯证明 `.github/workflows/ci.yml` 缺失；随后 10 项 policy test 覆盖真实回环正例、保留地址/DNS 反例、可恢复且幂等的 socket guard、gateway 禁止写/Webhook surface、密钥模式、SDK lock/admission 一致性、许可证、镜像 digest、非 root runtime、Docker context 与 CI 结构。guard 自测和其下 gateway `104 passed`；repository policy inventory 761 个第三方依赖、0 finding。首次 `pip-audit` 发现 cryptography/pyarrow/pytest 共 11 条已知漏洞，升级到 50.0.1/23.0.1/9.0.3 并配套 pytest-asyncio 1.4.0 后清零；完整 npm 审计另发现 16 条（12 high），升级 Cloudflare/Vinext/Vite/Wrangler、React RSC 与传递依赖后清零，最终两类 audit 均为 0。五个 job 使用 `contents: read`、完整 action SHA、无 Secrets；容器使用 tag＋digest，Engine/Gateway 最终 non-root，失败 artifact 仅 `test-results/**`/3 天/排除隐藏文件。Workflow YAML、uv 0.12.1 lock、两 Compose profile、两个 Dockerfile check 和完整 cache-only image build 通过；Web unit `424 passed`＋build，Engine unit `700 passed`，空库迁移后全 Engine `1428 passed in 22:52` 且临时容器残留 0，Chromium E2E 安装前先保留 launch 红灯，安装 Playwright 锁定浏览器并完成依赖升级后 `64 passed`，Ruff/Mypy `251 source files` 通过。GitHub-hosted CI 尚未推送/运行，本记录只证明本地可复现门禁。

## Slice 6：复星 SDK admission 与只读激活 gate

- [x] 补全 `docs/runbooks/fosun-sdk-admission.md` 的 candidate/current-main/许可/TLS 当前证据，不安装被阻塞 SDK。
- [x] 为 `FosunActivationResult` 、脱敏 probe artifact 与 contract hash 写纯函数单测，覆盖证据缺失、过期、多/少操作、write 操作和成功反例。
- [x] 实现无 transport 的 `nexus-fosun-activation-gate`；当前 manifest 生成且只能生成 `fixture_with_next_action`。
- [x] 新增 `docs/runbooks/fosun-read-only-activation.md`，记录真实 probe 的确认门、操作白名单、请求上限、脱敏 artifact 和回滚。
- [x] 在不接触凭据/账户的情况下运行当前 gate；提交 `feat(gateway): add guarded read-only activation result`。

验收记录（2026-08-27）：从 missing module 收集红灯开始，最终 24 项激活单测覆盖四类 admission
证据缺失、probe 过期、多/少/重复操作、下单/撤单/模拟/Subscription/Webhook 操作、
manifest/SDK subject 错配、每操作和总请求上限、六类证据失败、敏感字段拒绝、精确 artifact
字节哈希、未来时间和唯一成功反例。完整 loopback-only gateway 测试 `128 passed`，Ruff/Mypy、uv lock、10 项
repository policy 与 761 个依赖/0 finding 清单通过。公开仓库重新确认 candidate
`v1.2.0@891d95fb4967efaa682387c1b35966ceea38fb21`、current main
`7f0fb48f3e1ce04329b990cf16f391cf59078137`；两者仍无标准许可文件且仍有 `verify=False`，SDK 未
加入 lock。无 probe 执行当前 gate 稳定返回 `fixture_with_next_action / COMPLETE_SDK_ADMISSION`，
`--require-activated` 退出 2；未读取凭据、账户或调用 provider。

## Slice 7：最终演练、审查与验收

- [ ] 从清洁工作树运行 Web/Engine/Gateway/Compose/E2E/策略/演练全量门禁，记录精确总数与耗时。
- [x] 运行单执行者分离式架构/安全审查，重点复核真实路由、CI 外网、artifact 泄密、备份目标、恢复根、激活绕过和 Compose secret 域。
- [x] 修复所有 Critical/Important，重跑受影响门禁；新增 Task 21 review 文档。
- [ ] 更新 README、主计划和本地 progress ledger，明确 `phase0_offline_ready` 与 `fixture_with_next_action` 两个结论。
- [ ] 提交 `test: close NEXUS phase 0 and read-only phase 1 gates`。

## 中止条件

只有真实复星/账户网络探测、新凭据/secret manager/IP allowlist/客户端密钥、真实写操作/订单、部署、外部消息、付费、破坏性操作、Git 历史改写或超出已批准 Phase 0–1 边界时停止并请求用户确认。
