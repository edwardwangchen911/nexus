# 盘外三股纯研究 v2 实施计划

Goal: 正常 manual research_only 入口在盘外运行完整候选研究并可读回、收尾及独立下一准入。
Spec: 已完成独立评审 nexus-research-time-identity-contract-20261022-01；完整原文保存在主目录 .secrets/research-timing-contract-20261022/review-response.md。
Architecture: v1 保持；v2 请求固定版本与精确作用域，准备输入后 DB 时钟冻结共同 asof，不创建 PaperCycle。新观察身份是来源事实，不是当前远端权威。原生三阶段 SDK、用量预算和 Job 生命周期复用。

## 全局约束
- 不重放失败/未知历史，不改断言掩盖问题，不启用调度/交易或扩大费用。
- 正式身份/时效/行情/组合/风控守卫保留；预研 Output 在服务端拒绝进入正式消费。
- 所有源码在隔离 worktree；root 用户改动保持。主线串行集成。
- 新绑定落盘后回滚保留 schema/读回；停止新 v2 准入而非路由旧 Paper 请求。

## 任务与验收
- [x] 1. 观察身份（独立 worktree）：新 source/schema/policy，原始证据精确完整、无未来依赖、冲突拒绝。RED→GREEN 前日/未来/损坏/变更/旧兼容。
- [x] 2. 原生研究用途（独立 worktree）：prepare_research_snapshot(research_only=True)、admit_research_run(research_only=True)；新 immutable prompt，真实日线 authority，正式消费者拒绝旁路。RED→GREEN。
- [x] 3. 主线请求绑定：contract_version=2 仅 research_only，新 immutable manifest/member FK+unique；v1 hash/event 不变；全批次原子绑定。RED→GREEN 版本事件/并发幂等/全量绑定。
- [ ] 4. 主线正常入口/宿主/收尾：绕过纯研究不适用的 session/quote/risk；保留 scope、预算、账户互斥；逐股原生 Job/lease/usage 结算及未调用取消；下一请求完整准入。RED→GREEN 跨日/失败/unknown。
- [x] 5. 页面（独立 worktree）：可见各股状态、内容限制、冻结时间、观察身份，旧历史兼容；正常 GET 无副作用。RED→GREEN。
- [ ] 6. 集成审查：隔离 PG 实际生产者、API schema、页面、静态检查、必需 CI。修正后定向回归。
- [ ] 7. 合入兼容部署：核对版本/健康、备份与回滚；一个有界真实新三股请求，DB/API/页面/收尾/下一准入证据，完成即结束当前切片。

## Review Focus
跨UTC/NY午夜；同内容新回执不伪装更新；并发丢回执不重复Job；无cycle不是未调用证据；内容partial不冒充投资结论充分。

## 接口
Identity: register_observation_identity_binding(session, instrument_id=...); read_observation_identity_bindings(session,instrument_id=...,as_of=...,evidence_ids=...).
Runtime: ensure_research_only_prompts before cutoff; prepare_research_snapshot(...research_only=True); admit_research_run(...research_only=True,formal_stage_capacity=True,job_type=AGENT_PAPER_RESEARCH).
Receipt additive: research_contract=research_only_v2; research manifest_id/research_as_of/manifest_hash/members(instrument_id,run_id,job_id,status,quality,usage_known), remote_latest_verified=false/execution_locked=true. v2 cycle_id=null; active MANUAL_RESEARCH_STARTED/completed MANUAL_RESEARCH_FINISHED.

## 集成证据（2026-10-22，本地受控验证）

- 独立审查四项发现已处理：登记时冻结 runtime/budget/完整 prompt profile；主错误不被清理错误覆盖且取消独立提交；活动请求重启只收尾；提供共用宿主锁的 `run-research` 正常前台入口，不启动日历或调度。常驻通用宿主的服务启动要求保持。
- PostgreSQL 正常 v11 入口真实走三个研究阶段及原生 Worker，三股共 9 次受控 SDK 替身调用；精确 manifest、用量、API GET 与同键读回、独立下一准入通过，零 PaperCycle/订单。这不算真实模型验收。
- 原子准备中断新增 RED→GREEN；失联原生 attempt 的有效 lease 拒绝关闭，过期后 unknown 保留预算、两只未调用取消且下一准入拒绝；SDK 主错误及准备主错误在二次清理失败时均保留。
- 新请求与旧 API 25 项、身份/迁移/研究 runtime/旧孤儿及结算 40 项 PostgreSQL 回归通过。前端 1237 passed/1 skipped，lint/typecheck/generated/build 通过；全引擎 Ruff/Mypy 通过。新增后续收尾案例另行定向通过。
- 仓库 policy 与 187 项 policy 测试通过。完整 npm 审计只有原 braces 公告，锁文件、依赖链和 glob 输入路径不变；仅本次展示/生成合同/测试源码导致指纹变化，按既有授权更新精确指纹，截止仍 2026-10-23 00:00 UTC。
- 尚待：最终 head 必需 CI、兼容部署与页面实测，以及唯一全新三股真实模型请求后的 DB/API/页面/终态和下一准入验收。旧失败请求没有重放。

最终定向复核无剩余 P1/资金/风控阻断；按 CI 模块导入方式，后端相关单测 1633 passed/6 skipped。新增准备清理错误回归先失败后通过。

最终新增 PostgreSQL/单测 13 项按 CI importlib 模式通过。合成原件夹具在纽约午夜暴露默认两分钟前跨日问题，改用数据库纽约当日零点作合成接收时间；保留正式同日校验。手机/桌面浏览器及旧请求刷新 3 项通过，确认三股内容质量分开显示且刷新只 GET；截图位于本地 test-results/pure-research-{390,1440}.png，不属于真实业务证据。
