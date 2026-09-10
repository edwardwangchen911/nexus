# NEXUS 组合决策 Agent 技术方案审查

**审查对象：** `docs/superpowers/specs/2026-09-10-portfolio-agent-scenario-design.md`  
**最终结论：** 通过  
**最终评分：** 97/100  
**实施准入：** Critical=0，Important=0，达到 95 分门槛

## 1. 审查方法

审查对照现有情景合同、Agent 工具目录、Responses 工具循环、通用 Job/lease/fence、预算预留、持久化模型、Engine/BFF 和组合页面。每轮只在上一轮问题被设计中的明确合同、状态转换、文件边界和测试覆盖关闭后重新评分。

## 2. 第一轮：82/100

第一轮未通过，发现一个 Critical 和五个 Important：

- 现有 Job handler 使用单长事务，无法兑现 provider 前和第二轮前的 durable checkpoint；
- 完整 `EvidenceRef.object_id/location` 可能向模型暴露 symbol、UUID、URL 或路径；
- HTTP 幂等请求哈希与服务端冻结输入哈希混用；
- GET 缺少 run 所有权校验，页面关闭后的恢复合同不完整；
- 迁移 head、错误 allowlist、崩溃/并发测试和 downgrade 前置条件缺失；
- 自由文本 limitations 不能证明完整覆盖 unknown，失效条件组合未封闭。

对应修正：专用短事务 worker/checkpoint/reconcile、不透明 citation IDs、raw `request_hash` 与 frozen `input_hash` 分离、actor-scoped GET、`localStorage` 只读恢复、结构化 limitation 和信号矩阵，以及完整迁移与崩溃窗口测试边界。

## 3. 第二轮：91/100

第二轮未通过，发现五个 Important：

- failed 终态错误使用只支持 succeeded/skipped 的 `finalize_job()`；
- `prepared -> failed` 未被状态机允许；
- `tool_recorded` 的已知首轮 usage 与全额保守结算规则冲突；
- checkpoint/finalize 与 orphan reconcile 锁顺序相反；
- citation 唯一性与诊断过滤仍不够严格。

对应修正：活动 failed 使用 `execute_and_finalize_job(outcome="failed")`，允许请求前失败转换；费用按 provider phase 精确结算；所有路径统一 `job -> run -> output -> reservation`；citation 绑定 manifest index 与完整规范证据身份；模型上下文只允许三个精确集中度诊断 code。

## 4. 第三轮：97/100

独立审查结论：

- Critical：0；
- Important：0；
- 评分：97/100；
- 达到实施准入标准。

剩余 3 分属于实现期风险，不是设计阻塞：

- 专用短事务 worker 必须用 PostgreSQL 崩溃窗口测试证明 fence、预算和终态一致；
- 新迁移同时涉及三张新表、prompt role check 与诊断组合唯一约束，必须验证升级和受控降级；
- 页面真实验收必须证明加载/刷新仅 GET、明确点击才 POST，且旧方案、Shadow、订单和成交零增量。

## 5. 过度设计复核

方案没有引入 Mandate CRUD、候选/选择业务表、通用求解器、新 Agent 委员会、新队列/Worker 进程、自动重试、Shadow 或交易能力。新增三张表分别服务于 durable run、不可变 output 和跨运行类型共享预算，均有当前阶段的直接恢复或审计需求；不能安全地复用强制绑定单标身份的旧研究表。

因此最终结论是：架构有必要调整，但范围已收敛到实现 Agent 可调用确定性情景工具所需的最小闭环，不属于为未来场景预建平台。

## 6. 实施期验证（2026-09-10）

Tasks 1–6 已形成合同、迁移、单工具投影、预算与幂等、两轮模型适配、短事务 worker、
Engine/BFF 及显式触发页面。Task 7 新增的隔离跨层回归先通过正式情景 API 取得 result hash，
再经正常 Engine admission 创建 run；除无网络 deterministic Responses transport 外，不替换
冻结权威 loader 或正式工具 registry。用例验证两轮 provider turn、一次真实工具执行、持久化
output、预算结算、citation 反解和 authenticated GET；模型输入既没有逐标字段，也没有可识别的
逐标 ID、symbol 或当前/目标权重值。所有结果继续保持交易锁；方案、Shadow、模拟订单和成交表
零增量。

该真实路径发现并修复了一项此前模拟 registry 未覆盖的 Important 接线缺陷：情景引擎的成本
unknown 原因 `COST_MODEL_NOT_AVAILABLE` 不符合 Agent 投影 allowlist，导致第一次工具调用后
保守 abstain。修复仅在 Agent 投影边界映射为更粗粒度的
`COST_EVIDENCE_INCOMPLETE`，保留原始情景合同，并增加直接回归；单证券不可改善案例最终输出
`observe`，未把 infeasible 包装成调整建议。

实施期 UI 审查曾发现两项 Important：挂起 GET 可绕过 45 秒观察上限，以及按 portfolio
前缀删除恢复记录会破坏多标签页幂等。实现已改为给每次 GET 绑定剩余总时限，并以完整
snapshot/report/result hash 作用域键隔离而不跨 scope 删除；定向复核后 Critical=0、
Important=0。当前剩余项不是设计或代码审查问题，而是功能分支合入、共享数据库迁移及
对应 Engine/general Worker 运行更新；完成这些前不得把 synthetic 验收表述为真实页面闭环。

Task 7 定向复审确认上轮 authority seam 阻塞已关闭：正式 loader、registry、worker 和 GET 均在
测试中真实执行，只有 Responses transport 被替换；隐私值断言和成本原因码回归有效。最终结果
为 Critical=0、Important=0，可以提交，但不改变上述共享运行版本阻塞。
