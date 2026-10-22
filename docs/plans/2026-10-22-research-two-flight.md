# 两股有界并行实施计划

**目标**：正常纯研究入口最多同时运行两股，保留三阶段顺序、全批预算、精确租约及首失败收尾，用唯一新三股实测验收。

**授权与基线**：用户已明确授权两股并行、预算/租约/失败收尾、一次新真实验收及验证后的安全交付。已部署基线 main 为 4a0afe03（PR19/schema0085）；最新串行基线 485.408 秒，不重放该请求。由本主线唯一写入；使用已有隔离工作区。按 executing-plans / TDD 执行，项目约定优先。

**评审**：复用既有 CI/研究并行评审，增量请求 `nexus-research-two-flight-design-20261022-01` 已发往同一 Chat 评审会话。已取得完整、关联且未截断的有条件通过回复；SDK 最后提交检查、精确兄弟读取、预算日锁和失败前置通知均已据源码核验。实施后的完整差异终审尚未完成。

## 不变量

- 仅 research_only_v2；唯一宿主 flock；manifest 顺序，最多两个完整 worker 在途，有槽才 preclaim。
- 每股独立 SDK、committee、临时目录、DB session、用量、租约；八角色与三阶段内部顺序不变。
- 新请求冻结执行策略，旧 profile/hash 保持兼容；不回填旧请求。
- 先原子预留全批预算，unknown 保留，不增加额度/时限、不重试、不以零金额忽略未知调用。
- 首失败停止新股及下一阶段；已开始 SDK 按原 deadline 收尾，不因兄弟失败取消。全部在途处理后才收尾/下一准入。
- 正式价格、时效、风控及 execution_locked=true 不变；只读页面不触发模型。

## 已核验接缝及必须修正的竞争

- `manual_paper_host._dispatch_pure_research_request` 当前串行循环；替换为明确槽位调度，保留异常主因与独立取消/收尾事务。
- `research_only_capacity` 当前不接受 running；仅精确本请求 manifest/profile/model/cutoff、Job/Attempt/Lease/fence 证明的兄弟允许。不能调用会锁兄弟 Run 的 formal_stage_job_qualified。
- `require_reserved_daily_budget` 先锁 own reservation 再锁预算日，后者在 handler 长事务中跨 SDK 持有，使两个任务实际串行。新策略执行前仅持有精确本股预留行锁；全批 reserve 与 settle/release 的日锁保留。金额、pricing、run 绑定必须照旧核验。需独立 PG 证明不超卖、unknown 不释放。
- `dispatch_guard` 在阶段前及 SDK 真正 thread.run 前已有同步检查，组合原租约门禁和请求停止标记。停止与实际调用开始之间不能有未复检的 await。
- 失败应在阶段异常/语义校验失败时立即广播，不能等数据库提交后才停止兄弟。SDK 初始化中被阻止且 provider_call_started=False 的固定停止原因才能结算已有已知用量；真正已调用未知仍 reserved。
- 请求 ContextVar 缓存会被子任务继承，但只能存纯准备结果；不把 session/committee/动态准入加入缓存。退出 scope 前必须等所有子任务结束。当前未发现进程环境写入；线程化若引入需另核缓存访问安全。

## 实施与验证

- [x] 正常入口隔离 PG 重叠用例先 RED，保持原串行/历史合同回归；事件同步与宽 watchdog，不用亚秒时间断言。
- [ ] 新 profile 版本/合法性与历史兼容；请求局部绑定、两槽调度、立即停止、SDK 最终门禁及已知/未知失败结算。
- [ ] 精确兄弟动态证明、预算日锁边界、独立 session，验证无互等和无重复 claim/reserve。
- [ ] 首失败/第三股未调用、在途返回但禁止下一阶段、初始化时停止、unknown/timeout/限流无重试、持久化失败及宿主退出/下一准入。
- [ ] 归档零模型输入等价；集中独立审查；Ruff/Mypy/相关 PG 与旧回归；同 head 全部必需 CI。
- [ ] 最终镜像、备份实际恢复/兼容回滚、受控部署及版本健康。保留用户根目录改动，原 braces 接受截止不延长。
- [ ] 正常入口唯一新三股：预算 reserved→settled、实际重叠/峰值<=2、九阶段、DB/API/页面、自动收尾/下一准入；不再发第二轮。报告端到端和输入/模型波动限制。

## 当前状态

PR19 的最终退出审计、原进程句柄确认、取消主因保护及真实离线子进程测试已整合；相对4a0afe03不增加迁移或权限。此前未提交候选已先保存完整私有快照并提交，再合并主线，没有覆盖根目录用户改动。

集中静态审查发现两个P1，均先RED后修复：真实asyncio timer转换超时必须在cleanup等待前stop；SQLAlchemy提交失败可能内部rollback，故在当前session的engine rollback/rollback_savepoint事件、driver I/O之前同步stop，精确限定dispatch/run并finally移除监听。保留原异常边界及原超时预算。SDK直接TimeoutError的首因遗漏也由真实调用边界回归复现后修复。

完整回归还暴露已claim但纯capacity准备尚未调用SDK时被stop会误留预算。仅此固定错误+provider_call_started=False路径复核原lease、dispatch/claim/prepared和精确预留后走既有零调用收尾；已调用unknown路径不进入。原PR19未知预算断言未改。

当前35项PG通过：真实两股重叠/第三股补位/日预算锁不跨SDK、精确错request/lease/fence/worker拒绝、首失败已知/unknown、在途最后阶段可完成、实际handler commit失败不补第三股且保留未知预算、双任务取消等待drain、宿主退出/下一正常完整准备、旧四字段profile和退出审计。SDK/退出/停止回归及Ruff/Mypy检查通过；198项policy通过。私有归档对照九阶段wire/hash/schema/引用序列与既有验收一致，原stage/frozen hash一致，模型调用0、数据库写入0。

新增安全诊断输出：正常CLI stderr只包含request/run UUID、固定事件/阶段/原因、单调时钟及序号；记录slot取得/释放、SDK提交/返回、终态读回和首失败。SDK返回事件不代表子进程退出，退出继续由PR19独立核验。首异常只记录一次，后续兄弟屏障不覆盖。

整体实际diff终审已提交原Chat会话，标记nexus-research-two-flight-final-20261022-01；回复尚未完成。同head全部必需CI、合入部署、最终镜像与兼容回滚、唯一新真实三股验收仍待完成。当前不可宣称真实提速或新一轮连续运行已通过。运行仍为已部署PR19，未发起任何新真实模型请求，不重放485.408秒串行基线。

变更大小：已将独立退出迁移拆为PR19并部署；余下两槽调度、动态兄弟资格、预留检查、失败结算及SDK最终屏障构成不可部分启用的一个最小安全切片。测试按纯逻辑、SDK边界、真实PG及正常入口分层，避免将缺少一半保护的调度先上线。

私有证据位于根目录`.secrets/research-two-flight-20261022/integration/`。原braces风险范围和2026-10-23 00:00 UTC截止不变，前端源码/锁文件未改。真实验收须在部署后仅一次新请求，单调时间比较485.408秒，并说明输入及模型波动，不承诺固定提速。
