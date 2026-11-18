# 普通 UTC 日自然义务预算保护实施计划

> 执行：在当前工作树由唯一实现负责人按计划实施并验证；复用系统排障、测试先行和独立复核流程。

**目标：** 修复已批准长期 SEC 合同中的 H 保护遗漏，保持普通自然研究可正常逐项进入、全局只有一个 increment 资金 credit、每股日限及旧 once 不变。

**依据：** `docs/design/nexus-durable-sec-monitoring-v1.md`。指定独立审查 `NEXUS-DURABLE-BUDGET-H-20261118-R1` 已完成，确认是已批准合同的实现遗漏，无新增政策批准要求。完整审查在本轮私有验收目录 `long-monitor-audit-20261118T0705/budget-review-r1.md`。

## 已确认根因及 RED

当前运行版 2d2dd8d1，当日 single_sol scope 与 formal binding 均不存在。`require_protected_admission` 返回 false，`reserve_daily_budget` 随即仅验 U+A；阶段原预留检查也不计算 H。不能将 no_guard 的 remaining_obligations=0 解读为五项义务已完成。

`test_durable_two_generation_pg.py` 新增的 `natural-budget-protection` 分支（尚未提交）通过正常普通 durable SEC 材料准备及准入入口，以隔离原生账本的两笔无关预留复现：旧代码仍 admitted，期望 blocked，因此 RED。没有执行 worker 或模型。早期 RED1—3 是同证券准备中 Run 触发正确的 reconciliation 保护，不能当预算缺陷复现；RED4 将无关占资放在另一证券后，确实到达正常准入并暴露缺陷。日志 `normal-budget-red4.log` 为有效根因回归。

## 实现范围

- [x] 新建独立普通日保护身份；不冒用旧单次根，不自动开启 formal_budget_protection。采用最小持久化 scope/credit，冻结账户、UTC 预算日、候选批次/hash、正常模型/hash/价格/完整上限、既有日历、原自然槽及正常来源审计。
- [x] 正常产品入口在共享预算锁内初始化；宿主跨 UTC 日、普通自然研究和正常增量入口均有真实初始化路径。适用合同而证据缺失时明确拒绝本次分配，不能回退 H=0，也不能只做永久缺 scope 拒绝。
- [x] 五项任务为三股日研、synthesis、全局一个 increment；每项唯一原 owner，Run 唯一，记录禁止修改/删除。延迟 SQL 检查 Run/Job/原预留/route/冻结来源/账户/批次/模型/日历完整绑定。普通 mixed/durable 与 legacy 来源分别按完整原关系判定。
- [x] 在共同 `require_protected_admission` 接入，使 research、synthesis、portfolio 等共享分配都看到同一 H；旧保护拒绝不得改走更宽松普通分支，重叠义务不重复 H/credit。
- [x] 首次分配只增加一次 A；精确资助原任务时同事务减少对应 H。后续新付费发送以 U 已包含本身 A 的口径验 U+H，不重新 reserve、不增 credit。新增发送保护不得阻断已发送请求的 observation、原号结算、cleanup；`StagedProtocol._authorize` 与 terminal/finalize 调用必须区分。
- [x] reserved/unknown 的原 A 仍占资；已证实 closed settlement 才证明结案。released 但未结案恢复 H，原 owner 永不换人。直接复用 `closed_settlements`；不可整段搬用要求槽后三股全部结案的旧 `funded_obligations`，否则会阻断自然研究逐项创建。
- [x] 日内价格变化按正常更新入口同锁重算未资助义务；旧 owner 的原价格、费用和预留不被重写。UTC 预算日和 NYSE session 分开，不照搬 session_date=UTCday 假设。

## 相关验证

- [ ] 正常入口 RED4 转 GREEN，拒绝后没有可用新 Run/Job/预留/credit；同时原两代成功链保持通过。
- [ ] 三股原日研、synthesis 逐项准入；首笔增量只共享一次 credit；第二证券有余款可准入、无余款拒绝，不能变成账户每天只能一笔增量。
- [ ] stage 2/3 不重复 A，unknown、释放未结案、结算与新准入交错保持资金占用；费用拒绝不阻断旧请求清理。
- [ ] 两连接竞争、回滚、提交未知、重启、等锁跨 UTC 日、价格/模型更新和旧 scope 冲突；直接 SQL 缺失/伪造 owner 及绑定拒绝。
- [ ] 迁移、兼容维护回滚及正常备份/恢复；在新 schema 有事实后不降级删事实。复核后提交、正常合并，不等待 CI。
- [ ] 与已合入 PR158 原件归档修复一起完成实际部署；正常入口读回已归档 NVDA 真材料、实际页面与持续同宿主心跳。不能拿候选 CLI 或镜像构建冒充部署。

## 保持不变

$13、五项自然义务、旧 once、所有旧研究/通知/隔离费用及其未知预留不变。不得制造正式命中、绕过市场时段、重发旧请求、消耗新付费根验收。当前真实 NVDA 8-K 已完整保存官方声明原件，但 GRAPHIC 内容尚未解释，研究材料仍不合格；这不阻止独立的基础事件/通知判定，也不自动授予增量研究许可。

## 2026-11-18 定向收口

正常三股日研曾在第二股提前核验第一股尚未装配的 route，现对本事务可见的 in-progress credit（包括 savepoint）保留无条件 deferred commit proof；已提交 credit 立即复核。合格服务日历补足下一会话，统一测试数据库时钟后，正常原入口三股预留、完整装配、提交和唯一 credit 检查通过，非放宽源证据或 SQL 约束。首阶段 H 拒绝共用既有严格零调用收口；stage2 拒绝仅发生首阶段一次受控传输，known prefix 正常结算/清理；组合0次/1次前缀拒绝已验证。

真实134正常备份已隔离恢复并升级135，全部旧业务表全行摘要相同、4148对象完整、目标残留0。新旧精确退出恢复60例通过。私有证据为 routine-natural-budget-20261118T0750 中的 natural-subtransaction-proof.log、stage2-send-refusal.log、natural-zero-followup2.log、natural-and-prefix-pg.log、final-boundaries-units.log、exit-final-units.log、restore-verify.log 和 restore-compatibility.private.json。完整最新两代及并发回归还在执行；指定终审、正式部署及运行验收尚未完成。
