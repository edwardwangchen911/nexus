# 自然槽未知结果显式接受实施计划

> 执行方式：本任务单一主写入者按 superpowers:executing-plans 逐项实施；独立 Chat 评审合同与最终差异。

**目标：** 正常本地入口检查并显式接受一个精确自然槽的未知影响，仅解除其历史调度阻塞。
**架构：** 复用正常维护 CLI、数据库锁及不可变审计；保留原认领、未知费用及预算。检查与接受共享完整资格证明，所有旧执行写入口服从持久化拒绝标记。
**技术：** Python / SQLAlchemy / PostgreSQL，现有 schema 0086；不新增依赖或迁移。
**依据：** 用户批准最小方案；Chat 请求 nexus-scheduled-unknown-design-20261024-01 已返回有条件通过；实现吸收锁后重读、永久旧执行拦截与新周期独立性要求。

## 不变量

- 不重放任何模型或资料请求，不更换旧键，不修改原 failed/unknown 事实。
- 实际接受须另获针对该笔的明确授权；本轮仅实现、隔离验证、CI及部署。
- execution_locked=true；时效、价格、预算、账户及硬风控门禁不变。
- 不启动研究宿主；只有已归属的隔离测试数据库可运行。
- 当前根目录用户未提交文件与暂存内容必须保留。

## 重点反例

- 不适用的 route/schema/slot/hash/model/batch/保护预算绑定拒绝，不能从缺字段推断零影响。
- 检查无业务写入；接受独立重验，检查后新占用、迟到周期、活动任务/租约拒绝。
- 先接受与先写入两个事务顺序；锁竞争不等待重试，不产生部分接受。
- 原事实或审计损坏撤销豁免；旧执行永不因损坏而复活。
- 合法新周期不污染旧处置；旧认领、未知用量及预算逐字段保持。

## 1. 资格与显式接受

文件：新增 `paper/scheduled_unknown_acceptance.py`、`tests/integration/test_scheduled_unknown_acceptance_pg.py`；修改 `ops/manual_paper.py`。
- [x] 真实隔离 PG 正常 CLI 检查与接受回归先 RED。
- [x] 首版仅支持精确 scheduled failed/AGENT_PAPER_TRIGGER_FAILED/external_effects=unknown、无同槽 cycle、无保护预算绑定、本地既有零内部定价模型；用原生请求哈希证明历史批次与模型。
- [x] 首次检查/接受持生命周期、schema、账户、预算日、审计及执行表锁，重验实际生产者停止、无执行占用及其他历史守卫。
- [x] 检查仅回执；接受显式参数与新审计；重复返回原处置并验证原链。

## 2. 消费与旧执行拦截

文件：`paper/preparation_unknown_ack.py`、`paper/current_preparation_recovery.py`、`paper/agent_cycle.py`、`ops/formal_research_preparation.py`。
- [x] 原失败仍不可重放；有效审计仅豁免精确认领的历史阻塞。
- [x] reserve、准备、建周期、finish 写回各处同账户锁内拒绝已接受的旧 slot/claim。
- [x] 迟到写入、原件/审计损坏、新正常请求及跨账户/跨槽回归先 RED 后 GREEN。

## 3. 验证与交付

- [ ] 相关与全 fast、Ruff/Mypy、隔离 PG；补充已冻结真实记录只读资格证明，不接受。
- [ ] 独立终审，汇总同一 head 全部 CI；有失败统一修复，不放宽断言。
- [ ] 最终镜像、兼容备份和回滚、合并部署；API/Worker 同版本，宿主仍 inactive。
- [ ] 已部署正常 check 入口回执、业务/预算/未知占用/用户文件不变；入口齐备才请求指定未知影响接受。

## 当前验证记录

- 新增隔离 PG 20 项通过；审计 action 损坏反例已先 RED，修复查找后 GREEN。
- 全 fast：6334 passed，10 skipped，1 deselected；Ruff / Mypy 609 文件通过。
- 相关 PG 首轮 133 passed / 3 failed：均为辅助 claim 错误先于主请求未知错误。
  保留原断言，调整同账户锁内诊断顺序；3 项定向回归已过，完整相关回归重跑中。
- 独立代码审查两项发现（迟到周期 resume、历史批次 canonical）修复并定向复核通过。
- Chat 终审请求 `nexus-scheduled-unknown-final-20261024-01` 已发送，待完整答复。
- 尚未真实接受、重放、启宿主或更改主库；合并/部署必须等待最终 CI 与终审收口。
