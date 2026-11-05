# Budgeted Independent Research Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 使显式新单股研究在同日共享预算与自然义务保护下正常准入，并完成获准的一笔真实 AAPL 验收。

**Architecture:** 保留每日唯一 guard 和原 credit；新 v5 请求独立拥有预留、阶段和收尾，绑定原日保护上下文的不可变摘要。旧 v4 入口默认和读取合同不变，正常 CLI 显式选择共享预算政策；同一宿主消费新版本。

**Tech Stack:** Python 3.12、SQLAlchemy、PostgreSQL/Alembic、现有 CLI/API/BFF。

**Spec:** `docs/design/nexus-budgeted-independent-research-v1.md`

## Global Constraints

- 日共享 13 美元、每笔最大预留 2.12192 美元、整链输出 8192、零工具和最多三阶段不变。
- 旧未知、费用、预留及永久不重放保护不变；不得扩大未知接受范围。
- execution_locked=true，普通 Worker 停用，无组合、通知或订单副产物。
- 旧 0115/0116 不改写；新身份需非空维护回滚兼容。

## Review Focus

- 前笔已知失败、完全结算清理后，第二笔可申请；未知仍拒绝。
- 自然 credit 只属于精确原身份；释放后恢复未履约义务保护。
- 两个新请求及自然任务并发时，锁内预算不重扣、不漏计。
- 旧接受的迟到事实先后提交，后续阶段拒绝而已发清理照常保存。
- 新旧身份共存，维护回滚不恢复任何发送权。

### Task 1: 正常入口与持久化绑定

Files: single_sol_owner.py、single_sol_research.py、新 budgeted_single_sol.py、manual_request.py、research_request.py、manual_paper.py、0117 迁移及 migration head。

- [ ] 新增正常 CLI 回归：先完成旧 v4，再以 shared-budget 新政策创建第二笔；先验证失败。
- [ ] 增加 v5 绑定/意图摘要，正常 CLI 显式政策参数；同键旧内容读回，新旧版本冲突拒绝。
- [ ] 复用每日 guard；逐笔原生预留和保护摘要同事务确认；新版本专属约束，不改旧来源。
- [ ] 正常宿主、Worker/SDK、阶段准入、失败收尾、取消/孤儿及读回消费者接入 v5，保留新未知拒绝。
- [ ] 完成单股三阶段正例和第1阶段失败后零后续授权的离线回归。

### Task 2: 资金、并发与历史保护

Files: 新 test_budgeted_single_sol_pg.py、既有 single_sol_budget/unknown/recovery 消费者测试。

- [ ] 预算足/不足、不同新请求竞争、自然任务先后提交与 credit 释放。
- [ ] 当前占用、提交不明、未清理、迟到事实、旧接受失效、跨日/截止。
- [ ] 旧 v4 事实逐行不变，新 v5 不获得旧接受范围；正常下一轮只读准入。

### Task 3: 交付与真实功能验收

- [ ] 新旧非空数据的正式→维护→正式镜像验证；API/BFF/实际页面读回。
- [ ] 实际 diff 与证据送同一评审终审；修复有效意见，必需 CI 按同一候选汇总。
- [ ] 安全合入部署，保留用户文件与可行回滚，核对唯一宿主/版本/新鲜心跳。
- [ ] 实时准入通过后先持久化唯一新身份，再正常入口提交一次 AAPL；沿同一身份核对三阶段/结算/清理与自然义务。
- [ ] 一次终态即止；失败或未知只读取证，不重发、换键或补样本。
