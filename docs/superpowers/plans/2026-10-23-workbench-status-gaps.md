# Workbench 状态与限制实施计划

Goal: 用一个只读切片回答今天是否登记启动、当前轮候选进展与已保存研究限制。
Spec: ../specs/2026-10-23-workbench-status-gaps-design.md
Architecture: 请求感知的有界选择投影 + 精确冻结候选摘要 + 兼容 activity 页面；沿用 Python/SQLAlchemy/Pydantic 与 React/vinext。

- [ ] TDD选择/隔离/未知处置：新增workbench/activity.py及unit/integration覆盖；read_service调用，schema向后兼容。坏绑定/溢出未知，纽约登记日期与decision_session不混合。
- [ ] TDD候选：research_summary.py复用已验证读回，覆盖精确run/hash、partial/坏绑定和有界摘录；独立writer完成后主线串行集成。
- [ ] TDD页面：ActivitySummary、readiness文案及旧响应/stale/权限回归；独立writer，不写generated。
- [ ] 独立一致业务读：workbench GET使用独立read-only repeatable-read事务，保留认证nonce；PG/API验证。
- [ ] 生成客户端，定向Python/TS静态及单测、PG/API和实际Chrome已落盘案例；对新增功能核对零业务写。
- [ ] 集中代码审查及同Chat实际diff终审，修正后全部必需CI；安全窗口构建/集成/部署及版本/页面复验。

Review focus: preparing无cycle；跨日unknown已解除不再占用；多个活动歧义；冻结候选顺序不使用最新Run；未来证据不修饰旧Run；读取失败不混新旧对象；正常nonce不误算业务写。原门禁、未知费用、预算和今晚调度不变。
