# Workbench 状态与限制实施计划

Goal: 用一个只读切片回答今天是否登记启动、当前轮候选进展与已保存研究限制。
Spec: ../specs/2026-10-23-workbench-status-gaps-design.md
Architecture: 请求感知的有界选择投影 + 精确冻结候选摘要 + 兼容 activity 页面；沿用 Python/SQLAlchemy/Pydantic 与 React/vinext。

- [x] TDD选择/隔离/未知处置：新增workbench/activity.py及unit/integration覆盖；read_service调用，schema向后兼容。坏绑定/溢出未知，纽约登记日期与decision_session不混合。
- [x] TDD候选：research_summary.py复用已验证读回，覆盖精确run/hash、partial/坏绑定和有界摘录；独立writer完成后主线串行集成。
- [x] TDD页面：ActivitySummary、readiness文案及旧响应/stale/权限回归；独立writer，不写generated。
- [x] 独立一致业务读：workbench GET使用独立read-only repeatable-read事务，保留认证nonce；PG/API验证。
- [x] 生成客户端，定向Python/TS静态及单测、PG/API和实际Chrome已落盘案例；对新增功能核对零业务写。
- [ ] 集中代码审查及同Chat实际diff终审，修正后全部必需CI；安全窗口构建/集成/部署及版本/页面复验。

Review focus: preparing无cycle；跨日unknown已解除不再占用；多个活动歧义；冻结候选顺序不使用最新Run；未来证据不修饰旧Run；读取失败不混新旧对象；正常nonce不误算业务写。原门禁、未知费用、预算和今晚调度不变。

## 2026-10-23 最终本地验收

完整快速回归6312通过，8项原有跳过、1项排除；Mypy608源文件及Ruff通过。
最终45项相关PG回归通过，覆盖正常鉴权API只读事务、历史封存证明、精确请求绑定、
未来终态和跨账户租约；新边界回归经三个针对性变异验证确实失败。
前端lint/typecheck/全部单测/generated/build及198项政策测试通过；CI选择器11项通过。

用已落盘事实经候选API、正常BFF、真实Chrome验收，桌面1440和窄屏390无横向溢出，
展开、折叠和精确研究链接通过，无页面错误。两次真实BFF刷新7.396/7.711秒，
原10秒截止未变；首次开发编译整页10.706秒，其中Workbench请求7.2秒。
真实读取后另以浏览器故障注入验证500完整保留原摘要、403清除旧事实。
单次只读投影内复用精确版本原件字节，完整服务从17.8–18.8秒降至6.280秒，
独立身份、hash、回执与时点验证保留；正常API5.850秒。无模型或资料采集调用。

终审请求nexus-workbench-status-gaps-final-20261023-01已发至原Chat会话，等待完整回复。
GitHub最终同head必需CI、最终镜像和部署后核验待完成，本记录不宣称部署。
私有运行证据保留在本工作区.secrets/workbench-status-gaps/，不纳入仓库。
