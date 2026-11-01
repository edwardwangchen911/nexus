# 本地部署后的宿主就绪检查

API/Worker 健康、源码一致和一次纯研究成功不代表已启用的正式定时服务正在运行。
部署完成必须同时核对预期运行模式、托管进程、当前版本和数据库心跳。

正常 `nexus-local-stack up` 已包含：先启动依赖，然后在配置启用时启动宿主并核对新鲜心跳。
采用已构建镜像独立切换 API/Worker 时，切换前须证明旧任务已收尾，并保留业务审计、用户文件、备份和兼容回滚版本；不得在调用在途时切换。

既有配置启用 `NEXUS_AGENT_PAPER_CYCLE_ENABLED` 时，按正常服务入口恢复：

```bash
.venv/bin/python scripts/macos/agent-paper-loop-service.py start \
  --repo-root "$PWD" --env-file infra/.env.local --project-name nexus-fosun-live
.venv/bin/nexus-local-stack verify-deployment \
  --repo-root "$PWD" --env-file infra/.env.local --project-name nexus-fosun-live
```

`verify-deployment` 只读检查，不构建、不启动或停止服务、不采集数据、不调用模型。

- 配置启用：托管宿主必须 running，数据库心跳 fresh 且 release 与当前 checkout 一致，零外呼预检通过。
- 配置停用：宿主必须 inactive/stopped，不要求保留的历史心跳继续新鲜。
- 两种模式均核对 API、Worker 的精确 release；任何缺失、不一致或宿主状态不符均返回非零。

基础服务 readiness 与必要页面仍需独立验收。不得用基础 API 200、Compose Up、配置 true 或刚退出的一次性研究宿主的 fresh 心跳代替上述检查。`status` 保持逐项诊断用途，部署完成使用 `verify-deployment`。

2026-10-31 用户对候选 `2a33aaa6` 明确接受 minimatch 的
`GHSA-g3hv-75wg-m249` 单项中危风险，固定截至 **2026-11-01 北京时间 08:00**
（UTC 00:00）。范围仅为 `node_modules/minimatch` 的 3.1.5 和
`node_modules/@redocly/openapi-core/node_modules/minimatch` 的 5.1.9，及完整审计中
精确 14 项传递告警，用于开发、CI 和本地构建工具；不覆盖新公告、新安装路径或业务门禁。
旧代码缺少公告所述算法不代表官方判定不受影响。parser 分支已限域升级至 11.0.1，
新增 matcher 依赖也必须实际安装；只更新锁文件不满足接受条件。

2026-11-01 用户明确答复“仅延至11月2日08:00，其余不变”。续期仅覆盖上述同一公告、
两处版本/路径及精确14项告警，不改依赖或输入范围；重新完整审计确认范围未变。
新窗口自本机记录确认的 2026-11-01T02:12:35Z 起至 2026-11-02T00:00:00Z（右端独占）。
原窗口及其截止保留，已过期至本次确认之间仍拒绝，不追溯认定那段时间可发布。
到期、新风险或范围改变仍阻断；最终候选须重新核对完整审计和精确输入身份。

`scripts/ci/npm_audit_gate.py` 保留全量扫描，绑定公告依赖图、锁文件、源码与 CI
工作流、两处残余代码以及修补后的实际安装树。正常生成入口仅使用本地测试 schema，
同时核对同名安装集合和实际消费者的 Node 解析位置，拒绝嵌套副本、文件遮蔽及解析逃逸。
生成入口已在 JS 摘要中；另绑定 Python 的 main/settings、两处 pyproject 及 uv.lock。
schema 内容仍须通过正常 check:generated 与已绑定的签入契约一致核验，不宣称冻结全部 Python。
不接收外部配置；新增根目录 Redocly 配置会拒绝。lint 的 pattern/options 来自仓库
CLI、配置及插件选项，未审查源码不能视为可信输入。隔离完整工具链和构建 module graph
已验证，构建应用图中不含这些工具模块；开发依赖目录仍含它们，不能宣称物理移除。
CI 保存原始 npm 审计 artifact。部署前须在最终候选的独立、完整安装目录再次运行
`.venv/bin/python scripts/ci/npm_audit_gate.py --report <私有证据目录>/npm-audit.json`，检查真实
当前时间仍在期限内；较早的绿色 CI 不替代部署时检查。输出必须明确为该项风险接受，
不得称作 clean。到期、范围变化或任一身份不符均停止发布，不自动延期。

正常 `run-research` 是持锁的前台有界入口，自然退出和精确退出审计是成功条件；它不是常驻定时服务，也不能与常驻宿主争抢进程锁。正式 `start --mode research_and_paper` 只排队，由已恢复的唯一正常宿主领取，不能另启第二个执行者。

宿主因 unknown/非零退出停下时，不自动循环重启。先保存原日志、核对原请求和原生 Job/Run/租约/预算及进程退出；仅在证明已停止且符合现有恢复条件后使用既有恢复入口。确认停止托管注册不等于接受未知用量或释放未知预算。不要伪造心跳、删除审计或扩展 KeepAlive 掩盖阻塞。

错过的定时槽保持原事实。当前盘内手动入口是新的独立请求，需重新满足当时的身份、资料、价格、预算和风控合同；纯研究 partial 输出不能改用途当正式验收结果。
