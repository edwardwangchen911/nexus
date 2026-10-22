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

正常 `run-research` 是持锁的前台有界入口，自然退出和精确退出审计是成功条件；它不是常驻定时服务，也不能与常驻宿主争抢进程锁。正式 `start --mode research_and_paper` 只排队，由已恢复的唯一正常宿主领取，不能另启第二个执行者。

宿主因 unknown/非零退出停下时，不自动循环重启。先保存原日志、核对原请求和原生 Job/Run/租约/预算及进程退出；仅在证明已停止且符合现有恢复条件后使用既有恢复入口。确认停止托管注册不等于接受未知用量或释放未知预算。不要伪造心跳、删除审计或扩展 KeepAlive 掩盖阻塞。

错过的定时槽保持原事实。当前盘内手动入口是新的独立请求，需重新满足当时的身份、资料、价格、预算和风控合同；纯研究 partial 输出不能改用途当正式验收结果。
