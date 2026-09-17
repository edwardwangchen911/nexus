# 运行就绪与阻断解释最小切片

范围：基于 `455fa827`，独立分支 `edward/runtime-readiness`；只读扩充现有
`WorkbenchReadService`、`WorkbenchResponse` 和 `/workspace`。`/paper` 已有会话下一时段，
本切片复用该聚合结果，不另建页面、表、迁移或调度。Agent、风控、执行与锁定政策不变。

## 展示合同

- `readiness.stage` 是已持久化的周期阶段，区分窗口、采集/核对、Agent、风控、执行、
  恢复、完成和未知。窗口已有效但尚无周期时等待采集；盘前/闭市等待窗口。
- `readiness.blocker` 仅根据明确代码或已保存的风控结论分类。普通未完成订单等待原订单，
  不能视为结果未知；明确未知结果、对账中订单或部分成交却没有关联订单身份才要求核对。
- `RISK_INPUT_REQUIRED`、`NO_ELIGIBLE_CANDIDATE`、`RESEARCH_ADMISSION_UNAVAILABLE`、
  `PAPER_MARKET_WINDOW_UNAVAILABLE` 都可能包含多个根因，保留 unknown。研究未通过交易准入
  不等于研究明确看空，也不等于有充分证据的不调整；细分 Job/Provider 根因不从这些码反推。
- `NO_ORDER_REQUIRED` 保留正常不调整语义；确定性风控存在 rejected 时独立显示拒绝。
- `previous_cycle_id`、`previous_result_code` 和有界 `changes` 比较本交易日最近两个已保存周期的
  阶段、阻断、结果代码、风控结论、订单数量。无上一周期时不可比较，不评价收益改善或因果。
- 下一时段原样复用 `AgentPaperSessionSummaryResponse`，保留原因与北京时间；没有时间时
  明示需等待当前周期结束或日历证据，不估算下一次运行，也不承诺启动。
- 心跳复用现有宿主机严格分类器：有效 UUID 身份、合法时间、进程版本和迁移头。
  鲜活性使用同次 SQL 的 `clock_timestamp()`，与历史业务请求时点分开。无效/缺失不显示在线。
- `compatibility` 分别展示代码预期、心跳报告、实际数据库迁移头；兼容与在线分别判断。
  版本匹配不证明当前 Git 提交已经运行生效，现有心跳没有提交身份，不虚构该信息。
- 旧服务缺少新字段时页面明确未知。刷新保持 GET，只读取保存事实；无采集、模型、订单、
  恢复或重试控制。现有注意事项不再把全部 unavailable 归为数据失败。

## 验证

- 测试先失败后实现，覆盖完整/缺失/过期/未绑定/不调整/未知、窗口、相邻周期、旧响应和坏响应。
- 聚焦 PostgreSQL 测试使用独立 `postgres:17` 容器及 `nexus_test_*` 临时库。
  `SET TRANSACTION READ ONLY` 中实际调用聚合；同一组持久化相邻周期经正常认证 API
  回读，确认与聚合结果相同。没有访问或写入主线业务库。
- 本地后端/合同/隔离 PostgreSQL、前端工作台/决策记录/BFF、Ruff、Mypy、TypeScript、
  定向 ESLint、生成客户端一致性和 Web build 已验证。具体最终计数见交付消息。
- Playwright 在本 worktree 独立端口验证桌面和手机、无障碍、无横向溢出、版本展开、
  刷新只有 GET 和未知结果无写入按钮。页面使用合成响应，不能冒充主线真实运行验收。
  截图：`test-results/workbench-desktop.png`、`test-results/workbench-mobile.png`。
- 一轮独立只读审查发现并修复心跳时钟、普通挂单误归未知、汇总原因误归缺数据三项；定向复核通过。

## 集成与回滚

主线负责人可选择性 cherry-pick 本分支交付提交；不合并其他生命周期/模拟盘分支。
重点检查 `workbench` schema/read service、公共 `product-status.ts`、`tokens.css` 及三个生成合同文件。
生成合同冲突应保留双方 schema 修改后重新执行 `npm run generate:client`，不要手改生成片段。
新字段对新版消费者是可选字段，可读旧服务；旧版严格 BFF 不接受新增字段，Engine 与 BFF
应一起更新到含本切片的匹配版本。没有持久化格式变化，回滚时一并回退服务及消费端代码即可。
本任务不合并、不推送、不部署、不重启共享服务；真实主线页面生效需后续获授权的集成与运行更新。
