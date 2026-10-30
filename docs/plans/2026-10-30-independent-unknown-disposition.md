# 独立研究未知处置实施计划

> 执行：使用 executing-plans 在同一工作区串行实施，先红后绿；最终集中独立审查。

**目标：** 为已采纳的 v3 历史未知接受提供正式 CLI、共同证明与所有准入消费者、只读页面；不接受真实请求。

**架构：** 复用 AuditEvent、0099 原生阶段和原生成员关闭证明。稳定历史摘要与当前动态安全检查分离；新增动作仅追加审计，旧身份永久拒绝，未知预算不变。无需第二套执行协议。

**技术：** Python 3.12 / SQLAlchemy / PostgreSQL / Pydantic；现有 vinext BFF 与 Web。

**合同：** `docs/design/nexus-independent-unknown-disposition-v0.1.md`。用户 2026-10-30 已采纳，明确本笔暂不接受。

## 不变量及复核重点

- execution_locked=true；业务宿主维护停用，普通 Worker 不启动；真实请求/通知不重放。
- v3 正式路由、完整原 manifest；成功/未知/原生取消混合，原未知预算不变。
- 冻结财务 proof 不用于掩盖接受后的迟到新增事实；摘要覆盖缺失项及同时间戳新增。
- 手动和自然历史分支均消费同一证明；已接受不跳过其他 unknown、预算或风控。
- 有效计划可保留，其在途/未知后继不被根接受覆盖；无关未来检查不污染旧摘要。
- 并发及提交结果未知不产生重复接受；已存审计永久拒绝旧执行，即使接受例外失效。

## 1. 正式证明、接受及准入（当前切片）

文件：新增 `engine/src/nexus/paper/independent_unknown_acceptance.py` 和有限职责的 proof 模块；修改 `paper/manual_request.py`、`ops/manual_paper.py` 及必要旧执行入口。

接口：`inspect_or_accept_independent_unknown(session, *, settings, request_id, repo_root, env_file, accept=False, accept_unknown_effects=False, expected_proof_hash=None)`；`independent_unknown_accepted(session, row, *, now)`；`require_independent_execution_open(session, *, request_id)`。返回严格 Pydantic check/receipt，check 包含用户确认的 proof_hash。

- [ ] 新 `test_independent_unknown_acceptance_pg.py` 通过真实准备/Worker/SDK受控边缘构建 unknown，保持原拒绝断言；先验证新正常 CLI check 缺失而失败。
- [ ] 实现稳定集合 proof、接受审计验证、当前安全检查及 CLI，验证 check 零写、原费用预算和结果不改、重复同审计。
- [ ] 统一手动历史/自然历史及其恢复和增量消费者；验证精确接受后下一准入，其他占用仍拒绝，旧身份不重新派发。
- [ ] 执行定向 pytest、Ruff/Mypy；保留可追溯红绿证据，集中提交已验证切片。

## 2. 并发与混合回归

- [ ] 复用第一组夹具，覆盖无确认、错摘要、错 scope、v2/legacy、活租约、cleanup缺失、混合成功计划、未知增量、跨日及预算不足。
- [ ] 复用现有真实 COMMIT fault helper，覆盖提交/回滚确认丢失、并发一次审计、事务未决不能判回滚。
- [ ] 接受后同身份迟到与同时间戳新增使例外失效，旧派发仍拒绝；新请求/新审计/合法未来计划检查保持证明有效。
- [ ] 保留原未知拒绝和旧 seal 回归，登记 CI selector；不重做未变安全等价矩阵。

## 3. 只读展示与正式交付

- [ ] 原 manual request 只读 projection 新增独立 research_disposition，严格类型验证且历史字段形状不变；更新正常 BFF 与工作台中文说明。
- [ ] 真实 TCP API→BFF→桌面/手机页面验证 unknown+预算保留+仅历史占用接受三者并存，零模型/通知/写请求，原10秒截止不变。
- [ ] 向原 Chat 请求实现终审，处理有效意见；同 head 全部 CI 绿后合入，核主线 CI，最终 F/R/F 与新审计兼容验证，正常维护部署。
- [ ] 本笔真实请求仅只读查资格；不得执行 accept、恢复宿主或新建研究。需真实接受时另据具体资格取得用户明确授权。

## 执行记录

- 基线 39711b6e，复用已注册且干净的 SDK 工作区，分支 edward/independent-unknown-acceptance。PRIMARY 用户文件不动。
- 本计划为正式实现，不创建原型。待验证事项不冒称通过，不把本切片完成当完整研究监控 Goal 完成。

## 后续新增授权：调用层最小迁移评估（本切片不切换）

2026-10-30 收到用户经主线协作传来的指令：现有失败恢复继续实现，随后核对官方套餐直接 Responses API 路径，复用真实归档做有限配对完整三阶段对比。优先成功率、分析质量、引用和财务口径及异常收尾，再比较延迟；各路径下游必须消费自身上游真实输出。历史5轮计划仅完成2轮、第三轮停止，不能当成稳定性通过。

指定项目为 NEXUS 产品与投资体系设计，长期会话按当前 `AGENTS.md` 原文及项目归属动态解析，复用此前已验证的同一会话。只替换调用适配层，保留现有阶段、持久化、预算、租约、恢复和监控协议；新增套餐内隔离实验身份按该授权有界执行，禁止重放旧 unknown、接受本笔、恢复宿主、修改凭据或新增付费。先完成当前安全切片，再冻结对比设计并送既有 Chat 评审，验证通过才考虑切换。

### 2026-10-30 本地核心切片

正式 CLI 已实现 `check-independent-acceptance` 与 `accept-independent-unknown`，接受必须附指定请求、明确接受标志和原检查摘要；不在本回合对真实请求执行。稳定证明绑定完整原生集合，公共结案及手动/自然历史分支消费同一审计 reader。旧 prepare、Worker 和阶段授权保留永久拒绝；重复读回先验证原审计，不要求后续业务停下。

红绿回归已经定位并补上重复读事务旧快照、有效但未消费的终结租约、历史 reader 与迟到阶段写者并发，以及公共结案消费者四个接线缺口。原预算/用量/输出不变，迟到同时间戳新增事实使例外失效；两股并发未知和成功/未知/未调用取消混合使用真实 Worker/SDK 边界及隔离 PostgreSQL。真实提交/回滚后丢失客户端确认只读回同一身份，不自动补发。各日志保存在本工作区私有 evidence 目录，最终汇总及剩余项以 checkpoint.json 为准。

仍须完成：类型/身份拒绝矩阵及活跃监控/未知增量、实际预算不足与下一身份消费者；API/BFF/工作台只读投影和实际页面；指定 Chat 实现终审、完整 CI、支持新审计的兼容维护镜像及正常部署。本地核心回归不代表正式入口已部署、当前真实 unknown 已接受或研究监控闭环完成。

## 2026-10-30 11:31 增量：只读回执与页面

已接通 independent_disposition 严格字段与生成合同；原 unknown 保留，完整有效证明只表示检查时接受事实，迟到事实提示 recheck_required。展示使用无锁快照读取；准入仍使用原 READ COMMITTED 与共享锁证明。页面有效接受后可清除本地旧标识，但该操作不发送请求；新提交仍走正常准入。需复核时不提供清除入口。

19 项隔离 PG（含只读事务/迟到写入/原有竞争及重放保护）、13 项 Python 合同、33 项前端、4 项手机/桌面实际浏览器通过；生成客户端、类型、Ruff/Mypy及diff通过。浏览器使用合成失败未知/未调用取消成员，与真实业务验收区分。证据 .secrets/independent-disposition-view-20261030/checkpoint.json。隔离库零其他连接后停回，业务未变。剩余矩阵、实现终审、全部CI和部署仍待完成；直接API评审复用，尚未实调用。

## 2026-10-30 消费者及实现终审增量

补上真正 SQL READ ONLY 恢复检查：READ COMMITTED 下只读观察不取得禁止的行锁，可写准入仍持有 scope SHARE NOWAIT，并重新校验。增加错账户/scope/v2拒绝、正常新请求及未知预算保留、合格成功计划保留、price+SEC真实受控单股后继 unknown 独立阻断回归；原默认完整三股断言保留，为接受后的成功/未知/取消混合夹具另写精确状态断言。

实现终审请求08c8db15-e7b7-4719-bac0-8183f543b4ea、回复1273f340-819e-4e6e-b473-fab6b70a645f已完成，未发现确认P0/P1；要求最终消费者、迟到及并发证明齐备后收口，无变化不重复送审。实际页面及生成合同沿11:31同代码证据复用。

当前集中PG26通过、1次原生终态证明偶发拒绝；该新请求案例单独及有界6次诊断复验均通过，尚未确认首次拒绝原因，保留失败日志及新增合成事实诊断，不删除断言。只读ready后迟到写入的再准入拒绝另作增量验证。142项恢复/入口单测通过；前端全套1351通过、1既有跳过，lint/build/Mypy通过。首次并行构建时的未涉及前端等待失败保留记录，单文件和有界并行全套复验通过，未改超时。

证据 .secrets/independent-consumers-20261030/。最终CI、兼容维护镜像、合入部署尚未完成；本笔真实unknown、费用和预算不变，业务服务未启动。直接API保持独立已评审合同，不切换生产传输。
