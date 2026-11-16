# 单次单股监控用途研究 v1

本合同落实2026-11-16用户“仅允许本次新单股监控请求适用”的明确授权，以及独立评审NEXUS-SINGLE-MONITOR-ENTRY-20261116-R1。仅本次全新请求可沿用原已接受历史证明；不是旧请求重放、新unknown接受或持续监控研究的通用豁免。

- 请求版本6，mode为research_and_monitor；单独actor local-single-monitor-sol-v1和前缀single-monitor-sol。
- 唯一成员从原完整候选批次明确选择；保存原批次、hash、账户、模型及价档、证券、选择hash、UTC预算日、日历、截止、共享日保护hash、两份原历史证明hash。只创建一个manifest member/Run/Job。
- binding.history_policy为single_monitor_original_acceptances_once_v1，authorization_scope为single_monitor_root_20261116_once_v1，admission_policy固定shared_budget_v1。每账户最多一个v6永久请求；任何终态都不释放这次授权。同键只读原回执，第二新键拒绝。
- 使用原independent_monitor_research_v1快照与提示。原v3仍完整三股，v4/v5仍research_only。根及后代不进入正式组合决策或交易。
- 每次产生新效果前重核原证明、原请求身份、价档、窗口、共享日保护和当前已用额度。保留13USD及原五项自然义务，根没有自然credit；合法增量只由既有精确credit规则判定，未增加豁免。
- 旧未知费用、原预算日预留、审计与禁重放永久保留；本次新unknown仍阻塞且不得复用旧接受名单。
- 价格与SEC条件分别经原监控提案、反方、审计、精确采纳和来源资格验证。历史L1不授予实时价格权限；SEC recent baseline可为partial，partial无新增必须unknown，不冒充完整历史覆盖。
- watch、无合格计划、无真实命中均可为真实结果。只能正常一次新根；通知及对应增量必须由合法真实命中产生，不强造事件，也不证明设备收妥。

0122及0123只追加约束、唯一索引和精确验证分支，不更新历史行。无v6事实可降回0121；出现任意v6请求后拒绝降级，使用0122兼容维护镜像和原备份处理故障，不删除事实回退。

原增量准入在持有账户锁时先查v6根的永久claim，数据库BEFORE INSERT沿原plan/source_run与已持久化increment递归回溯同一v6根并原子占用一次。价格/SEC不同事件、继任计划、跨日和unknown均不释放。claims只追加且必须与精确已持久化increment绑定；不扩原增量credit或更改任何旧事实。有v6根后拒绝移除这项保护。
