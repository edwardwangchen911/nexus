# AAPL 纯研究聚合意图只读展示修复

本轮已落盘 AAPL 为 `research_only_v2`、`watch/none`，模型评价 sufficient、证据审计 complete，但缺正式价格权威使确定性质量为 partial。现有正式语义要求 complete，旧安全观察分支又拒绝 sufficient，导致聚合意图被隐藏；三阶段原文仍在。

仅在 ObservationReadback 增加展示专用形状：partial/watch/none、sufficient、审计 complete/support、八角色、正反论点、完整且已解决的审查。新分支必须验证绑定的纯研究快照用途、原始哈希、时间和证券身份，并保留原终态唯一性、输出哈希、模型/提示词、冻结证据及嵌套引用验证。正式 actionable/safe-no-trade、价格、时效和风控函数不变，不改变存储事实或把 partial 升为 complete。

先以相同形状的隔离 PG 正常 GET 回归重现 intent=null，再修复。覆盖普通快照、purpose/hash/instrument 损坏、引用/绑定损坏、非 watch、非 none、审查索引缺口；历史完整/有限输出回归保留。正常 GET 与摘要读取前后业务行摘要不变，正式消费继续拒绝。

本轮真实三股归档只读验证：使用正常研究 GET 处理函数，三股返回意图与原存储逐字段一致，三阶段 verified，质量 partial，正式消费均拒绝。没有模型调用。部署后必须重跑原三股页面验收，保留 intent 非空、刷新一致、用量对账和零写请求断言。最终候选全部 CI 通过才合入部署；新备份包含本轮研究，保留兼容回滚镜像。无 schema、依赖或配置变更。

私有证据位于 `.secrets/aapl-readonly-display-20261022/`；原始失败和不可重放标记保留在 `.secrets/optimized-three-stock-acceptance-20261022/`。本计划不含私人账户事实或完整研究文本。
