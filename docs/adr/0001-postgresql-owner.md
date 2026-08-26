# ADR 0001：PostgreSQL 作为交易事实的事务权威

- 状态：Accepted
- 日期：2026-08-27

## 背景

NEXUS 必须在并发任务、内部模拟订单、资金/仓位预留、双重记账、Shadow/Backtest authority、审计链和 outbox 之间保持可验证的事务不变量。MySQL 在抽象上可以承担部分业务，但“能用”不等于当前切换有收益。

## 决策

PostgreSQL 17 是 NEXUS 当前唯一事务权威。对象存储只保存带摘要/清单绑定的大对象，DuckDB/Polars 只做受控计算，二者都不代替 PostgreSQL 的状态机和账本权威。

当前实现明确依赖：

- `pg_advisory_xact_lock` 串行化账户、job key、审计链和多种 authority root；
- `FOR UPDATE SKIP LOCKED` 实现多 worker 的非阻塞领取，配合 lease/fence 防止过期 worker 提交；
- PostgreSQL `JSONB` 类型、查询和数据库 CHECK 约束用于冻结合同/证据文档；
- `DEFERRABLE INITIALLY DEFERRED` constraint trigger 在 commit time 校验精确集合、账本平衡、事件链和跨表 root 一致性；
- PostgreSQL-specific Alembic 迁移和集成测试已经是当前验收证据的一部分。

## 后果

优点是风控、订单、账本和审计不变量可在同一事务及数据库 guard 中证明，并且本地/CI/恢复演练使用同一语义。代价是不追求数据库无关的 ORM 表面兼容。

不选择当前迁移到 MySQL，因为那不是修改连接串或方言就能完成的替换：需要重新设计锁、JSON 约束、commit-time guard、迁移可逆性、恢复和所有并发/原始 SQL 绕过测试，当前没有产品收益可以抵消这一风险。

## 未来重议条件

只有在出现明确的运维/成本/法规约束，并且先形成逐项等价的事务不变量与数据迁移/回滚计划时，才重新评估数据库切换。这将是独立架构项目，不应夹带在业务功能中。
