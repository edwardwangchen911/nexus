# Task 19 内部模拟成交、绩效与模拟盘实施计划

> 实施依据：`docs/superpowers/specs/2026-08-26-task-19-internal-paper-execution-design.md`  
> 方法：每个切片先写会失败的测试，再写最小实现，运行切片验证，然后本地提交。

## Slice 1：执行模型与时间窗口

- [x] 新增 `engine/tests/unit/paper/test_fills.py`，覆盖严格模型、UTC/DST、窗口连续、`close_volume_vwap_v1`、零成交量、10/25 bps、tick 保守取整和 1% 上限。
- [x] 在 `engine/src/nexus/paper/fills.py` 实现冻结值对象、执行模型规范哈希、窗口准入和 fill 计算。
- [x] 扩展 `engine/src/nexus/paper/models.py` 仅放共用的 fill/持仓值对象，保持 `extra=forbid` 和精确 Decimal。（本切片值对象仅被执行域使用，保留在 `fills.py` 以避免扩大共用模型。）
- [x] 运行 `pytest engine/tests/unit/paper/test_fills.py -q`、Ruff 和 Mypy 相关路径。
- [x] 提交：`feat(engine): define deterministic paper fills`

## Slice 2：执行证据迁移和数据库守卫

- [x] 新增 `engine/tests/integration/test_paper_execution_migration.py`，先断言 0015 表/列/约束/触发器/种子和 downgrade/upgrade 精确往返。
- [x] 新增 `engine/alembic/versions/0015_paper_execution.py`，仅向前增表/增列，不修改 `0014_paper.py`。
- [x] 更新 `engine/src/nexus/persistence/models/backtest.py` 的 session open/close 列和 `paper.py` 的 Task 19 表模型。
- [x] 新增 PostgreSQL 窗口反例：未来 bar、非连续/5 根以外、跨标的、换源和更新不可变事实。成交根、超容量和超订单反例在 Slice 4 与原子 worker 一起验证。
- [x] 运行迁移往返和定向 PostgreSQL 测试。
- [x] 提交：`feat(db): add paper execution authorities`

## Slice 3：买卖成交记账与持仓投影

- [x] 扩展 `engine/tests/unit/paper/test_ledger.py`，先覆盖买入消耗/释放、卖出盈利/亏损/费用和所有账户平衡。
- [x] 新增 `engine/tests/unit/paper/test_performance.py`，先覆盖平均成本、连续买入、部分卖出、不足持仓、修正事件和规范哈希。
- [x] 在 `engine/src/nexus/paper/ledger.py` 实现买/卖 fill 和 release 事务工厂。
- [x] 在 `engine/src/nexus/paper/performance.py` 实现持仓调整、已实现/未实现损益、NAV、峰值/回撤纯函数；instrument/sector 归因聚合随 Slice 5 的权威读取和原子发布实现，避免在无数据库源事实时构造不完整归因。
- [x] 扩展属性测试，验证任意有效输入下不超限、不为负和借贷平衡。
- [x] 运行 paper 领域单测/属性测试、Ruff 和 Mypy（70 passed；Ruff/Mypy 通过）。
- [x] 提交：`feat(engine): post paper fills and performance`

## Slice 4：原子 worker 服务

- [x] 新增 `engine/tests/integration/test_paper_execution_pg.py`，先覆盖首窗口成交、多窗口部分成交、无报价 25 bps、合格报价 10 bps，10:15 缺数据取消、半日市余量取消、预留不足 fail closed。
- [x] 新增并发/幂等测试：两个 worker 同时推进、worker 与用户取消竞争、同键同/异请求、终态重试。
- [x] 新增故障注入：在 fill、账本、持仓事件、预留事件和 order event 之间抛错，确认全部回滚。
- [x] 在 `engine/src/nexus/paper/execution_service.py` 实现事务锁、权威加载、原子持久化、幂等和投影验证；新增 `0016_paper_execution_atomic` 前向迁移，在数据库层绑定 fill/order/reservation/position/ledger 根并强制终态预留归零。
- [x] 添加足以支持测试的 Task 19 fixture helper，禁止任何 provider 网络调用。
- [x] 运行全部 Task 18–19 PostgreSQL 原子性/并发/数据库守卫测试（74 passed），并运行执行迁移往返/元数据契约测试（27 passed）。
- [x] 提交：`feat(engine): execute internal paper orders atomically`

## Slice 5：日末绩效发布

- [x] 新增 `engine/tests/integration/test_paper_performance_pg.py`，先覆盖完整收盘权威、缺价拒绝、账本现金、持仓市值、已/未实现损益、费用/滑点、峰值/回撤、instrument/sector 归因和幂等发布。
- [x] 在 `performance.py` 增加仅从 PostgreSQL 冻结权威读取和同一事务发布 snapshot/attribution 的服务；不含 provider 网络或定时 worker。
- [x] 将 performance/position 快照加入源事实哈希验证和不可变守卫，内容哈希覆盖账本、持仓、价格、执行模型、归因与行业权威。
- [x] 运行绩效 PostgreSQL 定向测试（13 passed）、Ruff 和 Mypy；包含并发、故障回滚和数据库不可变反例。
- [x] 提交：`feat(engine): publish paper performance snapshots`

## Slice 6：Engine API 和生成合约

- [x] 新增 `engine/tests/unit/api/test_paper_contracts.py`、`test_paper_cursor.py` 和 `engine/tests/integration/test_paper_api.py`，先覆盖严格输入/输出、Decimal 字符串、脱敏、有界游标、status 过滤、时间线与取消竞争。
- [x] 新增 `engine/src/nexus/api/schemas/paper.py`、`engine/src/nexus/api/routes/paper.py` 和只读 service，并在 `main.py` 注册；仅开放 portfolio/orders 读取和用户取消，无 fill/advance/create-order 路由。
- [x] 实现签名 keyset cursor、账户与页大小上限、只由服务端折叠预留和终态后判定 `cancellable`。
- [x] 运行 paper API/contract 测试（12 passed）与全局 OpenAPI 合约（7 passed），然后运行 `npm run generate:client`、`check:generated` 和 TypeScript typecheck。
- [x] 提交：`feat(api): expose internal paper console`

## Slice 7：Web BFF 安全边界

- [x] 新增 `tests/unit/paper/routes.test.ts` 和 `runtime-boundary.test.ts`，先覆盖 GET/POST 映射、严格 query/body/header、动态 UUID 路径、响应上限和不可客户端导入。
- [x] 新增 `app/api/v1/paper/portfolio/route.ts`、`app/api/v1/paper/orders/route.ts`、`app/api/v1/paper/orders/[orderId]/cancel/route.ts`。
- [x] 扩展 `app/lib/engine/client.ts` 的允许路径、规范查询和 paper 响应上限，不放宽其他终端；同时收紧出站 caller header 为闭合 allowlist。
- [x] 运行 paper BFF 单测、typecheck 和生成合约检查。
- [x] 提交：`feat(web): add paper BFF boundary`

## Slice 8：`/paper` 页面和订单时间线

- [x] 新增 `tests/unit/paper/page.test.tsx`、`order-timeline.test.tsx` 和边界测试，先覆盖全状态、方向/状态标签、数字格式、事件顺序、source/feed/as-of 和取消确认。
- [x] 新增 `app/(protected)/paper/page.tsx`、`app/components/order-timeline.tsx` 和必要的独立类名样式；不改变 NEXUS 现有视觉语言。
- [x] 确认已有导航 `/paper` 可到达，屏幕阅读器、键盘、焦点、表格和动态状态均符合无障碍要求。
- [x] 运行 paper 页面单测、全量 Web 单测、lint 和 typecheck。
- [x] 提交：`feat(web): build internal paper console`

## Slice 9：E2E、全量门禁和验收

- [ ] 新增 `tests/e2e/paper.spec.ts`，使用真实 Engine fixture 覆盖 ready/partial/stale/error、游标分页、时间线与取消；不 mock 网络内部业务规则。
- [ ] 对 `/paper` 运行 axe，修复所有 critical/serious 问题。
- [ ] 运行 Engine 全量 Pytest、Ruff、Mypy；Web 全量 Vitest、ESLint、TypeScript、生成合约、build 和全量 E2E。
- [ ] 运行安全审查，重点检查订单取消越权、幂等绕过、数据前视、跨账户关联、响应泄密和账本不平。
- [ ] 完成独立审查，修复所有 Critical/Important，重跑受影响门禁。
- [ ] 新增 `docs/superpowers/reviews/2026-08-26-task-19-internal-paper-execution-review.md`，记录命令、结果、已修复问题和未解决项。
- [ ] 更新主计划和本地进度账簿，提交：`docs: record Task 19 paper execution acceptance`

## 中止条件

以下情况立即停止并向用户请求确认：真实账户/券商写操作、真实订单、部署、密钥或权限、付费、破坏性操作、Git 历史改写或明显扩大产品范围。
