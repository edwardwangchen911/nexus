# Task 19 内部模拟成交、绩效与模拟盘实施计划

> 实施依据：`docs/superpowers/specs/2026-08-26-task-19-internal-paper-execution-design.md`  
> 方法：每个切片先写会失败的测试，再写最小实现，运行切片验证，然后本地提交。

## Slice 1：执行模型与时间窗口

- [ ] 新增 `engine/tests/unit/paper/test_fills.py`，覆盖严格模型、UTC/DST、窗口连续、`close_volume_vwap_v1`、零成交量、10/25 bps、tick 保守取整和 1% 上限。
- [ ] 在 `engine/src/nexus/paper/fills.py` 实现冻结值对象、执行模型规范哈希、窗口准入和 fill 计算。
- [ ] 扩展 `engine/src/nexus/paper/models.py` 仅放共用的 fill/持仓值对象，保持 `extra=forbid` 和精确 Decimal。
- [ ] 运行 `pytest engine/tests/unit/paper/test_fills.py -q`、Ruff 和 Mypy 相关路径。
- [ ] 提交：`feat(engine): define deterministic paper fills`

## Slice 2：执行证据迁移和数据库守卫

- [ ] 新增 `engine/tests/integration/test_paper_execution_migration.py`，先断言 0015 表/列/约束/触发器/种子和 downgrade/upgrade 精确往返。
- [ ] 新增 `engine/alembic/versions/0015_paper_execution.py`，仅向前增表/增列，不修改 `0014_paper.py`。
- [ ] 更新 `engine/src/nexus/persistence/models/backtest.py` 的 session open/close 列和 `paper.py` 的 Task 19 表模型。
- [ ] 新增 PostgreSQL 反例：未来 bar、非连续/5 根以外、跨账户/标的、换源、超容量、超订单、成交不完整根、更新/删除不可变事实。
- [ ] 运行迁移往返和定向 PostgreSQL 测试。
- [ ] 提交：`feat(db): add paper execution authorities`

## Slice 3：买卖成交记账与持仓投影

- [ ] 扩展 `engine/tests/unit/paper/test_ledger.py`，先覆盖买入消耗/释放、卖出盈利/亏损/费用和所有账户平衡。
- [ ] 新增 `engine/tests/unit/paper/test_performance.py`，先覆盖平均成本、连续买入、部分卖出、不足持仓、修正事件和规范哈希。
- [ ] 在 `engine/src/nexus/paper/ledger.py` 实现买/卖 fill 和 release 事务工厂。
- [ ] 在 `engine/src/nexus/paper/performance.py` 实现持仓调整投影、已实现/未实现损益、NAV、峰值/回撤和归因纯函数。
- [ ] 扩展属性测试，验证任意有效输入下不超限、不为负和借贷平衡。
- [ ] 运行 paper 领域单测/属性测试、Ruff 和 Mypy。
- [ ] 提交：`feat(engine): post paper fills and performance`

## Slice 4：原子 worker 服务

- [ ] 新增 `engine/tests/integration/test_paper_execution_pg.py`，先覆盖首窗口成交、多窗口部分成交、无报价 25 bps、合格报价 10 bps，10:15 缺数据取消、半日市余量取消、预留不足 fail closed。
- [ ] 新增并发/幂等测试：两个 worker 同时推进、worker 与用户取消竞争、同键同/异请求、终态重试。
- [ ] 新增故障注入：在 fill、账本、持仓事件、预留事件和 order event 之间抛错，确认全部回滚。
- [ ] 在 `engine/src/nexus/paper/execution_service.py` 实现事务锁、权威加载、原子持久化、幂等和投影验证。
- [ ] 添加足以支持测试的 Task 19 fixture helper，禁止任何 provider 网络调用。
- [ ] 运行全部 Task 18–19 PostgreSQL 原子性/并发/数据库守卫测试。
- [ ] 提交：`feat(engine): execute internal paper orders atomically`

## Slice 5：日末绩效发布

- [ ] 新增 `engine/tests/integration/test_paper_performance_pg.py`，先覆盖完整收盘权威、缺价拒绝、账本现金、持仓市值、已/未实现损益、费用/滑点、峰值/回撤、instrument/sector 归因和幂等发布。
- [ ] 在 `performance.py` 增加仅从 PostgreSQL 权威读取和原子发布 snapshot/attribution 的服务。
- [ ] 将 performance/position 快照加入源事实哈希验证和不可变守卫。
- [ ] 运行绩效定向单测和 PostgreSQL 测试。
- [ ] 提交：`feat(engine): publish paper performance snapshots`

## Slice 6：Engine API 和生成合约

- [ ] 新增 `engine/tests/unit/api/test_paper_contracts.py` 和 `engine/tests/integration/test_paper_api.py`，先覆盖严格输入/输出、Decimal 字符串、脱敏、有界游标、status 过滤、时间线与取消竞争。
- [ ] 新增 `engine/src/nexus/api/schemas/paper.py`、`engine/src/nexus/api/routes/paper.py` 和只读 service，并在 `main.py` 注册。
- [ ] 实现签名 keyset cursor、账户与页大小上限、只由服务端判定 `cancellable`。
- [ ] 运行 API/contract/OpenAPI 测试，然后 `npm run generate:client`。
- [ ] 提交：`feat(api): expose internal paper console`

## Slice 7：Web BFF 安全边界

- [ ] 新增 `tests/unit/paper/routes.test.ts` 和 `runtime-boundary.test.ts`，先覆盖 GET/POST 映射、严格 query/body/header、动态 UUID 路径、响应上限和不可客户端导入。
- [ ] 新增 `app/api/v1/paper/portfolio/route.ts`、`app/api/v1/paper/orders/route.ts`、`app/api/v1/paper/orders/[orderId]/cancel/route.ts`。
- [ ] 扩展 `app/lib/engine/client.ts` 的允许路径、规范查询和 paper 响应上限，不放宽其他终端。
- [ ] 运行 paper BFF 单测、typecheck 和生成合约检查。
- [ ] 提交：`feat(web): add paper BFF boundary`

## Slice 8：`/paper` 页面和订单时间线

- [ ] 新增 `tests/unit/paper/page.test.tsx`、`order-timeline.test.tsx` 和边界测试，先覆盖全状态、方向/状态标签、数字格式、事件顺序、source/feed/as-of 和取消确认。
- [ ] 新增 `app/(protected)/paper/page.tsx`、`app/components/order-timeline.tsx` 和必要的独立类名样式；不改变 NEXUS 现有视觉语言。
- [ ] 确认已有导航 `/paper` 可到达，屏幕阅读器、键盘、焦点、表格和动态状态均符合无障碍要求。
- [ ] 运行 paper 页面单测、全量 Web 单测、lint 和 typecheck。
- [ ] 提交：`feat(web): build internal paper console`

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
