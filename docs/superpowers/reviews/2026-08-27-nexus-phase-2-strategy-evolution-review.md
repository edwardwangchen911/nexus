# NEXUS Phase 2 Strategy Evolution Lab 验收与安全审查


**日期：** 2026-08-27

**审查基线：** `f0336b6`

**可执行代码范围：** `f0336b6..09f090d`（13 个提交，134 个文件）

**工作树：** `.worktrees/nexus-phase-2` / `codex/nexus-phase-2`

**审查方式：** 单一执行者分离代码、安全和验收阶段自审；本轮未使用独立子 Agent。

## 结论

Phase 2A 在本地、离线/测试边界内验收通过，无未解决 Critical 或 Important 发现。它已形成“固定研究 Agent 提案 → 确定性候选生成 → 时点正确回测 → 独立 Strategy Shadow → 确定性闸门/交接 → 内部模拟盘冠军注册表”闭环。

本结论不是部署或激活批准：实盘仍关闭，没有访问复星账户、券商写接口、实时资金/持仓或交易密钥，没有创建真实订单，没有部署或修改 `.openai/hosting.json`。

## 架构与产品边界审查

- Agent 只能调用 4 个固定只读研究工具，输出严格结构化假设和登记搜索轴；不持有数据库会话、文件系统、网络、代码执行器、Provider 或 Broker 能力。
- 候选集、OOS 试验计数、基准/挑战者配对、固定闸门、Shadow 进度、交接风控、晋级/回滚、CAS、幂等、审批和 Forward 绑定全部由确定性代码与 PostgreSQL 约束实现。
- 历史回测保持 `promotable=false` 和 `historical_agent_overlay=false`；无 Agent 历史覆盖，无前视数据或动态重置试验预算路径。
- 冠军只影响未来内部模拟决策；回滚通过追加注册事件实现，不改写历史。所有 Phase 2 API 响应固定 `live_execution_enabled=false`。
- 新 `strategy_rnd` worker 只消费隔离队列，且必须由 Engine 组合根显式注入 executor；没有 CLI、默认 Agent/Provider/Broker 实现或自动激活路径。
- 空数据库只有 `baseline_v1` GENESIS，没有 active research lineage，因此实验启动默认 fail closed。

## 审查发现与修复

| 级别 | 发现 | 处理与证据 |
|---|---|---|
| Important（已修复） | `strategy_rnd` 有队列和注入式 handler，但缺少可复用的专用运行时组合根，无法清晰证明队列隔离和 worker heartbeat。 | 新增 `engine/src/nexus/evolution/worker.py` 和 PostgreSQL 集成测试；只领取 `strategy_rnd`，必须注入 executor，不构造 Broker/Provider。 |
| Important（已修复） | 浏览器在 POST 后网络中断/5xx 时只报“未确认”，但不保留原幂等键，再操作可以新键重复提交。 | 页面保留完整路径、body、ETag 和幂等键；结果未知时关闭其他写操作，只允许原命令原键重放。E2E 验证两次请求字节语义和幂等键一致。 |
| Important（已修复） | Engine 把注册历史倒序输出，Web 信任边界只接受严格递增且末项等于当前 head；发生一次 pause/resume 后总览会被拒绝。 | Engine 先验证完整事件链，再返回最新 20 条的正序窗口；OpenAPI 增加 `maxItems=20`，Engine 和 Web 分别验证 21 条截断与倒序拒绝。 |
| Important（已修复） | 通用策略解释器在 6 组回测情景的每个周频决策点重复验证同一不可变 CompiledStrategy，使 7 年 × 50 标的性能闸门超时。 | 保留每个调用入口的完整验证，对完整冻结模型的成功验证做 256 项有界 LRU 缓存；异常不缓存。测试先预热合法对象，再证明伪造副本仍被拒绝，原 25 秒闸门不放宽并通过。 |
| Minor（未修复，部署前闸门） | 仓库内未找到完整的 CSP/HSTS/frame/referrer/permissions 响应头组合，无法在未部署前证明托管边缘会注入它们。 | 本轮部署明确不在授权范围，也不修改 `.openai/hosting.json`。首次部署前必须在目标边缘验证严格 CSP、`frame-ancestors`、HSTS、`nosniff`、Referrer-Policy 和 Permissions-Policy；未验证不得部署。 |

兼容性维护中同时修正了 `0022` 之后旧 paper raw-SQL 测试夹具缺少新权威列，以及 API 稳定错误码完整集未包含 Phase 2 两个错误码的测试债务。

## 验收证据

### 数据库与运行边界

- 独立 Compose 项目 `nexus-phase2-acceptance`，PostgreSQL 17 和 MinIO 均健康后执行。
- 新建库完成 `fresh -> 0021 -> 0022 -> 0021 -> 0022`，最终唯一 head 为 `0022_strategy_evolution`。
- 应用层 `registry_chain_valid=true`；验收库为 1 个 head 和 1 条 GENESIS，research lineage、experiment、Shadow order/fill、paper order、broker order snapshot、provider run、worker heartbeat 均为 0。
- 测试完成后 `nexus_test_*` 残留数为 0；随后删除了仅属于本轮的 Compose 容器、网络和可重建临时卷。

### Engine

- PostgreSQL 全量集成测试分区：`240 passed in 1019.09s`、`280 passed in 1198.50s`、`214 passed in 1060.28s`，合计 734。
- 最终解释器/缓存代码的所有直接相关 PostgreSQL 回归：`82 passed in 258.50s`。
- 非集成全量（unit/property/evals/contract/health）：`853 passed in 51.84s`。
- 解释器语义、伪造缓存键和原性能闸门：`5 passed in 28.35s`。
- `uv run ruff check ...`：通过。`uv run mypy ...`：278 个源文件通过。`alembic heads`：单一 `0022_strategy_evolution` head。

注：734 项全量集成分区后，唯一生产变更是不取消验证的有界缓存性能修复；因此又对其全部直接解释器/回测/策略进化/Shadow 路径执行了 82 项 PostgreSQL 复验，而不把性能修复前的全量数量冒充为最终树的单次全量运行。

### Web

- `npm test`：41 个文件、`447 passed in 16.78s`。
- `npm run check:generated`、`npm run typecheck`、`npm run lint`、`npm run build`：全部通过，生产构建枚举 6 个 Strategy Evolution BFF 路由。
- `npm run test:e2e`：`79 passed in 2.7m`，包含 375/390/768/1024/1440 与横屏视口、Axe critical/serious=0、409 权威刷新、未知网络结果原幂等键重放、forbidden 证据清空和回滚对话框键盘行为。

### 安全与供应边界

- API key 和 private-key header 模式扫描均为 0；HMAC 命中只来自空的 `infra/env.example` 变量名和固定测试夹具。
- `engine/src/nexus/evolution` 没有 Fosun、Broker worker、OrderCreate、`SIMULATION_WRITE` 或 `LIVE_WRITE` 引用。
- BFF 只接受同源写入，body 严格枚举/空 JSON，保留 HMAC 服务端域、64 KiB 请求上限、响应契约验证、CAS 和幂等。
- `git diff --check` 通过；没有密钥、实盘开关、券商写路径、部署或 Git 历史改写。

## 限制、回滚和下一道闸门

- 本轮是单一执行者自审，不能冒充为独立第二审查者意见。后续合并前可再做一次独立 review，但它不是当前本地编码完成的阻断项。
- 只有空 Phase 2 运行事实的本地库允许 `0022 -> 0021`；存在 experiment/Shadow/promotion/新 Forward 事实时降级必须 fail closed。
- 代码回滚使用普通新提交 `git revert`（本轮未执行），不改写历史。
- 任何部署、Strategy R&D 外部 Agent/provider 激活、创建 active lineage、实盘/券商写操作或真实订单，仍需新的明确批准；真实订单仍必须逐笔人工确认。
