# NEXUS 真实 Fixture E2E 门禁实施计划

> **执行要求：** 每项行为变更使用 `superpowers:test-driven-development`；完成声明前使用
> `superpowers:verification-before-completion`；最终合并前执行独立代码与安全审查。

**目标：** 新增首条不使用 API Mock 的跨进程 E2E，证明空迁移库、Engine HMAC、Vinext BFF、
浏览器、PostgreSQL CAS/幂等和刷新持久化真实连通，同时保持复星、Provider、账户和交易关闭。

**架构：** 一个 test-only Node 启动器验证 loopback 专用数据库并运行迁移；独立 Playwright
配置启动真实 Engine/Web；`/integrations` 旅程执行一次安全 fixture 状态写入、同幂等键重放和
刷新验证；独立 CI Job 以临时 PostgreSQL service 运行该门禁。

**技术栈：** Node.js 22、TypeScript、Vitest、Playwright、Python 3.12、Alembic、FastAPI、
PostgreSQL 17、Vinext、GitHub Actions。

**规格：** `docs/superpowers/specs/2026-08-31-nexus-real-fixture-e2e-design.md`

## 全局约束

- 不读取、打印或传递 Provider、复星、Alpaca、FRED、OpenAI、账户或交易凭据。
- 不启动 worker、provider、broker 或 Gateway，不访问非 loopback 业务服务，不提交订单。
- 不创建/drop/truncate/delete 数据库；CI 提供临时空库，本地调用者提供专用 `nexus_e2e_*` 库。
- 不修改默认 `playwright.config.ts` 的 synthetic E2E，也不把真实 E2E 混入多 worker suite。
- 当前 worktree 唯一写入者为根 agent；审查 agent 只读。
- 每个行为先观察聚焦测试失败，再添加最小实现；每个连贯任务本地提交，不 push、不部署。

## Task 1：冻结真实 E2E 的仓库边界

**文件：**

- 修改：`scripts/ci/repository_policy.py`
- 修改：`tests/policy/test_repository_policy.py`

1. RED：增加策略测试，证明 `tests/e2e-real` 中出现 `page.route`、context route 或
   `route.fulfill` 会产生固定 finding。
2. RED：证明缺少真实配置/测试目录、`real-e2e` CI Job 或 `npm run test:e2e:real` 时失败。
3. GREEN：实现 `check_real_e2e_boundary`，仅扫描 test-only 目录和固定集成标记，并加入
   `run_all`。
4. 运行聚焦策略测试及完整 `tests/policy`。
5. 提交：`test(e2e): freeze real fixture boundary`。

## Task 2：实现安全启动器与专用 Playwright 配置

**文件：**

- 新增：`scripts/e2e/real-fixture.mjs`
- 新增：`scripts/e2e/run-real-fixture.mjs`
- 新增：`tests/unit/e2e/real-fixture-runner.test.ts`
- 新增：`playwright.real.config.ts`
- 修改：`package.json`

1. RED：测试缺失/格式错误/非 asyncpg/非 loopback/数据库名不以 `nexus_e2e_` 开头的 URL，
   以及 `NEXUS_ENV=production`，均在 runner 调用前失败且错误不含输入值。
2. RED：测试子进程环境只保留运行所需的非业务字段，并固定 test、fixture、Provider jobs off、
   synthetic HMAC；植入 credential canary，证明不进入迁移或 Playwright 环境。
3. RED：测试先迁移再 Playwright、迁移失败即停止、返回码传递及禁止 `--config` 覆盖。
4. GREEN：实现纯边界函数和 CLI runner；错误只使用固定代码/描述，不序列化环境。
5. GREEN：新增 real Playwright config，使用专用 loopback 端口、单 worker、真实 Engine/Web
   webServer；新增 `npm run test:e2e:real`。
6. 运行聚焦 Vitest、typecheck 和 lint。
7. 提交：`test(e2e): add safe real fixture runner`。

## Task 3：用真实浏览器证明 BFF、CAS、幂等与持久化

**文件：**

- 新增：`tests/e2e-real/integrations-real.spec.ts`

1. RED：在专用临时 PostgreSQL 容器/库中运行迁移后的空状态，先执行尚未实现完整断言的真实
   test，观察其因缺少旅程而失败；不添加 route mock。
2. GREEN：打开 `/integrations`，捕获真实 GET 响应并断言五个 Provider 的完整安全状态与
   Fosun `version=0`。
3. GREEN：点击按钮，断言真实 POST 的 URL/method/origin、幂等键、`If-Match: "0"`、空对象体，
   以及 response/UI 的 `fixture/version=1`。
4. GREEN：通过页面同源 fetch 使用同一业务头重放，断言 response 仍为 version 1；刷新页面，
   断言 GET 仍为 version 1 且 `as_of` 已更新。
5. 运行真实旅程两次，每次使用独立空数据库，证明无顺序依赖；确认没有非 loopback 请求或
   Provider 容器/进程。
6. 提交：`test(e2e): prove real fixture persistence`。

## Task 4：纳入 CI 与运维说明

**文件：**

- 修改：`.github/workflows/ci.yml`
- 修改：`docs/runbooks/ci-release-gates.md`
- 修改：`docs/runbooks/local-development.md`

1. RED：运行 Task 1 的 workflow 策略，观察缺少 `real-e2e` job/命令门禁。
2. GREEN：新增独立 `real-e2e` job：固定 PostgreSQL digest、synthetic `nexus_e2e_ci`、Node/uv
   锁定安装、Chromium、`npm run test:e2e:real`，失败 artifact 保持窄范围和三天保留。
3. 文档明确 synthetic 与 real E2E 的差异、专用数据库要求、默认不运行 Provider/交易，以及
   该门禁不构成复星激活证据；更新当前 job/测试事实时只写本轮实测结果。
4. 运行 workflow 策略、YAML/Compose 静态门禁及文档相关检查。
5. 提交：`ci(e2e): gate real fixture stack`。

## Task 5：完整验收、独立审查与本地合并

1. 在隔离临时 PostgreSQL 上全新运行：

   ```bash
   npm run test:e2e:real
   npm run test:e2e -- --workers=3
   npm test
   npm run lint
   npm run typecheck
   npm run build
   uv run pytest tests/policy -q
   uv run pytest engine/tests/integration/test_integrations_api.py -q --import-mode=importlib
   uv run ruff check engine scripts/ci
   uv run mypy engine/src
   ```

2. 验证 diff 无 secret signature、无 `page.route`/`route.fulfill`、无 Provider/Gateway/worker
   启动、无生产/账户/订单写操作；临时容器和数据库只清理本轮显式创建的目标。
3. 派发独立代码与安全审查；每个 P0-P2 或可执行范围内 finding 先补失败测试再修复，并重跑
   受影响与完整门禁。
4. 对比 base `217ac402b687891486d1fb4dd71300f4173eb707`；仅当 local main 仍是该祖先且合并可
   fast-forward 时本地合并。不得 push、部署、运行真实 Provider 或改变凭据/权限。

## 完成边界

本地完成表示 NEXUS 首次具备可持续运行的真实软件链路验收门禁；不表示复星 SDK 已准入、
真实行情/持仓已读取、SEC/FRED 已激活、策略已验证、平台已部署或可实盘。完成后下一本地开发
优先项是版本化个人 Investment Policy 与组合级 Agent Case；复星外部材料齐备时再进入单独的
受控只读激活。
