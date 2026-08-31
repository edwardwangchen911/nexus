# NEXUS 真实 Fixture E2E 门禁设计

**状态：** 已依据 NEXUS 长期目标中的自主本地实施授权完成整体自审。本规格只授权本地、
可逆的测试代码、CI 配置、文档、测试数据库迁移与 fixture 状态写入；不授权复星或其他
Provider 请求、账户访问、券商写入、订单、部署、密钥/权限变更或生产数据库操作。

## 问题

NEXUS 已有前端、BFF、Engine、PostgreSQL 迁移和大量分层测试，但当前 Playwright 门禁主要
通过 `page.route` 返回合成 API 响应。它能验证页面行为，却不能证明以下真实链路同时成立：

```text
浏览器身份头 -> Vinext BFF -> 同源写入校验 -> Engine HMAC/nonce
-> FastAPI -> PostgreSQL 事务/CAS/幂等 -> BFF 响应收敛 -> 页面刷新后持久化
```

`nexus-local-stack up` 已能启动 PostgreSQL、MinIO、迁移、API 与 worker，继续扩展 Agent 或
先构造完整 fixture 行情/财报 seed 都不能替代上述缺失的集成证据。复星真实只读激活仍受
SDK 许可、当前契约复审、精确 endpoint、测试向量、安全/TLS 证据、轮换后凭据和运行时
权限材料阻塞，因此本设计不得把真实 Provider 当作测试前置。

## 方案比较

### 方案 A：专用真实 Fixture E2E（采用）

新增独立 Playwright 配置和 CI Job，在专用空 PostgreSQL 上运行迁移，启动真实 Engine 与
Web，只执行 `/integrations` 的本地 fixture 连通性闭环。它以最小改动证明现有系统边界，
不改变本地或生产运行拓扑。

### 方案 B：现在把 Web 加入 Compose

这能让 `nexus-local-stack` 覆盖更多进程，但同时引入 Node 镜像、Vinext/Cloudflare 开发运行时、
容器身份头和生命周期管理。它解决的是开发便利性，不是当前缺失的真实闭环证据；在首条真实
E2E 通过前实施，故障归因也更困难，暂缓。

### 方案 C：先建立完整 fixture authority seed

写入 49 个标的、ETF 暴露、XNYS 日历、行情和财报 fixture 可以演示更多页面，但会新增一套
需要长期维护的伪权威。当前目标是验证基础链路而非模拟“已有真实数据”，暂缓；缺少权威时继续
显示 `empty/partial/stale` 才符合 fail-closed 产品约定。

## 采用的真实旅程

测试使用 migration-only 的数据库，不添加业务 seed：

1. 安全启动器只接受 `postgresql+asyncpg`、loopback host、且数据库名以 `nexus_e2e_` 开头的
   `NEXUS_REAL_E2E_DATABASE_URL`；拒绝 production 环境，不创建、删除、清空或重置数据库。
2. 启动器将 URL 仅映射为迁移与 Engine 子进程的 `DATABASE_URL`，运行 `alembic upgrade head`；
   子进程环境固定为 test/fixture，Provider jobs 关闭，且不传递 Provider、OpenAI、券商或复星
   凭据域。
3. Playwright 以单 worker 启动真实 Engine（专用 loopback 端口）和 Vinext Web（另一专用
   loopback 端口），使用固定 synthetic 用户和固定 synthetic Engine HMAC。
4. 浏览器打开 `/integrations`，等待真实同源 `GET /api/v1/integrations`，断言五个 Provider 的
   完整安全投影：复星为 `fixture/PASS/version=0`，其余为 `disabled/PARTIAL/version=0`，无能力
   或契约伪装。
5. 点击“测试非交易连接”，断言真实 POST 为同源请求，并携带非空 `Idempotency-Key` 与
   `If-Match: "0"`；Engine 返回 `fixture/version=1`，页面显示成功状态。
6. 浏览器使用相同业务请求体、幂等键和 CAS 版本再次调用 BFF。BFF 必须生成新的 HMAC nonce，
   Engine 必须从幂等记录返回相同 `version=1`，而不是重复写成 version 2。
7. 页面刷新并再次读取真实 GET，仍显示 `version=1` 与更新后的 `as_of`。这同时排除前端本地
   状态假成功和未持久化响应。

该 POST 只更新 `integration_status` 和 `idempotency_records` 中的 synthetic fixture 状态；它不
构造 capability、不访问账户、不读取行情、不启动 Gateway，也不是复星连通性或真实数据证据。

## 组件

### 安全启动器

`scripts/e2e/run-real-fixture.mjs` 负责边界校验、迁移与启动 Playwright：

- URL 不满足 test-only 规则时在任何数据库或子进程动作前失败；错误不得回显 URL、用户名、
  密码或环境值；
- 拒绝全部 CLI 参数，避免通过筛选、分片、重试、worker 或空测试选项削弱固定门禁；调试需直接
  使用底层 Playwright 命令，不得冒充本门禁结果；
- 在任何数据库动作前拒绝项目根目录的 `.env*` 与 `.dev.vars*`，同时关闭 Cloudflare/Vinext
  隐式环境文件与进程环境继承；
- 迁移前由独立 guard 确认数据库没有非系统 schema 或用户对象，并持有非阻塞 PostgreSQL
  advisory lock；连接与锁覆盖迁移、Playwright 和结果验证的完整生命周期；
- 迁移失败即停止，不启动浏览器；Playwright 返回码原样传递；
- 子进程环境从明确 allowlist 构造，再覆盖 synthetic test 配置，避免把调用 shell 中的 Provider
  或交易凭据继承给 Engine/Web；
- 不运行 `docker compose down -v`、DROP/TRUNCATE/DELETE 或其他清理。数据库生命周期属于调用者
  或 CI 的临时 service。
- 固定 JSON reporter 必须证明恰好一个命名旅程通过；仅有 Playwright 退出码 0 不足以通过门禁。

纯函数放在同一可导入模块中，以 Vitest 覆盖 URL、环境清洗、参数和执行顺序；真实子进程通过
注入 runner 测试，避免单元测试启动服务。

### Playwright 配置

`playwright.real.config.ts` 与现有 synthetic `playwright.config.ts` 隔离：

- `testDir=tests/e2e-real`、固定单一 spec、Chromium、单 worker、禁止重试和 Service Worker；
- Engine 只绑定 `127.0.0.1`，Web 只绑定 `127.0.0.1`；
- Engine 环境固定 `NEXUS_ENV=test`、`AGENT_RUNTIME_MODE=fixture`、
  `NEXUS_PROVIDER_JOBS_ENABLED=false`；
- Web 只接收 synthetic 用户、Engine loopback URL 与 synthetic HMAC；
- 不启动 worker、provider-worker、broker-worker、fosun-gateway 或 MinIO；本旅程不读取对象存储。

### 无 Mock 约束

`tests/e2e-real` 内禁止 `page.route`、`browserContext.route`、`route.fulfill` 和同类网络响应替换。
仓库策略门禁扫描该独立目录，并要求 CI 保留 `real-e2e` Job 和 `npm run test:e2e:real`。真实测试
可以监听 request/response 进行断言，但不能修改或满足请求。

### CI

新 Job 使用固定 digest 的 PostgreSQL 17 service、synthetic 数据库用户和 `nexus_e2e_ci` 数据库，
同时安装锁定的 Node/Python 依赖与 Chromium。它不引用 GitHub Secrets，不访问 Provider，失败时
仅上传既有 `test-results/**`，保留三天。

该 Job 与 synthetic E2E 分开，便于区分“页面契约回归”和“真实跨进程链路回归”。

## 失败语义

- URL、环境或参数越界：在迁移前失败；
- 迁移失败：不启动服务；
- 端口占用或 Engine/Web 未健康：Playwright webServer 失败；
- 初始状态不是 version 0：说明数据库并非本旅程所需的空状态，测试失败且不尝试清理；
- 请求头、HMAC、CAS、幂等或持久化任一失效：旅程失败；
- 任一失败都不得 fallback 到 `page.route`、fixture HTTP server 或真实 Provider。

## 延后事项

本子项目不实现：Web Compose service、完整本地 seed、XNYS 日历生成、真实行情/财报采集、
复星 SDK admission、真实只读 probe、模拟券商写入、实盘或 Agent 扩容。真实 E2E 通过后，下一步
仍按既定优先级处理个人 Investment Policy/组合上下文；复星外部材料齐备后再单独执行已批准范围
内的受控只读激活流程。

## 完成标准

本地完成要求：安全启动器单元测试、仓库策略测试、真实 Playwright 旅程、现有 synthetic E2E
受影响回归、lint/typecheck/build、Engine 相关回归全部通过；独立代码与安全审查无未处理 P0-P2；
变更仅本地提交并在 main 未漂移时 fast-forward 合并。它证明“基础软件链路可运行”，不证明复星
真实数据、账户同步、策略有效性、部署或实盘可用。
