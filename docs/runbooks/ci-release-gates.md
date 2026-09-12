# CI 与离线发布门禁 runbook

## 边界

`.github/workflows/ci.yml` 只验证 synthetic fixture、空 PostgreSQL 和本地回环服务。它不读取 GitHub Secrets，不接触复星、真实账户、真实行情 Provider 或 OpenAI；`permissions` 固定为 `contents: read`。依赖安装和漏洞数据库查询可以访问公开包服务，但复星 gateway 测试必须在 socket guard 下运行，任何非 loopback DNS/TCP 都在系统调用前失败。

CI 不部署、不发布镜像、不改变权限，也不构成复星 SDK admission 或真实只读激活证据。

## 六个独立 job

| Job | 门禁 |
| --- | --- |
| `web` | npm clean install、lint、typecheck、unit、generated client、production build |
| `gateway-offline` | guard 自测、guard 下全部 gateway 测试、Ruff、Mypy |
| `engine-postgres` | 固定 digest PostgreSQL、空库迁移到 head、全部 Engine 测试、隔离 PostgreSQL＋MinIO fixture 恢复演练、Ruff、Mypy |
| `e2e` | Playwright Chromium、3 workers 的 synthetic browser E2E；可使用浏览器层的合成 API 响应来验证页面契约 |
| `real-e2e` | 固定 digest PostgreSQL 17 的专用 `nexus_e2e_ci` 空库、锁定 Node/uv/npm/Python 依赖与 Chromium，只运行 `npm run test:e2e:real`，验证本地 Engine、同源 BFF、PostgreSQL 的 fixture 链路 |
| `policy` | 全部策略测试、密钥/精确只读面/TLS 修复/SDK lock/容器/CI 策略、依赖许可证清单、npm/Python 漏洞审计、四个 Compose profile |

第三方 Actions 使用完整 commit SHA；容器引用同时保留可读 tag 和不可变 SHA-256 digest。两个运行镜像最终都声明非 root 用户，`.dockerignore` 排除 VCS、secret、env、依赖缓存和测试产物域。

`real-e2e` 使用 synthetic 用户、密码、HMAC 和临时 service 数据库；不读取 GitHub Secrets，不启动 Provider、Gateway、worker 或 MinIO，也不访问外部业务 endpoint。它与 `e2e` 分开：前者禁止替换网络响应，覆盖本地真实 fixture 链路；后者覆盖浏览器页面契约。两者均不构成复星 SDK admission、复星激活、真实数据、账户或订单证据。

## 本地复现

本地结果只覆盖实际执行的命令，不能自动外推为六个 GitHub job 全部通过。CI 失败时先确认
run 的 head SHA 与待验收提交一致，并一次性检查所有失败或取消的 job；收齐日志、完成归因后
再修改和推送，避免逐个错误反复触发新 run。时间相关测试优先使用受控时钟或条件等待，复杂
E2E 可以设置有界的单测试超时，但不得删除断言或降低安全、策略门禁。完成判定以同一 head
SHA 的全部必需 job 成功为准。

```bash
NEXUS_OFFLINE_SOCKET_GUARD=1 \
PYTHONPATH="$PWD/scripts/offline_guard" \
uv run python scripts/offline_guard/selftest.py

install -d -m 700 "$PWD/.nexus-pytest"
NEXUS_OFFLINE_SOCKET_GUARD=1 \
PYTHONPATH="$PWD/scripts/offline_guard" \
uv run pytest integrations/fosun_gateway/tests -q \
  --basetemp="$PWD/.nexus-pytest/gateway"

uv run pytest tests/policy -q
uv run python scripts/ci/repository_policy.py \
  --root . \
  --inventory-output /tmp/nexus-dependency-inventory.json

npm audit --audit-level=high
uv run pip-audit --local --skip-editable
```

`pip-audit` 跳过两个 editable 的本地 NEXUS package，因为它们不是第三方发行依赖；本地代码由 Pytest、Ruff、Mypy、E2E 和策略门禁覆盖。审计返回任何第三方已知漏洞时 CI 失败，不通过忽略项静默放行。

## Artifact 与日志

只有 `e2e` 或 `real-e2e` 失败时上传各自独立命名的 `test-results/**` artifact，保留 3 天并排除隐藏文件。测试环境只使用 synthetic 用户、账户、金额和响应；不得扩大 artifact 路径到 `.wrangler`、环境文件、完整日志、数据库 dump 或任意工作区目录。其他 job 不上传 artifact。

策略脚本只报告规则名、路径和通用原因，不打印匹配到的密钥内容。依赖 inventory 写入 runner 临时目录且不上传。

## 依赖与镜像更新

1. 从官方发行来源确认目标版本和安全修复；更新直接约束并重新生成 `uv.lock`/`package-lock.json`。
2. 运行 npm/Python 漏洞审计、许可证 inventory 和全量受影响测试；任何强 copyleft 或无许可证元数据必须有单独书面准入，不能只改 allowlist。
3. 镜像升级同时更新可读 tag 与 registry digest，运行两个 Dockerfile `--check`、四个 Compose profile config 和一次本地空库迁移。
4. 复星 SDK 仍由独立 machine-readable admission 控制；`uv.lock` 只允许官方 Git URL、
   `fsopenapi==0.1.0` 与已审核 requested/resolved commit 完全一致的唯一条目。不要借普通依赖升级绕过该门禁。

## 历史本地证据（2026-08-27）

以下记录早于 `real-e2e` job，不包括该门禁，也不代表 GitHub-hosted run 已发生。

- 完整 `npm audit`（含 CI/build devDependencies）：0 vulnerability；`pip-audit`：0 known vulnerability；
- 首次 Python 审计发现 11 条已知漏洞，最小升级到 cryptography 50.0.1、pyarrow 23.0.1、pytest 9.0.3、pytest-asyncio 1.4.0 后清零；首次完整 npm 审计发现 16 条（其中 12 high），升级 Cloudflare/Vinext/Vite/Wrangler、React RSC 和传递依赖后清零；
- gateway 145/145、Engine unit 704/704、Web unit 424/424、Chromium synthetic E2E 64/64；加固后从空 PostgreSQL 顺序迁移至 `0021_manual_job_retry_allowance` 并完成 Engine 全量 1432/1432（23 分 00 秒），临时容器残留 0；
- 加固后 fixture 恢复演练验证 dump 736,450 bytes、1 个对象/245 bytes、migration head、表计数、账本、READY/绩效/审计根与对象哈希，database/object/container residue 全部为 0；
- 策略测试 12/12，761 个第三方依赖完成许可证 inventory，Ruff/Mypy/生成客户端/生产构建/Compose config 均通过；
- 空 PostgreSQL 验证容器结束后残留为 0。

以上是本机离线发布证据，不代表 GitHub-hosted run 已发生，也不代表部署或复星真实激活。
