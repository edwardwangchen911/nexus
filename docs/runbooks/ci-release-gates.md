# CI 与离线发布门禁 runbook

## 边界

`.github/workflows/ci.yml` 只验证 synthetic fixture、空 PostgreSQL 和本地回环服务。它不读取 GitHub Secrets，不接触复星、真实账户、真实行情 Provider 或 OpenAI；`permissions` 固定为 `contents: read`。依赖安装和漏洞数据库查询可以访问公开包服务，但复星 gateway 测试必须在 socket guard 下运行，任何非 loopback DNS/TCP 都在系统调用前失败。

CI 不部署、不发布镜像、不改变权限，也不构成复星 SDK admission 或真实只读激活证据。

## 五个独立 job

| Job | 门禁 |
| --- | --- |
| `web` | npm clean install、lint、typecheck、424 项 unit、generated client、production build |
| `gateway-offline` | guard 自测、guard 下全部 145 项 gateway 测试、Ruff、Mypy |
| `engine-postgres` | 固定 digest PostgreSQL、空库迁移到 head、全部 Engine 测试、隔离 PostgreSQL＋MinIO fixture 恢复演练、Ruff、Mypy |
| `e2e` | Playwright 锁定 Chromium、3 workers、64 项 synthetic E2E |
| `policy` | 12 项策略测试、密钥/禁止路由/TLS 修复/SDK lock/容器/CI 策略、761 项依赖许可证清单、npm/Python 漏洞审计、两 Compose profile |

第三方 Actions 使用完整 commit SHA；容器引用同时保留可读 tag 和不可变 SHA-256 digest。两个运行镜像最终都声明非 root 用户，`.dockerignore` 排除 VCS、secret、env、依赖缓存和测试产物域。

## 本地复现

```bash
NEXUS_OFFLINE_SOCKET_GUARD=1 \
PYTHONPATH="$PWD/scripts/offline_guard" \
uv run python scripts/offline_guard/selftest.py

NEXUS_OFFLINE_SOCKET_GUARD=1 \
PYTHONPATH="$PWD/scripts/offline_guard" \
uv run pytest integrations/fosun_gateway/tests -q

uv run pytest tests/policy -q
uv run python scripts/ci/repository_policy.py \
  --root . \
  --inventory-output /tmp/nexus-dependency-inventory.json

npm audit --audit-level=high
uv run pip-audit --local --skip-editable
```

`pip-audit` 跳过两个 editable 的本地 NEXUS package，因为它们不是第三方发行依赖；本地代码由 Pytest、Ruff、Mypy、E2E 和策略门禁覆盖。审计返回任何第三方已知漏洞时 CI 失败，不通过忽略项静默放行。

## Artifact 与日志

只有 E2E 失败时上传 `test-results/**`，保留 3 天并排除隐藏文件。测试环境只使用 synthetic 用户、账户、金额和响应；不得扩大 artifact 路径到 `.wrangler`、环境文件、完整日志、数据库 dump 或任意工作区目录。其他 job 不上传 artifact。

策略脚本只报告规则名、路径和通用原因，不打印匹配到的密钥内容。依赖 inventory 写入 runner 临时目录且不上传。

## 依赖与镜像更新

1. 从官方发行来源确认目标版本和安全修复；更新直接约束并重新生成 `uv.lock`/`package-lock.json`。
2. 运行 npm/Python 漏洞审计、许可证 inventory 和全量受影响测试；任何强 copyleft 或无许可证元数据必须有单独书面准入，不能只改 allowlist。
3. 镜像升级同时更新可读 tag 与 registry digest，运行两个 Dockerfile `--check`、两 Compose profile config 和一次本地空库迁移。
4. 复星 SDK 仍由独立 machine-readable admission 控制；当前 `uv.lock` 必须不含 `fsopenapi`。不要借普通依赖升级绕过该门禁。

## 当前本地证据（2026-08-27）

- 完整 `npm audit`（含 CI/build devDependencies）：0 vulnerability；`pip-audit`：0 known vulnerability；
- 首次 Python 审计发现 11 条已知漏洞，最小升级到 cryptography 50.0.1、pyarrow 23.0.1、pytest 9.0.3、pytest-asyncio 1.4.0 后清零；首次完整 npm 审计发现 16 条（其中 12 high），升级 Cloudflare/Vinext/Vite/Wrangler、React RSC 和传递依赖后清零；
- gateway 145/145、Engine unit 704/704、Web unit 424/424、Chromium E2E 64/64；本轮容器所有权加固前的 Engine＋空 PostgreSQL 全量证据为 1428/1428（22 分 52 秒），加固后已跑完全部 unit，仍需在可启动新容器的 runtime 上重跑空库全量；
- 策略测试 12/12，761 个第三方依赖完成许可证 inventory，Ruff/Mypy/生成客户端/生产构建/Compose config 均通过；
- 空 PostgreSQL 验证容器结束后残留为 0。

以上是本机离线发布证据，不代表 GitHub-hosted run 已发生，也不代表部署或复星真实激活。
