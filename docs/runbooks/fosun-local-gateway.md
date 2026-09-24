# 本机复星只读 Gateway

本流程只管理宿主机 `local_read_only` Gateway。它复用已有私有 TLS、HMAC、Provider 材料和
`.secrets/nexus-fosun-local/replay.sqlite3`，只绑定 `127.0.0.1:8081/8082`，不启用
Compose 的 `provider`/`broker` profile，不开放券商写接口，也不解除 `execution_locked=true`。

在主仓库 `main` 使用同一提交的项目虚拟环境。先检查本机固定配置；`check` 不启动服务、
不访问券商，也不证明网络或快照新鲜：
Gateway 源码、Provider manifest、项目依赖定义或锁文件有未提交改动时，入口拒绝把它们标记为
当前 release；其他工作文档的未提交改动不影响该检查。

```bash
uv sync --frozen --package nexus-fosun-gateway
uv run --package nexus-fosun-gateway nexus-fosun-gateway-local check --repo-root "$PWD"
uv run --package nexus-fosun-gateway nexus-fosun-gateway-service status --repo-root "$PWD"
```

显式启动与停止使用同一个 launchd 标签 `com.nexus.fosun-local-gateway`：

```bash
uv run --package nexus-fosun-gateway nexus-fosun-gateway-service start --repo-root "$PWD"
uv run --package nexus-fosun-gateway nexus-fosun-gateway-service status --repo-root "$PWD"
uv run --package nexus-fosun-gateway nexus-fosun-gateway-service stop --repo-root "$PWD"
```

`start` 先运行零网络配置检查，再注册只含入口路径和提交身份的私有 plist；不会自动刷新
账户。`status=ready` 要求注册实例归属与当前提交一致、replay 就绪标记绑定该 PID，且 broker
与 market 两条 mTLS `/health/ready` 均为 200。`degraded`、`inactive` 或其他失败码不代表
账户数据可用。运行满 12 小时的本地只读授权到期后会安全退出，launchd 不自动重启；先检查
状态和失败原因，再显式 `start` 建立新实例。原 replay sqlite 不删除、不替换。

只有服务 `ready` 后才按现有本地产品入口执行一次正常只读刷新：

```bash
uv run --project engine nexus-fosun-local refresh
uv run --project engine nexus-fosun-local status
```

刷新失败先查持久化 sync 状态和安全错误码，不换请求重试。健康状态与最新成功快照时间分别核对；
页面仍只读已持久化数据，不从浏览器触发刷新。运行或配置变更需遵守项目 `AGENTS.md` 的授权边界。
