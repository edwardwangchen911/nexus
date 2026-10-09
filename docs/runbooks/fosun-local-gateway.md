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

`LOCAL_GATEWAY_SERVICE_START_UNCONFIRMED` 只表示本次观察未确认启动，不能据此再次启动。
输出中的 `observation_stage` 区分 launchd 读取、状态解析、进程退出与健康检查；
`observation_code` 和 `observed_state` 仅输出固定安全值，未知状态不透传原始内容。
launchd 的 `xpcproxy` 表示启动中，不是就绪；首次启动只在既有观察上限内等待
同一实例变为 running 并通过健康检查。对已启动中的实例再调用 `start` 只报告
`starting`，不重复注册或启动；`stop` 此时也不绕过退出确认。
先用同一标签的 `status` 核对原实例，避免创建第二实例。显式 `stop` 在核对精确归属、
确认无 PID 且状态为 inactive 后清理注册；macOS 的信号退出无需伪造数字退出码。

只有服务 `ready` 后才按现有本地产品入口执行一次正常只读刷新：

```bash
uv run --project engine nexus-fosun-local refresh
uv run --project engine nexus-fosun-local status
```

刷新失败先查持久化 sync 状态和安全错误码，不换请求重试。健康状态与最新成功快照时间分别核对；
页面仍只读已持久化数据，不从浏览器触发刷新。运行或配置变更需遵守项目 `AGENTS.md` 的授权边界。

## 报价原始时钟字段与发布顺序

报价响应保留供应方 `qtDate` / `qtTime` 为可空的
`provider_quote_date` / `provider_quote_time`。它们只作为有界严格整数归档，
不自动解释成 UTC 或 Unix 时间；`as_of` 仍表示 Gateway 接收时刻。
旧归档缺字段时继续读取，存在但格式错误的字段按原固定错误码拒绝。
字段保留不清除时间戳未核实或盘口缺失标记，也不使报价取得研究或执行许可。

旧 Engine 使用禁止额外字段的响应合同，不能消费新版 Gateway 新增的字段，
包括值为 null 的字段。获准更新服务后，先升级 Engine 消费者并核验，再升级
Gateway 生产者；不能只更新 Gateway。回退先停止新生产者并恢复旧 Gateway，
再回退 Engine。保留所有新旧原件；新原件的后续读取仍须使用兼容新字段的
消费者，不改写原件或为兼容放宽严格校验。
