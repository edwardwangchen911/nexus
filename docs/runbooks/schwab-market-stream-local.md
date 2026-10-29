# 嘉信美股行情流本地验证

本入口用于申请凭据后的显式、短时、只读验证；它不调用账户、持仓或订单接口，不下单、不自动重连，也不会被页面刷新或调度器触发。采集命令只写私密归档；只有后文的显式 `import` 命令才会把已验证归档写入现有本地 PostgreSQL 和对象存储，仅用于盘中展示。为获得嘉信要求的推送登录参数，它会调用一次 `userPreference`；该响应可能同时包含账户偏好，但实现只在内存中提取五个推送字段，不保存原始响应。原始行情帧、来源时间与接收时间分开保留。

## 嘉信 Level One

静态 App 凭据统一放在主仓库忽略的 `infra/.env.local`：

```dotenv
NEXUS_SCHWAB_APP_KEY=...
NEXUS_SCHWAB_APP_SECRET=...
NEXUS_SCHWAB_CALLBACK_URL=https://127.0.0.1
```

不要把真实值写入命令行、仓库、测试材料或聊天。先从包含本功能代码的 checkout 运行授权命令，并让 `--repo-root` 指向保存上述配置的主仓库：

```sh
uv run --package nexus-engine nexus-engine-schwab-market-stream authorize \
  --repo-root /Users/wangchen/Documents/ChatGPT/投资
```

命令会打开嘉信授权页，并显示本机 macOS 引导弹窗。先在浏览器完成登录和授权；浏览器跳到 `https://127.0.0.1` 时出现无法连接是预期现象。回到弹窗，粘贴地址栏中的完整 URL 并点击“继续”。回调仅由本地弹窗传入当前进程，不进入命令参数、shell history 或日志。命令只交换一次授权码，不自动重试；成功后把动态 token 写到主仓库 `.secrets/nexus-schwab-local/tokens.json`，目录和文件权限分别为 `0700`、`0600`。非 macOS 环境才回退为终端隐藏输入。

可显式执行一次 refresh 验证当前 refresh token；命令只调用一次 token 端点，成功后原子替换同一私密文件，不输出 token。若嘉信轮换 refresh token 则保存新值，否则保留原值；失败时不覆盖现有文件：

```sh
uv run --package nexus-engine nexus-engine-schwab-market-stream refresh \
  --repo-root /Users/wangchen/Documents/ChatGPT/投资
```

随后立即运行短时采集：

```sh
uv run --package nexus-engine nexus-engine-schwab-market-stream schwab \
  --repo-root /Users/wangchen/Documents/ChatGPT/投资 \
  --symbol AAPL \
  --output-dir /absolute/path/to/new-private-schwab-archive \
  --duration 60
```

实现固定只接受 `wss://streamer-api.schwab.com/ws`，先 LOGIN，成功后只订阅 `LEVELONE_EQUITIES`；支持服务端心跳与 change-frame 状态合并。启动采集前若 access token 已过期或将在 5 分钟内到期，会自动 refresh 一次并原子保存；刷新不重试，失败时停止采集并保留旧文件，需要重新运行 `authorize` 才能恢复已失效的 refresh token。单次最多 500 个不重复美股代码、300 秒、100 帧和 8 MiB，任何断线都只留 partial/failed，不重连。`delayed` 只保存嘉信帧明确给出的值；字段缺失保持未知。

### Agent Paper 决策报价预检

正式候选批次先运行 `nexus-engine-agent-paper-readiness` 核对静态证据。`ready_static` 只证明当日静态资料齐备，`window_evidence_status=not_evaluated` 不代表正式行情或远端授权可用。新增只读 `authentication` 说明区分本地 access token 当前有效、待刷新或文件无效；`remote_authentication=not_checked` 始终明确远端尚未验证。过期 access token 通常由正常采集入口刷新，不能据此断言 refresh token 已失效，也不因此改变宿主准入。

OAuth 非成功响应保留原固定 HTTP 错误码，并仅在响应结构合格时提取 RFC 6749 白名单 `error` 类别，随正式失败诊断写入可选 `oauth_error_code`；不保存原始响应、描述或凭据。HTTP 400 本身不能区分 `invalid_grant`、`invalid_client` 等原因，旧记录缺少分类时保持未知，不为取回原因重放刷新。工作台无周期时只读取同账户、当前显示交易日的精确自然认领；不合格或未完成回执保持结果未知，不推断为成功或确定失败。

需要提前验证嘉信报价链路时，对该批次的实际标的各提供一条现有 Instrument 绑定，并使用全新私有目录：

```sh
uv run --project engine nexus-engine-schwab-market-stream paper-probe \
  --repo-root "$PWD" \
  --binding "AAPL=$AAPL_INSTRUMENT_ID" \
  --output-dir "$PRIVATE_NEW_PROBE_DIRECTORY" \
  --duration 30
```

该入口沿用正常采集、归档读回与 Agent Paper 报价解析合同；只有每只标的在采集结束时均有完整、非延迟且足够新的双边报价，才返回 `paper_quote_status=passed_at_observation`。来源调用有界且不自动重试；失败保留私有归档并返回固定错误码。它不导入业务库、不创建周期、订单或风险权威，`execution_locked` 始终为 true。盘后通过或失败都不能代替下一次合法决策窗口的行情与风控证据；窗口前尽量靠近决策时刻运行一次，才能提前发现授权、订阅或报价质量问题。

## 归档与判定

输出目录为 0700，`frame-NNNNN.json` 与 `receipt.json` 为 0600；不保存出站认证消息，检测到入站帧反射密钥时拒绝落盘。加载器重新核对文件集合、权限、长度、hash、时间、终态、认证/订阅顺序和观察数，仅接受 `bounded_complete`。这表示本次有限窗口正常结束，不表示全天稳定性或全市场覆盖已经成立。

导入前须先将数据库升级到当前 migration head，并使用 instrument master 中的真实 UUID：

```sh
uv run --package nexus-engine nexus-engine-schwab-market-stream import \
  --repo-root /Users/wangchen/Documents/ChatGPT/投资 \
  --binding AAPL=<instrument-uuid> \
  --stream-dir /absolute/path/to/private-schwab-archive
```

`import` 仅接受完整且每条观察都明确为 `delayed=false` 的归档；会冻结原始回执和帧、核对代码与 instrument UUID，并幂等写入最新成交价及双边报价。输出中始终保留 `execution_locked=true`，不授予研究准入或任何交易权限。

首轮有凭据验收应选取一小批流动性较好的美股，核对订阅是否被接受、`delayed` 标记、quote/trade 时间与本机接收时间差、空档和断线。通过的归档可作为盘中展示数据，但不接入研究准入或执行许可。
