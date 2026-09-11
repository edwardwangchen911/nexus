# 嘉信美股行情流本地验证

本入口用于申请凭据后的显式、短时、只读验证；它不调用账户、持仓或订单接口，不下单、不写数据库、不自动重连，也不会被页面刷新或调度器触发。为获得嘉信要求的推送登录参数，它会调用一次 `userPreference`；该响应可能同时包含账户偏好，但实现只在内存中提取五个推送字段，不保存原始响应。原始行情帧、来源时间与接收时间分开保留。

## 嘉信 Level One

静态 App 凭据统一放在主仓库忽略的 `infra/.env.local`：

```dotenv
NEXUS_SCHWAB_APP_KEY=...
NEXUS_SCHWAB_APP_SECRET=...
NEXUS_SCHWAB_CALLBACK_URL=https://127.0.0.1
```

不要把真实值写入命令行、仓库、测试材料或聊天。先从包含本功能代码的 checkout 运行授权命令，并让 `--repo-root` 指向保存上述配置的主仓库：

```sh
uv run --package nexus-engine nexus-engine-provider-market-stream authorize \
  --repo-root /Users/wangchen/Documents/ChatGPT/投资
```

命令会打开嘉信授权页。完成登录和授权后，浏览器跳到 `https://127.0.0.1` 时出现无法连接是预期现象；复制地址栏中的完整 URL，粘贴回终端的隐藏输入。授权码不会进入 shell history。命令只交换一次授权码，不自动重试；成功后把动态 token 写到主仓库 `.secrets/nexus-schwab-local/tokens.json`，目录和文件权限分别为 `0700`、`0600`。

随后立即运行短时采集：

```sh
uv run --package nexus-engine nexus-engine-provider-market-stream schwab \
  --repo-root /Users/wangchen/Documents/ChatGPT/投资 \
  --symbol AAPL \
  --output-dir /absolute/path/to/new-private-schwab-archive \
  --duration 60
```

实现固定只接受 `wss://streamer-api.schwab.com/ws`，先 LOGIN，成功后只订阅 `LEVELONE_EQUITIES`；支持服务端心跳与 change-frame 状态合并。单次最多 500 个不重复美股代码、300 秒、100 帧和 8 MiB，任何断线都只留 partial/failed，不重连。`delayed` 只保存嘉信帧明确给出的值；字段缺失保持未知。

当前切片负责首次浏览器授权和授权码换 token，但不自动刷新过期 token。若出现 `SCHWAB_OAUTH_ACCESS_TOKEN_EXPIRED`，需要重新运行 `authorize`；自动 refresh 留待真实首轮验收确认 token 响应后再实现。首轮验证还必须确认实际 user-preference 字段、账户行情权限，以及目标股票帧中的 `delayed=false`；仅 LOGIN/SUBS 成功不代表实时权限。

## 归档与判定

输出目录为 0700，`frame-NNNNN.json` 与 `receipt.json` 为 0600；不保存出站认证消息，检测到入站帧反射密钥时拒绝落盘。加载器重新核对文件集合、权限、长度、hash、时间、终态、认证/订阅顺序和观察数，仅接受 `bounded_complete`。这表示本次有限窗口正常结束，不表示全天稳定性、全市场覆盖或实时权限已经成立。

首轮有凭据验收应选取一小批流动性较好的美股，核对订阅是否被接受、`delayed` 标记、quote/trade 时间与本机接收时间差、空档和断线。在证据完成前，嘉信只是候选行情源，不接入研究准入或执行许可。
