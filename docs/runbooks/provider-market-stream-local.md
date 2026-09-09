# 嘉信美股行情流本地验证

本入口用于申请凭据后的显式、短时、只读验证；它不读取账户、不下单、不写数据库、不自动重连，也不会被页面刷新或调度器触发。原始帧、来源时间与接收时间分开保留。

## 嘉信 Level One

先在本机受保护的进程环境中提供 `NEXUS_SCHWAB_STREAM_ACCESS_TOKEN`、`NEXUS_SCHWAB_STREAM_CUSTOMER_ID`、`NEXUS_SCHWAB_STREAM_CORRELATION_ID`、`NEXUS_SCHWAB_STREAM_CHANNEL`、`NEXUS_SCHWAB_STREAM_FUNCTION_ID` 和 `NEXUS_SCHWAB_STREAM_URL`。这些值应来自已完成的 OAuth 流程及嘉信 user-preference 响应；不要把凭据写入命令行、仓库、测试材料或聊天。

```sh
uv run --package nexus-engine nexus-engine-provider-market-stream schwab \
  --symbol AAPL \
  --output-dir /absolute/path/to/new-private-schwab-archive \
  --duration 60
```

实现固定只接受 `wss://streamer-api.schwab.com/ws`，先 LOGIN，成功后只订阅 `LEVELONE_EQUITIES`；支持服务端心跳与 change-frame 状态合并。单次最多 500 个不重复美股代码、300 秒、100 帧和 8 MiB，任何断线都只留 partial/failed，不重连。`delayed` 只保存嘉信帧明确给出的值；字段缺失保持未知。

当前入口不负责浏览器授权、授权码换 token 或 token 刷新。申请完成后的首轮验证还必须确认实际 user-preference 字段、账户行情权限，以及目标股票帧中的 `delayed=false`；仅 LOGIN/SUBS 成功不代表实时权限。

## 归档与判定

输出目录为 0700，`frame-NNNNN.json` 与 `receipt.json` 为 0600；不保存出站认证消息，检测到入站帧反射密钥时拒绝落盘。加载器重新核对文件集合、权限、长度、hash、时间、终态、认证/订阅顺序和观察数，仅接受 `bounded_complete`。这表示本次有限窗口正常结束，不表示全天稳定性、全市场覆盖或实时权限已经成立。

首轮有凭据验收应选取一小批流动性较好的美股，核对订阅是否被接受、`delayed` 标记、quote/trade 时间与本机接收时间差、空档和断线。在证据完成前，嘉信只是候选行情源，不接入研究准入或执行许可。
