# 显式有界新闻流

```sh
python -m nexus.ops.news_stream --repo-root /path/to/repo --symbol AAPL --output-dir /path/to/new-private-directory --duration 60
```

默认 60 秒，最多 300 秒、100 入站消息、累计 8 MiB、单帧 2 MiB；单标的、单连接、不重连、不调度。复用本地 Alpaca 凭据，显式声明协议库 `websockets==17.1`，由正常 `uv sync --package nexus-engine --group dev` 安装；不自写 WebSocket 协议。锁文件复用已有版本，无其他依赖升级。库安装约 1.2 MiB，无付费或新增访问范围；回滚本片及依赖声明后重新同步环境即可。固定 `wss://stream.data.alpaca.markets/v1beta1/news`，拒绝重定向；只发送新闻订阅，不包含交易请求。

官方协议：[新闻流](https://docs.alpaca.markets/us/docs/streaming-real-time-news)、[流认证与错误码](https://docs.alpaca.markets/us/docs/streaming-market-data)。认证通过后单独订阅，收到精确 symbol 的 subscription 才记确认。无文章但订阅确认不等于无权限。409 insufficient subscription 与 406 connection limit 是不同错误；不升级套餐，不自动重试。连接关闭后不再“在线”；bounded_complete 仅指本次有限接收窗口正常终止，不证明持续覆盖或全部新闻已接收。

新目录 0700，入站 `frame-NNNNN.json` 和 `receipt.json` 0600 独占写入。收据 `nexus_news_stream_receipt_v1` 与 REST 合同独立，记录源、transport、symbol、开始/结束时间、请求时长/帧上限、单调时钟耗时、认证/订阅确认、终止原因、文章数及逐帧文件/接收时间/hash/长度；导入只接收满足这些内部一致性约束的 `bounded_complete`。不存出站认证或凭据；密钥反射帧不落盘。成功保存的异常帧保留以便离线诊断；断线保留 partial/failed，不能伪造成无新闻。文章创建/更新时间与帧接收时间分开，正文仅原件保留，不执行 HTML。0700 私有目录与导入前结构/一致性校验是本片威胁边界；导入后的原件哈希提供不可变性校验，但没有外部信任根时不能证明一个整体自洽的恶意本地原件改写。

`load_stream_archive(Path)` 和 `parse_stream_archive(receipt, frames)` 只读校验原件并返回文章、订阅事实与终态，不读写数据库、不回填历史研究。损坏或无效文章归档拒绝解析。流文章允许同 ID 不同修订作为不同接收事件保存；下游按修订去重时不得改原件。

复星同样存在真实行情 WebSocket：本地既有 SDK 准入文档记录 `wss://openapi-push.fosunxcz.com/msg/v1/push`；官方 [行情推送合同](https://openapi-docs.fosunxcz.com/specs/public/market/push/openapi.yaml) 使用 X-Api-Key/X-Session、act=sub、subRep 和每15秒 act=hb。其 qt/mqt/min/tk/ob 是行情 topic，不能据此声称已提供新闻。现有 pull-only 是本地接线范围，不是供应商没有推送能力；本片不改复星网关。
