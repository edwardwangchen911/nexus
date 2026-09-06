# 本地新闻只读采集

正常入口（使用项目虚拟环境，并让 `--repo-root` 指向已有本地配置的主目录）：

```sh
python -m nexus.ops.news_local collect --repo-root /path/to/repo --symbol AAPL --output-dir /path/to/new-private-directory
```

配置复用 `load_local_environment` 的 `ALPACA_API_KEY_ID` 与 `ALPACA_API_SECRET_KEY`；不打印凭据。目录必须不存在，创建权限 0700；`raw.json` 与 `receipt.json` 独占创建为 0600。失败只输出固定错误码，不自动重试、不覆盖已有归档。失败可能留下本次空目录或未完成文件；缺少完整收据不可导入。

固定 HTTPS origin `data.alpaca.markets`，路径 `/v1beta1/news`，一个 symbol、最多 50 条、按更新时间降序，仅一页，不跟随游标。请求 `include_content=false` 和 `exclude_contentless=false`。默认 UTC 窗口从当前时间前 7 天至前 15 分钟；可显式传入带时区的 `--start` / `--end`，跨度不得超过 7 天，end 不得晚于当前时间前 15 分钟。这是保守延迟查询，不是实时订阅，不宣称免费账户具有实时权限。403、429 或其他失败停止。

官方 https://docs.alpaca.markets/us/reference/news-3.md 明确 sort 采用 updated date，start/end 为包含边界的 interval，未明确 interval 字段。此入口额外按 `updated_at` 核验范围，超界整体拒绝；它是本地接纳规则，不冒充供应商过滤承诺。`created_at` 是文章创建时间，`updated_at` 是修订时间，`received_at` 是本地收到时间，三者分开保存。

收据 schema 为 `nexus_news_receipt_v1`，字段为 source、symbol、start、end、received_at、raw_sha256、raw_byte_length、has_more。`has_more=true` 表示单页覆盖不完整，不复制游标至收据。原件保留供应商响应；密钥反射在落盘前拒绝。正文 content 和 images 不进入解析投影；headline/summary 是普通字符串，消费者必须作为文本渲染，禁止 HTML 执行。非 HTTP(S) 或含凭据的链接降为 null。

本入口只采集原件，不调用模型、不生成研究结论、不写券商、不建立调度。历史研究不得因新消息被重标。

HTTP 成功且通过凭据反射检查后，身份、schema 或时间窗口核验失败会保存 `rejected-raw.json` 和 `rejected-receipt.json`（0700/0600），后者仅包含固定错误码、原件哈希/长度及接收时间。这组文件不能被正常导入器接纳，可用于离线诊断，不需要重采集。无法完成反射检查的非 JSON 响应和任何密钥反射仍不落盘。HTTP 策略错误保留白名单固定码；现有通用传输未暴露具体 401/403 状态，`NEWS_HTTP_STATUS_REJECTED` 不应推断为某个具体状态。
