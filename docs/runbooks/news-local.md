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

## 保存到本地平台并查看

采集成功后，使用同一标的的已有证券 UUID 显式导入。`--output` 必须是尚不存在的私有回执文件；程序先预约并写入 started，再执行事务，成功后在同一文件记录 snapshot_id。若报告 `NEWS_IMPORT_CHECK_PERSISTED_STATE`，先按归档收据哈希核查 `data_snapshots`，禁止因回执失败自动重放。

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.news_import \
  --repo-root /path/to/configured/repo \
  --instrument-id EXISTING_INSTRUMENT_UUID \
  --collection-dir /path/to/collected-directory \
  --output /path/to/new-private-result.json
```

导入仅允许本地数据库与对象存储；核对归档权限、精确字节、收据、窗口和证券别名后，保存两份原件与一个不可变消息快照。同一收据幂等；新采集追加快照，旧研究与方案不变。复用 DataArtifact/DataSnapshot，无新依赖、迁移或权限配置。对象已写入但事务失败时可能留下未引用原件，保留它们供核查。

市场页选定标的后，在“最新消息”查看本次最新一页。`GET /internal/v1/instruments/{instrument_id}/news` 与相应 BFF 路由只读持久化原件，并重新验证哈希、字节数和时间。未采集、返回空页、原件异常分开表达；不将损坏归档降为空消息。页面列出文章发布时间/修订时间、本机采集时间、查询窗口、来源和原文链接；不加载远程正文或图片。按钮“读取已保存消息”只重新读取本地数据，不采集供应商，不调用模型。切换标的不会显示上一标的的迟到响应。

真实查询可访问只证明现有凭据在这次请求被接受，不推导账户实时权益、零延迟或全面覆盖。新消息不会进入既有冻结研究，不能直接改变研究等级、策略方向或交易权限。

回滚：切回此前应用版本即可停止使用新增入口；无需迁移回滚或删除任何原件/快照。不要删除持久化记录来模拟未采集，也不要更新历史研究引用。

## 显式冻结到新研究

市场展示与研究证据分开。可将已保存的**指定消息快照**登记为补充研究证据，然后在新研究准备入口显式传入引用：

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.news_import \
  --repo-root /path/to/configured/repo --instrument-id EXISTING_INSTRUMENT_UUID \
  --news-snapshot-id SAVED_NEWS_SNAPSHOT_UUID --output /path/to/new-private-evidence-result.json
# 在已有本地配置的项目根目录运行：
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.openai_local prepare-research \
  --instrument-id EXISTING_INSTRUMENT_UUID --news-evidence-id REGISTERED_EVIDENCE_UUID
```

登记复用已持久化原件，不重新采集或调用模型；同一证券、消息页与选择规则幂等。按 updated_at/id 降序选取该页最新 5 条标题与摘要；保留收集总数、未收集页标记和来源未独立核实边界。规范证据封存精确原件字节、收据、哈希与独立可用时间；模型与 API 仅得到所选条目，不携带原始全文。不得从缺失摘要推测正文。

不传 `--news-evidence-id` 时延续原研究路径；显式无效引用拒绝，不自动回退最新消息。消息不能单独创建研究基础，也不会回填旧研究或解除执行锁。研究页面显示本次冻结消息，后续采集和 WebSocket 推送不会替换它。

此补充入口需要迁移 `0044_news_research`：只扩展现有冻结证据的证券归属校验，无新表或历史数据改写。应用可以回滚停止产生新消息研究；已有新闻研究引用时拒绝降级该迁移，避免破坏历史证据的可读取性。没有新闻研究引用时可按 Alembic 正常降级。实时接收的独立入口见 [news-stream-local.md](news-stream-local.md)，其归档不能冒充上述 REST 查询收据。
