# Alpaca 日线：本地归档与当前研究导入

归档命令只读本地主表并有界读取行情，将原件保存到新的私有目录，不写业务数据库。独立的导入命令将已验证的拆股调整日线保存为当前研究补充资料，不重新请求行情。两者都不创建研究或订单、不切换行情源。归档、导入、正常研究及页面真实验收分别判断，不能互相替代；消费者合同见团队计划 §9.23。

## 归档

使用项目虚拟环境，在仓库根目录执行。既有本地配置需提供 `DATABASE_URL`、`ALPACA_API_KEY_ID`、`ALPACA_API_SECRET_KEY`；数据库须为本机 PostgreSQL/asyncpg，身份查询事务强制只读。不把凭据或真实账户资料填入命令行、聊天或仓库。

以下变量分别代表既有本地标的 UUID、起止交易日、身份日期和新输出目录，须先按实际案例设置；示例不自动选择标的或日期。

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.alpaca_prices_local \
  --repo-root "$PWD" \
  --instrument-id "$NEXUS_PRICE_INSTRUMENT_ID" \
  --start "$NEXUS_PRICE_START" --end "$NEXUS_PRICE_END" \
  --adjustment all --identity-asof "$NEXUS_PRICE_IDENTITY_DATE" \
  --output-dir "$NEXUS_PRICE_NEW_DIRECTORY" --max-pages 10
```

`--adjustment` 允许 `raw`、`split` 或 `all`；`all` 仅作为供应商请求事实保存，后续仍须经独立价格权威准入，不因归档成功自动获得诊断或执行资格。对照采集分别使用全新目录；父目录须已存在，入口独占创建权限为0700的子目录。`--identity-asof` 可省略，默认等于请求结束日；显式提供时，本地 ticker 解析和远端 asof 使用同一天，不能晚于采集开始时的 UTC 日期。身份日期与价格区间独立：当前身份查询历史数据不证明当年已经知道该身份或这些调整价格，不修改本地 alias。

组合持仓仍必须使用有效的本地 ticker alias。产品固定的 SPY 诊断基准是唯一例外：仅当 instrument ID 等于标准 `stable_instrument_id("SPY")`，且本地主表精确为 ARCX/USD 时，采集入口可解析为 SPY；这不创建 alias，也不赋予历史身份或交易资格。

固定请求为单证券 bars、SIP、USD、1Day；不请求交易接口。窗口两端必须是安装版 XNYS 日历支持的交易日，相差不超过366天；结束交易日收盘加30分钟缓冲必须已经过去。缓冲不证明供应商最终完整。身份查询有15秒边界；随后采集、成功归档校验与发布共用120秒截止，每请求超时30秒、最多10页、每页8MiB，后续页间隔0.35秒。首错停止、无自动重试，不把观察超时当作未发出请求。

## 结果与恢复

- `page-*.json` 保存完整成功 HTTP 响应的原始字节，先落盘再做 JSON/身份检查；错误身份或坏 JSON 原件仍保留。非2xx、超大或未完整接收的响应不保存响应体。
- 每页 receipt 记录精确 query、身份解析日期、实际收件时刻、长度与哈希，不保存认证头。feed/currency/adjustment 是请求事实，不冒称响应回显认证。
- 成功的 `manifest.json` 经过全部原件与分页链读回校验，命令返回 `collected` 和 `readback_verified=true`。`manifest.pending.json` 不是成功标记，公开 reader 不接受它；pending 与 final 并存也拒绝。
- `pagination_exhausted=true` 只说明翻页终止。交易日完整性仍为 unknown，研究与历史回测资格仍为 false，执行保持锁定；空页不证明没有风险或完整市场数据。

失败或中断可能留下原件、pending 或失败 manifest；保留现场，先看固定安全错误及私有 receipt，不覆盖目录、不删除原件来重试。归档 reader 为 `nexus.research.alpaca_prices_archive.load_price_collection`，会重新核对私有文件、哈希、身份、日期和分页，不能靠修改状态字段强行导入。

仅归档命令无依赖或迁移变更。停用该命令即可停止采集，不删除已有归档；其成功不代表后续导入或研究已完成。

## 导入当前趋势资料

先由主线集成人按现有迁移流程应用 `0043_current_price_trends`。导入入口检查当前 schema，不自动执行迁移。使用既有 `DATABASE_URL` 及完整对象存储配置；数据库与对象存储端点必须在本机，导入不需要 Alpaca 行情密钥，也不会请求行情、模型或券商写接口。

下面的目录必须是已经成功归档的私有目录；结果文件必须是尚不存在的新文件，父目录预先存在。真实标的 UUID、证据引用及结果仅保留在授权的本地环境和私有文件中，不粘贴到聊天或提交进仓库。

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.alpaca_prices_import \
  --repo-root "$PWD" \
  --instrument-id "$NEXUS_PRICE_INSTRUMENT_ID" \
  --collection-dir "$NEXUS_PRICE_NEW_DIRECTORY" \
  --output "$NEXUS_PRICE_IMPORT_RESULT"
```

导入重新校验完整原件与分页链、本地主表身份、已发布 XNYS 日历和当前趋势用途政策，只有 SIP/split/USD/1Day 可进入本用途。原件与 canonical 保存到既有对象存储并读回，研究证据写入现有表；不写旧价格 authority、行情 bars 或回测发布记录。canonical 最大2MiB，超限保留归档而不准入。

当前指标复用既有 Decimal 算法，按最后实际观察日向前的连续交易日段计算；缺日不补零、不跨缺口连接收益。实际观察日、查询结束日、收到时间和导入可用时间分别保留。SMA20/200、RSI14、20日年化价格波动分别需要20/200、15、21个连续收盘；不足显示未知。此资料不证明历史 PIT、总收益或可成交价格，不授予估值、回测或订单资格，执行始终锁定。

成功时控制台仅输出安全状态 `stored_context`、`model_called=false`、`research_created=false`；私有结果文件保存精确证据 ID、hash、可用时间和用途限制。同归档再次导入保留首次证据及其时间，不覆盖旧资料；不同接收批次的相同原件可复用内容对象，各自 receipt 仍独立保留。

## 导入诊断价格权威

仅 `adjustment=all`、SIP、USD、`1Day` 且完整读回的单页归档可进入这个入口。它不再请求 Alpaca，不创建研究或订单；原件、规范化 Parquet、manifest、bar records 和 bar-set authority 在本地对象存储与 PostgreSQL 中绑定。多页归档不会只取最后一页，而是明确拒绝。

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.alpaca_adjusted_authority_import \
  --repo-root "$PWD" \
  --instrument-id "$NEXUS_PRICE_INSTRUMENT_ID" \
  --collection-dir "$NEXUS_PRICE_NEW_DIRECTORY" \
  --output "$NEXUS_PRICE_AUTHORITY_RESULT"
```

入口要求当前 schema 精确等于 `0045_adjusted_diagnostic_prices`，不自动迁移。控制台仅输出 `stored_authority`、`provider_called=false`、`model_called=false` 和 `execution_locked=true`；精确权威 ID 仅写入指定的私有结果文件。导入后的价格可用时间不能回填旧组合快照；必须在全部持仓及 SPY 都导入后新建一份只读组合快照，再运行确定性诊断。

参数或配置预检失败返回 `rejected`。开始导入后发生错误、中断，或数据库已提交但结果文件写入失败，返回 `unknown`；这不证明数据库没有写入，也不承诺所有对象已回滚。先核对本地数据库及精确归档对应证据，不自动重试、不覆盖结果文件。数据库连接超时10秒、单条命令15秒，导入事务有120秒边界；对象 I/O 沿用既有有界存储实现，退出时另有数据库连接回收步骤。

导入后，只有随后正常创建的新研究快照会选取截止前可用的资料；旧研究不自动补入，新资料也不能单独建立研究基底。还须验证实际研究工具、受保护 API/BFF 与页面，再按正常入口创建新研究；输入可见不代表研究结论合格或正式方案可执行。

回滚先停止新导入并保留既有读取能力，不删除原件、证据或已发布用途政策。有已持久化趋势资料时，迁移拒绝降级；无资料时可降回0042，但仍保留不可变政策，重新升级只允许复用身份和内容完全一致的记录。不存在用回滚重标旧资料或放宽交易门禁的步骤。

SDK 选择沿用团队计划中的核验：[官方股票客户端](https://github.com/alpacahq/alpaca-py/blob/master/alpaca/data/historical/stock.py)可提供 bars，但当前公开客户端的 raw 返回值已解码并合并分页，不能直接作为逐页字节原件。为保留原件、精度和既有无重试边界，本入口复用项目已有窄 HTTP 适配器，不另造通用 SDK；此结论不否定 SDK 的其他用途。


### 可选研究流动性观察

对同一已完整归档的 split 日线，可在原导入命令追加 `--liquidity-observation`。默认导入仍保持原投影和原 hash；可选导入沿同 collection 身份新增一个不可变修订，再通过正常 prepare-research 生成新冻结快照。重复同种导入复用对应修订，不回填旧研究。原件不重采，复用原件对象，仅新增投影对象/快照/证据；无迁移或额外权限。

观察值为固定最近 20 个预期交易日的 `mean(close × volume)`，单位 USD/交易日，保留 Decimal 精度；它是收盘价乘成交量的金额代理，不是逐笔成交额、正式 ADV20 或退出天数。样本截至归档查询末日，缺日不向前补齐，少于 20 日或窗口缺日均不给数值；完整 20 日成交量全零是已知零。它仅进入新研究输入和页面，不改变诊断/价格准入、回测资格或执行锁。

生产者沿已验证 SIP/split/USD 当前趋势合同；Alpaca 官方说明 split 同时调整价与量，但价量乘积仍受供应商价格/成交量缩放和舍入口径限制。金额代理不能宣称逐笔成交额精确性。回滚代码须保留新版投影 reader 的兼容读取；停止可选导入不删除新修订，旧快照继续按原引用读取，不重标历史。
