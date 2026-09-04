# Alpaca 日线：本地原件归档

本入口只读本地主表并有界读取行情，将原件保存到新的私有目录；不写业务数据库、不创建研究或订单、不切换行情源。归档成功不等于资料已导入或获准用于研究、回测、估值或交易。当前趋势的后续消费者见团队计划 §9.23，以实际集成交付为准。

## 运行

使用项目虚拟环境，在仓库根目录执行。既有本地配置需提供 `DATABASE_URL`、`ALPACA_API_KEY_ID`、`ALPACA_API_SECRET_KEY`；数据库须为本机 PostgreSQL/asyncpg，身份查询事务强制只读。不把凭据或真实账户资料填入命令行、聊天或仓库。

以下变量分别代表既有本地标的 UUID、起止交易日、身份日期和新输出目录，须先按实际案例设置；示例不自动选择标的或日期。

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.alpaca_prices_local \
  --repo-root "$PWD" \
  --instrument-id "$NEXUS_PRICE_INSTRUMENT_ID" \
  --start "$NEXUS_PRICE_START" --end "$NEXUS_PRICE_END" \
  --adjustment split --identity-asof "$NEXUS_PRICE_IDENTITY_DATE" \
  --output-dir "$NEXUS_PRICE_NEW_DIRECTORY" --max-pages 10
```

`--adjustment` 仅允许 `raw` 或 `split`。对照采集分别使用全新目录；父目录须已存在，入口独占创建权限为0700的子目录。`--identity-asof` 可省略，默认等于请求结束日；显式提供时，本地 ticker 解析和远端 asof 使用同一天，不能晚于采集开始时的 UTC 日期。身份日期与价格区间独立：当前身份查询历史数据不证明当年已经知道该身份或这些调整价格，不修改本地 alias。

固定请求为单证券 bars、SIP、USD、1Day；不请求交易接口。窗口两端必须是安装版 XNYS 日历支持的交易日，相差不超过366天；结束交易日收盘加30分钟缓冲必须已经过去。缓冲不证明供应商最终完整。身份查询有15秒边界；随后采集、成功归档校验与发布共用120秒截止，每请求超时30秒、最多10页、每页8MiB，后续页间隔0.35秒。首错停止、无自动重试，不把观察超时当作未发出请求。

## 结果与恢复

- `page-*.json` 保存完整成功 HTTP 响应的原始字节，先落盘再做 JSON/身份检查；错误身份或坏 JSON 原件仍保留。非2xx、超大或未完整接收的响应不保存响应体。
- 每页 receipt 记录精确 query、身份解析日期、实际收件时刻、长度与哈希，不保存认证头。feed/currency/adjustment 是请求事实，不冒称响应回显认证。
- 成功的 `manifest.json` 经过全部原件与分页链读回校验，命令返回 `collected` 和 `readback_verified=true`。`manifest.pending.json` 不是成功标记，公开 reader 不接受它；pending 与 final 并存也拒绝。
- `pagination_exhausted=true` 只说明翻页终止。交易日完整性仍为 unknown，研究与历史回测资格仍为 false，执行保持锁定；空页不证明没有风险或完整市场数据。

失败或中断可能留下原件、pending 或失败 manifest；保留现场，先看固定安全错误及私有 receipt，不覆盖目录、不删除原件来重试。归档 reader 为 `nexus.research.alpaca_prices_archive.load_price_collection`，会重新核对私有文件、哈希、身份、日期和分页，不能靠修改状态字段强行导入。

本片无依赖或迁移变更。停用命令即可停止采集；回滚应用不需要回滚业务数据库，也不删除已有归档。导入、正常研究输入和页面验收是后续独立交付，不能用本命令成功替代。

SDK 选择沿用团队计划中的核验：[官方股票客户端](https://github.com/alpacahq/alpaca-py/blob/master/alpaca/data/historical/stock.py)可提供 bars，但当前公开客户端的 raw 返回值已解码并合并分页，不能直接作为逐页字节原件。为保留原件、精度和既有无重试边界，本入口复用项目已有窄 HTTP 适配器，不另造通用 SDK；此结论不否定 SDK 的其他用途。
