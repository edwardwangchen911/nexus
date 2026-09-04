# Alpaca 公司行动：本地手动原件采集

本入口补充既有公司行动数据链，只做免费账户既有权限内的一次有界读取。不会修改数据源策略、写入公司行动账本、创建研究/订单或开启定时采集。代码与离线测试可先交付；真实账户访问未验收时不得声称数据已可用于回测。

## 运行

通过项目正常本地配置入口提供 `ALPACA_API_KEY_ID` 和 `ALPACA_API_SECRET_KEY`，不要把值放入命令行、聊天或仓库。入口不要求切换现有复星行情来源，也不使用 Alpaca 交易接口。

使用项目虚拟环境，在仓库根目录执行（示例证券仅表示查询对象，不代表持仓或推荐）：

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.alpaca_actions_local \
  --repo-root "$PWD" \
  --symbol AAPL --start 2024-06-01 --end 2024-06-30 \
  --output-dir /absolute/private/path/new-actions-collection \
  --max-pages 10
```

输出目录必须尚不存在；父目录须已准备好。入口创建私有目录和只可由当前用户读写的文件，不覆盖既有采集。窗口两端包含，最长相差 366 天；每页至多 1000 个事件，默认最多 10 页。页数是运行边界，不是新的逐次授权要求。失败不自动重试；再次执行前先看已保存 receipt 和失败状态，不把超时当作请求从未发出。

## 输出与解释

- `page-*.json`：HTTP 返回的逐字节原件，先保存再解析；解析失败的响应也保留。
- 每页 receipt：精确查询条件、页序、原件 SHA-256/长度及本机实际接收时间。不会保存认证头。
- `manifest.json`：此次采集范围、已保存页及终态。`collected` / `pagination_exhausted=true` 仅指该次查询翻页结束，**不等于历史公司行动完整**；超页数、循环游标和错误不得报告完成。原件与 receipt 需一起保存，不能只保留事件摘要。

手动中断、进程被终止或写盘故障可能留下原件而没有最终 manifest；这类目录一律视为未完成。入口不会自动删除、补写成功状态或复用该目录重放请求。对外 HTTP 沿用既有环境路由，不修改或绕过用户的代理配置。

固定请求为 `GET https://data.alpaca.markets/v1/corporate-actions`，显式使用单证券、`region=us`、`data_quality=all`、`sort=asc`，不限制事件类型。`start/end` 按当前接口日期语义查询，文档以 `process_date` 描述排序；不是除息日窗口，也没有 `date_type=ex_date` 参数。

当前官方事件按类型分桶，未提供旧适配器要求的 `status/revision`。金额按 Decimal 解码并保留原件；空币种、缺失日期及未知字段不填默认值。`data_quality=complete` 也可能返回已处理但字段不全的事件。接收时间不是公告时间，内容哈希不是上游修订号；分页结束不能证明历史时点可见或没有遗漏事件。

下一步在真实样本可用后核实证券身份、事件字段和时间含义，再接研究持久化及用途合同。此入口不向旧 `CorporateActionRecord` 填入猜测状态，不重写旧快照或解除执行锁。无迁移、配置模式或依赖变更；回滚代码不会删除已有原件。

来源：[官方接口与完整性说明](https://docs.alpaca.markets/us/reference/corporateactions-1)、[官方响应 schema](https://docs.alpaca.markets/us/reference/corporateactions-1.md)。
