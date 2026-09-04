# Alpaca 公司行动：本地采集与研究观测导入

采集入口只做免费账户既有权限内的一次有界读取；另一个显式导入入口把完成归档写入本地证据存储并冻结研究输入。两者均不会修改数据源策略、写入公司行动账本、调用模型、创建订单或开启定时采集。代码与离线测试可先交付；真实账户访问未验收时不得声称数据已可用于回测。

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

采集本身不涉及数据库迁移、配置模式或依赖变更。它不向旧 `CorporateActionRecord` 填入猜测状态，不重写旧快照或解除执行锁；回滚代码不会删除已有原件。

## 显式导入本地研究观测

导入只读取上一步的私有完成归档，不访问 Alpaca，也不需要为它重新提供行情凭据。现有本地 PostgreSQL 与对象存储须已就绪，数据库先按现有迁移流程升级至 `0041_action_observations`，应用版本须与该迁移匹配。迁移只扩展冻结研究的来源绑定校验，不新增表或回填旧事实；不要在其他任务使用共享服务时自行重启或迁移。

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.alpaca_actions_import \
  --repo-root "$PWD" \
  --instrument-id <existing-local-instrument-uuid> \
  --collection-dir /absolute/private/path/new-actions-collection \
  --output /absolute/private/path/new-import-result.json
```

输出文件必须尚不存在，父目录只允许当前用户访问。入口核对本地证券当前 ticker 与原件里的显式证券字段；不会把查询参数填成缺失的上游身份。未绑定事件只计数，不当作该证券的已确认事件。空事件列表也不是“完整查询证明没有公司行动”。

导入逐项核验 manifest、receipt 和逐字节原件的来源、精确查询、游标链、哈希、长度及接收时间。截断、错误、缺页、符号链接、文件被修改或时间越界会被拒绝。上限为 100 页、累计原件 32 MiB、10000 个事件；超限不截掉后导入，也不自动重采集。

成功后保存原件、receipt、manifest 和规范化文档到既有内容寻址对象存储，逐个读回，再绑定数据库证据及新研究快照。相同归档复用证据版本和对象；再次显式导入可以生成新截止时刻的研究输入快照，不重写历史。私有结果记录精确 evidence/snapshot 引用，终端只报告固定状态；成功不表示已经运行研究模型。

研究工具 `get_security_snapshot` 读取已冻结的 `corporate_action_observation`。工具只展示最多 32 个已绑定观测，同时保留总量、未绑定数量、是否截断和查询窗口；完整版本仍有原件/页索引可回溯。金额保持 Decimal，缺日期保持 null，空币种不变成 USD；接收时刻不被称为公告或历史可用时刻。`source_completeness=unknown`、`ledger_eligible=false`、`historical_backtest_eligible=false` 和执行锁保持；这不替代真实样本验收。

对象写入先于数据库事务提交，故障可能留下可恢复的不可变孤立对象；固定失败状态不能证明所有写入均已回滚。先检查私有结果及既有存储，不自动重跑。停用新入口可回滚应用代码，保留归档；迁移只在没有该类冻结证据时允许降级至 0040，已有冻结记录会拒绝降级，禁止为降级删除证据。仅本地对象客户端显式不使用环境代理，其他 HTTP 和既有调用者路由不变。

来源：[官方接口与完整性说明](https://docs.alpaca.markets/us/reference/corporateactions-1)、[官方响应 schema](https://docs.alpaca.markets/us/reference/corporateactions-1.md)。

## 查看冻结研究资料

应用更新后，在既有研究页打开导入结果对应的证券与 `as_of`，可查看“公司行动观测资料”。页面只读数据库；刷新不会重新采集、导入或调用模型。查询范围、分页、已绑定/未绑定数量先显示，逐项原字段与证据版本可展开；窗口不是除息覆盖范围，未绑定或空列表不是无事件证明。

现有准入与研究 GET 的 `inputs.corporate_action_observations` 最多返回一个冻结观测、32条事件；其余保留于原件，截断会明确显示。展示沿用最多3位小数，微小非零显示范围；超出API既有40字符十进制合同的值标为“原件数值超出展示范围”，不冒充未知或零，不改变底层数值。未提供的字段保持未知。

从历史简报进入 `?run_id=...` 时，资料来自该运行自己的快照与研究截止，不读取当前准入或新版本。旧运行没有适用的冻结资料时不补新资料。执行权限、研究质量和历史回测/账本资格不因能查看资料而改变。停用展示只需回滚应用，不能删除已冻结证据来回滚。
