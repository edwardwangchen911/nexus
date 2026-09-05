# baseline 首次回测

入口复用现有点时数据加载、回测引擎和事务写入器。回测只使用数据库内已有的完整源证据集合；不采集行情、不调用模型、不创建历史股票池、行业或公司行动证据，不连接交易接口。可以单独准备本地库的规范日历、直接运行已有数据包，或从完整源集合装配新数据包再运行。

## 只读预检未来内部股票池

沿用已配置的本地 `DATABASE_URL`，选择尚未开盘的有效 XNYS 交易日：

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.backtest_once \
  --preflight-universe --effective-session '<未来交易日 YYYY-MM-DD>'
```

该模式与其他模式互斥。固定定义随安装包保存在 `nexus/backtest/data/us_liquid_v1.csv`；49 个成员保持原定义，CSV 旧日期不作为发布时间。程序在同一 `REPEATABLE READ, READ ONLY` 事务中核对数据库时钟、已有 Instrument 与唯一有效 ticker alias。等于开盘时刻也拒绝，身份缺失、晚到、错配或歧义分别返回固定缺口分类。

输出仅包含 `status`、`member_count`、`gap_categories`。`BACKTEST_UNIVERSE_PREFLIGHT_READY` 表示本次身份和日期前置通过；`BACKTEST_UNIVERSE_PREFLIGHT_BLOCKED` 表示有明确缺口，两者都没有发布股票池、创建成员、生成回测或启动调度。预检结果不保留未来发布资格，实际发布必须重新核验。当前版本尚未实现发布动作，也不补成过去的 PIT 证据。

## 单独准备规范日历

使用当前 Python 环境安装的 `exchange_calendars` 生成 XNYS 日历。此模式只接收以下三个参数，与下方两种模式的其他参数互斥：

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.backtest_once \
  --prepare-calendar --start '2021-01-08' --end '2021-01-11'
```

开始和结束必须都是库支持范围内的实际交易日，且有序；不自动截断或将节假日移动到临近交易日。日期是格式示例；实际日历须覆盖拟用历史及策略预热区间，不仅是回测起止两日。所有 session、开闭市及收盘后 30 分钟决策时点均取当前安装库，节假日、提前收市及夏令时由该版本日历确定。

成功只返回 `BACKTEST_CALENDAR_PREPARED`、`calendar_authority_id`、`content_hash`、实际包版本组成的 `version`、`start/end`、`backtest_executed=false` 与 `execution_locked=true`。将此 ID 填入下方 `calendar_authority_id`；本命令不创建数据包、回测结果或订单，不调用外部接口或模型。

根和全部 sessions 在同一事务写入，创建/发布时间取实际物化时刻，不倒填到历史日期。同版本、同区间精确核验并复用原 ID；并发不会重复创建。发现已存在记录字段、hash、时点或 session 集不一致时拒绝，不修补或重标旧根。现有 Fosun display 日历保持原样，不能因同属 XNYS 而当作本日历复用。

这里的完整性只表示当前安装库对应区间的规范输出，不宣称交易所原始官方归档、历史版本修订或源市场数据已验收。缺失 universe/sector/actions、历史价格或来源资格时，下方组包仍拒绝；日历成功不代表 T1 历史输入已齐备。

## 从现有完整源集合准备后运行

准备模式与下方已有数据包模式互斥，不接受额外的旧模式参数。请求文件须为不超过 1 MiB 的普通文件（不接受符号链接），内容为闭合 JSON：

```json
{
  "strategy_version": "baseline_v1",
  "calendar_authority_id": "<已有日历 ID>",
  "universe_collection_id": "<完整历史股票池集合 ID>",
  "sector_collection_id": "<完整行业集合 ID>",
  "corporate_action_collection_id": "<完整公司行动集合 ID>",
  "spy_instrument_alias_id": "<原 SPY ticker 别名 ID>",
  "equal_weight_universe_snapshot_id": "<上述历史股票池集合中的基准快照 ID>",
  "source_policy_id": "<已发布来源政策 ID>",
  "cost_policy_id": "<历史回测成本政策 ID>",
  "start": "2021-01-08",
  "end": "2021-01-11",
  "initial_nav": "10000",
  "idempotency_key": "baseline-local-example"
}
```

示例日期及净值只是参数格式，不代表已有可用历史。填写实际已存在的源集合及相应区间；金额使用十进制字符串。key 为 1–100 个字母、数字、下划线、点、冒号或连字符。

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.backtest_once \
  --prepare-request-file '<本地请求 JSON 文件>'
```

程序从集合读取精确成员，按原日历及已发布政策选择 raw/all 价格候选，并独立核对股票池、行业、公司行动和基准。请求不能提供成员列表、选择结果、哈希或 complete/PASS 标记。不存在完整源集合时继续拒绝；空行动列表、分页耗尽或当前持仓名单不能替代历史完整性证明。

新数据包保存实际冻结时间，后续回测读取截止不早于该物化时间；准备模式不接受调用者倒填 `as_of`。同 key 同参数核验并复用原包，不重新挑选后来资料；同 key 不同参数拒绝。源集合和旧数据包均不改写。

准备阶段独立事务提交合法数据包，然后调用原回测入口。因此随后回测失败时，合法数据包可能已经存在：不能宣称整个命令零写入，也不应换 key 重复组包。排障后按原请求重入复用；准备阶段本身失败则回滚该次新包、成员与快照。所有终端失败仍只返回固定错误码，不打印请求正文或数据库异常。

本模式只补“已有完整源集合→冻结数据包”的生产接线，不生产历史源集合，不证明真实历史、多年 OOS 或晋级资格已齐备。

## 输入

在项目根目录使用 Python 3.12 虚拟环境，沿用已配置的 `DATABASE_URL`；不要把连接凭据写入命令或日志。

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.backtest_once \
  --strategy-version baseline_v1 \
  --authority-bundle-id '<已有证据集合 ID>' \
  --data-snapshot-id '<该集合的数据快照 ID>' \
  --source-policy-id '<该集合的来源政策 ID>' \
  --cost-policy-id '<该集合的历史成本政策 ID>' \
  --start '<集合开始交易日 YYYY-MM-DD>' \
  --end '<集合结束交易日 YYYY-MM-DD>' \
  --initial-nav '<正的初始模拟净值>' \
  --as-of '<本次读取截止时刻，ISO 8601，含 UTC 时区>'
```

ID 来自现有 `backtest_authority_bundles`，与其 `data_snapshot_id`、`source_policy_id`、`cost_policy_id`、`range_start`、`range_end` 一起核对。区间须与冻结集合一致；初始净值作为明确的模拟参数纳入协议哈希。不存在合格集合时，先补实际数据与证据；不能临时建一个占位集合或用当前持仓替代历史股票池。

基准固定沿用 SPY、同一冻结股票池等权和 baseline。成本取已发布的 `backtest_cost_policy_v1`：佣金 0、价差 5、滑点 10 基点；这是历史模拟假设，不代表当前券商实际费用，税务处理仍未知。不得把优化比较的零成本占位政策传入。

## 输出与核验

- 成功返回 `BACKTEST_COMPLETED`、`run_id` 和结果/数据/证据哈希。同一结果重复或并发运行返回同一记录，复用时为 `BACKTEST_REPLAYED`。
- 所有子记录在一个事务内写入。失败回滚，不留下部分回测；终端仅输出固定错误码，不输出原始数据库异常。
- 已有受保护查询 `GET /internal/v1/strategies`（Web BFF 为 `/api/v1/strategies`）返回 `latest_backtest`，可用 `result_hash` 对照命令行输出；来源模式由已持久化证据推导。
- 保存成功不表示多年历史/OOS 要求已满足；以 `history_requirement_met` 为准。历史结果保持 `promotable=false`、`execution_locked=true`，不自动晋级或创建订单。

## 常见缺口

| 错误码 | 含义 |
| --- | --- |
| `BACKTEST_DATABASE_UNCONFIGURED` | 当前入口未取得数据库配置 |
| `BACKTEST_REQUEST_INVALID` | 缺少必要参数或混用互斥模式；不加载运行配置 |
| `BACKTEST_CALENDAR_RANGE_INVALID` | 日期无序、端点不是实际 session 或超出安装库支持范围 |
| `BACKTEST_CALENDAR_AUTHORITY_INVALID` | 同自然键的既有日历不完整、不一致或时点不可见；不修补历史 |
| `BACKTEST_CUTOFF_IN_FUTURE` | 读取截止时刻在未来 |
| `BACKTEST_FROZEN_AUTHORITY_MISSING` | 指定的冻结回测证据集合不存在 |
| `BACKTEST_AUTHORITY_BINDING_MISMATCH` | 集合与快照、来源或成本政策不一致 |
| `BACKTEST_COST_POLICY_MISMATCH` | 成本政策不是已有的历史回测政策 |
| `BACKTEST_STRATEGY_UNAVAILABLE` | 指定时刻尚无可用的策略定义 |
| `BACKTEST_AUTHORITY_OR_DATA_INCOMPLETE` | 时点、区间、数据哈希、公司行动、股票池、基准或来源核验未通过 |
| `BACKTEST_RUN_FAILED` | 参数、配置、连接或执行失败；先排查，不自动重试 |

本入口无调度或共享服务注册。停用相应准备/运行模式即可回退入口；已提交日历、数据包及结果保留为不可变证据，不通过删除记录或改写历史来回滚。无新增迁移或依赖，旧参数模式和旧包哈希/回放语义保持。

## 有来源的当前普通股首次建档

`instrument_identity_local` 只处理固定 `us_liquid_v1` 定义中的普通股，不建假持仓、
不发布股票池、不创建研究、不授予交易许可。它将 SEC 同证券登记行的普通股标题、
TradingSymbol、Exchange/context/CIK，与当前 FMP profile 的 symbol、CIK、USD、交易所
及类型冲突标记核对。普通股结论来自 SEC 明确标题和 CommonStockMember，不能由
`isEtf=false` 推断。当前窄解析只支持已验证的根命名空间、同一表行/上下文、XNYS/XNAS
结构；其他结构明确拒绝，不拼接债券、优先股或另一证券的信息。

已有 FMP 原件沿 `fmp_profile_local collect` 取得，格式见对应运行手册。需要 SEC 原件时
使用一次显式收集，复用正常 `SEC_CONTACT`，不重试、跳转或调用模型：

```bash
.venv/bin/python -m nexus.ops.instrument_identity_local --repo-root "$PWD" collect-sec \
  --source-url 'https://www.sec.gov/Archives/edgar/data/1551152/000155115226000008/abbv-20251231.htm' \
  --output-dir /absolute/private/new-sec-archive
```

SEC 目录必须仅有 URL 指定的 HTML 文件和 `receipt.json`，目录0700、文件0600；receipt
包含 `source_url/received_at/sha256/byte_length/http_status=200`。已有诊断归档可复制原件
和原 receipt 到新标准目录，保留实际接收时间，不改原归档、不把适配时间当采集时间。
SEC accession 前缀是提交者（可能为代理）CIK，不要求等于发行人；URL发行人目录、
上下文 entity identifier、CIK事实及FMP必须一致。收据未来时间、符号链接、重复键、
原件hash不符或缺失都拒绝。SEC报告期和当前观察分别保存，不证明其间从未发生更名。

完全离线导入和精确重读；输出文件必须尚不存在，父目录预先设为私有：

```bash
.venv/bin/python -m nexus.ops.instrument_identity_local --repo-root "$PWD" import \
  --symbol ABBV --fmp-archive /absolute/private/fmp-archive \
  --sec-archive /absolute/private/sec-archive --output /absolute/private/import-result.json
.venv/bin/python -m nexus.ops.instrument_identity_local --repo-root "$PWD" read \
  --snapshot-id '<snapshot_id from the private import result>'
```

入口自行读取正常本地配置，数据库和对象存储限定本地地址。完全缺失时使用该定义的
stable UUID，alias从数据库本次核验UTC日开始，tick_size保留unknown。有效既有身份可
复用，但历史同symbol冲突、类型/币种/交易所或源标识冲突、期望UUID被占用均拒绝，
不合并或改旧行。同symbol事务锁加现有唯一/排斥约束处理并发；精确重放返回同一证明。
证明与新身份同事务提交，失败不留下部分身份；已经写入的孤立对象按既有回收规则处理。

相同内容优先复用已有 DataArtifact URI/schema 并核读原件；证明仅存入
`DataSnapshot(source=current_instrument_identity)`，不改研究authority。精确reader检查
原件、收据、证明绑定及当前数据库日期下的唯一有效身份，不能把旧证明当当前可用。
`created/existing/verified` 都不表示49成员预检通过、股票池发布或历史回测可用。
安全错误统一 `CURRENT_IDENTITY_*`：先诊断状态/原件/合同，不重复采集或重放未知结果。

回退方式是停止该import入口，保留已提交身份、证明和读取兼容。无需新表、迁移、
依赖或服务重启；不删事实、改写历史，亦不回退券商刷新身份校验。

### 显式自定义普通股成员定义归档

自定义 `StatementClassOfStockAxis` 成员不因名字含有 Common Stock 而合格。
必须由原 SEC HTML 引用同 accession 的 schema，再绑定 definition/label linkbase、
同角色内标准轴到域到成员的关系、精确英文标签及原登记行。解析不自动下载导入的
taxonomy，也不将自定义 QName 改写为标准成员。

已有官方原件和真实收据时，用正常离线入口封装（六个输入文件须位于私有目录）：

```bash
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.instrument_identity_local \
  --repo-root "$PWD" archive-taxonomy --symbol '<固定定义普通股代码>' \
  --fmp-archive '<FMP归档目录>' --sec-archive '<SEC正文归档目录>' \
  --schema '<schema原件>' --schema-receipt '<schema原收据>' \
  --definition '<definition原件>' --definition-receipt '<definition原收据>' \
  --label '<label原件>' --label-receipt '<label原收据>' \
  --output-dir '<新的私有归档目录>'
```

每份收据使用原 `source_url / received_at / sha256 / byte_length / http_status`，
不重新生成接收时间。封装前严格离线核验，输出固定 `schema.xml / definition.xml /
label.xml` 及各自的 `*-receipt.json`。随后仅在正常 `import` 命令显式添加
`--taxonomy-archive '<上述目录>'`；不支持在 `revise-listing` 中隐式启用。

新证明使用 `current_custom_member_v1`：保留四项 `artifacts`，
另绑定六项 `taxonomy_artifacts`，正常存储读取会重新核验全部十份原件。
旧四原件证明和 JSON 不变，无数据库迁移。回滚旧程序后无法读取新版本证明；
应保留兼容读取程序，不能删掉新增原件或字段伪装旧证明。历史回测资格仍为 false，
执行继续锁定。

## 只读准备未来身份绑定

当既有合法证券使用 broker UUID 时，旧 `us_liquid_v1` 固定 UUID 预检仍按原合同报告
错配。以下入口重验已导入的当前来源证明，输出独立的未来绑定准备报告：

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.instrument_identity_local \
  --repo-root "$PWD" prepare-universe --effective-session '<未来交易日 YYYY-MM-DD>' \
  --output '/absolute/private/new-universe-preparation.json'
```

复用现有本地配置、对象存储和严格身份 reader，不外部采集。输出文件须为新路径，
明细仅存私有文件；终端仅报状态。报告保留全部49成员、原定义hash、未来交易日、
实际核验时间及各成员缺口。成功绑定记录实际UUID、proof snapshot/hash与alias，
另用未来交易日重验唯一有效ticker；当前证明不能代替未来适用性。

新候选版本为 `us_liquid_current_identity_v1`，与旧定义隔离。任何成员缺口都会
返回 `BLOCKED` 和 `candidate=null`；当前proof仅支持普通股，ETF明确返回
`IDENTITY_TYPE_UNSUPPORTED`，无证明普通股返回 `IDENTITY_PROOF_MISSING`。
报告hash固定本次检查结果，不授予发布资格。此入口不写数据库、不修改旧UUID、
旧定义、股票池或成员，也不生成历史PIT或回测资格。后续发布若实施，须重核当时
来源及身份，不直接信任这个可编辑的本地报告。停止使用该只读入口即可回退。

## 追加当前上市信息修订

既有身份的上市所与严格双源证明冲突时，可显式追加上市修订；不修改原Instrument、
alias、旧proof、研究或价格。仅允许上市所差异，类型、币种、风险分类、CIK和已知
身份标识冲突仍拒绝。输入沿用已取得的同证券FMP/SEC归档，不需要重新采集：

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.instrument_identity_local \
  --repo-root "$PWD" revise-listing --symbol '<固定定义普通股代码>' \
  --fmp-archive '/absolute/private/fmp' --sec-archive '/absolute/private/sec' \
  --output '/absolute/private/new-listing-revision.json'
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.instrument_identity_local \
  --repo-root "$PWD" read-listing-revision --snapshot-id '<新修订snapshot UUID>'
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.instrument_identity_local \
  --repo-root "$PWD" prepare-universe --use-listing-revisions \
  --effective-session '<未来交易日 YYYY-MM-DD>' \
  --output '/absolute/private/new-listing-preparation.json'
```

修订使用独立DataSnapshot合同，保留原exchange和旧记录指纹，存原件绑定及新来源
上市所；UUID不变。checked_at/effective_at/available_at是数据库事务内实际导入核验
时间，不是来源接收日期，也不是精确commit时间；不提供任意历史提交可见性还原。
reader拒绝早于核验/创建时间的as_of并重验原件、指纹和当前alias。相同导入幂等；
并发按同一身份串行验证，最新证明失效不悄悄回退。当前修订不解释或改写旧字段历史。

默认prepare-universe及旧身份reader仍使用原合同。只有显式启用新修订的准备报告
输出listing_exchange和所用proof合同，候选版本独立为us_liquid_listing_revision_v1；
49项及ETF缺口仍保留，缺任何成员时candidate=null。行情、历史研究、价格与旧v1
预检未接入该修订，不自动切换。回退时停止显式使用新入口，保留追加事实和原始记录。

### 当前股票 ETF 身份（独立于普通股及历史回测）

`nexus.ops.etf_identity_local` 接收既有 FMP 两文件归档、SEC submissions/index/497K
及逐响应 receipts、SEC 基金目录原件。仅接纳已绑定 series/class 的股票 ETF，且需
497K 封面上市信息、该基金的 ETF 法律表述和美国股票投资目标；不扩展至债券、黄金或
UIT。FMP `AMEX` 仅保留为供应商标签，精确上市交易所来自已绑定 SEC 封面，普通股映射不变。

正常入口（参数指向受保护目录，输出必须是新文件）：

```sh
.venv/bin/python -m nexus.ops.etf_identity_local --repo-root "$NEXUS_REPO" collect-sec \
  --symbol IWM --accession "$ACCESSION" --mapping-file "$MAPPING_RAW" \
  --mapping-archive "$MAPPING_ARCHIVE" --output-dir "$NEW_SEC_ARCHIVE"
.venv/bin/python -m nexus.ops.etf_identity_local --repo-root "$NEXUS_REPO" import \
  --symbol IWM --fmp-archive "$FMP_ARCHIVE" --sec-archive "$SEC_ARCHIVE" \
  --mapping-file "$MAPPING_RAW" --output "$PRIVATE_RESULT"
.venv/bin/python -m nexus.ops.etf_identity_local --repo-root "$NEXUS_REPO" read \
  --snapshot-id "$SNAPSHOT_ID"
```

目录归档的 `mapping-binding.json` 保留原始 key/version_id/hash/stored_at，
`original_received_at=null`。正常采集/导入先从本地对象存储核验精确版本、原文及
真实 LastModified；stored_at 不当作上游接收时间。新 DataArtifact 的已知接收时间
使用原始 FMP/SEC receipt；目录原文及其绑定文件的 received_at/observed_at 是本次
本地归档接收时间，原 remote receipt 仍未知。proof.checked_at 是本次数据库核验时间，
不声称历史 PIT 或精确事务提交时间。

正常采集最多一次 submissions、index、正文读取，首个错误即停，保留已收到原件，
不自动重试。不为已有完整归档再次采集。导入仅支持第一份身份及其幂等重读，已有
master 冲突拒绝且不覆盖；不发布股票池，不创建研究或订单。

未来池准备显式增加 `--use-etf-identities`，可与 `--use-listing-revisions` 并用；
默认普通股合同不变，仍保留全部 49 项，任一缺口都不生成可发布候选。没有迁移、
服务重启或历史回填；关闭该显式读取选项即可恢复原消费者行为，已归档事实不删除。

股票ETF份额类别的窄兼容分支另以SEC index的唯一series名称、class的ETF Shares标签、
正文完整类名/ticker/同基金股票投资目标与策略确认身份。FMP依然核ticker/CIK/USD/isEtf，
其展示名不充当法律名称；新身份子类型及显式未来准备同时保留legal_name、fmp_name和
fmp_name_matches_sec，差异不删除、不改名或宣称一致。不接受错series/class/ticker、
债券目标或另一基金封面；网页检索中尚未正常归档的CUSIP不进入证明。
这是内部JSON合同增量，旧IWM原proof字段/序列化及默认读取保持兼容；新proof落库后，
回滚可关闭显式ETF消费者，但必须保留新子类型兼容reader，不直接退回只认旧字段的
应用或裁剪已存proof。原件仍8份，未增加来源、数据库schema或迁移。


NASDAQ 股票ETF窄分支以当前目录的精确 CIK/series/class、完整 497K 封面代码、
同基金股票目标/策略及开放式基金改制声明绑定当前身份。QQQ 索引页 ticker 单元格
为空时保留 `filing_index_ticker=null`，不从名称猜代码；绑定依据单独保留为
`current_directory_and_bound_497k_cover`。索引表头允许省略行结束标签，但不合并
目标 series/class 行，也不放宽唯一性。非目标页面布局表不参与身份解析。
改制日期仅记录来源事实，当前 alias 仍从本次核验日期起，不回填历史身份或类型。
新增内部 JSON 子类型携带 XNAS、原件空字段及改制日期，旧 IWM/VTI JSON 兼容；
回滚继续采用关闭显式消费者并保留兼容 reader，不删除已存证据。无新增迁移或依赖。


股票型 UIT 当前身份支持已分别验证来源合同的 SPY 与 DIA，使用 `etf_identity_local import-uit`，
参数为 `--symbol SPY --fmp-archive PATH --sec-archive PATH --output NEW_PATH`。
FMP 目录仍为 profile.json/receipt.json；SEC 目录为 submissions.raw、
submissions-receipt.json、filing-index.raw、index-receipt.json、disclosure.raw、
disclosure-receipt.json，共八份原件。正文及索引复用 instrument_identity_local
collect-sec 的正常收据格式，submissions 使用既有 SEC client 单次有界读取并保存
相同收据字段；归档组装仅复制原字节，不重写收到时间或来源。

此分支不要求 mutual-fund 目录中不存在的 series/class。以精确 CIK、submissions
当前唯一 ticker 和唯一目标 497 记录、索引正文链接、完整招募说明书的当前/旧名及
本体上市代码、股票目标与策略绑定；NYSE Arca 来自封面，FMP AMEX 和 submissions
NYSE 仅保留来源标签。独立子类型保存 `legal_structure=unit_investment_trust`，
法律名称/旧名/展示名不互相覆盖。旧三种 ETF JSON 及默认消费者不变，未来准备仍须
显式 ETF 选项，当前身份不授予历史资格或执行许可。无 schema 迁移；回退停用新入口
与显式消费，但须保留新 JSON 的兼容 reader 和不可变原件。

DIA 复用八原件显式 import-uit 路径，但校验独立的 Dow Jones Industrial Average
目标与 DJIA 普通股组合策略；SPY 仍要求 S&P 500，不跨基金替换目标。DIA 旧名中的
已证实商标 SM 仅在封面旧名的 INDUSTRIAL AVERAGE 与 ETF TRUST 之间规范化，
原始封面声明与原件仍保留，不全局删字符。无新增 JSON 字段、数据库或迁移。


国债基金当前身份仅 TLT 使用 `etf_identity_local import-treasury`，参数与原 import
相同（包括 mapping-file），采集复用现有 collect-sec。原 import/UIT 入口仍拒绝
非股票身份；新入口也拒绝股票基金。TLT 固定保留定义中的 non_equity_etf 与
US_TREASURY，精确目录 series/class、索引 ticker、完整 497K 封面 Nasdaq、同基金
二十年以上国债目标及原文 80%/90% 策略共同校验。比例是来源事实，不设新风控规则。
独立内部 JSON 子类型记录 underlying_asset_class=us_treasury_bonds，旧 ETF proof
JSON 不变。存储/严格读取同时核对原件、master 与原池资产类型和风险分类。
未来 prepare 只在显式ETF选项下消费该 TLT 合同，默认仍拒绝；GLD 仍单列类型未支持。
没有改写股票池定义、历史资格或数据库 schema，也没有新增迁移。回退停用显式入口/消费者，并保留
新 JSON 的兼容 reader 与不可变原件；采集收据保留每份实际收到时间。


黄金信托当前身份仅 GLD 使用 `etf_identity_local import-gold`，参数为
`--symbol GLD --fmp-archive <目录> --sec-archive <目录> --output <私有结果文件>`，
不接收基金 series/class 映射。八原件布局同 import-uit，但证据来自绑定的完整 10-K：
同一注册行的黄金受益份额、ticker、NYSE Arca 与同上下文 CIK，以及该信托正文的
纽约法投资信托、实物黄金、受益份额和非 1940 Act 注册公司声明。不能从税务 grantor
称谓推断 UIT。SEC 索引的 `/ix?doc=` 只接受精确同源、同 CIK、同 accession 正文路径；
保留原索引和正文收据，不重新标记来源。

GLD 保留 `non_equity_etf/GOLD`，新内部 JSON 子类型记录 `gold_bullion` 和
`new_york_investment_trust`，不影响旧六种已存 ETF JSON；无迁移或新增依赖。
正常读取重新核对原件，股票池仍需显式 `--use-etf-identities`；当前身份不授予历史
资格、不生成候选或解除执行锁。回滚关闭新导入与显式消费，保留兼容 reader 和原件。
