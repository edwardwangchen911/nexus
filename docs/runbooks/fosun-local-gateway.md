# 本机复星只读 Gateway

本流程只管理宿主机 `local_read_only` Gateway。它复用已有私有 TLS、HMAC、Provider 材料和
`.secrets/nexus-fosun-local/replay.sqlite3`，只绑定 `127.0.0.1:8081/8082`，不启用
Compose 的 `provider`/`broker` profile，不开放券商写接口，也不解除 `execution_locked=true`。

在主仓库 `main` 使用同一提交的项目虚拟环境。先检查本机固定配置；`check` 不启动服务、
不访问券商，也不证明网络或快照新鲜：
Gateway 源码、Provider manifest、项目依赖定义或锁文件有未提交改动时，入口拒绝把它们标记为
当前 release；其他工作文档的未提交改动不影响该检查。

```bash
uv sync --frozen --package nexus-fosun-gateway
uv run --package nexus-fosun-gateway nexus-fosun-gateway-local check --repo-root "$PWD"
uv run --package nexus-fosun-gateway nexus-fosun-gateway-service status --repo-root "$PWD"
```

配置加载同时只读检查本机服务端、三套 CA 和两个客户端证书的有效期；
必须为单个 PEM 证书，并满足 `notBefore <= 本机UTC时钟 < notAfter`。
格式不合法、过期或尚未生效分别返回固定码
`LOCAL_GATEWAY_TLS_CERTIFICATE_INVALID`、`LOCAL_GATEWAY_TLS_CERTIFICATE_EXPIRED`、
`LOCAL_GATEWAY_TLS_CERTIFICATE_NOT_YET_VALID`，正常 `check` / `run` 均在网络和
Gateway启动前拒绝。此项不证明证书链、私钥匹配或远端可用，仍须完成正常
mTLS健康检查；也不自动续签或替换材料。证书续签须按认证材料变更取得授权，
备份原件，保留三套角色信任域和只读权限。

显式启动与停止使用同一个 launchd 标签 `com.nexus.fosun-local-gateway`：

```bash
uv run --package nexus-fosun-gateway nexus-fosun-gateway-service start --repo-root "$PWD"
uv run --package nexus-fosun-gateway nexus-fosun-gateway-service status --repo-root "$PWD"
uv run --package nexus-fosun-gateway nexus-fosun-gateway-service stop --repo-root "$PWD"
```

`start` 先运行零网络配置检查，再注册只含入口路径和提交身份的私有 plist；不会自动刷新
账户。`status=ready` 要求注册实例归属与当前提交一致、replay 就绪标记绑定该 PID，且 broker
与 market 两条 mTLS `/health/ready` 均为 200。`degraded`、`inactive` 或其他失败码不代表
账户数据可用。运行满 12 小时的本地只读授权到期后会安全退出，launchd 不自动重启；先检查
状态和失败原因，再显式 `start` 建立新实例。原 replay sqlite 不删除、不替换。

`LOCAL_GATEWAY_SERVICE_START_UNCONFIRMED` 只表示本次观察未确认启动，不能据此再次启动。
输出中的 `observation_stage` 区分 launchd 读取、状态解析、进程退出与健康检查；
`observation_code` 和 `observed_state` 仅输出固定安全值，未知状态不透传原始内容。
launchd 的 `xpcproxy` 表示启动中，不是就绪；首次启动只在既有观察上限内等待
同一实例变为 running 并通过健康检查。对已启动中的实例再调用 `start` 只报告
`starting`，不重复注册或启动；`stop` 此时也不绕过退出确认。
先用同一标签的 `status` 核对原实例，避免创建第二实例。显式 `stop` 在核对精确归属、
确认无 PID 且状态为 inactive 后清理注册；macOS 的信号退出无需伪造数字退出码。

只有服务 `ready` 后才按现有本地产品入口执行一次正常只读刷新：

```bash
uv run --project engine nexus-fosun-local refresh
uv run --project engine nexus-fosun-local status
```

刷新失败先查持久化 sync 状态和安全错误码，不换请求重试。健康状态与最新成功快照时间分别核对；
页面仍只读已持久化数据，不从浏览器触发刷新。运行或配置变更需遵守项目 `AGENTS.md` 的授权边界。

## 报价原始时钟字段与发布顺序

报价响应保留供应方 `qtDate` / `qtTime` 为可空的
`provider_quote_date` / `provider_quote_time`。它们只作为有界严格整数归档，
不自动解释成 UTC 或 Unix 时间；`as_of` 仍表示 Gateway 接收时刻。
旧归档缺字段时继续读取，存在但格式错误的字段按原固定错误码拒绝。
字段保留不清除时间戳未核实或盘口缺失标记，也不使报价取得研究或执行许可。

旧 Engine 使用禁止额外字段的响应合同，不能消费新版 Gateway 新增的字段，
包括值为 null 的字段。获准更新服务后，先升级 Engine 消费者并核验，再升级
Gateway 生产者；不能只更新 Gateway。回退先停止新生产者并恢复旧 Gateway，
再回退 Engine。保留所有新旧原件；新原件的后续读取仍须使用兼容新字段的
消费者，不改写原件或为兼容放宽严格校验。

## 独立账户同步的身份缺口诊断

SEC 完整提交包可通过现有身份工具的独立归档入口取得：

```bash
uv run --project engine python -m nexus.ops.instrument_identity_local \
  --repo-root "$PWD" collect-submission \
  --source-url 'https://www.sec.gov/Archives/edgar/data/CIK/ACCESSION_WITHOUT_DASHES/ACCESSION.txt' \
  --output-dir /absolute/private/new-submission-archive
```

示例中的 CIK 和 accession 必须替换为已定位的官方申报值；目录中的十八位 accession
必须与文件名移除连字符后的值完全一致。此入口只接受 SEC 官方 HTTPS 完整提交包，
复用单请求、不重试、字节上限、私有目录和摘要回执；不导入身份、不访问券商或业务库。
旧 `collect-sec` 仍只接受原 HTML 文件范围。归档成功不代表份额、当前上市状态或账户
对账通过；后续须严格核验完整提交结构、唯一正式组件及其实际内容。

`parse_single_class_submission` 仅核验单申报人、单基金份额与唯一 `497K` 正式组件的
结构绑定，直接读取完整提交包内的组件；不得混用另一次下载的组件原件。它拒绝多份额、
字段冲突、未来接受时间、截断与封面代码或法定名称不一致。这一解析结果尚未接入身份
登记或账户同步，不能替代当前有效的官方 MIC、上市状态及持仓来源核验，也不扩大候选
池、研究或执行许可。

官方 MIC 原件通过同一工具的独立入口归档：

```bash
uv run --project engine python -m nexus.ops.instrument_identity_local \
  --repo-root "$PWD" collect-mic --output-dir /absolute/private/new-mic-archive
```

来源固定为 ISO 20022 的 MIC 公布页与 ISO 10383 CSV，每个来源单次请求，不跟随
重定向、不重试；目录必须全新，保存原字节、来源 URL、实际接收时刻、摘要及长度。
第二项失败保留首项归档和安全失败回执，不产生完整成功回执。归档不写业务库，不
登记证券、不构造券商客户端；还须独立校验版本的公布及生效日期、下次公布日期、
唯一 MIC 行、市场分段与运营市场归属及有效状态。新的公布日到来后不能沿用旧归档
宣称当前证据齐备。MIC 本身不证明某只基金仍上市，也不扩大研究或交易准入。

当前 Cboe 上市观察通过固定来源独立归档，不能改用“Symbols Traded”统计：

```bash
uv run --project engine python -m nexus.ops.instrument_identity_local \
  --repo-root "$PWD" collect-cboe-listing --symbol SYMBOL \
  --output-dir /absolute/private/new-cboe-listing-archive
```

每个固定来源仅请求一次：官方上市名单 HTML、同入口 CSV 和该代码的产品页。
失败保留原件及安全回执，不自动重试；完整归档不写业务库。目录地址通过显式末尾
斜杠发出，通用 HTTP 客户端默认路径行为、固定来源、路径段校验和禁止重定向不变。
`parse_cboe_listing` 核对 HTML/CSV 的完整成员集合及唯一代码、产品页标题和上市日期，
保留交易所实际更新时间，并拒绝未来时间、夏令时冲突及超过 24 小时的观察。页面中
Book Viewer 的标题不作为基金标题。产品页名称原样保留，SEC 法定份额名称仍须独立
核验；该观察尚未接入身份登记或账户同步，不能单独充当完整持仓身份权威。

`validate_cboe_source_chain` 离线核对 FMP、完整 SEC 提交包、ISO 公布页/CSV 和 Cboe
上市名单/CSV/产品页，共 14 项原件及回执；逐项严格校验固定来源、字节摘要、长度、
HTTP 状态和实际接收时刻。FMP/MIC/上市采集不得超过 24 小时，同一 MIC 或上市批次
的接收时刻差不得超过两分钟，上市数据更新时间不得晚于采集时刻。历史 SEC 原件
保留原时点，不被重标为今天；招募书修订日期允许晚于最初上市日期。

ISO 新版本已经发布而修改尚未生效时，须显式附加前一生效版本公布页/CSV及其回执，
共 18 项。前一版本须在新公布日期前已经采集、当时有效；新旧 BATS 与 XCBO 整行
必须一致，且两版本公布日期相接。旧版本只作历史佐证，不能当作当前版本或跳过
新增变更生效时间；仅凭较早的行更新时间不通过。新版本已生效时不附加历史版本。

此链仍明确返回 `identity_registration_ready=false`：当前 SEC 目录冲突检查、SEC
策略分类、新鲜券商持仓来源，以及身份登记、严格读回和账户解析接线尚须完成。
不得将来源链通过当作 canonical 身份、账户对账、候选池或执行许可。

`nexus-fosun-local refresh-broker` 使用现有 broker 只读接口，不构造行情客户端，
不受逐持仓行情采集预算替代。它仍须对所有非零持仓核对本地证券身份；身份缺失时
返回 `LOCAL_BROKER_INSTRUMENT_SOURCE_REQUIRED`，不继续现金/订单等对账阶段，
也不生成新的完整账户快照。

正常 CLI 尝试在 `.secrets/nexus-fosun-local/identity-gaps/` 留下不可覆盖的私有诊断 JSON。
归档失败仍返回原身份拒绝码，不能据此认为诊断已保存；须核对文件实际存在。
`.secrets`、父目录和专用目录须为当前用户所有的真实目录，权限 0700；文件为 0600。
诊断只保存待核验 provider code、币种、持仓页观察时刻和归一化页摘要，
不保存账户标识、数量、成本或资金。`identity_authority=false`：摘要不是原始响应归档，
诊断不能导入证券身份、替代分类/官方身份来源，或作为研究和执行证据。
后续须通过既有分类及官方身份来源补齐绑定；不能据代码名称臆测证券类型。
本入口不自动重试，不删除既有诊断；失败后先读原回执，不重复调用来刷新状态。


## 已保存身份缺口的分批处理

`nexus-pending-identity-local --repo-root "$PWD" --list-pending` 只读列出经原件绑定校验的
待处理 discovery ID、分类路由和证据可用时刻；不输出持仓代码、访问券商或采集来源，
不写业务库。缺口诊断文件本身不在此清单中：仍须先取得并保存真实分类证据。

需要分批处理时，通过正常入口显式选择清单中的 ID：

```sh
uv run --project engine nexus-pending-identity-local --repo-root "$PWD" \
  --output-dir '<新的私有归档目录>' \
  --discovery-id '<清单中的 discovery UUID>'
```

`--discovery-id` 可重复，最多 3 项；未知、已不再待处理或重复 ID 拒绝，不能传股票代码。
不指定 ID 时保留原行为：全部待处理记录超过 3 项则拒绝，不自动截断或循环执行。
选中记录仍走原有分类路由、官方身份证据与实际持仓校验。`pending_before` 和 `remaining`
统计全部待处理记录，包括未选择的项目；只要仍有缺口，结果不会报告整体 resolved。
`manual_review_required` 是本批次人工审核项数。不自动追加下一批、同步账户或解除执行锁。

此扩展不改 schema、凭据、来源预算或身份准入。回退代码不会删除已经保存的证据及身份事实；
上线后须独立核对真实来源、导入读回和账户快照，隔离测试不代表实际缺口已经补齐。


## 从私有缺口采集分类证据

正常入口 `discover-identity-gaps` 读取已有 `identity-gaps/<UUID>.json` 私有诊断，
仅将其中代码作为候选选择器。文件及父目录须为当前用户所有、0600/0700，拒绝软链接、
重复 JSON 键、额外字段或宣称身份权威的文档。`--gap-item-index` 是诊断中 `unresolved`
数组从 0 开始的索引，可重复但不得重复同一索引，每次最多 3 项；仅支持 USD 美股。

```sh
uv run --project engine nexus-fosun-local discover-identity-gaps \
  --identity-gap-file '.secrets/nexus-fosun-local/identity-gaps/<UUID>.json' \
  --gap-item-index 0 --gap-item-index 1
```

入口通过现有受保护配置读取一次账户和一次完整持仓页，并重新检查本地身份。
所选代码须仍为该账户的非零 USD 持仓且身份尚未解决；账户不唯一、分页未完成、
币种/账户不匹配或已解决时，在报价分类采集前拒绝。即使账户持仓超过 20 项，
分类请求也只包含显式选择的最多 3 项；原完整行情同步的 20 项上限不变。

随后只请求一次现有报价分类接口，将原始响应及请求绑定存为不可变归档，逐字节读回，
在单一事务内保存分类发现及读回核验。只保存 discovery，不创建 canonical Instrument、
账户快照、LatestQuote、研究、周期或订单，也不自动补身份或同步账户。
`remaining_identity_gaps` 是采集时全账户未解决身份的数量，分类成功不会减少它。

成功后再用 `nexus-pending-identity-local --list-pending` 查询新增的记录 ID，
通过既有官方身份证据流程显式分批处理。失败停止，不追加另一批或放宽身份准入。
无 schema 变更；回退代码保留已归档的原件和发现记录。真实来源与账户刷新仍须独立验收。

### Cboe 基金来源候选核验（尚未上线）

`instrument_identity_local collect-sec-directory --kind company|fund` 使用正常受保护配置，
每次仅归档一份固定 SEC 官方目录，不导入业务库。已有归档可供纯只读核验，不能重标来源时间。
候选来源核验将公司目录冲突、基金目录绑定、完整 SEC 申报、ISO MIC 和 Cboe 上市原件一起检查。
基金目录缺失目标行不视为普通股票；当前支持的独立 SEC 招募书分支保留每日两倍做多单股 ETF
分类，并要求独立段落标题分隔投资目标与费用、策略与风险，正文的费用措辞不能截断目标。

来源分类通过仍返回 `identity_registration_ready=false`、`new_position_eligible=false`。
来源分类本身不提供新鲜券商持仓绑定、canonical 身份登记或完整账户快照。
真实账户验收尚待完成，不扩展选股池、回测或交易权限。

候选 `cboe_held_fund_evidence_v1` 已定义脱敏持仓绑定与原件读回格式：登记时必须有五分钟内、
分页完整且账户唯一的正常券商持仓页，代码、USD 币种和非零持仓均须匹配。证据只记录来源请求
绑定及原件哈希，不包含账户编号、持仓数量或资金。历史读回按原登记时刻复核来源与持仓绑定，
不能将旧证据重新视为当前账户对账或当前行情。
纯合同测试成功不代表真实账户身份已补齐。

候选数据库登记与严格读回已完成隔离 PostgreSQL 验证：复用现有表，无迁移；同一来源和
同一持仓请求重复或并发登记只产生一条身份事实，新持仓请求追加独立事实。已有主档须有
本分支可读回的原件证据，兼容的字段和别名本身不构成权威；不接管无证据主档。
归档保留原回执的来源时刻，原件损坏、伪造快照或身份不一致时拒绝。
候选两处账户解析入口已接严格读回，普通证券不增加原件读取；该类基金无原件读取器、
无证明或最新证明损坏时拒绝，不回退旧证明。正常完整/仅账户刷新、最终入库及分类发现
均传递延迟创建的原件读取器。隔离数据库验证覆盖登记到两处解析，正常 CLI 的独立进程
启动检查与刷新顺序回归通过。真实账户同步与页面验收仍待完成，
也未合入或部署；本段不宣称页面持仓已刷新。

候选正常入口已加入现有 `nexus.ops.etf_identity_local`：`import-held-cboe` 登记，
`read-held-cboe --snapshot-id <UUID>` 只读复核。登记显式指定 `--symbol`、`--output`、
`--fmp-archive`、`--submission-file`、`--mic-archive`、`--listing-archive`、
`--company-directory-file`、`--company-receipt-file`、`--fund-directory-file` 和
`--fund-receipt-file`；需要前一有效 MIC 版本时指定 `--previous-mic-archive`。
完整 SEC 申报的回执固定为同目录 `receipt.json`，其余目录遵循对应采集入口文件名。
所有输入文件须属于当前用户、0600，直接父目录须为0700；拒绝文件及目录软链接。

正常配置仍由 `--repo-root` 解析；来源先校验后仅读取一次账户及一次完整持仓页，
再用数据库时钟登记并在新只读事务严格回读。不获取现金、订单、行情或完整账户快照，
不自动补采集或重试。输出路径必须全新，回执只保存登记结果 ID，不输出账户资料。
隔离正常入口到数据库和账户解析已验证；失败或结果不明先查持久化状态，不重放。
