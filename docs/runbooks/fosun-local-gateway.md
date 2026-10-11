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
