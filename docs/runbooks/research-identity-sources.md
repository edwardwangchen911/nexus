# SEC 研究身份证据采集

此入口仅归档 SEC 当前 ticker/exchange 目录和对应 issuer submissions，供后续研究身份绑定使用。
不连接业务数据库，不注册研究权威，不创建周期、模型任务或订单，也不修改已有身份原件。
`archived` 只证明本批来源取得且关联检查通过，不代表研究身份、决策或执行准入已完成。

从仓库根目录运行正常 CLI；受保护配置中的 `SEC_CONTACT` 由正常配置入口读取，不写入回执：

```sh
uv run --project engine python -m nexus.ops.research_identity_local \
  --repo-root "$PWD" --symbol NVDA --symbol AVGO --symbol AAPL \
  --output-dir '/absolute/private/new-leaf-directory'
```

父目录必须已存在、属于当前用户且权限为 0700；叶子目录必须不存在，入口创建为 0700。
所有归档及回执权限为 0600。目录存在时拒绝运行，应只读检查原结果，不更换目录重试。

一次目录请求供同批 1–5 个唯一 symbol 共享，每个 symbol 最多一次 submissions 请求。
所有请求固定 SEC HTTPS origin、禁止重定向且不重试，总窗口 60 秒；首个失败停止本批。
精确核对唯一 symbol/exchange/CIK 对应，不以字段分别存在代替关联；重复 JSON 字段拒绝。

成功归档包括 `directory.json`、`directory-receipt.json`、每只标的的
`<symbol>-submissions.json` 与 receipt，以及 `result.json`。回执保存原始字节 hash、长度、
来源 URL 和实际 received_at；received_at 不代表 SEC 文件生成或更新时间。
失败保留已取得的非敏感原件，result 只保存固定错误码、阶段和请求次数。

研究用途的 `research_current_identity_session_v1` 已经独立方案审阅：旧严格身份证明作为基础，
两份 SEC 当前来源须在原 Run decision_time 所属纽约日期取得，且全部依赖及引用在决策前可用。
Paper 另须核对正式 decision_session。本采集入口不会自行注册引用，更不会将旧来源或
本次接收时间倒填到历史 Run。这里只证明当前研究规则下的证券—发行人对应；不证明 SEC
当日更新、可交易性、财务修订已消除或任何执行许可。

## 正常导入与冻结

已应用 `0066_research_identity_binding` 的本地数据库可使用以下正常入口：

```sh
uv run --project engine python -m nexus.ops.research_identity_binding_local \
  --repo-root "$PWD" --identity-snapshot-id '<strict-existing-identity-snapshot-uuid>' \
  --collection-dir '/absolute/private/existing-collection-directory'
```

入口以数据库时钟验证旧严格身份证明和同纽约日期的两份 SEC 原件/回执，写入不可变对象、
DataSnapshot 与 ResearchEvidenceRef；重复导入相同包装返回原引用，不更新原件时间。
实际登记时间保留，不允许把决策后登记的引用用于更早决策。同日不同包装并存时，普通
选择保留未知，不挑选最新或较有利的一份；已有 Run 只读其冻结的精确引用。

`registered` 不等于研究或执行通过。仅未来新建的、已有基本研究材料的快照可增加此引用；
身份引用本身不能创建研究快照。总冻结引用仍不超过 100，保留价格与组合上下文预留容量。
研究引用达到 98 时，不为身份补充移除任何已有研究引用（包括未决修订及其依赖）；
新快照和 security tool 明示 `research_identity_evidence_capacity_exhausted`，身份保持 unknown。
工具和执行入口按原 Run decision_time 重读原件；过期、错配、损坏均拒绝，不回填旧 Run。
入口不调用模型、不创建周期或订单，`execution_locked=true` 保持不变。

迁移只扩展现有证据归属函数，不改历史表或记录。启用新版本前须在受控更新中先应用迁移；
旧运行版本不应加载此代码。尚无冻结身份引用时可降回 `0065_portfolio_management`；一旦
引用已被冻结，降级会明确拒绝，应保留迁移和审计记录，以代码回退处理。


## 提前只读检查

使用正常入口 `uv run --project engine nexus-engine-agent-paper-readiness --repo-root "$PWD"`。
`research_inputs.current_identity_binding` 分候选报告当前研究身份凭证的 `evidence_status`
和研究快照的 `snapshot_status`。前者 ready 表示当前严格原件读回符合目标纽约交易日；后者
ready 还要求在研究快照原 cutoff 严格重验其冻结引用、归属和元数据。旧快照不会因为新证据
到达而变成 ready；次日会重新变成 unknown/missing，不沿用昨日凭证。

缺失、错配、歧义或损坏统一报告 `RESEARCH_IDENTITY_EVIDENCE_MISSING_OR_INVALID`；
证据已合格但未冻结到快照报告 `RESEARCH_IDENTITY_SNAPSHOT_MISSING`；引用容量耗尽报告
`research_identity_evidence_capacity_exhausted`。检查只读已持久化原件，不采集新的 SEC 来源，
不写数据库、不调用模型、不创建周期或订单。原 `ready_static` 保持原静态输入语义；身份、
宏观和趋势分别判读，不将这些字段冒充全部研究内容、财务修订审查或交易许可通过。
