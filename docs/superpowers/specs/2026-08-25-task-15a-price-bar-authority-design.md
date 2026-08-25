# Task 15A：Agent 行情 Bar 集合证据设计规格

- 日期：2026-08-25
- 状态：待用户书面审阅
- 适用范围：Task 15 只读 Agent 的 `get_price_features` 与行情证据冻结
- 前置提交：`42a8610 fix(engine): freeze auditable price features`
- 后续门禁：本规格实现并通过独立审查前，不启动 Task 16

## 1. 结论

Task 15A 使用“数据库规范化 Bar 集合证据”方案：PostgreSQL 对实际写入 `price_bar_records`、并将被确定性指标工具读取的完整集合生成版本化 SHA-256；独立的不可变 `price_bar_set_authorities` 记录把该摘要、来源政策、集合边界和 `ResearchEvidenceRef` 绑定在一起。

Parquet 工件继续证明归档文件，不能再替代 Agent 计算输入的证据。Agent 数据快照只冻结 Bar 集合证据；`get_price_features` 只从该证据拥有的不可变 Bar 集合计算指标。

该设计同时关闭以下四个已确认问题：

1. Parquet 哈希与数据库计算输入不是同一对象。
2. 应用层来源筛选可被预建 evidence link 绕过。
3. Bar 写入与集合冻结之间存在并发竞态。
4. 带质量标记的 Bar 仍可能产生 `status=ok`。

## 2. 范围与非目标

### 2.1 本次包含

- 为 Agent 日线计算建立不可变、内容寻址的数据库 Bar 集合证据。
- 在数据库和运行时双重执行 Agent 价格来源政策。
- 使用同一 manifest 级事务锁串行化 Bar 写入和 Bar 集合最终化。
- 任何非空 `quality_flags` 均不能进入 Agent 价格权威集合。
- 保持 close、SMA20、SMA200、RSI14、20 日波动率的现有确定性计算接口。
- 为迁移、并发、来源绕过、质量降级、时点正确性和特征计算增加测试。

### 2.2 本次不包含

- 不增加或修改 Web 页面、BFF API、OpenAPI Schema 或研究响应字段。
- 不从对象存储下载 Parquet 后在 Agent 进程中计算。
- 不新增任意指标、策略参数、回测逻辑或 Task 16 代码。
- 不启用复星真实账户、复星行情、模拟账户或实盘下单。
- 不改变 Agent 只读工具白名单、硬风控、预算或人工审批边界。
- 不迁移历史生产数据；当前 `0011_agents` 尚未发布，因此在该迁移内收敛设计。

## 3. 已否决方案

### 3.1 Agent 直接读取 Parquet

该方案能让文件哈希直接覆盖输入，但会把对象存储网络、凭据、超时和对象读取能力带入 Agent 工具运行域，扩大权限与故障面，不采用。

### 3.2 入库时只保存预计算指标

该方案查询简单，但会把特征版本与入库耦合，弱化原始 Bar 到指标的可复算性，并妨碍后续受控增加指标，不采用。

## 4. 信任边界

```text
Provider 原始响应
  -> 原始归档 + Parquet DataArtifact（文件证据）
  -> PriceSeriesManifest + PriceBarRecord（数据库规范化事实）
  -> PostgreSQL 规范化、来源/质量/PIT 校验和 SHA-256
  -> PriceBarSetAuthority + ResearchEvidenceRef（计算证据）
  -> AgentDataSnapshotEvidenceRecord（研究快照冻结）
  -> get_price_features（确定性只读计算）
  -> Agent 只消费结构化结果
```

权限原则：

- Provider/ingestion 可以写入未授权行情事实，但不能自行声明其可供 Agent 使用。
- PostgreSQL 是 Bar 集合是否完整、清洁、来源合格和已冻结的最终裁决者。
- Agent 运行时不能写 Bar、创建证据、修改政策或访问对象存储。
- `get_price_features` 不能使用未被当前研究快照冻结的 manifest。
- LLM 不能选择来源、修改计算字段、提交 SQL 或影响摘要算法。

## 5. 数据模型

### 5.1 保留现有 Parquet 证据

`PriceSeriesManifest.evidence_ref_id` 继续引用现有 Parquet `ResearchEvidenceRef`：

- `provider_object_id = price-series-manifest:{manifest_id}`
- `revision = DataArtifact.sha256`
- `location = DataArtifact.uri`
- `content_hash = DataArtifact.sha256`
- `byte_length = DataArtifact.byte_size`

该引用服务于原始/规范化文件追溯、诊断和重放，不进入 Agent 的 `authority_kind=price` 快照绑定。

### 5.2 新增 `price_bar_set_authorities`

新增 ORM `PriceBarSetAuthority`，表字段固定为：

| 字段 | 类型 | 约束与语义 |
| --- | --- | --- |
| `id` | UUID | 主键，UUIDv7 |
| `price_series_manifest_id` | UUID | FK；唯一；一个 manifest 最多最终化一次 |
| `evidence_ref_id` | UUID | FK；唯一；引用 Bar 集合证据 |
| `source_policy_id` | UUID | FK；必须是不可变 `source_policy_v1` 的固定 ID |
| `source` | varchar(100) | 必须等于 manifest 与 evidence source |
| `provider_object_id` | varchar(255) | 固定为 `price-bar-set:{manifest_id}` |
| `revision` | varchar(255) | 必须等于 `content_hash` |
| `hash_version` | varchar(64) | 固定为 `price_bar_set_sha256_v1` |
| `policy_version` | varchar(64) | 固定为 `agent_price_authority_v1` |
| `content_hash` | char(64) | 小写十六进制 SHA-256 |
| `byte_length` | bigint | 规范化 UTF-8 字节长度，必须大于 0 |
| `bar_count` | integer | 必须与 manifest 和数据库实际行数一致，范围 1..10000 |
| `first_session` | date | 实际最早 session |
| `last_session` | date | 实际最晚 session，且不早于 first |
| `available_at` | timestamptz | 集合内最大 `received_at`，表示系统最早可使用时点 |
| `created_at` | timestamptz | 数据库生成，不可修改 |

表必须使用复合外键把 `evidence_ref_id/source/provider_object_id/revision` 绑定到同一个 `ResearchEvidenceRef` owner identity。UPDATE 和 DELETE 一律被不可变触发器拒绝。

### 5.3 Bar 集合 EvidenceRef

Bar 集合证据格式固定为：

- `source = manifest.source`
- `provider_object_id = price-bar-set:{manifest_id}`
- `revision = content_hash`
- `available_at = max(price_bar_records.received_at)`
- `location = db://price-bar-set/{manifest_id}@sha256:{content_hash}`
- `content_hash = price_bar_set_authorities.content_hash`
- `byte_length = price_bar_set_authorities.byte_length`

`research_evidence_refs.location_content_addressed` 增加且仅增加上述 `db://price-bar-set/…@sha256:` 形式。任意普通 `db://`、无摘要位置或不匹配摘要的位置仍被拒绝。

## 6. 规范化摘要协议

### 6.1 唯一实现者

摘要仅由 PostgreSQL 17 + `pgcrypto.digest(..., 'sha256')` 实现。Python 不重写规范化或摘要算法，只消费数据库返回的摘要结果。

迁移执行 `CREATE EXTENSION IF NOT EXISTS pgcrypto`。降级不删除 `pgcrypto`，因为扩展可能被同一数据库的其他模块共享；降级只删除 Task 15A 自有函数、触发器和表。

### 6.2 规范化内容

`price_bar_set_sha256_v1` 的 UTF-8 内容是 PostgreSQL `jsonb` 的确定性文本表示，顶层包含：

```json
{
  "schema": "price_bar_set_sha256_v1",
  "manifest": {
    "instrument": "UUID string",
    "source": "alpaca|fixture",
    "feed": "string",
    "delivery_mode": "delayed|fixture",
    "adjustment": "string",
    "interval": "1Day"
  },
  "bars": [
    [
      "018f75b4-4d86-7c36-bc4c-cd71f2f64520",
      "2026-08-24",
      101.000000000000,
      103.000000000000,
      100.000000000000,
      102.000000000000,
      1200000.000000000000,
      "2026-08-24T20:00:00.000000Z",
      "2026-08-24T20:15:00.000000Z",
      "2026-08-24T20:15:01.000000Z",
      "1",
      []
    ]
  ]
}
```

具体规则：

- JSON 由 `jsonb_build_object/jsonb_build_array/jsonb_agg` 生成，不由字符串拼接生成。
- `NUMERIC(38,12)` 直接作为 JSON number 编码，不经过 binary float。
- 时间统一通过 `to_char(value AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')` 编码为固定 6 位微秒的 JSON string。
- `quality_flags` 在编码前按字符串升序排列；授权集合要求数组为空，但字段仍被摘要覆盖。
- Bar 按 `session, observed_at, revision, instrument_id` 升序聚合。
- 授权集合必须满足一个 session 恰好一条 Bar；禁止同一 session 的多 revision 混入一个权威集合。
- 随机数据库行 ID、manifest ID、authority ID 和 `created_at` 不进入内容摘要。

数据库提供两个职责分离的函数：

```text
nexus_price_bar_set_canonical_v1(manifest_id uuid) -> canonical_text text
```

该函数只负责生成第 6.2 节定义的内容，供摘要实现和黄金测试共用；它不声明集合可被 Agent 使用。

准备函数返回：

```text
nexus_prepare_price_bar_set(manifest_id uuid)
  -> content_hash text
  -> byte_length bigint
  -> bar_count integer
  -> first_session date
  -> last_session date
  -> available_at timestamptz
```

函数先获取第 8 节定义的事务锁。只有来源、质量、PIT、数量和集合一致性全部通过时才返回一行；不合格候选返回零行，不创建证据。

`BEFORE INSERT` authority 触发器必须在同一事务内再次计算上述结果，并逐字段验证待插入记录及 EvidenceRef。调用方不能通过伪造摘要、行数、位置或 EvidenceRef 绕过数据库裁决。

摘要版本升级必须新增 `price_bar_set_sha256_v2` 与新 policy/version 路径，不能静默改变 v1 函数输出。PostgreSQL 升级前必须运行黄金摘要测试。

## 7. 来源与质量政策

### 7.1 数据库允许的候选

`agent_price_authority_v1` 只接受：

| 用途 | source | feed | delivery_mode | interval |
| --- | --- | --- | --- | --- |
| 生产候选 | `alpaca` | `sip` | `delayed` | `1Day` |
| 测试候选 | `fixture` | 非空 | `fixture` | `1Day` |

同时必须满足：

- `source_policy_id` 等于 `source_policy_v1` 固定 UUID，且数据库中该不可变记录的 key/version/schema/payload 与代码常量完全一致。
- manifest instrument 能解析为 UUID，且每条 Bar 的 `instrument_id` 全部与其一致。
- manifest `row_count` 等于实际行数，范围 1..10000。
- manifest `start_at/end_at` 等于实际 `observed_at` 最小/最大值。
- 每条 Bar 满足 `observed_at <= available_at <= received_at`。
- 每条 Bar 的 `quality_flags` 必须是 JSON array 且必须等于 `[]`。
- 每个 session 恰好一条 Bar。

`adjustment` 和 `feed` 被摘要覆盖；本次不新增超出现有 Provider 契约的 adjustment 枚举。后续如果策略只允许某种 adjustment，应通过新的 policy version 引入，不能修改 v1。

### 7.2 运行时政策

数据库允许保存 fixture authority 供测试使用，但生产/Responses 模式绝不能选择它。

`freeze_price_authority(..., allow_fixture: bool = False)`：

- 默认只允许 `alpaca/sip/delayed/1Day`。
- 只有组合根显式传入 `allow_fixture=True` 时才允许 fixture；API 只在 `AgentRuntimeMode.FIXTURE` 时传入 True。
- Responses 模式即使运行在 development，也传入 False。
- existing-link fast path 必须重新加载 authority、manifest、EvidenceRef 和 policy，执行同一运行时校验；不能看到 link 就直接返回。
- 已存在但无效或当前模式不允许的 price link 视为审计完整性错误，研究 admission 失败且不创建 run/job/budget reservation。
- 没有任何合格候选不是完整性错误：不创建 price link，研究可继续，但价格工具只能返回 unavailable，市场状态角色必须 abstain，综合结论不得把缺失价格证据解释为正向信号。

### 7.3 复星后续接入

现有平台仍坚持“复星合格数据优先”。Task 15A 不把复星加入 `agent_price_authority_v1`，原因是当前实现尚未证明复星美股日线的不可变对象身份、修订语义、调整方式和本地 Bar 集合证据完整性。

复星加入生产权威必须单独满足：

- `fosun_get_bars` 只读 capability 已验收且未过期；
- 市场范围为 US；
- revision、adjustment、available/received time 与分页完整性契约已验证；
- 增加新的 `agent_price_authority_v2`，数据库和运行时同时升级；
- Alpaca 继续作为补缺或交叉验证，而不是无审计地字段级混合。

## 8. 并发与不可变性

### 8.1 唯一锁键

所有相关事务使用同一锁语句：

```sql
SELECT pg_advisory_xact_lock(
  hashtextextended('price-series-manifest:' || manifest_id::text, 0)
);
```

锁只覆盖单个 manifest，不使用表锁或全局锁。

### 8.2 写入规则

`price_bar_records` 的 `BEFORE INSERT OR UPDATE OR DELETE` guard：

1. 根据 NEW/OLD 解析 manifest ID。
2. 获取上述事务锁。
3. INSERT 时如果已有 `price_bar_set_authorities` 立即拒绝。
4. UPDATE/DELETE 继续由既有 append-only 规则拒绝；guard 仍先参与同一串行化协议。

`price_bar_set_authorities` 的 `BEFORE INSERT` guard：

1. 获取相同事务锁。
2. 重算候选摘要及边界。
3. 验证 source policy、quality、EvidenceRef 和所有派生字段。
4. 通过后插入；唯一约束保证只能最终化一次。

结果只有两种：

- Bar 事务先持锁：最终化必须看到并摘要该 Bar。
- 最终化事务先持锁并提交：后续 Bar 写入看到 authority 后被拒绝。

不存在“摘要完成但集合继续增长”的第三种状态。

### 8.3 Agent 快照绑定

`agent_data_snapshot_evidence` 的 price owner trigger 改为从 `PriceBarSetAuthority` 反查 owner，不再从 `PriceSeriesManifest.evidence_ref_id` 证明价格计算权威。触发器必须验证：

- authority、manifest、EvidenceRef 和 source policy 全部存在且一致；
- snapshot/instrument 与 manifest/Bar instrument 一致；
- `authority.available_at <= selected_at`，且 `selected_at` 不晚于 snapshot `available_at`；admission 代码另行保证 snapshot 和 evidence 均不晚于 run `decision_time`；
- evidence location/hash/revision/byte_length 与 authority 精确一致；
- 数据库来源规则成立。

快照 link 插入后仍保持 UPDATE/DELETE 禁止，且每个 snapshot/instrument 最多一个 price authority。

## 9. 入库与最终化流程

`ingest_price_page` 保持原始归档、Parquet 发布和数据库写入的单事务语义，流程调整为：

1. 归档原始响应。
2. 写临时 Parquet 并原子 promote。
3. 插入 `DataArtifact`。
4. 插入 Parquet `ResearchEvidenceRef`。
5. 插入 `PriceSeriesManifest`。
6. 插入全部 `PriceBarRecord` 并 flush。
7. 调用数据库 `nexus_prepare_price_bar_set(manifest_id)`。
8. 若返回一行，插入 Bar 集合 `ResearchEvidenceRef` 和 `PriceBarSetAuthority`；数据库触发器重验。
9. 若返回零行，仍提交原始工件、manifest 和 Bar，但不产生 Agent 价格权威。
10. 事务提交后返回结果；失败时回滚数据库并清理临时对象。

`MarketIngestionResult.evidence_ref` 继续表示 Parquet evidence，以保持现有消费者兼容；新增：

```python
price_authority: PriceBarSetAuthority | None
price_evidence_ref: ResearchEvidenceRef | None
```

被质量标记或来源不合格的数据仍可用于诊断和人工检查，但不能进入 Agent 决策证据。

## 10. `get_price_features` 行为

### 10.1 权威查询

工具查询链改为：

```text
AgentDataSnapshotEvidenceRecord
  -> ResearchEvidenceRef
  -> PriceBarSetAuthority
  -> PriceSeriesManifest
  -> PriceBarRecord
```

工具必须验证快照冻结字段与当前 authority/evidence 完全一致。任何 owner、hash、location、source、policy、instrument 或时点不一致都抛出受控 `ToolInputError`，由现有 durable executor 终止为 fail-closed 状态。

### 10.2 计算集合

- 只读取 authority 对应 manifest 的 Bar。
- authority 保证每 session 一行、无 flag、不可再修改。
- 按 `session DESC, observed_at DESC` 取最近 200 行，再按时间升序交给现有 Decimal 特征函数。
- 工具在读路径检查实际 manifest 行数/边界以及所读 Bar 无 flag；发现不一致返回 `unavailable`，reason 为稳定内部枚举，不向响应泄漏 SQL 或数据库细节。
- close、SMA20、SMA200、RSI14、volatility20 计算公式和允许字段不在 Task 15A 中改变。

### 10.3 状态语义

| 条件 | status | reason |
| --- | --- | --- |
| 无冻结 price authority | `unavailable` | `no_frozen_price_authority` |
| 集合完整但少于某指标 warm-up | `partial` | `insufficient_price_history` |
| owner/digest/数量/边界损坏 | `unavailable` | `invalid_price_bar_set_authority` |
| 出现非空 quality flags | `unavailable` | `flagged_price_bars` |
| 所有请求指标可计算 | `ok` | null |

任何 flagged 或 authority 损坏场景都不能返回 `ok`。响应仍只包含已有结构化字段，不增加数据库位置、账户信息、总资产或密钥。

## 11. 错误处理与可恢复性

- 来源或质量不合格：保留数据，不建立 Agent authority；等待新一批合格数据。
- manifest 已最终化后重放相同页面：现有对象/业务幂等路径处理；不得向已冻结集合追加行。
- authority EvidenceRef 插入后 authority 插入失败：同一事务整体回滚，不留下孤立 Bar 集合 EvidenceRef。
- 对象存储 promote 成功但数据库失败：沿用当前原始/临时对象恢复策略；Task 15A 不扩大对象删除权限。
- 无有效价格 authority：研究可基于其他证据继续，但价格角色 abstain，不能生成依赖价格的确定性 target。
- 已有无效 price link：视为审计损坏，admission 失败；不能静默忽略后再附加第二条 link。
- 所有外部错误和数据库错误经现有安全错误映射输出，不记录密钥、完整 SQL 或真实账户数据。

## 12. 迁移策略

Task 15A 修改尚未发布的 `engine/alembic/versions/0011_agents.py`，不创建 `0012`，以保留原计划中 Task 16 使用 `0012_backtests` 的修订序列。

升级顺序：

1. `CREATE EXTENSION IF NOT EXISTS pgcrypto`。
2. 扩展 `research_evidence_refs.location_content_addressed`。
3. 创建 `price_bar_set_authorities` 及索引/约束。
4. 创建规范化、准备、校验和锁函数。
5. 替换旧的 frozen-price-bar 与 price-owner 触发器。
6. 为 authority 和 Bar 建立 append-only/guard 触发器。

降级顺序必须严格反向：先删除依赖触发器和函数，再删除 authority 表和位置约束修订，最后恢复 0010 的 ResearchEvidenceRef 位置约束及 0011 之前的 manifest 结构。`pgcrypto` 不由降级删除。

迁移测试必须覆盖：

- 空库升级到 head。
- `0010_research_data -> 0011_agents`。
- `0011_agents -> 0010_research_data -> 0011_agents`。
- 降级后无 Task 15A 自有表、函数和触发器残留。
- shared `pgcrypto` 仍存在不视为残留错误。
- 不手工修改已经生成并发布的旧迁移；本次只修改未发布 0011。

## 13. 测试设计

### 13.1 摘要黄金测试

- 固定 manifest/Bar fixture 产生固定 v1 SHA-256 和 byte length。
- 分别改变 instrument、session、OHLCV 每一列、三个时间、revision、flag、source、feed、delivery mode、adjustment、interval，摘要必须改变。
- 改变 Bar 插入顺序、随机 row ID、authority ID 或 created_at，摘要必须不变。
- 相同 NUMERIC 值的合法输入表示归一后摘要一致；binary float 仍在 Provider 边界被拒绝。

### 13.2 数据库政策测试

- 合格 Alpaca SIP delayed 1Day 集合可最终化。
- fixture 可最终化但只有显式 fixture runtime 可冻结到 Agent snapshot。
- Alpaca 非 SIP、非 delayed、非 1Day 和任意其他 source 被数据库拒绝建立 authority。
- 伪造 source policy ID/payload、EvidenceRef owner、location、hash、byte length、行数或边界均失败。
- 非空 flag、重复 session、instrument 混用、manifest count/time 不一致均返回非候选或被 authority trigger 拒绝。
- 直接 SQL 预建非批准 evidence/link 不能绕过 owner trigger。

### 13.3 两事务并发测试

使用两个独立 PostgreSQL connection 和可控 barrier：

- 事务 A 插 Bar 持锁，事务 B 最终化等待；A 提交后 B 的摘要包含新 Bar。
- 事务 A 最终化持锁，事务 B 插 Bar 等待；A 提交后 B 以约束错误失败。
- 失败后两连接均可继续使用，测试不能依赖 sleep 推断时序。

### 13.4 工具与 Agent 测试

- 正常集合精确返回 Decimal close/SMA/RSI/volatility。
- 少于 20/200 行保持现有 partial/warm-up 行为。
- 无 authority 返回 unavailable。
- flagged、owner 损坏或不匹配集合不能返回 ok。
- production/Responses 模式拒绝 fixture existing link；fixture 模式接受合法 fixture。
- Agent 证据引用指向 `db://price-bar-set/...@sha256:...`，而不是 Parquet URI。
- 研究输出继续绑定全部冻结 evidence；无价格时市场状态角色 abstain。

### 13.5 回归门禁

实现完成至少运行：

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_market_ingestion.py -q
uv run --package nexus-engine pytest engine/tests/integration/test_agent_execution_pg.py -q
uv run --package nexus-engine pytest engine/tests/unit/agents -q
uv run --package nexus-engine pytest engine/tests/evals/test_research_committee.py -q
uv run --package nexus-engine pytest engine/tests/evals/test_research_committee.py -q
uv run --package nexus-engine pytest engine/tests/helpers engine/tests/integration -q
uv run --package nexus-engine pytest engine/tests -q
uv run --package nexus-engine ruff check engine
uv run --package nexus-engine mypy engine/src
npm test
```

若 Task 15A 未修改 Web 代码，Web build/lint 沿用 Task 15 最近一次绿色证据；最终独立审查可要求补跑。所有 Agent eval fixture 必须连续运行两次并保持结构化输出一致。

## 14. 文件边界

预期只修改或新增以下职责明确的文件；实施 Plan 必须逐一核对实际行号：

- `engine/src/nexus/persistence/models/data.py`：`PriceBarSetAuthority` ORM。
- `engine/alembic/versions/0011_agents.py`：表、函数、触发器、扩展和降级。
- `engine/src/nexus/market/price_bar_authority.py`：调用数据库 prepare、构建 EvidenceRef/authority 的小型服务。
- `engine/src/nexus/market/ingestion.py`：在 Bar flush 后调用最终化服务并扩展返回值。
- `engine/src/nexus/agents/price_authority.py`：只选择合法 BarSetAuthority，重验 existing link，执行 runtime fixture 门禁。
- `engine/src/nexus/agents/persistence.py` 与 `engine/src/nexus/api/routes/research.py`：显式传递 fixture 使用权限。
- `engine/src/nexus/agents/database_tools.py`：从 BarSetAuthority 读取并防御性降级。
- 对应 migration、market ingestion、Agent PostgreSQL integration 与 unit tests。

不进行无关重构，不修改 Web、复星网关、交易、回测或部署配置。

## 15. 验收标准

Task 15A 只有同时满足以下条件才可接受：

1. Agent price evidence 的 SHA-256 覆盖工具所读取 manifest 的完整规范化 Bar 集合和来源元数据。
2. Parquet SHA 与 BarSet SHA 在模型和查询中语义分离。
3. 应用 fast path、正常 selector 和数据库 trigger 执行一致的来源边界。
4. production/Responses 模式不能使用 fixture authority。
5. Bar 最终化与后续写入不存在可复现 TOCTOU。
6. 非空 quality flags 永远不能产生 `status=ok`。
7. 无价格证据时系统观望/abstain，而不是伪造价格或阻止其他独立研究证据被读取。
8. 迁移可升级、降级、重升级，且不破坏共享扩展。
9. 全量 Engine 测试、静态检查、双次 eval 和相关 Web 回归全部通过。
10. 新的独立审查确认四个 Task 15 Round 5 finding 全部关闭，且没有新增 Critical/Important。

Task 15A 未达到上述标准时，Task 15 保持未接受，Task 16 不得开始。
