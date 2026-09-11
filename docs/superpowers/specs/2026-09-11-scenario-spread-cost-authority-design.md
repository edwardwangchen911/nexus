# NEXUS 情景点差成本权威设计

**状态：** 待用户审阅；通过后进入实施计划

**基线：** `main@c65a091c`

**目标：** 在不调用行情 Provider、不修改旧快照、不解除交易锁的前提下，让一个在首次选择时已提交、且事件/可用/接收时间与组合估值时点匹配的 Alpaca IEX 冻结盘口，为只读调整情景提供可重复的点差成本估算。当前真实快照没有合格盘口时继续返回 unknown。本切片不声称数据库事实必然在估值时点前已提交；若未来需要这种审计语义，必须另增提交或 ingestion watermark，不能由 `created_at` 倒推。

## 1. 结论与范围

本设计是现有 `POST /internal/v1/portfolio-scenarios/evaluate` 的受控扩展，不新建优化器、行情服务或候选持久化系统。

它是 `2026-09-10-portfolio-scenario-decision-design.md` 阶段 A 完成后的成本证据增量。旧文档中“阶段 A 不迁移、点差 unknown”的条款继续描述历史交付；仅对采用 v3 幂等权威的新请求，由本文覆盖。旧 v2 请求、结果哈希和 Agent 恢复身份不得重算。

只实施以下能力：

1. 发布一条不可变的 `portfolio_scenario_cost_analysis` 来源政策；
2. 从已持久化 `latest_quotes` 读取与组合快照估值时点同步的 Alpaca IEX bid/ask；
3. 对情景实际减持权重计算单边半点差成本；
4. 用首次幂等请求冻结政策和报价身份，重放不得重新选择新数据；
5. 在现有成本表中显示已知点差或精确 unknown，其他成本与执行锁不变。

明确不实施：

- 页面点击时启动 Alpaca WebSocket、复星刷新或任何 Provider 调用；
- 用请求时新报价给旧组合快照重新估值；
- 复用 `display_only_intraday_monitor` 政策；
- 复星美股盘口准入；当前没有相应权限和真实 bid/ask；
- 盘前、盘后或隔夜点差；首版仅限 XNYS 常规交易时段；
- 佣金、滑点、监管费转嫁、税务或成本合计已知化；
- 新增候选表、报价束表、通用成本框架、Shadow、订单或执行能力。

因此，即使点差已知，结果仍为 `analysis_status=partial`、`execution_eligible=false`、`execution_locked=true`，候选 `estimated_cost` 和不含税合计仍为 unknown。

## 2. 方案比较

### 方案 A：专用消费政策 + 现有报价表 + 幂等证据清单（采用）

新增一条政策记录；首次请求从现有不可变报价中选择证据，并把政策 ID、报价 ID 与哈希写入现有幂等记录的内部证据清单。重放只按清单读取，不重新选择。

优点：不新增业务表；用途、时点和重放边界完整；改动集中在迁移、成本证据加载和情景 API。缺点：只有先完成同步行情采集的快照才会得到已知点差。

### 方案 B：持久化完整 `ScenarioValuationBundle`（暂缓）

新增估值束、成员和引用表，把持仓、现金、价格与报价统一冻结。

优点：适合后续保存候选和 Shadow。缺点：当前没有查询、选择或生命周期需求，会扩大迁移、隐私和回滚范围，不符合最短可用路线。

### 方案 C：请求时拉取行情或复用展示政策（拒绝）

它会让页面产生外部调用，或把仅展示证据提升为决策证据；还可能用新盘口倒填旧快照，违反现有产品和审计边界。

## 3. 时间语义

本设计不新增第二个估值时点。唯一估值时点仍是：

```text
valuation_time = portfolio_snapshot.as_of = diagnostic_report.snapshot_as_of = request.as_of
```

报价必须同时满足：

```text
quote.observed_at <= valuation_time
quote.available_at <= valuation_time
quote.received_at <= valuation_time
artifact.observed_at <= valuation_time
artifact.available_at <= valuation_time
artifact.received_at <= valuation_time
valuation_time - quote.observed_at <= 45 seconds
```

此外，报价、原件和政策的 `created_at` 必须不晚于 `valuation_time`。该条件只证明**插入事务起点**不晚于估值时点；它可以拒绝估值后才启动的普通补录事务，但不能证明记录已经在估值前提交，也不能排除估值前启动、估值后提交的长事务。产品文案、API 和审计均不得把它表述为“估值时已落库”或“首次请求的知识截止”。

首次 owner 在生成无成本候选后，用**一条 SQL 选择语句**同时读取固定政策和各减持标的的合格报价。PostgreSQL `READ COMMITTED` 的该语句级 MVCC snapshot 是唯一的首次可见性边界：只允许语句开始时已提交且满足估值时间约束的记录。语句返回 `statement_timestamp()` 作为 `evidence_selected_at`，并返回每个被选记录的固定身份与哈希；它不作为事件时间，也不单独证明可见性。选择结束后只认清单，不再执行第二次“找最新”查询。并发事务即使拥有较早的 `created_at`，若在该选择语句开始时尚未提交，也不得进入本次结果。

真正的重放稳定性由冻结清单、预期结果哈希和全部计算依赖版本共同保证；重放只按固定 ID 读取并核验，不重新执行时段判定或最新报价选择。

若 `valuation_time` 不在对应 XNYS 常规交易时段内，点差保持 unknown。不得把请求到达时间、Gateway 接收时间、数据库 `created_at` 或页面时间解释成行情事件时间。

## 4. 专用来源政策

新增 Alembic 数据迁移 `0050_scenario_spread_cost_policy`，只向现有 `source_policies` 插入一行，不新增表或列。

固定合同：

```json
{
  "id": "7733d99e-1594-5f1e-87ea-e5cc23575159",
  "key": "portfolio_scenario_cost_analysis",
  "version": "scenario_spread_cost_policy_v1",
  "schema_version": "1",
  "payload": {
    "purpose": "portfolio_scenario_cost_analysis",
    "selection": "latest_qualified_before_valuation_cutoff",
    "fallback": "disabled",
    "session_scope": "xnys_regular",
    "max_quote_age_seconds": 45,
    "calculation": "sell_half_spread_from_snapshot_mark_v1",
    "calendar_authority": {
      "library": "exchange_calendars",
      "library_version": "4.13.2",
      "calendar": "XNYS",
      "session_window_contract": "xnys_regular_open_closed_v1"
    },
    "require_verified_evidence": true,
    "require_empty_quality_flags": true,
    "alpaca": {
      "feed": "iex",
      "delivery_mode": "real_time",
      "transport": "websocket",
      "artifact_schema": "alpaca_stock_stream_v1",
      "market_scope": "US",
      "event_kind": "quote"
    }
  }
}
```

政策加载必须核对固定 ID、key、version、schema 和完整 payload。`engine/pyproject.toml` 将 `exchange-calendars` 固定为 `==4.13.2`（当前 lock 已是该版本），运行时还要核对实际包版本。任何政策缺失、漂移、运行时日历版本不符，或政策 `created_at` 晚于 `valuation_time`，均返回“政策不可用”，不得退回展示政策。这样新政策不会追认迁移前的旧组合快照。未来更换日历版本、交易时段边界或开闭区间语义时，必须发布新政策版本并升级幂等 namespace，不能在 v1 内静默替换。

首次选择语句把每个合格报价对应的 XNYS session date、开盘 UTC、收盘 UTC 和上述日历权威 payload hash 一并返回并冻结。重放核对这些冻结值，不再调用可变化的日历库重新判断；缺失、版本冲突或 session window 冲突均失败关闭。

降级只允许精确删除该固定记录。若任何 `portfolio_scenario_v3` 幂等证据清单已经引用该政策，降级必须拒绝；不得级联删除历史结果或改写清单。

## 5. 报价证据准入

新增窄模块 `engine/src/nexus/optimizer/scenario_spread_cost.py`，只负责政策加载、报价选择、冻结清单复读和点差计算。它不访问 Provider，不修改报价。

每个候选报价必须满足：

- `source=alpaca`、`feed=iex`、`delivery_mode=real_time`、`event_kind=quote`；
- `evidence_status=verified`；
- bid、ask 均为有限正数且 `bid <= ask`；
- `quality_flags=[]`，任何标记都失败关闭；
- `data_artifact_id` 非空；
- `DataArtifact.source=alpaca`、`schema_version=alpaca_stock_stream_v1`、`mime_type=application/json`；
- `LatestQuote.revision = DataArtifact.sha256`；
- 使用既有 `canonical_quote_observation()` 从数据库行的 instrument、source/feed/delivery mode/event kind、bid/ask/last、三个业务时点、revision、排序后的 quality flags **重新生成规范化 bytes 和 SHA-256**，结果必须同时等于行内 `content_hash` 和 manifest 中的 quote hash；禁止只比较两个已存 hash 字段；
- 通过既有本地 object store reader 读取 `DataArtifact.uri` 的实际 bytes，核对 content-addressed URI、`byte_size` 与重新计算的 SHA-256，再按 `alpaca_stock_stream_v1` 解析并核对本次 quote 的 symbol、bid、ask、event time 与归一化行；禁止只比较 `DataArtifact.sha256` 数据库字段；
- 报价和原件时间满足第 3 节；
- 报价和原件的 `created_at` 均不晚于 `valuation_time`；
- 标的身份属于情景已经核验的 USD 标准美股集合。

首次选择在第 3 节的一条 SQL/MVCC snapshot 内按每个 instrument 独立排序：

```text
observed_at DESC,
available_at DESC,
received_at DESC,
id DESC
```

不跨来源拼接 bid/ask，不使用 trade/last 推导盘口，不使用 fixture、delayed SIP、SIP 或复星记录兜底。

## 6. 幂等证据清单

复用 `idempotency_records`，不新增表。情景命名空间和 claim schema 升级为：

```text
/internal/v1/portfolio-scenarios/evaluate:portfolio_scenario_evaluation_v3
portfolio_scenario_v3
```

这只升级幂等权威版本，不修改公开请求和 `portfolio_scenario_evaluation_v2` 响应结构。旧 v2 claim 保持只读，不回填、不迁移。

首次 owner 在 `response_payload` 仅保存内部清单：

```json
{
  "manifest_schema": "scenario_spread_evidence_manifest_v1",
  "evidence_selected_at": "2026-09-11T01:00:00.000000Z",
  "policy_status": "available",
  "source_policy_id": "7733d99e-1594-5f1e-87ea-e5cc23575159",
  "source_policy_version": "scenario_spread_cost_policy_v1",
  "source_policy_payload_hash": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
  "calendar_authority": {
    "library": "exchange_calendars",
    "library_version": "4.13.2",
    "calendar": "XNYS",
    "session_window_contract": "xnys_regular_open_closed_v1",
    "authority_hash": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
    "session_date": "2026-09-10",
    "session_open": "2026-09-10T13:30:00.000000Z",
    "session_close": "2026-09-10T20:00:00.000000Z"
  },
  "calculation_version": "scenario_spread_cost_v1",
  "no_cost_candidate_hash": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
  "scenario_cost_basis_hash": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
  "regulatory_reference_hash": null,
  "expected_result_hash": "1111111111111111111111111111111111111111111111111111111111111111",
  "selected_quotes": [
    {
      "instrument_id": "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e701",
      "latest_quote_id": "019d0000-0000-7000-8000-000000000101",
      "content_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "artifact_id": "019d0000-0000-7000-8000-000000000201",
      "artifact_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    }
  ]
}
```

`policy_status` 只允许 `available` 或 `unavailable`。`available` 必须携带上述固定政策 ID/version/payload hash、日历权威与 session window；`unavailable` 必须把这些政策与日历字段设为 null。`selected_quotes` 只包含情景实际减持标的；若某个减持标的没有合格报价，则清单仍记录其他已选事实，但点差整体为 unknown。若政策不可用，报价数组必须为空，重放不得采用后来发布的政策。

`no_cost_candidate_hash` 精确覆盖调用成本模块之前的规范化 `PortfolioScenarioEvaluationV2` 候选内容（成本字段保持阶段 A 原始 unknown，不含 `result_hash`）；`scenario_cost_basis_hash_v1` 覆盖 `portfolio_snapshot_id`、`valuation_time`、组合 USD 总值，以及按 instrument 排序的 `{instrument_id, snapshot_current_price, current_weight, target_weight}`。它必须包含每个实际减持标的，不能用现有 `broker_position_hash_v1` 代替，因为后者不含 `current_price`。清单不直接保存这些敏感数值，只保存该规范化输入的哈希。`regulatory_reference_hash` 明确保存本次 bundle 实际使用的公开监管参考哈希；未使用时必须为 null。`expected_result_hash` 保存首次规范化最终响应的结果哈希。

同 key 重放必须：

1. 核对 request hash 与 claim schema；
2. 解析并严格校验清单；
3. 按清单精确加载政策与报价 ID；
4. 重新核对所有固定身份和哈希；
5. 核对 calculation version、calendar authority/session window、无成本候选哈希、成本基线哈希和监管参考哈希；
6. 缺失或冲突时失败关闭，不重新选择替代报价；
7. 使用同一清单重算，并与 `expected_result_hash` 比较；不一致即 409，不能返回漂移结果。

首次 owner 先生成不含 `expected_result_hash` 的证据输入，计算成本 bundle 和最终响应，再把最终 hash 填入持久化清单并计算完整 `scenario_evidence_manifest_hash`；最终响应的成本 bundle 不包含 manifest hash 或 expected result hash，因此不存在循环哈希。claim 清单与审计事件在同一数据库事务提交。任何异常全部回滚，避免空 claim 成为半成品。

## 7. 点差计算

阶段 A 的候选仍由现有确定性引擎先生成。对每个 `target_weight < current_weight` 的标的：

```text
sold_weight = current_weight - target_weight
midpoint = (bid + ask) / 2
sold_value_at_snapshot_mark = sold_weight * portfolio_value
sold_quantity = sold_value_at_snapshot_mark / snapshot_current_price
instrument_spread_cost = sold_quantity * (midpoint - bid) / portfolio_value
                       = sold_weight * (midpoint - bid) / snapshot_current_price
portfolio_spread_cost = sum(instrument_spread_cost)
```

这是按冻结快照标记价格换算卖出数量、再计入中价到 bid 的单边半点差估算，不把快照标记价与盘口中价强行视为相等，也不是成交保证、滑点或费用合计。`snapshot_current_price` 已由现有情景基线校验为有限正数。计算使用现有 Decimal 精度环境，禁止 binary float；最终值保持底层精度，由页面统一格式化。

只有全部实际减持标的都有合格且与清单一致的报价时，`cost_breakdown.spread=known(value)`。否则：

```text
spread = unknown/BID_ASK_EVIDENCE_UNAVAILABLE
```

点差已知时，成本权威变为 `scenario_cost_evidence_bundle_v1`，其哈希覆盖：`scenario_cost_basis_hash_v1`、无成本候选哈希、监管费公开参考哈希或明确 null、专用政策完整身份与 payload hash、calculation version、日历权威与冻结 session window、`evidence_selected_at`，以及按 instrument 排序的已使用报价/原件身份。`available_at` 为所有组成证据可用时点的最大值，且不得晚于 `valuation_time`。现有 `position_hash` 可作为旁证，但不是成本输入完整性的权威。

由于佣金、滑点和监管费转嫁仍未知：

```text
candidate.estimated_cost = unknown/COST_EVIDENCE_INCOMPLETE
total_estimated_cost_ex_tax = unknown/COST_EVIDENCE_INCOMPLETE
```

不得把已知点差展示成已知总成本。

若候选没有任何实际减持，首版保持现有成本 unknown 语义，不借本切片把佣金、点差或总成本改写为已知零。

## 8. API 与页面行为

情景请求和 BFF 合同不变。Engine 内部顺序调整为：

```text
校验快照和诊断
  → claim v3 幂等权威
  → 生成无成本候选
  → 得到实际减持标的集合
  → 首次请求用单条 SQL/MVCC snapshot 选择并冻结政策、日历 session 与报价清单 / 重放精确复读清单
  → 计算点差并生成最终 result_hash
  → 写审计摘要并提交
```

审计摘要只新增 `spread_status=known|unknown` 和政策版本，不写报价 ID、标的 ID、权重或金额。

### 8.1 Agent 决策链

现有 Agent admission 和工具执行都会再次调用 `evaluate_adjustment_scenario()`；若只在情景 API 的隐藏清单中加入点差，Agent 会重算出不同结果。因此新情景不能继续冻结成 `PortfolioDecisionInputV1`。

新增 JSON 合同 `PortfolioDecisionInputV2`，只在现有 `portfolio_decision_runs.frozen_input` 中使用，不新增数据库列。它在 V1 字段基础上增加：

```text
scenario_claim_id
scenario_evidence_manifest_hash
```

服务器在 Agent admission 收到既有 `expected_scenario_result_hash` 时，按同 actor、v3 route/claim schema 和 `response_payload.expected_result_hash` 解析情景 claim，不把 claim ID 暴露到公开 BFF。低规模本地场景直接使用 JSONB 谓词，不为该内部解析新增索引。候选按 `(created_at, id)` 排序，逐条做清单结构和 exact dependency 校验，确定性选择第一条完整有效的 claim；不同 idempotency key 可以因“政策不可用”和“缺报价”等原因产生相同结果 hash、不同 manifest，这不是冲突，也不要求所有 manifest hash 相同。零条有效 claim 才失败关闭。选定后把该 claim ID 和 manifest hash 冻结到 V2，后续恢复不再扫描其他 claim。

Agent admission、恢复和 `portfolio_decision_tools` 对 V2 都必须加载该 exact claim，核对 actor、request hash、manifest hash、`expected_result_hash` 和所有固定依赖，再携带同一证据清单重算。工具不得执行“最新报价”查询。历史 V1 run 仍走原阶段 A 重算路径，不读取 v3 成本清单、不改变结果哈希。V1/V2 分支须覆盖正常运行、worker 恢复和 SDK/tool 消费；不能只修改 API handler。

### 8.2 页面展示

页面沿用现有成本表，但必须按 `evidence_version` 分支，不能继续把所有证据版本都称作监管费率：

- 已知点差显示组合价值占比；
- 缺证据继续显示当前精确中文说明；
- `us_regulatory_fee_reference_*`：沿用“公开监管费率参考已记录”，并说明不等于券商实际收费；
- `scenario_cost_evidence_bundle_v1`：显示“情景成本证据束已记录”，单独说明点差依据已冻结、监管费转嫁仍未证明；
- 未识别版本：显示“成本证据版本未识别”，不宣称任何权威已具备；
- 不新增刷新行情、接受方案、Shadow 或执行按钮；
- `analysis_comparable=true` 与 `execution_eligible=false` 保持。

## 9. 错误与失败关闭

以下情况不阻止情景分析，但点差保持 unknown：

- 估值时点不在 XNYS 常规交易时段；
- 任一减持标的缺报价；
- 报价超过 45 秒、带质量标记或 bid/ask 不完整；
- 首次请求时专用政策尚未发布。

以下情况返回稳定 409，不生成新结果：

- v3 重放清单结构损坏；
- 清单引用的政策、报价、原件或哈希缺失/冲突；
- 报价行规范化重算 hash 不一致，或原件实际 bytes/解析字段与数据库身份不一致；
- 日历、计算版本、无成本候选、成本基线、监管参考或最终结果哈希冲突；
- Agent V2 找不到任何完整有效的 v3 claim，或恢复时 frozen exact claim 不匹配；
- 同幂等 key 的请求 hash 不一致。

新增内部错误码：

- `SCENARIO_SPREAD_EVIDENCE_REPLAY_INVALID`

普通证据缺失不使用错误码，也不重试 Provider。

## 10. 迁移、兼容与回滚

- 迁移只插入一条 `source_policies` 数据，不修改 schema；
- Alembic `down_revision` 精确为 `0049_pd_terminal_phases`；
- downgrade 先以 `ACCESS EXCLUSIVE` 锁定 `idempotency_records` 与 `source_policies`，再检查所有 v3 manifest 引用；存在引用即拒绝，否则按现有不可变触发器约定临时禁用、精确删除固定政策行并恢复触发器；
- 旧 v2 幂等记录、旧结果哈希和既有 Agent V1 已完成运行保持不变；
- 新请求使用 v3 幂等 namespace，不会命中旧空清单；
- 公开 OpenAPI、BFF 请求/响应和 Agent 工具投影结构不变；Agent 内部 frozen input 新增 V2 分支；
- 若回滚应用代码但保留政策行，旧版本不会读取它；
- 数据库降级仅在没有 v3 引用时删除政策行，否则拒绝并保留审计事实；
- 不修改 `.openai/hosting.json`、密钥、权限、Provider 套餐或行情采集配置。

## 11. 实施切片

按依赖串行实施，不并行修改共享 API/迁移：

1. **政策与迁移：** 固定 payload、运行时严格加载、upgrade/downgrade 回归；
2. **报价清单：** 单条 SQL/MVCC snapshot 首次选择、精确复读、45 秒/质量/时段/原件校验；
3. **纯计算：** 点差公式、部分覆盖 unknown、权威 bundle hash；
4. **API 与 Agent 接线：** v3 claim、清单写入/复读、V2 frozen input、worker 恢复与工具 exact replay；
5. **页面与真实验收：** 两类证据文案分支、已知/未知显示、当前真实 0/12 保持 unknown。

每个切片独立 TDD、定向验证和审查；不为下一切片提前新增抽象。

## 12. 验收标准

必须覆盖以下真实合同：

1. 全部减持标的具备 45 秒内合格盘口：点差为手算已知值；总成本仍 unknown；
2. 恰好 45 秒可用，超过 45 秒 unknown；
3. 任一减持标的缺报价：整体点差 unknown；
4. `quality_flags` 非空、future available/received、crossed quote、错误 artifact hash/schema 均不能成为已知点差；
5. XNYS 常规时段外保持 unknown；
6. 不使用未减持标的缺失来阻止已知点差；
7. 同 key 首次得到空清单后插入历史时间戳报价，重放仍为相同 unknown/result hash；
8. 同 key 首次得到已知点差后插入更新报价，重放仍使用原报价并保持 result hash；
9. 清单引用被破坏时 409，不重新选择替代事实；
10. 不同 key 也不能采用插入事务起点 `created_at > valuation_time` 的报价或后来发布的政策；不得把通过条件表述为“估值前已提交”；
11. 并发事务在首次选择语句前开始、但在语句开始后才提交，即使 `created_at <= valuation_time` 也不能进入本次 manifest；下一不同 key 在新的选择语句中可以看到它，但只能声称其事件/可用/接收时间匹配估值时点，不能声称估值时已落库；
12. 同 `observed_at` 并列报价按完整排序稳定选择；跨进程/重启后 exact replay 不依赖运行时最新日历；
13. 日历库版本、政策 payload、计算版本、无成本候选、成本基线、监管参考或 expected result hash 任一漂移均 409；
14. 两个不同 key 产生相同 unknown/result hash、但 manifest 不同，Agent V2 确定性选择最早的完整有效 claim；worker 恢复和工具调用复用该 exact claim并得到 expected result hash；历史 V1 不变；
15. quote 的 bid/ask 或其他 canonical 字段被修改而行内 `content_hash` 未变时重放 409；原件 bytes、byte size、URI 或解析字段不一致时同样 409；
16. 页面分别验证监管参考、成本证据束和未知版本三种文案，不把 bundle 误称为监管费率；
17. 政策缺失或漂移时首次请求只返回点差 unknown，不回退展示政策；
18. 迁移从 0049 升级到唯一 0050 head，并验证并发引用锁和受引用后的 downgrade 拒绝；
19. 正常 `API → DB → 纯引擎 → response` PostgreSQL 路径覆盖 known 和 unknown；
20. 当前真实组合仍显示 0/12 合格报价和点差 unknown，不修改旧快照、不调用 Provider；
21. 验收前后 Shadow、订单和成交数量不变，`execution_locked=true`。

## 13. 完成定义

当且仅当一个用途合格、质量通过的 Alpaca IEX 盘口，其事件/available/received 时间与插入事务起点满足组合估值 cutoff，且它在首次选择语句的 MVCC snapshot 中已经提交可见并被清单冻结时，NEXUS 才把该只减仓候选的点差成本显示为已知。这不等于证明盘口在估值时已经落库。任何缺口继续明确 unknown；没有任何路径因此获得执行资格或触发外部行情、Shadow 或交易。
