# NEXUS 分钟级盘中市场监控实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不触发真实 Provider 请求的前提下，完成默认关闭、可审计、分钟级只读的美股盘中 Snapshot 采集、确定性波动指标、API 与市场页面闭环。

**Architecture:** Provider Worker 以 15 秒耐久 bucket 对最多 30 个观察标的执行固定 Alpaca Snapshot GET，raw-first 归档后写入追加式 trade/quote observation 和独立的 1Min monitor authority。Market API 从 PostgreSQL authority 读取日线和分钟事实，服务端计算波动指标；浏览器只轮询 NEXUS，不直连 Provider，Agent/风控/交易不能消费盘中域。

**Tech Stack:** Python 3.13、FastAPI、Pydantic v2、SQLAlchemy asyncio、PostgreSQL 17、Alembic、httpx、PyArrow、pytest、Hypothesis、TypeScript、React、vinext、Vitest、Playwright、Chart.js、Docker Compose

**Spec:** `docs/superpowers/specs/2026-08-28-nexus-intraday-market-monitor-design.md`

## Global Constraints

- `NEXUS_INTRADAY_MONITOR_MODE` 默认 `disabled`；测试以依赖注入运行，不产生外部请求。
- 生产 transport 只能访问 `GET https://data.alpaca.markets/v2/stocks/snapshots`，查询键严格为 `symbols/feed/currency`。
- 单批 1–30 个由数据库 authority 解析的美股 USD ticker；bucket 固定 15 秒，业务有效期固定 45 秒。
- `feed=iex|sip` 必须配 `delivery_mode=real_time`；`feed=delayed_sip` 必须配 `delivery_mode=delayed`。
- trade/quote observation 追加写；已有 paper execution 引用的 quote 行永不更新或删除。
- 1Min monitor authority 与研究/回测 `PriceBarSetAuthority` 隔离，不能进入 Agent、风控、订单、模拟成交或策略晋级。
- Decimal 从 JSON 直接解析，不经过二进制 float；所有 API 金融数值继续使用字符串。
- 浏览器轮询只读数据库，不触发 Provider、Job、proposal、order 或 broker command。
- 不修改历史迁移；新 schema 只通过 `0025_intraday_market_monitor` 前向迁移引入。
- 不配置真实密钥、不启用 Provider profile、不执行部署、不产生付费调用。

---

### Task 1: 追加式 quote observation 与独立 1Min monitor authority

**Files:**
- Modify: `engine/src/nexus/persistence/models/market.py`
- Modify: `engine/src/nexus/persistence/models/data.py`
- Create: `engine/alembic/versions/0025_intraday_market_monitor.py`
- Create: `engine/tests/integration/test_intraday_market_migration.py`
- Modify: `engine/tests/helpers/migration_schema_expectations.py`
- Modify: `engine/tests/helpers/migration_helpers.py`
- Modify: `engine/tests/integration/test_migration.py`
- Modify: `engine/src/nexus/persistence/migrations.py`
- Modify: `engine/src/nexus/quality/source_policy.py`

**Interfaces:**
- Produces: `LatestQuote.event_kind`, `LatestQuote.content_hash`, `LatestQuote.evidence_status`, `LatestQuote.data_artifact_id`
- Produces: `IntradayBarSetAuthority`
- Produces: fixed source policy constants `INTRADAY_SOURCE_POLICY_ID/KEY/VERSION/PAYLOAD`
- Preserves: every existing `latest_quotes.id` and inbound FK

- [ ] **Step 1: Add RED migration and model tests**

Write PostgreSQL tests that upgrade from `0024_broker_simulation` to head and assert:

```python
assert current_head(connection) == "0025_intraday_market_monitor"
assert not has_constraint(connection, "latest_quotes", "uq_latest_quotes_instrument_source_feed")
assert has_constraint(connection, "latest_quotes", "uq_latest_quotes_observation")
assert columns(connection, "latest_quotes") >= {
    "event_kind", "content_hash", "evidence_status", "data_artifact_id"
}
assert table_exists(connection, "intraday_bar_set_authorities")
assert source_policy_payload(connection, "intraday_monitor", "intraday_monitor_policy_v1") == {
    "priority": ["fosun", "alpaca", "fixture"],
    "fosun": {"mode": "read_only", "operation": "fosun_get_bars", "market_scope": "US"},
    "alpaca": {
        "feeds": ["iex", "sip", "delayed_sip"],
        "delivery_modes": ["real_time", "delayed"],
    },
    "fixture": {"delivery_mode": "fixture"},
    "purpose": "display_only_intraday_monitor",
}
```

Add behavioral tests:

```python
async def test_quote_observations_are_append_only_and_replay_idempotent(...):
    first_id = await insert_trade_observation(..., content_hash="a" * 64)
    replay_id = await insert_trade_observation(..., content_hash="a" * 64)
    second_id = await insert_trade_observation(..., content_hash="b" * 64)
    assert replay_id == first_id
    assert second_id != first_id
    with pytest.raises(IntegrityError):
        await update_quote_price(first_id, Decimal("101"))

async def test_existing_paper_quote_fk_survives_append_only_migration(...):
    quote_id, window_id = await seed_0024_paper_quote_reference(...)
    await upgrade_to_head(...)
    await insert_newer_trade_observation(...)
    assert await paper_window_quote_id(window_id) == quote_id
    assert await quote_last(quote_id) == Decimal("100")
```

- [ ] **Step 2: Run RED migration tests**

Run:

```bash
uv run --package nexus-engine pytest \
  engine/tests/integration/test_intraday_market_migration.py \
  engine/tests/integration/test_migration.py -q
```

Expected: failures name the missing revision, columns, table, constraints and policy.

- [ ] **Step 3: Add ORM models and generate the new migration**

Change `LatestQuote` so the old unique constraint is replaced by:

```python
UniqueConstraint(
    "instrument_id", "source", "feed", "event_kind", "observed_at", "content_hash",
    name="uq_latest_quotes_observation",
)
CheckConstraint("event_kind IN ('trade','quote','snapshot','fixture')", name="event_kind_allowed")
CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical")
CheckConstraint(
    "evidence_status IN ('verified','legacy_unverified','fixture')",
    name="evidence_status_allowed",
)
CheckConstraint("available_at >= observed_at AND received_at >= available_at", name="time_order")
CheckConstraint("bid IS NULL OR bid > 0", name="bid_positive")
CheckConstraint("ask IS NULL OR ask > 0", name="ask_positive")
CheckConstraint("last IS NULL OR last > 0", name="last_positive")
CheckConstraint("bid IS NULL OR ask IS NULL OR bid <= ask", name="spread_order")
CheckConstraint(
    "(event_kind = 'trade' AND last IS NOT NULL AND bid IS NULL AND ask IS NULL) OR "
    "(event_kind = 'quote' AND bid IS NOT NULL AND ask IS NOT NULL AND last IS NULL) OR "
    "(event_kind IN ('snapshot','fixture') AND (bid IS NOT NULL OR ask IS NOT NULL OR last IS NOT NULL))",
    name="event_shape",
)
CheckConstraint(
    "(source = 'fixture' AND evidence_status = 'fixture' AND data_artifact_id IS NULL) OR "
    "(source <> 'fixture' AND evidence_status = 'verified' AND data_artifact_id IS NOT NULL) OR "
    "(source <> 'fixture' AND evidence_status = 'legacy_unverified' AND data_artifact_id IS NULL)",
    name="evidence_state_consistent",
)
```

Add mapped fields with `event_kind`, `content_hash`, `evidence_status`, and a nullable FK to `data_artifacts.id`. Add `IntradayBarSetAuthority` in `data.py` with:

```text
id, price_series_manifest_id, evidence_ref_id, source_policy_id,
source, provider_object_id, revision,
hash_version="intraday_bar_set_sha256_v1",
policy_version="intraday_monitor_authority_v1",
content_hash, byte_length, bar_count,
first_observed_at, last_observed_at, available_at
```

Use composite evidence-owner FK `(evidence_ref_id, source, provider_object_id, revision)`, unique manifest and evidence references, canonical SHA-256, positive byte length, bar count 1–90, ordered observed timestamps and immutable created-at coverage. Add the four fixed `INTRADAY_SOURCE_POLICY_*` constants and payload in `source_policy.py`.

Generate one new revision from the model diff:

```bash
uv run --package nexus-engine alembic -c engine/alembic.ini revision \
  --autogenerate -m "add intraday market monitor authority"
```

Name the generated revision `0025_intraday_market_monitor`, set `down_revision='0024_broker_simulation'`, and add the reviewed data backfill, immutable trigger, fixed source-policy insert and guarded downgrade. Historical migration files remain byte-identical.

- [ ] **Step 4: Implement SQL authority guards**

The migration must:

1. Backfill legacy rows with `event_kind='fixture'` and `evidence_status='fixture'` for fixture source; use `event_kind='snapshot'` and `evidence_status='legacy_unverified'` for old non-fixture rows without raw artifacts.
2. Compute legacy `content_hash` using `pgcrypto.digest` over a deterministic concatenation of ID, source, feed, times, revision and Decimal text.
3. Install `trg_latest_quotes_observation_immutable BEFORE UPDATE OR DELETE`.
4. Install a trigger for `intraday_bar_set_authorities` that recomputes bar count, first/last observed time, content hash, byte length, source/evidence ownership and requires manifest interval `1Min`.
5. Refuse downgrade when either a non-legacy observation or any intraday authority exists.

Runtime ingestion may create only `verified` or `fixture`; source-policy selection must reject `legacy_unverified`. Migration tests prove historical non-fixture rows remain addressable through existing paper FKs but are ineligible for new market reads.

Use stable SQLSTATE `23514` for authority rejection. Add the new tables to created-at immutability coverage and migration residue checks.

- [ ] **Step 5: Run migration round trip GREEN**

Run:

```bash
uv run --package nexus-engine pytest \
  engine/tests/integration/test_intraday_market_migration.py \
  engine/tests/integration/test_migration.py \
  engine/tests/integration/test_paper_execution_migration.py -q
```

Expected: empty `0024 -> 0025 -> 0024 -> 0025` passes; downgrade with facts is rejected; no temporary databases remain.

- [ ] **Step 6: Commit Task 1**

```bash
git add engine/src/nexus/persistence engine/src/nexus/quality/source_policy.py \
  engine/alembic/versions/0025_intraday_market_monitor.py \
  engine/tests/integration/test_intraday_market_migration.py \
  engine/tests/integration/test_migration.py engine/tests/helpers
git commit -m "feat(db): add intraday market authority"
```

---

### Task 2: Strict Alpaca Snapshot transport and exact Decimal parsing

**Files:**
- Create: `engine/src/nexus/providers/market/snapshot_contracts.py`
- Create: `engine/src/nexus/providers/market/alpaca_snapshot.py`
- Modify: `engine/src/nexus/providers/market/alpaca_http.py`
- Modify: `engine/src/nexus/providers/market/alpaca.py`
- Modify: `engine/src/nexus/providers/settings.py`
- Modify: `engine/src/nexus/providers/composition.py`
- Create: `engine/tests/fixtures/providers/alpaca_snapshot.json`
- Create: `engine/tests/unit/providers/test_alpaca_snapshot.py`
- Modify: `engine/tests/unit/providers/test_alpaca_http.py`
- Modify: `engine/tests/unit/providers/test_composition.py`

**Interfaces:**
- Produces: `IntradaySnapshotRequest`, `MarketSnapshot`, `SnapshotBatch`
- Produces: `AlpacaSnapshotProvider.fetch_snapshots(request) -> SnapshotBatch`
- Produces: `build_alpaca_snapshot_provider(settings, client, resolver)`

- [ ] **Step 1: Write strict RED contract tests**

Define the wished-for request:

```python
request = IntradaySnapshotRequest(
    instruments=(
        SnapshotInstrument(instrument_id=AAPL_ID, symbol="AAPL"),
        SnapshotInstrument(instrument_id=MSFT_ID, symbol="MSFT"),
    ),
    feed="iex",
    delivery_mode="real_time",
)
batch = await provider.fetch_snapshots(request)
assert batch.provenance.source == "alpaca"
assert batch.provenance.feed == "iex"
assert batch.snapshots[0].trade.price == Decimal("227.16")
assert batch.snapshots[0].quote.bid == Decimal("227.14")
assert batch.snapshots[0].minute_bar.interval == "1Min"
```

Assert the fake fixed-origin client receives exactly:

```python
path_segments == ("v2", "stocks", "snapshots")
query == (("currency", "USD"), ("feed", "iex"), ("symbols", "AAPL,MSFT"))
```

Parameterized failures cover 0/31 instruments, duplicate symbol/ID, lowercase or unsafe ticker, feed/delivery mismatch, top-level unknown ticker, duplicate JSON key, unknown per-symbol field, binary float injected after parsing, NaN/Infinity, bid above ask, negative volume, invalid OHLC, naive time and response over 8 MiB.

- [ ] **Step 2: Run provider RED tests**

```bash
uv run --package nexus-engine pytest \
  engine/tests/unit/providers/test_alpaca_snapshot.py \
  engine/tests/unit/providers/test_alpaca_http.py \
  engine/tests/unit/providers/test_composition.py -q
```

Expected: import failures for the new contracts/provider and one historical numeric-payload test failing because the old parser creates floats.

- [ ] **Step 3: Implement immutable contracts**

Use frozen Pydantic models with `extra="forbid"`:

```python
class SnapshotTrade(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    price: Decimal
    observed_at: datetime
    provider_trade_id: str | None

class SnapshotQuote(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    bid: Decimal
    ask: Decimal
    observed_at: datetime

class SnapshotMinuteBar(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    bar: PriceBar

class MarketSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    instrument_id: InstrumentId
    symbol: str
    trade: SnapshotTrade | None
    quote: SnapshotQuote | None
    minute_bar: SnapshotMinuteBar | None
```

Every Decimal validator rejects `float`, accepts only `Decimal|int|str`, and requires finite values. Every datetime becomes aware UTC.

- [ ] **Step 4: Implement the fixed Snapshot transport**

Parse with:

```python
json.loads(
    raw,
    object_pairs_hook=_unique_object,
    parse_float=Decimal,
    parse_int=Decimal,
    parse_constant=_reject_json_constant,
)
```

Apply the same exact parser to existing Alpaca historical bars so real numeric JSON never traverses binary float. Preserve raw bytes unchanged. `dailyBar` and `prevDailyBar` are recognized keys but not normalized into business models.

- [ ] **Step 5: Add fail-closed settings and composition**

Add:

```python
class IntradayMonitorMode(StrEnum):
    DISABLED = "disabled"
    PRODUCTION = "production"
```

Provider settings fields map to the exact environment names in the spec. Validate poll seconds equals 15 and max symbols equals 30. `build_alpaca_snapshot_provider` raises before HTTP-client construction when disabled, incomplete or inconsistent. Fixture bundle does not contain a production snapshot provider.

- [ ] **Step 6: Run provider GREEN and socket guard**

```bash
NEXUS_OFFLINE_SOCKET_GUARD=1 PYTHONPATH="$PWD/scripts/offline_guard" \
  uv run --package nexus-engine pytest \
  engine/tests/unit/providers/test_alpaca_snapshot.py \
  engine/tests/unit/providers/test_alpaca_http.py \
  engine/tests/unit/providers/test_market_providers.py \
  engine/tests/unit/providers/test_composition.py -q
```

Expected: all pass; external socket attempts 0; disabled mode constructs no client.

- [ ] **Step 7: Commit Task 2**

```bash
git add engine/src/nexus/providers engine/tests/unit/providers engine/tests/fixtures/providers/alpaca_snapshot.json
git commit -m "feat(provider): normalize bounded intraday snapshots"
```

---

### Task 3: Raw-first intraday ingestion and immutable replay

**Files:**
- Create: `engine/src/nexus/market/intraday_ingestion.py`
- Create: `engine/src/nexus/market/intraday_authority.py`
- Modify: `engine/src/nexus/market/object_store.py`
- Create: `engine/tests/integration/test_intraday_ingestion_pg.py`
- Create: `engine/tests/unit/market/test_intraday_ingestion.py`

**Interfaces:**
- Produces: `canonical_quote_observation(...) -> bytes`
- Produces: `ingest_intraday_snapshot(session, object_store, batch, commit=False) -> IntradayIngestionResult`
- Produces: `finalize_intraday_bar_set(session, manifest) -> IntradayBarSetAuthority | None`

- [ ] **Step 1: Write RED ingestion tests**

Cover:

```python
result = await ingest_intraday_snapshot(session, store, batch, commit=False)
assert result.created_trade_observations == 2
assert result.created_quote_observations == 2
assert result.created_bar_authorities == 2
assert result.replayed == 0
assert store.archived_payloads == [batch.raw_payload]
```

Re-run the identical batch and assert no second observation, DataArtifact, manifest or authority. Change only a same-minute bar revision and assert a new immutable monitor authority is created while the first remains unchanged. Inject a forged artifact owner, partial DB failure and object-store promotion failure; assert no readable partial authority remains.

- [ ] **Step 2: Run ingestion RED tests**

```bash
uv run --package nexus-engine pytest \
  engine/tests/unit/market/test_intraday_ingestion.py \
  engine/tests/integration/test_intraday_ingestion_pg.py -q
```

Expected: missing module/functions and tables fail for the intended reasons.

- [ ] **Step 3: Implement canonical quote observations**

Canonical JSON includes only:

```python
{
    "instrument_id": str(instrument_id),
    "source": source,
    "feed": feed,
    "delivery_mode": delivery_mode,
    "event_kind": event_kind,
    "bid": decimal_or_none(bid),
    "ask": decimal_or_none(ask),
    "last": decimal_or_none(last),
    "observed_at": utc_text(observed_at),
    "available_at": utc_text(available_at),
    "received_at": utc_text(received_at),
    "revision": revision,
}
```

Hash UTF-8 canonical bytes with SHA-256. Insert through PostgreSQL `ON CONFLICT ... DO NOTHING RETURNING id`; on replay select the exact existing row and compare every field before reporting replay.

- [ ] **Step 4: Implement raw-first batch ingestion**

Archive the exact raw response once under a content-addressed key. Persist `DataArtifact(schema_version="alpaca_stock_snapshots_v1")`. Create separate `trade` and `quote` observations referencing it. Serialize each minute bar to deterministic Parquet, but finalize it through `IntradayBarSetAuthority`, never `PriceBarSetAuthority`.

All DB mutations use the caller transaction when `commit=False`. Temporary normalized objects are cleaned in `finally`; an archived raw object is content-addressed and may remain after rollback without becoming readable authority.

- [ ] **Step 5: Verify GREEN and corruption guards**

```bash
uv run --package nexus-engine pytest \
  engine/tests/unit/market/test_intraday_ingestion.py \
  engine/tests/integration/test_intraday_ingestion_pg.py \
  engine/tests/integration/test_market_ingestion.py \
  engine/tests/integration/test_price_bar_authority_pg.py -q
```

- [ ] **Step 6: Commit Task 3**

```bash
git add engine/src/nexus/market engine/tests/unit/market engine/tests/integration/test_intraday_ingestion_pg.py
git commit -m "feat(market): ingest immutable intraday evidence"
```

---

### Task 4: 15-second durable scheduler and collection Job

**Files:**
- Create: `engine/src/nexus/jobs/intraday_schedule.py`
- Create: `engine/src/nexus/jobs/intraday_collection.py`
- Modify: `engine/src/nexus/jobs/models.py`
- Modify: `engine/src/nexus/worker.py`
- Create: `engine/tests/unit/jobs/test_intraday_schedule.py`
- Create: `engine/tests/unit/jobs/test_intraday_collection.py`
- Create: `engine/tests/integration/test_intraday_collection_pg.py`
- Modify: `engine/tests/unit/jobs/test_job_registry.py`

**Interfaces:**
- Produces: `IntradaySnapshotJobPayloadV1`
- Produces: `schedule_due_intraday_snapshot(session, now, settings) -> Job | None`
- Produces: `build_intraday_collection_registry(provider, store, now) -> JobRegistry`

- [ ] **Step 1: Write RED pure schedule tests**

Assert `bucket_for(2026-08-28T14:30:29Z)` returns `[14:30:15Z,14:30:30Z)`. Cover DST, half-day, before open, at close, missing/unpublished calendar, empty watchlist, 31 members, duplicate ticker, non-USD and alias collision.

The legal payload must equal:

```python
{
    "schema": "intraday_snapshot_job_v1",
    "calendar_authority_id": str(calendar_id),
    "session": "2026-08-28",
    "bucket_start": "2026-08-28T14:30:15.000000Z",
    "bucket_end": "2026-08-28T14:30:30.000000Z",
    "instruments": [
        {"instrument_id": str(AAPL_ID), "symbol": "AAPL"},
        {"instrument_id": str(MSFT_ID), "symbol": "MSFT"},
    ],
    "symbol_set_hash": expected_hash,
    "feed": "iex",
    "delivery_mode": "real_time",
    "contract_hash": SNAPSHOT_CONTRACT_HASH,
}
```

- [ ] **Step 2: Run scheduler RED tests**

```bash
uv run --package nexus-engine pytest \
  engine/tests/unit/jobs/test_intraday_schedule.py \
  engine/tests/unit/jobs/test_intraday_collection.py -q
```

- [ ] **Step 3: Implement idempotent schedule and closed Job authority**

Add `JobType.COLLECT_INTRADAY_SNAPSHOT = "collect.intraday_snapshot"` with priority 160. Schedule through `enqueue_job` using bucket start as effective time and canonical payload hash. Concurrent scheduler calls must resolve to one Job.

Handler revalidates schema, contract, feed, bucket age and ordered instrument hash before provider construction. It maps results exactly:

```python
created > 0 and missing == 0 -> succeeded / INTRADAY_SNAPSHOT_COLLECTED
created == 0 and replayed > 0 and missing == 0 -> succeeded / INTRADAY_SNAPSHOT_REPLAYED
missing > 0 -> skipped / INTRADAY_SNAPSHOT_PARTIAL
now >= bucket_end + 45 seconds -> skipped / INTRADAY_BUCKET_EXPIRED
```

Unknown provider errors surface only through existing sanitized `HANDLER_FAILED`; no raw error text enters Job.

- [ ] **Step 4: Wire worker without enabling external traffic**

In the worker loop, call schedule only when validated mode is `production`. Register the handler only in that mode. In disabled mode, tests assert no Snapshot provider and no `httpx` request is constructed. Keep general daily pipeline and Agent registries byte-compatible.

- [ ] **Step 5: Run concurrency/failure GREEN**

```bash
uv run --package nexus-engine pytest \
  engine/tests/unit/jobs/test_intraday_schedule.py \
  engine/tests/unit/jobs/test_intraday_collection.py \
  engine/tests/integration/test_intraday_collection_pg.py \
  engine/tests/integration/test_job_worker.py \
  engine/tests/integration/test_job_leases.py -q
```

- [ ] **Step 6: Commit Task 4**

```bash
git add engine/src/nexus/jobs engine/src/nexus/worker.py \
  engine/tests/unit/jobs engine/tests/integration/test_intraday_collection_pg.py
git commit -m "feat(worker): schedule bounded intraday snapshots"
```

---

### Task 5: Database loaders and deterministic intraday metrics

**Files:**
- Create: `engine/src/nexus/market/database_series.py`
- Create: `engine/src/nexus/market/intraday_metrics.py`
- Modify: `engine/src/nexus/market/service.py`
- Modify: `engine/src/nexus/quality/source_policy.py`
- Create: `engine/tests/unit/market/test_intraday_metrics.py`
- Create: `engine/tests/integration/test_market_database_series_pg.py`
- Modify: `engine/tests/integration/test_market_api.py`

**Interfaces:**
- Produces: `load_daily_series(session, instrument_id, decision_time) -> tuple[PriceSeries, ...]`
- Produces: `load_intraday_series(session, instrument_id, decision_time) -> IntradayAuthorityView`
- Produces: `compute_intraday_metrics(view, decision_time) -> IntradayMetrics`

- [ ] **Step 1: Write RED metric tests**

Build Decimal 1Min fixtures and assert:

```python
metrics = compute_intraday_metrics(view, DECISION_TIME)
assert metrics.return_5m == Decimal("0.010000000000")
assert metrics.return_30m == Decimal("0.030000000000")
assert metrics.range_30m == Decimal("0.040000000000")
assert metrics.spread_bps == Decimal("4.402200000000")
assert metrics.sample_count == 31
```

Separate cases remove one middle minute, duplicate a timestamp, add a future/incomplete bar, use stale quote, cross source, flagged bar and out-of-session time. Assert the affected metric is `None` and the stable reason is present; never assert a fabricated zero.

- [ ] **Step 2: Write RED PostgreSQL loader tests**

Without setting `app.state.market_price_series_loader`, seed approved daily and intraday authorities. Assert `/internal/v1/market?selected_instrument_id=...` returns both. Seed a later revision available after decision time and assert it is excluded. Corrupt authority/evidence ownership with direct SQL and assert the response degrades to unavailable rather than reading it.

- [ ] **Step 3: Run loader/metric RED tests**

```bash
uv run --package nexus-engine pytest \
  engine/tests/unit/market/test_intraday_metrics.py \
  engine/tests/integration/test_market_database_series_pg.py \
  engine/tests/integration/test_market_api.py -q
```

- [ ] **Step 4: Implement authority-preserving database loaders**

Daily loader joins `PriceBarSetAuthority -> PriceSeriesManifest -> PriceBarRecord -> ResearchEvidenceRef`, re-runs `prepare_price_bar_set`, enforces point-in-time and returns whole candidates. Intraday loader joins `IntradayBarSetAuthority -> PriceSeriesManifest -> PriceBarRecord -> ResearchEvidenceRef`, re-runs the same canonical bar validation with the `1Min` interval requirement, deduplicates same-minute revisions by latest authority visible at decision time, keeps one source/feed series and returns at most the most recent 90 minutes.

Tests may override loaders, but `MarketService` must use database loaders by default.

- [ ] **Step 5: Implement exact metrics and quality state**

Use only complete bars (`observed_at + 1 minute <= decision_time`). A continuous window requires timestamps spaced exactly one minute. Quantize API-facing values through existing `decimal_to_string`; internal calculations retain full Decimal precision. Use separate trade, quote and bar timestamps and the 45/90-second stale limits from the spec.

- [ ] **Step 6: Bound latest quote queries**

Replace the unbounded `select(LatestQuote)` with a PostgreSQL window/subquery that returns at most one latest visible row per `(instrument_id, source, feed, event_kind)`. Cross-source checks operate only on last-bearing `trade|snapshot|fixture` rows; spread reads only `quote|snapshot|fixture` rows.

- [ ] **Step 7: Run GREEN and query scale tests**

```bash
uv run --package nexus-engine pytest \
  engine/tests/unit/market/test_intraday_metrics.py \
  engine/tests/integration/test_market_database_series_pg.py \
  engine/tests/integration/test_market_api.py \
  engine/tests/integration/test_source_policy.py -q
```

Add a scale case with 30 instruments × 1,000 historical observations and assert the page result remains bounded and correct; use SQL statement count rather than a wall-clock threshold.

- [ ] **Step 8: Commit Task 5**

```bash
git add engine/src/nexus/market engine/src/nexus/quality/source_policy.py \
  engine/tests/unit/market engine/tests/integration/test_market_database_series_pg.py \
  engine/tests/integration/test_market_api.py engine/tests/integration/test_source_policy.py
git commit -m "feat(market): expose deterministic intraday metrics"
```

---

### Task 6: Strict API, BFF runtime contract, and generated client

**Files:**
- Modify: `engine/src/nexus/api/schemas/market.py`
- Modify: `engine/src/nexus/api/routes/market.py`
- Modify: `engine/tests/contract/test_openapi.py`
- Modify: `contracts/openapi.json`
- Modify: `app/lib/generated/schema.d.ts`
- Modify: `app/lib/generated/runtime-contract.ts`
- Modify: `tests/unit/engine/client-security.test.ts`
- Modify: `tests/unit/engine/market-routes.test.ts`

**Interfaces:**
- Produces: `IntradayBarResponse`, `IntradayMonitorResponse`
- Extends: `MarketPageResponse.intraday_monitor`
- Preserves: BFF query and method surface

- [ ] **Step 1: Write RED API and runtime-boundary tests**

Assert the strict response contains the exact fields in the spec, at most 60 bars, aware UTC timestamps and Decimal strings. Reject extra nested secret-like fields, 61 bars, numeric JSON finance values, invalid status, unknown quality reason shape and source/selected-instrument mismatch.

In TypeScript, extend the canonical `marketEnvelope()` fixture and assert `rebuildAllowedEngineResponse` accepts the legal envelope but returns null for every malformed variant.

- [ ] **Step 2: Run RED contracts**

```bash
uv run --package nexus-engine pytest \
  engine/tests/contract/test_openapi.py \
  engine/tests/integration/test_market_api.py -q
npm run test:unit -- tests/unit/engine/client-security.test.ts tests/unit/engine/market-routes.test.ts
```

- [ ] **Step 3: Implement Pydantic response schemas**

`IntradayMonitorResponse` uses:

```python
status: Literal["ready", "partial", "stale", "outside_session", "unavailable"]
source: str | None
feed: str | None
delivery_mode: DeliveryMode | None
last: DecimalString | None
bid: DecimalString | None
ask: DecimalString | None
trade_as_of: datetime | None
quote_as_of: datetime | None
bar_as_of: datetime | None
return_5m: DecimalString | None
return_30m: DecimalString | None
range_30m: DecimalString | None
spread_bps: DecimalString | None
quote_age_seconds: int | None
bar_age_seconds: int | None
sample_count: int
quality_reasons: tuple[str, ...]
bars: tuple[IntradayBarResponse, ...]
```

Field validators require aware UTC, non-negative ages/sample count and at most 60 bars. The route performs no network or writes.

- [ ] **Step 4: Regenerate artifacts through project scripts**

```bash
npm run generate:client
npm run check:generated
```

Do not hand-edit generated files after generation.

- [ ] **Step 5: Run API/BFF GREEN**

```bash
uv run --package nexus-engine pytest engine/tests/contract/test_openapi.py engine/tests/integration/test_market_api.py -q
npm run test:unit -- tests/unit/engine/client-security.test.ts tests/unit/engine/market-routes.test.ts
npm run typecheck
```

- [ ] **Step 6: Commit Task 6**

```bash
git add engine/src/nexus/api engine/tests/contract/test_openapi.py \
  contracts/openapi.json app/lib/generated tests/unit/engine
git commit -m "feat(api): expose intraday market evidence"
```

---

### Task 7: Accessible intraday monitor UI

**Required skills before implementation:** `frontend-design`, `ui-ux-pro-max`

**Files:**
- Create: `app/components/intraday-monitor.tsx`
- Modify: `app/(protected)/market/page.tsx`
- Modify: `app/styles/tokens.css`
- Modify: `tests/unit/market/page.test.tsx`
- Modify: `tests/unit/market/market-boundary.test.tsx`
- Modify: `tests/e2e/market.spec.ts`

**Interfaces:**
- Consumes: generated `IntradayMonitorResponse`
- Produces: `<IntradayMonitor symbol monitor />`

- [ ] **Step 1: Write RED component tests**

Render ready IEX evidence and assert:

```typescript
expect(screen.getByRole("region", { name: "AAPL 盘中波动观察" })).toHaveTextContent("实时 IEX（仅 IEX）");
expect(screen.getByText("5 分钟收益").nextSibling).toHaveTextContent("+1.00%");
expect(screen.getByText("30 分钟收益").nextSibling).toHaveTextContent("+3.00%");
expect(screen.getByText("买卖价差").nextSibling).toHaveTextContent("4.40 bps");
expect(screen.getByRole("table", { name: "AAPL 1 分钟行情数据" })).toHaveTextContent("227.160000000000");
```

Cases cover null metric rendered as “证据不足”, stale/partial/outside-session/unavailable, delayed SIP text, no investment language, selected-instrument mismatch, 60 bars, keyboard table access and chart/table value equality.

- [ ] **Step 2: Run UI RED tests**

```bash
npm run test:unit -- tests/unit/market/page.test.tsx tests/unit/market/market-boundary.test.tsx
```

- [ ] **Step 3: Implement the focused component**

Use existing tokens and `PriceChart` visual language, but keep a separate minute chart component inside `intraday-monitor.tsx` so daily labels cannot leak into minute data. The chart is decorative unless paired with the semantic table. No animation runs when `prefers-reduced-motion` is set.

`MarketPage` stores `data.intraday_monitor` alongside selected series, clears both on selection/context changes and ignores out-of-order responses using the existing refresh version. `classifyMarketState` includes intraday stale/partial status without turning `outside_session` into an error.

- [ ] **Step 4: Add responsive and accessibility CSS**

Reuse semantic surface/border/action tokens. Four facts use a responsive grid: four columns desktop, two tablet, one at narrow/200% reflow. The minute table has a named scroll region and 44px focusable target. Direction and status never rely only on red/green.

- [ ] **Step 5: Extend Playwright evidence**

Update the intercepted market envelope. Test 390 and 1440 widths plus the existing global viewport/reflow suite. Assert:

- source/feed/delivery labels;
- metric null semantics;
- chart canvas and table;
- keyboard navigation;
- no horizontal page overflow;
- Axe serious/critical violations 0;
- 15/60-second browser polling remains GET-only.

- [ ] **Step 6: Run UI GREEN**

```bash
npm run test:unit -- tests/unit/market/page.test.tsx tests/unit/market/market-boundary.test.tsx
npm run test:e2e -- tests/e2e/market.spec.ts --workers=1
npm run lint
npm run typecheck
npm run build
```

- [ ] **Step 7: Commit Task 7**

```bash
git add app/components/intraday-monitor.tsx 'app/(protected)/market/page.tsx' \
  app/styles/tokens.css tests/unit/market tests/e2e/market.spec.ts
git commit -m "feat(web): add intraday market monitor"
```

---

### Task 8: Compose policy, runbook, README and activation stop boundary

**Files:**
- Modify: `infra/env.example`
- Modify: `infra/compose.yaml`
- Create: `docs/runbooks/intraday-market-monitor.md`
- Modify: `docs/runbooks/local-development.md`
- Modify: `README.md`
- Create: `engine/tests/unit/ops/test_intraday_compose_policy.py`
- Modify: `engine/tests/unit/ops/test_provider_compose_policy.py`

**Interfaces:**
- Produces: static disabled/default configuration and activation runbook
- Preserves: app profile has no Provider credentials or monitor configuration

- [ ] **Step 1: Write RED Compose policy tests**

Parse Compose for app/provider/broker profiles and assert:

```python
assert "NEXUS_INTRADAY_MONITOR_MODE" not in app_api_environment
assert "NEXUS_INTRADAY_MONITOR_MODE" not in app_worker_environment
assert provider_worker_environment["NEXUS_INTRADAY_MONITOR_MODE"] == "${NEXUS_INTRADAY_MONITOR_MODE:-disabled}"
assert provider_worker_environment["NEXUS_INTRADAY_POLL_SECONDS"] == "${NEXUS_INTRADAY_POLL_SECONDS:-15}"
assert provider_worker_environment["NEXUS_INTRADAY_MAX_SYMBOLS"] == "${NEXUS_INTRADAY_MAX_SYMBOLS:-30}"
assert not broker_environment.keys() & intraday_environment_keys
```

Assert rendered app config contains no Alpaca/Fosun/OpenAI secret values.

- [ ] **Step 2: Run RED ops tests**

```bash
uv run --package nexus-engine pytest \
  engine/tests/unit/ops/test_intraday_compose_policy.py \
  engine/tests/unit/ops/test_provider_compose_policy.py -q
```

- [ ] **Step 3: Add static configuration without activation**

Add variable names and safe disabled defaults only. Do not place values in `infra/.env.local`. Provider profile may pass the variable names to provider-worker, but no ordinary command starts or activates it.

- [ ] **Step 4: Write the runbook and current status**

The runbook must state:

```text
Intraday Monitor offline_ready / external_data_activation_blocked
```

It documents fixed endpoint, IEX-vs-SIP labeling, account entitlement evidence, secret injection outside Git, exact bounded probe, request budget, UTC window, stop conditions, rollback, data-retention impact and approval boundary. It must not contain a real key, account ID or executable activation command populated with secrets.

Update README to show Phase 2, Phase 3A and Intraday Monitor accurately; external Fosun simulation and live remain locked.

- [ ] **Step 5: Run ops GREEN**

```bash
uv run --package nexus-engine pytest engine/tests/unit/ops -q
docker compose --env-file infra/env.example -f infra/compose.yaml --profile app config --quiet
docker compose --env-file infra/env.example -f infra/compose.yaml --profile provider config --quiet
docker compose --env-file infra/env.example -f infra/compose.yaml --profile broker config --quiet
git diff --check
```

- [ ] **Step 6: Commit Task 8**

```bash
git add infra docs/runbooks README.md engine/tests/unit/ops
git commit -m "docs(ops): gate intraday provider activation"
```

---

### Task 9: Full acceptance, review and local cleanup

**Files:**
- Create: `docs/superpowers/reviews/2026-08-28-nexus-intraday-market-monitor-review.md`
- Modify only if a reproduced review finding requires a tested fix

**Interfaces:**
- Produces: final local acceptance evidence

- [ ] **Step 1: Create an ignored, task-specific acceptance environment**

Create `infra/.env.intraday-acceptance` with `apply_patch`; it must remain ignored and contain only disposable fixture values:

```dotenv
NEXUS_ENV=test
DATABASE_URL=postgresql+asyncpg://nexus_intraday:nexus-intraday-fixture@postgres:5432/nexus_intraday
OBJECT_STORE_ENDPOINT=http://minio:9000
OBJECT_STORE_REGION=us-east-1
OBJECT_STORE_BUCKET=nexus-intraday-fixtures
OBJECT_STORE_ACCESS_KEY_ID=nexus-intraday-fixture
OBJECT_STORE_SECRET_ACCESS_KEY=nexus-intraday-fixture-secret
NEXUS_ENGINE_BASE_URL=http://api:8000
NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=intraday-fixture
NEXUS_ENGINE_HMAC_CURRENT_SECRET=intraday-fixture-secret
AGENT_RUNTIME_MODE=fixture
NEXUS_PROVIDER_RUNTIME_MODE=fixture
POSTGRES_DB=nexus_intraday
POSTGRES_USER=nexus_intraday
POSTGRES_PASSWORD=nexus-intraday-fixture
MINIO_ROOT_USER=nexus-intraday-fixture
MINIO_ROOT_PASSWORD=nexus-intraday-fixture-secret
NEXUS_POSTGRES_HOST_PORT=55433
NEXUS_MINIO_HOST_PORT=5900
NEXUS_MINIO_CONSOLE_HOST_PORT=5901
NEXUS_API_HOST_PORT=5800
```

Verify before startup:

```bash
git check-ignore -q infra/.env.intraday-acceptance
git status --short
```

- [ ] **Step 2: Run focused PostgreSQL acceptance**

Use a task-specific Compose project name and existing Docker-compatible runtime. Do not stop or clean other NEXUS projects.

```bash
COMPOSE_PROJECT_NAME=nexus-intraday-acceptance \
  docker compose --env-file infra/.env.intraday-acceptance \
  -f infra/compose.yaml up -d --wait postgres minio
DATABASE_URL="postgresql+asyncpg://nexus_intraday:nexus-intraday-fixture@127.0.0.1:55433/nexus_intraday" \
  uv run --package nexus-engine alembic -c engine/alembic.ini upgrade head
DATABASE_URL="postgresql+asyncpg://nexus_intraday:nexus-intraday-fixture@127.0.0.1:55433/postgres" \
  uv run --package nexus-engine pytest \
  engine/tests/integration/test_intraday_market_migration.py \
  engine/tests/integration/test_intraday_ingestion_pg.py \
  engine/tests/integration/test_intraday_collection_pg.py \
  engine/tests/integration/test_market_database_series_pg.py \
  engine/tests/integration/test_market_api.py -q
```

Expected: all pass; one Alembic head `0025_intraday_market_monitor`; temporary DB residue 0.

- [ ] **Step 3: Run complete Python gates**

```bash
NEXUS_OFFLINE_SOCKET_GUARD=1 PYTHONPATH="$PWD/scripts/offline_guard" \
  uv run --package nexus-engine pytest engine/tests -q --import-mode=importlib \
  --ignore=engine/tests/integration
uv run ruff check engine integrations/fosun_gateway
uv run mypy engine/src integrations/fosun_gateway/src
DATABASE_URL="postgresql+asyncpg://nexus_intraday:nexus-intraday-fixture@127.0.0.1:55433/postgres" \
  uv run --package nexus-engine pytest engine/tests/integration -q --import-mode=importlib
```

Record the exact pass count and duration from the complete PostgreSQL integration suite.

- [ ] **Step 4: Run complete Web gates**

```bash
npm run check:generated
npm run lint
npm run typecheck
npm run test:unit
npm run build
npm run test:e2e -- --workers=3
```

- [ ] **Step 5: Prove safety boundaries**

Run and record:

```bash
rg -n "intraday|Intraday" engine/src/nexus/agents engine/src/nexus/risk \
  engine/src/nexus/paper engine/src/nexus/providers/broker
rg -n "ak_[A-Za-z0-9]|BEGIN .*PRIVATE KEY|APCA-API-SECRET-KEY[^A-Z_]" \
  engine app infra docs contracts
docker ps --format '{{.Names}}' | rg 'fosun|broker|intraday' || true
```

Expected: forbidden import hits 0, tracked credential-shaped findings 0, externally active Fosun/Broker/Intraday containers 0.

- [ ] **Step 6: Review changed code against the risk checklist**

Review the complete diff for:

- point-in-time leakage and incomplete-minute use;
- unbounded quote queries and duplicate/replayed observations;
- artifact/authority mismatch and partial transaction visibility;
- source/feed/delivery mislabeling;
- disabled-mode client construction or external socket;
- Agent/risk/order boundary imports;
- browser/provider coupling, secret leakage and accessibility regressions;
- downgrade with facts and cleanup scope.

For every finding, first add a failing regression test, reproduce, then implement the minimum fix. Record Critical/Important/Minor counts and final residual findings in the review document.

- [ ] **Step 7: Clean only the task-specific runtime**

```bash
COMPOSE_PROJECT_NAME=nexus-intraday-acceptance \
  docker compose --env-file infra/.env.intraday-acceptance -f infra/compose.yaml down
```

Do not use `--volumes` unless the exact task volume is separately verified disposable. Confirm task containers/networks 0, delete only the task-owned ignored `infra/.env.intraday-acceptance` with `apply_patch`, and leave other NEXUS projects untouched.

- [ ] **Step 8: Commit review evidence**

```bash
git add docs/superpowers/reviews/2026-08-28-nexus-intraday-market-monitor-review.md
git commit -m "test: accept intraday market monitor"
git status --short --branch
```

Final report must state `Intraday Monitor offline_ready / external_data_activation_blocked`, exact tests, migration head, no external calls, no paid activation, no broker/account action, and the separately approval-gated activation prerequisites.

## Plan Self-Review

- Spec coverage: Tasks 1–9 cover schema, provider, ingestion, scheduling, loaders/metrics, API, UI, ops and acceptance.
- Type consistency: Snapshot contracts flow unchanged from Provider to ingestion; API response types are distinct from persistence models; UI consumes only generated schemas.
- Authority consistency: research price authority remains unchanged; new 1Min authority is display-only and import-isolated.
- Execution readiness: every task names exact paths, RED command, interface, GREEN command and commit boundary.
- Scope: WebSocket, notifications, Agent features, real Provider activation, Fosun writes, deployment and live trading remain excluded.
