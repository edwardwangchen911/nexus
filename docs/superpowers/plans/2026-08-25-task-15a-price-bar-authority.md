# Task 15A Price Bar Authority Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every Agent price-feature citation cryptographically prove the exact immutable PostgreSQL Bar set used by the deterministic calculation, while enforcing source, point-in-time, quality, and concurrency rules fail-closed.

**Architecture:** Keep the existing Parquet artifact evidence for archive provenance, then add an independent immutable `PriceBarSetAuthority` whose PostgreSQL-generated SHA-256 covers manifest provenance and every normalized Bar field. Ingestion finalizes clean eligible sets under a manifest advisory lock; Agent admission freezes only the Bar-set EvidenceRef; the price tool revalidates that authority and computes from its local immutable rows.

**Tech Stack:** Python 3.12, SQLAlchemy 2 async ORM, Alembic, PostgreSQL 17, `pgcrypto`, asyncpg, pytest/pytest-asyncio, Ruff, Mypy, existing Decimal price-feature engine.

**Spec:** `docs/superpowers/specs/2026-08-25-task-15a-price-bar-authority-design.md`

## Global Constraints

- Modify the unreleased `engine/alembic/versions/0011_agents.py`; do not create `0012`, because Task 16 reserves `0012_backtests`.
- Preserve the in-progress Task 16 head assertion and backtest files already present in the worktree. Extend `test_agents_migration.py` additively; do not rename, remove, or replace its `0012_backtests` single-head test.
- PostgreSQL 17 plus `pgcrypto.digest(..., 'sha256')` is the only Bar-set hash implementation; Python must not duplicate canonicalization.
- Hash version is exactly `price_bar_set_sha256_v1`; policy version is exactly `agent_price_authority_v1`.
- Production authority is exactly `alpaca/sip/delayed/1Day`; fixture is usable only when `AgentRuntimeMode.FIXTURE` explicitly enables it.
- `source_policy_id` is exactly `3c60888d-e92d-5e04-913c-7d427004cfef` and the immutable `source_policy_v1` row must match its committed payload.
- Any non-empty `quality_flags`, duplicate session, mixed instrument, future row, row-count mismatch, or owner mismatch makes the set ineligible or unavailable; none can return `status=ok`.
- Money and prices remain PostgreSQL `NUMERIC(38,12)` and Python `Decimal`; binary float is forbidden.
- Agent remains read-only: no object-store credentials, SQL input, broker identifiers, trade tool, order path, real-account access, deployment, or production write is added.
- `00656.HK` remains read-only and this task does not authorize Fosun price data; Fosun needs a later `agent_price_authority_v2` after its Bar contract is verified.
- `CREATE EXTENSION IF NOT EXISTS pgcrypto` is allowed in upgrade; downgrade must not drop the shared extension.
- Preserve unrelated user changes and do not modify Web, Fosun gateway, trading, backtest, or `.openai/hosting.json` files.

## File Map

| File | Responsibility |
| --- | --- |
| `engine/src/nexus/persistence/models/data.py` | `PriceBarSetAuthority` ORM and immutable constraints |
| `engine/src/nexus/persistence/models/research.py` | Bar-set `db://...@sha256:` EvidenceRef location constraint |
| `engine/alembic/versions/0011_agents.py` | Table, pgcrypto, canonical functions, policy/owner guards, advisory locking, downgrade |
| `engine/src/nexus/market/price_bar_authority.py` | Typed DB prepare/finalize service; no hash implementation |
| `engine/src/nexus/market/ingestion.py` | Finalize eligible Bar sets in the existing ingestion transaction |
| `engine/src/nexus/agents/price_authority.py` | Runtime-eligible selection and existing-link revalidation |
| `engine/src/nexus/agents/persistence.py` | Explicit fixture-authority admission parameter |
| `engine/src/nexus/api/routes/research.py` | Derive fixture permission from `AgentRuntimeMode` |
| `engine/src/nexus/agents/database_tools.py` | Resolve Bar-set authority and fail-closed feature computation |
| `engine/src/nexus/agents/synthesis.py` | Force watch/none and market-state abstention when price authority is absent |
| `engine/tests/integration/test_price_bar_authority_pg.py` | Canonical hash, policy, immutability, direct-SQL and concurrency tests |
| `engine/tests/helpers/price_bar_authority.py` | Deterministic PostgreSQL candidate fixture builder |
| `engine/tests/integration/test_market_ingestion.py` | Atomic ingestion/finalization behavior |
| `engine/tests/integration/test_agent_execution_pg.py` | Admission, runtime policy, tool output and corruption behavior |
| `engine/tests/integration/test_agents_migration.py` | 0010↔0011 residue, function, trigger and extension checks |
| `engine/tests/helpers/migration_schema_expectations.py` | Reflected head table and FK expectations |

---

### Task 1: Add the Bar-set authority schema and reversible migration boundary

**Files:**
- Modify: `engine/src/nexus/persistence/models/data.py:1-205`
- Modify: `engine/src/nexus/persistence/models/research.py:1-52`
- Modify: `engine/alembic/versions/0011_agents.py:1-646`
- Modify: `engine/tests/integration/test_agents_migration.py:14-80`
- Modify: `engine/tests/helpers/migration_schema_expectations.py:3-200`

**Interfaces:**
- Consumes: existing `PriceSeriesManifest`, `ResearchEvidenceRef`, `SourcePolicy`, `ImmutableCreatedAt`, UUIDv7 defaults and Alembic revision `0011_agents`.
- Produces: ORM `PriceBarSetAuthority`; table `price_bar_set_authorities`; exact Bar-set EvidenceRef location form; reversible 0010↔0011 schema.

- [ ] **Step 1: Write failing migration and metadata tests**

Add the new table/FK expectations and extend the 0011 round-trip test with an explicit Task 15A object query:

Keep the existing `test_0012_backtests_is_the_single_head_after_agents` test unchanged. Add the assertions below to the separate 0011 round-trip test so Task 15A validates its own unreleased boundary without taking over Task 16's revision.

```python
def test_0011_round_trips_from_previous_release(
    temporary_database_url: str, monkeypatch
) -> None:
    engine_root = Path(__file__).resolve().parents[2]
    config = Config(str(engine_root / "alembic.ini"))
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(config, "0010_research_data")
    assert _task_15a_tables(temporary_database_url) == set()
    command.upgrade(config, "0011_agents")
    assert _task_15a_tables(temporary_database_url) == {"price_bar_set_authorities"}
    command.downgrade(config, "0010_research_data")
    assert _task_15a_tables(temporary_database_url) == set()
    command.upgrade(config, "0011_agents")
    assert _task_15a_tables(temporary_database_url) == {"price_bar_set_authorities"}


def _task_15a_tables(database_url: str) -> set[str]:
    async def inspect_tables() -> set[str]:
        engine = create_async_engine(database_url)
        try:
            async with engine.connect() as connection:
                names = await connection.run_sync(lambda sync: inspect(sync).get_table_names())
            return {name for name in names if name == "price_bar_set_authorities"}
        finally:
            await engine.dispose()

    return asyncio.run(inspect_tables())
```

Add to reflected expectations:

```python
EXPECTED_TABLES = EXPECTED_TABLES | {"price_bar_set_authorities"}
EXPECTED_FOREIGN_KEYS["price_bar_set_authorities"] = {
    "fk_price_bar_set_authorities_manifest",
    "fk_price_bar_set_authorities_evidence_owner",
    "fk_price_bar_set_authorities_source_policy",
}
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_agents_migration.py engine/tests/integration/test_migration.py -q
```

Expected: FAIL because `price_bar_set_authorities` and its foreign keys do not exist.

- [ ] **Step 3: Add the exact ORM model and EvidenceRef constraint**

Add imports for `Integer` and define the model after `PriceBarRecord`:

```python
class PriceBarSetAuthority(ImmutableCreatedAt, Base):
    """Immutable content authority for one complete normalized Bar manifest."""

    __tablename__ = "price_bar_set_authorities"
    __table_args__ = (
        ForeignKeyConstraint(
            ["evidence_ref_id", "source", "provider_object_id", "revision"],
            [
                "research_evidence_refs.id",
                "research_evidence_refs.source",
                "research_evidence_refs.provider_object_id",
                "research_evidence_refs.revision",
            ],
            name="fk_price_bar_set_authorities_evidence_owner",
        ),
        UniqueConstraint("price_series_manifest_id", name="uq_price_bar_set_authority_manifest"),
        UniqueConstraint("evidence_ref_id", name="uq_price_bar_set_authority_evidence"),
        CheckConstraint("hash_version = 'price_bar_set_sha256_v1'", name="hash_version_v1"),
        CheckConstraint("policy_version = 'agent_price_authority_v1'", name="policy_version_v1"),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash_canonical"),
        CheckConstraint("byte_length > 0", name="byte_length_positive"),
        CheckConstraint("bar_count BETWEEN 1 AND 10000", name="bar_count_bounded"),
        CheckConstraint("first_session <= last_session", name="session_ordered"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    price_series_manifest_id: Mapped[UUID] = mapped_column(
        ForeignKey("price_series_manifests.id", name="fk_price_bar_set_authorities_manifest")
    )
    evidence_ref_id: Mapped[UUID]
    source_policy_id: Mapped[UUID] = mapped_column(
        ForeignKey("source_policies.id", name="fk_price_bar_set_authorities_source_policy")
    )
    source: Mapped[str] = mapped_column(String(100))
    provider_object_id: Mapped[str] = mapped_column(String(255))
    revision: Mapped[str] = mapped_column(String(255))
    hash_version: Mapped[str] = mapped_column(String(64), default="price_bar_set_sha256_v1")
    policy_version: Mapped[str] = mapped_column(String(64), default="agent_price_authority_v1")
    content_hash: Mapped[str] = mapped_column(String(64))
    byte_length: Mapped[int] = mapped_column(BigInteger)
    bar_count: Mapped[int] = mapped_column(Integer)
    first_session: Mapped[date] = mapped_column(Date)
    last_session: Mapped[date] = mapped_column(Date)
    available_at: Mapped[datetime] = mapped_column(UTCDateTime())
```

Extend the `ResearchEvidenceRef.location_content_addressed` ORM expression with this exact alternative:

```python
"(location ~ ('^db://price-bar-set/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
"[0-9a-f]{4}-[0-9a-f]{12}@sha256:' || content_hash || '$')) OR "
```

- [ ] **Step 4: Add the migration table and reversible location constraint**

In `upgrade()`, create `pgcrypto`, replace the location constraint, and create the table with the same names/types as the ORM:

```python
op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
op.drop_constraint(
    op.f("ck_research_evidence_refs_location_content_addressed"),
    "research_evidence_refs",
    type_="check",
)
op.create_check_constraint(
    "location_content_addressed",
    "research_evidence_refs",
    "(location LIKE 'memory://sha256/' || content_hash || '%') OR "
    "(location LIKE 'memory://normalized/market/%/' || content_hash || '.parquet') OR "
    "(location LIKE 'package://%' AND location LIKE '%@sha256:' || content_hash || '%') OR "
    "(location LIKE 's3://%' AND (location LIKE '%/sha256/' || content_hash || '%' "
    "OR location ~ '[?&]versionId=[^&#]+')) OR "
    "(location ~ ('^db://price-bar-set/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    "[0-9a-f]{4}-[0-9a-f]{12}@sha256:' || content_hash || '$'))",
)
```

Create `price_bar_set_authorities` after `price_bar_records`, using `_UUID`, `sa.BigInteger`, `sa.Integer`, `sa.Date`, timezone-aware DateTime, the three named foreign keys and the constraints shown in the ORM. Add it to the created-at trigger loop and `_IMMUTABLE_TABLES` so UPDATE/DELETE fail.

In `downgrade()`, drop Task 15A triggers first, then `price_bar_set_authorities`, restore the prior 0010-compatible location constraint, and deliberately do not execute `DROP EXTENSION pgcrypto`.

- [ ] **Step 5: Run migration and metadata tests GREEN**

Run:

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_agents_migration.py engine/tests/integration/test_migration.py -q
uv run --package nexus-engine ruff check engine/src/nexus/persistence/models engine/tests/integration/test_agents_migration.py
```

Expected: PASS; 0010→0011→0010→0011 leaves exactly the new table and retains a single Alembic head.

- [ ] **Step 6: Commit the schema boundary**

```bash
git add engine/src/nexus/persistence/models/data.py engine/src/nexus/persistence/models/research.py engine/alembic/versions/0011_agents.py engine/tests/integration/test_agents_migration.py engine/tests/helpers/migration_schema_expectations.py
git commit -m "feat(engine): add immutable price bar set authority"
```

---

### Task 2: Implement PostgreSQL canonical hashing, policy validation and write guards

**Files:**
- Create: `engine/tests/helpers/price_bar_authority.py`
- Create: `engine/tests/integration/test_price_bar_authority_pg.py`
- Modify: `engine/alembic/versions/0011_agents.py:1-700`
- Modify: `engine/tests/integration/test_agents_migration.py:24-110`

**Interfaces:**
- Consumes: Task 1 table, existing immutable `source_policy_v1`, `price_bar_records`, PostgreSQL 17 and pgcrypto.
- Produces: `nexus_price_bar_set_canonical_v1(uuid) -> text`; `nexus_prepare_price_bar_set(uuid) -> table`; manifest-level advisory locking; database-enforced eligibility and authority validation.

- [ ] **Step 1: Add deterministic candidate fixtures and RED canonical tests**

Create a helper with this public interface and fixed values from the approved spec:

```python
INSTRUMENT_ID = UUID("018f75b4-4d86-7c36-bc4c-cd71f2f64520")
MANIFEST_ID = UUID("018f75b4-4d86-7c36-bc4c-cd71f2f64521")
OBSERVED_AT = datetime(2026, 8, 24, 20, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 8, 24, 20, 15, tzinfo=UTC)
RECEIVED_AT = datetime(2026, 8, 24, 20, 15, 1, tzinfo=UTC)


@dataclass(frozen=True)
class SeededPriceCandidate:
    instrument_id: UUID
    manifest_id: UUID
    bar_ids: tuple[UUID, ...]


async def seed_price_candidate(
    factory: async_sessionmaker[AsyncSession],
    *,
    manifest_id: UUID = MANIFEST_ID,
    source: str = "alpaca",
    feed: str = "sip",
    delivery_mode: str = "delayed",
    interval: str = "1Day",
    adjustment: str = "all",
    quality_flags: tuple[str, ...] = (),
    bar_count: int = 1,
    manifest_overrides: Mapping[str, object] | None = None,
    bar_overrides: Mapping[str, object] | None = None,
) -> SeededPriceCandidate:
    """Insert one manifest whose stored boundaries exactly match its Bars."""
    manifest_overrides = dict(manifest_overrides or {})
    bar_overrides = dict(bar_overrides or {})
    artifact_digest = sha256(str(manifest_id).encode()).hexdigest()
    artifact_id = new_uuid7()
    parquet_evidence_id = new_uuid7()
    bar_ids = tuple(new_uuid7() for _ in range(bar_count))
    rows: list[PriceBarRecord] = []
    for index, bar_id in enumerate(bar_ids):
        days_back = bar_count - index - 1
        observed_at = OBSERVED_AT - timedelta(days=days_back)
        values: dict[str, object] = {
            "id": bar_id,
            "price_series_manifest_id": manifest_id,
            "instrument_id": INSTRUMENT_ID,
            "session": observed_at.date(),
            "open": Decimal("101.000000000000"),
            "high": Decimal("103.000000000000"),
            "low": Decimal("100.000000000000"),
            "close": Decimal("102.000000000000"),
            "volume": Decimal("1200000.000000000000"),
            "observed_at": observed_at,
            "available_at": AVAILABLE_AT - timedelta(days=days_back),
            "received_at": RECEIVED_AT - timedelta(days=days_back),
            "revision": "1",
            "quality_flags": list(quality_flags),
        }
        if index == bar_count - 1:
            values.update(bar_overrides)
        rows.append(PriceBarRecord(**values))
    observed_values = [row.observed_at for row in rows]
    async with factory() as session, session.begin():
        if await session.get(Instrument, INSTRUMENT_ID) is None:
            session.add(
                Instrument(
                    id=INSTRUMENT_ID,
                    asset_type="equity",
                    exchange="NASDAQ",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="equity",
                )
            )
        session.add(
            DataArtifact(
                id=artifact_id,
                uri=f"memory://sha256/{artifact_digest}",
                sha256=artifact_digest,
                byte_size=471,
                mime_type="application/vnd.apache.parquet",
                schema_version="price_bars_v1",
                source=source,
                observed_at=min(observed_values),
                available_at=max(row.available_at for row in rows),
                received_at=max(row.received_at for row in rows),
            )
        )
        session.add(
            ResearchEvidenceRef(
                id=parquet_evidence_id,
                source=source,
                provider_object_id=f"price-series-manifest:{manifest_id}",
                revision=artifact_digest,
                available_at=max(row.available_at for row in rows),
                location=f"memory://sha256/{artifact_digest}",
                content_hash=artifact_digest,
                byte_length=471,
            )
        )
        await session.flush()
        manifest_values: dict[str, object] = {
            "id": manifest_id,
            "evidence_ref_id": parquet_evidence_id,
            "data_artifact_id": artifact_id,
            "instrument": str(INSTRUMENT_ID),
            "source": source,
            "feed": feed,
            "delivery_mode": delivery_mode,
            "adjustment": adjustment,
            "interval": interval,
            "start_at": min(observed_values),
            "end_at": max(observed_values),
            "row_count": bar_count,
        }
        manifest_values.update(manifest_overrides)
        session.add(PriceSeriesManifest(**manifest_values))
        await session.flush()
        session.add_all(rows)
    return SeededPriceCandidate(INSTRUMENT_ID, manifest_id, bar_ids)
```

The helper must insert one shared `Instrument`, a unique `DataArtifact`/Parquet EvidenceRef per manifest, the manifest, and `bar_count` Bars with one distinct session each and matching start/end/count. The unmodified one-Bar values are exactly:

```python
bar_values: dict[str, object] = {
    "instrument_id": INSTRUMENT_ID,
    "session": date(2026, 8, 24),
    "open": Decimal("101.000000000000"),
    "high": Decimal("103.000000000000"),
    "low": Decimal("100.000000000000"),
    "close": Decimal("102.000000000000"),
    "volume": Decimal("1200000.000000000000"),
    "observed_at": OBSERVED_AT,
    "available_at": AVAILABLE_AT,
    "received_at": RECEIVED_AT,
    "revision": "1",
    "quality_flags": list(quality_flags),
}
bar_values.update(bar_overrides or {})
```

For multiple Bars, subtract whole days from session and all three timestamps so the final Bar retains the values above. Derive artifact SHA with `sha256(str(manifest_id).encode()).hexdigest()`; this is only test Parquet identity, never the Bar-set hash. Merge `manifest_overrides` only after deriving correct defaults so count/time mismatch cases are explicit. It must not insert `PriceBarSetAuthority`.

Add the exact golden assertion:

```python
EXPECTED_CANONICAL = (
    '{"bars": [["018f75b4-4d86-7c36-bc4c-cd71f2f64520", "2026-08-24", '
    '101.000000000000, 103.000000000000, 100.000000000000, 102.000000000000, '
    '1200000.000000000000, "2026-08-24T20:00:00.000000Z", '
    '"2026-08-24T20:15:00.000000Z", "2026-08-24T20:15:01.000000Z", "1", []]], '
    '"schema": "price_bar_set_sha256_v1", "manifest": {"feed": "sip", '
    '"source": "alpaca", "interval": "1Day", "adjustment": "all", '
    '"instrument": "018f75b4-4d86-7c36-bc4c-cd71f2f64520", '
    '"delivery_mode": "delayed"}}'
)
EXPECTED_HASH = "a7ff0e567da70d9323da75fffa2e1258f258360e12050f0147e40139585c2cc6"


@pytest.mark.asyncio
async def test_price_bar_set_v1_has_a_fixed_postgresql_17_golden_hash(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await seed_price_candidate(factory)
    async with factory() as session:
        canonical = await session.scalar(
            text("SELECT nexus_price_bar_set_canonical_v1(:manifest_id)"),
            {"manifest_id": MANIFEST_ID},
        )
        prepared = (
            await session.execute(
                text("SELECT * FROM nexus_prepare_price_bar_set(:manifest_id)"),
                {"manifest_id": MANIFEST_ID},
            )
        ).mappings().one()
    assert canonical == EXPECTED_CANONICAL
    assert prepared["content_hash"] == EXPECTED_HASH
    assert prepared["byte_length"] == 471
```

Add this parameter matrix; each case uses a second manifest with the same instrument and only the named override, then asserts canonical text differs:

```python
CANONICAL_MUTATIONS = (
    ({"source": "fosun", "feed": "sip", "delivery_mode": "delayed"}, {}),
    ({"feed": "iex"}, {}),
    ({"delivery_mode": "real_time"}, {}),
    ({"adjustment": "raw"}, {}),
    ({"interval": "1Min"}, {}),
    ({}, {"session": date(2026, 8, 23)}),
    ({}, {"open": Decimal("101.100000000000")}),
    ({}, {"high": Decimal("103.100000000000")}),
    ({}, {"low": Decimal("99.900000000000")}),
    ({}, {"close": Decimal("102.100000000000")}),
    ({}, {"volume": Decimal("1200001.000000000000")}),
    ({}, {"observed_at": OBSERVED_AT + timedelta(microseconds=1)}),
    ({}, {"available_at": AVAILABLE_AT + timedelta(microseconds=1)}),
    ({}, {"received_at": RECEIVED_AT + timedelta(microseconds=1)}),
    ({}, {"revision": "2"}),
    ({}, {"quality_flags": ["suspect"]}),
)
```

Use a separate explicit mixed-instrument case because the manifest instrument and Bar instrument must be varied independently. Insert equivalent multi-Bar manifests in opposite insertion order and with different Bar IDs; assert canonical text is identical.

- [ ] **Step 2: Run the canonical tests and verify RED**

Run:

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_price_bar_authority_pg.py -q
```

Expected: FAIL with undefined `nexus_price_bar_set_canonical_v1`/`nexus_prepare_price_bar_set`.

- [ ] **Step 3: Add canonical and prepare functions**

Create canonical JSON only with PostgreSQL JSONB builders. The core correlated Bar aggregation must be:

```sql
jsonb_build_object(
  'schema', 'price_bar_set_sha256_v1',
  'manifest', jsonb_build_object(
    'instrument', manifest.instrument,
    'source', manifest.source,
    'feed', manifest.feed,
    'delivery_mode', manifest.delivery_mode,
    'adjustment', manifest.adjustment,
    'interval', manifest.interval
  ),
  'bars', COALESCE((
    SELECT jsonb_agg(
      jsonb_build_array(
        bar.instrument_id::text,
        to_char(bar.session, 'YYYY-MM-DD'),
        bar.open, bar.high, bar.low, bar.close, bar.volume,
        to_char(bar.observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        to_char(bar.available_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        to_char(bar.received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        bar.revision,
        COALESCE((
          SELECT jsonb_agg(flag.value ORDER BY flag.value)
          FROM jsonb_array_elements_text(bar.quality_flags) AS flag(value)
        ), '[]'::jsonb)
      ) ORDER BY bar.session, bar.observed_at, bar.revision, bar.instrument_id
    )
    FROM price_bar_records AS bar
    WHERE bar.price_series_manifest_id = manifest.id
  ), '[]'::jsonb)
)::text
```

`nexus_prepare_price_bar_set` must acquire:

```sql
PERFORM pg_advisory_xact_lock(
  hashtextextended('price-series-manifest:' || p_manifest_id::text, 0)
);
```

It then loads manifest/count/min/max/session-count/max-received, checks exactly:

```sql
manifest.interval = '1Day'
AND (
  (manifest.source = 'alpaca' AND manifest.feed = 'sip'
   AND manifest.delivery_mode = 'delayed')
  OR (manifest.source = 'fixture' AND manifest.feed <> ''
      AND manifest.delivery_mode = 'fixture')
)
AND actual_count BETWEEN 1 AND 10000
AND actual_count = manifest.row_count
AND first_observed = manifest.start_at
AND last_observed = manifest.end_at
AND actual_count = distinct_session_count
AND NOT EXISTS (
  SELECT 1 FROM price_bar_records bar
  WHERE bar.price_series_manifest_id = manifest.id
    AND (
      bar.instrument_id::text <> manifest.instrument
      OR bar.observed_at > bar.available_at
      OR bar.available_at > bar.received_at
      OR jsonb_typeof(bar.quality_flags) <> 'array'
      OR bar.quality_flags <> '[]'::jsonb
    )
)
```

It must also require this exact immutable source-policy record. When any predicate fails, execute plain `RETURN` with zero rows:

```sql
policy.id = '3c60888d-e92d-5e04-913c-7d427004cfef'::uuid
AND policy.key = 'market_price'
AND policy.version = 'source_policy_v1'
AND policy.schema_version = '1'
AND policy.payload = '{
  "priority":["fosun","alpaca","fixture"],
  "fosun":{"mode":"read_only","operation":"fosun_get_bars","market_scope":"US"},
  "alpaca":{"feed":"sip","modes":["delayed","fixture"]},
  "fixture":{"mode":"fixture"}
}'::jsonb
```

Otherwise compute:

```sql
content_hash := encode(digest(convert_to(canonical_text, 'UTF8'), 'sha256'), 'hex');
byte_length := octet_length(convert_to(canonical_text, 'UTF8'));
```

and return count/session/`max(received_at)` with the digest.

- [ ] **Step 4: Add authority validation and mutation guards**

Add `nexus_validate_price_bar_set_authority()` as a `BEFORE INSERT` trigger. It calls `nexus_prepare_price_bar_set`, rejects zero rows, loads the EvidenceRef, and requires:

```sql
NEW.source_policy_id = '3c60888d-e92d-5e04-913c-7d427004cfef'::uuid
AND NEW.hash_version = 'price_bar_set_sha256_v1'
AND NEW.policy_version = 'agent_price_authority_v1'
AND NEW.source = manifest.source
AND NEW.provider_object_id = 'price-bar-set:' || manifest.id::text
AND NEW.revision = prepared.content_hash
AND NEW.content_hash = prepared.content_hash
AND NEW.byte_length = prepared.byte_length
AND NEW.bar_count = prepared.bar_count
AND NEW.first_session = prepared.first_session
AND NEW.last_session = prepared.last_session
AND NEW.available_at = prepared.available_at
AND evidence.location = 'db://price-bar-set/' || manifest.id::text
  || '@sha256:' || prepared.content_hash
```

Any mismatch raises SQLSTATE `23514` with the safe message `price bar set authority is invalid`.

Replace the old frozen-insert trigger with `nexus_guard_price_bar_set_mutation()`. Name its trigger `trg_00_price_bar_records_authority_guard` so PostgreSQL executes it before the existing immutable trigger. It locks on NEW/OLD manifest ID and rejects INSERT when an authority exists; UPDATE/DELETE return to the existing append-only rejection after taking the same lock.

- [ ] **Step 5: Add direct-SQL policy and immutability tests**

Add tests that use raw INSERT to try a non-SIP Alpaca authority, forged policy ID, forged hash/location/count, flagged Bar, duplicate session and mixed instrument. Each must raise `DBAPIError`/return no prepare row. Add:

```python
@pytest.mark.asyncio
async def test_finalized_price_bar_set_cannot_grow_or_change(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    seeded = await seed_price_candidate(factory)
    await insert_valid_authority_from_prepare(factory, seeded.manifest_id)
    async with factory() as session:
        with pytest.raises(DBAPIError):
            async with session.begin():
                session.add(clean_bar(manifest_id=seeded.manifest_id, session=date(2026, 8, 25)))
                await session.flush()
    with pytest.raises(DBAPIError):
        async with factory() as session, session.begin():
            authority = await session.scalar(select(PriceBarSetAuthority))
            assert authority is not None
            await session.delete(authority)
            await session.flush()
```

Define the helper used above in `engine/tests/helpers/price_bar_authority.py`:

```python
async def insert_valid_authority_from_prepare(
    factory, manifest_id: UUID
) -> PriceBarSetAuthority:
    async with factory() as session, session.begin():
        manifest = await session.get(PriceSeriesManifest, manifest_id)
        assert manifest is not None
        prepared = (
            await session.execute(
                text("SELECT * FROM nexus_prepare_price_bar_set(:manifest_id)"),
                {"manifest_id": manifest_id},
            )
        ).mappings().one()
        digest = str(prepared["content_hash"])
        evidence = ResearchEvidenceRef(
            source=manifest.source,
            provider_object_id=f"price-bar-set:{manifest_id}",
            revision=digest,
            available_at=prepared["available_at"],
            location=f"db://price-bar-set/{manifest_id}@sha256:{digest}",
            content_hash=digest,
            byte_length=int(prepared["byte_length"]),
        )
        session.add(evidence)
        await session.flush()
        authority = PriceBarSetAuthority(
            price_series_manifest_id=manifest_id,
            evidence_ref_id=evidence.id,
            source_policy_id=UUID("3c60888d-e92d-5e04-913c-7d427004cfef"),
            source=manifest.source,
            provider_object_id=evidence.provider_object_id,
            revision=digest,
            hash_version="price_bar_set_sha256_v1",
            policy_version="agent_price_authority_v1",
            content_hash=digest,
            byte_length=evidence.byte_length,
            bar_count=int(prepared["bar_count"]),
            first_session=prepared["first_session"],
            last_session=prepared["last_session"],
            available_at=prepared["available_at"],
        )
        session.add(authority)
        await session.flush()
    return authority
```

Also expose this constructor from the helper:

```python
def clean_bar(*, manifest_id: UUID, session: date) -> PriceBarRecord:
    observed_at = datetime.combine(session, time(20), tzinfo=UTC)
    return PriceBarRecord(
        price_series_manifest_id=manifest_id,
        instrument_id=INSTRUMENT_ID,
        session=session,
        open=Decimal("101.000000000000"),
        high=Decimal("103.000000000000"),
        low=Decimal("100.000000000000"),
        close=Decimal("102.000000000000"),
        volume=Decimal("1200000.000000000000"),
        observed_at=observed_at,
        available_at=observed_at + timedelta(minutes=15),
        received_at=observed_at + timedelta(minutes=15, seconds=1),
        revision="1",
        quality_flags=[],
    )
```

- [ ] **Step 6: Run Task 2 tests GREEN and commit**

Run:

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_price_bar_authority_pg.py engine/tests/integration/test_agents_migration.py -q
uv run --package nexus-engine ruff check engine/tests/helpers/price_bar_authority.py engine/tests/integration/test_price_bar_authority_pg.py engine/alembic/versions/0011_agents.py
```

Expected: PASS with the fixed hash `a7ff0e567da70d9323da75fffa2e1258f258360e12050f0147e40139585c2cc6`, all direct-SQL bypasses rejected, and authority/Bar rows immutable.

```bash
git add engine/alembic/versions/0011_agents.py engine/tests/helpers/price_bar_authority.py engine/tests/integration/test_price_bar_authority_pg.py engine/tests/integration/test_agents_migration.py
git commit -m "feat(engine): bind canonical price bars to evidence"
```

---

### Task 3: Finalize eligible Bar sets during atomic market ingestion

**Files:**
- Create: `engine/src/nexus/market/price_bar_authority.py`
- Modify: `engine/src/nexus/market/ingestion.py:1-150`
- Modify: `engine/tests/integration/test_market_ingestion.py:1-253`

**Interfaces:**
- Consumes: Task 2 `nexus_prepare_price_bar_set(uuid)` and Task 1 ORM.
- Produces: `PriceBarSetPreparation`; `prepare_price_bar_set(session, manifest_id)`; `finalize_price_bar_set(session, manifest)`; optional authority/evidence in `MarketIngestionResult`.

- [ ] **Step 1: Write failing clean/flagged ingestion tests**

Preserve the current synthetic flagged `_page()` expectation of one Parquet evidence and no Bar-set authority. Add a clean page and assert semantic separation:

```python
def _clean_page() -> ProviderPage[PriceBar]:
    clean_bar = _bars()[0].model_copy(update={"quality_flags": ()})
    return _page().model_copy(update={"items": (clean_bar,)})


@pytest.mark.asyncio
async def test_clean_ingestion_atomically_finalizes_db_bar_set_evidence(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        result = await ingest_price_page(session, FakeObjectStore(), _clean_page())
    assert result.price_authority is not None
    assert result.price_evidence_ref is not None
    assert result.evidence_ref.content_hash == result.artifact.sha256
    assert result.price_evidence_ref.content_hash == result.price_authority.content_hash
    assert result.price_evidence_ref.location == (
        f"db://price-bar-set/{result.manifest.id}"
        f"@sha256:{result.price_authority.content_hash}"
    )
    assert result.price_evidence_ref.content_hash != result.artifact.sha256


@pytest.mark.asyncio
async def test_flagged_ingestion_remains_diagnostic_only(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        result = await ingest_price_page(session, FakeObjectStore(), _page())
    assert result.price_authority is None
    assert result.price_evidence_ref is None
```

- [ ] **Step 2: Run ingestion tests and verify RED**

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_market_ingestion.py -q
```

Expected: FAIL because `MarketIngestionResult` has no Bar-set fields and ingestion never finalizes.

- [ ] **Step 3: Implement the typed DB service without Python hashing**

Create:

```python
SOURCE_POLICY_ID = UUID("3c60888d-e92d-5e04-913c-7d427004cfef")
HASH_VERSION = "price_bar_set_sha256_v1"
POLICY_VERSION = "agent_price_authority_v1"


@dataclass(frozen=True)
class PriceBarSetPreparation:
    content_hash: str
    byte_length: int
    bar_count: int
    first_session: date
    last_session: date
    available_at: datetime


async def prepare_price_bar_set(
    session: AsyncSession, manifest_id: UUID
) -> PriceBarSetPreparation | None:
    row = (
        await session.execute(
            text("SELECT * FROM nexus_prepare_price_bar_set(:manifest_id)"),
            {"manifest_id": manifest_id},
        )
    ).mappings().one_or_none()
    return None if row is None else PriceBarSetPreparation(**dict(row))


async def finalize_price_bar_set(
    session: AsyncSession, manifest: PriceSeriesManifest
) -> tuple[PriceBarSetAuthority, ResearchEvidenceRef] | None:
    prepared = await prepare_price_bar_set(session, manifest.id)
    if prepared is None:
        return None
    provider_object_id = f"price-bar-set:{manifest.id}"
    evidence = ResearchEvidenceRef(
        source=manifest.source,
        provider_object_id=provider_object_id,
        revision=prepared.content_hash,
        available_at=prepared.available_at,
        location=(
            f"db://price-bar-set/{manifest.id}@sha256:{prepared.content_hash}"
        ),
        content_hash=prepared.content_hash,
        byte_length=prepared.byte_length,
    )
    session.add(evidence)
    await session.flush()
    authority = PriceBarSetAuthority(
        price_series_manifest_id=manifest.id,
        evidence_ref_id=evidence.id,
        source_policy_id=SOURCE_POLICY_ID,
        source=manifest.source,
        provider_object_id=provider_object_id,
        revision=prepared.content_hash,
        hash_version=HASH_VERSION,
        policy_version=POLICY_VERSION,
        content_hash=prepared.content_hash,
        byte_length=prepared.byte_length,
        bar_count=prepared.bar_count,
        first_session=prepared.first_session,
        last_session=prepared.last_session,
        available_at=prepared.available_at,
    )
    session.add(authority)
    await session.flush()
    return authority, evidence
```

- [ ] **Step 4: Integrate finalization into the existing transaction**

After the existing Bar generator, flush Bars, call `finalize_price_bar_set`, then commit. The edited block is:

```python
session.add_all(
    PriceBarRecord(
        price_series_manifest_id=manifest.id,
        instrument_id=bar.instrument_id,
        session=bar.session,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        observed_at=bar.observed_at or bar.available_at,
        available_at=bar.available_at,
        received_at=bar.received_at or bar.available_at,
        revision=bar.revision,
        quality_flags=list(bar.quality_flags),
    )
    for bar in page.items
)
await session.flush()
finalized = await finalize_price_bar_set(session, manifest)
```

Extend the result without changing `evidence_ref` semantics. Reuse the `finalized` value computed immediately after the Bar flush; do not flush or call `finalize_price_bar_set` a second time:

```python
@dataclass(frozen=True)
class MarketIngestionResult:
    raw_uri: str
    artifact: DataArtifact
    manifest: PriceSeriesManifest
    evidence_ref: ResearchEvidenceRef
    provenance: MarketDataProvenance
    price_authority: PriceBarSetAuthority | None
    price_evidence_ref: ResearchEvidenceRef | None

price_authority = None if finalized is None else finalized[0]
price_evidence_ref = None if finalized is None else finalized[1]
await session.commit()
```

Do not catch authority constraint errors separately: evidence, authority, manifest and Bars must roll back together. Preserve temporary-object cleanup and never delete the promoted shared object on duplicate ingestion.

- [ ] **Step 5: Run ingestion and type gates GREEN; commit**

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_market_ingestion.py -q
uv run --package nexus-engine ruff check engine/src/nexus/market engine/tests/integration/test_market_ingestion.py
uv run --package nexus-engine mypy engine/src/nexus/market
git add engine/src/nexus/market/price_bar_authority.py engine/src/nexus/market/ingestion.py engine/tests/integration/test_market_ingestion.py
git commit -m "feat(engine): finalize clean price bar evidence"
```

Expected: clean data creates two semantically separate EvidenceRefs in one transaction; flagged/non-approved data persists only archive/diagnostic records.

---

### Task 4: Freeze only runtime-eligible Bar-set authority into Agent snapshots

**Files:**
- Modify: `engine/alembic/versions/0011_agents.py:430-505`
- Modify: `engine/src/nexus/agents/price_authority.py:1-94`
- Modify: `engine/src/nexus/agents/persistence.py:221-305`
- Modify: `engine/src/nexus/api/routes/research.py:130-170`
- Modify: `engine/tests/integration/test_agent_execution_pg.py:904-1125,1571-1800`

**Interfaces:**
- Consumes: Task 3 authority/evidence; fixed `SOURCE_POLICY_ID`, `POLICY_VERSION`; `EngineSettings.agent_runtime_mode`.
- Produces: `freeze_price_authority(..., allow_fixture: bool = False)`; `admit_research_run(..., allow_fixture_price_authority: bool = False)`; database price-link owner enforcement.

- [ ] **Step 1: Rewrite fixture seeding and add RED runtime/source-bypass tests**

Update `_seed_database_tool_authority` so it creates the Parquet EvidenceRef first, inserts Bars, calls `finalize_price_bar_set`, then optionally links the returned Bar-set EvidenceRef. Stop using the Parquet evidence ID as the Agent price authority.

Change the existing assertion to:

```python
price_items = [
    item
    for item in admission.run.evidence_manifest
    if item["provider_object_id"].startswith("price-bar-set:")
]
assert len(price_items) == 1
assert price_items[0]["location"].startswith("db://price-bar-set/")
assert "@sha256:" in price_items[0]["location"]
```

Add these exact behaviors:

```python
async def _admit_price_run(
    session: AsyncSession,
    prompt_ids: Mapping[AgentRole, UUID],
    *,
    allow_fixture_price_authority: bool,
) -> ResearchAdmission:
    return await admit_research_run(
        session,
        actor_id=f"price-authority-{allow_fixture_price_authority}",
        route="/internal/v1/research-runs",
        idempotency_key=f"price-authority-{allow_fixture_price_authority}",
        request_hash=("8" if allow_fixture_price_authority else "9") * 64,
        request=ResearchRequest(
            instrument_id=_INSTRUMENT_ID,
            data_snapshot_id=_SNAPSHOT_ID,
            decision_time=_NOW,
        ),
        prompt_version_id=prompt_ids["orchestrator"],
        model_config_id=_MODEL_ID,
        daily_limit_usd=Decimal("3.00"),
        now=_NOW,
        allow_fixture_price_authority=allow_fixture_price_authority,
    )


@pytest.mark.asyncio
async def test_fixture_price_authority_requires_explicit_fixture_admission(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(factory, link_price=False, price_source="fixture")
    with pytest.raises(ValueError, match="price authority"):
        async with factory() as session, session.begin():
            await _admit_price_run(
                session, prompt_ids, allow_fixture_price_authority=False
            )
    async with factory() as session, session.begin():
        admission = await _admit_price_run(
            session, prompt_ids, allow_fixture_price_authority=True
        )
    assert any(
        item["provider_object_id"].startswith("price-bar-set:")
        for item in admission.run.evidence_manifest
    )
```

Add a direct ORM/SQL-equivalent attempt to link the Parquet EvidenceRef as price authority; the database trigger must reject it:

```python
with pytest.raises(DBAPIError):
    async with factory() as session, session.begin():
        manifest = await session.get(PriceSeriesManifest, _PRICE_MANIFEST_ID)
        assert manifest is not None and manifest.evidence_ref_id is not None
        evidence = await session.get(ResearchEvidenceRef, manifest.evidence_ref_id)
        assert evidence is not None
        session.add(
            AgentDataSnapshotEvidenceRecord(
                data_snapshot_id=_SNAPSHOT_ID,
                instrument_id=_INSTRUMENT_ID,
                evidence_ref_id=evidence.id,
                authority_kind="price",
                selected_at=_NOW,
                source=evidence.source,
                provider_object_id=evidence.provider_object_id,
                revision=evidence.revision,
                available_at=evidence.available_at,
                location=evidence.location,
                content_hash=evidence.content_hash,
            )
        )
        await session.flush()
```

Add an existing legal fixture link followed by `allow_fixture=False`; expect admission failure and assert:

```python
async with factory() as session:
    assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
    assert await session.scalar(select(func.count()).select_from(Job)) == 0
    assert (
        await session.scalar(
            select(func.count()).select_from(AgentDailyBudgetReservationRecord)
        )
        == 0
    )
```

Add a separate flagged/non-eligible candidate case with no existing price link:

```python
await _seed_database_tool_authority(
    factory,
    link_price=False,
    price_source="fixture",
    price_quality_flags=("suspect",),
)
async with factory() as session, session.begin():
    admission = await _admit_price_run(
        session,
        prompt_ids,
        allow_fixture_price_authority=True,
    )
assert not any(
    item["provider_object_id"].startswith("price-bar-set:")
    for item in admission.run.evidence_manifest
)
```

Extend `_seed_database_tool_authority` with `price_quality_flags: tuple[str, ...] = ()` and persist it on every seeded Bar. This proves “no candidate” degrades research instead of being mistaken for audit corruption.

- [ ] **Step 2: Run focused Agent tests and verify RED**

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_agent_execution_pg.py -k 'price_authority or price_owner' -q
```

Expected: FAIL because the selector still joins Parquet evidence and the existing-link fast path skips policy validation.

- [ ] **Step 3: Replace the database snapshot price-owner path**

In `nexus_validate_agent_snapshot_evidence`, change only the `authority_kind='price'` branch to join:

```sql
price_bar_set_authorities bar_set
JOIN price_series_manifests manifest
  ON manifest.id = bar_set.price_series_manifest_id
JOIN research_evidence_refs evidence
  ON evidence.id = bar_set.evidence_ref_id
JOIN source_policies policy
  ON policy.id = bar_set.source_policy_id
```

Require the fixed owner/hash/location/version/policy UUID, `manifest.instrument = NEW.instrument_id::text`, `bar_set.available_at = NEW.available_at`, `bar_set.available_at <= NEW.selected_at`, and `NEW.selected_at <= snapshot_time`. The branch must not join `manifest.evidence_ref_id` or `DataArtifact` to prove price authority. Keep the existing research branch unchanged.

- [ ] **Step 4: Implement runtime-safe selection and existing-link validation**

Change the signature:

```python
async def freeze_price_authority(
    session: AsyncSession,
    *,
    snapshot: DataSnapshot,
    instrument_id: UUID,
    decision_time: datetime,
    allow_fixture: bool = False,
) -> None:
```

Use one helper for both existing and new links:

```python
def _runtime_eligible(manifest: PriceSeriesManifest, *, allow_fixture: bool) -> bool:
    if (
        manifest.source == "alpaca"
        and manifest.feed == "sip"
        and manifest.delivery_mode == "delayed"
        and manifest.interval == "1Day"
    ):
        return True
    return (
        allow_fixture
        and manifest.source == "fixture"
        and bool(manifest.feed)
        and manifest.delivery_mode == "fixture"
        and manifest.interval == "1Day"
    )
```

The selection query must join `PriceBarSetAuthority -> ResearchEvidenceRef -> PriceSeriesManifest`, constrain fixed hash/policy/source-policy versions, instrument, available time and runtime source. Order eligible rows deterministically by production source before fixture, then `PriceBarSetAuthority.available_at DESC`, then `PriceSeriesManifest.id DESC`, and select one row. The existing-link path must load the same tuple and raise `ValueError("existing price authority is invalid")` when owner fields or runtime eligibility fail; it must never return solely because an evidence ID exists.

- [ ] **Step 5: Thread fixture authority explicitly from API composition**

Add a secure default to admission:

```python
async def admit_research_run(
    session: AsyncSession,
    *,
    actor_id: str,
    route: str,
    idempotency_key: str,
    request_hash: str,
    request: ResearchRequest,
    prompt_version_id: UUID | None,
    model_config_id: UUID | None,
    daily_limit_usd: Decimal,
    now: datetime,
    allow_fixture_price_authority: bool = False,
    after_authority: Callable[[], Awaitable[None]] | None = None,
) -> ResearchAdmission:
```

Pass it to `freeze_price_authority`. In the API route import `AgentRuntimeMode` and pass:

```python
allow_fixture_price_authority=(
    settings.agent_runtime_mode is AgentRuntimeMode.FIXTURE
),
```

Responses mode therefore rejects fixture even in development. Update only price-specific direct tests to pass True when they intentionally use fixture.

- [ ] **Step 6: Run Agent admission tests GREEN; commit**

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_agent_execution_pg.py -k 'price_authority or price_owner or admission' -q
uv run --package nexus-engine pytest engine/tests/unit/api/test_research_routes.py -q
uv run --package nexus-engine ruff check engine/src/nexus/agents/price_authority.py engine/src/nexus/agents/persistence.py engine/src/nexus/api/routes/research.py engine/tests/integration/test_agent_execution_pg.py
git add engine/alembic/versions/0011_agents.py engine/src/nexus/agents/price_authority.py engine/src/nexus/agents/persistence.py engine/src/nexus/api/routes/research.py engine/tests/integration/test_agent_execution_pg.py
git commit -m "fix(engine): freeze policy-bound price bar authority"
```

---

### Task 5: Compute price features only from the frozen Bar-set evidence

**Files:**
- Modify: `engine/src/nexus/agents/database_tools.py:1-275`
- Modify: `engine/src/nexus/agents/synthesis.py:55-170`
- Modify: `engine/tests/integration/test_agent_execution_pg.py:735-1125`
- Modify: `engine/tests/unit/agents/test_execution.py:90-168`
- Verify unchanged: `engine/src/nexus/agents/price_features.py`

**Interfaces:**
- Consumes: Task 3 `prepare_price_bar_set`; Task 4 frozen `PriceBarSetAuthority`; existing `compute_price_features` Decimal allowlist.
- Produces: deterministic `ok/partial/unavailable` results tied to the Bar-set EvidenceRef; stable `flagged_price_bars` and `invalid_price_bar_set_authority` reasons.

- [ ] **Step 1: Add RED evidence, corruption and flagged-data tests**

Keep normal close/SMA/RSI/volatility and warm-up assertions. Assert the returned evidence ID is the Bar-set EvidenceRef, not the Parquet one. Add corruption tests that deliberately simulate disabled-trigger storage damage and always re-enable triggers in `finally`:

```python
_PRICE_MANIFEST_ID = UUID("0198d500-1000-7000-8000-000000001707")


def _price_tool_payload() -> dict[str, object]:
    return {
        "instrument_id": str(_INSTRUMENT_ID),
        "data_snapshot_id": str(_SNAPSHOT_ID),
        "as_of": _NOW,
        "features": ["close", "sma_20", "rsi_14", "volatility_20"],
    }


async def _prepared_price_run(database_url: str):
    factory = session_factory(EngineSettings(database_url=database_url))
    prompt_ids = await _seed_authority(factory)
    await _seed_database_tool_authority(factory, link_price=False, bar_count=200)
    async with factory() as session, session.begin():
        admission = await _admit_price_run(
            session,
            prompt_ids,
            allow_fixture_price_authority=True,
        )
    return factory, admission.run


@pytest.mark.asyncio
async def test_price_tool_never_returns_ok_for_flagged_stored_bars(
    migrated_database_url: str,
) -> None:
    factory, run = await _prepared_price_run(migrated_database_url)
    async with factory() as session, session.begin():
        await session.execute(text("ALTER TABLE price_bar_records DISABLE TRIGGER USER"))
        try:
            await session.execute(
                text(
                    "UPDATE price_bar_records SET quality_flags='[\"suspect\"]'::jsonb "
                    "WHERE price_series_manifest_id=:manifest_id"
                ),
                {"manifest_id": _PRICE_MANIFEST_ID},
            )
        finally:
            await session.execute(text("ALTER TABLE price_bar_records ENABLE TRIGGER USER"))
    async with factory() as session:
        tools = await database_read_only_tools(session, run, allow_fixture_evidence=True)
        result = await tools.execute("get_price_features", _price_tool_payload())
    assert (result["status"], result["reason"]) == (
        "unavailable",
        "flagged_price_bars",
    )
    assert result["items"] == []
```

Add a second corruption case that executes this mutation under the same disable/finally-enable discipline and expects `unavailable/invalid_price_bar_set_authority`:

```sql
UPDATE price_bar_records
SET close = close + 0.010000000000
WHERE id = (
  SELECT id FROM price_bar_records
  WHERE price_series_manifest_id = :manifest_id
  ORDER BY session DESC
  LIMIT 1
)
```

Add a request with `instrument_id=UUID("0198d500-1000-7000-8000-000000009999")`, then one with a different snapshot UUID, then `as_of=_NOW + timedelta(seconds=1)`; each must retain `ToolInputError` because caller-scope violations are not stored-data degradation.

Update `_synthesize` in `test_execution.py` to accept `include_price_authority: bool = True` and build evidence exactly as follows before creating role outputs:

```python
def _synthesize(
    votes: tuple[str, ...], *, include_price_authority: bool = True
):
    now = datetime(2026, 8, 24, tzinfo=UTC)
    content_hash = "a" * 64
    object_id = (
        "price-bar-set:018f75b4-4d86-7c36-bc4c-cd71f2f64521"
        if include_price_authority
        else "filing:0000000001-26-000001"
    )
    location = (
        "db://price-bar-set/018f75b4-4d86-7c36-bc4c-cd71f2f64521"
        f"@sha256:{content_hash}"
        if include_price_authority
        else f"memory://sha256/{content_hash}"
    )
    evidence = EvidenceRef(
        source="fixture" if include_price_authority else "sec",
        object_id=object_id,
        available_at=now,
        location=location,
        content_hash=content_hash,
    )
```

Retain the existing run/model/prompt/output construction below this block. Add:

```python
def test_missing_price_authority_forces_market_abstention_and_watch() -> None:
    intent = _synthesize(
        ("support", "support", "support", "support", "support", "oppose", "abstain", "abstain"),
        include_price_authority=False,
    )
    market_vote = next(vote for vote in intent.role_votes if vote.role == "market_state")
    assert market_vote.vote == "abstain"
    assert (intent.stance, intent.exposure_action) == ("watch", "none")
    assert intent.quality_status == "partial"
    assert "Price authority is unavailable." in intent.uncertainties
```

- [ ] **Step 2: Run focused tool tests and verify RED**

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_agent_execution_pg.py -k 'price_features' -q
```

Expected: FAIL because the tool still proves authority with `DataArtifact.sha256` and ignores flags.

- [ ] **Step 3: Replace the authority query and evidence verifier**

First load the frozen price binding. If absent, return `unavailable/no_frozen_price_authority`. Then join by its evidence ID through `PriceBarSetAuthority` and manifest. Replace `_price_evidence` with:

```python
def _price_evidence(
    self,
    authority: PriceBarSetAuthority,
    manifest: PriceSeriesManifest,
) -> ResearchEvidenceRef | None:
    return next(
        (
            record
            for record in self._evidence
            if record.id == authority.evidence_ref_id
            and record.source == authority.source == manifest.source
            and record.provider_object_id
            == authority.provider_object_id
            == f"price-bar-set:{manifest.id}"
            and record.revision == authority.revision == authority.content_hash
            and record.available_at == authority.available_at
            and record.location
            == f"db://price-bar-set/{manifest.id}@sha256:{authority.content_hash}"
            and record.content_hash == authority.content_hash
            and record.byte_length == authority.byte_length
        ),
        None,
    )
```

Require fixed source-policy/hash/policy versions and selected/decision times in the query.

- [ ] **Step 4: Add read-time validation and stable degradation**

Before computing, query whether any row has `quality_flags <> '[]'::jsonb`; if yes return `unavailable/flagged_price_bars`. Call `prepare_price_bar_set(session, manifest.id)` and compare every returned field to authority. If prepare returns None or differs, return `unavailable/invalid_price_bar_set_authority`.

Read exactly the latest 200 rows with:

```python
.order_by(
    PriceBarRecord.session.desc(),
    PriceBarRecord.observed_at.desc(),
)
.limit(200)
```

Reverse to chronological order before `compute_price_features`. Keep existing allowlisted feature calculation unchanged. Use `partial/insufficient_price_history` only for legitimate warm-up deficiency; stored-authority damage never becomes partial or ok.

- [ ] **Step 5: Enforce deterministic synthesis abstention without price authority**

In `synthesize_intent`, derive authority from the frozen evidence identity, not source name:

```python
price_missing = not any(
    evidence.object_id.startswith("price-bar-set:")
    and evidence.location.startswith("db://price-bar-set/")
    for evidence in evidence_refs
)
votes = tuple(
    RoleVote(
        role=output.role,
        vote=(
            "abstain"
            if (fundamental_missing and output.role == "fundamental")
            or (price_missing and output.role == "market_state")
            else output.vote
        ),
    )
    for output in role_outputs
)
```

Extend `_decision` with `price_missing: bool`; return `("watch", "none")` before vote-count decisions whenever it is true. Set `quality_status="partial"` when `fundamental_missing or price_missing`. Build uncertainties deterministically:

```python
uncertainties = tuple(
    message
    for condition, message in (
        (fundamental_missing, "HKEX fundamental authority is unavailable."),
        (price_missing, "Price authority is unavailable."),
    )
    if condition
) or ("Research remains subject to point-in-time evidence quality.",)
```

This gate changes only the non-executable `ResearchIntent`; it does not invent evidence or add a trade path.

- [ ] **Step 6: Run price tools and all Agent tests GREEN; commit**

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_agent_execution_pg.py -k 'price' -q
uv run --package nexus-engine pytest engine/tests/unit/agents engine/tests/evals/test_research_committee.py -q
uv run --package nexus-engine ruff check engine/src/nexus/agents/database_tools.py engine/src/nexus/agents/synthesis.py engine/tests/integration/test_agent_execution_pg.py engine/tests/unit/agents/test_execution.py
uv run --package nexus-engine mypy engine/src/nexus/agents
git add engine/src/nexus/agents/database_tools.py engine/src/nexus/agents/synthesis.py engine/tests/integration/test_agent_execution_pg.py engine/tests/unit/agents/test_execution.py
git commit -m "fix(engine): compute features from cited bar set"
```

---

### Task 6: Prove freeze/write serialization and migration residue behavior

**Files:**
- Modify: `engine/tests/integration/test_price_bar_authority_pg.py`
- Modify: `engine/tests/integration/test_agents_migration.py`
- Modify: `engine/alembic/versions/0011_agents.py`

**Interfaces:**
- Consumes: shared manifest advisory key, Task 3 finalizer, Task 2 write guard.
- Produces: two-connection proof that freeze and writes have only the two serialized outcomes; complete downgrade residue evidence.

- [ ] **Step 1: Add RED two-transaction concurrency tests**

Use two independent async sessions and a third observation connection. Do not use fixed sleep as proof. Poll `pg_stat_activity` inside `asyncio.timeout(2)` until the competing backend reports `wait_event='advisory'`, yielding with `await asyncio.sleep(0)` only between authoritative DB observations:

```python
async def _wait_for_advisory_wait(factory, backend_pid: int) -> None:
    async with asyncio.timeout(2):
        while True:
            async with factory() as observer:
                wait_event = await observer.scalar(
                    text(
                        "SELECT wait_event FROM pg_stat_activity "
                        "WHERE pid=:backend_pid"
                    ),
                    {"backend_pid": backend_pid},
                )
            if wait_event == "advisory":
                return
            await asyncio.sleep(0)
```

Writer-first test:

```python
async def writer_first() -> None:
    async with factory() as session, session.begin():
        session.add(clean_bar(manifest_id=manifest_id, session=date(2026, 8, 25)))
        await session.flush()
        writer_locked.set()
        await release_writer.wait()


async def finalizer_second() -> PriceBarSetAuthority:
    await writer_locked.wait()
    async with factory() as session, session.begin():
        finalizer_pid.set_result(
            int(await session.scalar(text("SELECT pg_backend_pid()")))
        )
        manifest = await session.get(PriceSeriesManifest, manifest_id)
        assert manifest is not None
        finalized = await finalize_price_bar_set(session, manifest)
        assert finalized is not None
        return finalized[0]
```

Initialize `finalizer_pid` with `asyncio.get_running_loop().create_future()`. Start both tasks, call `_wait_for_advisory_wait(factory, await finalizer_pid)`, release writer, then assert the committed authority `bar_count == 2` and its digest matches prepare.

Authority-first test uses this writer and keeps the finalizer transaction open until its authority row has flushed:

```python
async def late_writer() -> None:
    await authority_locked.wait()
    async with factory() as session, session.begin():
        writer_pid.set_result(
            int(await session.scalar(text("SELECT pg_backend_pid()")))
        )
        session.add(clean_bar(manifest_id=manifest_id, session=date(2026, 8, 25)))
        await session.flush()


async with factory() as finalizer, finalizer.begin():
    manifest = await finalizer.get(PriceSeriesManifest, manifest_id)
    assert manifest is not None
    finalized = await finalize_price_bar_set(finalizer, manifest)
    assert finalized is not None
    authority_locked.set()
    writer_task = asyncio.create_task(late_writer())
    await _wait_for_advisory_wait(factory, await writer_pid)
```

Exit the finalizer transaction to commit authority, then assert `writer_task` raises `DBAPIError` with `frozen price series cannot grow`. Afterwards both connections must execute `SELECT 1` successfully.

- [ ] **Step 2: Extend migration residue checks**

Query `pg_proc`, `pg_trigger`, `pg_extension` after upgrade/downgrade/re-upgrade. Assert upgrade has exactly:

```python
EXPECTED_TASK_15A_FUNCTIONS = {
    "nexus_price_bar_set_canonical_v1",
    "nexus_prepare_price_bar_set",
    "nexus_validate_price_bar_set_authority",
    "nexus_guard_price_bar_set_mutation",
}
EXPECTED_TASK_15A_TRIGGERS = {
    "trg_price_bar_set_authorities_validate",
    "trg_price_bar_set_authorities_immutable",
    "trg_00_price_bar_records_authority_guard",
}
```

After downgrade, all Task 15A functions/triggers/table must be absent; `pgcrypto` must remain present and must not be treated as residue. Re-upgrade must restore the exact same set.

- [ ] **Step 3: Run concurrency/migration tests and fix only observed defects**

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_price_bar_authority_pg.py -k 'concurrent or serialized' -q
uv run --package nexus-engine pytest engine/tests/integration/test_agents_migration.py -q
```

Expected: writer-first digest includes the new Bar; authority-first late INSERT fails after waiting; migration object sets are exact on both upgrades.

If trigger order fails, retain the `trg_00_` name rather than adding a table/global lock. If downgrade residue fails, correct dependency drop order rather than using `CASCADE`.

- [ ] **Step 4: Commit the concurrency proof**

```bash
git add engine/tests/integration/test_price_bar_authority_pg.py engine/tests/integration/test_agents_migration.py engine/alembic/versions/0011_agents.py
git commit -m "test(engine): prove price authority serialization"
```

---

### Task 7: Run full release gates and prepare a fresh independent review

**Files:**
- Verify: all Task 15A files from the File Map
- Update evidence ledger: `/Users/wangchen/Documents/ChatGPT/投资/.superpowers/sdd/2026-08-22-nexus-phase-0-1/progress.md`
- Update review package: `/Users/wangchen/Documents/ChatGPT/投资/.superpowers/sdd/2026-08-22-nexus-phase-0-1/task-15-review-report.md`

**Interfaces:**
- Consumes: Tasks 1–6 completed commits.
- Produces: exact command evidence and a scoped review package for the four Task 15 Round 5 findings; no Task 16 work.

- [ ] **Step 1: Run the focused Task 15A gate**

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_price_bar_authority_pg.py engine/tests/integration/test_market_ingestion.py engine/tests/integration/test_agents_migration.py -q
uv run --package nexus-engine pytest engine/tests/integration/test_agent_execution_pg.py -k 'price' -q
```

Expected: all tests pass; record exact counts and duration.

- [ ] **Step 2: Run the complete Engine quality gate**

```bash
uv run --package nexus-engine pytest engine/tests -q
uv run --package nexus-engine ruff check engine
uv run --package nexus-engine mypy engine/src
uv lock --check
```

Expected: every command exits 0. A narrow focused pass cannot substitute for the full Engine suite.

- [ ] **Step 3: Run deterministic evals twice**

```bash
uv run --package nexus-engine pytest engine/tests/evals/test_research_committee.py -q
uv run --package nexus-engine pytest engine/tests/evals/test_research_committee.py -q
```

Expected: both runs pass with identical structured fixture outputs and the same test count.

- [ ] **Step 4: Run unchanged Web regression gates**

```bash
npm test
npm run lint
npm run build
```

Expected: all exit 0; Task 15A must not change generated Web/API contracts.

- [ ] **Step 5: Check the final diff for scope and secrets**

```bash
git status --short
git diff 42a8610 --check
git diff 42a8610 --stat
git diff 42a8610 -- engine/src engine/tests engine/alembic docs/superpowers
rg -n "ak_|FSOPENAPI_API_KEY|OPENAI_API_KEY|PRIVATE KEY|BEGIN RSA" engine docs app contracts
```

Expected: only planned Engine/test/docs files changed; whitespace check passes; the secret scan finds only safe variable names or fixture text, never a credential value. Do not print environment files or full environment variables.

- [ ] **Step 6: Record evidence and request fresh review**

Append exact commit IDs, command outputs/counts/durations, scope exclusions and rollback note to the existing SDD progress ledger. Add a new Task 15A review section mapping evidence to all four findings:

1. BarSet hash proves exact calculation rows.
2. DB plus runtime reject source-policy bypass and fixture production use.
3. Shared advisory lock closes freeze/write TOCTOU.
4. Flags can never produce ok.

The reviewer must inspect new breakage as well as these frozen findings. Task 15A is accepted only when the independent review returns no Critical or Important. Do not start Task 16 before that result.

- [ ] **Step 7: Commit any review-request documentation inside the repository**

If the final diff contains only already committed code/tests, do not create an empty commit. If the spec or plan received factual corrections during implementation, commit only those documentation changes:

```bash
git add docs/superpowers/specs/2026-08-25-task-15a-price-bar-authority-design.md docs/superpowers/plans/2026-08-25-task-15a-price-bar-authority.md
git commit -m "docs: record Task 15A verification gates"
```

Final handoff must state that real Fosun/account/trade/deploy operations were not executed and that Task 16 remains gated on independent acceptance.
