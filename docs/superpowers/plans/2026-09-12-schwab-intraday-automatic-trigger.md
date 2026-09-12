# Schwab Intraday Automatic Trigger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让现有 Provider worker 在合格的 15 秒盘中桶内自动创建并执行一次有界 Schwab Level One 采集，把原始证据冻结到对象存储，并通过既有数据库链路发布成交价与双边 bid/ask。

**Architecture:** 保留 `NEXUS_MARKET_PROVIDER` 负责日线和通用行情，新增可选的 `NEXUS_INTRADAY_PROVIDER` 只选择盘中来源。Schwab 复用既有 Job、worker、PostgreSQL、MinIO 和 raw-first 归档边界；每个 Job 只建立一次 WebSocket 采集，不驻留、不重连、不跨供应商回退。Mac 本地入口加载既有本地配置与受保护令牌目录，不新增服务身份或基础设施。

**Tech Stack:** Python 3.12、Pydantic、SQLAlchemy、Alembic、PostgreSQL、httpx/WebSocket、pytest、Ruff、Mypy、Docker Compose。

**Spec:** `docs/superpowers/specs/2026-09-12-schwab-intraday-automatic-trigger-design.md`

## Global Constraints

- 保持 `execution_locked=true`，此次变更只提供展示证据，不提供研究或执行授权。
- 先持久化原始 Schwab transcript，再做解析、规范化和发布；解析失败不得丢失原始证据。
- 不改变现有每桶单次尝试、30 秒 handler deadline、60 秒供应商冷却和交易日历判断。
- 不新增常驻流服务、重连循环、Alpaca 回退、Schwab 专属数据库角色、MinIO 身份、bucket 或前缀。
- 测试仅替换外部 Schwab 网络边界；Job、数据库、对象存储和归一化链路使用真实项目实现。
- 每个任务先写失败测试，确认失败原因，再写最小实现并运行定向验证。

---

## Task 1: Split intraday provider selection and admit immutable v3 Jobs

**Files:**

- Modify: `engine/src/nexus/providers/settings.py`
- Modify: `engine/src/nexus/jobs/intraday_schedule.py`
- Modify: `engine/src/nexus/jobs/intraday_collection.py`
- Modify: `engine/src/nexus/provider_worker.py`
- Modify: `engine/src/nexus/persistence/migrations.py`
- Modify: `engine/src/nexus/persistence/models/runtime.py`
- Create: `engine/alembic/versions/0055_schwab_intraday_job_v3.py`
- Modify: `engine/tests/unit/providers/test_composition.py`
- Modify: `engine/tests/unit/jobs/test_intraday_schedule.py`
- Modify: `engine/tests/unit/jobs/test_intraday_collection.py`
- Modify: `engine/tests/unit/providers/test_provider_worker.py`
- Modify: `engine/tests/integration/test_intraday_collection_pg.py`
- Modify: `engine/tests/integration/test_intraday_schema_admission_migration.py`
- Modify: `engine/tests/integration/test_provider_runtime_role_pg.py`
- Create: `engine/tests/integration/test_schwab_intraday_job_migration.py`

### Contract to implement

Add an `IntradayProvider` enum with `fosun`, `alpaca`, and `schwab`. Add optional `intraday_provider` from `NEXUS_INTRADAY_PROVIDER`, plus a read-only `selected_intraday_provider` property that inherits `market_provider` when the new variable is absent. Do not add Schwab to the daily `MarketProvider` enum.

Publish `intraday_snapshot_job_v3`. Its Schwab branch must require:

- `source="schwab"`
- `feed="l1"`
- `real_time=true`
- `include_minute_bars=false`
- one to thirty unique symbols
- no Fosun account or authority fields

The v3 Fosun and Alpaca branches retain their v2 field rules. Add `SCHWAB_SNAPSHOT_CONTRACT_HASH`, `IntradaySnapshotJobPayloadV3`, and `build_schwab_intraday_payload`. Scheduling and worker schema validation must use v3 exclusively after migration.

### Steps

- [x] Add failing settings tests proving an absent intraday setting inherits Fosun or Alpaca, an explicit Schwab value is accepted, and `NEXUS_MARKET_PROVIDER=schwab` is rejected.

- [x] Add failing payload tests proving the exact Schwab contract above, the thirty-symbol bound, duplicate-symbol rejection, and rejection of minute bars or non-real-time data.

- [x] Add failing scheduling tests proving a Schwab-selected worker creates v3 Schwab Jobs while the daily market provider remains Fosun, and proving omitted configuration preserves current behavior.

- [x] Implement the enum, setting, inherited selector, v3 payload model, builders, schema constant, and scheduler selection with no changes to retry or cooldown policy.

- [x] Add migration `0055_schwab_intraday_job_v3.py`. Upgrade must lock the admitted-schema state, fail closed if actionable v2 Jobs remain, replace the admission trigger and partial index with an exact v3 predicate, and preserve all Job rows. Downgrade must apply the symmetric v3 drain fence and restore the exact v2 trigger and index from migration 0031.

- [x] Add PostgreSQL migration tests covering successful admission of each v3 provider branch, rejection of malformed Schwab payloads, upgrade refusal with actionable v2 rows, downgrade refusal with actionable v3 rows, and row preservation.

- [x] Run the focused tests and static checks:

```bash
uv run --project engine pytest engine/tests/unit/providers/test_composition.py engine/tests/unit/jobs/test_intraday_schedule.py engine/tests/unit/jobs/test_intraday_collection.py engine/tests/unit/providers/test_provider_worker.py engine/tests/integration/test_schwab_intraday_job_migration.py -q
uv run --project engine ruff check engine/src/nexus/providers/settings.py engine/src/nexus/jobs/intraday_schedule.py engine/src/nexus/jobs/intraday_collection.py engine/src/nexus/provider_worker.py engine/src/nexus/persistence/migrations.py engine/src/nexus/persistence/models/runtime.py engine/alembic/versions/0055_schwab_intraday_job_v3.py
uv run --project engine mypy engine/src/nexus/providers/settings.py engine/src/nexus/jobs/intraday_schedule.py engine/src/nexus/jobs/intraday_collection.py engine/src/nexus/provider_worker.py engine/src/nexus/persistence/migrations.py engine/src/nexus/persistence/models/runtime.py
```

- [x] Commit this contract slice:

```bash
git add engine/src/nexus/providers/settings.py engine/src/nexus/jobs/intraday_schedule.py engine/src/nexus/jobs/intraday_collection.py engine/src/nexus/provider_worker.py engine/src/nexus/persistence/migrations.py engine/src/nexus/persistence/models/runtime.py engine/alembic/versions/0055_schwab_intraday_job_v3.py engine/tests docs/superpowers/plans/2026-09-12-schwab-intraday-automatic-trigger.md
git commit -m "feat: admit Schwab intraday jobs"
```

---

## Task 2: Adapt bounded Schwab transcripts to the raw-first snapshot boundary

**Files:**

- Modify: `engine/src/nexus/ops/schwab_market_stream.py`
- Create: `engine/src/nexus/providers/market/schwab_snapshot.py`
- Modify: `engine/src/nexus/providers/market/snapshot_contracts.py`
- Modify: `engine/src/nexus/providers/base.py`
- Modify: `engine/src/nexus/providers/market/fosun.py`
- Modify: `engine/src/nexus/jobs/intraday_collection.py`
- Modify: `engine/src/nexus/market/intraday_ingestion.py`
- Modify: `engine/tests/unit/ops/test_schwab_market_stream.py`
- Create: `engine/tests/unit/providers/test_schwab_snapshot.py`
- Modify: `engine/tests/unit/jobs/test_intraday_collection.py`
- Modify: `engine/tests/unit/providers/test_fosun_market.py`

### Contract to implement

Extract two shared operations from the existing manual-import path:

```python
def pack_provider_market_stream_archive(directory: Path) -> bytes:
    """Validate and package the bounded capture files without requiring completeness."""

def parse_schwab_snapshot_batch(
    raw_payload: bytes,
    request: SnapshotRequest,
) -> SnapshotBatch:
    """Require a complete, real-time, exact-symbol capture and normalize its latest events."""
```

The packer accepts an incomplete bounded capture so the generic handler can freeze its raw files before normalization fails. The parser must reject missing authentication/subscription completion, unknown delay, `delayed=true`, unexpected symbols, malformed timestamps, and malformed frames. A complete transcript may contain only a subset of requested symbols: publish valid observed symbols and let the handler return `INTRADAY_SNAPSHOT_PARTIAL`. Select the latest valid trade and two-sided quote per symbol; never publish a one-sided quote as a real-time quote and never invent a missing side.

Create `SchwabSnapshotProvider` with injected repository root, `httpx.AsyncClient`, clock, and capture callable. `fetch_raw()` uses a private temporary directory, invokes the existing bounded `run_schwab_stream` once, packages the raw capture, and returns a `RawSnapshotEnvelope`. `normalize()` delegates to the shared parser. Temporary files are removed after packaging; credentials and preferences never enter the envelope metadata.

### Steps

- [x] Add failing parser tests for a complete real-time two-sided quote, a valid requested-symbol subset, trade-only evidence, one-sided quote rejection, unknown or delayed data, incomplete authentication/subscription, wrong symbol binding, malformed frames, and out-of-order frames.

- [x] Add failing provider tests proving one bounded collector invocation, deterministic request metadata, raw bytes returned before normalization, private temporary-directory cleanup, and no retry after collector failure.

- [x] Extend `MarketDataProvenance.source` and raw archive schema mapping with `schwab`, while retaining exact schema/hash validation for Fosun and Alpaca.

- [x] Implement the packer, shared parser, and provider. Increase the generic L1 request bound to thirty only where required by the Schwab v3 contract; retain the Fosun payload limit of ten in its Job and provider validators.

- [x] Refactor the manual Schwab import to use the same packer and parser so manual and automatic paths cannot diverge on completeness, delay, symbol binding, or price selection.

- [x] Run focused tests and static checks (actual paths listed above; the PostgreSQL manual-import integration test also passed):

```bash
cd engine
uv run pytest tests/unit/test_schwab_market_stream.py tests/unit/test_schwab_snapshot_provider.py tests/unit/test_intraday_collection.py tests/unit/test_schwab_snapshot_import.py -q
uv run ruff check src/nexus/ops/schwab_market_stream.py src/nexus/providers/market/schwab_snapshot.py src/nexus/providers/market/snapshot_contracts.py src/nexus/jobs/intraday_collection.py src/nexus/ops/schwab_snapshot_import.py tests/unit/test_schwab_market_stream.py tests/unit/test_schwab_snapshot_provider.py tests/unit/test_intraday_collection.py tests/unit/test_schwab_snapshot_import.py
uv run mypy src/nexus/ops/schwab_market_stream.py src/nexus/providers/market/schwab_snapshot.py src/nexus/providers/market/snapshot_contracts.py src/nexus/jobs/intraday_collection.py src/nexus/ops/schwab_snapshot_import.py
```

- [x] Commit this adapter slice:

```bash
git add engine/src/nexus/ops/schwab_market_stream.py engine/src/nexus/providers/market/schwab_snapshot.py engine/src/nexus/providers/market/snapshot_contracts.py engine/src/nexus/jobs/intraday_collection.py engine/src/nexus/ops/schwab_snapshot_import.py engine/tests/unit/test_schwab_market_stream.py engine/tests/unit/test_schwab_snapshot_provider.py engine/tests/unit/test_intraday_collection.py engine/tests/unit/test_schwab_snapshot_import.py
git commit -m "feat: adapt Schwab intraday snapshots"
```

---

## Task 3: Wire Schwab into the existing worker and prove the persisted automatic path

**Files:**

- Modify: `engine/src/nexus/provider_worker.py`
- Modify: `engine/src/nexus/jobs/intraday_collection.py`
- Modify: `engine/src/nexus/jobs/intraday_schedule.py`
- Modify: `engine/src/nexus/providers/market/schwab_snapshot.py`
- Modify: `engine/tests/unit/providers/test_provider_worker.py`
- Modify: `engine/tests/unit/jobs/test_intraday_collection.py`
- Modify: `engine/tests/unit/providers/test_schwab_snapshot.py`
- Create: `engine/tests/integration/test_schwab_intraday_automatic_job.py`
- Modify: `engine/tests/integration/test_intraday_collection_pg.py`

### Contract to implement

Keep `_production_bundle()` responsible for the daily/general provider chosen by `NEXUS_MARKET_PROVIDER`. In `_provider_registry()`, choose only the snapshot provider from `selected_intraday_provider`: reuse the bundle snapshot provider for Fosun or Alpaca and construct `SchwabSnapshotProvider` for Schwab with the worker's ordinary `httpx.AsyncClient`. This prevents Schwab from becoming a daily-data provider.

Map stable Schwab failures at the handler boundary:

- authentication, token-refresh, or required local configuration rejection to `INTRADAY_PROVIDER_AUTH_REQUIRED`;
- incomplete, disconnected, or unavailable capture to `INTRADAY_PROVIDER_UNAVAILABLE`;
- delayed or contradictory stream evidence to `INTRADAY_PROVIDER_POLICY_REJECTED`;
- payload/schema mismatch to `INTRADAY_CONTRACT_MISMATCH`.

Do not retry inside the adapter. The existing one-attempt Job policy and cross-bucket cooldown remain the only retry controls.

### Steps

- [x] Add failing registry tests proving Fosun daily plus Schwab intraday coexist in one worker and proving Fosun/Alpaca configurations remain unchanged.

- [x] Add failing handler tests proving the exact stable error mapping, raw artifact persistence before parse rejection, valid subset publication with `INTRADAY_SNAPSHOT_PARTIAL`, and no `LatestQuote` publication for incomplete, delayed, or one-sided quote evidence.

- [x] Implement snapshot-only selection and stable error mapping. Add `INTRADAY_PROVIDER_AUTH_REQUIRED` to the existing provider-failure cooldown set; do not change daily bundle construction, handler deadline, attempts, or the 60-second duration.

- [x] Add a PostgreSQL integration test that seeds an eligible calendar row and watchlist, schedules a real v3 Schwab Job, executes `run_job_worker_once`, and asserts the persisted `Job`, `JobAttempt`, `DataArtifact`, normalized quote observations, `LatestQuote`, and query view. Replace only the external WebSocket/preferences boundary with a deterministic transcript.

- [x] Add a second integration case where the bounded capture is incomplete: the raw artifact exists, the Job ends with the expected stable code, and neither normalized quote observations nor `LatestQuote` are written.

- [x] Run focused verification using the actual paths listed above.

```bash
cd engine
uv run pytest tests/unit/test_provider_worker.py tests/unit/test_intraday_collection.py tests/integration/test_schwab_intraday_automatic_job.py -q
uv run ruff check src/nexus/provider_worker.py src/nexus/jobs/intraday_collection.py tests/unit/test_provider_worker.py tests/unit/test_intraday_collection.py tests/integration/test_schwab_intraday_automatic_job.py
uv run mypy src/nexus/provider_worker.py src/nexus/jobs/intraday_collection.py
```

- [x] Commit the worker slice:

```bash
git add engine/src/nexus/provider_worker.py engine/src/nexus/jobs/intraday_collection.py engine/tests/unit/test_provider_worker.py engine/tests/unit/test_intraday_collection.py engine/tests/integration/test_schwab_intraday_automatic_job.py
git commit -m "feat: run Schwab intraday jobs"
```

---

## Task 4: Add a minimal Mac-local worker entry and document its configuration

**Files:**

- Create: `engine/src/nexus/ops/provider_worker_local.py`
- Modify: `engine/src/nexus/provider_worker.py`
- Modify: `engine/pyproject.toml`
- Create: `engine/tests/unit/ops/test_provider_worker_local.py`
- Modify: `infra/env.example`
- Modify: `README.md`
- Modify: `docs/runbooks/intraday-market-monitor.md`

### Contract to implement

Add one local CLI entry, `nexus-engine-provider-worker-local`, accepting only `--repo-root`. It calls the existing `load_local_environment`, uses `.env.local` plus process environment precedence, and points Schwab at the existing `.secrets/nexus-schwab-local` path through the current settings contract. It may map the project's common local database and object-store variable names to existing provider-worker aliases only when those aliases are absent. It must not mutate environment variables after settings construction, print secret values, or accept credentials on the command line.

The runbook must state that:

- `NEXUS_MARKET_PROVIDER=fosun` remains the daily/general choice;
- `NEXUS_INTRADAY_PROVIDER=schwab` opts only the intraday worker into Schwab;
- PostgreSQL and MinIO are the existing shared local services;
- the worker can be stopped with one interrupt and owns no external write permission;
- a closed-market run validates calendar suppression, not a successful market capture.

### Steps

- [x] Add failing CLI/config tests for precedence, loopback localization of Compose hostnames, absent-secret-path rejection, secret redaction, and preservation of an explicitly supplied provider-worker alias.

- [x] Implement the local settings loader and CLI entry using the existing worker loop and signal handling.

- [x] Add the optional `NEXUS_INTRADAY_PROVIDER=` line to `infra/env.example` with a concise explanation; do not add Schwab secrets or a Compose profile.

- [x] Update the README and existing intraday-monitor runbook with the exact startup, shutdown, migration, and durable-state inspection commands; do not create a duplicate local-read-only runbook.

- [x] Run focused tests and packaging checks:

```bash
cd engine
uv run pytest tests/unit/ops/test_provider_worker_local.py tests/unit/providers/test_provider_worker.py -q
uv run ruff check src/nexus/provider_worker.py src/nexus/ops/provider_worker_local.py tests/unit/providers/test_provider_worker.py tests/unit/ops/test_provider_worker_local.py
uv run mypy src/nexus/provider_worker.py src/nexus/ops/provider_worker_local.py
uv run nexus-engine-provider-worker-local --help
```

- [x] Commit the local entry slice:

```bash
git add engine/src/nexus/provider_worker.py engine/src/nexus/ops/provider_worker_local.py engine/pyproject.toml engine/tests/unit/ops/test_provider_worker_local.py infra/env.example README.md docs/runbooks/intraday-market-monitor.md docs/superpowers/plans/2026-09-12-schwab-intraday-automatic-trigger.md
git commit -m "feat: add local provider worker entry"
```

---

## Task 5: Consolidate verification and perform bounded local acceptance

**Files:**

- Modify only if verification exposes a defect in files already listed above.
- Update: `docs/superpowers/specs/2026-09-12-schwab-intraday-automatic-trigger-design.md`

### Acceptance definition

Code acceptance requires the same real chain used by the product entry:

```text
calendar and watchlist -> intraday_snapshot_job_v3 -> Provider worker
-> bounded Schwab capture -> immutable raw artifact -> normalized observations
-> LatestQuote and query view
```

The deterministic PostgreSQL integration test proves this chain without spending external quota. Live acceptance is bounded to one eligible bucket and one terminal Job. Before any live request, inspect `run -> job -> outputs` and existing artifacts so an uncertain earlier attempt is not replayed.

### Steps

- [x] Run the complete affected Python suite:

```bash
uv run --project engine pytest engine/tests/unit/providers/test_composition.py engine/tests/unit/providers/test_fosun_market.py engine/tests/unit/providers/test_provider_worker.py engine/tests/unit/providers/test_schwab_snapshot.py engine/tests/unit/jobs/test_intraday_schedule.py engine/tests/unit/jobs/test_intraday_collection.py engine/tests/unit/ops/test_schwab_market_stream.py engine/tests/unit/ops/test_provider_worker_local.py engine/tests/integration/test_intraday_collection_pg.py engine/tests/integration/test_intraday_schema_admission_migration.py engine/tests/integration/test_provider_runtime_role_pg.py engine/tests/integration/test_schwab_intraday_job_migration.py engine/tests/integration/test_schwab_intraday_automatic_job.py -q
uv run --project engine ruff check engine/src/nexus/providers/settings.py engine/src/nexus/providers/base.py engine/src/nexus/providers/market/snapshot_contracts.py engine/src/nexus/providers/market/schwab_snapshot.py engine/src/nexus/jobs/intraday_schedule.py engine/src/nexus/jobs/intraday_collection.py engine/src/nexus/market/intraday_ingestion.py engine/src/nexus/provider_worker.py engine/src/nexus/ops/schwab_market_stream.py engine/src/nexus/ops/provider_worker_local.py engine/src/nexus/persistence/migrations.py engine/src/nexus/persistence/models/runtime.py engine/alembic/versions/0055_schwab_intraday_job_v3.py
uv run --project engine mypy engine/src/nexus/providers/settings.py engine/src/nexus/providers/base.py engine/src/nexus/providers/market/snapshot_contracts.py engine/src/nexus/providers/market/schwab_snapshot.py engine/src/nexus/jobs/intraday_schedule.py engine/src/nexus/jobs/intraday_collection.py engine/src/nexus/market/intraday_ingestion.py engine/src/nexus/provider_worker.py engine/src/nexus/ops/schwab_market_stream.py engine/src/nexus/ops/provider_worker_local.py engine/src/nexus/persistence/migrations.py engine/src/nexus/persistence/models/runtime.py
```

- [x] Validate migration and Compose configuration through normal project entry points:

```bash
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app config --quiet
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app run --rm --no-deps --build api alembic -c engine/alembic.ini upgrade head
```

- [x] Inspect the current durable Job state and Schwab artifacts before starting the worker. Record exact Job IDs, terminal states, attempt counts, artifact hashes, and timestamps without printing credentials or raw personal account facts.

- [x] Start the Mac-local worker with the documented entry and existing local configuration:

```bash
cd engine
uv run nexus-engine-provider-worker-local --repo-root /Users/wangchen/Documents/ChatGPT/投资
```

- [x] On a market-closed date, verify that the calendar path creates no Schwab Job and makes no Schwab network call, then stop the worker cleanly. Report that live automatic-trigger acceptance remains pending until an eligible market bucket; do not create a fake open session.

- [ ] During an eligible market bucket, wait for one newly persisted v3 Schwab Job to reach a terminal state. Inspect that Job once and verify its attempt, raw artifact, normalized quote, bid/ask timestamps, `delayed=false`, source identity, and query-view visibility. Stop the worker after this single bounded acceptance.

- [ ] If the live Job fails, diagnose from its persisted error code and frozen artifact. Fix only a defect demonstrated by that evidence, rerun the smallest affected test set, and do not replay the provider request merely because observation output was interrupted.

- [x] Update the design status and runbook acceptance section with the actual result and remaining external dependency. Do not claim full live acceptance from integration tests or closed-market suppression.

- [x] Run final repository checks:

```bash
git diff --check
git status --short
```

- [x] Commit only if verification required code or documentation changes:

```bash
git add docs/runbooks/intraday-market-monitor.md docs/superpowers/plans/2026-09-12-schwab-intraday-automatic-trigger.md docs/superpowers/specs/2026-09-12-schwab-intraday-automatic-trigger-design.md
git commit -m "test: verify Schwab intraday automation"
```

## Completion evidence

The implementation is complete only when the focused tests and static checks pass, migration 0055 is applied through the normal entry, the local worker starts with protected configuration, closed-market suppression is observed when applicable, and one eligible live bucket has persisted a terminal v3 Schwab Job with raw evidence and verified normalized output. Until that eligible live bucket succeeds, the code path may be implementation-ready but automatic live acceptance remains explicitly pending.
