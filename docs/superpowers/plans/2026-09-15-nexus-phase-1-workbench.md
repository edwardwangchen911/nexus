# NEXUS Phase 1 Workbench Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the read-only NEXUS trading workbench, decision record list/detail, and the new navigation without adding browser-triggered research, market collection, order creation, approval, or live trading.

**Architecture:** Add one bounded Agent-paper loop liveness fact and two read-only Engine projections: a workbench aggregate and a cycle detail. Expose them through the existing signed BFF, then compose new Vinext pages from small presentation components and a single deterministic product-status mapper. Keep existing routes and business records intact; the new navigation points to new Phase 1 pages where available and to existing compatible pages for areas scheduled for later phases.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2, Alembic, PostgreSQL 17, Pydantic 2, React 19, TypeScript 5.9, Vinext, Tailwind CSS 4, Lucide React, Vitest, Testing Library, Playwright.

**Spec:** `docs/superpowers/specs/2026-09-15-nexus-product-experience-redesign-design.md`

## Global Constraints

- Current mode is `internal_paper`; real accounts remain read-only and `execution_locked` remains exactly `true`.
- Page loads and refreshes perform GET-only persisted-data reads; they must not collect market data, call a model, replay a cycle, create or cancel an order, or alter approval state.
- The loop process state and the latest persisted cycle state are separate fields and must never be inferred from each other.
- Missing, stale, partial, failed, risk-rejected, no-order, partially-filled, reconciling, and result-unknown states remain distinct.
- Existing `/overview`, `/paper`, `/portfolio`, `/market`, `/research`, `/optimize`, `/what-if`, `/strategies`, `/risk-audit`, `/integrations`, `/broker-simulation`, and `/live-approvals` routes remain directly readable; no historical data or module is deleted.
- The Phase 1 UI must not show working run, stop, approval, risk-edit, Bark-detail, broker-write, or live-trading controls.
- Reuse the current CSS tokens, Lucide icons, typography, and components; add no frontend dependency.
- Dates retain their actual source timezone semantics; stale or unknown evidence is never relabeled as current.
- Every task follows red-green-refactor testing and ends in one focused commit.

## File Structure

### Engine runtime and read model

- Modify `engine/src/nexus/jobs/models.py`: add the exact `agent_paper` heartbeat queue identity.
- Modify `engine/src/nexus/persistence/models/runtime.py`: admit `agent_paper` in the heartbeat table constraint.
- Modify `engine/src/nexus/persistence/migrations.py`: advance the single expected Alembic head.
- Create `engine/alembic/versions/0060_agent_paper_loop_heartbeat.py`: upgrade/downgrade the queue constraint without rewriting heartbeat facts.
- Modify `engine/src/nexus/ops/agent_paper_loop.py`: publish a bounded heartbeat while the foreground loop is alive.
- Create `engine/src/nexus/api/schemas/workbench.py`: strict workbench, loop-liveness, attention, and account-summary contracts.
- Modify `engine/src/nexus/api/schemas/paper.py`: strict cycle-detail contract containing the exact cycle and its linked orders.
- Create `engine/src/nexus/workbench/__init__.py`: package marker.
- Create `engine/src/nexus/workbench/read_service.py`: compose persisted Paper facts and heartbeat facts without writes.
- Modify `engine/src/nexus/paper/read_service.py`: expose one exact cycle-by-ID read owned by the internal Paper account.
- Create `engine/src/nexus/api/routes/workbench.py`: authenticated GET-only workbench endpoint.
- Modify `engine/src/nexus/api/routes/paper.py`: authenticated GET-only cycle-detail endpoint.
- Modify `engine/src/nexus/main.py`: register the workbench router.

### Web BFF and product state

- Create `app/api/v1/workbench/route.ts`: signed GET proxy with no query parameters.
- Create `app/api/v1/paper/cycles/[cycleId]/route.ts`: UUID-validated signed GET proxy.
- Modify `app/lib/paper/bff.ts`: add `paperCycleId` validation.
- Modify `app/lib/engine/client.ts`: admit the two new exact Engine targets under existing Paper response limits.
- Regenerate `contracts/openapi.json`, `app/lib/generated/schema.d.ts`, and `app/lib/generated/runtime-contract.ts` through `npm run generate:client`.
- Create `app/lib/product-status.ts`: the only mapping from backend facts/codes to Chinese product states, health, attention, and safe actions.

### Web pages and components

- Create `app/components/workbench/mode-strip.tsx`: current mode, live lock, loop liveness, persisted cycle, and next slot.
- Create `app/components/workbench/latest-decision.tsx`: decision-first summary with reason and invalidation/evidence links.
- Create `app/components/workbench/attention-list.tsx`: bounded attention items and safe read-only actions.
- Create `app/components/workbench/order-summary.tsx`: active-order and valid no-order states.
- Create `app/components/workbench/account-summary.tsx`: compact internal-paper account facts.
- Create `app/(protected)/workspace/page.tsx`: GET-only workbench composition.
- Create `app/components/decisions/decision-list.tsx`: cycle-backed decision rows.
- Create `app/components/decisions/decision-timeline.tsx`: evidence-to-result detail timeline.
- Create `app/(protected)/decisions/page.tsx`: bounded decision list.
- Create `app/(protected)/decisions/[cycleId]/page.tsx`: cycle detail.
- Modify `app/components/app-shell.tsx`: new information architecture and controlled mobile drawer close.
- Modify `app/page.tsx`: redirect `/` to `/workspace`.
- Modify `app/styles/globals.css`: workbench and decision responsive layout using existing tokens.

### Tests and documentation

- Create `engine/tests/integration/test_agent_paper_loop_heartbeat_migration.py`.
- Modify `engine/tests/unit/ops/test_agent_paper_loop.py`.
- Modify `engine/tests/integration/test_schwab_intraday_job_migration.py`.
- Modify `engine/tests/integration/test_agent_paper_schwab_migration.py`.
- Modify `engine/tests/integration/test_agent_paper_market_migration.py`.
- Create `engine/tests/integration/test_workbench_read_service.py`.
- Create `engine/tests/contract/test_workbench_api.py`.
- Modify `engine/tests/integration/test_paper_api.py`.
- Modify `engine/tests/contract/test_openapi.py`.
- Create `tests/unit/workbench/product-status.test.ts`.
- Create `tests/unit/workbench/page.test.tsx`.
- Create `tests/unit/decisions/pages.test.tsx`.
- Create `tests/unit/workbench/routes.test.ts`.
- Modify `tests/unit/paper/routes.test.ts`.
- Modify `tests/unit/components/app-shell.test.tsx`.
- Modify `tests/e2e/app-shell.contract.spec.ts`.
- Create `tests/e2e/workbench.spec.ts`.
- Create `tests/e2e/decisions.spec.ts`.
- Modify `README.md` and `docs/runbooks/agent-paper-cycle.md`: describe the new read-only pages and liveness semantics.

---

### Task 1: Persist Agent-paper Loop Liveness Without Changing Business State

**Files:**
- Modify: `engine/src/nexus/jobs/models.py`
- Modify: `engine/src/nexus/persistence/models/runtime.py`
- Modify: `engine/src/nexus/persistence/migrations.py`
- Create: `engine/alembic/versions/0060_agent_paper_loop_heartbeat.py`
- Modify: `engine/src/nexus/ops/agent_paper_loop.py`
- Test: `engine/tests/integration/test_agent_paper_loop_heartbeat_migration.py`
- Test: `engine/tests/unit/ops/test_agent_paper_loop.py`
- Test: `engine/tests/integration/test_schwab_intraday_job_migration.py`
- Test: `engine/tests/integration/test_agent_paper_schwab_migration.py`
- Test: `engine/tests/integration/test_agent_paper_market_migration.py`

**Interfaces:**
- Consumes: existing `upsert_worker_heartbeat(...)`, `CURRENT_MIGRATION_HEAD`, and foreground loop inspection boundaries.
- Produces: `QueueName.AGENT_PAPER = "agent_paper"`; `AGENT_PAPER_LOOP_PROCESS_VERSION = "agent-paper-loop-v1"`; `heartbeat_agent_paper_loop(settings, *, worker_id, started_at, seen_at) -> None`; a heartbeat at least every 15 seconds while the process is alive.

- [ ] **Step 1: Write the migration and enum failure tests**

Add assertions that head `0060_agent_paper_loop_heartbeat` admits only the new exact queue value and that downgrade refuses while an `agent_paper` heartbeat exists:

```python
def test_0060_admits_agent_paper_heartbeat(migrated_database_url: str) -> None:
    command.upgrade(config(migrated_database_url), "0060_agent_paper_loop_heartbeat")
    asyncio.run(insert_heartbeat(migrated_database_url, queue="agent_paper"))


def test_0060_downgrade_refuses_live_agent_paper_fact(migrated_database_url: str) -> None:
    command.upgrade(config(migrated_database_url), "0060_agent_paper_loop_heartbeat")
    asyncio.run(insert_heartbeat(migrated_database_url, queue="agent_paper"))
    with pytest.raises(DBAPIError, match="agent_paper heartbeat"):
        command.downgrade(config(migrated_database_url), "0059_agent_paper_intraday_cycles")
```

- [ ] **Step 2: Run the migration tests and verify red**

Run:

```bash
uv run pytest engine/tests/integration/test_agent_paper_loop_heartbeat_migration.py -q --import-mode=importlib
```

Expected: FAIL because revision `0060_agent_paper_loop_heartbeat` and queue `agent_paper` do not exist.

- [ ] **Step 3: Add the migration and exact queue identity**

Add:

```python
class QueueName(StrEnum):
    GENERAL = "general"
    PROVIDER = "provider"
    BROKER = "broker"
    STRATEGY_RND = "strategy_rnd"
    AGENT_PAPER = "agent_paper"
```

Migration `0060` replaces `ck_worker_heartbeats_queue_allowed` with:

```sql
queue IN ('general','provider','broker','strategy_rnd','agent_paper')
```

The downgrade first checks for any `agent_paper` row and raises a descriptive database exception before restoring the `0059` constraint. Update `CURRENT_MIGRATION_HEAD` to `0060_agent_paper_loop_heartbeat` and update existing exact-head assertions.

- [ ] **Step 4: Run the migration tests and verify green**

Run:

```bash
uv run pytest \
  engine/tests/integration/test_agent_paper_loop_heartbeat_migration.py \
  engine/tests/integration/test_schwab_intraday_job_migration.py \
  engine/tests/integration/test_agent_paper_schwab_migration.py \
  engine/tests/integration/test_agent_paper_market_migration.py \
  -q --import-mode=importlib
```

Expected: PASS with one Alembic head and reversible empty-table downgrade.

- [ ] **Step 5: Write failing loop-heartbeat tests**

Add a fake heartbeat callback and short clock-controlled waits:

```python
def test_foreground_loop_heartbeats_while_waiting_for_slot() -> None:
    seen: list[datetime] = []
    result = asyncio.run(loop.run_foreground_loop(
        inspect=inspect_until_closed,
        execute=no_execute,
        sleep=no_real_sleep,
        heartbeat=lambda now: append_heartbeat(seen, now),
        heartbeat_interval=timedelta(seconds=15),
    ))
    assert result is A.STOP_SESSION_CLOSED
    assert seen == [NOW, NOW + timedelta(seconds=15), NOW + timedelta(seconds=30)]
```

Also assert heartbeat persistence uses `QueueName.AGENT_PAPER`, the current migration head, one opaque process ID, and never writes a cycle or order.

- [ ] **Step 6: Run loop tests and verify red**

Run:

```bash
uv run pytest engine/tests/unit/ops/test_agent_paper_loop.py -q
```

Expected: FAIL because `run_foreground_loop` has no heartbeat contract.

- [ ] **Step 7: Implement bounded heartbeat scheduling**

Change the pure loop interface to:

```python
async def run_foreground_loop(
    *,
    inspect: Callable[[], Awaitable[AgentPaperLoopSnapshot]],
    execute: Callable[[AgentPaperLoopAction, AgentPaperLoopSnapshot], Awaitable[AgentPaperLoopAction | None]],
    sleep: Callable[[float], Awaitable[None]],
    heartbeat: Callable[[datetime], Awaitable[None]],
    heartbeat_interval: timedelta = timedelta(seconds=15),
) -> AgentPaperLoopAction:
```

Implement `sleep_with_heartbeat(until)` so no sleep segment exceeds 15 seconds. Persist heartbeat before the first inspection, between wait segments, and immediately after returning from a product action. Use one `worker_id = f"agent-paper-loop-{new_uuid7()}"` and one `started_at` per process. Catch heartbeat persistence failure at the foreground-loop boundary, return `STOP_RESULT_UNKNOWN`, and do not execute another business action.

- [ ] **Step 8: Run focused loop and repository tests**

Run:

```bash
uv run pytest engine/tests/unit/ops/test_agent_paper_loop.py engine/tests/unit/jobs/test_repository_checkpoint.py -q
uv run ruff check engine/src/nexus/ops/agent_paper_loop.py engine/src/nexus/jobs/models.py engine/alembic/versions/0060_agent_paper_loop_heartbeat.py
uv run mypy engine/src/nexus/ops/agent_paper_loop.py engine/src/nexus/jobs/models.py
```

Expected: PASS.

- [ ] **Step 9: Commit the liveness fact**

```bash
git add engine/src/nexus/jobs/models.py engine/src/nexus/persistence/models/runtime.py engine/src/nexus/persistence/migrations.py engine/alembic/versions/0060_agent_paper_loop_heartbeat.py engine/src/nexus/ops/agent_paper_loop.py engine/tests/integration/test_agent_paper_loop_heartbeat_migration.py engine/tests/unit/ops/test_agent_paper_loop.py engine/tests/integration/test_schwab_intraday_job_migration.py engine/tests/integration/test_agent_paper_schwab_migration.py engine/tests/integration/test_agent_paper_market_migration.py
git commit -m "feat(runtime): persist agent paper loop liveness"
```

### Task 2: Add Read-only Workbench and Cycle-detail Projections

**Files:**
- Create: `engine/src/nexus/api/schemas/workbench.py`
- Modify: `engine/src/nexus/api/schemas/paper.py`
- Create: `engine/src/nexus/workbench/__init__.py`
- Create: `engine/src/nexus/workbench/read_service.py`
- Modify: `engine/src/nexus/paper/read_service.py`
- Create: `engine/src/nexus/api/routes/workbench.py`
- Modify: `engine/src/nexus/api/routes/paper.py`
- Modify: `engine/src/nexus/main.py`
- Test: `engine/tests/integration/test_workbench_read_service.py`
- Test: `engine/tests/contract/test_workbench_api.py`
- Test: `engine/tests/integration/test_paper_api.py`
- Test: `engine/tests/contract/test_openapi.py`

**Interfaces:**
- Consumes: `PaperReadService.agent_cycles`, `PaperReadService.portfolio`, `PaperReadService.orders`, `WorkerHeartbeat`, and current request time.
- Produces: `GET /internal/v1/workbench`; `GET /internal/v1/paper/cycles/{cycle_id}`; strict workbench and cycle-detail response types below.

- [ ] **Step 1: Write failing strict-schema tests**

Define the required contract in tests before implementation:

```python
class WorkbenchLoopResponse(_StrictSchema):
    status: Literal["online", "offline", "unknown"]
    started_at: AwareDatetime | None
    last_seen_at: AwareDatetime | None
    stale_after_seconds: Literal[30]


class WorkbenchAttentionResponse(_StrictSchema):
    kind: Literal["data", "model", "risk", "execution", "service"]
    severity: Literal["info", "warning", "critical"]
    title_code: str = Field(min_length=1, max_length=100)
    reason_code: str = Field(min_length=1, max_length=100)
    safe_action: Literal["view_evidence", "view_decision", "verify_original_result", "wait"]
    href: str = Field(pattern=r"^/")
    cycle_id: UUID | None = None
    order_id: UUID | None = None


class WorkbenchResponse(_StrictSchema):
    mode: Literal["internal_paper"]
    execution_locked: Literal[True]
    loop: WorkbenchLoopResponse
    session: AgentPaperSessionSummaryResponse
    latest_cycle: AgentPaperCycleResponse | None
    active_orders: tuple[PaperOrderResponse, ...] = Field(max_length=20)
    account: WorkbenchAccountSummaryResponse | None
    attention: tuple[WorkbenchAttentionResponse, ...] = Field(max_length=10)


class AgentPaperCycleDetailResponse(_StrictSchema):
    cycle: AgentPaperCycleResponse
    orders: tuple[PaperOrderResponse, ...] = Field(max_length=50)


class AgentPaperCycleDetailEnvelope(_StrictSchema):
    data: AgentPaperCycleDetailResponse
    meta: SuccessMetaResponse
```

Define `AgentPaperCycleDetailResponse` and `AgentPaperCycleDetailEnvelope` in `schemas/paper.py`; the workbench schema imports and reuses existing Paper response types without redefining them.

Reject extra fields, non-local hrefs, more than 10 attention items, and any `execution_locked=False` payload.

- [ ] **Step 2: Run schema tests and verify red**

Run:

```bash
uv run pytest engine/tests/contract/test_workbench_api.py -q --import-mode=importlib
```

Expected: FAIL because the workbench schema and route do not exist.

- [ ] **Step 3: Implement cycle ownership read**

Add one account-owned detail read:

```python
async def agent_cycle_detail(self, cycle_id: UUID) -> AgentPaperCycleDetailResponse | None:
    account = await self._internal_account()
    cycle = await self._session.scalar(
        select(AgentPaperCycleRecord).where(
            AgentPaperCycleRecord.id == cycle_id,
            AgentPaperCycleRecord.account_id == account.id,
            AgentPaperCycleRecord.decision_time <= self._request_time,
        )
    )
    if cycle is None:
        return None
    view = await self._agent_cycle(cycle)
    orders = await self._orders_for_cycle(cycle_id, limit=50)
    return AgentPaperCycleDetailResponse(cycle=view, orders=orders)
```

`_orders_for_cycle` joins `PaperOrderRecord → StrategyDecisionRecord.agent_paper_cycle_id`, applies the request-time boundary, returns canonical order projections, and rejects more than 50 linked orders as `DATA_PARTIAL`. The route returns 404 when the cycle is absent or belongs to another account. It accepts no query parameters. The Web must not reconstruct this relationship from the generic order list.

- [ ] **Step 4: Implement the workbench read service**

Use this public interface:

```python
class WorkbenchReadService:
    def __init__(self, session: AsyncSession, request_time: datetime) -> None:
        if request_time.tzinfo is None or request_time.utcoffset() is None:
            raise ValueError("request_time must be timezone-aware")
        self._session = session
        self._request_time = request_time.astimezone(UTC)

    async def read(self) -> WorkbenchResponse:
        paper = PaperReadService(self._session, self._request_time)
        cycles = await paper.agent_cycles(decision_session=None, limit=20)
        portfolio = await paper.portfolio()
        active_orders = await self._active_orders(limit=20)
        latest = cycles.items[0] if cycles.items else None
        loop = await self._loop_status()
        return WorkbenchResponse(
            mode="internal_paper",
            execution_locked=True,
            loop=loop,
            session=cycles.summary,
            latest_cycle=latest,
            active_orders=active_orders,
            account=account_summary(portfolio),
            attention=attention_items(
                latest=latest,
                orders=active_orders,
                loop=loop,
                session=cycles.summary,
            ),
        )
```

The private `_active_orders`, `_loop_status`, `account_summary`, and `attention_items` helpers implement the exact bounds and mapping rules below; they do not invoke producers or mutate records.

Select only `queue == "agent_paper"`, matching `CURRENT_MIGRATION_HEAD`, with `seen_at <= request_time`. Map the newest heartbeat to `online` when its age is at most 30 seconds, `offline` when older, and `unknown` when absent or incompatible. Never use a cycle timestamp as process liveness.

Compose latest cycle, session summary, up to 20 non-terminal orders, and the Paper account summary through existing read services. Generate at most 10 attention facts from exact conditions:

- cycle `unavailable` → `data` / `view_evidence`;
- cycle `failed` → `model` / `view_decision`;
- any rejected risk outcome → `risk` / `view_decision`;
- order `reconciling` → `execution` / `verify_original_result`;
- stale heartbeat while the session is eligible → `service` / `wait`.

Keep reason codes unchanged and let the Web layer supply Chinese copy.

- [ ] **Step 5: Add integration cases for complete, empty, stale, and partial data**

Test one representative persisted graph per case. Assert query output is bounded, ordered, account-owned, time-bounded, and free of secret/config fields. Verify `NO_ORDER_REQUIRED` returns an empty `active_orders` tuple without an error attention item.

- [ ] **Step 6: Run focused Engine tests**

Run:

```bash
uv run pytest engine/tests/integration/test_workbench_read_service.py engine/tests/contract/test_workbench_api.py engine/tests/integration/test_paper_api.py engine/tests/contract/test_openapi.py -q --import-mode=importlib
uv run ruff check engine/src/nexus/api/schemas/workbench.py engine/src/nexus/workbench engine/src/nexus/api/routes/workbench.py engine/src/nexus/api/routes/paper.py
uv run mypy engine/src/nexus/api/schemas/workbench.py engine/src/nexus/workbench engine/src/nexus/api/routes/workbench.py engine/src/nexus/api/routes/paper.py
```

Expected: PASS.

- [ ] **Step 7: Commit the read projections**

```bash
git add engine/src/nexus/api/schemas/workbench.py engine/src/nexus/api/schemas/paper.py engine/src/nexus/workbench engine/src/nexus/paper/read_service.py engine/src/nexus/api/routes/workbench.py engine/src/nexus/api/routes/paper.py engine/src/nexus/main.py engine/tests/integration/test_workbench_read_service.py engine/tests/contract/test_workbench_api.py engine/tests/integration/test_paper_api.py engine/tests/contract/test_openapi.py
git commit -m "feat(api): expose read-only workbench facts"
```

### Task 3: Add Signed BFF Routes and Generated Contracts

**Files:**
- Create: `app/api/v1/workbench/route.ts`
- Create: `app/api/v1/paper/cycles/[cycleId]/route.ts`
- Modify: `app/lib/paper/bff.ts`
- Modify: `app/lib/engine/client.ts`
- Modify: `app/lib/engine/errors.ts`
- Regenerate: `contracts/openapi.json`
- Regenerate: `app/lib/generated/schema.d.ts`
- Regenerate: `app/lib/generated/runtime-contract.ts`
- Create: `tests/unit/workbench/routes.test.ts`
- Modify: `tests/unit/paper/routes.test.ts`
- Modify: `tests/unit/engine/client-security.test.ts`

**Interfaces:**
- Consumes: the exact Engine endpoints from Task 2 and `withNexusBff` local-only actor enforcement.
- Produces: browser-safe `GET /api/v1/workbench`; browser-safe `GET /api/v1/paper/cycles/{cycleId}`; generated `WorkbenchResponse` TypeScript type.

- [ ] **Step 1: Write failing BFF route tests**

Assert exact forwarding targets and fail-closed input behavior:

```ts
expect(target).toEqual("/internal/v1/workbench");
expect(response.status).toBe(200);

await expect(GET(new Request("http://localhost/api/v1/workbench?account_id=secret")))
  .resolves.toHaveProperty("status", 400);

await expect(GET(new Request("http://localhost/api/v1/paper/cycles/not-a-uuid")))
  .resolves.toHaveProperty("status", 400);
```

- [ ] **Step 2: Run route tests and verify red**

Run:

```bash
npm exec vitest run tests/unit/workbench/routes.test.ts tests/unit/paper/routes.test.ts
```

Expected: FAIL because the routes and validation function do not exist.

- [ ] **Step 3: Implement exact GET proxies**

Use the existing wrappers:

```ts
export const GET = withNexusBff(async (request, actor) => {
  rejectQuery(request, "The workbench query is invalid.");
  return engineFetch("/internal/v1/workbench", { method: "GET", actor });
});
```

For cycle detail, validate the pathname UUID with `paperCycleId(request)` and forward with template `/internal/v1/paper/cycles/{cycle_id}`. Do not accept account IDs, arbitrary queries, or caller-supplied Engine headers.

- [ ] **Step 4: Regenerate and validate OpenAPI consumers**

Run:

```bash
npm run generate:client
npm run check:generated
```

Expected: generated files include the exact new paths and strict schema; no manual changes remain in generated files.

- [ ] **Step 5: Add response sanitizer coverage**

Extend `client-security.test.ts` to prove both new paths are allowed, remain bounded by `MAX_PAPER_RESPONSE_BYTES`, reject malformed dynamic cycle IDs, strip upstream headers, and sanitize invalid response bodies to `INTERNAL_ERROR`.

- [ ] **Step 6: Run BFF and security tests**

Run:

```bash
npm exec vitest run tests/unit/workbench/routes.test.ts tests/unit/paper/routes.test.ts tests/unit/engine/client-security.test.ts
npm run typecheck
```

Expected: PASS.

- [ ] **Step 7: Commit the Web contracts**

```bash
git add app/api/v1/workbench app/api/v1/paper/cycles app/lib/paper/bff.ts app/lib/engine/client.ts app/lib/engine/errors.ts contracts/openapi.json app/lib/generated/schema.d.ts app/lib/generated/runtime-contract.ts tests/unit/workbench/routes.test.ts tests/unit/paper/routes.test.ts tests/unit/engine/client-security.test.ts
git commit -m "feat(web): add workbench read contracts"
```

### Task 4: Centralize Product-state Language

**Files:**
- Create: `app/lib/product-status.ts`
- Create: `tests/unit/workbench/product-status.test.ts`

**Interfaces:**
- Consumes: generated `AgentPaperCycleResponse`, `AgentPaperSessionSummaryResponse`, `PaperOrderResponse`, and `WorkbenchAttentionResponse`.
- Produces: `cycleDecisionPresentation(cycle)`, `riskPresentation(cycle)`, `executionPresentation(cycle, orders)`, `loopPresentation(loop, session)`, and `attentionPresentation(item)`.

- [ ] **Step 1: Write the complete failing product-state matrix**

Use table-driven tests for these exact outcomes:

```ts
expect(cycleDecisionPresentation(cycle({ status: "unavailable" })).summary.key).toBe("data_insufficient");
expect(cycleDecisionPresentation(cycle({ status: "failed" })).summary.key).toBe("model_failed");
expect(cycleDecisionPresentation(cycle({ result_code: "NO_ORDER_REQUIRED", candidates: [hold] })).candidates[0].key).toBe("hold");
expect(cycleDecisionPresentation(cycle({ result_code: "NO_ORDER_REQUIRED", candidates: [watch] })).candidates[0].key).toBe("watch");
expect(riskPresentation(cycle({ risk_outcomes: ["rejected"] })).key).toBe("risk_rejected");
expect(executionPresentation(cycle({ status: "awaiting_market" }), [partialOrder]).key).toBe("partially_filled");
expect(executionPresentation(cycle({ status: "partially_filled" }), []).key).toBe("result_unknown");
```

Also cover buy, sell/reduce, no-order, waiting market, submitted, filled, cancelled, rejected, reconciling, online, offline, and unknown. Add a multi-candidate case proving the mapper preserves every candidate in canonical API order and never selects one candidate as the cycle-wide winner.

- [ ] **Step 2: Run the mapper tests and verify red**

Run:

```bash
npm exec vitest run tests/unit/workbench/product-status.test.ts
```

Expected: FAIL because the module does not exist.

- [ ] **Step 3: Implement discriminated presentation types**

Use stable keys and fixed copy:

```ts
export type ProductTone = "neutral" | "positive" | "warning" | "critical";
export type ProductPresentation<Key extends string> = {
  key: Key;
  label: string;
  detail: string;
  tone: ProductTone;
};

export type CycleDecisionPresentation = {
  summary: ProductPresentation<"in_progress" | "decision_recorded" | "data_insufficient" | "model_failed">;
  candidates: Array<ProductPresentation<"buy" | "sell" | "hold" | "watch" | "data_insufficient"> & {
    instrumentId: string;
    symbol: string;
  }>;
};
```

Unknown reason codes must map to a neutral “已保存状态，需查看记录代码” presentation and preserve the raw code in a separate field. Never convert unknown to success or failure. `result_unknown` exposes only `verify_original_result` as its safe action.

- [ ] **Step 4: Run mapper tests and static checks**

Run:

```bash
npm exec vitest run tests/unit/workbench/product-status.test.ts
npm run lint -- --quiet
npm run typecheck
```

Expected: PASS.

- [ ] **Step 5: Commit the state vocabulary**

```bash
git add app/lib/product-status.ts tests/unit/workbench/product-status.test.ts
git commit -m "feat(ui): centralize decision state language"
```

### Task 5: Build the Read-only Trading Workbench

**Files:**
- Create: `app/components/workbench/mode-strip.tsx`
- Create: `app/components/workbench/latest-decision.tsx`
- Create: `app/components/workbench/attention-list.tsx`
- Create: `app/components/workbench/order-summary.tsx`
- Create: `app/components/workbench/account-summary.tsx`
- Create: `app/(protected)/workspace/page.tsx`
- Modify: `app/styles/globals.css`
- Create: `tests/unit/workbench/page.test.tsx`
- Create: `tests/e2e/workbench.spec.ts`

**Interfaces:**
- Consumes: `GET /api/v1/workbench` and Task 4 presentation functions.
- Produces: `/workspace`, labeled `交易工作台`, with one GET-only refresh action and direct links to `/decisions/{cycleId}`, `/portfolio`, and existing evidence pages.

- [ ] **Step 1: Write failing page-state tests**

Cover loading, ready, empty, partial, stale, error, forbidden, offline loop, unknown loop, valid no-order, and result-unknown. In the ready test assert:

```ts
expect(screen.getByRole("heading", { name: "交易工作台" })).toBeVisible();
expect(screen.getByText("内部模拟")).toBeVisible();
expect(screen.getByText("实盘锁定")).toBeVisible();
expect(screen.getByRole("region", { name: "最新决策" })).toBeVisible();
expect(screen.getByRole("region", { name: "需要处理" })).toBeVisible();
expect(screen.queryByRole("button", { name: /运行|停止|批准/ })).not.toBeInTheDocument();
```

Spy on `fetch` and assert every request method is absent or `GET`.

- [ ] **Step 2: Run page tests and verify red**

Run:

```bash
npm exec vitest run tests/unit/workbench/page.test.tsx
```

Expected: FAIL because the page and components do not exist.

- [ ] **Step 3: Implement the bounded loader**

Use one abortable fetch with a 10-second timeout and stale-while-visible behavior matching existing pages:

```ts
const response = await fetch("/api/v1/workbench", {
  cache: "no-store",
  signal: AbortSignal.timeout(10_000),
});
```

On refresh failure, retain the last valid facts and mark the page stale. On 401/403, clear facts. Invalid response shape becomes error and never renders partial guessed data.

- [ ] **Step 4: Implement the selected decision-first layout**

Render in this order: mode strip; latest decision plus attention; active orders; recent cycle link; account summary. Use semantic headings, definition lists, and one `role="status"` announcement region. The main decision link is `查看完整决策`; no write control is present.

- [ ] **Step 5: Add focused responsive styles**

At desktop widths use a dominant decision column and a narrower attention column. Below the existing mobile breakpoint, use one column in semantic reading order; do not horizontally scroll the core summary. Reuse existing CSS variables and 44px interactive target minimums.

- [ ] **Step 6: Run unit and synthetic browser tests**

Run:

```bash
npm exec vitest run tests/unit/workbench/page.test.tsx
npm run test:e2e -- tests/e2e/workbench.spec.ts --workers=1
npm run lint
npm run typecheck
```

Expected: PASS; Playwright confirms desktop and 390×844 mobile layouts, GET-only refresh, and explicit offline/unknown states.

- [ ] **Step 7: Commit the workbench**

```bash
git add app/components/workbench 'app/(protected)/workspace/page.tsx' app/styles/globals.css tests/unit/workbench/page.test.tsx tests/e2e/workbench.spec.ts
git commit -m "feat(ui): add decision-first trading workbench"
```

### Task 6: Build Decision Records and Evidence Timeline

**Files:**
- Create: `app/components/decisions/decision-list.tsx`
- Create: `app/components/decisions/decision-timeline.tsx`
- Create: `app/(protected)/decisions/page.tsx`
- Create: `app/(protected)/decisions/[cycleId]/page.tsx`
- Modify: `app/styles/globals.css`
- Create: `tests/unit/decisions/pages.test.tsx`
- Create: `tests/e2e/decisions.spec.ts`

**Interfaces:**
- Consumes: `GET /api/v1/paper/cycles?limit=20`, the exact cycle-and-orders payload from `GET /api/v1/paper/cycles/{cycleId}`, and Task 4 presentation functions.
- Produces: `/decisions`; `/decisions/{cycleId}`; a read-only timeline from run window through evidence, decision, sizing, risk, order, execution, and outcome.

- [ ] **Step 1: Write failing list and detail tests**

Assert the list exposes one semantic row per cycle and visibly distinguishes:

```ts
for (const label of ["持有", "观察", "数据不足", "模型失败", "风控拒绝", "部分成交", "结果未知"]) {
  expect(screen.getByText(label)).toBeVisible();
}
```

The detail test asserts the ordered headings `运行窗口`, `市场与研究证据`, `Agent 决策`, `确定性仓位计算`, `确定性风控`, `订单结论`, `执行结果`, and `后续表现`. Missing stages render `未知` with a reason; they are not omitted or marked successful.

- [ ] **Step 2: Run decision tests and verify red**

Run:

```bash
npm exec vitest run tests/unit/decisions/pages.test.tsx
```

Expected: FAIL because the pages and components do not exist.

- [ ] **Step 3: Implement the bounded list**

Fetch 20 current-session cycles. Provide only date, decision type, and final-status filters in Phase 1. Filtering happens over the bounded loaded page and never changes or replays the underlying cycle. Each row links by exact `cycle_id`.

- [ ] **Step 4: Implement the detail timeline**

Use the cycle-detail GET as the sole authority for the cycle and its linked orders. Display the decision and short reason first. Put evidence identifiers, source timestamps, risk outcomes, order events, fills, and raw result code in collapsible sections. When the returned cycle/order facts cannot prove the result, show `结果未知` and the single safe action `核对原操作结果`.

- [ ] **Step 5: Run unit and browser flow tests**

Run:

```bash
npm exec vitest run tests/unit/decisions/pages.test.tsx tests/unit/workbench/product-status.test.ts
npm run test:e2e -- tests/e2e/decisions.spec.ts --workers=1
npm run lint
npm run typecheck
```

Expected: PASS; Playwright proves one click from `/workspace` to the latest decision, ordered timeline reading, keyboard-operable disclosures, and no POST/PUT/PATCH/DELETE request.

- [ ] **Step 6: Commit decision records**

```bash
git add app/components/decisions 'app/(protected)/decisions' app/styles/globals.css tests/unit/decisions/pages.test.tsx tests/e2e/decisions.spec.ts
git commit -m "feat(ui): add decision record timeline"
```

### Task 7: Migrate Navigation and Close the Mobile Drawer

**Files:**
- Modify: `app/components/app-shell.tsx`
- Modify: `app/page.tsx`
- Modify: `tests/unit/components/app-shell.test.tsx`
- Modify: `tests/e2e/app-shell.contract.spec.ts`
- Modify: `tests/e2e/overview.spec.ts`
- Modify: `tests/e2e/paper.spec.ts`

**Interfaces:**
- Consumes: `/workspace` and `/decisions` from Tasks 5–6; compatible existing `/portfolio`, `/paper`, and `/integrations` routes.
- Produces: primary navigation in this exact order: `交易工作台`, `决策记录`, `账户与交易`, `复盘`; secondary navigation: `设置与运行`; root redirect to `/workspace`; drawer closes after client navigation.

- [ ] **Step 1: Update navigation contract tests first**

Use these exact transitional destinations:

```ts
const primary = [
  ["交易工作台", "/workspace"],
  ["决策记录", "/decisions"],
  ["账户与交易", "/portfolio"],
  ["复盘", "/paper#paper-comparison-title"],
];
const secondary = [["设置与运行", "/integrations"]];
```

Add a mobile test that opens the `<details>`, clicks `决策记录`, expects `router.push("/decisions")`, and expects `drawer.open === false`. Assert every desktop and mobile link has the visible name and a 44×44px minimum hit area.

- [ ] **Step 2: Run navigation tests and verify red**

Run:

```bash
npm exec vitest run tests/unit/components/app-shell.test.tsx
npm run test:e2e -- tests/e2e/app-shell.contract.spec.ts --workers=1
```

Expected: FAIL against the old navigation and uncontrolled drawer.

- [ ] **Step 3: Implement the new information architecture**

Represent navigation items with explicit active paths so `/paper` activates `复盘`, `/portfolio` activates `账户与交易`, and nested decision routes activate `决策记录`. Preserve contextual direct URLs but remove `市场`, `优化`, `风险与审计`, `策略`, and `集成` from the top-level list.

Add a drawer ref and close it only after an unmodified primary-button navigation:

```ts
const drawerRef = useRef<HTMLDetailsElement>(null);

function onMobileNavigate(): void {
  if (drawerRef.current) drawerRef.current.open = false;
}
```

The existing route-change focus behavior remains: initial load does not steal focus; a client route change focuses `#main-content`.

- [ ] **Step 4: Change the root redirect and repair route assertions**

Change only:

```ts
redirect("/workspace");
```

Update old E2E tests to reach `/overview` and `/paper` directly where they are testing those legacy pages. Do not rewrite their page semantics.

- [ ] **Step 5: Run navigation and legacy page regressions**

Run:

```bash
npm exec vitest run tests/unit/components/app-shell.test.tsx tests/unit/workbench/page.test.tsx tests/unit/decisions/pages.test.tsx
npm run test:e2e -- tests/e2e/app-shell.contract.spec.ts tests/e2e/overview.spec.ts tests/e2e/paper.spec.ts tests/e2e/workbench.spec.ts tests/e2e/decisions.spec.ts --workers=1
npm run lint
npm run typecheck
```

Expected: PASS.

- [ ] **Step 6: Commit navigation migration**

```bash
git add app/components/app-shell.tsx app/page.tsx tests/unit/components/app-shell.test.tsx tests/e2e/app-shell.contract.spec.ts tests/e2e/overview.spec.ts tests/e2e/paper.spec.ts
git commit -m "feat(ui): migrate NEXUS primary navigation"
```

### Task 8: Document and Verify the Complete Phase 1 Slice

**Files:**
- Modify: `README.md`
- Modify: `docs/runbooks/agent-paper-cycle.md`
- Modify: `docs/superpowers/specs/2026-09-15-nexus-product-experience-redesign-design.md`
- Test: all Phase 1 focused suites and current CI gates.

**Interfaces:**
- Consumes: all Phase 1 deliverables.
- Produces: user-facing entry instructions, exact liveness semantics, verified real local flow, and a design spec status of `阶段 1 已实现` only after acceptance succeeds.

- [ ] **Step 1: Update documentation with exact user behavior**

Document `/workspace` as the default entry and state explicitly:

```text
“循环在线”来自 30 秒内兼容的 agent_paper heartbeat；“最近周期已完成”来自已保存周期。
二者互不替代。页面刷新只重读已保存事实，不启动或重放 Agent。
```

Keep local-only access and `execution_locked=true` wording unchanged.

- [ ] **Step 2: Run repository-wide generated and formatting checks**

Run:

```bash
git diff --check
npm run check:generated
npm run lint
npm run typecheck
npm run test:unit
npm run build
uv run ruff check engine
uv run mypy engine/src
```

Expected: all commands exit 0.

- [ ] **Step 3: Run the focused Engine and browser acceptance suites**

Run:

```bash
uv run pytest -q --import-mode=importlib \
  engine/tests/unit/ops/test_agent_paper_loop.py \
  engine/tests/integration/test_agent_paper_loop_heartbeat_migration.py \
  engine/tests/integration/test_workbench_read_service.py \
  engine/tests/contract/test_workbench_api.py \
  engine/tests/integration/test_paper_api.py \
  engine/tests/contract/test_openapi.py
npm run test:e2e -- \
  tests/e2e/app-shell.contract.spec.ts \
  tests/e2e/workbench.spec.ts \
  tests/e2e/decisions.spec.ts \
  tests/e2e/overview.spec.ts \
  tests/e2e/paper.spec.ts \
  --workers=1
```

Expected: all commands exit 0 without external broker/model calls.

- [ ] **Step 4: Verify one real local persisted read flow**

Start the current Engine and Web through documented local entrypoints. Without triggering any producer, verify:

1. `/workspace` shows the actual loop liveness and latest persisted cycle as separate fields;
2. the latest decision opens `/decisions/{cycleId}` in one click;
3. the detail references the same persisted cycle, decision, order IDs, and timestamps as the API;
4. an empty active-order set displays a valid no-order state;
5. a stale heartbeat displays offline or unknown, never online;
6. browser network history contains no business POST, PUT, PATCH, or DELETE.

Record only redacted IDs and timestamps in the existing phase evidence document. Do not replay a cycle to manufacture evidence.

- [ ] **Step 5: Run a focused final review**

Review for forward-looking data, status conflation, secret exposure, order replay, approval affordances, mobile focus, accessible names, and compatibility of legacy routes. Fix findings and rerun only affected focused suites plus `git diff --check`.

- [ ] **Step 6: Commit documentation and acceptance evidence**

```bash
git add README.md docs/runbooks/agent-paper-cycle.md docs/superpowers/specs/2026-09-15-nexus-product-experience-redesign-design.md
git commit -m "docs(product): document Phase 1 workbench flow"
```

## Completion Boundary

Phase 1 is complete only when the new workbench and decision flow read the same persisted facts as the Engine, the loop heartbeat is independently verified, every required state is covered, legacy routes remain usable, and the real local browser flow performs no business write. A passing build, an HTTP 200, or a recent completed cycle alone is not acceptance.

After completion, create separate plans for account/transaction restructuring, review/experiments, Bark summaries, and browser-based run controls. None of those follow-on scopes are implicit in this plan.
