# Agent Paper Research Universe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a versioned `AAPL / MSFT / JPM` Agent Paper research universe with one-symbol-per-slot deterministic rotation and fail-closed recovery.

**Architecture:** A small immutable universe module owns the ordered members and pure rotation reducer. Each cycle freezes its selected target in the append-only prepared event, while all evidence producers consume that same target; existing cycle, risk, order, and ledger boundaries remain unchanged.

**Tech Stack:** Python 3.12, Pydantic 2, SQLAlchemy async, PostgreSQL, pytest, Ruff, Mypy

**Spec:** `docs/superpowers/specs/2026-09-17-agent-paper-research-universe-design.md`

## Global Constraints

- Universe version is exactly `agent_paper_research_v1` with ordered symbols `AAPL`, `MSFT`, `JPM`.
- One cycle contains at most one candidate and one research job.
- Candidate failures rotate only after attempt 3; system/authority failures never advance the cursor.
- Existing persisted manifests and event hashes remain readable without mutation.
- `execution_locked=true`, deterministic sizing, portfolio risk, order, fill, and ledger code are unchanged.

---

### Task 1: Versioned universe and pure rotation reducer

**Files:**
- Create: `engine/src/nexus/paper/agent_research_universe.py`
- Create: `engine/tests/unit/paper/test_agent_research_universe.py`

**Interfaces:**
- Produces: `ResearchTarget`, `ResearchCycleOutcome`, `AGENT_PAPER_RESEARCH_UNIVERSE_V1`, `choose_research_target(outcomes)`.
- `ResearchTarget` carries `version: str`, `ordinal: int`, `symbol: Literal["AAPL", "MSFT", "JPM"]`, and `attempt: int`.

- [ ] **Step 1: Write failing table-driven tests** for empty history, completed advance, first/second candidate failure retry, third failure advance, wraparound, version isolation, and non-candidate failure hold.
- [ ] **Step 2: Run** `uv run pytest engine/tests/unit/paper/test_agent_research_universe.py -q --import-mode=importlib` and confirm import/behavior failures.
- [ ] **Step 3: Implement the immutable definition and reducer** with literal expected outputs; reject malformed ordinal, attempt, nonterminal ordering, or unknown current-version symbols with `AGENT_PAPER_ROTATION_INVALID`.
- [ ] **Step 4: Re-run the test** and confirm all cases pass.

### Task 2: Freeze and reload the selected target

**Files:**
- Modify: `engine/src/nexus/paper/agent_cycle_models.py`
- Modify: `engine/src/nexus/paper/agent_cycle_authority.py`
- Modify: `engine/src/nexus/paper/agent_cycle.py`
- Modify: `engine/tests/unit/paper/test_agent_cycle.py`
- Modify: `engine/tests/integration/test_agent_paper_cycle_pg.py`

**Interfaces:**
- Consumes: `ResearchTarget` and `choose_research_target` from Task 1.
- Produces: `select_agent_paper_research_target(session, account_id)` and prepared-event facts `research_universe_version`, `research_universe_ordinal`, `research_symbol`, `research_attempt`.

- [ ] **Step 1: Add failing manifest tests** proving AAPL/MSFT/JPM validate, an outside ticker fails, and the old AAPL manifest hash remains the existing literal.
- [ ] **Step 2: Add failing PostgreSQL tests** that seed terminal cycle events and assert target selection plus prepared-event freeze/readback.
- [ ] **Step 3: Run the focused tests** and confirm failures are caused by missing target support.
- [ ] **Step 4: Expand the manifest symbol literal**, query current-version outcomes from prepared and terminal events, and pass one target into candidate preparation/validation.
- [ ] **Step 5: Append target facts with `CYCLE_PREPARED`** and map the third candidate failure to `unavailable / RESEARCH_BLOCKED`; leave authority failures unchanged.
- [ ] **Step 6: Re-run focused unit and PostgreSQL tests** and confirm old-cycle compatibility plus new rotation behavior.

### Task 3: Thread one target through provider prerequisites

**Files:**
- Modify: `engine/src/nexus/ops/agent_paper_once.py`
- Modify: `engine/src/nexus/ops/agent_paper_loop.py`
- Modify: `engine/src/nexus/ops/schwab_market_stream.py`
- Modify: `engine/tests/unit/ops/test_agent_paper_once.py`
- Modify: `engine/tests/unit/ops/test_agent_paper_loop.py`
- Modify: `engine/tests/unit/ops/test_schwab_market_stream.py`

**Interfaces:**
- Consumes: frozen `ResearchTarget` selected during trigger reservation.
- Produces: symbol-parameterized daily/trend preparation, source proofs, stream capture, quote validation, and cycle start.

- [ ] **Step 1: Add failing tests** using `MSFT` to assert every provider boundary receives `MSFT` and no assertion still reads the `AAPL` key.
- [ ] **Step 2: Run focused ops tests** and confirm the hard-coded AAPL behavior fails them.
- [ ] **Step 3: Add the target to trigger reservation/hash**, parameterize prerequisite scopes and `capture_and_import_schwab_level_one_once(symbol, instrument_id)`, and validate the returned quote by target symbol.
- [ ] **Step 4: Re-run focused ops tests** and confirm AAPL compatibility plus MSFT/JPM parameterization.

### Task 4: Scheduler continuation and regression verification

**Files:**
- Modify: `engine/src/nexus/ops/agent_paper_loop.py`
- Modify: `engine/tests/unit/ops/test_agent_paper_loop.py`
- Modify: `docs/runbooks/nexus-agent-paper-loop.md`

**Interfaces:**
- Consumes: candidate failure result codes and attempt facts.
- Produces: same-slot wait then next-slot retry; `RESEARCH_BLOCKED` proceeds to the next member without weakening hard-stop codes.

- [x] **Step 1: Add failing planner tests** proving retryable candidate failure waits for the next slot, blocked completion continues, and authority/result-unknown failures still stop.
- [x] **Step 2: Run the planner tests** and confirm current `STOP_CYCLE_UNAVAILABLE` behavior fails the retry case.
- [x] **Step 3: Add result code/attempt to the loop snapshot** and narrowly classify retryable candidate terminal states.
- [x] **Step 4: Update the runbook** with the fixed version, order, retry threshold, exact stop boundary, and persisted readback fields.
- [x] **Step 5: Run focused tests, then** `uv run ruff check engine`, `uv run mypy engine/src`, and the related Agent Paper unit/integration suite.
- [ ] **Step 6: Inspect persisted `run → job → output` state** before any normal CLI acceptance; resume the existing 17:25 UTC cycle if still unfinished, and never replay an uncertain provider/model call.
