# Fosun Classification Agent Evidence Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Persist already-returned Fosun classification evidence and expose the exact frozen observation through `get_security_snapshot`, without a new provider call or execution authority.

**Architecture:** Reuse `DataArtifact -> DataSnapshot + ResearchEvidenceRef -> AgentDataSnapshotEvidenceRecord`; capture in the existing full-refresh transaction, freeze one observation, and project only frozen IDs.

**Tech Stack:** Python 3.12, Pydantic v2, SQLAlchemy async, PostgreSQL, Alembic, pytest, Ruff, Mypy.

---

## Preconditions

- Work only in `.worktrees/fosun-classification-agent-evidence`.
- Never alter main's dirty Provider Worker/compose files.
- No real broker/model call, new table/dependency/tool/page fetch, or execution change.
- Tests precede implementation and each task ends green before the next begins.

### Task 1: Canonical document

**Files:** create `engine/src/nexus/research/fosun_security_classification.py` and `engine/tests/unit/research/test_fosun_security_classification.py`.

- [ ] Test deterministic bytes/object ID; three routing families; empty strings;
  unequal/future clocks; bad code/hash; caller-injected routing mismatch.
- [ ] Run `uv run --package nexus-engine pytest engine/tests/unit/research/test_fosun_security_classification.py -q`; record expected import failure.
- [ ] Implement design section 4 using existing Fosun validator/classifier and `require_utc`.
- [ ] Re-run test and focused Ruff.
- [ ] Commit `feat(engine): define Fosun classification evidence`.

### Task 2: Persistence and PIT reader

**Files:** create `engine/src/nexus/research/fosun_security_classification_storage.py` and `engine/tests/integration/test_fosun_security_classification_storage.py`.

- [ ] Seed isolated PostgreSQL Instrument/raw artifact fixtures. Test canonical
  readback, family variants, replay, owner conflict, raw mismatch, future clocks,
  cutoff, evidence filter, same-rank conflict, and corruption.
- [ ] Confirm absent functions fail.
- [ ] Implement content-addressed save, strict metadata readback, nested idempotent
  evidence/snapshot writes, raw/canonical validation, and deterministic reader.
- [ ] Run focused integration test and Ruff.
- [ ] Commit `feat(engine): persist Fosun classification evidence`.

### Task 3: Full-refresh capture

**Files:** modify `engine/src/nexus/ops/fosun_local_refresh.py`,
`engine/tests/integration/test_fosun_local_refresh.py`, and only necessary focused fixtures.

- [ ] Add complete/partial/absent items; assert artifact/snapshot/evidence and raw
  binding; assert existing provider request count is unchanged.
- [ ] Confirm assertions fail.
- [ ] Build/persist/read-back documents immediately after raw quote artifact flush,
  only for non-`None` classification; caller retains transaction ownership.
- [ ] Run local-refresh tests and Ruff.
- [ ] Commit `feat(engine): capture Fosun classifications on refresh`.

### Task 4: Snapshot freezing

**Files:** modify `engine/src/nexus/agents/snapshots.py`; create or extend focused Agent PostgreSQL tests.

- [ ] Test classification cannot create a base snapshot; cutoff; changed evidence
  changes only a new revision; same-rank conflict; SEC-only capacity eviction; old shape.
- [ ] Confirm failures, then select one reference after the base-evidence check and
  preserve non-SEC supplements.
- [ ] Run focused tests/Ruff and commit `feat(engine): freeze Fosun classification evidence`.

### Task 5: Agent projection

**Files:** modify `engine/src/nexus/agents/database_tools.py` and focused Agent tests.

- [ ] Test exact JSON/citation; old Run; later evidence ignored; missing/corrupt
  owner, raw tamper, wrong instrument, and no latest fallback.
- [ ] Confirm failures, then compare frozen classification ID set to validated reader
  keys before projection; mismatch uses existing invalid-authority result.
- [ ] Run focused tests/Ruff and commit `feat(engine): expose frozen Fosun classification`.

### Task 6: Ownership migration

**Files:** create `engine/alembic/versions/0052_fosun_security_classification.py`
(revision ID `0052_fosun_classification`) and
`engine/tests/integration/test_fosun_security_classification_migration.py`.

- [ ] Test 0051->0052; valid freeze; invalid schema/source/object/instrument/hash/
  length/location/clocks/raw binding/flags; safe downgrade; refused downgrade with
  frozen owner; persisted generic rows survive re-upgrade.
- [ ] Confirm missing revision fails, then add the bounded validation-function branch;
  create no table.
- [ ] Run focused migration test/head check and commit `feat(db): guard Fosun classification evidence`.

### Task 7: Regression and handoff

- [ ] Run all new tests together plus existing Fosun roundtrip, local refresh, Agent
  snapshot/tool, and neighboring migration tests.
- [ ] Run focused Ruff, `uv run --package nexus-engine mypy engine/src/nexus`,
  `git diff --check`, status, and diff-stat inspection.
- [ ] Confirm no secret, Web, execution, risk, dependency, or unrelated Provider
  Worker change. Re-score any changed contract before acceptance.
- [ ] Do not merge into dirty main; report branch/worktree and exact integration dependency.

Completion requires the isolated migration at head, zero extra provider requests in
the refresh test, exact raw-bound frozen projection, passing focused verification,
and unchanged execution lock. A helper-only result is incomplete.
