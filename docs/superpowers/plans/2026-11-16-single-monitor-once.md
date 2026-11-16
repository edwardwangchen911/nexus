# Single monitor once implementation plan

> For agentic workers: use superpowers:executing-plans. One Python writer here; independent migration writer in its own checkout.

**Goal:** One newly authorized Sol Responses single-stock monitoring root, preserving the original full candidate batch, five natural obligations, all old unknowns, and 13USD.
**Architecture:** New explicit request v6/research_and_monitor, exact BudgetedSingleSolBinding-derived immutable selection, independent_monitor snapshot/prompt. Existing v5/v3 unchanged. A unique partial account index allows only one v6 request ever; replays read the same original key. New unknown cannot use old acceptance allowlists. Historical proof hashes rechecked before each effect. Normal CLI controls inspect/start.
**Tech Stack:** Python3.12, SQLAlchemy/PostgreSQL, Alembic.
**Spec:** user approval 2026-11-16 only this new single-monitor request; existing Chat review marker NEXUS-SINGLE-MONITOR-ENTRY-20261116-R1.

## Global constraints
- New v6 actor local-single-monitor-sol-v1, prefix single-monitor-sol.
- Binding history_policy single_monitor_original_acceptances_once_v1; admission_policy shared_budget_v1; authorization_scope single_monitor_root_20261116_once_v1.
- Full batch/hash retained; member exactly selected instrument. Model/pricing/calendar/deadline/protection hash remain exact.
- Research purpose independent_monitor_research_v1, independent monitor prompt, no trading/research_only relabel.
- No v6 in unknown acceptance allowlists. No bypass deadline, SEC freshness, native quote authority, two reviewers/adoption, budget, or settlement/cleanup.

## Review focus
- Second different key after a completed/failed/unknown v6 must fail (not just active occupancy).
- v5 cannot acquire monitor purpose and v6 cannot use research_only prompt.
- Selection/actor/prefix/context tampering must fail Python and SQL.
- Per-stage budget/deadline guards apply before every effect.
- Old raw archives/output/settlement/proofs remain immutable; schema downgrade rejects any v6 fact.

## Task 1: Python entrance and execution
- [ ] Add failing focused tests for binding version/mode/policy and CLI flags.
- [ ] Implement v6 binding, normal inspect/start-single-monitor-sol and one-time admission fence.
- [ ] Update exact prepare/read/profile/host/worker/stage branches; retain all old rejection branches.
- [ ] Run focused unit + lifecycle tests and lint/type checks.

## Task 2: Database binding (separate writer)
- [ ] New migration0122, strict version6 constraint + one-time unique account index.
- [ ] Extend manifest/route/snapshot guards only exact v6 identity, selected member and monitor purpose.
- [ ] Real isolated PG upgrade/downgrade/positive and negative constraints.

## Task 3: Actual material/deployment/root
- [ ] Reuse archived raw identity/submissions via normal recent index path if legitimately sufficient; listed conflict remains rejected.
- [ ] Independent whole diff review and same Chat final review.
- [ ] Relevant verification, backup and restore, compatible maintenance image, normal deployment and stable heartbeat.
- [ ] Normal inspect fresh budget/materials, one new submission only after proved absent, track original ID through output/settlement/cleanup/API/page.
- [ ] Preserve all old unknowns, retain rollback, bounded cleanup, update existing handoff.
