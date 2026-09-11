# Fosun Classification Agent Evidence Design Review

Review date: 2026-09-12  
Method: existing-code fit review and 100-point implementation-readiness rubric

## Gate result

**PASS — 97/100. Development may start.**

The first draft failed because it introduced a source-specific table despite the
existing generic evidence path. That duplication was removed before scoring.

| Category | Weight | Score | Basis |
| --- | ---: | ---: | --- |
| Requirements/scope | 15 | 15 | Outcome, non-goals, lock, acceptance are explicit |
| Architecture fit | 15 | 15 | Existing artifacts, snapshots, refs, frozen links and tool reused |
| Contract/provenance | 20 | 20 | Types, canonicalization, IDs, raw binding, hashes, clocks specified |
| Temporal/realtime | 15 | 15 | PIT, conflict, no tool fetch/backfill, intraday exclusion specified |
| Transaction/failures | 15 | 14 | Ownership, savepoint, replay, readback, rollback and failures specified |
| Migration/compatibility | 10 | 9 | Upgrade/refusal downgrade/old Runs/rollback specified |
| Testing/operability | 10 | 9 | Red-green tasks, exact files/commands, no-real-call acceptance specified |
| **Total** | **100** | **97** | **Above the 95 gate** |

## Findings resolved

1. Removed the over-designed classification table and second owner model.
2. Made normalized evidence own the Research ref while binding the exact raw quote.
3. Restricted production to full refresh; Agent/page/snapshot are persisted reads.
4. Required frozen source IDs to equal validated reader keys, preventing silent omit.
5. Defined capacity behavior: evict SEC only, retain other supplements.
6. Made downgrade refuse frozen owners and forbid evidence deletion.
7. Ensured new/unknown securities do not fail refresh unless source types are malformed.

## Remaining implementation risks

- The database ownership function is already large; add one bounded branch only.
- Wire request identifiers using the exact existing `RawGatewayResponse` names without
  weakening the binding if nomenclature differs.
- Main is dirty from another task; implementation and testing stay isolated.

The three unawarded points require runtime evidence, not more abstraction. The
documents are detailed enough for direct task-by-task coding.

Implementation review note: the first migration run exposed the repository's
32-character Alembic version-column limit. The revision ID was shortened to
`0052_fosun_classification`; the transaction rolled back and the contract did not
change. Score remains 97/100.
