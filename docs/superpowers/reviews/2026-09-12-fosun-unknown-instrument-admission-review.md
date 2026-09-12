# Fosun Unknown Instrument Admission Design Review

Review date: 2026-09-12
Method: verified production-path trace and 100-point implementation-readiness rubric

## Gate result

**PASS — 97/100. Development may start after user review.**

| Category | Weight | Score | Basis |
| --- | ---: | ---: | --- |
| Root cause and requirements | 15 | 15 | Real failure, exact reject point, and desired partial outcome are explicit |
| Source semantics | 15 | 15 | Useful Fosun fields are retained without claiming exchange or ETF/ETN authority |
| Architecture fit | 15 | 14 | Generic immutable evidence and existing identity importers are reused |
| Provenance and temporal rules | 15 | 15 | Raw binding, canonical revision, clocks, replay, and later resolution are specified |
| Transaction and failure behavior | 15 | 15 | Known market commit, partial sync, no partial portfolio, and stop boundaries are explicit |
| Safety and compatibility | 10 | 10 | No invented identity, no execution unlock, no provider calls from reads, additive rollback |
| Testability and operations | 15 | 13 | Exact matrix and rollout gate are defined; final API file names depend on existing routing layout |
| **Total** | **100** | **97** | **Above the 95 implementation gate** |

## Critical correction to the prior review

The earlier classification review awarded credit for “new/unknown securities do not
fail refresh.” That statement was not exercised through the actual full-refresh order.
The production trace and real run prove the opposite: identity resolution occurs
before the quote/classification producer. This review withdraws that claim and makes
the unknown-holding path a required cross-layer acceptance case.

## Rejected alternatives

1. **Create a US Instrument directly from Fosun fields.** Rejected because `us` is not
   an exact listing exchange and observed ETF/ETN classifications collide.
2. **Ignore the unknown position and publish a READY subset portfolio.** Rejected
   because cash, positions, orders, and totals would have different scopes.
3. **Fail before quote acquisition as today.** Rejected because it discards safe,
   useful discovery evidence and blocks updates for known market instruments.
4. **Automatically call every identity source from the page or refresh.** Rejected
   because it creates hidden external work, mixes cadences, and can consume quota.
5. **Add a source-specific pending table.** Rejected because immutable artifacts and
   snapshots already represent the evidence; pending is derived from current alias
   resolution.

## Residual risks and controls

- Splitting quote and bar acquisition changes request accounting. Literal request-count
  tests must cover READY and pending branches.
- A PARTIAL sync without a PortfolioSnapshot must not replace the latest complete
  portfolio in read models. PostgreSQL and API tests are mandatory.
- The raw quote contains unresolved and resolved rows. Per-code normalized documents
  must bind the same whole-response artifact without pretending it is per-code raw.
- Discovery rows may accumulate. They are immutable audit evidence; current pending
  reads select bounded latest rows and derive resolution, rather than deleting history.
- Exact ETF/ETN resolution remains external to this slice. The UI and Agent must call
  it pending, not “ETF detected.”
- Cold-start pending runs reuse the existing local Account/Portfolio container rules;
  tests must prove foreign-key correctness without creating an unresolved Instrument.

The three unawarded points are implementation-layout uncertainty, not unresolved
business semantics. The design is sufficiently precise for direct test-first coding.

## Implementation verification

Verification date: 2026-09-12
Branch: `codex/fosun-identity-pending`
Feature migration impact: none. The isolated branch was verified at
`0052_fosun_classification`; after integration, unrelated Alpaca/Schwab migrations
advance current `main` to `0054_schwab_stream_policy`.

The implementation preserves the previous complete portfolio snapshot, records the
new attempt as `PARTIAL / IDENTITY_PENDING`, and publishes only bounded provider-code
discovery fields. Unknown securities do not receive an `Instrument`, alias, position,
order, decision authority, or execution authority. Portfolio and page reads remain
persisted-only and perform no provider or model work.

### Verification evidence

- Fresh empty PostgreSQL migration from `0001` through
  `0052_fosun_classification`, followed by all six affected unit/integration files:
  **97 passed**.
- Discovery artifact/snapshot replay, raw-binding tamper rejection, same-clock
  conflict, and post-alias resolution: **3 passed**.
- Post-merge Engine CI gate on `main` at migration head
  `0054_schwab_stream_policy`: **3630 passed, 6 skipped, 1 deselected**.
- Engine CI merge gate, including all three new PostgreSQL files: **3625 passed,
  6 skipped, 1 performance test deselected**.
- Ruff: all Engine checks passed. Mypy: no issues in **431 source files**.
- OpenAPI generated-client check, frontend ESLint, and TypeScript: passed.
- Frontend unit suite: **976 passed, 1 skipped**. Portfolio Playwright suite across
  mobile, tablet, desktop, keyboard, and read-only boundaries: **12 passed**.
- Production Web build: passed.

### Final review findings

Two verification gaps were found and fixed; neither required a production-policy
change:

1. The explicit Engine merge gate did not collect the three new PostgreSQL files.
   They are now listed in `.github/workflows/ci.yml`.
2. The maximum legal portfolio-envelope test used the old response contract. It now
   includes the maximum 20 pending records together with 2000 positions and 2000 open
   orders and still verifies the 4 MiB BFF limit.

No remaining code finding was identified for invented identity, partial-portfolio
leakage, raw/hash/clock mismatch acceptance, hidden provider calls, request-count
drift, API privacy, or page-triggered external work.

### Residual risks

- No real provider refresh was run during development verification. Actual Fosun
  response shape, local credentials, service image/code version, and object-store
  connectivity still require one bounded runtime acceptance after integration.
- Fosun routing evidence intentionally does not distinguish ETF from ETN. Resolution
  remains the responsibility of an existing authoritative identity importer.
- A provider result whose completion is uncertain must be checked in persisted
  `run -> job -> outputs` state and must not be replayed merely because observation
  timed out.

### Rollout sequence

1. Merge the isolated branch only when the main checkout is clean or its unrelated
   work has been safely committed by its owner; never reset or stash those changes.
2. Create a protected complete backup using the existing backup runbook.
3. Rebuild the API/Web/runtime artifacts from the merged revision and verify Alembic
   still reports `0052_fosun_classification` as head.
4. Replace only the affected local services and verify their running revision and
   `local_read_only` configuration before any provider call.
5. Inspect persisted `BrokerSyncRun`, job, and output state. If no completed or
   uncertain acceptance attempt exists, invoke exactly one normal full Fosun refresh.
6. Verify the same attempt across raw quote artifact, discovery snapshot, known-market
   updates, `PARTIAL / IDENTITY_PENDING` sync, absence of a new portfolio snapshot,
   protected API projection, and the portfolio page. Execution must remain locked.
7. If the attempt is READY because every identity is already authoritative, verify
   the unchanged complete-snapshot path instead; do not manufacture an unknown symbol.
