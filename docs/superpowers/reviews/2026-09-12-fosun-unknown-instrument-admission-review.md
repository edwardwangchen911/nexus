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

The three unawarded points are implementation-layout uncertainty, not unresolved
business semantics. The design is sufficiently precise for direct test-first coding.
