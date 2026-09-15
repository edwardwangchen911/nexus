# 交易工作台 Design QA

Result: passed

## Source truth and captures

- Selected visual source: `/Users/wangchen/.codex/generated_images/01a0a3e3-543b-7ab3-8b2a-98754ae30bff/exec-62ff1c41-f607-4cd2-a9a7-11b66423b28a.png`
- Desktop implementation: `test-results/workbench-desktop.png`
- Mobile implementation: `test-results/workbench-mobile.png`
- Full comparison input: `test-results/workbench-comparison.png`
- Browser verification URL: `http://127.0.0.1:4174/workspace`

## Verification context

- Desktop viewport: 1440 × 1000 CSS px; full-page capture: 1440 × 1456 px.
- Mobile viewport: 390 × 844 CSS px; full-page capture: 390 × 2274 px.
- Reference image: 1487 × 1058 px.
- Density: 1× CSS pixel capture. Reduced motion was enabled for deterministic browser verification.
- State: internal paper, execution locked, loop online, one persisted AAPL hold decision, no active order, one risk attention item, ready account summary.
- Focused-region capture was not required: the latest-decision and attention regions are fully visible together in the desktop viewport and the full-page comparison covers all five required surfaces.

## Comparison findings

- Product hierarchy matches the source: persistent mode/lock strip, dominant latest decision, narrow attention rail, orders, then account facts.
- The implementation preserves the repository shell and its global data-source boundary. This adds vertical space above the source composition but keeps the decision completely visible in the initial desktop viewport.
- The source contains generated natural-language rationale and a system log. The current API does not provide either fact, so the implementation uses saved product-state explanations and omits an invented log.
- Color, border weight, compact typography, numeric alignment, status accents, and wide-to-narrow primary grid follow the selected direction while reusing repository tokens.
- Mobile collapses in semantic order with no core horizontal scrolling. Decision, attention, orders, and account facts remain legible at 390 px.

## Required surfaces

1. Running mode: internal simulation, live execution lock, loop state, heartbeat and session are visible.
2. Latest decision: decision status, all candidates, deterministic risk, execution result, timestamp and detail link are visible.
3. Attention: distinct safe state, raw reason code and safe navigation are visible.
4. Active orders: valid no-order state and recent-cycle link are visible; populated orders are covered by unit tests.
5. Account summary: currency, net asset value, cash, market value, P&L and fact time are visible.

## Interaction and safety checks

- Refresh was exercised in desktop and mobile browser tests; every `/api/v1/workbench` request used GET.
- Failed refresh retains the last valid facts and marks them stale. Forbidden responses clear prior facts.
- Offline, unknown-result, empty, partial, stale, error and forbidden states have explicit coverage.
- No run, stop, approve, submit or transaction-retry control appears on this page.
- Desktop and mobile accessibility checks passed, including 44 px targets, no serious/critical Axe findings and no page-level horizontal overflow.
- In-app browser console: no warnings or errors.

## Iteration history

1. Initial implementation used the repository's existing shell, status tokens and generated workbench contract.
2. Browser comparison confirmed the decision-first hierarchy. Mobile and desktop assertions were tightened to validate column ordering and read-only refresh behavior.
3. Final in-app browser capture confirmed the ready state and zero console warnings/errors; no further visual correction was required.
