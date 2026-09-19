# Task 12 — selected-source implementation QA

final result: blocked

The selected direction is implemented and the four-file functional e2e suite passes (30/30); full unit verification passes (1090 passed, 1 skipped). Exact source-state visual acceptance remains blocked. The current Task 11 read contract cannot reproduce all the reference facts. No demonstration values, inferred actual weights, target amounts, cash totals, or invented cancellation attempt keys were added to make the screenshots look equivalent.

## Comparison targets and evidence

Source visual truth:

- Decisions: `/tmp/codex-remote-attachments/01a09987-af26-7692-95ec-a9e9483a13d2/2A87A2C6-CB40-4EEE-BE53-53B05FFB80F4/1-照片-1.jpg` — 1280 × 910 pixels.
- Paper: `/tmp/codex-remote-attachments/01a09987-af26-7692-95ec-a9e9483a13d2/2A87A2C6-CB40-4EEE-BE53-53B05FFB80F4/2-照片-2.jpg` — 1280 × 1066 pixels.

Browser-rendered implementation (relative to project root):

| Route/state | Viewport, CSS px | Full viewport screenshot | Focused content screenshot, pixels |
| --- | --- | --- | --- |
| Decisions detail, blocked proposal/current V7 | 1280 × 910 | `docs/design/qa/task-12/task12-decisions-1280.png` | `docs/design/qa/task-12/task12-decisions-focused-1280.png`, 963 × 941 |
| Same decision, mobile | 390 × 844 | `docs/design/qa/task-12/task12-decisions-390.png` | `docs/design/qa/task-12/task12-decisions-focused-390.png`, 358 × 1512 |
| Paper, occupied partially filled plan | 1280 × 1066 | `docs/design/qa/task-12/task12-paper-1280.png` | `docs/design/qa/task-12/task12-paper-focused-1280.png`, 963 × 1365 |
| Same plan, mobile | 390 × 844 | `docs/design/qa/task-12/task12-paper-390.png` | `docs/design/qa/task-12/task12-paper-focused-390.png`, 358 × 2281 |

Chromium, existing Playwright webServer, isolated worktree port 4297, dark theme, authenticated fixture routes. Browser deviceScaleFactor is 1; full screenshot pixel dimensions equal CSS viewport. Sources were viewed at their native pixel dimensions with no density resampling. Mobile has no matching source viewport; it is a responsive adaptation check, not a pixel-match claim. Focused screenshots exclude the unchanged app shell and include the full newly implemented section.

Final capture run: 30/30 e2e tests passed in 55.1s. Full and focused source/implementation evidence was reopened after capture correction; fractional CSS bounding boxes are rounded to the recorded clip pixel dimensions by Chromium. Final lint, typecheck and build passed; vinext retains its existing static route-classification notice.

Source and implementation images were opened together in the same tool comparison input for each desktop full-view comparison and for focused content inspection. This was a combined image-input comparison, not a generated side-by-side composite. The focused desktop and mobile screenshots were also opened with the relevant source. Source files are local attachments; implementation evidence is committed in the repository.

## Findings

- [P1, blocked] Source state cannot be faithfully reproduced from the current read contract. The reference includes more symbols, actual allocation/target amount facts, and a discoverable original pending cancellation attempt. Our fixture contains two target instruments, exposes projected plan money/quantity/cash facts only, and a refreshed page cannot discover an old idempotency key. Evidence: focused decisions table and paper original-operation panel. Impact: exact content/state comparison is not valid. Required next action: provide the same persisted source-state fixture and an authorized read contract for the missing facts; this task explicitly does not expand the backend. Until then show unavailable or disabled, not substitute source demonstration values.
- [P2, blocked] Full-view density differs because the current app retains its existing global safety/status shell, while the source uses a much thinner header. The first fold still contains the blocking conclusion and Agent judgment, but less of the deterministic table is visible. Evidence: both 1280 full captures versus their sources; focused captures separate this shell difference from local panel layout. Changing the global shell is outside this slice. Required next action: agree a source capture incorporating the actual shell or separately authorize shell changes; do not compress page content blindly.
- [P2, resolved] Mobile disclosure target was initially too short. `.pm-disclosure summary` now has a 44px minimum height. Post-fix focused decisions screenshots and mobile keyboard/target-size test confirm the control is usable.
- [P2, resolved] An unavailable management summary increased the existing real-portfolio mobile page beyond its height budget. Portfolio now renders this optional internal-simulation summary only when a management fact was read; no compacting/negative-margin CSS workaround remains. The exact 375px and 390px regressions pass. This is an information-state correction, not a source fidelity claim.

## Required fidelity surfaces

- Fonts/typography: retained existing Geist/PingFang and numeric monospace tokens; no font dependency added. Clear heading/body/meta hierarchy. Primary proposal/plan labels and unmapped instruments now use explicit object prefixes plus short IDs; full UUIDs remain in title attributes and evidence content for traceability. Full evidence strings wrap without horizontal clipping. Font family/optical metrics in a raster source cannot be proven exactly; no exact-match claim.
- Spacing/layout rhythm: warning first, Agent judgment before deterministic comparison, timeline and original-operation panel before historical evaluation. Desktop two-column plan region collapses to mobile cards; check panel moves before the timeline. Focused evidence shows no horizontal crop or overlapping controls. Existing sidebar/header remain unchanged; the first-fold density difference remains blocked as above.
- Colors/tokens: existing navy surfaces, subdued borders, action blue, amber uncertainty and mint confirmation retained. Text communicates status independently of color. No new gradients/decorative artwork; existing shell styling remains outside this slice.
- Image quality/assets/icons: references are primarily text/data UI, no photographic assets required. Existing NEXUS identity and lucide icon family reused. No fake avatars, rasterized UI, custom SVG illustrations, or replacement decorative art. Captures are browser-rendered PNGs at density 1.
- Copy/content: target/proposal/plan/order/fill/reservation are distinct. Agent choices are not presented as numeric configuration authority. Missing facts remain unavailable; inherited targets explicitly do not renew research validity. Original-result check explicitly says it never resends an action. Demo source values are not product facts.

## Iteration history

1. Implemented selected hierarchy against typed fixture facts. Full-view comparison identified legacy hero/safety-shell density and source-state differences. Removed the redundant local legacy hero treatment on management decisions, retaining the required global navigation/safety shell; preserved unavailable states instead of adding fake facts.
2. Mobile tests found a short disclosure tap target and the added unavailable portfolio-summary height. Increased only the disclosure hit area; after confirming the unavailable portfolio state, conditioned that optional summary on actual management facts and removed temporary density adjustments. Re-ran failed viewport tests with unchanged assertions.
3. Captured the final decisions and paper views at source desktop sizes and 390px mobile, plus focused sections. Reopened source plus implementation in shared comparison inputs. Required fidelity surfaces above were checked. Source-state and global-shell mismatches remain explicitly blocked; neither build success nor test success closes them.
4. [P2, resolved] Full UUID primary labels made the banner and unknown instruments visually dense. TDD introduced short-label/title assertions (2 expected RED failures), then shortened display text without changing identity values or links; 118 focused unit tests and 30 four-file e2e tests passed. Recaptured desktop/mobile views show `提案 …0008`, `标的 …0009`, `调整计划 …0010`; complete identifiers remain accessible in title/evidence. Focused capture was also corrected from element auto-scroll to a full-page clip, avoiding sticky navigation obscuring the top of the captured component. No layout CSS was changed for capture.

Earlier e2e failures were investigated before rerun: one navigation `ERR_NETWORK_IO_SUSPENDED`, two 30-second timeouts spanning 15.5 minutes. macOS power logs confirmed long Idle Sleep intervals during that run. The original run had no trace configured; exact portfolio timeout step remains unavailable. Each original failure passed independently with trace enabled, all three passed sequentially, and the four-file concurrent run passed while a command-scoped `caffeinate -i` prevented sleep. No retry, timeout extension, assertion relaxation, or system power-setting change was made.

## Interactions and residual gaps

Verified by focused unit/e2e coverage: semantic target comparison, exact plan link, keyboard disclosure, mobile no-overflow and tap targets, unknown/error/permission states, exact-key GET cancellation checks, duplicate-click guard, repeated read after completion, close/reopen of an unknown cancel without a second POST, and unchanged real-account separation. Existing axe/no-console-error checks run in the selected e2e suite; this is not a claim that every fixture state has an individual console audit. vinext test-server renderer warnings are recorded in test logs and do not establish a product failure.

No real account facts, broker requests, model requests, or shared application server were used. Screenshots use normal application rendering with isolated fixture API responses, not database integration or live business acceptance. Browser zoom/text-rescaling beyond tested viewport sizes and exact source-state equivalence remain unverified.

## Implementation checklist / follow-up

- [x] Selected hierarchy, existing tokens/navigation, desktop and mobile evidence.
- [x] Read-only projections; original-attempt GET check stays separate from explicit cancel mutation.
- [x] Correct mobile disclosure hit target and unavailable portfolio summary.
- [ ] Supply a matching source-state fixture/read contract before claiming source fidelity acceptance.
- [ ] Resolve full-view shell/reference density as a separate authorized decision.

P3 polish: once a matching fixture is available, re-evaluate identifier wrapping and local panel spacing against that fixture; do not abbreviate away traceability merely to match demonstration copy.
