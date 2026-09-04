import type { components } from "../../../app/lib/generated/schema";

type Observation = components["schemas"]["ResearchInputActionObservation"];
type Event = components["schemas"]["ResearchInputActionEvent"];

export function actionEvent(overrides: Partial<Event> = {}): Event {
  return {
    category: "future_action_category", provider_event_id: "synthetic-event-" + "x".repeat(120),
    page_number: 1, index: 0, symbol: "TEST", old_symbol: null, new_symbol: "NEW",
    process_date: "2026-08-20", ex_date: null, record_date: "2026-08-21",
    payable_date: null, declaration_date: "2026-08-19", rate: "0", old_rate: "0.0000001",
    new_rate: null, currency: "", unavailable_numeric_fields: ["new_rate"],
    ...overrides,
  };
}

export function actionObservation(overrides: Partial<Observation> = {}): Observation {
  return {
    evidence_ref_id: "0198d600-1000-7000-8000-000000001583", source: "alpaca_action_observation",
    revision: "a".repeat(64), content_hash: "a".repeat(64), symbol: "TEST",
    query_start: "2026-08-01", query_end: "2026-08-23", available_at: "2026-08-24T18:30:00Z",
    manifest_content_hash: "b".repeat(64), page_count: 2,
    total_event_count: 3, bound_event_count: 1, unbound_event_count: 2,
    truncated: false, events: [actionEvent()], pagination_exhausted: true,
    source_completeness: "unknown", revision_semantics: "local_content_hash",
    historical_backtest_eligible: false, ledger_eligible: false, execution_locked: true,
    available_at_semantics: "maximum_page_receipt_not_historical_availability", ...overrides,
  };
}

export function actionInputs(): components["schemas"]["ResearchInputsSummary"] {
  return {
    display_symbol: "TEST", filings: [], facts: [], total_facts: 0, truncated: false,
    issuer_distributions: [], issuer_fund_observations: [], issuer_nav_histories: [],
    corporate_action_observations: [actionObservation()],
  };
}
