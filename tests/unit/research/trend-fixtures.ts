// Synthetic source/indicator values; no live provider, model or account data.
import type { components } from "../../../app/lib/generated/schema";
import { fundObservationInputs } from "./fixtures";

export function priceTrend(): components["schemas"]["ResearchInputPriceTrend"] {
  return {
    evidence_ref_id: "0198d600-1000-7000-8000-000000001595",
    source: "alpaca_current_trends", revision: "a".repeat(64), content_hash: "a".repeat(64),
    feed: "sip", adjustment: "split", currency: "USD", timeframe: "1Day",
    identity_asof: "2026-08-24", query_start: "2026-01-02", query_end: "2026-08-21",
    first_observed_on: "2026-01-02", last_observed_on: "2026-08-21",
    observed_sessions: 160, expected_sessions: 161, contiguous_tail_sessions: 21,
    missing_sessions: ["2026-07-01"], missing_sessions_total: 1, missing_sessions_truncated: false,
    received_at: "2026-08-24T17:00:00Z", available_at: "2026-08-24T18:00:00Z",
    features: { close: "123.456789", sma_20: "120.000000000001", sma_200: null, rsi_14: "0", volatility_20: "0.000000000001" },
    historical_backtest_eligible: false, market_price_eligible: false,
    total_return_eligible: false, execution_locked: true,
  };
}

export function trendInputs(): components["schemas"]["ResearchInputsSummary"] {
  return { ...fundObservationInputs(), issuer_fund_observations: [], current_price_trends: [priceTrend()] };
}
