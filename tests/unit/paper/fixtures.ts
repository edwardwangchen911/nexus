import type { components } from "../../../app/lib/generated/schema";

export type PaperPortfolio = components["schemas"]["PaperPortfolioResponse"];
export type PaperOrder = components["schemas"]["PaperOrderResponse"];

export const portfolioId = "0198f7a0-1111-7777-8111-111111111121";
export const orderId = "0198f7a0-1111-7777-8111-111111111122";
export const instrumentId = "0198f7a0-1111-7777-8111-111111111123";

export const paperPortfolio: PaperPortfolio = {
  portfolio_id: portfolioId,
  name: "NEXUS Internal Paper",
  mode: "internal_paper",
  currency: "USD",
  initial_capital: "10000.000000000000",
  data_state: "READY",
  performance: {
    as_of: "2026-08-26T20:00:00Z",
    cash: "9000.000000000000",
    available_cash: "8500.000000000000",
    reserved_cash: "500.000000000000",
    market_value: "1000.000000000000",
    nav: "10000.000000000000",
    peak_nav: "10100.000000000000",
    realized_pnl: "25.000000000000",
    unrealized_pnl: "50.000000000000",
    fees: "2.500000000000",
    drawdown_ratio: "0.009900990099",
  },
  positions: [{
    instrument_id: instrumentId,
    symbol: "AAPL",
    quantity: "5.000000000000",
    reserved_quantity: "1.000000000000",
    available_quantity: "4.000000000000",
    cost_basis: "950.000000000000",
    average_cost: "190.000000000000",
    reference_close: "200.000000000000",
    market_value: "1000.000000000000",
    unrealized_pnl: "50.000000000000",
    price_source: "alpaca",
    price_feed: "iex",
    delivery_mode: "delayed",
    price_as_of: "2026-08-26T20:00:00Z",
  }],
  attributions: [{
    dimension: "instrument",
    dimension_key: instrumentId,
    market_value: "1000.000000000000",
    realized_pnl: "25.000000000000",
    unrealized_pnl: "50.000000000000",
    fees: "2.500000000000",
    slippage: "0.420000000000",
    completeness: "FULL",
  }, {
    dimension: "sector",
    dimension_key: "information_technology",
    market_value: "1000.000000000000",
    realized_pnl: "25.000000000000",
    unrealized_pnl: "50.000000000000",
    fees: "2.500000000000",
    slippage: "0.420000000000",
    completeness: "FULL",
  }],
};

export const cancellableOrder: PaperOrder = {
  order_id: orderId,
  instrument_id: instrumentId,
  symbol: "AAPL",
  side: "buy",
  quantity: "5.000000000000",
  filled_quantity: "2.000000000000",
  remaining_quantity: "3.000000000000",
  worst_price: "205.000000000000",
  status: "partially_filled",
  cancellable: true,
  current_event_seq: 4,
  submitted_at: "2026-08-26T19:30:00Z",
  strategy_version: "baseline_v1",
  risk_policy_version: "paper_risk_v1",
  events: [{
    event_seq: 1,
    from_status: null,
    to_status: "proposed",
    actor_kind: "task18",
    reason_code: "paper_order_proposed",
    occurred_at: "2026-08-26T19:27:00Z",
  }, {
    event_seq: 2,
    from_status: "proposed",
    to_status: "risk_approved",
    actor_kind: "task18",
    reason_code: "paper_risk_approved",
    occurred_at: "2026-08-26T19:28:00Z",
  }, {
    event_seq: 3,
    from_status: "risk_approved",
    to_status: "submitted",
    actor_kind: "task19",
    reason_code: "paper_order_submitted",
    occurred_at: "2026-08-26T19:30:00Z",
  }, {
    event_seq: 4,
    from_status: "submitted",
    to_status: "partially_filled",
    actor_kind: "task19",
    reason_code: "paper_partial_fill",
    occurred_at: "2026-08-26T20:00:00Z",
  }],
  fills: [{
    fill_id: "0198f7a0-1111-7777-8111-111111111124",
    fill_seq: 1,
    quantity: "2.000000000000",
    price: "200.000000000000",
    notional: "400.000000000000",
    fee: "1.000000000000",
    occurred_at: "2026-08-26T20:00:00Z",
    source: "alpaca",
    feed: "iex",
    delivery_mode: "delayed",
    source_as_of: "2026-08-26T19:59:00Z",
    execution_model_hash: "a".repeat(64),
    slippage_bps: 10,
    slippage: "0.400000000000",
  }],
};

export const filledOrder: PaperOrder = {
  ...cancellableOrder,
  order_id: "0198f7a0-1111-7777-8111-111111111125",
  status: "filled",
  cancellable: false,
  filled_quantity: "5.000000000000",
  remaining_quantity: "0.000000000000",
  current_event_seq: 5,
  events: [...cancellableOrder.events, {
    event_seq: 5,
    from_status: "partially_filled",
    to_status: "filled",
    actor_kind: "task19",
    reason_code: "paper_order_filled",
    occurred_at: "2026-08-26T20:01:00Z",
  }],
  fills: [...cancellableOrder.fills, {
    ...cancellableOrder.fills[0],
    fill_id: "0198f7a0-1111-7777-8111-111111111126",
    fill_seq: 2,
    quantity: "3.000000000000",
    notional: "600.000000000000",
    occurred_at: "2026-08-26T20:01:00Z",
  }],
};

export function meta() {
  return { request_id: "0198f7a0-1111-7777-8111-111111111127", schema_version: "1", as_of: "2026-08-26T20:00:01Z" };
}
