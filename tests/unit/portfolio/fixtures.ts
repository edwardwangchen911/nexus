import type {
  PortfolioItem,
  PortfolioSnapshot,
} from "../../../app/(protected)/portfolio/page";

export const portfolioId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e701";
export const snapshotId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e702";

export const portfolio: PortfolioItem = {
  portfolio_id: portfolioId,
  name: "Fosun read-only",
  kind: "real_read_only",
  coverage_scope: "broker_account",
  provider: "fosun",
  latest_snapshot_status: "READY",
  latest_snapshot_as_of: "2026-08-23T08:00:00Z",
  original_currencies: ["HKD", "USD"],
  last_sync_run_status: "READY",
};

export const snapshot: PortfolioSnapshot = {
  portfolio_snapshot_id: snapshotId,
  portfolio_id: portfolioId,
  coverage_scope: "broker_account",
  status: "READY",
  display_readiness: "DISPLAY_READY",
  decision_readiness: "DECISION_READY",
  cash_flow_quality: "PASS",
  cash_flow_count: 1,
  execution_locked: true,
  as_of: "2026-08-23T08:00:00Z",
  time_basis: "provider_event",
  cash: [
    { currency: "HKD", cash_balance: "8000.00", available_cash: "7000.00", provider_as_of: "2026-08-23T08:00:00Z" },
    { currency: "USD", cash_balance: "1000.00", available_cash: "900.00", provider_as_of: "2026-08-23T08:00:00Z" },
  ],
  positions: [{
    instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e703",
    symbol: "00656.HK",
    exchange: "XHKG",
    currency: "HKD",
    quantity: "3000",
    available_quantity: "2800",
    frozen_quantity: "200",
    avg_cost: "6.20",
    diluted_cost: "5.75",
    current_price: "6.50",
    provider_as_of: "2026-08-23T08:00:00Z",
    read_only: true,
  }],
  open_orders: [{
    broker_order_snapshot_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e704",
    instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e703",
    symbol: "00656.HK",
    currency: "HKD",
    side: "BUY",
    quantity: "100",
    filled_quantity: "0",
    price: "6.10",
    status: "SUBMITTED",
    updated_at: "2026-08-23T07:59:00Z",
    read_only: true,
  }],
  converted_view: null,
};

export function responseMeta() {
  return { request_id: "0198f7a0-1111-7777-8111-111111111114", schema_version: "1", as_of: "2026-08-23T08:00:00Z" };
}
