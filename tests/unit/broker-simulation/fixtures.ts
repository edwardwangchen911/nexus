import type { components } from "../../../app/lib/generated/schema";

export type BrokerSimulation = components["schemas"]["BrokerSimulationResponse"];

export const proposalGroupId = "0198f7a0-1111-7777-8111-111111111201";
export const proposalId = "0198f7a0-1111-7777-8111-111111111202";
export const intentId = "0198f7a0-1111-7777-8111-111111111203";
export const orderId = "0198f7a0-1111-7777-8111-111111111204";
export const instrumentId = "0198f7a0-1111-7777-8111-111111111205";
export const bindingId = "0198f7a0-1111-7777-8111-111111111206";
export const runId = "0198f7a0-1111-7777-8111-111111111207";
export const bodyHash = "a".repeat(64);

export const readyWorkbench: BrokerSimulation = {
  capability: {
    status: "offline_ready",
    external_activation: "external_activation_blocked",
    live_trading: false,
    simulation_write_enabled: false,
  },
  account: {
    binding_id: bindingId,
    redacted_label: "FOSUN SIM ··· 8421",
    reconciliation_status: "matched",
    locked: false,
    capability_expires_at: "2026-08-29T20:00:00Z",
  },
  intent: {
    intent_id: intentId,
    status: "prepared",
    version: 1,
    body_hash: bodyHash,
    total_notional: "1200.000000000000",
    prepared_at: "2026-08-28T13:00:00Z",
    expires_at: "2026-08-28T20:00:00Z",
    evidence_hashes: ["b".repeat(64), "c".repeat(64)],
    orders: [{
      order_id: orderId,
      instrument_id: instrumentId,
      side: "buy",
      quantity: "6.000000000000",
      limit_price: "200.000000000000",
      currency: "USD",
      order_type: "limit",
      time_in_force: "DAY",
      session: "regular",
      status: "not_sent",
    }],
  },
  events: [{
    sequence: 1,
    order_id: orderId,
    source: "gateway",
    status: "not_sent",
    occurred_at: "2026-08-28T13:00:01Z",
  }],
  reconciliation: {
    run_id: runId,
    status: "matched",
    diff_count: 0,
    started_at: "2026-08-28T12:59:00Z",
    completed_at: "2026-08-28T13:00:00Z",
  },
  actions: {
    can_prepare: true,
    can_approve: true,
    can_reject: true,
    can_dispatch: false,
    can_modify: false,
    can_cancel: false,
  },
};

export function envelope(data: BrokerSimulation = readyWorkbench) {
  return {
    data,
    meta: {
      request_id: "0198f7a0-1111-7777-8111-111111111208",
      schema_version: "1",
      as_of: "2026-08-28T13:00:02Z",
    },
  };
}

