import { expect, it } from "vitest";
import { rebuildAllowedEngineResponse } from "../../../app/lib/engine/errors";
import { researchReceipt } from "./research-fixture";
const id = "0198f7a0-1111-7777-8111-111111111111";
const meta = { request_id: id, schema_version: "1", as_of: "2026-10-09T14:00:00Z" };
const data = { request_id: id, mode: "research_only", requested_at: meta.as_of, status: "queued", code: "MANUAL_REQUEST_QUEUED", cycle_id: null, execution_locked: true };
const path = "http://engine.local/internal/v1/paper/manual-requests";
it("permits only exact receipts and readonly null lookup", () => {
  expect(rebuildAllowedEngineResponse(200, { data, meta }, undefined, path, undefined, "POST")).not.toBeNull();
  expect(rebuildAllowedEngineResponse(200, { data: null, meta }, undefined, path, undefined, "GET")).not.toBeNull();
  expect(rebuildAllowedEngineResponse(200, { data: null, meta }, undefined, path, undefined, "POST")).toBeNull();
  for (const invalid of [{ ...data, execution_locked: false }, { ...data, code: "MANUAL_PAPER_COMPLETED" }, { ...data, account: "private" }, { ...data, status: "completed" }]) {
    expect(rebuildAllowedEngineResponse(200, { data: invalid, meta }, undefined, path, undefined, "GET")).toBeNull();
  }
});
it("discards raw error details and preserves only manual fixed reason codes", async () => {
  const value = { error: { code: "DATA_PARTIAL", request_id: id, message: "private raw response", details: { reason_code: "MANUAL_HOST_NOT_READY", account: "private" } } };
  const safe = rebuildAllowedEngineResponse(409, value, undefined, path)!;
  expect((await safe.json()).error).toMatchObject({ details: { reason_code: "MANUAL_HOST_NOT_READY" } });
  expect(await rebuildAllowedEngineResponse(409, value, undefined, "http://engine.local/internal/v1/workbench")!.json()).not.toEqual(value);
  const unknown = rebuildAllowedEngineResponse(409, { error: { ...value.error, details: { reason_code: "PRIVATE_NEW_CODE" } } }, undefined, path)!;
  expect((await unknown.json()).error.details).toEqual({});
});

it("carries only the exact unknown preparation acknowledgement through the BFF", () => {
  const acknowledged = { ...data, status: "unknown", code: "MANUAL_RESULT_UNKNOWN", preparation_disposition: "unknown_acknowledged" };
  expect(rebuildAllowedEngineResponse(200, { data: acknowledged, meta }, undefined, path, undefined, "GET")).not.toBeNull();
  for (const invalid of [{ ...data, preparation_disposition: "unknown_acknowledged" }, { ...acknowledged, cycle_id: id }, { ...acknowledged, preparation_disposition: "all_unknowns_retriable" }, { ...acknowledged, preparation_disposition: true }]) {
    expect(rebuildAllowedEngineResponse(200, { data: invalid, meta }, undefined, path, undefined, "GET")).toBeNull();
  }
});

it("reads a bound pure research batch through the real BFF without a Paper cycle", async () => {
  const response = rebuildAllowedEngineResponse(200, { data: researchReceipt, meta }, undefined, path, undefined, "GET");
  expect(response).not.toBeNull();
  expect((await response!.json()).data).toEqual(researchReceipt);
  for (const state of [
    { status: "active", code: "MANUAL_RESEARCH_STARTED" },
    { status: "unknown", code: "MANUAL_RESULT_UNKNOWN" },
    { status: "unavailable", code: "MANUAL_INPUTS_UNAVAILABLE" },
  ]) {
    expect(rebuildAllowedEngineResponse(200, { data: { ...researchReceipt, ...state }, meta }, undefined, path, undefined, "GET")).not.toBeNull();
  }
});

it("allows unbound queued, preparing and failed pure research requests", () => {
  for (const state of [
    { status: "queued", code: "MANUAL_REQUEST_QUEUED" },
    { status: "preparing", code: "MANUAL_PREPARATION_STARTED" },
    { status: "unknown", code: "MANUAL_RESULT_UNKNOWN" },
    { status: "unavailable", code: "MANUAL_INPUTS_UNAVAILABLE" },
  ]) {
    const unbound = { ...researchReceipt, research: undefined };
    expect(rebuildAllowedEngineResponse(200, { data: { ...unbound, ...state }, meta }, undefined, path, undefined, "GET")).not.toBeNull();
  }
});

it("rejects mixed contracts, fake cycles and malformed or leaking research bindings", () => {
  const research = researchReceipt.research;
  const member = research.members[0];
  for (const invalid of [
    { ...researchReceipt, research_contract: undefined },
    { ...researchReceipt, research_contract: "research_only_v3" },
    { ...researchReceipt, mode: "research_and_paper" },
    { ...researchReceipt, cycle_id: id },
    { ...researchReceipt, research: undefined },
    { ...researchReceipt, status: "active", code: "MANUAL_CYCLE_CREATED" },
    { ...researchReceipt, code: "MANUAL_RESEARCH_COMPLETED" },
    { ...researchReceipt, status: "queued", code: "MANUAL_REQUEST_QUEUED" },
    { ...researchReceipt, preparation_recovery: { disposition: "cycle_reconciled", external_effects: "cycle_recorded", cycle_id: id } },
    { ...researchReceipt, preparation_recovery: false },
    { ...researchReceipt, preparation_disposition: "" },
    ...[
      { remote_latest_verified: true }, { execution_locked: false }, { manifest_id: "bad" },
      { manifest_hash: "bad" }, { research_as_of: "2026-10-22" }, { members: [] }, { api_key: "private" },
      { members: [{ ...member, run_id: "bad" }] }, { members: [{ ...member, usage_known: "true" }] },
      { members: [{ ...member, account: "private" }] }, { members: [member, member] },
    ].map((change) => ({ ...researchReceipt, research: { ...research, ...change } })),
  ]) {
    expect(rebuildAllowedEngineResponse(200, { data: invalid, meta }, undefined, path, undefined, "GET")).toBeNull();
  }
});
