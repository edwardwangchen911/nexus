import { describe, expect, it } from "vitest";

import { sanitizeEngineResponse } from "../../../app/lib/engine/client";
import { groupId, overviewData, proposalGroup } from "./fixtures";

const meta = { request_id: "0198d500-1000-7000-8000-000000001399", schema_version: "1", as_of: "2026-08-24T00:00:00Z" };

describe("optimization Engine response boundary", () => {
  it("accepts only the closed generated three-proposal response", async () => {
    const payload = { data: proposalGroup(), meta };
    const accepted = await sanitizeEngineResponse(json(payload), undefined, `/internal/v1/optimization-proposals/${groupId}`);
    expect(accepted.status).toBe(200);
    expect(await accepted.json()).toEqual(payload);

    const nestedSecret = proposalGroup();
    Object.assign(nestedSecret.proposals[0], { credential: "must-not-escape" });
    expect((await sanitizeEngineResponse(json({ data: nestedSecret, meta }), undefined, `/internal/v1/optimization-proposals/${groupId}`)).status).toBe(500);
  });

  it("rejects duplicate or reordered variants even when every field is schema-valid", async () => {
    const reordered = proposalGroup();
    reordered.proposals = [reordered.proposals[1], reordered.proposals[0], reordered.proposals[2]];
    expect((await sanitizeEngineResponse(json({ data: reordered, meta }), undefined, `/internal/v1/optimization-proposals/${groupId}`)).status).toBe(500);

    const duplicate = proposalGroup();
    duplicate.proposals = [duplicate.proposals[0], { ...duplicate.proposals[1], proposal_id: duplicate.proposals[0].proposal_id }, duplicate.proposals[2]];
    expect((await sanitizeEngineResponse(json({ data: duplicate, meta }), undefined, `/internal/v1/optimization-proposals/${groupId}`)).status).toBe(500);
  });

  it("binds a proposal response to the requested group identity", async () => {
    const mismatched = proposalGroup({ proposal_group_id: "0198d500-1000-7000-8000-000000001397" });
    expect((await sanitizeEngineResponse(json({ data: mismatched, meta }), undefined, `/internal/v1/optimization-proposals/${groupId}`)).status).toBe(500);

    const uppercasePath = `/internal/v1/optimization-proposals/${groupId.toUpperCase()}`;
    expect((await sanitizeEngineResponse(json({ data: proposalGroup(), meta }), undefined, uppercasePath)).status).toBe(200);
  });

  it("binds overview admission and diagnosis to the returned snapshot", async () => {
    const payload = { data: overviewData(), meta };
    expect((await sanitizeEngineResponse(json(payload), undefined, "/internal/v1/overview")).status).toBe(200);

    const mismatch = overviewData({ portfolio_snapshot_id: "0198d500-1000-7000-8000-000000001397" });
    expect((await sanitizeEngineResponse(json({ data: mismatch, meta }), undefined, "/internal/v1/overview")).status).toBe(500);
  });
});

function json(value: unknown): Response {
  return new Response(JSON.stringify(value), { status: 200, headers: { "content-type": "application/json" } });
}
