import { describe, expect, it } from "vitest";
import { sanitizeEngineResponse } from "../../../app/lib/engine/client";
const id = "0198d500-1000-7000-8000-000000001399";
const data = {instrument_id:id,status:"unavailable",symbol:null,received_at:null,window_start:null,window_end:null,
  content_hash:null,has_more:false,articles:[],source:"alpaca",delivery:"delayed_query_latency_unverified",
  coverage:"single_source_bounded",execution_locked:true};
const meta = {request_id:id,schema_version:"1",as_of:"2026-09-06T00:00:00Z"};
async function read(changes:object) {
  return sanitizeEngineResponse(Response.json({data:{...data,...changes},meta}), undefined, `/internal/v1/instruments/${id}/news`);
}
describe("saved news BFF boundary", () => {
  it("accepts an absent saved collection without inventing news", async () => {
    const response = await read({});
    expect(response.status).toBe(200);
    expect((await response.json()).data).toEqual(data);
  });
  it.each([{instrument_id:"0198d500-1000-7000-8000-000000001400"},{delivery:"real_time"},{execution_locked:false},{source:"unknown"}])("rejects changed identity or authority %j", async (change) => {
    expect((await read(change)).status).toBe(500);
  });
});
