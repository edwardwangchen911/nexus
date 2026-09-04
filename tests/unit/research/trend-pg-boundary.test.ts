/** Invoked by the isolated PG test with its real synthetic-source API responses. */
import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { sanitizeEngineResponse } from "../../../app/lib/engine/client";

const capture = process.env.NEXUS_TREND_API_CAPTURE;

// The Python integration test requires this test to run and pass (not skip).
it.skipIf(!capture)("preserves actual frozen PG admission/new run and keeps old run unfilled", async () => {
  const responses = JSON.parse(readFileSync(capture!, "utf8")) as Array<{ path: string; body: object }>;
  expect(responses).toHaveLength(3);
  for (const [index, item] of responses.entries()) {
    const url = new URL(item.path, "http://test");
    const result = await sanitizeEngineResponse(Response.json(item.body), url.toString(), url.pathname, "GET");
    expect(result.status).toBe(200);
    const body = await result.json();
    expect(body).toEqual(item.body);
    expect(body.data.inputs.filings).toHaveLength(1);
    expect(body.data.inputs.current_price_trends).toHaveLength(index === 2 ? 0 : 1);
    if (index < 2) {
      const trend = body.data.inputs.current_price_trends[0];
      expect(trend.features.sma_200).toBe("100.500000000000");
      expect(trend.execution_locked).toBe(true);
      expect(trend.market_price_eligible).toBe(false);
      expect(trend.historical_backtest_eligible).toBe(false);
      expect(trend.total_return_eligible).toBe(false);
    }
  }
});
