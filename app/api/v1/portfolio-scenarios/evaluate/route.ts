import { engineFetch } from "../../../../lib/engine/client";
import { NexusBffError } from "../../../../lib/engine/errors";
import { withNexusBff } from "../../../../lib/engine/with-nexus-bff";
import {
  readExactJson,
  requireIdempotencyKey,
  requirePortfolioScenarioRequest,
} from "../../../../lib/optimization/bff";
import { sameUuid } from "../../../../lib/optimization/uuid";

export const POST = withNexusBff(async (request, actor) => {
  const body = await readExactJson(request, "The portfolio scenario request is invalid.");
  requirePortfolioScenarioRequest(body.value);
  const response = await engineFetch("/internal/v1/portfolio-scenarios/evaluate", {
    method: "POST",
    actor,
    body: body.bytes,
    headers: {
      "content-type": "application/json",
      "idempotency-key": requireIdempotencyKey(request),
    },
  });
  if (!response.ok) return response;
  const envelope = await response.clone().json() as {
    data?: {
      portfolio_snapshot_id?: unknown;
      diagnostic_report_id?: unknown;
      valuation_time?: unknown;
      execution_locked?: unknown;
    };
  };
  if (!sameUuid(envelope.data?.portfolio_snapshot_id, body.value.portfolio_snapshot_id)
    || !sameUuid(envelope.data?.diagnostic_report_id, body.value.diagnostic_report_id)
    || !sameInstant(envelope.data?.valuation_time, body.value.as_of)
    || envelope.data?.execution_locked !== true) {
    throw new NexusBffError(500, "INTERNAL_ERROR", "The request could not be completed.");
  }
  return response;
});

function sameInstant(left: unknown, right: unknown): boolean {
  if (typeof left !== "string" || typeof right !== "string") return false;
  const leftTime = Date.parse(left);
  const rightTime = Date.parse(right);
  return Number.isFinite(leftTime) && leftTime === rightTime;
}
