import { engineFetch } from "../../../lib/engine/client";
import { NexusBffError } from "../../../lib/engine/errors";
import { withNexusBff } from "../../../lib/engine/with-nexus-bff";
import {
  decisionIdempotencyKey,
  readPortfolioDecisionCreate,
  requirePortfolioDecisionResponse,
} from "../../../lib/portfolio-decisions/bff";

export const POST = withNexusBff(async (request, actor) => {
  const body = await readPortfolioDecisionCreate(request);
  const upstream = await engineFetch("/internal/v1/portfolio-decision-runs", {
    method: "POST",
    actor,
    body: body.bytes,
    headers: {
      "content-type": "application/json",
      "idempotency-key": decisionIdempotencyKey(request),
    },
  });
  if (upstream.status !== 202) return upstream;
  const envelope: unknown = await upstream.json();
  requirePortfolioDecisionResponse(envelope, {
    portfolioSnapshotId: body.value.portfolio_snapshot_id as string,
    diagnosticReportId: body.value.diagnostic_report_id as string,
    expectedScenarioResultHash: body.value.expected_scenario_result_hash as string,
  });
  const runId = envelope.data.run_id;
  if (typeof runId !== "string") {
    throw new NexusBffError(500, "INTERNAL_ERROR", "The request could not be completed.");
  }
  return Response.json(envelope, {
    status: 202,
    headers: { location: `/api/v1/portfolio-decision-runs/${runId}` },
  });
});
