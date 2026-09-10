import { engineFetch } from "../../../../lib/engine/client";
import { withNexusBff } from "../../../../lib/engine/with-nexus-bff";
import {
  readPortfolioDecisionScope,
  requirePortfolioDecisionResponse,
} from "../../../../lib/portfolio-decisions/bff";

export const GET = withNexusBff(async (request, actor) => {
  const { runId, scope } = readPortfolioDecisionScope(request);
  const upstream = await engineFetch({
    template: "/internal/v1/portfolio-decision-runs/{run_id}",
    path: `/internal/v1/portfolio-decision-runs/${runId}`,
  }, { method: "GET", actor });
  if (upstream.status !== 200) return upstream;
  const envelope: unknown = await upstream.json();
  requirePortfolioDecisionResponse(envelope, scope, runId);
  return Response.json(envelope);
});
