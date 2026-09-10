import { engineFetch } from "../../../../lib/engine/client";
import { withNexusBff } from "../../../../lib/engine/with-nexus-bff";
import {
  readLatestPortfolioDecisionScope,
  requirePortfolioDecisionResponse,
} from "../../../../lib/portfolio-decisions/bff";

export const GET = withNexusBff(async (request, actor) => {
  const scope = readLatestPortfolioDecisionScope(request);
  const query = new URLSearchParams({
    portfolio_snapshot_id: scope.portfolioSnapshotId,
    diagnostic_report_id: scope.diagnosticReportId,
    expected_scenario_result_hash: scope.expectedScenarioResultHash,
  }).toString();
  const upstream = await engineFetch({
    template: "/internal/v1/portfolio-decision-runs/latest",
    path: "/internal/v1/portfolio-decision-runs/latest",
    query,
  }, { method: "GET", actor });
  if (upstream.status !== 200) return upstream;
  const envelope: unknown = await upstream.json();
  requirePortfolioDecisionResponse(envelope, scope);
  return Response.json(envelope);
});
