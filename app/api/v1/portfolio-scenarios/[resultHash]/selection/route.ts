import { engineFetch } from "../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../lib/engine/with-nexus-bff";
import {
  readExactJson,
  requireIdempotencyKey,
  requirePortfolioScenarioSelection,
  requirePortfolioScenarioSelectionResponse,
  scenarioResultHash,
} from "../../../../../lib/optimization/bff";

const target = (resultHash: string) => ({
  template: "/internal/v1/portfolio-scenarios/{scenario_result_hash}/selection" as const,
  path: `/internal/v1/portfolio-scenarios/${resultHash}/selection`,
});

export const PUT = withNexusBff(async (request, actor) => {
  const resultHash = scenarioResultHash(request);
  const body = await readExactJson(request, "The portfolio scenario selection is invalid.");
  const scope = requirePortfolioScenarioSelection(request, body.value);
  const upstream = await engineFetch(target(resultHash), {
    method: "PUT",
    actor,
    body: body.bytes,
    headers: {
      "content-type": "application/json",
      "idempotency-key": requireIdempotencyKey(request),
      "if-match": request.headers.get("if-match")!,
    },
  });
  if (upstream.status !== 200) return upstream;
  const envelope: unknown = await upstream.json();
  requirePortfolioScenarioSelectionResponse(envelope, {
    resultHash,
    portfolioSnapshotId: scope.portfolioSnapshotId,
    diagnosticReportId: scope.diagnosticReportId,
  });
  return Response.json(envelope);
});

export const GET = withNexusBff(async (request, actor) => {
  const resultHash = scenarioResultHash(request);
  const upstream = await engineFetch(target(resultHash), { method: "GET", actor });
  if (upstream.status !== 200) return upstream;
  const envelope: unknown = await upstream.json();
  requirePortfolioScenarioSelectionResponse(envelope, { resultHash });
  return Response.json(envelope);
});
