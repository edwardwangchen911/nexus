import { validate as validateUuid } from "uuid";
import { engineFetch } from "../../../../../../lib/engine/client";
import { NexusBffError } from "../../../../../../lib/engine/errors";
import { withNexusBff } from "../../../../../../lib/engine/with-nexus-bff";

const PATH = /^\/api\/v1\/portfolios\/([^/]+)\/snapshots\/latest$/;

export const GET = withNexusBff(async (request, actor) => {
  const portfolioId = PATH.exec(new URL(request.url).pathname)?.[1];
  if (!portfolioId || !validateUuid(portfolioId)) {
    throw new NexusBffError(400, "VALIDATION_ERROR", "The portfolio is invalid.");
  }
  return engineFetch({
    template: "/internal/v1/portfolios/{portfolio_id}/snapshots/latest",
    path: `/internal/v1/portfolios/${portfolioId}/snapshots/latest`,
  }, { method: "GET", actor });
});
