import { engineFetch } from "../../../../lib/engine/client";
import { withNexusBff } from "../../../../lib/engine/with-nexus-bff";
import { routeUuid } from "../../../../lib/research/bff";

const PATH = /^\/api\/v1\/research-runs\/([^/]+)$/;

export const GET = withNexusBff(async (request, actor) => {
  const runId = routeUuid(request, PATH, "The research run is invalid.");
  return engineFetch({
    template: "/internal/v1/research-runs/{run_id}",
    path: `/internal/v1/research-runs/${runId}`,
  }, { method: "GET", actor });
});
