import { engineFetch } from "../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../lib/engine/with-nexus-bff";
import { canonicalResearchAsOf, routeUuid } from "../../../../../lib/research/bff";

const PATH = /^\/api\/v1\/instruments\/([^/]+)\/research$/;

export const GET = withNexusBff(async (request, actor) => {
  const instrumentId = routeUuid(request, PATH, "The research instrument is invalid.");
  const query = canonicalResearchAsOf(request);
  return engineFetch({
    template: "/internal/v1/instruments/{instrument_id}/research",
    path: `/internal/v1/instruments/${instrumentId}/research`,
    query,
  }, { method: "GET", actor });
});
