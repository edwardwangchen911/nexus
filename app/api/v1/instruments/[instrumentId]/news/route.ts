import { engineFetch } from "../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../lib/engine/with-nexus-bff";
import { NexusBffError } from "../../../../../lib/engine/errors";
import { routeUuid } from "../../../../../lib/research/bff";

export const GET = withNexusBff(async (request, actor) => {
  const instrumentId = routeUuid(request, /^\/api\/v1\/instruments\/([^/]+)\/news$/, "The news instrument is invalid.");
  if (new URL(request.url).search) throw new NexusBffError(400, "VALIDATION_ERROR", "The news query is invalid.");
  return engineFetch({
    template: "/internal/v1/instruments/{instrument_id}/news",
    path: `/internal/v1/instruments/${instrumentId}/news`,
  }, { method: "GET", actor });
});
