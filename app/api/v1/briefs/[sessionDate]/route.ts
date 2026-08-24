import { engineFetch } from "../../../../lib/engine/client";
import { withNexusBff } from "../../../../lib/engine/with-nexus-bff";
import { routeSessionDate } from "../../../../lib/research/bff";

const PATH = /^\/api\/v1\/briefs\/([^/]+)$/;

export const GET = withNexusBff(async (request, actor) => {
  const sessionDate = routeSessionDate(request, PATH);
  return engineFetch({
    template: "/internal/v1/briefs/{session_date}",
    path: `/internal/v1/briefs/${sessionDate}`,
  }, { method: "GET", actor });
});
