import { engineFetch } from "../../../../lib/engine/client";
import { withNexusBff } from "../../../../lib/engine/with-nexus-bff";
import { canonicalPaperCycleQuery } from "../../../../lib/paper/bff";

export const GET = withNexusBff(async (request, actor) => {
  const query = canonicalPaperCycleQuery(new URL(request.url).searchParams);
  return engineFetch({
    template: "/internal/v1/paper/cycles",
    path: "/internal/v1/paper/cycles",
    query,
  }, { method: "GET", actor });
});
