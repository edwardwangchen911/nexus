import { engineFetch } from "../../../../lib/engine/client";
import { withNexusBff } from "../../../../lib/engine/with-nexus-bff";
import { canonicalPaperOrderQuery } from "../../../../lib/paper/bff";

export const GET = withNexusBff(async (request, actor) => {
  const query = canonicalPaperOrderQuery(new URL(request.url).searchParams);
  return engineFetch({
    template: "/internal/v1/paper/orders",
    path: "/internal/v1/paper/orders",
    query,
  }, { method: "GET", actor });
});
