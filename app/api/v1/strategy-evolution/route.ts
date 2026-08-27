import { engineFetch } from "../../../lib/engine/client";
import { withNexusBff } from "../../../lib/engine/with-nexus-bff";
import { canonicalEvolutionQuery } from "../../../lib/strategy-evolution/bff";

export const GET = withNexusBff(async (request, actor) => {
  const query = canonicalEvolutionQuery(new URL(request.url).searchParams);
  return engineFetch({
    template: "/internal/v1/strategy-evolution",
    path: "/internal/v1/strategy-evolution",
    query,
  }, { method: "GET", actor, headers: { accept: "application/json" } });
});
