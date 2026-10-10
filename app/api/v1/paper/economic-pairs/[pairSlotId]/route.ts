import { engineFetch } from "../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../lib/engine/with-nexus-bff";
import { economicPairId, rejectQuery } from "../../../../../lib/paper/bff";

export const GET = withNexusBff(async (request, actor) => {
  rejectQuery(request, "The economic pair query is invalid.");
  const pairId = economicPairId(request);
  return engineFetch({
    template: "/internal/v1/paper/economic-pairs/{pair_slot_id}",
    path: `/internal/v1/paper/economic-pairs/${pairId}`,
  }, { method: "GET", actor });
});
