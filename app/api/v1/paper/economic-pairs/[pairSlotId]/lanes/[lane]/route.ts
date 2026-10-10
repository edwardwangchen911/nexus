import { engineFetch } from "../../../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../../../lib/engine/with-nexus-bff";
import { economicLaneRef, rejectQuery } from "../../../../../../../lib/paper/bff";

export const GET = withNexusBff(async (request, actor) => {
  rejectQuery(request, "The economic lane query is invalid.");
  const { pairId, lane } = economicLaneRef(request);
  return engineFetch({ template: "/internal/v1/paper/economic-pairs/{pair_slot_id}/lanes/{lane}",
    path: `/internal/v1/paper/economic-pairs/${pairId}/lanes/${lane}` }, { method: "GET", actor });
});
