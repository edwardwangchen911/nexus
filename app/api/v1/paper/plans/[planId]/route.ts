import { engineFetch } from "../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../lib/engine/with-nexus-bff";
import { paperPlanId, rejectQuery } from "../../../../../lib/paper/bff";

export const GET = withNexusBff(async (request, actor) => {
  rejectQuery(request, "The paper plan query is invalid.");
  const planId = paperPlanId(request);
  return engineFetch({
    template: "/internal/v1/paper/plans/{batch_id}",
    path: `/internal/v1/paper/plans/${planId}`,
  }, { method: "GET", actor });
});
