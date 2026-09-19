import { engineFetch } from "../../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../../lib/engine/with-nexus-bff";
import { paperManagementCycleId, rejectQuery } from "../../../../../../lib/paper/bff";

export const GET = withNexusBff(async (request, actor) => {
  rejectQuery(request, "The Agent paper management cycle query is invalid.");
  const cycleId = paperManagementCycleId(request);
  return engineFetch({
    template: "/internal/v1/paper/management/cycles/{cycle_id}",
    path: `/internal/v1/paper/management/cycles/${cycleId}`,
  }, { method: "GET", actor });
});
