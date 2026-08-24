import { engineFetch } from "../../../../lib/engine/client";
import { withNexusBff } from "../../../../lib/engine/with-nexus-bff";
import { proposalGroupId } from "../../../../lib/optimization/bff";

export const GET = withNexusBff(async (request, actor) => {
  const groupId = proposalGroupId(request);
  return engineFetch({
    template: "/internal/v1/optimization-proposals/{group_id}",
    path: `/internal/v1/optimization-proposals/${groupId}`,
  }, { method: "GET", actor });
});
