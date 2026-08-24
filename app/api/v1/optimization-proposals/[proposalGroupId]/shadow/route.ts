import { engineFetch } from "../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../lib/engine/with-nexus-bff";
import { proposalGroupId, readExactJson, requireIdempotencyKey, requireLifecycleVersion } from "../../../../../lib/optimization/bff";

export const POST = withNexusBff(async (request, actor) => {
  const groupId = proposalGroupId(request, "shadow");
  const body = await readExactJson(request, "The shadow request is invalid.");
  requireLifecycleVersion(request, body.value);
  return engineFetch({
    template: "/internal/v1/optimization-proposals/{group_id}/shadow",
    path: `/internal/v1/optimization-proposals/${groupId}/shadow`,
  }, {
    method: "POST",
    actor,
    body: body.bytes,
    headers: {
      "content-type": "application/json",
      "idempotency-key": requireIdempotencyKey(request),
      "if-match": request.headers.get("if-match")!,
    },
  });
}, { requireVersion: true });
