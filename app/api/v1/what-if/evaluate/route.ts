import { engineFetch } from "../../../../lib/engine/client";
import { NexusBffError } from "../../../../lib/engine/errors";
import { withNexusBff } from "../../../../lib/engine/with-nexus-bff";
import { readExactJson, requireIdempotencyKey, requireWhatIf } from "../../../../lib/optimization/bff";

export const POST = withNexusBff(async (request, actor) => {
  const body = await readExactJson(request, "The what-if request is invalid.");
  requireWhatIf(body.value);
  const response = await engineFetch("/internal/v1/what-if/evaluate", {
    method: "POST",
    actor,
    body: body.bytes,
    headers: {
      "content-type": "application/json",
      "idempotency-key": requireIdempotencyKey(request),
    },
  });
  if (!response.ok) return response;
  const envelope = await response.clone().json() as { data?: { proposal_group_id?: unknown; proposal_id?: unknown } };
  if (envelope.data?.proposal_group_id !== body.value.proposal_group_id
    || envelope.data?.proposal_id !== body.value.proposal_id) {
    throw new NexusBffError(500, "INTERNAL_ERROR", "The request could not be completed.");
  }
  return response;
});
