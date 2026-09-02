import { engineFetch } from "../../../lib/engine/client";
import { withNexusBff } from "../../../lib/engine/with-nexus-bff";
import { readExactJson, requireIdempotencyKey, requireProposalDraft } from "../../../lib/optimization/bff";

export const POST = withNexusBff(async (request, actor) => {
  const body = await readExactJson(request, "The draft request is invalid.");
  requireProposalDraft(body.value);
  return engineFetch("/internal/v1/optimization-proposal-drafts", {
    method: "POST", actor, body: body.bytes,
    headers: {
      "content-type": "application/json",
      "idempotency-key": requireIdempotencyKey(request),
    },
  });
});
