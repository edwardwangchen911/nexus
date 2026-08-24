import { engineFetch } from "../../../../lib/engine/client";
import { withNexusBff } from "../../../../lib/engine/with-nexus-bff";
import { readExactJson, requireIdempotencyKey, requireWhatIf } from "../../../../lib/optimization/bff";

export const POST = withNexusBff(async (request, actor) => {
  const body = await readExactJson(request, "The what-if request is invalid.");
  requireWhatIf(body.value);
  return engineFetch("/internal/v1/what-if/evaluate", {
    method: "POST",
    actor,
    body: body.bytes,
    headers: {
      "content-type": "application/json",
      "idempotency-key": requireIdempotencyKey(request),
    },
  });
});
