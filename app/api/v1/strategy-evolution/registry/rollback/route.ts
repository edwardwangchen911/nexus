import { engineFetch } from "../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../lib/engine/with-nexus-bff";
import { readRollback, requireEvolutionWriteHeaders, rewriteEvolutionLocation } from "../../../../../lib/strategy-evolution/bff";

export const POST = withNexusBff(async (request, actor) => {
  const body = await readRollback(request);
  const headers = requireEvolutionWriteHeaders(request);
  const response = await engineFetch("/internal/v1/strategy-registry/rollback", {
    method: "POST",
    actor,
    body,
    headers: {
      "content-type": "application/json",
      "idempotency-key": headers.idempotencyKey,
      "if-match": headers.ifMatch,
    },
  });
  return rewriteEvolutionLocation(response);
}, { requireIdempotencyKey: true, requireVersion: true });
