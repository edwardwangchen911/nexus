import { engineFetch } from "../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../lib/engine/with-nexus-bff";
import { readRegistryControl, requireEvolutionWriteHeaders } from "../../../../../lib/strategy-evolution/bff";

export const POST = withNexusBff(async (request, actor) => {
  const { body } = await readRegistryControl(request);
  const headers = requireEvolutionWriteHeaders(request);
  return engineFetch("/internal/v1/strategy-registry/control", {
    method: "POST",
    actor,
    body,
    headers: {
      "content-type": "application/json",
      "idempotency-key": headers.idempotencyKey,
      "if-match": headers.ifMatch,
    },
  });
}, { requireIdempotencyKey: true, requireVersion: true });
