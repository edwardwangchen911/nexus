import { engineFetch } from "../../../../lib/engine/client";
import { withNexusBff } from "../../../../lib/engine/with-nexus-bff";
import { readPrepare, requireBrokerIdempotencyKey } from "../../../../lib/broker-simulation/bff";

export const POST = withNexusBff(async (request, actor) => engineFetch(
  "/internal/v1/broker-simulation/intents",
  {
    method: "POST",
    actor,
    body: await readPrepare(request),
    headers: {
      "content-type": "application/json",
      "idempotency-key": requireBrokerIdempotencyKey(request),
    },
  },
), { requireIdempotencyKey: true });

