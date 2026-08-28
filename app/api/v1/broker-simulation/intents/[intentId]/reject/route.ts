import { engineFetch } from "../../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../../lib/engine/with-nexus-bff";
import { brokerResourceId, readRejection, requireBrokerIdempotencyKey } from "../../../../../../lib/broker-simulation/bff";

export const POST = withNexusBff(async (request, actor) => {
  const intentId = brokerResourceId(request, "intent", "reject");
  return engineFetch({
    template: "/internal/v1/broker-simulation/intents/{intent_id}/reject",
    path: `/internal/v1/broker-simulation/intents/${intentId}/reject`,
  }, {
    method: "POST",
    actor,
    body: await readRejection(request),
    headers: {
      "content-type": "application/json",
      "idempotency-key": requireBrokerIdempotencyKey(request),
      "if-match": request.headers.get("if-match")!,
    },
  });
}, { requireIdempotencyKey: true, requireVersion: true });

