import { engineFetch } from "../../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../../lib/engine/with-nexus-bff";
import { brokerResourceId, readOrderMutation, requireBrokerIdempotencyKey } from "../../../../../../lib/broker-simulation/bff";

export const POST = withNexusBff(async (request, actor) => {
  const orderId = brokerResourceId(request, "order", "modify");
  return engineFetch({
    template: "/internal/v1/broker-simulation/orders/{order_id}/modify",
    path: `/internal/v1/broker-simulation/orders/${orderId}/modify`,
  }, {
    method: "POST",
    actor,
    body: await readOrderMutation(request, "modify"),
    headers: {
      "content-type": "application/json",
      "idempotency-key": requireBrokerIdempotencyKey(request),
      "if-match": request.headers.get("if-match")!,
    },
  });
}, { requireIdempotencyKey: true, requireVersion: true });

