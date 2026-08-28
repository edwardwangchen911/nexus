import { engineFetch } from "../../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../../lib/engine/with-nexus-bff";
import { brokerResourceId, readOrderMutation, requireBrokerIdempotencyKey } from "../../../../../../lib/broker-simulation/bff";

export const POST = withNexusBff(async (request, actor) => {
  const orderId = brokerResourceId(request, "order", "cancel");
  return engineFetch({
    template: "/internal/v1/broker-simulation/orders/{order_id}/cancel",
    path: `/internal/v1/broker-simulation/orders/${orderId}/cancel`,
  }, {
    method: "POST",
    actor,
    body: await readOrderMutation(request, "cancel"),
    headers: {
      "content-type": "application/json",
      "idempotency-key": requireBrokerIdempotencyKey(request),
      "if-match": request.headers.get("if-match")!,
    },
  });
}, { requireIdempotencyKey: true, requireVersion: true });

