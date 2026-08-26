import { engineFetch } from "../../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../../lib/engine/with-nexus-bff";
import { paperOrderId, readPaperCancel, requirePaperIdempotencyKey } from "../../../../../../lib/paper/bff";

export const POST = withNexusBff(async (request, actor) => {
  const orderId = paperOrderId(request);
  const body = await readPaperCancel(request);
  return engineFetch({
    template: "/internal/v1/paper/orders/{order_id}/cancel",
    path: `/internal/v1/paper/orders/${orderId}/cancel`,
  }, {
    method: "POST",
    actor,
    body,
    headers: {
      "content-type": "application/json",
      "idempotency-key": requirePaperIdempotencyKey(request),
    },
  });
});
