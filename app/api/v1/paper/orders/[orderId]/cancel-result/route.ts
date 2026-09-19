import { engineFetch } from "../../../../../../lib/engine/client";
import { withNexusBff } from "../../../../../../lib/engine/with-nexus-bff";
import {
  paperCancelResultOrderId,
  rejectQuery,
  requirePaperIdempotencyKey,
} from "../../../../../../lib/paper/bff";

export const GET = withNexusBff(async (request, actor) => {
  rejectQuery(request, "The paper cancellation result query is invalid.");
  const orderId = paperCancelResultOrderId(request);
  const idempotencyKey = requirePaperIdempotencyKey(request);
  return engineFetch({
    template: "/internal/v1/paper/orders/{order_id}/cancel-result",
    path: `/internal/v1/paper/orders/${orderId}/cancel-result`,
  }, {
    method: "GET",
    actor,
    headers: { "idempotency-key": idempotencyKey },
  });
});
