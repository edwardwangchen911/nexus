import { engineFetch } from "../../../lib/engine/client";
import { withNexusBff } from "../../../lib/engine/with-nexus-bff";
import { rejectBrokerQuery } from "../../../lib/broker-simulation/bff";

export const GET = withNexusBff(async (request, actor) => {
  rejectBrokerQuery(request);
  return engineFetch("/internal/v1/broker-simulation", {
    method: "GET",
    actor,
    headers: { accept: "application/json" },
  });
});

