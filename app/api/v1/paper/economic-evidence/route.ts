import { engineFetch } from "../../../../lib/engine/client";
import { withNexusBff } from "../../../../lib/engine/with-nexus-bff";
import { rejectQuery } from "../../../../lib/paper/bff";

export const GET = withNexusBff(async (request, actor) => {
  rejectQuery(request, "The evidence query is invalid.");
  return engineFetch("/internal/v1/paper/economic-evidence", { method: "GET", actor });
});
