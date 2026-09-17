import { engineFetch } from "../../../lib/engine/client";
import { withNexusBff } from "../../../lib/engine/with-nexus-bff";
import { rejectQuery } from "../../../lib/paper/bff";

export const GET = withNexusBff(async (request, actor) => {
  rejectQuery(request, "The live-evaluation query is invalid.");
  return engineFetch("/internal/v1/live-evaluation", { method: "GET", actor });
});
