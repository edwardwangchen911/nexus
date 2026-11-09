import { engineFetch } from "../../../lib/engine/client";
import { withNexusBff } from "../../../lib/engine/with-nexus-bff";
import { rejectQuery } from "../../../lib/paper/bff";

export const GET = withNexusBff(async (request, actor) => {
  rejectQuery(request, "The workbench query is invalid.");
  const started = performance.now();
  const response = await engineFetch("/internal/v1/workbench", { method: "GET", actor });
  response.headers.append("Server-Timing", `bff;dur=${(performance.now() - started).toFixed(1)}`);
  return response;
});
