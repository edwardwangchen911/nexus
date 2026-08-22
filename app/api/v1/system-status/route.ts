import { engineFetch } from "../../../lib/engine/client";
import { withNexusBff } from "../../../lib/engine/with-nexus-bff";

export const GET = withNexusBff(async (_request, actor) =>
  engineFetch("/internal/v1/system-status", { method: "GET", actor }),
);
