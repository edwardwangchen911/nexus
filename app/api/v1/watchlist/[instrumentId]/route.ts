import { validate as validateUuid } from "uuid";
import type { NexusActor } from "../../../../lib/auth/nexus-user";
import { engineFetch } from "../../../../lib/engine/client";
import { NexusBffError } from "../../../../lib/engine/errors";
import { withNexusBff } from "../../../../lib/engine/with-nexus-bff";

const WATCHLIST_PATH = /^\/api\/v1\/watchlist\/([^/]+)$/;

async function mutateWatchlist(request: Request, method: "PUT" | "DELETE", actor: NexusActor): Promise<Response> {
  const match = WATCHLIST_PATH.exec(new URL(request.url).pathname);
  const instrumentId = match?.[1];
  if (!instrumentId || !validateUuid(instrumentId)) {
    throw new NexusBffError(400, "VALIDATION_ERROR", "The instrument is invalid.");
  }

  return engineFetch(
    {
      template: "/internal/v1/watchlist/{instrument_id}",
      path: `/internal/v1/watchlist/${instrumentId}`,
    },
    { method, actor },
  );
}

export const PUT = withNexusBff(
  async (request, actor) => mutateWatchlist(request, "PUT", actor),
  { requireIdempotencyKey: false },
);

export const DELETE = withNexusBff(
  async (request, actor) => mutateWatchlist(request, "DELETE", actor),
  { requireIdempotencyKey: false },
);
