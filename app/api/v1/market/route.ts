import { validate as validateUuid } from "uuid";
import { engineFetch } from "../../../lib/engine/client";
import { NexusBffError } from "../../../lib/engine/errors";
import { withNexusBff } from "../../../lib/engine/with-nexus-bff";

const MARKET_QUERY_KEYS = new Set([
  "cursor",
  "limit",
  "selected_instrument_id",
  "watchlist",
]);

export const GET = withNexusBff(async (request, actor) => {
  const query = canonicalMarketQuery(new URL(request.url).searchParams);
  return engineFetch(
    {
      template: "/internal/v1/market",
      path: "/internal/v1/market",
      query,
    },
    { method: "GET", actor },
  );
});

function canonicalMarketQuery(search: URLSearchParams): string {
  const seen = new Set<string>();
  const entries: Array<[string, string]> = [];

  for (const [key, value] of search) {
    if (!MARKET_QUERY_KEYS.has(key) || seen.has(key)) {
      throw new NexusBffError(400, "VALIDATION_ERROR", "The market query is invalid.");
    }
    validateMarketValue(key, value);
    seen.add(key);
    entries.push([strictPercentEncode(key), strictPercentEncode(value)]);
  }

  entries.sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);
  return entries.map(([key, value]) => `${key}=${value}`).join("&");
}

function validateMarketValue(key: string, value: string): void {
  if (key === "limit" && (!/^\d+$/.test(value) || Number(value) < 1 || Number(value) > 100)) {
    throw new NexusBffError(400, "VALIDATION_ERROR", "The market limit is invalid.");
  }
  if (key === "watchlist" && value !== "true" && value !== "false") {
    throw new NexusBffError(400, "VALIDATION_ERROR", "The watchlist filter is invalid.");
  }
  if (key === "selected_instrument_id" && !validateUuid(value)) {
    throw new NexusBffError(400, "VALIDATION_ERROR", "The selected instrument is invalid.");
  }
  if (key === "cursor" && (value.length === 0 || value.length > 2048)) {
    throw new NexusBffError(400, "VALIDATION_ERROR", "The market cursor is invalid.");
  }
}

function strictPercentEncode(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/g, (character) =>
    `%${character.charCodeAt(0).toString(16).toUpperCase()}`,
  );
}
