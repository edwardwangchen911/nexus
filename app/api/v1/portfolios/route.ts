import { engineFetch } from "../../../lib/engine/client";
import { NexusBffError } from "../../../lib/engine/errors";
import { withNexusBff } from "../../../lib/engine/with-nexus-bff";

const QUERY_KEYS = new Set(["cursor", "kind", "limit"]);
const KINDS = new Set(["real_read_only", "internal_paper", "shadow"]);

export const GET = withNexusBff(async (request, actor) => {
  const query = canonicalPortfolioQuery(new URL(request.url).searchParams);
  return engineFetch({
    template: "/internal/v1/portfolios",
    path: "/internal/v1/portfolios",
    query,
  }, { method: "GET", actor });
});

function canonicalPortfolioQuery(search: URLSearchParams): string {
  const seen = new Set<string>();
  const entries: Array<[string, string]> = [];
  for (const [key, value] of search) {
    if (!QUERY_KEYS.has(key) || seen.has(key) || !validValue(key, value)) {
      throw new NexusBffError(400, "VALIDATION_ERROR", "The portfolio query is invalid.");
    }
    seen.add(key);
    entries.push([strictPercentEncode(key), strictPercentEncode(value)]);
  }
  entries.sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);
  return entries.map(([key, value]) => `${key}=${value}`).join("&");
}

function validValue(key: string, value: string): boolean {
  if (key === "limit") return /^\d+$/.test(value) && Number(value) >= 1 && Number(value) <= 50;
  if (key === "kind") return KINDS.has(value);
  if (key === "cursor") return value.length >= 1 && value.length <= 2048;
  return false;
}

function strictPercentEncode(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/g, (character) =>
    `%${character.charCodeAt(0).toString(16).toUpperCase()}`,
  );
}
