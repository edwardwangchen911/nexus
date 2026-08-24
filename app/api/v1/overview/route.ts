import { validate as validateUuid } from "uuid";

import { engineFetch } from "../../../lib/engine/client";
import { NexusBffError } from "../../../lib/engine/errors";
import { withNexusBff } from "../../../lib/engine/with-nexus-bff";

export const GET = withNexusBff(async (request, actor) => {
  const query = canonicalOverviewQuery(new URL(request.url).searchParams);
  return engineFetch({ template: "/internal/v1/overview", path: "/internal/v1/overview", query }, { method: "GET", actor });
});

function canonicalOverviewQuery(search: URLSearchParams): string {
  const allowed = new Set(["portfolio_id", "session_date"]);
  const seen = new Set<string>();
  const entries: Array<[string, string]> = [];
  for (const [key, value] of search) {
    if (!allowed.has(key) || seen.has(key) || !validOverviewValue(key, value)) {
      throw new NexusBffError(400, "VALIDATION_ERROR", "The overview query is invalid.");
    }
    seen.add(key);
    entries.push([key, encodeURIComponent(value)]);
  }
  entries.sort(([left], [right]) => left.localeCompare(right));
  return entries.map(([key, value]) => `${key}=${value}`).join("&");
}

function validOverviewValue(key: string, value: string): boolean {
  if (key === "portfolio_id") return validateUuid(value);
  if (key !== "session_date" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const date = new Date(`${value}T00:00:00Z`);
  return !Number.isNaN(date.valueOf()) && date.toISOString().slice(0, 10) === value;
}
