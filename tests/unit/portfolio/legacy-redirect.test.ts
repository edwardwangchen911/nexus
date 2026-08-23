import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

describe("legacy Fosun holdings boundary", () => {
  it("contains only one permanent redirect and no data-fetch capability", async () => {
    const source = await readFile(
      join(process.cwd(), "app/(protected)/holdings/fosun/page.tsx"),
      "utf8",
    );

    expect(source).toContain('permanentRedirect("/portfolio?source=fosun")');
    expect(source).not.toMatch(/\bfetch\s*\(|engineFetch|\/api\/v1\//);
    expect(source.match(/permanentRedirect\s*\(/g)).toHaveLength(1);
  });
});
