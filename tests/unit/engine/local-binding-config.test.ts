import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

describe("local BFF worker bindings", () => {
  it("injects only the current Engine identity required by the server-side BFF", () => {
    const source = readFileSync("vite.config.ts", "utf8");
    const localEnvironment = source.match(
      /const localEnvironment = Object\.fromEntries\([\s\S]*?\) as Record<string, string>;/,
    )?.[0];

    expect(localEnvironment).toBeDefined();
    expect(localEnvironment).toContain("NEXUS_ENGINE_BASE_URL");
    expect(localEnvironment).toContain("NEXUS_ENGINE_HMAC_CURRENT_KEY_ID");
    expect(localEnvironment).toContain("NEXUS_ENGINE_HMAC_CURRENT_SECRET");
    expect(localEnvironment).not.toContain("NEXUS_ENGINE_HMAC_PREVIOUS");
    expect(localEnvironment).not.toContain("FOSUN_");
    expect(localEnvironment).not.toContain("ALPACA_");
    expect(localEnvironment).not.toContain("OPENAI_");
  });
});
