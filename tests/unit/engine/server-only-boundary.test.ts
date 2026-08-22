import { execFileSync } from "node:child_process";
import { describe, expect, it } from "vitest";

describe("server-only boundary", () => {
  it("rejects loading the server-only poison package under the default client-like condition", () => {
    expect(() => execFileSync(process.execPath, ["--input-type=module", "-e", "await import('server-only')"], { stdio: "pipe" })).toThrow();
  });
});
