import { execFileSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

describe("server-only boundary", () => {
  it("rejects loading the server-only poison package under the default client-like condition", () => {
    expect(() => execFileSync(process.execPath, ["--input-type=module", "-e", "await import('server-only')"], { stdio: "pipe" })).toThrow();
  });

  it("rejects a client import graph that reaches the actual signer", async () => {
    const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
    const entry = resolve(root, "tests/fixtures/illegal-engine-client.ts");

    await expect(assertNoServerOnlyReachability(entry)).rejects.toThrow("server-only module");
  });
});

async function assertNoServerOnlyReachability(file: string, visited = new Set<string>()): Promise<void> {
  if (visited.has(file)) return;
  visited.add(file);
  const source = await readFile(file, "utf8");
  if (source.includes('import "server-only"')) throw new Error(`server-only module reachable: ${file}`);
  const imports = [...source.matchAll(/from\s+["']([^"']+)["']/g)].map((match) => match[1]);
  await Promise.all(imports.filter((specifier) => specifier.startsWith(".")).map((specifier) =>
    assertNoServerOnlyReachability(resolve(dirname(file), `${specifier}.ts`), visited),
  ));
}
