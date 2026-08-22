import { readdir } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { describe, expect, it } from "vitest";
import { isNexusBffRoute } from "../../../app/lib/engine/with-nexus-bff";

async function routeModules(directory: string): Promise<string[]> {
  const entries = await readdir(directory, { withFileTypes: true });
  const nested = await Promise.all(
    entries.map(async (entry) => {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) return routeModules(path);
      return entry.isFile() && entry.name === "route.ts" ? [path] : [];
    }),
  );
  return nested.flat();
}

describe("NEXUS BFF boundary", () => {
  it("registers every API route handler through the mandatory wrapper", async () => {
    const projectRoot = join(dirname(fileURLToPath(import.meta.url)), "../../..");
    const modules = await routeModules(join(projectRoot, "app/api/v1"));

    expect(modules).not.toHaveLength(0);
    for (const modulePath of modules) {
      const route = await import(pathToFileURL(modulePath).href);
      for (const method of ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]) {
        if (method in route) expect(isNexusBffRoute(route[method])).toBe(true);
      }
    }
  });
});
