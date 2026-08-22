import { execFile } from "node:child_process";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { describe, expect, it } from "vitest";

const run = promisify(execFile);
const root = "/Users/wangchen/Documents/ChatGPT/投资/.worktrees/nexus-phase-0-1";

describe("server-only production client boundary", () => {
  it.each(["hmac", "client"])("vinext rejects real %s in a client page", async (target) => {
    const fixture = await mkdtemp(join(tmpdir(), `nexus-vinext-${target}-`));
    try {
      await mkdir(join(fixture, "app"));
      await writeFile(join(fixture, "package.json"), '{"private":true,"type":"module"}');
      await writeFile(join(fixture, "vite.config.ts"), 'import vinext from "vinext"; import { defineConfig } from "vite"; export default defineConfig({plugins:[vinext()]});');
      await writeFile(join(fixture, "app/layout.tsx"), 'export default function Layout({children}:{children:React.ReactNode}){return <html><body>{children}</body></html>}');
      const symbol = target === "hmac" ? "createEngineHmacHeaders" : "engineFetch";
      await writeFile(join(fixture, "app/page.tsx"), `"use client"; import { ${symbol} } from "${root}/app/lib/engine/${target}.ts"; export default function Page(){return <p>{typeof ${symbol}}</p>}`);
      await symlink(join(root, "node_modules"), join(fixture, "node_modules"), "dir");
      const output = await run(join(root, "node_modules/.bin/vinext"), ["build"], { cwd: fixture, timeout: 30_000 }).then(() => "", (error: { stdout?: string; stderr?: string }) => `${error.stdout ?? ""}${error.stderr ?? ""}`);
      expect(output).toContain("'server-only' cannot be imported in client build ('ssr' environment):");
      expect(output).toContain(`app/lib/engine/${target}.ts`);
    } finally { await rm(fixture, { recursive: true, force: true }); }
  }, 60_000);
});
