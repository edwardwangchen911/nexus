import { execFile } from "node:child_process";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { describe, expect, it } from "vitest";

const run = promisify(execFile);
const testFile = fileURLToPath(import.meta.url);
const root = resolve(dirname(testFile), "../../..");
const isCwdProbe = process.env.NEXUS_SERVER_ONLY_CWD_PROBE === "1";

async function runClientBuild(target: "hmac" | "client") {
  const fixture = await mkdtemp(join(tmpdir(), `nexus-vinext-${target}-`));
  try {
    await mkdir(join(fixture, "app"));
    await writeFile(join(fixture, "package.json"), '{"private":true,"type":"module"}');
    await writeFile(join(fixture, "vite.config.ts"), 'import vinext from "vinext"; import { defineConfig } from "vite"; export default defineConfig({plugins:[vinext()]});');
    await writeFile(join(fixture, "app/layout.tsx"), 'export default function Layout({children}:{children:React.ReactNode}){return <html><body>{children}</body></html>}');
    const symbol = target === "hmac" ? "createEngineHmacHeaders" : "engineFetch";
    await writeFile(join(fixture, "app/page.tsx"), `"use client"; import { ${symbol} } from "${root}/app/lib/engine/${target}.ts"; export default function Page(){return <p>{typeof ${symbol}}</p>}`);
    await symlink(join(root, "node_modules"), join(fixture, "node_modules"), "dir");
    return await run(join(root, "node_modules/.bin/vinext"), ["build"], { cwd: fixture, timeout: 30_000 })
      .then(({ stdout, stderr }) => ({ exitCode: 0 as number | string, output: `${stdout}${stderr}` }))
      .catch((error: { code?: number | string; stdout?: string; stderr?: string }) => ({
        exitCode: error.code ?? "unknown",
        output: `${error.stdout ?? ""}${error.stderr ?? ""}`,
      }));
  } finally {
    await rm(fixture, { recursive: true, force: true });
  }
}

describe("server-only production client boundary", () => {
  it.each(["hmac", "client"])("vinext rejects real %s in a client page", async (target) => {
    const result = await runClientBuild(target as "hmac" | "client");

    expect(result.exitCode).not.toBe(0);
    expect(result.output).toContain("'server-only' cannot be imported in client build ('ssr' environment):");
    expect(result.output).toContain(`app/lib/engine/${target}.ts`);
  }, 60_000);

  it.runIf(!isCwdProbe)("passes both probes when the test process starts outside the repository", async () => {
    const childCwd = await mkdtemp(join(tmpdir(), "nexus-vitest-cwd-"));
    try {
      const { stdout, stderr } = await run(
        process.execPath,
        [join(root, "node_modules/vitest/vitest.mjs"), "run", "tests/unit/engine/server-only-boundary.test.ts", "--root", root],
        {
          cwd: childCwd,
          env: { ...process.env, NEXUS_SERVER_ONLY_CWD_PROBE: "1" },
          timeout: 120_000,
        },
      );
      expect(`${stdout}${stderr}`).toContain("2 passed");
    } finally {
      await rm(childCwd, { recursive: true, force: true });
    }
  }, 150_000);
});
