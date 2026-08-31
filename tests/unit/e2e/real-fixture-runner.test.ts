import { describe, expect, it } from "vitest";

import {
  buildRealFixtureEnvironment,
  runRealFixture,
  validateRealFixtureInput,
} from "../../../scripts/e2e/real-fixture.mjs";

const safeUrl = "postgresql+asyncpg://fixture:fixture@127.0.0.1:55432/nexus_e2e_unit";

describe("real fixture runner", () => {
  it.each([
    [undefined, "missing URL"],
    ["not a url", "malformed URL"],
    ["postgresql://fixture:fixture@127.0.0.1/nexus_e2e_unit", "non-asyncpg URL"],
    ["postgresql+asyncpg://fixture:fixture@example.test/nexus_e2e_unit", "non-loopback URL"],
    ["postgresql+asyncpg://fixture:fixture@127.0.0.1/nexus_other", "non-fixture database"],
  ] as Array<[string | undefined, string]>)("rejects %s before a child process without exposing its input", async (url) => {
    const input = url ?? "missing-secret-url";
    const calls: string[] = [];

    expect(() => validateRealFixtureInput({ NEXUS_REAL_E2E_DATABASE_URL: url })).toThrow(
      "real fixture E2E database URL is invalid",
    );
    try {
      validateRealFixtureInput({ NEXUS_REAL_E2E_DATABASE_URL: url });
    } catch (error) {
      expect(String(error)).not.toContain(input);
    }
    await expect(runRealFixture({
      env: { NEXUS_REAL_E2E_DATABASE_URL: url },
      run: async (executable) => {
        calls.push(executable);
        return 0;
      },
    })).rejects.toThrow("real fixture E2E database URL is invalid");
    expect(calls).toEqual([]);
  });

  it("rejects production before a child process without exposing the environment value", async () => {
    const calls: string[] = [];
    await expect(runRealFixture({
      env: { NEXUS_ENV: "production", NEXUS_REAL_E2E_DATABASE_URL: safeUrl },
      run: async (executable) => {
        calls.push(executable);
        return 0;
      },
    })).rejects.toThrow("real fixture E2E environment is disallowed");
    expect(calls).toEqual([]);
    try {
      validateRealFixtureInput({ NEXUS_ENV: "production", NEXUS_REAL_E2E_DATABASE_URL: safeUrl });
    } catch (error) {
      expect(String(error)).not.toContain("production");
    }
  });

  it("rejects database query routing overrides before a child process", async () => {
    const calls: string[] = [];
    const unsafeUrl = "postgresql+asyncpg://fixture:fixture@127.0.0.1:55432/nexus_e2e_unit?host=remote.example:5432";

    await expect(runRealFixture({
      env: { NEXUS_REAL_E2E_DATABASE_URL: unsafeUrl },
      run: async (executable) => {
        calls.push(executable);
        return 0;
      },
    })).rejects.toThrow("real fixture E2E database URL is invalid");
    expect(calls).toEqual([]);
  });

  it("constructs a narrow synthetic environment that drops credential canaries", () => {
    const child = buildRealFixtureEnvironment({
      PATH: "/safe/bin",
      LANG: "C",
      NEXUS_REAL_E2E_DATABASE_URL: safeUrl,
      OPENAI_API_KEY: "openai-canary",
      FOSUN_API_KEY: "fosun-canary",
      ALPACA_API_KEY: "alpaca-canary",
      FRED_API_KEY: "fred-canary",
      NEXUS_BROKER_TOKEN: "broker-canary",
      NEXUS_ACCOUNT_ID: "account-canary",
      NEXUS_TRADING_SECRET: "trading-canary",
    });

    expect(child).toMatchObject({
      PATH: "/safe/bin",
      LANG: "C",
      DATABASE_URL: safeUrl,
      NEXUS_ENV: "test",
      AGENT_RUNTIME_MODE: "fixture",
      NEXUS_PROVIDER_JOBS_ENABLED: "false",
      NEXUS_ENGINE_HMAC_CURRENT_SECRET: "nexus-real-e2e-synthetic-hmac-secret-32-bytes",
    });
    expect(child).not.toHaveProperty("NEXUS_REAL_E2E_DATABASE_URL");
    for (const key of [
      "OPENAI_API_KEY",
      "FOSUN_API_KEY",
      "ALPACA_API_KEY",
      "FRED_API_KEY",
      "NEXUS_BROKER_TOKEN",
      "NEXUS_ACCOUNT_ID",
      "NEXUS_TRADING_SECRET",
    ]) expect(child).not.toHaveProperty(key);
  });

  it("runs migration before Playwright with the same sanitized environment", async () => {
    const calls: Array<{ executable: string; args: string[]; env: Record<string, string> }> = [];
    const exit = await runRealFixture({
      env: { PATH: "/safe/bin", NEXUS_REAL_E2E_DATABASE_URL: safeUrl, OPENAI_API_KEY: "canary" },
      args: ["tests/e2e-real/integrations.spec.ts"],
      run: async (executable, args, env) => {
        calls.push({ executable, args, env });
        return 0;
      },
    });

    expect(exit).toBe(0);
    expect(calls.map(({ executable, args }) => [executable, args])).toEqual([
      ["uv", ["run", "--package", "nexus-engine", "alembic", "-c", "engine/alembic.ini", "upgrade", "head"]],
      [process.execPath, ["./node_modules/@playwright/test/cli.js", "test", "--config", "playwright.real.config.ts", "tests/e2e-real/integrations.spec.ts"]],
    ]);
    expect(calls[0]?.env).toEqual(calls[1]?.env);
    expect(calls[0]?.env).not.toHaveProperty("OPENAI_API_KEY");
  });

  it("stops after a migration failure and propagates the Playwright exit code", async () => {
    const calls: string[] = [];
    const migrationFailure = await runRealFixture({
      env: { NEXUS_REAL_E2E_DATABASE_URL: safeUrl },
      args: [],
      run: async (executable) => {
        calls.push(executable);
        return 19;
      },
    });
    expect(migrationFailure).toBe(19);
    expect(calls).toEqual(["uv"]);

    const playwrightFailure = await runRealFixture({
      env: { NEXUS_REAL_E2E_DATABASE_URL: safeUrl },
      args: [],
      run: async (executable) => executable === "uv" ? 0 : 7,
    });
    expect(playwrightFailure).toBe(7);
  });

  it("refuses config overrides before spawning", async () => {
    const run = async () => 0;

    await expect(runRealFixture({
      env: { NEXUS_REAL_E2E_DATABASE_URL: safeUrl },
      args: ["--config=playwright.config.ts"],
      run,
    })).rejects.toThrow("real fixture E2E arguments are invalid");
    await expect(runRealFixture({
      env: { NEXUS_REAL_E2E_DATABASE_URL: safeUrl },
      args: ["--config", "playwright.config.ts"],
      run,
    })).rejects.toThrow("real fixture E2E arguments are invalid");
    await expect(runRealFixture({
      env: { NEXUS_REAL_E2E_DATABASE_URL: safeUrl },
      args: ["-c", "playwright.config.ts"],
      run,
    })).rejects.toThrow("real fixture E2E arguments are invalid");
    await expect(runRealFixture({
      env: { NEXUS_REAL_E2E_DATABASE_URL: safeUrl },
      args: ["-c=playwright.config.ts"],
      run,
    })).rejects.toThrow("real fixture E2E arguments are invalid");
    await expect(runRealFixture({
      env: { NEXUS_REAL_E2E_DATABASE_URL: safeUrl },
      args: ["-cplaywright.config.ts"],
      run,
    })).rejects.toThrow("real fixture E2E arguments are invalid");
  });
});
