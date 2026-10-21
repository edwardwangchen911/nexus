import { EventEmitter } from "node:events";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it, vi } from "vitest";

import {
  buildRealFixtureEnvironment,
  RealFixtureRunnerError,
  runSubprocess,
  runRealFixture,
  validateRealFixtureInput,
} from "../../../scripts/e2e/real-fixture.mjs";

const safeUrl = "postgresql+asyncpg://fixture:fixture@127.0.0.1:55432/nexus_e2e_unit";
const inertGuard = async () => ({ stop: async () => undefined });

function createFakeChild() {
  return Object.assign(new EventEmitter(), {
    kill: vi.fn<(signal?: NodeJS.Signals) => boolean>(() => true),
  });
}

describe("real fixture runner", () => {
  it.each([
    [undefined, "missing URL"],
    ["not a url", "malformed URL"],
    ["postgresql://fixture:fixture@127.0.0.1/nexus_e2e_unit", "non-asyncpg URL"],
    ["postgresql+asyncpg://fixture:fixture@example.test/nexus_e2e_unit", "non-loopback URL"],
    ["postgresql+asyncpg://fixture:fixture@127.0.0.1/nexus_other", "non-fixture database"],
    ["postgresql+asyncpg://fixture:fixture@127.0.0.1/nexus_e2e_unit%2Fother", "encoded database path"],
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
      startGuard: inertGuard,
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
      startGuard: inertGuard,
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
      startGuard: inertGuard,
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
      CLOUDFLARE_LOAD_DEV_VARS_FROM_DOT_ENV: "false",
      CLOUDFLARE_INCLUDE_PROCESS_ENV: "false",
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

  it("holds the database guard across migration, offline seed, Playwright, and verification", async () => {
    const calls: Array<{ executable: string; args: string[]; env: Record<string, string> }> = [];
    const lifecycle: string[] = [];
    const exit = await runRealFixture({
      env: { PATH: "/safe/bin", NEXUS_REAL_E2E_DATABASE_URL: safeUrl, OPENAI_API_KEY: "canary" },
      args: [],
      startGuard: async () => {
        lifecycle.push("guard-start");
        return { stop: async () => { lifecycle.push("guard-stop"); } };
      },
      run: async (executable, args, env, options) => {
        lifecycle.push(options.stage);
        calls.push({ executable, args, env });
        return 0;
      },
    });

    expect(exit).toBe(0);
    expect(calls.map(({ executable, args }) => [executable, args])).toEqual([
      ["uv", ["run", "--package", "nexus-engine", "alembic", "-c", "engine/alembic.ini", "upgrade", "head"]],
      ["uv", ["run", "--package", "nexus-engine", "python", "scripts/e2e/seed_observation_readback.py"]],
      [process.execPath, ["./node_modules/@playwright/test/cli.js", "test", "--config", "playwright.real.config.ts"]],
      [process.execPath, ["scripts/e2e/verify-real-fixture-result.mjs"]],
    ]);
    expect(calls[0]?.env).toEqual(calls[1]?.env);
    expect(calls[1]?.env).toEqual(calls[2]?.env);
    expect(calls[2]?.env).toEqual(calls[3]?.env);
    expect(calls[0]?.env).not.toHaveProperty("OPENAI_API_KEY");
    expect(lifecycle).toEqual(["guard-start", "migration", "observation-seed", "playwright", "result-verifier", "guard-stop"]);
  });

  it("stops after a migration failure and propagates the Playwright exit code", async () => {
    const calls: string[] = [];
    const migrationFailure = await runRealFixture({
      env: { NEXUS_REAL_E2E_DATABASE_URL: safeUrl },
      args: [],
      startGuard: inertGuard,
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
      startGuard: inertGuard,
      run: async (executable) => executable === "uv" ? 0 : 7,
    });
    expect(playwrightFailure).toBe(7);
  });

  it.each([
    ["--list"],
    ["--grep", "fixture"],
    ["--pass-with-no-tests"],
    ["--workers=2"],
    ["--retries", "3"],
    ["--repeat-each=2"],
    ["--shard=1/2"],
    ["--config=playwright.config.ts"],
    ["--config", "playwright.config.ts"],
    ["-cplaywright.config.ts"],
    ["tests/e2e-real/integrations-real.spec.ts"],
  ])("refuses every CLI argument before a child or database guard action: %j", async (...args) => {
    const actions: string[] = [];
    await expect(runRealFixture({
      env: { NEXUS_REAL_E2E_DATABASE_URL: safeUrl },
      args,
      startGuard: async () => {
        actions.push("guard");
        return { stop: async () => undefined };
      },
      run: async () => {
        actions.push("child");
        return 0;
      },
    })).rejects.toThrow("real fixture E2E arguments are invalid");
    expect(actions).toEqual([]);
  });

  it.each([".env", ".env.local", ".dev.vars", ".dev.vars.test"]) (
    "rejects a root configuration canary before a child or database action: %s",
    async (fileName) => {
      const root = mkdtempSync(join(tmpdir(), "nexus-real-e2e-root-"));
      const actions: string[] = [];
      writeFileSync(join(root, fileName), "FOSUN_API_KEY=credential-canary\n", { mode: 0o600 });
      try {
        await expect(runRealFixture({
          env: { NEXUS_REAL_E2E_DATABASE_URL: safeUrl },
          args: [],
          projectRoot: root,
          startGuard: async () => {
            actions.push("guard");
            return { stop: async () => undefined };
          },
          run: async () => {
            actions.push("child");
            return 0;
          },
        })).rejects.toThrow("real fixture E2E project configuration is disallowed");
        expect(actions).toEqual([]);
      } finally {
        rmSync(root, { recursive: true, force: true });
      }
    },
  );

  it("releases the database guard when migration or Playwright fails", async () => {
    for (const failureStage of ["migration", "observation-seed", "playwright"]) {
      const lifecycle: string[] = [];
      const exit = await runRealFixture({
        env: { NEXUS_REAL_E2E_DATABASE_URL: safeUrl },
        startGuard: async () => ({ stop: async () => { lifecycle.push("guard-stop"); } }),
        run: async (_executable, _args, _env, options) => {
          const stage = options.stage;
          lifecycle.push(stage);
          return stage === failureStage ? 17 : 0;
        },
      });
      expect(exit).toBe(17);
      expect(lifecycle.at(-1)).toBe("guard-stop");
      expect(lifecycle).not.toContain("result-verifier");
      if (failureStage !== "playwright") expect(lifecycle).not.toContain("playwright");
    }
  });

  it("forwards termination to the active child and emits only fixed diagnostics", async () => {
    const child = createFakeChild();
    const signalSource = new EventEmitter();
    const diagnostics: string[] = [];
    const promise = runSubprocess("unsafe-command-canary", ["secret-argument-canary"], {
      DATABASE_URL: "postgresql+asyncpg://secret-canary@127.0.0.1/nexus_e2e_unit",
    }, {
      spawnProcess: () => child,
      signalSource,
      stage: "playwright",
      writeDiagnostic: (line) => diagnostics.push(line),
    });

    signalSource.emit("SIGTERM");
    expect(child.kill).toHaveBeenCalledWith("SIGTERM");
    child.emit("close", null, "SIGTERM");

    await expect(promise).resolves.toBe(1);
    expect(diagnostics.join("\n")).toContain("playwright");
    expect(diagnostics.join("\n")).not.toContain("unsafe-command-canary");
    expect(diagnostics.join("\n")).not.toContain("secret-argument-canary");
    expect(diagnostics.join("\n")).not.toContain("secret-canary");
  });

  it("uses the injected stage name without rendering the command", async () => {
    const child = createFakeChild();
    const diagnostics: string[] = [];
    const promise = runSubprocess(
      "command-canary",
      ["argument-canary"],
      {},
      {
        spawnProcess: () => child,
        signalSource: new EventEmitter(),
        stage: "migration",
        writeDiagnostic: (line) => diagnostics.push(line),
      },
    );
    child.emit("close", 0, null);

    await expect(promise).resolves.toBe(0);
    expect(diagnostics).toEqual([
      "[real-e2e] migration started",
      "[real-e2e] migration exited with code 0",
    ]);
    expect(diagnostics.join("\n")).not.toContain("command-canary");
    expect(diagnostics.join("\n")).not.toContain("argument-canary");
  });

  it("terminates the active child when its abort signal fires", async () => {
    const child = createFakeChild();
    const controller = new AbortController();
    const promise = runSubprocess("command-canary", [], {}, {
      abortSignal: controller.signal,
      spawnProcess: () => child,
      signalSource: new EventEmitter(),
      writeDiagnostic: () => undefined,
    });

    controller.abort();
    expect(child.kill).toHaveBeenCalledWith("SIGTERM");
    child.emit("close", null, "SIGTERM");
    await expect(promise).resolves.toBe(1);
  });

  it("fails closed and aborts the active stage if the database guard exits", async () => {
    let rejectGuard: (error: Error) => void = () => undefined;
    const failure = new Promise<never>((_resolve, reject) => { rejectGuard = reject; });
    const lifecycle: string[] = [];
    const runPromise = runRealFixture({
      env: { NEXUS_REAL_E2E_DATABASE_URL: safeUrl },
      startGuard: async () => ({
        failure,
        stop: async () => { lifecycle.push("guard-stop"); },
      }),
      run: async (_executable, _args, _env, options) => new Promise((resolve) => {
        lifecycle.push(options.stage);
        options.abortSignal.addEventListener("abort", () => {
          lifecycle.push("stage-abort");
          resolve(1);
        }, { once: true });
      }),
    });

    await vi.waitFor(() => expect(lifecycle).toEqual(["migration"]));
    rejectGuard(new RealFixtureRunnerError(
      "DATABASE_GUARD_FAILED",
      "real fixture E2E database guard failed",
    ));

    await expect(runPromise).rejects.toThrow("real fixture E2E database guard failed");
    expect(lifecycle).toEqual(["migration", "stage-abort", "guard-stop"]);
  });
});
