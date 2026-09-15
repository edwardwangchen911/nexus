import { defineConfig } from "@playwright/test";

const engineUrl = "http://127.0.0.1:8100";
const webUrl = "http://127.0.0.1:4174";
const realFixtureEnvironment = {
  NEXUS_ENV: "test",
  AGENT_RUNTIME_MODE: "fixture",
  NEXUS_PROVIDER_JOBS_ENABLED: "false",
  NEXUS_ENGINE_HMAC_CURRENT_KEY_ID: "real-fixture",
  NEXUS_ENGINE_HMAC_CURRENT_SECRET: "nexus-real-e2e-synthetic-hmac-secret-32-bytes",
  CLOUDFLARE_LOAD_DEV_VARS_FROM_DOT_ENV: "false",
  CLOUDFLARE_INCLUDE_PROCESS_ENV: "false",
};

export default defineConfig({
  testDir: "./tests/e2e-real",
  testMatch: "integrations-real.spec.ts",
  fullyParallel: false,
  retries: 0,
  workers: 1,
  reporter: [["json", { outputFile: "test-results/real-e2e-results.json" }]],
  use: {
    baseURL: webUrl,
    browserName: "chromium",
    serviceWorkers: "block",
    viewport: { width: 390, height: 844 },
  },
  webServer: [
    {
      command: "uv run --package nexus-engine uvicorn nexus.main:create_engine_app --factory --host 127.0.0.1 --port 8100",
      env: {
        ...realFixtureEnvironment,
        DATABASE_URL: process.env.DATABASE_URL ?? "",
      },
      url: `${engineUrl}/health/live`,
      reuseExistingServer: false,
    },
    {
      command: "WRANGLER_LOG_PATH=.wrangler/wrangler.log vinext dev --hostname 127.0.0.1 --port 4174",
      env: {
        ...realFixtureEnvironment,
        DATABASE_URL: "",
        NEXUS_ENGINE_BASE_URL: engineUrl,
      },
      url: `${webUrl}/__debug`,
      reuseExistingServer: false,
    },
  ],
});
