import { defineConfig } from "@playwright/test";

const port = process.env.NEXUS_E2E_PORT ?? "4173";

export default defineConfig({
  testDir: "./tests/e2e",
  use: {
    baseURL: `http://127.0.0.1:${port}`,
    browserName: "chromium",
    channel: process.env.NEXUS_E2E_BROWSER_CHANNEL === "chromium" ? "chromium" : "chrome",
    viewport: { width: 390, height: 844 },
  },
  webServer: {
    command: `WRANGLER_LOG_PATH=.wrangler/wrangler.log vinext dev --host 127.0.0.1 --port ${port}`,
    env: {
      NEXUS_ENV: "development",
    },
    url: `http://127.0.0.1:${port}/__debug`,
    reuseExistingServer: false,
  },
});
