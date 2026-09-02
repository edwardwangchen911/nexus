import { defineConfig } from "@playwright/test";

const port = process.env.NEXUS_E2E_PORT ?? "4173";

export default defineConfig({
  testDir: "./tests/e2e",
  use: {
    baseURL: `http://127.0.0.1:${port}`,
    browserName: "chromium",
    channel: process.env.NEXUS_E2E_BROWSER_CHANNEL === "chromium" ? "chromium" : "chrome",
    viewport: { width: 390, height: 844 },
    extraHTTPHeaders: {
      "oai-authenticated-user-id": "task-1-browser-test-user",
      "oai-authenticated-user-email": "task-1-browser-test@example.test",
    },
  },
  webServer: {
    command: `WRANGLER_LOG_PATH=.wrangler/wrangler.log vinext dev --hostname 127.0.0.1 --port ${port}`,
    env: {
      NEXUS_ALLOWED_USER_ID: "task-1-browser-test-user",
      NEXUS_ENV: "development",
    },
    url: `http://127.0.0.1:${port}/__debug`,
    reuseExistingServer: false,
  },
});
