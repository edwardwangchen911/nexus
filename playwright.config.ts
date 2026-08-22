import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/e2e",
  globalTeardown: "./tests/e2e/global-teardown.ts",
  use: {
    baseURL: "http://127.0.0.1:4173",
    browserName: "chromium",
    channel: "chrome",
    viewport: { width: 390, height: 844 },
    extraHTTPHeaders: {
      "oai-authenticated-user-id": "task-1-browser-test-user",
      "oai-authenticated-user-email": "task-1-browser-test@example.test",
    },
  },
  webServer: {
    command: "sh -c 'if [ -f .env ]; then rm -f .playwright-env-created; else cp .env.playwright .env; : > .playwright-env-created; fi; NEXUS_ENV=development WRANGLER_LOG_PATH=.wrangler/wrangler.log vinext dev --host 127.0.0.1 --port 4173'",
    url: "http://127.0.0.1:4173/__debug",
    reuseExistingServer: false,
  },
});
