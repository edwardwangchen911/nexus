import vinext from "vinext";
import { defineConfig } from "vite";

const localEnvironment = Object.fromEntries(
  Object.entries({
    NEXUS_ENGINE_BASE_URL: process.env.NEXUS_ENGINE_BASE_URL,
    NEXUS_ENGINE_HMAC_CURRENT_KEY_ID:
      process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID,
    NEXUS_ENGINE_HMAC_CURRENT_SECRET:
      process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET,
    NEXUS_ENV: process.env.NEXUS_ENV,
  }).filter((entry): entry is [string, string] => typeof entry[1] === "string"),
) as Record<string, string>;

// macOS Seatbelt blocks FSEvents, so Codex previews need polling for HMR.
const isCodexSeatbeltSandbox = process.env.CODEX_SANDBOX === "seatbelt";

const localBindingConfig = {
  main: "./worker/index.ts",
  compatibility_flags: ["nodejs_compat"],
  vars: localEnvironment,
};

export default defineConfig(async () => {
  // Keep Wrangler and Miniflare state project-local. These are non-secret tool
  // settings; application environment belongs in ignored `.env*` files.
  process.env.WRANGLER_WRITE_LOGS ??= "false";
  process.env.WRANGLER_LOG_PATH ??= ".wrangler/logs";
  process.env.MINIFLARE_REGISTRY_PATH ??= ".wrangler/registry";

  // Wrangler snapshots its log path while the Cloudflare plugin is imported.
  const { cloudflare } = await import("@cloudflare/vite-plugin");

  return {
    server: {
      host: "127.0.0.1",
      ...(isCodexSeatbeltSandbox
        ? { watch: { useFsEvents: false, usePolling: true } }
        : {}),
    },
    plugins: [
      vinext(),
      cloudflare({
        viteEnvironment: { name: "rsc", childEnvironments: ["ssr"] },
        config: localBindingConfig,
      }),
    ],
  };
});
