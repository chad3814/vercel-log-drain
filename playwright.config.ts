import { defineConfig } from '@playwright/test';

/**
 * Browser tests for the SPA. A separate directory and runner from `test/`,
 * which vitest owns: vitest runs with `environment: 'node'` and matches only
 * `test/**\/*.test.ts`, so nothing under `browser/` is picked up twice.
 *
 * These exist because the SPA had no coverage of any kind. The first bug they
 * pin -- the sink Name field losing focus on every keystroke -- passed every
 * gate in this repo and had to be reported by a human using the UI.
 */
/**
 * The port the test server binds, derived from this checkout's path.
 *
 * A fixed port breaks as soon as two worktrees run the gate at once, and it
 * breaks in the worst way: the second run's webServer finds the port already
 * serving, `reuseExistingServer: false` notwithstanding, and the tests can
 * end up driving the OTHER worktree's build -- passing or failing against
 * code from a different branch. Deriving it from `process.cwd()` keeps each
 * checkout on its own port with no coordination. `PW_PORT` overrides it when
 * a specific port is needed.
 */
function derivePort(): number {
  const override = process.env['PW_PORT'];
  if (override !== undefined && override.trim().length > 0) {
    const parsed = Number.parseInt(override, 10);
    if (!Number.isInteger(parsed) || parsed < 1024 || parsed > 65_535) {
      throw new Error(`PW_PORT must be a port between 1024 and 65535, received "${override}"`);
    }
    return parsed;
  }
  // Cheap deterministic hash of the checkout path into the high ephemeral
  // range, avoiding the low ports a dev server or the smoke test may hold.
  let hash = 0;
  for (const char of process.cwd()) hash = (hash * 31 + char.charCodeAt(0)) % 20_000;
  return 40_000 + hash;
}

const PORT = derivePort();

export default defineConfig({
  testDir: './browser',
  // One worker: every test drives the same server against one config file, so
  // parallel tests would fight over it.
  workers: 1,
  use: {
    baseURL: `http://127.0.0.1:${String(PORT)}`,
  },
  webServer: {
    // AUTH_MODE=disabled so the admin UI is reachable without a proxy. The
    // state directories are throwaway: a browser test must never inherit
    // config from a previous run, or a stale sink makes it pass or fail for
    // reasons that have nothing to do with the code.
    command:
      'rm -rf .playwright-state && mkdir -p .playwright-state/config .playwright-state/spool .playwright-state/logs && ' +
      'CONFIG_DIR=.playwright-state/config SPOOL_DIR=.playwright-state/spool LOGS_ROOT=.playwright-state/logs ' +
      `WEB_ROOT=web/dist AUTH_MODE=disabled LOG_LEVEL=silent PORT=${String(PORT)} npm start`,
    url: `http://127.0.0.1:${String(PORT)}/healthz`,
    reuseExistingServer: false,
    timeout: 60_000,
  },
});
