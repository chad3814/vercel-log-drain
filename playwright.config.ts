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
export default defineConfig({
  testDir: './browser',
  // One worker: every test drives the same server against one config file, so
  // parallel tests would fight over it.
  workers: 1,
  use: {
    baseURL: 'http://127.0.0.1:8099',
  },
  webServer: {
    // AUTH_MODE=disabled so the admin UI is reachable without a proxy. The
    // state directories are throwaway: a browser test must never inherit
    // config from a previous run, or a stale sink makes it pass or fail for
    // reasons that have nothing to do with the code.
    command:
      'rm -rf .playwright-state && mkdir -p .playwright-state/config .playwright-state/spool .playwright-state/logs && ' +
      'CONFIG_DIR=.playwright-state/config SPOOL_DIR=.playwright-state/spool LOGS_ROOT=.playwright-state/logs ' +
      'WEB_ROOT=web/dist AUTH_MODE=disabled LOG_LEVEL=silent PORT=8099 npm start',
    url: 'http://127.0.0.1:8099/healthz',
    reuseExistingServer: false,
    timeout: 60_000,
  },
});
