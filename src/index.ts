import { mkdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { serve } from '@hono/node-server';
import { buildApp } from './server/app.js';
import { nodePeerResolver, parseAuthConfig } from './server/middleware/proxy-auth.js';
import { ConfigStore } from './config/store.js';
import { Dispatcher } from './pipeline/dispatcher.js';
import { Metrics } from './status/metrics.js';
import { createLogger } from './log.js';
import { VERSION } from './version.js';
import type { Hono } from 'hono';
import type { AppConfig } from './config/schema.js';
import type { AppEnv } from './server/types.js';

export type BootOptions = {
  env: Record<string, string | undefined>;
  webRoot: string | null;
};

export type Booted = {
  app: Hono<AppEnv>;
  dispatcher: Dispatcher;
  shutdown: () => Promise<void>;
};

async function assertWritable(label: string, dir: string): Promise<void> {
  const probe = join(dir, `.write-probe-${String(process.pid)}`);
  try {
    await mkdir(dir, { recursive: true });
    await writeFile(probe, 'ok');
    await rm(probe, { force: true });
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new Error(
      `${label} (${dir}) is not writable by uid ${String(process.getuid?.() ?? -1)}: ${detail}\n` +
        `Fix it on the host with:  chown -R 10001:10001 ${dir}`,
      { cause: error },
    );
  }
}

/**
 * Reads a positive-integer env var, or throws. Deliberately not tolerant: a
 * typo silently falling back to the default is how a deployment ends up
 * retrying on a cadence nobody chose, and boot already fails loudly for every
 * other malformed variable.
 */
function positiveIntEnv(
  env: Record<string, string | undefined>,
  name: string,
  fallback: number,
): number {
  const raw = env[name]?.trim();
  if (raw === undefined || raw.length === 0) return fallback;
  const value = Number.parseInt(raw, 10);
  if (!Number.isInteger(value) || value <= 0 || String(value) !== raw) {
    throw new Error(`${name} must be a positive integer, received "${raw}"`);
  }
  return value;
}

/**
 * Boots the service without starting a listener, so tests can drive
 * `booted.app.request(...)` directly. `parseAuthConfig` runs before any
 * directory is touched: a bad `AUTH_MODE` or an invalid
 * `AUTH_TRUSTED_PROXIES` CIDR must reject the boot promise loudly, never
 * fail open and never get masked by a later filesystem error.
 */
export async function boot(options: BootOptions): Promise<Booted> {
  const env = options.env;
  const configDir = env['CONFIG_DIR'] ?? '/config';
  const spoolDir = env['SPOOL_DIR'] ?? '/spool';
  const logsRoot = env['LOGS_ROOT'] ?? '/logs';

  // Parse auth before touching disk: a bad AUTH_MODE must fail fast.
  const authConfig = parseAuthConfig(env);
  // Read straight from the environment, not from authConfig: the strip has to
  // happen whenever an operator has named an identity header, whatever
  // AUTH_MODE says. parseAuthConfig has already rejected an invalid or
  // reserved name in proxy mode; in the other modes a bad value here can only
  // ever remove a header nobody should be sending.
  const identityHeader = env['AUTH_USER_HEADER']?.trim().toLowerCase() ?? null;

  await assertWritable('CONFIG_DIR', configDir);
  await assertWritable('SPOOL_DIR', spoolDir);
  await assertWritable('LOGS_ROOT', logsRoot);

  const log = createLogger(env['LOG_LEVEL'] ?? 'info');
  if (authConfig.mode === 'disabled') {
    log.warn(
      'AUTH_MODE=disabled — the admin interface is UNAUTHENTICATED. Never use this outside local development.',
    );
  }
  if (authConfig.mode === 'unset') {
    log.warn(
      'AUTH_MODE is not set — the admin interface will return 503. Ingest is unaffected. Set AUTH_MODE=proxy with AUTH_TRUSTED_PROXIES and AUTH_USER_HEADER to enable it.',
    );
  }

  const metrics = new Metrics();
  const store = new ConfigStore(configDir);
  // Once, before anything can write: reaps temp files stranded by a crashed
  // predecessor. Safe only here — see the note on sweepStaleTemps.
  await store.sweepStaleTemps();
  const loaded = await store.load();

  let config: AppConfig = loaded.config;
  let etag = loaded.etag;

  const dispatcher = new Dispatcher({
    spoolRoot: spoolDir,
    logsRoot,
    metrics,
    log,
    baseBackoffMs: positiveIntEnv(env, 'RETRY_BASE_MS', 1000),
    maxBackoffMs: positiveIntEnv(env, 'RETRY_MAX_MS', 60_000),
  });
  await dispatcher.applyConfig(config);
  dispatcher.start();

  const app = buildApp({
    authConfig,
    identityHeader,
    peerResolver: nodePeerResolver,
    webRoot: options.webRoot,
    drain: { getConfig: () => config, dispatcher, metrics, log },
    status: {
      getConfig: () => config,
      dispatcher,
      metrics,
      version: VERSION,
      configDir,
      spoolDir,
    },
    admin: {
      store,
      dispatcher,
      getConfig: () => config,
      getEtag: () => etag,
      setConfig: (next, nextEtag) => {
        config = next;
        etag = nextEtag;
      },
      log,
    },
  });

  return {
    app,
    dispatcher,
    shutdown: async () => {
      // Bounded and graceful: in-flight deliveries settle within the
      // deadline, then dispatcher.stop() returns regardless. Anything still
      // unacknowledged stays on disk -- SinkWorker never acks a batch it did
      // not confirm delivered -- and is picked up again on the next boot.
      await dispatcher.stop(10_000);
    },
  };
}

async function main(): Promise<void> {
  const booted = await boot({
    env: process.env,
    webRoot: process.env['WEB_ROOT'] ?? 'web/dist',
  });

  const port = Number.parseInt(process.env['PORT'] ?? '8080', 10);
  const hostname = process.env['HOST'] ?? '0.0.0.0';
  const server = serve({ fetch: booted.app.fetch, port, hostname });

  const log = createLogger(process.env['LOG_LEVEL'] ?? 'info');
  log.info({ port, hostname, version: VERSION }, 'vercel-log-drain listening');

  let shuttingDown = false;
  const stop = (signal: string): void => {
    if (shuttingDown) return;
    shuttingDown = true;
    log.info({ signal }, 'shutting down');
    server.close(() => {
      void booted.shutdown().then(() => {
        process.exit(0);
      });
    });
  };
  process.on('SIGTERM', () => stop('SIGTERM'));
  process.on('SIGINT', () => stop('SIGINT'));
}

// Only run the server when executed directly, so tests can import boot()
// without opening a socket.
if (process.argv[1]?.endsWith('index.js') === true) {
  void main();
}
