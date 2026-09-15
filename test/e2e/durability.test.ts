import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createHmac } from 'node:crypto';
import { createServer } from 'node:http';
import type { Server } from 'node:http';
import { gunzip } from 'node:zlib';
import { promisify } from 'node:util';
import { mkdir, mkdtemp, readdir, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { boot } from '../../src/index.js';
import { ConfigStore } from '../../src/config/store.js';
import { defaultAppConfig } from '../../src/config/schema.js';
import type { Booted } from '../../src/index.js';

const gunzipAsync = promisify(gunzip);
const SECRET = 'e'.repeat(40);

function event(id: string) {
  return {
    id,
    timestamp: Date.UTC(2026, 8, 8, 12, 0, 0),
    source: 'lambda',
    projectId: 'p1',
    projectName: 'my-app',
    environment: 'production',
    level: 'info',
    message: `event ${id}`,
  };
}

function sign(body: string): string {
  return createHmac('sha1', SECRET).update(body).digest('hex');
}

async function post(booted: Booted, events: ReturnType<typeof event>[]) {
  const body = JSON.stringify(events);
  return booted.app.request('/api/drain/e2e-drain', {
    method: 'POST',
    body,
    headers: { 'x-vercel-signature': sign(body) },
  });
}

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 8000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error('condition not met before timeout');
}

/**
 * Reads a response body as `T`. This is an UNCHECKED cast, concentrated in one
 * place on purpose: the server tsconfig has no "dom" lib, so
 * `Response.json()` is typed `Promise<unknown>` and cannot be landed in a
 * typed binding without one. Prefer asserting directly --
 * `expect(await response.json()).toMatchObject({...})` takes `unknown` and
 * needs no cast. Reach for this only where a test genuinely has to read a
 * value out of the body: reuse it in a later request, filter a list, or
 * compare a number. Never launder the cast through
 * `JSON.parse(await response.text())`, which hides it behind `any`.
 */
async function jsonBody<T>(response: Response): Promise<T> {
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion
  return (await response.json()) as T;
}

describe('end-to-end durability', () => {
  let root = '';
  let configDir = '';
  let spoolDir = '';
  let logsRoot = '';
  let lokiServer: Server | null = null;
  const lokiReceived: string[] = [];

  beforeEach(async () => {
    root = await mkdtemp(join(tmpdir(), 'vld-e2e-'));
    configDir = join(root, 'config');
    spoolDir = join(root, 'spool');
    logsRoot = join(root, 'logs');
    for (const dir of [configDir, spoolDir, logsRoot]) await mkdir(dir, { recursive: true });
    lokiReceived.length = 0;
  });

  afterEach(async () => {
    if (lokiServer !== null) {
      await new Promise<void>((resolve) => lokiServer?.close(() => resolve()));
      lokiServer = null;
    }
    await rm(root, { recursive: true, force: true });
  });

  async function startLoki(port: number): Promise<void> {
    const instance = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on('data', (chunk: Buffer) => chunks.push(chunk));
      req.on('end', () => {
        void (async () => {
          const raw = Buffer.concat(chunks);
          const text =
            req.headers['content-encoding'] === 'gzip'
              ? (await gunzipAsync(raw)).toString('utf8')
              : raw.toString('utf8');
          lokiReceived.push(text);
          res.writeHead(204);
          res.end();
        })();
      });
    });
    lokiServer = instance;
    await new Promise<void>((resolve) => instance.listen(port, '127.0.0.1', resolve));
  }

  async function writeConfig(lokiPort: number): Promise<void> {
    const base = defaultAppConfig();
    await new ConfigStore(configDir).save(
      {
        ...base,
        drains: [{ id: 'e2e-drain', name: 'e2e', secret: SECRET, enabled: true, createdAt: 1 }],
        sinks: [
          {
            name: 'local',
            enabled: true,
            filter: {},
            maxSpoolBytes: 1_048_576,
            maxBatchEvents: 1000,
            maxBatchBytes: 1_048_576,
            config: {
              type: 'file',
              directory: join(logsRoot, 'local'),
              filePrefix: 'events',
              retentionDays: 0,
              freeSpaceFloorBytes: 0,
            },
          },
          {
            name: 'loki',
            enabled: true,
            filter: {},
            maxSpoolBytes: 1_048_576,
            maxBatchEvents: 1000,
            maxBatchBytes: 1_048_576,
            config: {
              type: 'loki',
              url: `http://127.0.0.1:${String(lokiPort)}`,
              auth: { kind: 'none' },
              tenantId: null,
              labels: { static: { job: 'vercel' }, fromFields: ['level'] },
              timeoutMs: 1000,
            },
          },
        ],
      },
      null,
    );
  }

  function bootService(): Promise<Booted> {
    return boot({
      env: {
        CONFIG_DIR: configDir,
        SPOOL_DIR: spoolDir,
        LOGS_ROOT: logsRoot,
        LOG_LEVEL: 'silent',
        AUTH_MODE: 'disabled',
      },
      webRoot: null,
    });
  }

  it('loses nothing across a sink outage and a hard restart', async () => {
    // Pick a port nothing is listening on yet, so Loki is "down".
    const lokiPort = 45_231;
    await writeConfig(lokiPort);

    // --- Phase 1: Loki is down. Deliveries must still be accepted. ---
    const first = await bootService();

    for (const ids of [['a1', 'a2'], ['a3'], ['a4', 'a5']]) {
      const response = await post(first, ids.map(event));
      expect(response.status).toBe(200);
    }

    // The file sink drains normally even though Loki cannot be reached.
    await waitFor(async () => {
      const files = await readdir(join(logsRoot, 'local')).catch(() => []);
      if (files.length === 0) return false;
      const contents = await readFile(join(logsRoot, 'local', files[0] ?? ''), 'utf8');
      return contents.trimEnd().split('\n').length === 5;
    });

    // Loki's spool still holds the batches, undelivered.
    const spooledBefore = (await readdir(join(spoolDir, 'loki'))).filter((f) =>
      f.endsWith('.jsonl'),
    );
    expect(spooledBefore.length).toBe(3);
    expect(lokiReceived).toEqual([]);

    // --- Phase 2: simulate a crash. No graceful drain. ---
    await first.dispatcher.stop(0);

    // --- Phase 3: Loki comes back; a fresh process must replay the spool. ---
    await startLoki(lokiPort);
    const second = await bootService();

    try {
      await waitFor(async () => {
        const remaining = (await readdir(join(spoolDir, 'loki'))).filter((f) =>
          f.endsWith('.jsonl'),
        );
        return remaining.length === 0;
      });

      const delivered = lokiReceived
        .flatMap((text) => {
          const payload: { streams: { values: [string, string][] }[] } = JSON.parse(text);
          return payload.streams.flatMap((stream) => stream.values);
        })
        .map(([, line]) => {
          const entry: { id: string } = JSON.parse(line);
          return entry.id;
        });

      // Every event arrives, exactly once.
      expect(delivered.toSorted()).toEqual(['a1', 'a2', 'a3', 'a4', 'a5']);

      // And nothing was dead-lettered along the way.
      const dead = await readdir(join(spoolDir, 'loki', 'dead')).catch(() => []);
      expect(dead).toEqual([]);
    } finally {
      await second.shutdown();
    }
  }, 30_000);

  it('keeps accepting deliveries while a sink is wedged, and reports degraded', async () => {
    const lokiPort = 45_232;
    await writeConfig(lokiPort);
    const booted = await bootService();

    try {
      expect((await post(booted, [event('b1')])).status).toBe(200);

      // The dispatcher only reports "failed" (not merely "retrying") after
      // FAILURE_THRESHOLD (5) consecutive failures, and the worker's default
      // exponential backoff between attempts is base=1000ms doubling each
      // time (1s, 2s, 4s, 8s, uncapped by maxBackoffMs=60s at this scale),
      // so reaching the 5th attempt can take ~15-20s of real wall-clock time
      // even with jitter working in our favor. The default waitFor() timeout
      // of 8s is not enough to observe this transition; see task-24-report.md.
      await waitFor(() => Promise.resolve(booted.dispatcher.isDegraded()), 25_000);

      const status = await booted.app.request('/api/status');
      const snapshot = await jsonBody<{
        service: { state: string };
        sinks: { name: string; health: { state: string }; queue: { files: number } }[];
      }>(status);
      expect(snapshot.service.state).toBe('degraded');
      const loki = snapshot.sinks.find((sink) => sink.name === 'loki');
      expect(loki?.health.state).toBe('failed');
      expect(loki?.queue.files).toBeGreaterThan(0);

      expect((await booted.app.request('/readyz')).status).toBe(503);
      expect((await booted.app.request('/healthz')).status).toBe(200);
    } finally {
      await booted.shutdown();
    }
  }, 35_000);
});
