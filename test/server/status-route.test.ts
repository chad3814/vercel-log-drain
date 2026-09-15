import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Hono } from 'hono';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import { healthRoutes, statusRoutes } from '../../src/server/routes/status.js';
import { Dispatcher } from '../../src/pipeline/dispatcher.js';
import { Metrics } from '../../src/status/metrics.js';
import { defaultAppConfig } from '../../src/config/schema.js';
import type { AppConfig } from '../../src/config/schema.js';
import type { AppEnv } from '../../src/server/types.js';
import type { SinkHealth, StatusSnapshot } from '../../types/api.js';

const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));

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

describe('status routes', () => {
  let spoolRoot = '';
  let logsRoot = '';
  let dispatcher: Dispatcher;
  let metrics: Metrics;
  let config: AppConfig;

  beforeEach(async () => {
    spoolRoot = await mkdtemp(join(tmpdir(), 'vld-status-spool-'));
    logsRoot = await mkdtemp(join(tmpdir(), 'vld-status-logs-'));
    metrics = new Metrics();
    dispatcher = new Dispatcher({ spoolRoot, logsRoot, metrics, log: silentLog });
    config = {
      ...defaultAppConfig(),
      drains: [{ id: 'd1', name: 'prod', secret: 'x'.repeat(32), enabled: true, createdAt: 1 }],
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
      ],
    };
    await dispatcher.applyConfig(config);
  });

  afterEach(async () => {
    await dispatcher.stop(500);
    await rm(spoolRoot, { recursive: true, force: true });
    await rm(logsRoot, { recursive: true, force: true });
  });

  function app() {
    const deps = {
      getConfig: () => config,
      dispatcher,
      metrics,
      version: '9.9.9',
      configDir: spoolRoot,
      spoolDir: spoolRoot,
    };
    const instance = new Hono<AppEnv>();
    instance.route('/api/status', statusRoutes(deps));
    instance.route('/', healthRoutes(deps));
    return instance;
  }

  it('reports service, volumes, drains, and sinks', async () => {
    const response = await app().request('/api/status');
    expect(response.status).toBe(200);

    const snapshot = await jsonBody<StatusSnapshot>(response);
    expect(snapshot.service.state).toBe('ok');
    expect(snapshot.service.version).toBe('9.9.9');
    expect(snapshot.volumes.spool.totalBytes).toBeGreaterThan(0);
    expect(snapshot.drains[0]).toMatchObject({ id: 'd1', name: 'prod', enabled: true });
    expect(snapshot.sinks[0]).toMatchObject({ name: 'local', type: 'file', enabled: true });
    expect(snapshot.sinks[0]?.queue.oldestAgeSec).toBeNull();
  });

  it('never exposes a drain secret', async () => {
    const body = await (await app().request('/api/status')).text();
    expect(body).not.toContain('x'.repeat(32));
  });

  it('reports degraded when a sink has failed', async () => {
    metrics.setSinkHealth('local', {
      state: 'failed',
      consecutiveFailures: 6,
      lastError: 'loki down',
      lastErrorAt: 1,
      lastSuccessAt: null,
      nextRetryAt: 2,
    });
    expect(await (await app().request('/api/status')).json()).toMatchObject({
      service: { state: 'degraded' },
    });
  });

  it("represents one sink's status failure without failing the whole request", async () => {
    // Dispatcher.snapshotSinks() isolates a per-sink stat failure and is
    // unit-tested directly in dispatcher-reconcile.test.ts, via a Metrics
    // subclass whose getSinkHealth() throws for one sink name -- the same
    // seam snapshotSinks() already calls, no test-only method on
    // Dispatcher required. This test proves the ROUTE surfaces that
    // isolation end to end: the response is still 200, the failing sink is
    // represented as failed rather than omitted, and the failure never
    // touches shared `metrics` -- which would otherwise leak into
    // /readyz.
    class FlakyMetrics extends Metrics {
      override getSinkHealth(sinkName: string): SinkHealth {
        if (sinkName === 'local') {
          throw new Error('spool directory missing');
        }
        return super.getSinkHealth(sinkName);
      }
    }

    const flakyMetrics = new FlakyMetrics();
    const flaky = new Dispatcher({ spoolRoot, logsRoot, metrics: flakyMetrics, log: silentLog });
    try {
      await flaky.applyConfig(config);

      const deps = {
        getConfig: () => config,
        dispatcher: flaky,
        metrics: flakyMetrics,
        version: '9.9.9',
        configDir: spoolRoot,
        spoolDir: spoolRoot,
      };
      const instance = new Hono<AppEnv>();
      instance.route('/api/status', statusRoutes(deps));

      const response = await instance.request('/api/status');
      expect(response.status).toBe(200);

      expect(await response.json()).toMatchObject({
        // isDegraded() reads through the same throwing getSinkHealth(), so
        // a sink whose health cannot be read fails safe to "degraded"
        // rather than silently reporting "ok".
        service: { state: 'degraded' },
        sinks: [
          {
            name: 'local',
            health: { state: 'failed', lastError: 'status unavailable: spool directory missing' },
            queue: { files: 0, bytes: 0, oldestAgeSec: null },
          },
        ],
      });
      // Read straight off the snapshot map, since getSinkHealth('local')
      // on this instance throws by construction above.
      expect(flakyMetrics.snapshot().sinkHealth['local']?.state).toBe('ok');
    } finally {
      await flaky.stop(500);
    }
  });

  it('always answers healthz with 200, even when the service is degraded', async () => {
    // Asserting 200 against a HEALTHY dispatcher cannot tell "unconditional"
    // from "currently agrees with a healthy dispatcher": wiring /healthz to
    // isDegraded() leaves that assertion green. Liveness must not depend on
    // anything that can fail -- a 503 here makes the orchestrator kill a
    // process that is still accepting and spooling deliveries, which is the
    // outage liveness exists to prevent. So degrade the service first and
    // assert it still answers 200.
    expect((await app().request('/healthz')).status).toBe(200);

    metrics.setSinkHealth('local', {
      state: 'failed',
      consecutiveFailures: 9,
      lastError: 'loki unreachable',
      lastErrorAt: Date.now(),
      lastSuccessAt: null,
      nextRetryAt: null,
    });
    expect(dispatcher.isDegraded()).toBe(true);

    // Readiness is allowed to say no here; liveness is not.
    expect((await app().request('/readyz')).status).toBe(503);
    expect((await app().request('/healthz')).status).toBe(200);
  });

  it('answers readyz with 200 when healthy and 503 when degraded', async () => {
    expect((await app().request('/readyz')).status).toBe(200);
    metrics.setSinkHealth('local', {
      state: 'failed',
      consecutiveFailures: 6,
      lastError: 'x',
      lastErrorAt: 1,
      lastSuccessAt: null,
      nextRetryAt: 2,
    });
    expect((await app().request('/readyz')).status).toBe(503);
  });
});
