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
import type { AppConfig, SinkEntry } from '../../src/config/schema.js';
import type { AppEnv } from '../../src/server/types.js';
import type { SinkCounters, SinkStatus, StatusSnapshot } from '../../types/api.js';

const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));

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

    const snapshot: StatusSnapshot = JSON.parse(await response.text());
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
    const snapshot: StatusSnapshot = JSON.parse(await (await app().request('/api/status')).text());
    expect(snapshot.service.state).toBe('degraded');
  });

  it("represents one sink's status failure without failing the whole request", async () => {
    // Dispatcher.snapshotSinks() isolates a per-sink stat failure and is
    // unit-tested directly in dispatcher-reconcile.test.ts. This test
    // proves the ROUTE surfaces that isolation end to end: the response is
    // still 200, the failing sink is represented as failed rather than
    // omitted, and the failure never touches shared `metrics` -- which
    // would otherwise leak into /readyz.
    class FlakyDispatcher extends Dispatcher {
      protected override async sinkStatusFor(
        entry: SinkEntry,
        counters: Record<string, SinkCounters>,
      ): Promise<SinkStatus> {
        if (entry.name === 'local') {
          throw new Error('spool directory missing');
        }
        return super.sinkStatusFor(entry, counters);
      }
    }

    const flaky = new FlakyDispatcher({ spoolRoot, logsRoot, metrics, log: silentLog });
    try {
      await flaky.applyConfig(config);

      const deps = {
        getConfig: () => config,
        dispatcher: flaky,
        metrics,
        version: '9.9.9',
        configDir: spoolRoot,
        spoolDir: spoolRoot,
      };
      const instance = new Hono<AppEnv>();
      instance.route('/api/status', statusRoutes(deps));

      const response = await instance.request('/api/status');
      expect(response.status).toBe(200);

      const snapshot: StatusSnapshot = JSON.parse(await response.text());
      expect(snapshot.sinks[0]).toMatchObject({
        name: 'local',
        health: { state: 'failed', lastError: 'spool directory missing' },
        queue: { files: 0, bytes: 0, oldestAgeSec: null },
      });
      expect(metrics.getSinkHealth('local').state).toBe('ok');
    } finally {
      await flaky.stop(500);
    }
  });

  it('always answers healthz with 200', async () => {
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
