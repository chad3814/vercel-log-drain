import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdir, mkdtemp, readdir, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import { Dispatcher } from '../../src/pipeline/dispatcher.js';
import { defaultAppConfig } from '../../src/config/schema.js';
import { Metrics } from '../../src/status/metrics.js';
import type { AppConfig, SinkEntry } from '../../src/config/schema.js';

const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));

function event(id: string, overrides: Record<string, string> = {}) {
  return { id, timestamp: 1000, source: 'lambda', projectId: 'p1', level: 'info', ...overrides };
}

function configWith(sinks: SinkEntry[]): AppConfig {
  return { ...defaultAppConfig(), sinks };
}

function noFreeSpace(): Promise<number> {
  return Promise.resolve(0);
}

describe('Dispatcher', () => {
  let spoolRoot = '';
  let logsRoot = '';
  let metrics: Metrics;
  let dispatcher: Dispatcher;

  beforeEach(async () => {
    spoolRoot = await mkdtemp(join(tmpdir(), 'vld-spoolroot-'));
    logsRoot = await mkdtemp(join(tmpdir(), 'vld-logsroot-'));
    metrics = new Metrics();
    dispatcher = new Dispatcher({ spoolRoot, logsRoot, metrics, log: silentLog });
  });

  afterEach(async () => {
    await dispatcher.stop(500);
    await rm(spoolRoot, { recursive: true, force: true });
    await rm(logsRoot, { recursive: true, force: true });
  });

  function fileSink(name: string, overrides: Partial<SinkEntry> = {}): SinkEntry {
    return {
      name,
      enabled: true,
      filter: {},
      maxSpoolBytes: 1_048_576,
      maxBatchEvents: 1000,
      maxBatchBytes: 1_048_576,
      config: {
        type: 'file',
        directory: join(logsRoot, name),
        filePrefix: 'events',
        retentionDays: 0,
        freeSpaceFloorBytes: 0,
      },
      ...overrides,
    };
  }

  it('creates a spool directory per enabled sink', async () => {
    await dispatcher.applyConfig(configWith([fileSink('one'), fileSink('two')]));
    expect((await readdir(spoolRoot)).toSorted()).toEqual(['one', 'two']);
  });

  it('routes events only to sinks whose filter matches', async () => {
    await dispatcher.applyConfig(
      configWith([
        fileSink('errors-only', { filter: { minLevel: 'error' } }),
        fileSink('everything'),
      ]),
    );

    await dispatcher.enqueue([event('a', { level: 'info' })]);

    const statuses = await dispatcher.snapshotSinks();
    const errorsOnly = statuses.find((s) => s.name === 'errors-only');
    const everything = statuses.find((s) => s.name === 'everything');
    expect(errorsOnly?.queue.files).toBe(0);
    expect(everything?.queue.files).toBe(1);
  });

  it('does not enqueue to a disabled sink', async () => {
    await dispatcher.applyConfig(configWith([fileSink('off', { enabled: false })]));
    await dispatcher.enqueue([event('a')]);
    const statuses = await dispatcher.snapshotSinks();
    expect(statuses.find((s) => s.name === 'off')?.queue.files).toBe(0);
  });

  it('preserves the spool when a sink is removed, and reports it as orphaned', async () => {
    await dispatcher.applyConfig(configWith([fileSink('temporary')]));
    await dispatcher.enqueue([event('a')]);

    await dispatcher.applyConfig(configWith([]));

    expect(await readdir(spoolRoot)).toContain('temporary');
    const orphans = await dispatcher.listOrphanedSpools();
    expect(orphans.map((o) => o.name)).toEqual(['temporary']);
    expect(orphans[0]?.files).toBe(1);
    expect(orphans[0]?.bytes).toBeGreaterThan(0);
  });

  it('resumes the same spool when a sink setting changes', async () => {
    await dispatcher.applyConfig(configWith([fileSink('keeper', { enabled: false })]));
    await dispatcher.applyConfig(configWith([fileSink('keeper', { enabled: true })]));
    await dispatcher.enqueue([event('a')]);

    // Same name means same queue identity, so nothing is orphaned.
    expect(await dispatcher.listOrphanedSpools()).toEqual([]);
  });

  it('keeps previously spooled data when an enabled sink is reconfigured', async () => {
    // Unlike the toggle test above, this sink is enabled throughout and
    // already has undelivered data on disk before its settings (not its
    // enabled flag) change. applyConfig must reopen the SAME spool
    // directory rather than one whose prior contents are lost.
    await dispatcher.applyConfig(configWith([fileSink('reconfigured')]));
    await dispatcher.enqueue([event('a')]);
    let statuses = await dispatcher.snapshotSinks();
    expect(statuses.find((s) => s.name === 'reconfigured')?.queue.files).toBe(1);

    await dispatcher.applyConfig(
      configWith([fileSink('reconfigured', { maxBatchEvents: 42 })]),
    );

    statuses = await dispatcher.snapshotSinks();
    expect(statuses.find((s) => s.name === 'reconfigured')?.queue.files).toBe(1);
  });

  it('discards an orphaned spool on request', async () => {
    await dispatcher.applyConfig(configWith([fileSink('gone')]));
    await dispatcher.enqueue([event('a')]);
    await dispatcher.applyConfig(configWith([]));

    await dispatcher.discardOrphan('gone');

    expect(await readdir(spoolRoot)).not.toContain('gone');
    expect(await dispatcher.listOrphanedSpools()).toEqual([]);
  });

  it('refuses to discard a name that is not an orphan', async () => {
    await dispatcher.applyConfig(configWith([fileSink('active')]));
    await expect(dispatcher.discardOrphan('active')).rejects.toThrow(/active/);
  });

  it('refuses a traversal name in discardOrphan', async () => {
    await expect(dispatcher.discardOrphan('../..')).rejects.toThrow();
  });

  it('rejects a file sink whose directory escapes the logs root', async () => {
    const escaping = fileSink('escape', {
      config: {
        type: 'file',
        directory: '/etc',
        filePrefix: 'events',
        retentionDays: 0,
        freeSpaceFloorBytes: 0,
      },
    });
    await expect(dispatcher.applyConfig(configWith([escaping]))).rejects.toThrow(/outside/i);
  });

  it('runs a sink test and reports success', async () => {
    await dispatcher.applyConfig(configWith([fileSink('probe')]));
    const result = await dispatcher.testSink('probe');
    expect(result.ok).toBe(true);
    expect(await readdir(join(logsRoot, 'probe'))).toHaveLength(1);
  });

  it('reports a sink test failure without throwing', async () => {
    await dispatcher.applyConfig(
      configWith([
        {
          name: 'bad-loki',
          enabled: true,
          filter: {},
          maxSpoolBytes: 1_048_576,
          maxBatchEvents: 1000,
          maxBatchBytes: 1_048_576,
          config: {
            type: 'loki',
            url: 'http://127.0.0.1:1',
            auth: { kind: 'none' },
            tenantId: null,
            labels: { static: {}, fromFields: [] },
            timeoutMs: 200,
          },
        },
      ]),
    );

    const result = await dispatcher.testSink('bad-loki');
    expect(result.ok).toBe(false);
    expect(result.detail.length).toBeGreaterThan(0);
  });

  it('reports an unknown sink test as a failure', async () => {
    const result = await dispatcher.testSink('nope');
    expect(result.ok).toBe(false);
  });

  it('does not touch the live spool or metrics when testing a sink', async () => {
    // A regression here would be a probe that goes through the real queue
    // (leaving a phantom file) or that reports delivery via metrics
    // (bumping the running worker's counters as a side effect of a
    // one-shot check nobody asked it to run for real).
    await dispatcher.applyConfig(configWith([fileSink('probe-isolated')]));
    await dispatcher.testSink('probe-isolated');

    const statuses = await dispatcher.snapshotSinks();
    const status = statuses.find((s) => s.name === 'probe-isolated');
    expect(status?.queue.files).toBe(0);
    expect(status?.counters.delivered).toBe(0);
  });

  it('reports degraded when a sink health is failed', async () => {
    await dispatcher.applyConfig(configWith([fileSink('ok-sink')]));
    expect(dispatcher.isDegraded()).toBe(false);
    metrics.setSinkHealth('ok-sink', {
      state: 'failed',
      consecutiveFailures: 9,
      lastError: 'x',
      lastErrorAt: 1,
      lastSuccessAt: null,
      nextRetryAt: 2,
    });
    expect(dispatcher.isDegraded()).toBe(true);
  });

  it('forgets metrics for a sink once it leaves the config', async () => {
    await dispatcher.applyConfig(configWith([fileSink('leaving')]));
    metrics.setSinkHealth('leaving', {
      state: 'failed',
      consecutiveFailures: 9,
      lastError: 'x',
      lastErrorAt: 1,
      lastSuccessAt: null,
      nextRetryAt: 2,
    });

    await dispatcher.applyConfig(configWith([]));

    expect(metrics.snapshot().sinkHealth['leaving']).toBeUndefined();
  });

  it('ignores a non-directory entry in the spool root when listing orphans', async () => {
    await writeFile(join(spoolRoot, 'stray-file'), 'x');
    await mkdir(join(spoolRoot, 'orphan-dir'), { recursive: true });
    const orphans = await dispatcher.listOrphanedSpools();
    expect(orphans.map((o) => o.name)).toEqual(['orphan-dir']);
  });

  it('honors the free-space floor from the config being applied, even on the first call', async () => {
    // A regression here looks like: applyConfig uses a stale/absent
    // `this.config` to source the free-space floor while starting new
    // sinks, because the field is only assigned at the very end of
    // applyConfig. On the very first call there is no previous config at
    // all, so the floor silently falls back to 0 and this probe would
    // wrongly report full acceptance.
    const floored = new Dispatcher({
      spoolRoot,
      logsRoot,
      metrics,
      log: silentLog,
      freeSpace: noFreeSpace,
    });
    const config = configWith([fileSink('floor-test')]);
    config.server.spoolFreeSpaceFloorBytes = 1_000_000;

    await floored.applyConfig(config);
    await floored.enqueue([event('a')]);

    const statuses = await floored.snapshotSinks();
    expect(statuses.find((s) => s.name === 'floor-test')?.queue.files).toBe(0);

    await floored.stop(500);
  });

  it('waits for an in-flight delivery to finish before removing a sink', async () => {
    // The other reconciliation tests never call dispatcher.start(), so their
    // workers never run a loop iteration — stop() succeeds trivially whether
    // or not applyConfig actually awaits it. This test drives a real worker
    // through a real (slow) delivery and removes the sink while that
    // delivery is in flight, so a regression that stops awaiting
    // worker.stop() shows up as applyConfig returning almost instantly
    // instead of after the delivery completes.
    let received = 0;
    const DELIVERY_DELAY_MS = 300;
    const server = createServer((req, res) => {
      req.resume();
      req.on('end', () => {
        setTimeout(() => {
          received += 1;
          res.writeHead(204);
          res.end();
        }, DELIVERY_DELAY_MS);
      });
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    if (address === null || typeof address === 'string') {
      throw new Error('expected the test server to bind a TCP port');
    }
    const port = address.port;

    const slowLoki: SinkEntry = {
      name: 'slow-loki',
      enabled: true,
      filter: {},
      maxSpoolBytes: 1_048_576,
      maxBatchEvents: 1000,
      maxBatchBytes: 1_048_576,
      config: {
        type: 'loki',
        url: `http://127.0.0.1:${String(port)}`,
        auth: { kind: 'none' },
        tenantId: null,
        labels: { static: {}, fromFields: [] },
        timeoutMs: 5000,
      },
    };

    try {
      await dispatcher.applyConfig(configWith([slowLoki]));
      await dispatcher.enqueue([event('a')]);
      dispatcher.start();

      // Give the worker time to claim the batch and start the slow request.
      await new Promise((resolve) => setTimeout(resolve, 100));

      const before = Date.now();
      await dispatcher.applyConfig(configWith([]));
      const elapsed = Date.now() - before;

      expect(elapsed).toBeGreaterThanOrEqual(DELIVERY_DELAY_MS - 100);
      expect(received).toBe(1);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});
