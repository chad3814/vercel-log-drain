import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mkdir, mkdtemp, readdir, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import { Dispatcher } from '../../src/pipeline/dispatcher.js';
import { SpoolQueue } from '../../src/pipeline/spool.js';
import { defaultAppConfig } from '../../src/config/schema.js';
import { Metrics } from '../../src/status/metrics.js';
import type { AppConfig, SinkEntry } from '../../src/config/schema.js';
import type { SinkHealth } from '../../types/api.js';

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

  it('opens one spool per sink under overlapping applyConfig calls', async () => {
    // Counting opens rather than inspecting settled state: `active` is keyed
    // by sink name, so it holds one entry whichever call won, and a test that
    // asserts on it passes with the reconcile chain removed. Without the
    // chain both calls clear the active.has() check for the same new sink and
    // each opens its own SpoolQueue on the same directory, the second
    // orphaning the first worker, which keeps draining that spool untracked.
    const openSpy = vi.spyOn(SpoolQueue, 'open');
    try {
      await Promise.all([
        dispatcher.applyConfig(configWith([fileSink('shared')])),
        dispatcher.applyConfig(configWith([fileSink('shared')])),
      ]);

      expect(openSpy).toHaveBeenCalledTimes(1);
    } finally {
      openSpy.mockRestore();
    }
  });

  it('keeps a disabled sink out of the orphan list and preserves its spool', async () => {
    // This test previously toggled enabled with an EMPTY spool and asserted
    // only that nothing was orphaned, so it passed even when the queue was
    // wiped on every applyConfig. Enqueue first, so the disable/enable cycle
    // has something to lose. A disabled sink is still configured and so is
    // never an orphan -- classifying it as one would offer an operator's
    // undelivered data to discardOrphan.
    await dispatcher.applyConfig(configWith([fileSink('keeper')]));
    await dispatcher.enqueue([event('a')]);
    const filesFor = async (name: string): Promise<number | undefined> =>
      (await dispatcher.snapshotSinks()).find((sink) => sink.name === name)?.queue.files;
    expect(await filesFor('keeper')).toBe(1);

    await dispatcher.applyConfig(configWith([fileSink('keeper', { enabled: false })]));
    expect(await dispatcher.listOrphanedSpools()).toEqual([]);

    await dispatcher.applyConfig(configWith([fileSink('keeper', { enabled: true })]));
    expect(await dispatcher.listOrphanedSpools()).toEqual([]);
    expect(await filesFor('keeper')).toBe(1);
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

    await dispatcher.applyConfig(configWith([fileSink('reconfigured', { maxBatchEvents: 42 })]));

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

  it("isolates one sink's status failure from the rest of the snapshot", async () => {
    // snapshotSinks() already calls metrics.getSinkHealth() inside its own
    // try/catch, and `metrics` is an injected constructor dependency --
    // no test-only seam on Dispatcher is needed to reach that failure.
    // Overriding getSinkHealth() to throw for exactly one sink name, while
    // delegating to the real implementation via `super` for the other,
    // drives the actual production try/catch in snapshotSinks() rather
    // than asserting against a hand-built fixture that never touches that
    // code path.
    class FlakyMetrics extends Metrics {
      override getSinkHealth(sinkName: string): SinkHealth {
        if (sinkName === 'broken') {
          throw new Error('stat failed: permission denied');
        }
        return super.getSinkHealth(sinkName);
      }
    }

    const flakyMetrics = new FlakyMetrics();
    const flaky = new Dispatcher({ spoolRoot, logsRoot, metrics: flakyMetrics, log: silentLog });
    try {
      await flaky.applyConfig(configWith([fileSink('healthy'), fileSink('broken')]));
      await flaky.enqueue([event('a')]);

      const statuses = await flaky.snapshotSinks();

      const healthy = statuses.find((s) => s.name === 'healthy');
      expect(healthy?.queue.files).toBe(1);
      expect(healthy?.health.state).toBe('ok');

      const broken = statuses.find((s) => s.name === 'broken');
      expect(broken?.health.state).toBe('failed');
      expect(broken?.health.lastError).toBe('status unavailable: stat failed: permission denied');
      expect(broken?.queue).toEqual({ files: 0, bytes: 0, oldestAgeSec: null });

      // The synthesized failure must never leak into shared metrics: the
      // status route this feeds is read-only, and readyz/isDegraded() must
      // keep reflecting real sink health, not a transient stat error. Read
      // straight off the snapshot map, since getSinkHealth('broken') on
      // this instance throws by construction above.
      expect(flakyMetrics.snapshot().sinkHealth['broken']?.state).toBe('ok');
    } finally {
      await flaky.stop(500);
    }
  });

  it('does not let a config apply racing shutdown resurrect a worker', async () => {
    // Without the `stopped` gate and the chain await in stop(), the queued
    // reconcile repopulates `active` AFTER stop() clears it, leaving a worker
    // that was never started and that enqueue() would still write to -- a
    // spool nobody drains. `active` is private, so the observable is whether
    // a batch lands on disk after shutdown.
    const batchCount = async (): Promise<number> => {
      const entries = await readdir(spoolRoot, { recursive: true });
      return entries.filter((entry) => entry.endsWith('.jsonl')).length;
    };

    const pending = dispatcher.applyConfig(configWith([fileSink('late')]));
    await dispatcher.stop(500);
    await pending;

    const before = await batchCount();
    await dispatcher.enqueue([event('a')]);
    expect(await batchCount()).toBe(before);
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

  it('fails safe to degraded when a sink health cannot even be read', async () => {
    // isDegraded() backs both /api/status's service.state and /readyz, so
    // a getSinkHealth() that throws (the same seam the snapshotSinks()
    // isolation test drives) must not crash it -- and reporting healthy
    // when the health is literally unreadable would be a worse answer
    // than reporting degraded.
    class FlakyMetrics extends Metrics {
      override getSinkHealth(sinkName: string): SinkHealth {
        if (sinkName === 'unreadable') {
          throw new Error('stat failed: permission denied');
        }
        return super.getSinkHealth(sinkName);
      }
    }

    const flakyMetrics = new FlakyMetrics();
    const flaky = new Dispatcher({
      spoolRoot,
      logsRoot,
      metrics: flakyMetrics,
      log: silentLog,
    });
    try {
      await flaky.applyConfig(configWith([fileSink('unreadable')]));
      expect(flaky.isDegraded()).toBe(true);
    } finally {
      await flaky.stop(500);
    }
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
    let notifyRequestArrived: (() => void) | null = null;
    const requestArrived = new Promise<void>((resolve) => {
      notifyRequestArrived = resolve;
    });
    const DELIVERY_DELAY_MS = 300;
    const server = createServer((req, res) => {
      req.resume();
      req.on('end', () => {
        // Resolved as soon as the request body is in, before the artificial
        // delay below — awaited so the test removes the sink exactly once
        // delivery is genuinely in flight, instead of guessing with a fixed
        // sleep that could in principle fire before the worker has even
        // claimed the batch under scheduler pressure.
        notifyRequestArrived?.();
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

      // Wait until the worker has actually claimed the batch and its
      // request has arrived at the server, rather than guessing with a
      // fixed sleep. Bounded so a genuine failure to deliver times out
      // instead of hanging the test.
      const timedOut = Symbol('timed out waiting for the worker to start its delivery');
      const outcome = await Promise.race([
        requestArrived,
        new Promise<typeof timedOut>((resolve) => setTimeout(() => resolve(timedOut), 2000)),
      ]);
      if (outcome === timedOut) {
        throw new Error('timed out waiting for the worker to start its delivery');
      }

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
