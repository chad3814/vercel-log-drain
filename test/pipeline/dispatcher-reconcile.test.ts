import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { chmod, mkdir, mkdtemp, readdir, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import { Dispatcher, NoEnabledSinkError } from '../../src/pipeline/dispatcher.js';
import { SpoolFloorError, SpoolQueue } from '../../src/pipeline/spool.js';
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
    // Paired with an ENABLED sink deliberately. A disabled sink on its own
    // leaves zero enabled sinks, which `enqueue` now refuses outright, and
    // the refusal would satisfy "nothing in the disabled sink's spool" for
    // the wrong reason -- covering the zero-sink guard twice and the
    // per-sink `enabled` check not at all.
    await dispatcher.applyConfig(configWith([fileSink('off', { enabled: false }), fileSink('on')]));
    await dispatcher.enqueue([event('a')]);
    const statuses = await dispatcher.snapshotSinks();
    expect(statuses.find((s) => s.name === 'off')?.queue.files).toBe(0);
    expect(statuses.find((s) => s.name === 'on')?.queue.files).toBe(1);
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

  it("reports a disabled sink's undelivered backlog instead of zeroes", async () => {
    // Measured before this: queue {files: 0, bytes: 0}, orphanedSpools [],
    // and 1 file / 73 B sitting on disk -- an operator "pausing" a sink with
    // a large backlog saw nothing anywhere reporting it, while those bytes
    // still counted against the spool volume and so against the free-space
    // floor. A disabled sink has no SpoolQueue, so the figures have to come
    // off disk.
    await dispatcher.applyConfig(configWith([fileSink('paused')]));
    await dispatcher.enqueue([event('a')]);
    const enabled = (await dispatcher.snapshotSinks()).find((sink) => sink.name === 'paused');
    expect(enabled?.queue.files).toBe(1);
    expect(enabled?.queue.bytes).toBeGreaterThan(0);

    await dispatcher.applyConfig(configWith([fileSink('paused', { enabled: false })]));

    const disabled = (await dispatcher.snapshotSinks()).find((sink) => sink.name === 'paused');
    expect(disabled?.enabled).toBe(false);
    expect(disabled?.queue.files).toBe(1);
    expect(disabled?.queue.bytes).toBe(enabled?.queue.bytes);
    expect(disabled?.queue.oldestAgeSec).not.toBeNull();
  });

  it('reports dead-letter files and bytes per sink and per orphan', async () => {
    // `dead/` appeared in no byte figure: not in queue.bytes, not in
    // orphanedSpools[].bytes, and not against maxSpoolBytes -- so an
    // operator could not see it growing at all, which is what turns a
    // misconfigured sink into the spool volume crossing its floor.
    await dispatcher.applyConfig(configWith([fileSink('letters')]));
    await dispatcher.enqueue([event('a')]);

    // Planted out of band on purpose: no worker is started in these tests,
    // and what is under test here is that the status snapshot READS dead/,
    // not how a batch gets there -- that is covered in spool.test.ts and
    // dispatcher-worker.test.ts.
    const queue = await SpoolQueue.open(join(spoolRoot, 'letters'), {
      maxSpoolBytes: 1_048_576,
      freeSpaceFloorBytes: 0,
    });
    const batch = await queue.nextBatch(1000, 1_048_576);
    await queue.deadLetter(batch!);

    const sink = (await dispatcher.snapshotSinks()).find((entry) => entry.name === 'letters');
    expect(sink?.dead.files).toBe(1);
    expect(sink?.dead.bytes).toBeGreaterThan(0);

    // And once the sink leaves the config, the same bytes are still visible
    // on the orphan the discard button would destroy.
    await dispatcher.applyConfig(configWith([]));
    const orphan = (await dispatcher.listOrphanedSpools()).find(
      (entry) => entry.name === 'letters',
    );
    expect(orphan?.dead.files).toBe(1);
    expect(orphan?.dead.bytes).toBe(sink?.dead.bytes);
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
    // The reconcile was skipped, so there is no config and no enabled sink,
    // and `enqueue` now refuses rather than accepting into nothing. Both
    // halves matter: a resurrected worker would make this RESOLVE (the
    // config having been applied after all) and push the batch count up.
    await expect(dispatcher.enqueue([event('a')])).rejects.toThrow(NoEnabledSinkError);
    expect(await batchCount()).toBe(before);
  });

  it('ignores a config apply that arrives after shutdown has finished', async () => {
    // Distinct from the racing test above, and it pins a different guard.
    // The chain await covers a reconcile queued or in flight AT the moment
    // stop() runs; this covers one arriving AFTER stop() has returned, which
    // only the `stopped` flag rejects. Without the flag a post-shutdown
    // apply -- an admin PUT landing as SIGTERM is handled -- starts workers
    // on a dispatcher that has already shut down.
    const batchCount = async (): Promise<number> => {
      const entries = await readdir(spoolRoot, { recursive: true });
      return entries.filter((entry) => entry.endsWith('.jsonl')).length;
    };

    await dispatcher.stop(500);
    await dispatcher.applyConfig(configWith([fileSink('too-late')]));

    const before = await batchCount();
    // The reconcile was skipped, so there is no config and no enabled sink,
    // and `enqueue` now refuses rather than accepting into nothing. Both
    // halves matter: a resurrected worker would make this RESOLVE (the
    // config having been applied after all) and push the batch count up.
    await expect(dispatcher.enqueue([event('a')])).rejects.toThrow(NoEnabledSinkError);
    expect(await batchCount()).toBe(before);
  });

  it.skipIf(process.getuid?.() === 0)(
    'fails one sink whose spool cannot be opened, not the whole process',
    async () => {
      // SpoolQueue.recover() rethrows anything but ENOENT when it reads
      // dead/, because a sequence counter it could not check against dead/
      // may reissue a name already in there (spec §3.4). That rethrow is
      // right, but letting it reject applyConfig took ingest down for EVERY
      // drain and sink over one directory -- `BOOT FAILED -> EACCES ...
      // scandir` -- and it is reachable through the README's own "clear them
      // by hand" procedure, which can leave a root-owned dead/ behind.
      //
      // Skipped as root, which ignores the mode bits: the spool would open
      // fine and the assertions would be vacuous rather than wrong.
      await mkdir(join(spoolRoot, 'unreadable', 'dead'), { recursive: true });
      await chmod(join(spoolRoot, 'unreadable', 'dead'), 0o000);

      try {
        await dispatcher.applyConfig(configWith([fileSink('unreadable'), fileSink('healthy')]));

        // The healthy sink runs and still accepts deliveries: ingest for
        // every drain survives one broken directory.
        await dispatcher.enqueue([event('a')]);
        const statuses = await dispatcher.snapshotSinks();
        expect(statuses.find((sink) => sink.name === 'healthy')?.queue.files).toBe(1);
        expect(statuses.find((sink) => sink.name === 'healthy')?.health.state).toBe('ok');

        // The broken one is visible, not swallowed: failed health naming
        // what could not be opened, an entry in the error ring, and a
        // degraded service.
        const broken = statuses.find((sink) => sink.name === 'unreadable');
        expect(broken?.health.state).toBe('failed');
        expect(broken?.health.lastError).toMatch(/sink could not be started/);
        expect(broken?.health.lastError).toMatch(/unreadable/);
        expect(dispatcher.isDegraded()).toBe(true);
        expect(metrics.snapshot().recent.errors.some((entry) => entry.scope === 'unreadable')).toBe(
          true,
        );
      } finally {
        await chmod(join(spoolRoot, 'unreadable', 'dead'), 0o755);
      }
    },
  );

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
    // Rejecting is the observable: with the floor silently at 0 the probe is
    // never consulted, the enqueue SUCCEEDS, and both of these flip -- the
    // rejection to a resolve and the file count to 1.
    await expect(floored.enqueue([event('a')])).rejects.toThrow(SpoolFloorError);

    const statuses = await floored.snapshotSinks();
    expect(statuses.find((s) => s.name === 'floor-test')?.queue.files).toBe(0);

    await floored.stop(500);
  });

  it('refuses the delivery and reports it when the spool volume is below its floor', async () => {
    // The backpressure decision (spec §4), from the dispatcher's side. Two
    // things must both hold: the error reaches the caller, so the route can
    // answer 500 and Vercel redelivers; AND the condition is recorded before
    // it leaves, because the status page's whole view of a full volume is
    // built from what an enqueue observed. Recording without propagating was
    // the old behaviour (200 over a batch stored nowhere); propagating
    // without recording would leave /readyz 200 and `recent.errors` empty
    // while every delivery was refused.
    const floored = new Dispatcher({
      spoolRoot,
      logsRoot,
      metrics,
      log: silentLog,
      freeSpace: noFreeSpace,
    });
    const config = configWith([fileSink('floored')]);
    config.server.spoolFreeSpaceFloorBytes = 1_000_000;

    try {
      await floored.applyConfig(config);
      expect(floored.isDegraded()).toBe(false);

      const failure = await floored.enqueue([event('a'), event('b')]).then(
        () => null,
        (error: unknown) => error,
      );
      expect(failure).toBeInstanceOf(SpoolFloorError);

      // Nothing reached disk, and the service says so.
      expect((await floored.snapshotSinks()).find((s) => s.name === 'floored')?.queue.files).toBe(
        0,
      );
      expect(floored.isDegraded()).toBe(true);
      expect(floored.spoolBelowFloor()).toBe(true);
      // Nothing was DROPPED: the delivery was refused, and Vercel still has
      // it. That counter means "shed inside a sink's byte budget", and it
      // used to move for both conditions, which is what made them
      // indistinguishable.
      expect(metrics.snapshot().sinkCounters['floored']?.dropped ?? 0).toBe(0);
      const errors = metrics.snapshot().recent.errors;
      expect(errors).toHaveLength(1);
      expect(errors[0]?.scope).toBe('floored');
      expect(errors[0]?.message).toContain('free-space floor');
    } finally {
      await floored.stop(500);
    }
  });

  it('stops reporting degraded once the spool volume recovers', async () => {
    // The other direction of the same guard. A latch that only ever sets
    // leaves the service `degraded` for the rest of the process's life after
    // one transient dip below the floor, and /readyz never comes back --
    // which is how a correct-looking degraded signal becomes one nobody
    // trusts. Asserting only the set direction cannot tell those apart.
    let freeBytes = 0;
    const flapping = new Dispatcher({
      spoolRoot,
      logsRoot,
      metrics,
      log: silentLog,
      freeSpace: () => Promise.resolve(freeBytes),
    });
    const config = configWith([fileSink('flapping')]);
    config.server.spoolFreeSpaceFloorBytes = 1_000_000;

    try {
      await flapping.applyConfig(config);
      await expect(flapping.enqueue([event('a')])).rejects.toThrow(SpoolFloorError);
      expect(flapping.isDegraded()).toBe(true);

      freeBytes = 50_000_000;
      await flapping.enqueue([event('b')]);

      expect(flapping.isDegraded()).toBe(false);
      expect((await flapping.snapshotSinks()).find((s) => s.name === 'flapping')?.queue.files).toBe(
        1,
      );
    } finally {
      await flapping.stop(500);
    }
  });

  it('keeps reporting degraded when a sink is reconfigured while the volume is full', async () => {
    // The reconcile path had no test at all (`grep belowFloor test/` found
    // nothing), and the teardown cleared the latch for ANY changed entry.
    // Measured: drop, /readyz 503, then an ordinary PUT changing one sink
    // setting -> /readyz 200 and service.state ok, with the volume still
    // full. Recreating a sink against the same spool directory (spec §8.3)
    // does not empty a volume, and nothing re-checks until the next
    // enqueue, so on a quiet drain the page reads green for hours -- while
    // the operator is doing exactly the right thing, having seen `degraded`
    // and gone to lower maxSpoolBytes.
    const floored = new Dispatcher({
      spoolRoot,
      logsRoot,
      metrics,
      log: silentLog,
      freeSpace: noFreeSpace,
    });
    const config = configWith([fileSink('reconfigured-full')]);
    config.server.spoolFreeSpaceFloorBytes = 1_000_000;

    try {
      await floored.applyConfig(config);
      await expect(floored.enqueue([event('a')])).rejects.toThrow(SpoolFloorError);
      expect(floored.isDegraded()).toBe(true);
      expect(floored.spoolBelowFloor()).toBe(true);

      // An ordinary settings change on that same sink: stop, recreate,
      // restart against the same spool directory.
      const changed = configWith([fileSink('reconfigured-full', { maxBatchEvents: 42 })]);
      changed.server.spoolFreeSpaceFloorBytes = 1_000_000;
      await floored.applyConfig(changed);

      expect(floored.spoolBelowFloor()).toBe(true);
      expect(floored.isDegraded()).toBe(true);
    } finally {
      await floored.stop(500);
    }
  });

  it('reaches a running sink when only spoolFreeSpaceFloorBytes changes', async () => {
    // Issue #5: `spoolFreeSpaceFloorBytes` lives on `server`, not on a sink
    // entry, so the per-sink "unchanged" check in reconcileNow never sees it
    // move -- and SpoolQueue.open reads the floor once, at open, not per
    // enqueue. A PUT /config that changes only the floor left every running
    // queue on the old value. Here the sink entry is byte-identical across
    // both applyConfig calls; only the floor moves. Without the fix, the
    // second enqueue still succeeds (old floor of 0, or the queue never
    // reopened) and SpoolQueue.open is called only once.
    let freeBytes = 500_000;
    const live = new Dispatcher({
      spoolRoot,
      logsRoot,
      metrics,
      log: silentLog,
      freeSpace: () => Promise.resolve(freeBytes),
    });
    const config = configWith([fileSink('floor-live')]);
    config.server.spoolFreeSpaceFloorBytes = 0;

    const openSpy = vi.spyOn(SpoolQueue, 'open');
    try {
      await live.applyConfig(config);
      expect(openSpy).toHaveBeenCalledTimes(1);
      await live.enqueue([event('a')]);
      expect(live.spoolBelowFloor()).toBe(false);

      // Same sink entry, unchanged; only the server-wide floor moves, above
      // the free space the probe reports.
      const raised = configWith([fileSink('floor-live')]);
      raised.server.spoolFreeSpaceFloorBytes = 1_000_000;
      await live.applyConfig(raised);

      expect(openSpy).toHaveBeenCalledTimes(2);
      await expect(live.enqueue([event('b')])).rejects.toThrow(SpoolFloorError);
      expect(live.spoolBelowFloor()).toBe(true);
    } finally {
      openSpy.mockRestore();
      await live.stop(500);
    }
  });

  it.each([
    ['removed', (): SinkEntry[] => []],
    ['disabled', (): SinkEntry[] => [fileSink('going-away', { enabled: false })]],
  ])('clears the floor latch when the sink is %s', async (_label, nextSinks) => {
    // The other direction, which the fix above must not break -- the bug was
    // a delete firing too broadly, so pinning only "it stops firing" would
    // trade one wrong answer for another. A latch is cleared exactly when
    // the sink stops being an enqueue destination: removed, or disabled,
    // which spec §8.3 treats together as "stop enqueuing". A flag
    // describing a sink's last enqueue is meaningless for a sink that will
    // receive none, and a latch nothing can clear is its own outage --
    // /readyz would answer 503 for the rest of the process's life, even
    // after an operator freed the volume, because the sink that set it
    // never enqueues again.
    //
    // Asserting spoolBelowFloor() rather than isDegraded(): in the disabled
    // case this leaves zero enabled sinks, which is degraded in its own
    // right, so isDegraded() cannot tell the two reasons apart. Readiness
    // reads this predicate.
    const floored = new Dispatcher({
      spoolRoot,
      logsRoot,
      metrics,
      log: silentLog,
      freeSpace: noFreeSpace,
    });
    const config = configWith([fileSink('going-away')]);
    config.server.spoolFreeSpaceFloorBytes = 1_000_000;

    try {
      await floored.applyConfig(config);
      await expect(floored.enqueue([event('a')])).rejects.toThrow(SpoolFloorError);
      expect(floored.spoolBelowFloor()).toBe(true);

      const next = configWith(nextSinks());
      next.server.spoolFreeSpaceFloorBytes = 1_000_000;
      await floored.applyConfig(next);

      expect(floored.spoolBelowFloor()).toBe(false);
    } finally {
      await floored.stop(500);
    }
  });

  it('reports degraded while no sink is enabled to store anything', async () => {
    // Same silent-loss shape as the floor drop, one step earlier: with no
    // enabled sink the drain route answers 200 for events that are stored
    // nowhere and not even counted as dropped. The first-boot window -- a
    // drain created before any sink -- is exactly this state.
    expect(dispatcher.isDegraded()).toBe(true);

    await dispatcher.applyConfig(configWith([fileSink('only')]));
    expect(dispatcher.isDegraded()).toBe(false);

    await dispatcher.applyConfig(configWith([fileSink('only', { enabled: false })]));
    expect(dispatcher.isDegraded()).toBe(true);
  });

  it.each([
    ['no sink is configured', (): SinkEntry[] => []],
    ['the only sink is disabled', (): SinkEntry[] => [fileSink('off', { enabled: false })]],
  ])('refuses a delivery when %s, and records it', async (_label, sinks) => {
    // Nothing can ever store this batch, so acknowledging it is silent loss
    // (spec §4). It used to log and return, which answered 200. Refusing
    // keeps the events at Vercel, which retries, while an operator enables a
    // sink -- and the recorded error is still the thing that tells them to.
    //
    // Both shapes, because spec §8.3 treats "removed" and "disabled" alike
    // as "stop enqueuing" and only the second one leaves a sink entry behind
    // for the count to get wrong.
    await dispatcher.applyConfig(configWith(sinks()));

    await expect(dispatcher.enqueue([event('a'), event('b')])).rejects.toThrow(NoEnabledSinkError);

    const errors = metrics.snapshot().recent.errors;
    expect(errors).toHaveLength(1);
    expect(errors[0]?.scope).toBe('ingest');
    expect(errors[0]?.message).toContain('no sink is enabled');
  });

  it('still accepts a delivery that no sink filter matches', async () => {
    // The distinction backpressure must not blur. A sink exists and can
    // store events; this delivery simply contains none it wants, which is
    // the operator's INTENT expressed as a filter. Refusing it would make
    // Vercel redeliver forever -- the same events would match nothing on
    // every retry -- so only "no enabled sink at all", above, refuses.
    await dispatcher.applyConfig(
      configWith([fileSink('errors-only', { filter: { minLevel: 'error' } })]),
    );

    await expect(dispatcher.enqueue([event('a', { level: 'info' })])).resolves.toBeUndefined();

    expect((await dispatcher.snapshotSinks())[0]?.queue.files).toBe(0);
    expect(metrics.snapshot().recent.errors).toHaveLength(0);
    expect(dispatcher.isDegraded()).toBe(false);
  });

  it('refuses the whole delivery when one of several sinks is below the floor', async () => {
    // Partial fan-out, and it is ACCEPTED rather than worked around. The
    // spools are written one sink at a time, so 'takes-it' commits before
    // 'refuses-it' is even probed: this throws with the batch already on
    // disk for the first sink, the route answers 500, and Vercel's
    // redelivery hands 'takes-it' a duplicate. That is the at-least-once
    // trade the design already makes (spec §5) -- a duplicate is
    // recoverable, loss is not. The wrong "fix" is to swallow the error and
    // answer 200, which trades a duplicate in one sink for silent loss in
    // the other.
    const mixed = new Dispatcher({
      spoolRoot,
      logsRoot,
      metrics,
      log: silentLog,
      // The probe is called with the sink's own spool directory, which is
      // what lets one volume look full and the other not.
      freeSpace: (path: string) => Promise.resolve(path.endsWith('refuses-it') ? 0 : 50_000_000),
    });
    const config = configWith([fileSink('takes-it'), fileSink('refuses-it')]);
    config.server.spoolFreeSpaceFloorBytes = 1_000_000;

    try {
      await mixed.applyConfig(config);

      await expect(mixed.enqueue([event('a')])).rejects.toThrow(SpoolFloorError);

      const statuses = await mixed.snapshotSinks();
      expect(statuses.find((s) => s.name === 'takes-it')?.queue.files).toBe(1);
      expect(statuses.find((s) => s.name === 'refuses-it')?.queue.files).toBe(0);
      expect(mixed.spoolBelowFloor()).toBe(true);
    } finally {
      await mixed.stop(500);
    }
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
