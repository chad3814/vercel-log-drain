import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import { SpoolQueue } from '../../src/pipeline/spool.js';
import { backoffDelayMs, SinkWorker } from '../../src/pipeline/dispatcher.js';
import { Metrics } from '../../src/status/metrics.js';
import { AuthDeliveryError, PermanentDeliveryError } from '../../src/sinks/types.js';
import type { LogEvent } from '../../src/vercel/event.js';
import type { Sink } from '../../src/sinks/types.js';

const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));

function event(id: string) {
  return { id, timestamp: 1000, source: 'lambda', projectId: 'p1' };
}

class FakeSink implements Sink {
  readonly type = 'fake';
  readonly received: LogEvent[][] = [];
  constructor(
    readonly name: string,
    private readonly behavior: (attempt: number) => Error | null = () => null,
  ) {}
  private attempts = 0;
  deliver(events: LogEvent[]): Promise<void> {
    this.attempts += 1;
    const error = this.behavior(this.attempts);
    if (error !== null) return Promise.reject(error);
    this.received.push(events);
    return Promise.resolve();
  }
  close(): Promise<void> {
    return Promise.resolve();
  }
}

describe('backoffDelayMs', () => {
  it('grows exponentially from the base', () => {
    expect(backoffDelayMs(1, 1000, 60_000, () => 0)).toBe(1000);
    expect(backoffDelayMs(2, 1000, 60_000, () => 0)).toBe(2000);
    expect(backoffDelayMs(3, 1000, 60_000, () => 0)).toBe(4000);
  });

  it('caps at the maximum', () => {
    expect(backoffDelayMs(30, 1000, 60_000, () => 0)).toBe(60_000);
  });

  it('adds jitter above the base delay', () => {
    const withJitter = backoffDelayMs(1, 1000, 60_000, () => 1);
    expect(withJitter).toBeGreaterThan(1000);
    expect(withJitter).toBeLessThanOrEqual(1300);
  });

  it('never returns less than the base for the first failure', () => {
    expect(backoffDelayMs(0, 1000, 60_000, () => 0)).toBe(1000);
  });
});

describe('SinkWorker', () => {
  let dir = '';
  let metrics: Metrics;

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'vld-worker-'));
    metrics = new Metrics();
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  async function makeWorker(
    sink: Sink,
    maxBatchEvents = 1000,
  ): Promise<{ worker: SinkWorker; queue: SpoolQueue }> {
    const queue = await SpoolQueue.open(dir, { maxSpoolBytes: 1_048_576, freeSpaceFloorBytes: 0 });
    const worker = new SinkWorker({
      sink,
      queue,
      metrics,
      log: silentLog,
      maxBatchEvents,
      maxBatchBytes: 1_048_576,
      baseBackoffMs: 10,
      maxBackoffMs: 40,
      random: () => 0,
    });
    return { worker, queue };
  }

  it('delivers a spooled batch and removes it', async () => {
    const sink = new FakeSink('ok');
    const { worker, queue } = await makeWorker(sink);
    await queue.enqueue([event('a')]);

    expect(await worker.drainOnce()).toBe(true);

    expect(sink.received[0]?.map((e) => e['id'])).toEqual(['a']);
    expect(queue.fileCount()).toBe(0);
    expect(metrics.snapshot().sinkCounters['ok']?.delivered).toBe(1);
    expect(worker.health().state).toBe('ok');
  });

  it('reports nothing to do on an empty queue', async () => {
    const { worker } = await makeWorker(new FakeSink('idle'));
    expect(await worker.drainOnce()).toBe(false);
  });

  it('keeps the batch on a retryable failure and escalates after the threshold', async () => {
    const sink = new FakeSink('flaky', () => new Error('loki down'));
    const { worker, queue } = await makeWorker(sink);
    await queue.enqueue([event('a')]);

    await worker.drainOnce();
    expect(queue.fileCount()).toBe(1);
    expect(worker.health().state).toBe('retrying');
    expect(worker.health().consecutiveFailures).toBe(1);
    expect(worker.health().lastError).toContain('loki down');
    expect(worker.health().nextRetryAt).not.toBeNull();

    for (let index = 0; index < 4; index += 1) await worker.drainOnce();
    expect(worker.health().consecutiveFailures).toBe(5);
    expect(worker.health().state).toBe('failed');
    expect(queue.fileCount()).toBe(1);
  });

  it('treats an unexpected error as retryable, never discarding logs', async () => {
    const sink = new FakeSink('buggy', () => new TypeError('undefined is not a function'));
    const { worker, queue } = await makeWorker(sink);
    await queue.enqueue([event('a')]);

    await worker.drainOnce();

    expect(queue.fileCount()).toBe(1);
    expect(metrics.snapshot().sinkCounters['buggy']?.deadLettered ?? 0).toBe(0);
  });

  it('dead-letters on a permanent failure and advances', async () => {
    const sink = new FakeSink('poison', (attempt) =>
      attempt === 1 ? new PermanentDeliveryError('400 malformed') : null,
    );
    // maxBatchEvents is pinned to 1 so 'bad' and 'good' are drained as two
    // separate batches. With the generous limits used elsewhere in this
    // file, SpoolQueue.nextBatch() (see src/pipeline/spool.ts) merges both
    // enqueued files into a single batch, and the permanent failure on the
    // first delivery attempt would dead-letter 'good' along with 'bad' —
    // which is not what this test is trying to demonstrate.
    const { worker, queue } = await makeWorker(sink, 1);
    await queue.enqueue([event('bad')]);
    await queue.enqueue([event('good')]);

    await worker.drainOnce();
    expect(metrics.snapshot().sinkCounters['poison']?.deadLettered).toBe(1);

    await worker.drainOnce();
    expect(sink.received[0]?.map((e) => e['id'])).toEqual(['good']);
    expect(queue.fileCount()).toBe(0);
  });

  it('escalates health immediately on an auth failure', async () => {
    const sink = new FakeSink('auth', () => new AuthDeliveryError('401 check credentials'));
    const { worker, queue } = await makeWorker(sink);
    await queue.enqueue([event('a')]);

    await worker.drainOnce();

    expect(worker.health().state).toBe('failed');
    expect(worker.health().consecutiveFailures).toBe(1);
    expect(queue.fileCount()).toBe(1);
  });

  it('recovers health after a success', async () => {
    const sink = new FakeSink('recover', (attempt) => (attempt <= 2 ? new Error('down') : null));
    const { worker, queue } = await makeWorker(sink);
    await queue.enqueue([event('a')]);

    await worker.drainOnce();
    await worker.drainOnce();
    expect(worker.health().state).toBe('retrying');

    await worker.drainOnce();
    expect(worker.health().state).toBe('ok');
    expect(worker.health().consecutiveFailures).toBe(0);
    expect(worker.health().lastSuccessAt).not.toBeNull();
  });

  it('drains everything when started, then stops cleanly', async () => {
    const sink = new FakeSink('runner');
    const { worker, queue } = await makeWorker(sink);
    await queue.enqueue([event('a')]);
    await queue.enqueue([event('b')]);

    worker.start();
    await new Promise((resolve) => setTimeout(resolve, 200));
    await worker.stop(1000);

    expect(queue.fileCount()).toBe(0);
    expect(
      sink.received
        .flat()
        .map((e) => e['id'])
        .toSorted(),
    ).toEqual(['a', 'b']);
  });

  it('stop() halts the loop and leaves no timer armed to do more work later', async () => {
    let attempts = 0;
    const sink = new FakeSink('halt', () => {
      attempts += 1;
      return new Error('down');
    });
    const { worker, queue } = await makeWorker(sink);
    await queue.enqueue([event('a')]);

    worker.start();
    // Let a couple of failure/backoff cycles happen (base 10ms, cap 40ms).
    await new Promise((resolve) => setTimeout(resolve, 50));
    await worker.stop(1000);

    const attemptsAtStop = attempts;
    expect(attemptsAtStop).toBeGreaterThan(0);

    // If stop() left a timer armed, or the loop kept running in the
    // background, more delivery attempts would show up here even though
    // stop() already resolved.
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(attempts).toBe(attemptsAtStop);
    expect(queue.fileCount()).toBe(1);
  });
});
