import { AuthDeliveryError, PermanentDeliveryError } from '../sinks/types.js';
import { initialSinkHealth } from '../status/metrics.js';
import type { Logger } from '../log.js';
import type { Metrics } from '../status/metrics.js';
import type { Sink } from '../sinks/types.js';
import type { SpoolQueue } from './spool.js';
import type { SinkHealth } from '../../types/api.js';

const FAILURE_THRESHOLD = 5;
const IDLE_POLL_MS = 500;
const JITTER_FRACTION = 0.3;

export function backoffDelayMs(
  consecutiveFailures: number,
  baseMs: number,
  capMs: number,
  random: () => number,
): number {
  const exponent = Math.max(0, consecutiveFailures - 1);
  const raw = baseMs * 2 ** exponent;
  const capped = Math.min(raw, capMs);
  const jitter = capped * JITTER_FRACTION * random();
  return Math.min(Math.round(capped + jitter), Math.round(capMs * (1 + JITTER_FRACTION)));
}

export type SinkWorkerOptions = {
  sink: Sink;
  queue: SpoolQueue;
  metrics: Metrics;
  log: Logger;
  maxBatchEvents: number;
  maxBatchBytes: number;
  baseBackoffMs?: number;
  maxBackoffMs?: number;
  random?: () => number;
};

export class SinkWorker {
  private state: SinkHealth = initialSinkHealth();
  private running = false;
  private loop: Promise<void> | null = null;
  // Set only while the run loop is parked in an idle/backoff sleep. stop()
  // calls this to wake the loop immediately instead of waiting out the rest
  // of the delay, so shutdown never leaves a timer armed past the point
  // stop() resolves.
  private wake: (() => void) | null = null;
  // Memoises the in-flight (or completed) shutdown. A boolean guard would let
  // a second concurrent stop() caller return immediately while the first
  // call's shutdown — including sink.close() — was still running, reporting
  // a clean stop that had not actually happened yet. Sharing the same
  // promise means every caller, however many, waits for and observes the
  // same single close().
  private stopping: Promise<void> | null = null;
  private readonly baseBackoffMs: number;
  private readonly maxBackoffMs: number;
  private readonly random: () => number;

  constructor(private readonly options: SinkWorkerOptions) {
    this.baseBackoffMs = options.baseBackoffMs ?? 1000;
    this.maxBackoffMs = options.maxBackoffMs ?? 60_000;
    this.random = options.random ?? Math.random;
    options.metrics.setSinkHealth(options.sink.name, this.state);
  }

  health(): SinkHealth {
    return { ...this.state };
  }

  private publish(): void {
    this.options.metrics.setSinkHealth(this.options.sink.name, this.health());
  }

  private onSuccess(count: number): void {
    this.state = {
      state: 'ok',
      consecutiveFailures: 0,
      lastError: null,
      lastErrorAt: null,
      lastSuccessAt: Date.now(),
      nextRetryAt: null,
    };
    this.options.metrics.recordDelivered(this.options.sink.name, count);
    this.publish();
  }

  private onFailure(error: Error): void {
    const consecutiveFailures = this.state.consecutiveFailures + 1;
    const delay = backoffDelayMs(
      consecutiveFailures,
      this.baseBackoffMs,
      this.maxBackoffMs,
      this.random,
    );
    // An auth failure is surfaced as failed immediately: waiting five rounds
    // to tell the operator their password is wrong wastes their time. It
    // still carries a nextRetryAt because AuthDeliveryError is retryable —
    // once the operator fixes the credential, the worker must resume on its
    // own, not require a restart.
    const escalate = error instanceof AuthDeliveryError;
    this.state = {
      state: escalate || consecutiveFailures >= FAILURE_THRESHOLD ? 'failed' : 'retrying',
      consecutiveFailures,
      lastError: error.message,
      lastErrorAt: Date.now(),
      lastSuccessAt: this.state.lastSuccessAt,
      nextRetryAt: Date.now() + delay,
    };
    this.options.metrics.recordError(this.options.sink.name, error.message);
    this.publish();
  }

  /**
   * Attempts one batch. Returns true if a batch was claimed (delivered,
   * dead-lettered, or failed and left in place), false if the queue was
   * empty. Independent of start()/stop() so retry semantics can be driven
   * deterministically in tests without timers.
   */
  async drainOnce(): Promise<boolean> {
    const batch = await this.options.queue.nextBatch(
      this.options.maxBatchEvents,
      this.options.maxBatchBytes,
    );
    if (batch === null) return false;

    try {
      await this.options.sink.deliver(batch.events);
    } catch (error) {
      const failure = error instanceof Error ? error : new Error(String(error));
      if (failure instanceof PermanentDeliveryError) {
        await this.options.queue.deadLetter(batch);
        this.options.metrics.recordDeadLettered(this.options.sink.name, batch.events.length);
        this.options.log.error(
          { sink: this.options.sink.name, files: batch.files.length, err: failure.message },
          'batch dead-lettered',
        );
        return true;
      }
      // Anything else — a RetryableDeliveryError, an AuthDeliveryError, or an
      // outright bug in the sink — is retryable. The batch stays on disk:
      // at-least-once delivery means we never remove a batch we are not sure
      // was accepted.
      this.onFailure(failure);
      return true;
    }

    await this.options.queue.ack(batch);
    this.onSuccess(batch.events.length);
    return true;
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loop = this.run();
  }

  private async sleep(ms: number): Promise<void> {
    if (ms <= 0) return;
    await new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        this.wake = null;
        resolve();
      }, ms);
      this.wake = () => {
        clearTimeout(timer);
        this.wake = null;
        resolve();
      };
    });
  }

  private async run(): Promise<void> {
    while (this.running) {
      let claimed = false;
      try {
        claimed = await this.drainOnce();
      } catch (error) {
        const failure = error instanceof Error ? error : new Error(String(error));
        this.options.log.error(
          { sink: this.options.sink.name, err: failure.message },
          'worker iteration failed',
        );
        this.onFailure(failure);
      }

      if (!this.running) break;

      const waitMs = !claimed
        ? IDLE_POLL_MS
        : this.state.consecutiveFailures > 0
          ? Math.max(0, (this.state.nextRetryAt ?? 0) - Date.now())
          : 0;
      if (waitMs > 0) await this.sleep(waitMs);
    }
  }

  /**
   * Idempotent, concurrency-safe entry point. However many times, and
   * however many callers, invoke stop() — a signal handler racing a
   * config-reload path, say — the underlying shutdown in stopOnce() runs
   * exactly once, and every caller awaits that same run to completion
   * before returning.
   */
  async stop(deadlineMs: number): Promise<void> {
    this.stopping ??= this.stopOnce(deadlineMs);
    await this.stopping;
  }

  /**
   * Signals the loop to stop, wakes it immediately if it is parked in a
   * sleep, and waits up to `deadlineMs` for the current iteration to finish
   * before giving up. Either way, no timer from this worker remains armed
   * once this resolves: the wake-up clears the sleep's own timer, and the
   * deadline's timer is cleared in the `finally` below.
   *
   * sink.close() is called unconditionally, even when `pending` is null
   * (the worker was constructed but never started, or has already fully
   * stopped) — a worker that never started still owns a sink that must be
   * released, and returning early here would leak it.
   */
  private async stopOnce(deadlineMs: number): Promise<void> {
    this.running = false;
    this.wake?.();
    const pending = this.loop;
    this.loop = null;
    if (pending !== null) {
      let timeoutHandle: NodeJS.Timeout | undefined;
      const timeout = new Promise<void>((resolve) => {
        timeoutHandle = setTimeout(resolve, deadlineMs);
      });
      try {
        await Promise.race([pending, timeout]);
      } finally {
        if (timeoutHandle !== undefined) clearTimeout(timeoutHandle);
      }
    }
    await this.options.sink.close();
  }
}
