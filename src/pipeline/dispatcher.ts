import { mkdir, readdir, rm, stat } from 'node:fs/promises';
import { join } from 'node:path';
import { compileFilter } from './filter.js';
import { readSpoolDirStats, SpoolFloorError, SpoolQueue } from './spool.js';
import { createSink } from '../sinks/registry.js';
import { resolveLogsDirectory } from '../sinks/file.js';
import { SINK_NAME_PATTERN } from '../config/schema.js';
import { EscalatingDeliveryError, PermanentDeliveryError } from '../sinks/types.js';
import { initialSinkHealth } from '../status/metrics.js';
import type { AppConfig, SinkEntry } from '../config/schema.js';
import type { DeadStats, EnqueueResult } from './spool.js';
import type { EventPredicate } from './filter.js';
import type { Logger } from '../log.js';
import type { Metrics } from '../status/metrics.js';
import type { FreeSpaceProbe, Sink } from '../sinks/types.js';
import type { LogEvent } from '../vercel/event.js';
import type { OrphanedSpool, SinkHealth, SinkStatus } from '../../types/api.js';

function emptyDeadStats(): DeadStats {
  return { files: 0, bytes: 0 };
}

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
    // A failure the operator has to fix -- a wrong password, a body limit
    // below the sink's maxBatchBytes -- is surfaced as failed immediately:
    // waiting five rounds to tell them wastes their time. It still carries a
    // nextRetryAt because these errors are retryable -- once the
    // configuration is corrected, the worker must resume on its own, not
    // require a restart.
    const escalate = error instanceof EscalatingDeliveryError;
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
        // Also onto the error ring, so a sink quietly dead-lettering every
        // batch shows up on the status page. A dead-letter is not a delivery
        // FAILURE -- health stays as it was, because the queue did advance --
        // so before this the only surface was `counters.deadLettered`, a
        // number an operator would have to already be watching.
        this.options.metrics.recordError(
          this.options.sink.name,
          `batch dead-lettered to dead/: ${failure.message}`,
        );
        this.options.log.error(
          { sink: this.options.sink.name, files: batch.files.length, err: failure.message },
          'batch dead-lettered',
        );
        return true;
      }
      // Anything else — a RetryableDeliveryError, an escalating one, or an
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

export type TestSinkResult = { ok: boolean; detail: string };

/**
 * Thrown by `spoolDirFor` for a name that does not match `SINK_NAME_PATTERN`
 * -- syntactically invalid, never a real sink or orphan regardless of what
 * exists on disk. Kept distinct from "not an orphan" (a plain `Error` from
 * `discardOrphan`) so a caller like the admin route can answer 400 for one
 * and 404 for the other, rather than collapsing both guards into one
 * observable outcome.
 */
export class InvalidSinkNameError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidSinkNameError';
  }
}

/**
 * A delivery arrived and no sink is enabled to store it, so it is refused
 * rather than acknowledged.
 *
 * Same reasoning as `SpoolFloorError`, one step earlier: nothing can ever
 * store this batch, and answering 200 for it is silent loss. Refusing makes
 * Vercel hold the events and redeliver while an operator enables a sink.
 * Distinct from a filter matching nothing, which is the operator's intent and
 * is still acknowledged.
 */
export class NoEnabledSinkError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NoEnabledSinkError';
  }
}

export type DispatcherOptions = {
  spoolRoot: string;
  logsRoot: string;
  metrics: Metrics;
  log: Logger;
  freeSpace?: FreeSpaceProbe;
  /**
   * Retry cadence for every worker this dispatcher starts. Operational, not
   * merely a test seam: how hard to retry a sink that has been down for
   * hours is a deployment decision, and the defaults (1s doubling to 60s)
   * suit a brief outage rather than a long one. Threading them through also
   * lets the end-to-end durability test reach a `failed` sink in
   * milliseconds instead of the ~18s five jittered backoffs take at the
   * production defaults -- that one test otherwise accounted for 18s of a
   * 19s suite, which is how a durability test ends up skipped.
   */
  baseBackoffMs?: number;
  maxBackoffMs?: number;
};

type ActiveSink = {
  entry: SinkEntry;
  predicate: EventPredicate;
  queue: SpoolQueue;
  worker: SinkWorker;
};

export class Dispatcher {
  private active = new Map<string, ActiveSink>();
  private config: AppConfig | null = null;
  private started = false;
  /**
   * Reconciliation runs one at a time. `applyConfig` awaits worker shutdown
   * and spool opening, so two concurrent calls can interleave across those
   * awaits: both pass the `active.has()` check for the same new sink, both
   * open a SpoolQueue on the same directory, and the second `active.set()`
   * orphans the first worker, which keeps running untracked against that
   * directory. Task 22 calls this straight from an HTTP handler, so two
   * overlapping PUTs are an ordinary occurrence rather than a rare race.
   * The guarantee belongs here, in the component that owns the state, not in
   * every caller. Same chain pattern as ConfigStore.save.
   *
   * Tested by counting `SpoolQueue.open` invocations, not by inspecting
   * settled state. An earlier attempt asserted on `active` after both calls
   * resolved and passed 20/20 with the chain removed: the map is keyed by
   * sink name, so there is exactly one entry whichever call won. The number
   * of spools opened for one directory is the thing that actually differs.
   */
  private reconcileChain: Promise<void> = Promise.resolve();
  /** Set by `stop()`; makes any later or still-queued reconcile a no-op. */
  private stopped = false;
  /**
   * Sinks whose last enqueue was REFUSED because the spool volume is below
   * `spoolFreeSpaceFloorBytes`. Held here rather than in sink HEALTH, which
   * describes delivery: a sink at the floor may be delivering perfectly, and
   * conflating the two would report "the sink is broken" for a problem that
   * is the volume's. `isDegraded()` reads it, so /readyz and
   * `service.state` react to a full spool volume as spec §10 requires.
   */
  private belowFloor = new Set<string>();

  constructor(private readonly options: DispatcherOptions) {}

  private spoolDirFor(name: string): string {
    if (!SINK_NAME_PATTERN.test(name)) {
      throw new InvalidSinkNameError(`invalid sink name "${name}"`);
    }
    return join(this.options.spoolRoot, name);
  }

  /**
   * Normalizes a sink entry, resolving and containing a file sink's directory.
   * Throws before anything is created so an invalid config cannot half-apply.
   */
  private async normalize(entry: SinkEntry): Promise<SinkEntry> {
    if (entry.config.type !== 'file') return entry;
    const directory = await resolveLogsDirectory(entry.config.directory, this.options.logsRoot);
    return { ...entry, config: { ...entry.config, directory } };
  }

  async applyConfig(config: AppConfig): Promise<void> {
    // `.then(work, work)` so a rejected reconciliation does not wedge every
    // later one; the chain is kept alive below whatever the outcome.
    const run = this.reconcileChain.then(
      () => this.reconcileNow(config),
      () => this.reconcileNow(config),
    );
    this.reconcileChain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private async reconcileNow(config: AppConfig): Promise<void> {
    if (this.stopped) {
      this.options.log.warn('ignoring a config apply that arrived after shutdown');
      return;
    }
    // Awaited as a whole before anything below runs: `normalize` resolves a
    // file sink's directory with `realpath`, which is I/O, and it is still
    // the only step allowed to reject the config -- so it must finish, and
    // reject, before a single worker has been stopped or started.
    const normalized = await Promise.all(config.sinks.map((entry) => this.normalize(entry)));
    const desired = new Map(normalized.map((entry) => [entry.name, entry]));

    // `spoolFreeSpaceFloorBytes` lives on `server`, not on a sink entry, so
    // it is invisible to the per-sink `unchanged` check below -- a sink
    // entry can be byte-identical while the floor its queue was OPENED with
    // (SpoolQueue.open reads it once, not per enqueue; see startSink) is
    // stale. Recording the change here, before `this.config` is overwritten,
    // is what makes every active sink recreate below even though nothing in
    // ITS entry moved. Read per-enqueue instead was the other option; kept
    // the floor read at open, on the hot path, to widen "changed" instead --
    // deliberately, and only for this field. `maxBodyBytes` and
    // `maxDecompressedBytes` also live on `server` but affect ingest, not a
    // running SpoolQueue, so tying recreation to the whole `server` block
    // would restart every sink -- dropping the in-flight batch, per
    // SinkWorker.stop's own wording -- over settings that do not touch it.
    const floorChanged =
      this.config !== null &&
      this.config.server.spoolFreeSpaceFloorBytes !== config.server.spoolFreeSpaceFloorBytes;

    // Committed to applying from here on: `normalize()` above is the only
    // step that can reject the whole config, and it already has. Setting
    // `this.config` now — before any worker is stopped or started — means
    // `startSink()` below sources the free-space floor and any other
    // server-wide setting from the config actually being applied, not from
    // whatever was left over from the previous call (or nothing, on the
    // very first call, where there would be no previous config at all).
    this.config = { ...config, sinks: normalized };

    for (const [name, current] of this.active) {
      const next = desired.get(name);
      const unchanged =
        !floorChanged &&
        next !== undefined &&
        JSON.stringify(next) === JSON.stringify(current.entry);
      if (unchanged) continue;
      await current.worker.stop(5000);
      this.active.delete(name);
      // The floor latch survives a RECONFIGURE, and must. Recreating a sink
      // against the same spool directory (spec §8.3, "settings changed")
      // does not empty a full volume, and clearing it here reported
      // `service.state: ok` and /readyz 200 over a volume that was still
      // refusing every delivery -- measured: refusal, /readyz 503, then an
      // ordinary PUT changing one
      // setting, and the page went green with the volume still full. Nothing
      // re-checks until the next enqueue, so on a quiet drain that reads
      // green for hours and a load balancer returns the instance to
      // rotation on it. Worse, it happens while the operator is doing
      // exactly the right thing: they saw `degraded` and went to lower
      // maxSpoolBytes or disable the noisy sink. Now that a full volume
      // refuses ingest outright, a false green is worse still: it says the
      // service is taking traffic it is in fact answering 500 for.
      //
      // It is cleared only when this sink stops being an enqueue
      // destination -- removed, or disabled, which §8.3 treats together as
      // "stop enqueuing". A flag describing a sink's last enqueue is
      // meaningless for a sink that will receive none, and a latch nothing
      // can clear is its own outage: /readyz would answer 503 for the rest
      // of the process's life, unclearable even after an operator freed the
      // volume, because the sink that set it never enqueues again. Any
      // other still-enabled sink re-detects a full volume on its very next
      // delivery, and a sole remaining disabled sink leaves
      // `enabledSinkCount() === 0`, which is degraded on its own.
      if (next === undefined || !next.enabled) {
        this.belowFloor.delete(name);
      }
      if (next === undefined) {
        // Deliberately leaves the spool directory on disk.
        this.options.metrics.forgetSink(name);
      }
    }

    for (const entry of normalized) {
      if (this.active.has(entry.name)) continue;
      if (!entry.enabled) continue;
      try {
        await this.startSink(entry);
      } catch (error: unknown) {
        // One sink that cannot be started must not take the process down.
        // Same isolation as snapshotSinks(): this sink reports `failed` with
        // a message naming what could not be opened, every other sink keeps
        // running, and ingest keeps working for every drain.
        //
        // The case that forced this: SpoolQueue.recover() rethrows anything
        // but ENOENT when it reads `dead/`, because a sequence counter it
        // could not verify against dead/ may reissue a name already there
        // (spec §3.4). Rethrowing was right; letting it reject applyConfig
        // was not -- `BOOT FAILED -> EACCES ... scandir` took ingest down
        // for every drain and sink over one directory, and it is reachable
        // through the README's own "clear them by hand" procedure, which can
        // leave a root-owned dead/ behind.
        //
        // Not swallowed: the health entry and the error ring both carry the
        // message, and `failed` health degrades the service. This is
        // visible without being fatal, which is the whole point.
        const message = error instanceof Error ? error.message : 'unknown error';
        this.options.log.error(
          { sink: entry.name, err: message },
          'sink could not be started; leaving it failed and continuing',
        );
        this.options.metrics.setSinkHealth(entry.name, {
          state: 'failed',
          // Zero, because no delivery attempt failed -- the sink never
          // started. The prefix on lastError says so, since `failed` with no
          // consecutive failures would otherwise read as "just started
          // failing".
          consecutiveFailures: 0,
          lastError: `sink could not be started: ${message}`,
          lastErrorAt: Date.now(),
          lastSuccessAt: null,
          nextRetryAt: null,
        });
        this.options.metrics.recordError(entry.name, `sink could not be started: ${message}`);
      }
    }
  }

  private async startSink(entry: SinkEntry): Promise<void> {
    const dir = this.spoolDirFor(entry.name);
    await mkdir(dir, { recursive: true });

    const queue = await SpoolQueue.open(dir, {
      maxSpoolBytes: entry.maxSpoolBytes,
      freeSpaceFloorBytes: this.config?.server.spoolFreeSpaceFloorBytes ?? 0,
      log: this.options.log,
      ...(this.options.freeSpace === undefined ? {} : { freeSpace: this.options.freeSpace }),
    });

    const sink = createSink(entry.name, entry.config, {
      log: this.options.log,
      ...(this.options.freeSpace === undefined ? {} : { freeSpace: this.options.freeSpace }),
    });

    // Conditional spread, not `baseBackoffMs: this.options.baseBackoffMs`:
    // `exactOptionalPropertyTypes` is on, so an explicit `undefined` is not
    // assignable to an optional property.
    const worker = new SinkWorker({
      sink,
      queue,
      metrics: this.options.metrics,
      log: this.options.log,
      maxBatchEvents: entry.maxBatchEvents,
      maxBatchBytes: entry.maxBatchBytes,
      ...(this.options.baseBackoffMs === undefined
        ? {}
        : { baseBackoffMs: this.options.baseBackoffMs }),
      ...(this.options.maxBackoffMs === undefined
        ? {}
        : { maxBackoffMs: this.options.maxBackoffMs }),
    });

    this.active.set(entry.name, {
      entry,
      predicate: compileFilter(entry.filter),
      queue,
      worker,
    });
    if (this.started) worker.start();
  }

  /**
   * Spool a delivery to every sink whose filter matches it.
   *
   * Throws if the delivery could not be stored, and the caller must fail the
   * request on that: the drain route answers 500 and Vercel redelivers. This
   * is what makes "no acknowledged delivery is ever lost" hold without
   * qualification, and it is the whole of the backpressure decision recorded
   * in spec §4.
   *
   * The fan-out is sequential and NOT atomic. If sink A commits and sink B
   * then refuses, this throws with A's copy already on disk, and the
   * redelivery gives A a duplicate. That is correct and accepted: delivery is
   * at-least-once by design (spec §5), a duplicate is recoverable and loss is
   * not, and Loki collapses identical entries anyway. Do NOT "fix" it by
   * catching here and answering 200 -- that trades a duplicate in A for
   * silent loss in B, which is the behaviour this replaced.
   */
  async enqueue(events: LogEvent[]): Promise<void> {
    if (events.length === 0) return;

    // Nowhere to put them, and no redelivery of THIS batch can change that --
    // but refusing it keeps the events at Vercel, which retries, instead of
    // acknowledging them into nothing. This used to log and return, so the
    // route answered 200 and the batch was stored nowhere. `isDegraded()`
    // reports the state independently of whether a delivery has arrived, so a
    // first-boot deployment with no sink yet is visible before the first
    // refusal rather than after.
    if (this.enabledSinkCount() === 0) {
      const message = `${String(events.length)} events were refused because no sink is enabled to store them`;
      this.options.metrics.recordError('ingest', message);
      this.options.log.error({ events: events.length }, message);
      throw new NoEnabledSinkError(message);
    }

    for (const active of this.active.values()) {
      if (!active.entry.enabled) continue;
      // No event matching this sink's filter is the operator's INTENT, not a
      // failure: the sink is skipped and the delivery is still acknowledged.
      // Only "no enabled sink at all", above, refuses.
      const matching = events.filter((event) => active.predicate(event));
      if (matching.length === 0) continue;

      let result: EnqueueResult;
      try {
        result = await active.queue.enqueue(matching);
      } catch (error) {
        // Record before propagating. The status page's view of a full volume
        // is built entirely from what an enqueue observed, so losing these
        // three lines would leave /readyz 200 and `recent.errors` empty while
        // every delivery was being refused.
        if (error instanceof SpoolFloorError) this.noteBelowFloor(active.entry.name, error);
        throw error;
      }

      // A write got through, so whatever the volume looked like last time, it
      // is not refusing this sink now. Cleared in both directions on purpose:
      // a latch that only ever sets leaves the service reporting `degraded`
      // for the rest of the process's life after one transient dip below the
      // floor.
      this.belowFloor.delete(active.entry.name);
      if (result.droppedEvents > 0) {
        // Drop-oldest inside this sink's `maxSpoolBytes`, which is a bounded
        // buffer doing its job and NOT the volume-level condition above: the
        // incoming batch was committed. Deliberately still shedding here --
        // only the volume free-space floor applies backpressure.
        this.options.metrics.recordDropped(active.entry.name, result.droppedEvents);
        this.options.log.warn(
          { sink: active.entry.name, dropped: result.droppedEvents },
          'spool overflow dropped oldest batches',
        );
      }
    }
  }

  /**
   * Everything an operator reads about a refused delivery, recorded before the
   * error leaves `enqueue`.
   *
   * Three surfaces have to move, because an operator reading any one of them
   * must see it: the floor latch (so `degraded`, and therefore /readyz 503,
   * spec §10), a `recent.errors` entry naming the sink and the bytes free, and
   * the log line. Scoped to the SINK rather than to `ingest` -- the drain
   * route records an `ingest` entry of its own for the refusal, and the useful
   * thing this one adds is which sink's volume it was.
   *
   * Recorded per refusal rather than per transition: while this condition
   * holds it deserves to crowd the error ring, because the service is
   * answering 500 to everything Vercel sends and there is no worse state for
   * it to be in.
   *
   * `counters.dropped` deliberately does NOT move: nothing was dropped. That
   * counter means "shed inside a sink's byte budget", and letting a refusal
   * move it too is what made the two indistinguishable before.
   */
  private noteBelowFloor(sink: string, error: SpoolFloorError): void {
    this.belowFloor.add(sink);
    const message = `${error.message}: refusing the delivery so Vercel redelivers it`;
    this.options.metrics.recordError(sink, message);
    this.options.log.error(
      { sink, freeBytes: error.freeBytes, floorBytes: error.floorBytes },
      message,
    );
  }

  private enabledSinkCount(): number {
    return (this.config?.sinks ?? []).filter((entry) => entry.enabled).length;
  }

  start(): void {
    this.started = true;
    for (const active of this.active.values()) active.worker.start();
  }

  async stop(deadlineMs: number): Promise<void> {
    // Close the door before draining the room. Setting `stopped` first makes
    // any reconcile still queued on the chain a no-op, and awaiting the chain
    // lets one already in flight finish -- otherwise it repopulates `active`
    // AFTER this method has cleared it, leaving an orphaned worker that was
    // never started and that `enqueue` would still write to. A config reload
    // racing a SIGTERM is ordinary, not a rare interleaving.
    this.stopped = true;
    await this.reconcileChain.catch(() => undefined);

    this.started = false;
    await Promise.all([...this.active.values()].map((active) => active.worker.stop(deadlineMs)));
    this.active.clear();
  }

  async listOrphanedSpools(): Promise<OrphanedSpool[]> {
    let names: string[];
    try {
      names = await readdir(this.options.spoolRoot);
    } catch {
      return [];
    }

    const configured = new Set((this.config?.sinks ?? []).map((entry) => entry.name));
    const orphans: OrphanedSpool[] = [];

    for (const name of names) {
      if (configured.has(name)) continue;
      const dir = join(this.options.spoolRoot, name);
      const stats = await stat(dir).catch(() => null);
      if (stats === null || !stats.isDirectory()) continue;

      // Includes `dead/`, separately: a discard deletes the directory whole,
      // so reporting only the live batches understated what the button on
      // the status page destroys.
      const contents = await readSpoolDirStats(dir);
      orphans.push({
        name,
        files: contents.files,
        bytes: contents.bytes,
        dead: contents.dead,
      });
    }
    return orphans;
  }

  async discardOrphan(name: string): Promise<void> {
    // Pattern-checked before the orphan-membership lookup, and unconditionally
    // -- not only when a matching directory happens to exist -- so a
    // syntactically invalid name (InvalidSinkNameError, 400) and a
    // well-formed name that simply is not an orphan (plain Error, 404) stay
    // distinguishable outcomes rather than both collapsing into "not found".
    const dir = this.spoolDirFor(name);
    const orphans = await this.listOrphanedSpools();
    if (!orphans.some((orphan) => orphan.name === name)) {
      throw new Error(`"${name}" is not an orphaned spool directory`);
    }
    await rm(dir, { recursive: true, force: true });
  }

  async testSink(name: string): Promise<TestSinkResult> {
    const active = this.active.get(name);
    if (active === undefined) {
      return { ok: false, detail: `sink "${name}" is not running; enable and save it first` };
    }
    const probe: LogEvent = {
      id: `test-${String(Date.now())}`,
      timestamp: Date.now(),
      source: 'external',
      projectId: 'vercel-log-drain',
      level: 'info',
      message: `test event from vercel-log-drain for sink ${name}`,
    };
    const sink = createSink(active.entry.name, active.entry.config, {
      log: this.options.log,
      ...(this.options.freeSpace === undefined ? {} : { freeSpace: this.options.freeSpace }),
    });
    try {
      await sink.deliver([probe]);
      return { ok: true, detail: 'test event accepted' };
    } catch (error) {
      return { ok: false, detail: error instanceof Error ? error.message : String(error) };
    } finally {
      await sink.close();
    }
  }

  async snapshotSinks(): Promise<SinkStatus[]> {
    const counters = this.options.metrics.snapshot().sinkCounters;
    const statuses: SinkStatus[] = [];

    for (const entry of this.config?.sinks ?? []) {
      // The try wraps only THIS entry's work, not the loop: wrapping the
      // loop would lose every sink to one failure, which is the exact bug
      // this isolation exists to avoid.
      try {
        const active = this.active.get(entry.name);
        // A disabled (or otherwise not-running) sink has no SpoolQueue, and
        // reporting the absent queue's zeroes made its entire undelivered
        // backlog disappear from every surface at once: `queue {files: 0,
        // bytes: 0}` here, and filtered out of `listOrphanedSpools` because
        // it is still configured. An operator "pausing" a sink with 400 MB
        // spooled saw nothing, anywhere, reporting those 400 MB -- while
        // they still counted against the spool volume and so against the
        // free-space floor. Read them off disk instead.
        //
        // Spec §8.3 says such a directory "appear[s] as orphanedSpools on
        // the status page"; it deliberately does not here. An orphan is
        // offered to `discardOrphan`, and one click must not be able to
        // throw away a configured sink's queued data. See the report
        // accompanying this change: the spec's wording is the thing that
        // needs reconciling, not this behaviour.
        const onDisk =
          active === undefined ? await readSpoolDirStats(this.spoolDirFor(entry.name)) : null;
        const oldest =
          active === undefined ? onDisk?.oldestMtimeMs : await active.queue.oldestMtimeMs();
        statuses.push({
          name: entry.name,
          type: entry.config.type,
          enabled: entry.enabled,
          health: this.options.metrics.getSinkHealth(entry.name),
          queue: {
            files: active?.queue.fileCount() ?? onDisk?.files ?? 0,
            bytes: active?.queue.bytes() ?? onDisk?.bytes ?? 0,
            oldestAgeSec:
              oldest === null || oldest === undefined
                ? null
                : Math.floor((Date.now() - oldest) / 1000),
          },
          dead:
            active === undefined
              ? (onDisk?.dead ?? emptyDeadStats())
              : await active.queue.deadStats(),
          counters: counters[entry.name] ?? { delivered: 0, dropped: 0, deadLettered: 0 },
        });
      } catch (error: unknown) {
        // One sink's stat call failing -- its spool directory removed, or a
        // permission change underneath it -- must not blank out every other
        // sink's status in the same response. This synthesized entry
        // reports the failure for THIS response only; it is never written
        // to `metrics`, because the status route this feeds is read-only
        // and must not mutate shared state as a side effect of being
        // polled.
        const message = error instanceof Error ? error.message : 'unknown error';
        this.options.log.warn({ sink: entry.name, err: message }, 'sink status snapshot failed');
        statuses.push({
          name: entry.name,
          type: entry.config.type,
          enabled: entry.enabled,
          health: {
            state: 'failed',
            // Zero, because no delivery attempt failed -- the sink's status
            // could not be READ. The prefix on lastError says so, since
            // `failed` with no consecutive failures would otherwise read to
            // an operator as "just started failing".
            consecutiveFailures: 0,
            lastError: `status unavailable: ${message}`,
            lastErrorAt: Date.now(),
            lastSuccessAt: null,
            nextRetryAt: null,
          },
          queue: { files: 0, bytes: 0, oldestAgeSec: null },
          dead: emptyDeadStats(),
          counters: counters[entry.name] ?? { delivered: 0, dropped: 0, deadLettered: 0 },
        });
      }
    }
    return statuses;
  }

  /**
   * Whether the spool volume is refusing writes for at least one sink, so
   * deliveries are being answered 500 rather than stored (spec §4).
   *
   * Exposed separately from `isDegraded()` because this is the ONLY condition
   * `/readyz` may report: spec §10, "readiness must not gate the surface that
   * fixes it". An orchestrator acting on a failing readiness probe also
   * removes the admin UI and API, which this same process serves — and a
   * failed sink's URL, or a config with no sinks at all, are both fixed
   * through that UI. Gating readiness on those deadlocks a fresh deployment:
   * the default config has no sinks, so it would never become ready and an
   * operator could never reach the page that would make it ready. A full
   * spool volume is different: an operator frees or resizes the volume, not
   * the browser, and refusing traffic is honest because the service really
   * cannot store what it would be handed.
   */
  spoolBelowFloor(): boolean {
    return this.belowFloor.size > 0;
  }

  isDegraded(): boolean {
    // A service with nowhere to put a delivery is not healthy, whatever the
    // per-sink health says: every delivery is refused with a 500. This also
    // covers the never-configured dispatcher, and the first-boot window where
    // a drain exists before any sink does.
    //
    // Reported on the status page, NOT by /readyz -- see spoolBelowFloor().
    if (this.enabledSinkCount() === 0) return true;

    if (this.spoolBelowFloor()) return true;

    for (const entry of this.config?.sinks ?? []) {
      if (!entry.enabled) continue;
      try {
        if (this.options.metrics.getSinkHealth(entry.name).state === 'failed') return true;
      } catch {
        // Same reasoning as the try/catch in snapshotSinks(): isDegraded()
        // backs both /api/status's `service.state` and /readyz, so a sink
        // whose health cannot even be read must not crash either one.
        // Failing safe to "degraded" here, rather than silently treating
        // an unreadable sink as healthy, keeps that guarantee meaningful
        // instead of merely non-crashing.
        return true;
      }
    }
    return false;
  }
}
