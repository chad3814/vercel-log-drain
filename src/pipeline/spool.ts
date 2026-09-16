import { mkdir, open, readdir, readFile, rename, rm, stat, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { logEventSchema } from '../vercel/event.js';
import { statfsFreeSpace } from '../sinks/file.js';
import type { LogEvent } from '../vercel/event.js';
import type { FreeSpaceProbe } from '../sinks/types.js';
import type { Logger } from '../log.js';

const BATCH_NAME = /^\d{12}\.jsonl$/;
/**
 * Sequence prefix of any file in `dead/`. Matches both the plain
 * `000000000042.jsonl` and the collision-avoiding
 * `000000000042.1763078400000.jsonl`, so recovery can read the sequence number
 * off either form.
 */
const DEAD_SEQ = /^(\d{12})\./;
const SEQ_WIDTH = 12;
const DEAD_DIR = 'dead';

export type SpoolOptions = {
  maxSpoolBytes: number;
  freeSpaceFloorBytes: number;
  freeSpace?: FreeSpaceProbe;
  /**
   * Optional, but supply it in production. Without it, a filesystem error
   * during `ack`, `deadLetter` or overflow eviction is swallowed silently —
   * which is how a dead-letter name collision went undetected long enough to
   * destroy a batch during development.
   */
  log?: Logger;
};

export type SpoolBatch = { files: string[]; events: LogEvent[]; bytes: number };
/**
 * A batch that WAS written. `droppedEvents` counts what a drop-oldest
 * overflow eviction reclaimed to fit it inside this sink's `maxSpoolBytes`.
 *
 * There is no field for the free-space floor: a volume below its floor
 * refuses, and `enqueue` throws `SpoolFloorError` rather than returning. An
 * earlier version reported it here as a `floorDrop` the caller could ignore,
 * and the caller carried on to answer Vercel 200 for events that reached no
 * disk anywhere.
 */
export type EnqueueResult = {
  writtenBytes: number;
  droppedEvents: number;
};

/**
 * The spool volume sits below its free-space floor, so `enqueue` wrote
 * NOTHING and the caller must refuse the delivery (spec §4).
 *
 * Backpressure, not shedding. Vercel redelivers on a 500, which is what makes
 * refusing safe, and it is what makes "no acknowledged delivery is ever lost"
 * hold without qualification: the service never acknowledges what it could
 * not store. The accepted cost is that a full volume stops ingest for every
 * sink, including healthy ones -- no sink can free a volume that some other
 * sink, or something outside this service entirely, has filled.
 *
 * Deliberately NOT how a drop-oldest overflow eviction is reported. That is a
 * budgeted trade inside one sink that still commits the incoming batch, so it
 * stays a `droppedEvents` count on a successful `EnqueueResult`. Only a full
 * volume refuses.
 */
export class SpoolFloorError extends Error {
  constructor(
    readonly freeBytes: number,
    readonly floorBytes: number,
  ) {
    super(
      'spool volume is below its free-space floor ' +
        `(${String(freeBytes)} B free, floor ${String(floorBytes)} B)`,
    );
    this.name = 'SpoolFloorError';
  }
}

type Entry = { name: string; bytes: number };

export type DeadStats = { files: number; bytes: number };
export type SpoolDirStats = {
  files: number;
  bytes: number;
  oldestMtimeMs: number | null;
  dead: DeadStats;
};

async function statDir(dir: string, matches: (name: string) => boolean): Promise<SpoolDirStats> {
  const stats: SpoolDirStats = {
    files: 0,
    bytes: 0,
    oldestMtimeMs: null,
    dead: { files: 0, bytes: 0 },
  };
  for (const name of await readdir(dir).catch(() => [])) {
    if (!matches(name)) continue;
    const entry = await stat(join(dir, name)).catch(() => null);
    if (entry === null || !entry.isFile()) continue;
    stats.files += 1;
    stats.bytes += entry.size;
    if (stats.oldestMtimeMs === null || entry.mtimeMs < stats.oldestMtimeMs) {
      stats.oldestMtimeMs = entry.mtimeMs;
    }
  }
  return stats;
}

/**
 * What is on disk in a spool directory, read without opening a `SpoolQueue`.
 *
 * Needed because a queue exists only for a RUNNING sink, so the only figures
 * available for a disabled sink or an abandoned directory were the zeroes an
 * absent queue reports -- which is how a disabled sink's undelivered backlog
 * came to be reported as `0 files / 0 B` while it sat on disk, and how
 * `dead/` came to appear in no byte figure anywhere. Read-only by
 * construction: the status route that consumes this is polled every two
 * seconds and must not mutate anything.
 *
 * `dead/` is counted separately rather than folded into `bytes`, because the
 * two mean different things to an operator: live bytes will drain on their
 * own, dead bytes never will and are what they have to go and clear by hand.
 */
export async function readSpoolDirStats(dir: string): Promise<SpoolDirStats> {
  const live = await statDir(dir, (name) => BATCH_NAME.test(name));
  const dead = await statDir(join(dir, DEAD_DIR), () => true);
  return { ...live, dead: { files: dead.files, bytes: dead.bytes } };
}

function serialize(events: LogEvent[]): Buffer {
  return Buffer.from(`${events.map((event) => JSON.stringify(event)).join('\n')}\n`, 'utf8');
}

function parseLines(text: string): LogEvent[] {
  const events: LogEvent[] = [];
  for (const line of text.split('\n')) {
    if (line.trim().length === 0) continue;
    try {
      const parsed = logEventSchema.safeParse(JSON.parse(line));
      if (parsed.success) events.push(parsed.data);
    } catch {
      // A corrupt line is skipped. The batch may end up empty, which the
      // worker acks — that is how the queue heals past corruption instead of
      // retrying a broken head forever.
    }
  }
  return events;
}

function countLines(text: string): number {
  return text.split('\n').filter((line) => line.trim().length > 0).length;
}

export class SpoolQueue {
  private entries: Entry[] = [];
  private totalBytes = 0;
  private seq = 0;
  private readonly freeSpace: FreeSpaceProbe;

  private constructor(
    private readonly dir: string,
    private readonly options: SpoolOptions,
  ) {
    this.freeSpace = options.freeSpace ?? statfsFreeSpace;
  }

  static async open(dir: string, options: SpoolOptions): Promise<SpoolQueue> {
    const queue = new SpoolQueue(dir, options);
    await mkdir(join(dir, DEAD_DIR), { recursive: true });
    await queue.recover();
    return queue;
  }

  private async recover(): Promise<void> {
    const names = await readdir(this.dir);
    const batches: string[] = [];

    for (const name of names) {
      if (name.endsWith('.tmp')) {
        await unlink(join(this.dir, name)).catch(() => undefined);
        continue;
      }
      if (BATCH_NAME.test(name)) batches.push(name);
    }
    // toSorted, not sort: oxlint's unicorn/no-array-sort bans the mutating
    // form. Lexicographic order is deliberate — the 12-digit zero padding
    // makes it identical to numeric order.
    const ordered = batches.toSorted();

    const entries: Entry[] = [];
    let total = 0;
    let maxSeq = -1;
    for (const name of ordered) {
      const stats = await stat(join(this.dir, name));
      entries.push({ name, bytes: stats.size });
      total += stats.size;
      maxSeq = Math.max(maxSeq, Number.parseInt(name.slice(0, SEQ_WIDTH), 10));
    }

    // Sequence numbers must also clear anything already in `dead/`. Without
    // this, a restart whose live directory is empty resets the counter to 0 and
    // reissues a name a dead-lettered file already holds; `deadLetter`'s rename
    // then replaces that file, because POSIX rename replaces its destination.
    // Reproduced before this was added: a batch dead-lettered in one process
    // life was silently destroyed by an unrelated batch dead-lettered after a
    // restart, with no error and no log line.
    // ENOENT only. `catch(() => [])` also swallowed EACCES, EIO and
    // ENOTDIR, and any of those means the sequence counter was recovered
    // from the live directory alone -- so it can reissue a name `dead/`
    // already holds, and rename replaces its destination silently. One
    // permissions change on the volume, or a transient NFS error on a single
    // boot, was enough. That must not be recoverable-looking: this queue
    // cannot honour the monotonicity §3.4 binds it to, so opening it fails
    // and says why. `Dispatcher.reconcileNow` catches that and fails THIS
    // SINK -- reporting `failed` health naming the directory -- rather than
    // the process: one unreadable dead/ taking ingest down for every drain
    // and sink was worse than the collision it guards against, and it is
    // reachable through the README's "clear them by hand" procedure.
    // deadPathFor's stat guard stays as the backstop for whatever this does
    // not foresee.
    for (const name of await readdir(join(this.dir, DEAD_DIR)).catch((error: unknown) => {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return [];
      throw error;
    })) {
      const match = DEAD_SEQ.exec(name);
      if (match?.[1] === undefined) continue;
      maxSeq = Math.max(maxSeq, Number.parseInt(match[1], 10));
    }

    this.entries = entries;
    this.totalBytes = total;
    this.seq = maxSeq + 1;
  }

  bytes(): number {
    return this.totalBytes;
  }

  fileCount(): number {
    return this.entries.length;
  }

  async enqueue(events: LogEvent[]): Promise<EnqueueResult> {
    if (events.length === 0) return { writtenBytes: 0, droppedEvents: 0 };

    // Throws rather than returning a "nothing was written" result: the caller
    // has to fail the delivery so Vercel redelivers, and a result it is free
    // to ignore is exactly how this condition came to answer 200 for events
    // that reached no disk anywhere. See SpoolFloorError.
    if (this.options.freeSpaceFloorBytes > 0) {
      const available = await this.freeSpace(this.dir);
      if (available < this.options.freeSpaceFloorBytes) {
        throw new SpoolFloorError(available, this.options.freeSpaceFloorBytes);
      }
    }

    const payload = serialize(events);
    const name = `${String(this.seq).padStart(SEQ_WIDTH, '0')}.jsonl`;
    this.seq += 1;
    const tmpPath = join(this.dir, `${name}.tmp`);

    // Commit the new batch COMPLETELY before evicting anything: write, fsync,
    // rename, and fsync the directory. Eviction is a real unlink, so any step
    // still ahead of it is a step that can fail with the old batch already
    // destroyed. An earlier version evicted between the fsync and the rename,
    // which left a narrow window where a failing rename lost both the evicted
    // batch and the replacement — boot recovery deletes stray `.tmp` files, so
    // the replacement had nowhere to survive. Ordering is the whole fix here;
    // do not move `makeRoom` back inside this block.
    try {
      const handle = await open(tmpPath, 'w');
      try {
        await handle.writeFile(payload);
        await handle.sync();
      } finally {
        await handle.close();
      }
      await rename(tmpPath, join(this.dir, name));

      const dirHandle = await open(this.dir, 'r');
      try {
        await dirHandle.sync();
      } finally {
        await dirHandle.close();
      }
    } catch (error) {
      // Nothing has been evicted yet, so the only cleanup is the temp file,
      // which must not be left for boot recovery to find. After a successful
      // rename `tmpPath` is already gone and this is a no-op.
      await rm(tmpPath, { force: true }).catch(() => undefined);
      throw error;
    }

    // Safe now: the new batch is durable, so reclaiming space can at worst
    // leave the spool briefly over budget. `makeRoom` swallows and logs its own
    // filesystem errors, so a throw here means a programming error; the batch
    // is on disk either way and boot recovery will pick it up.
    const droppedEvents = await this.makeRoom(payload.byteLength);

    this.entries.push({ name, bytes: payload.byteLength });
    this.totalBytes += payload.byteLength;
    return { writtenBytes: payload.byteLength, droppedEvents };
  }

  private async makeRoom(incoming: number): Promise<number> {
    let dropped = 0;
    while (this.entries.length > 0 && this.totalBytes + incoming > this.options.maxSpoolBytes) {
      const oldest = this.entries.shift();
      if (oldest === undefined) break;
      const path = join(this.dir, oldest.name);
      try {
        dropped += countLines(await readFile(path, 'utf8'));
      } catch {
        // Already gone; still account for its bytes below.
      }
      this.totalBytes -= oldest.bytes;
      await unlink(path).catch((error: unknown) => {
        this.reportFsError('unlink during overflow eviction', oldest.name, error);
      });
    }
    // If a single batch is larger than the whole budget the loop empties the
    // queue and we still write it: refusing the newest data would be worse.
    return dropped;
  }

  async nextBatch(maxEvents: number, maxBytes: number): Promise<SpoolBatch | null> {
    if (this.entries.length === 0) return null;

    const files: string[] = [];
    const events: LogEvent[] = [];
    const unreadable: string[] = [];
    let bytes = 0;

    for (const entry of this.entries) {
      let text: string;
      try {
        text = await readFile(join(this.dir, entry.name), 'utf8');
      } catch (error) {
        // Stop tracking it. Leaving the entry in place used to overcount
        // totalBytes for the rest of the process's life, which could evict
        // live batches to make room that was never occupied — and the entry
        // was re-read and re-skipped on every later call.
        this.reportFsError('read', entry.name, error);
        unreadable.push(entry.name);
        continue;
      }
      const parsed = parseLines(text);

      const wouldExceed =
        events.length + parsed.length > maxEvents || bytes + entry.bytes > maxBytes;
      if (files.length > 0 && wouldExceed) break;

      files.push(entry.name);
      events.push(...parsed);
      bytes += entry.bytes;

      if (events.length >= maxEvents || bytes >= maxBytes) break;
    }

    if (unreadable.length > 0) this.untrack(unreadable);
    if (files.length === 0) return null;
    return { files, events, bytes };
  }

  async ack(batch: SpoolBatch): Promise<void> {
    await this.removeAll('ack', batch.files, (name) => unlink(join(this.dir, name)));
  }

  async deadLetter(batch: SpoolBatch): Promise<void> {
    await this.removeAll('dead-letter', batch.files, async (name) => {
      await rename(join(this.dir, name), await this.deadPathFor(name));
    });
  }

  /**
   * Where a dead-lettered batch should land. Recovery now scans `dead/` when
   * choosing sequence numbers, so a collision should be impossible — but this
   * checks anyway, because POSIX rename REPLACES its destination and a
   * collision here would silently destroy an already-failed batch, which is
   * the single thing this directory exists to prevent.
   */
  private async deadPathFor(name: string): Promise<string> {
    const preferred = join(this.dir, DEAD_DIR, name);
    try {
      await stat(preferred);
    } catch {
      return preferred;
    }
    const suffixed = join(
      this.dir,
      DEAD_DIR,
      `${name.slice(0, SEQ_WIDTH)}.${String(Date.now())}.jsonl`,
    );
    this.options.log?.error(
      { dir: this.dir, name, suffixed },
      'dead-letter name already taken; preserving both rather than replacing',
    );
    return suffixed;
  }

  private reportFsError(operation: string, name: string, error: unknown): void {
    const failure = error instanceof Error ? error : new Error(String(error));
    // An ENOENT means the file was already gone, which is the expected race and
    // not worth a log line. Anything else is a real filesystem problem.
    if ('code' in failure && failure.code === 'ENOENT') return;
    this.options.log?.warn(
      { dir: this.dir, name, operation, err: failure.message },
      'spool filesystem operation failed',
    );
  }

  private untrack(names: readonly string[]): void {
    const removing = new Set(names);
    const kept: Entry[] = [];
    for (const entry of this.entries) {
      if (removing.has(entry.name)) {
        this.totalBytes -= entry.bytes;
      } else {
        kept.push(entry);
      }
    }
    this.entries = kept;
  }

  private async removeAll(
    operation: string,
    names: string[],
    action: (name: string) => Promise<void>,
  ): Promise<void> {
    for (const name of names) {
      await action(name).catch((error: unknown) => {
        this.reportFsError(operation, name, error);
      });
    }
    // The entry is untracked whether or not the filesystem call succeeded. A
    // file that genuinely cannot be removed would otherwise sit at the head of
    // the queue and be redelivered forever, which is worse than the byte
    // undercount — and the failure is now logged rather than swallowed.
    this.untrack(names);
  }

  /**
   * Files and bytes in `dead/`. Not tracked in memory like the live entries,
   * because nothing in the running service reads it on a hot path and a
   * counter would drift from the directory an operator is actually clearing
   * by hand. Deliberately NOT counted against `maxSpoolBytes`: `makeRoom`
   * evicts by unlinking, and `dead/` is terminal storage that nothing the
   * service does may remove (spec §3.4). Visibility is the fix here, not
   * eviction.
   */
  async deadStats(): Promise<DeadStats> {
    const stats = await statDir(join(this.dir, DEAD_DIR), () => true);
    return { files: stats.files, bytes: stats.bytes };
  }

  async oldestMtimeMs(): Promise<number | null> {
    const oldest = this.entries[0];
    if (oldest === undefined) return null;
    try {
      const stats = await stat(join(this.dir, oldest.name));
      return stats.mtimeMs;
    } catch {
      return null;
    }
  }

  async discardAll(): Promise<void> {
    for (const entry of this.entries) {
      await unlink(join(this.dir, entry.name)).catch((error: unknown) => {
        this.reportFsError('unlink during discardAll', entry.name, error);
      });
    }
    this.entries = [];
    this.totalBytes = 0;
    await rm(join(this.dir, DEAD_DIR), { recursive: true, force: true });
    await mkdir(join(this.dir, DEAD_DIR), { recursive: true });
  }
}
