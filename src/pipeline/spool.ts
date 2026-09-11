import { mkdir, open, readdir, readFile, rename, rm, stat, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { logEventSchema } from '../vercel/event.js';
import { statfsFreeSpace } from '../sinks/file.js';
import type { LogEvent } from '../vercel/event.js';
import type { FreeSpaceProbe } from '../sinks/types.js';

const BATCH_NAME = /^\d{12}\.jsonl$/;
const SEQ_WIDTH = 12;
const DEAD_DIR = 'dead';

export type SpoolOptions = {
  maxSpoolBytes: number;
  freeSpaceFloorBytes: number;
  freeSpace?: FreeSpaceProbe;
};

export type SpoolBatch = { files: string[]; events: LogEvent[]; bytes: number };
export type EnqueueResult = { writtenBytes: number; droppedEvents: number };

type Entry = { name: string; bytes: number };

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

    if (this.options.freeSpaceFloorBytes > 0) {
      const available = await this.freeSpace(this.dir);
      if (available < this.options.freeSpaceFloorBytes) {
        return { writtenBytes: 0, droppedEvents: events.length };
      }
    }

    const payload = serialize(events);
    const droppedEvents = await this.makeRoom(payload.byteLength);

    const name = `${String(this.seq).padStart(SEQ_WIDTH, '0')}.jsonl`;
    this.seq += 1;
    const tmpPath = join(this.dir, `${name}.tmp`);

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
      await unlink(path).catch(() => undefined);
    }
    // If a single batch is larger than the whole budget the loop empties the
    // queue and we still write it: refusing the newest data would be worse.
    return dropped;
  }

  async nextBatch(maxEvents: number, maxBytes: number): Promise<SpoolBatch | null> {
    if (this.entries.length === 0) return null;

    const files: string[] = [];
    const events: LogEvent[] = [];
    let bytes = 0;

    for (const entry of this.entries) {
      let text: string;
      try {
        text = await readFile(join(this.dir, entry.name), 'utf8');
      } catch {
        continue;
      }
      const parsed = parseLines(text);

      const wouldExceed = events.length + parsed.length > maxEvents || bytes + entry.bytes > maxBytes;
      if (files.length > 0 && wouldExceed) break;

      files.push(entry.name);
      events.push(...parsed);
      bytes += entry.bytes;

      if (events.length >= maxEvents || bytes >= maxBytes) break;
    }

    if (files.length === 0) return null;
    return { files, events, bytes };
  }

  async ack(batch: SpoolBatch): Promise<void> {
    await this.removeAll(batch.files, (name) => unlink(join(this.dir, name)));
  }

  async deadLetter(batch: SpoolBatch): Promise<void> {
    await this.removeAll(batch.files, (name) =>
      rename(join(this.dir, name), join(this.dir, DEAD_DIR, name)),
    );
  }

  private async removeAll(
    names: string[],
    action: (name: string) => Promise<void>,
  ): Promise<void> {
    const removing = new Set(names);
    for (const name of names) {
      await action(name).catch(() => undefined);
    }
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
      await unlink(join(this.dir, entry.name)).catch(() => undefined);
    }
    this.entries = [];
    this.totalBytes = 0;
    await rm(join(this.dir, DEAD_DIR), { recursive: true, force: true });
    await mkdir(join(this.dir, DEAD_DIR), { recursive: true });
  }
}
