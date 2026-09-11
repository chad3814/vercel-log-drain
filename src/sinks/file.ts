import { mkdir, open, readdir, statfs, unlink } from 'node:fs/promises';
import type { FileHandle } from 'node:fs/promises';
import { isAbsolute, join, relative, resolve } from 'node:path';
import { z } from 'zod';
import type { LogEvent } from '../vercel/event.js';
import { RetryableDeliveryError } from './types.js';
import type { FreeSpaceProbe, Sink, SinkContext, SinkType } from './types.js';

const HANDLE_CACHE_LIMIT = 3;
const DAY_MS = 86_400_000;
const PRUNE_INTERVAL_MS = 3_600_000;

export const fileSinkConfigSchema = z.object({
  type: z.literal('file'),
  directory: z.string().min(1),
  filePrefix: z
    .string()
    .min(1)
    .regex(/^[A-Za-z0-9][A-Za-z0-9._-]*$/, 'prefix must be filename-safe'),
  retentionDays: z.number().int().min(0),
  freeSpaceFloorBytes: z.number().int().min(0),
});

export type FileSinkConfig = z.infer<typeof fileSinkConfigSchema>;

export function utcDateKey(timestampMs: number): string {
  return new Date(timestampMs).toISOString().slice(0, 10);
}

export function dailyFileName(prefix: string, timestampMs: number): string {
  return `${prefix}-${utcDateKey(timestampMs)}.jsonl`;
}

export function groupByUtcDate(events: LogEvent[]): Map<string, LogEvent[]> {
  const grouped = new Map<string, LogEvent[]>();
  for (const event of events) {
    const key = utcDateKey(event.timestamp);
    const bucket = grouped.get(key);
    if (bucket === undefined) {
      grouped.set(key, [event]);
    } else {
      bucket.push(event);
    }
  }
  return grouped;
}

export function resolveLogsDirectory(candidate: string, logsRoot: string): string {
  const root = resolve(logsRoot);
  const target = resolve(candidate);
  const rel = relative(root, target);
  const escapes = rel.startsWith('..') || isAbsolute(rel);
  if (escapes) {
    throw new Error(`directory ${candidate} resolves outside the logs root ${logsRoot}`);
  }
  return target;
}

export const statfsFreeSpace: FreeSpaceProbe = async (path) => {
  const stats = await statfs(path);
  return stats.bavail * stats.bsize;
};

export async function pruneRetention(
  dir: string,
  prefix: string,
  retentionDays: number,
  nowMs: number,
): Promise<string[]> {
  if (retentionDays <= 0) return [];

  let entries: string[];
  try {
    entries = await readdir(dir);
  } catch {
    return [];
  }

  const pattern = new RegExp(`^${prefix}-(\\d{4}-\\d{2}-\\d{2})\\.jsonl$`);
  const cutoff = nowMs - retentionDays * DAY_MS;
  const deleted: string[] = [];

  for (const entry of entries) {
    const match = pattern.exec(entry);
    if (match === null) continue;
    const dateKey = match[1];
    if (dateKey === undefined) continue;
    const fileMs = Date.parse(`${dateKey}T00:00:00.000Z`);
    if (Number.isNaN(fileMs) || fileMs >= cutoff) continue;
    await unlink(join(dir, entry));
    deleted.push(entry);
  }
  return deleted;
}

class FileSink implements Sink {
  readonly type = 'file';
  private readonly handles = new Map<string, FileHandle>();
  private pruneTimer: NodeJS.Timeout | null = null;

  constructor(
    readonly name: string,
    private readonly config: FileSinkConfig,
    private readonly ctx: SinkContext,
  ) {}

  private get freeSpace(): FreeSpaceProbe {
    return this.ctx.freeSpace ?? statfsFreeSpace;
  }

  startPruner(): void {
    if (this.config.retentionDays <= 0) return;
    void this.prune();
    this.pruneTimer = setInterval(() => {
      void this.prune();
    }, PRUNE_INTERVAL_MS);
    this.pruneTimer.unref();
  }

  private async prune(): Promise<void> {
    try {
      const deleted = await pruneRetention(
        this.config.directory,
        this.config.filePrefix,
        this.config.retentionDays,
        Date.now(),
      );
      if (deleted.length > 0) {
        this.ctx.log.info({ sink: this.name, deleted: deleted.length }, 'pruned old log files');
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.ctx.log.warn({ sink: this.name, err: message }, 'retention prune failed');
    }
  }

  private async assertSpaceAvailable(): Promise<void> {
    if (this.config.freeSpaceFloorBytes <= 0) return;
    const available = await this.freeSpace(this.config.directory);
    if (available < this.config.freeSpaceFloorBytes) {
      throw new RetryableDeliveryError(
        `only ${available} bytes free in ${this.config.directory}, floor is ${this.config.freeSpaceFloorBytes}`,
      );
    }
  }

  /**
   * Called on every deliver, deliberately uncached. `mkdir` with `recursive`
   * is idempotent and costs one syscall per coalesced batch, whereas caching a
   * "directory exists" flag means that if the directory is removed externally
   * — an operator cleaning up, a volume remount — every later write fails with
   * ENOENT permanently, until the process restarts. That trades a negligible
   * saving for an unrecoverable durability regression.
   */
  private async ensureDirectory(): Promise<void> {
    await mkdir(this.config.directory, { recursive: true });
  }

  private async handleFor(dateKey: string): Promise<FileHandle> {
    const existing = this.handles.get(dateKey);
    if (existing !== undefined) {
      // Refresh recency: re-inserting moves the key to the end of a Map's
      // iteration order, which is what makes the eviction below an LRU.
      this.handles.delete(dateKey);
      this.handles.set(dateKey, existing);
      return existing;
    }

    const path = join(this.config.directory, `${this.config.filePrefix}-${dateKey}.jsonl`);
    const handle = await open(path, 'a');
    this.handles.set(dateKey, handle);

    // Bounded cache. A long replay walks through many dates, and leaking a
    // descriptor per day would eventually exhaust the process limit.
    while (this.handles.size > HANDLE_CACHE_LIMIT) {
      const oldest = this.handles.keys().next();
      if (oldest.done === true) break;
      const key = oldest.value;
      const evicted = this.handles.get(key);
      try {
        if (evicted !== undefined) await evicted.close();
      } catch (error) {
        // A failed close must not fail the delivery that triggered eviction —
        // the write we are here for would otherwise have succeeded.
        const message = error instanceof Error ? error.message : String(error);
        this.ctx.log.warn({ sink: this.name, err: message }, 'failed to close evicted handle');
      } finally {
        // Untrack in `finally`: closing first is what avoids leaking a
        // descriptor, but the key must go regardless or a throwing close
        // would spin this loop forever.
        this.handles.delete(key);
      }
    }
    return handle;
  }

  async deliver(events: LogEvent[]): Promise<void> {
    if (events.length === 0) return;
    await this.assertSpaceAvailable();
    await this.ensureDirectory();

    for (const [dateKey, batch] of groupByUtcDate(events)) {
      const handle = await this.handleFor(dateKey);
      const payload = `${batch.map((event) => JSON.stringify(event)).join('\n')}\n`;
      await handle.write(payload, null, 'utf8');
      await handle.sync();
    }
    this.ctx.log.debug({ sink: this.name, count: events.length }, 'file sink wrote batch');
  }

  async close(): Promise<void> {
    if (this.pruneTimer !== null) {
      clearInterval(this.pruneTimer);
      this.pruneTimer = null;
    }
    const handles = [...this.handles.values()];
    this.handles.clear();
    await Promise.all(handles.map((handle) => handle.close()));
  }
}

export const fileSinkType: SinkType<FileSinkConfig> = {
  type: 'file',
  configSchema: fileSinkConfigSchema,
  create(name, config, ctx) {
    const sink = new FileSink(name, config, ctx);
    sink.startPruner();
    return sink;
  },
  warnings() {
    return [];
  },
};
