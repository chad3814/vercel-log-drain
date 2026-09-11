import { mkdir, open } from 'node:fs/promises';
import type { FileHandle } from 'node:fs/promises';
import { isAbsolute, join, relative, resolve } from 'node:path';
import { z } from 'zod';
import type { LogEvent } from '../vercel/event.js';
import type { Sink, SinkContext, SinkType } from './types.js';

const HANDLE_CACHE_LIMIT = 3;

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

class FileSink implements Sink {
  readonly type = 'file';
  private readonly handles = new Map<string, FileHandle>();
  private directoryReady = false;

  constructor(
    readonly name: string,
    private readonly config: FileSinkConfig,
    private readonly ctx: SinkContext,
  ) {}

  private async ensureDirectory(): Promise<void> {
    if (this.directoryReady) return;
    await mkdir(this.config.directory, { recursive: true });
    this.directoryReady = true;
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
      const evicted = this.handles.get(oldest.value);
      this.handles.delete(oldest.value);
      if (evicted !== undefined) await evicted.close();
    }
    return handle;
  }

  async deliver(events: LogEvent[]): Promise<void> {
    if (events.length === 0) return;
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
    const handles = [...this.handles.values()];
    this.handles.clear();
    await Promise.all(handles.map((handle) => handle.close()));
  }
}

export const fileSinkType: SinkType<FileSinkConfig> = {
  type: 'file',
  configSchema: fileSinkConfigSchema,
  create(name, config, ctx) {
    return new FileSink(name, config, ctx);
  },
  warnings() {
    return [];
  },
};
