import { randomBytes, randomUUID } from 'node:crypto';
import { z } from 'zod';
import { sinkConfigSchema } from '../sinks/registry.js';

export const SINK_NAME_PATTERN = /^[a-z0-9][a-z0-9-]{0,63}$/;

export const DEFAULT_MAX_SPOOL_BYTES = 536_870_912; // 512 MiB
export const DEFAULT_MAX_BATCH_EVENTS = 1000;
export const DEFAULT_MAX_BATCH_BYTES = 4_194_304; // 4 MiB
export const DEFAULT_MAX_BODY_BYTES = 16_777_216; // 16 MiB
export const DEFAULT_MAX_DECOMPRESSED_BYTES = 134_217_728; // 128 MiB
export const DEFAULT_SPOOL_FREE_FLOOR_BYTES = 268_435_456; // 256 MiB

export const sinkFilterSchema = z.object({
  minLevel: z.enum(['info', 'warning', 'error']).optional(),
  sources: z.array(z.string().min(1)).optional(),
  environments: z.array(z.string().min(1)).optional(),
  projectIds: z.array(z.string().min(1)).optional(),
});

export type SinkFilter = z.infer<typeof sinkFilterSchema>;

export const drainEntrySchema = z.object({
  id: z.string().min(8),
  name: z.string().min(1).max(128),
  secret: z.string().min(16),
  enabled: z.boolean(),
  createdAt: z.number().int().nonnegative(),
});

export type DrainEntry = z.infer<typeof drainEntrySchema>;

export const sinkEntrySchema = z
  .object({
    name: z.string().regex(SINK_NAME_PATTERN, 'sink name must match ^[a-z0-9][a-z0-9-]{0,63}$'),
    enabled: z.boolean(),
    filter: sinkFilterSchema,
    maxSpoolBytes: z.number().int().min(1_048_576),
    maxBatchEvents: z.number().int().min(1).max(100_000),
    maxBatchBytes: z.number().int().min(1024),
    config: sinkConfigSchema,
  })
  // An operator-sanity guard, NOT a correctness fix. `maxBatchBytes` bounds how
  // much the worker coalesces per delivery, and the queue tolerates the
  // mismatch either way: enqueue writes an over-budget batch regardless after
  // draining to make room, and nextBatch always returns at least its first
  // file. So this cannot deadlock. But a coalescing bound larger than the whole
  // spool budget can never actually be reached, which is almost always a typo
  // worth catching at save time rather than leaving to puzzle over later.
  .refine((entry) => entry.maxBatchBytes <= entry.maxSpoolBytes, {
    message: 'maxBatchBytes must not exceed maxSpoolBytes',
    path: ['maxBatchBytes'],
  });

export type SinkEntry = z.infer<typeof sinkEntrySchema>;

export const serverConfigSchema = z.object({
  maxBodyBytes: z.number().int().min(1024),
  maxDecompressedBytes: z.number().int().min(1024),
  spoolFreeSpaceFloorBytes: z.number().int().min(0),
});

export type ServerConfig = z.infer<typeof serverConfigSchema>;

function uniqueBy<T>(items: T[], key: (item: T) => string): boolean {
  return new Set(items.map(key)).size === items.length;
}

export const appConfigSchema = z
  .object({
    version: z.literal(1),
    drains: z.array(drainEntrySchema),
    sinks: z.array(sinkEntrySchema),
    server: serverConfigSchema,
  })
  .refine((config) => uniqueBy(config.sinks, (sink) => sink.name), {
    message: 'sink names must be unique, because a sink name is also its spool directory',
    path: ['sinks'],
  })
  .refine((config) => uniqueBy(config.drains, (drain) => drain.id), {
    message: 'drain ids must be unique',
    path: ['drains'],
  });

export type AppConfig = z.infer<typeof appConfigSchema>;

export function newDrainId(): string {
  return randomUUID().replace(/-/g, '');
}

export function newDrainSecret(): string {
  return randomBytes(32).toString('base64url');
}

export function defaultAppConfig(): AppConfig {
  return {
    version: 1,
    drains: [],
    sinks: [],
    server: {
      maxBodyBytes: DEFAULT_MAX_BODY_BYTES,
      maxDecompressedBytes: DEFAULT_MAX_DECOMPRESSED_BYTES,
      spoolFreeSpaceFloorBytes: DEFAULT_SPOOL_FREE_FLOOR_BYTES,
    },
  };
}
