import { randomBytes, randomUUID } from 'node:crypto';
import { z } from 'zod';
import { sinkConfigSchema } from '../sinks/registry.js';
import { hasNoUsableLabels } from '../sinks/loki-payload.js';

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
  // Purpose: catch a typo. A coalescing bound larger than the whole spool
  // budget can never actually be reached, so configuring one is almost always a
  // mistake, and it is kinder to reject it at save time than to leave someone
  // puzzling over why a setting appears to do nothing.
  //
  // This is NOT a correctness fix, and the distinction matters if you are
  // tempted to lean on it: the queue tolerates the mismatch fine. `enqueue`
  // writes an over-budget batch regardless, after draining to make room, and
  // `nextBatch` always returns at least its first file. Do not re-derive a
  // deadlock theory here and then weaken something else on the strength of it.
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

/**
 * `appConfigSchema`, plus one extra check: reject a loki sink whose labels
 * can never resolve to anything (see hasNoUsableLabels in
 * src/sinks/loki-payload.ts) -- `labels: { static: {}, fromFields: [] }`
 * resolves every event to `stream: {}`, which Loki answers with a
 * permanent 400 that dead-letters the whole batch (issue #9).
 *
 * Deliberately a SEPARATE schema from `appConfigSchema`, not folded into
 * it. `ConfigStore.load()` parses every config.json through
 * `appConfigSchema` and must stay tolerant of a pre-existing file in this
 * shape: refusing to load would crash-loop the whole container over one
 * misconfigured sink, taking every other sink and drain down with it, with
 * no way back in through the (now-unreachable) admin UI to fix it -- the
 * same deadlock `/readyz` used to cause and was deliberately changed to
 * avoid. `LokiSink`'s constructor (src/sinks/loki.ts) throws for this same
 * condition, and `reconcileNow` already isolates a sink that fails to
 * start, so a config already on disk in this shape boots with that one
 * sink `failed` and every other sink and drain running.
 *
 * `appConfigWriteSchema` is for the one path that can still afford to be
 * strict: `restoreSecrets` in src/config/redact.ts, which validates the
 * body of `PUT /api/admin/config` before anything is persisted. An operator
 * saving this shape gets a 400 naming the sink, and the state becomes
 * unrepresentable going forward.
 *
 * Do not merge this back into `appConfigSchema` "for simplicity" -- that
 * reintroduces the crash-loop this split exists to avoid.
 */
export const appConfigWriteSchema = appConfigSchema.superRefine((config, ctx) => {
  config.sinks.forEach((entry, index) => {
    if (entry.config.type !== 'loki') return;
    if (!hasNoUsableLabels(entry.config.labels)) return;
    ctx.addIssue({
      code: 'custom',
      path: ['sinks', index, 'config', 'labels'],
      message:
        `sink "${entry.name}": loki labels are empty (no static entry has a value, and ` +
        'fromFields has no entries), so every event resolves to stream {} and Loki rejects it ' +
        'with a permanent 400 that dead-letters the whole batch. Add at least one static label ' +
        '(e.g. static: { job: "vercel" }) or one fromFields entry (e.g. fromFields: ["environment"]).',
    });
  });
});

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
