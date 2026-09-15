import { z } from 'zod';
import type { JsonValue } from '../../types/json.js';

export const jsonValueSchema: z.ZodType<JsonValue> = z.lazy(() =>
  z.union([
    z.string(),
    z.number(),
    z.boolean(),
    z.null(),
    z.array(jsonValueSchema),
    z.record(z.string(), jsonValueSchema),
  ]),
);

/**
 * Largest millisecond offset a JavaScript `Date` can represent (±100 000 000
 * days from the epoch). Anything outside it makes `new Date(ms).toISOString()`
 * throw `RangeError: Invalid time value`.
 */
const MAX_DATE_MS = 8_640_000_000_000_000;

export const logEventSchema = z
  .object({
    id: z.string(),
    timestamp: z
      .number()
      // Bounded at the boundary, not left to the sinks. zod already rejects
      // NaN and Infinity, but a finite out-of-range number -- `1e21`, a
      // timestamp in microseconds or nanoseconds, a replay tool's bad unit
      // conversion -- used to validate here and then throw RangeError inside
      // the file sink's date derivation. That escaped as a plain Error, which
      // the worker correctly classifies as retryable, so the batch sat at the
      // head of the queue and was re-attempted forever: nothing delivered,
      // nothing written, `lastError: "Invalid time value"` with no sink name
      // or event id, surviving every restart because the batch is on disk,
      // and self-healing only via drop-oldest after 512 MiB of loss.
      //
      // One malformed event among a thousand good ones is exactly what the
      // ingest path's reject counter and `recent.rejects` ring exist for, so
      // the boundary is where this belongs: the event is counted, reported
      // with its reason, and cannot reach a sink. It also heals an
      // already-poisoned spool, because SpoolQueue re-validates every line it
      // reads back.
      .min(0, 'timestamp must be epoch milliseconds within the representable Date range')
      .max(MAX_DATE_MS, 'timestamp must be epoch milliseconds within the representable Date range'),
    source: z.string(),
    projectId: z.string(),
  })
  .catchall(jsonValueSchema);

export type LogEvent = z.infer<typeof logEventSchema>;

export type EventLevel = 'info' | 'warning' | 'error';

const LEVEL_RANKS: Record<EventLevel, number> = { info: 0, warning: 1, error: 2 };

export function levelRank(level: EventLevel): number {
  return LEVEL_RANKS[level];
}

export function eventLevel(event: LogEvent): EventLevel {
  const raw = event['level'];
  if (typeof raw !== 'string') return 'info';
  const normalized = raw.toLowerCase();
  if (normalized === 'error' || normalized === 'fatal') return 'error';
  if (normalized === 'warning' || normalized === 'warn') return 'warning';
  return 'info';
}
