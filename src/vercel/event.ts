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

export const logEventSchema = z
  .object({
    id: z.string(),
    timestamp: z.number(),
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
