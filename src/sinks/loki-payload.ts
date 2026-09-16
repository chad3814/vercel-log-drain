import { HIGH_CARDINALITY_FIELDS } from '../../types/api.js';
import type { LogEvent } from '../vercel/event.js';

// Re-exported, not just imported: this module is where `labelWarnings`
// below uses the name, and it's also where every existing importer of
// `HIGH_CARDINALITY_FIELDS` from this file expects to find it. A bare
// `export ... from '../../types/api.js'` would satisfy those importers but
// not `labelWarnings` in *this* file, which needs the binding in scope --
// so this has to be a real import plus a separate `export { ... }`, not a
// re-export statement.
export { HIGH_CARDINALITY_FIELDS };

const PUSH_PATH = '/loki/api/v1/push';
const LABEL_VALUE_LIMIT = 1024;

export type LokiLabelConfig = { static: Record<string, string>; fromFields: string[] };
export type LokiStream = { stream: Record<string, string>; values: [string, string][] };
export type LokiPushPayload = { streams: LokiStream[] };

export const DEFAULT_LABEL_CONFIG: LokiLabelConfig = {
  static: { job: 'vercel' },
  fromFields: ['projectName', 'environment', 'source', 'level'],
};

export function sanitizeLabelName(name: string): string {
  const replaced = name.replace(/[^a-zA-Z0-9_]/g, '_');
  return /^[0-9]/.test(replaced) ? `_${replaced}` : replaced;
}

export function resolveLabels(event: LogEvent, config: LokiLabelConfig): Record<string, string> {
  const labels: Record<string, string> = {};

  // Static labels are written first, so a field-derived label with the same
  // sanitized name overwrites them. That precedence is deliberate: `fromFields`
  // names a property of the event, which is more specific than a blanket
  // static value, and an operator who sets both plainly meant the event's.
  for (const [name, value] of Object.entries(config.static)) {
    if (value.length === 0) continue;
    labels[sanitizeLabelName(name)] = value.slice(0, LABEL_VALUE_LIMIT);
  }

  for (const field of config.fromFields) {
    const raw = event[field];
    if (typeof raw !== 'string' && typeof raw !== 'number' && typeof raw !== 'boolean') continue;
    const value = String(raw);
    if (value.length === 0) continue;
    labels[sanitizeLabelName(field)] = value.slice(0, LABEL_VALUE_LIMIT);
  }

  return labels;
}

export function buildPushPayload(events: LogEvent[], config: LokiLabelConfig): LokiPushPayload {
  const byLabelSet = new Map<string, LokiStream>();

  for (const event of events) {
    const labels = resolveLabels(event, config);
    // Sorted keys make the grouping key stable regardless of field order.
    const key = JSON.stringify(Object.entries(labels).toSorted(([a], [b]) => (a < b ? -1 : 1)));
    const nanos = String(BigInt(Math.trunc(event.timestamp)) * 1_000_000n);
    const entry: [string, string] = [nanos, JSON.stringify(event)];

    const stream = byLabelSet.get(key);
    if (stream === undefined) {
      byLabelSet.set(key, { stream: labels, values: [entry] });
    } else {
      stream.values.push(entry);
    }
  }

  const streams = [...byLabelSet.values()];
  for (const stream of streams) {
    // Reassign rather than sort in place: oxlint's unicorn/no-array-sort bans
    // the mutating Array#sort, and toSorted returns a new array.
    stream.values = stream.values.toSorted((left, right) => {
      const leftNanos = BigInt(left[0]);
      const rightNanos = BigInt(right[0]);
      if (leftNanos < rightNanos) return -1;
      if (leftNanos > rightNanos) return 1;
      // Equal timestamps must return 0. A comparator that answers 1 for a tie
      // claims each side sorts after the other, which is antisymmetric-
      // violating: the engine is then free to order same-millisecond events
      // arbitrarily. Vercel batches routinely carry several lines on one
      // millisecond (a request's start and end, for instance), and returning 0
      // lets the stable sort keep them in arrival order.
      return 0;
    });
  }
  return { streams };
}

/**
 * True when this label config can never produce a non-empty label set: no
 * `static` entry carries a non-empty value (resolveLabels above skips
 * empty-string values) and no `fromFields` entry is a non-empty field name.
 * A sink stuck in this state resolves every event to `stream: {}`, and Loki
 * answers a labelless stream with a permanent 400 that dead-letters the
 * whole batch (issue #9).
 *
 * Single source of truth for that condition, shared by two call sites that
 * must never drift apart: the admin-write schema check
 * (`appConfigWriteSchema` in src/config/schema.ts, which refuses to SAVE
 * this shape) and `LokiSink`'s constructor (src/sinks/loki.ts, which
 * refuses to START with it -- the safety net for a config already on disk
 * in this shape, which the schema that `ConfigStore.load()` parses
 * deliberately still accepts).
 */
export function hasNoUsableLabels(config: LokiLabelConfig): boolean {
  const hasStatic = Object.values(config.static).some((value) => value.length > 0);
  const hasFromFields = config.fromFields.some((field) => field.length > 0);
  return !hasStatic && !hasFromFields;
}

export function labelWarnings(config: LokiLabelConfig): string[] {
  const risky = config.fromFields.filter((field) => HIGH_CARDINALITY_FIELDS.includes(field));
  if (risky.length === 0) return [];
  return [
    `Labels ${risky.join(', ')} are high-cardinality; each distinct value creates a new Loki stream. Prefer querying them from the log line with | json.`,
  ];
}

export function normalizePushUrl(base: string): string {
  const trimmed = base.replace(/\/+$/, '');
  return trimmed.endsWith(PUSH_PATH) ? trimmed : `${trimmed}${PUSH_PATH}`;
}
