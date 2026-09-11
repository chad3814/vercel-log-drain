import type { LogEvent } from '../vercel/event.js';

const PUSH_PATH = '/loki/api/v1/push';
const LABEL_VALUE_LIMIT = 1024;

export type LokiLabelConfig = { static: Record<string, string>; fromFields: string[] };
export type LokiStream = { stream: Record<string, string>; values: [string, string][] };
export type LokiPushPayload = { streams: LokiStream[] };

export const DEFAULT_LABEL_CONFIG: LokiLabelConfig = {
  static: { job: 'vercel' },
  fromFields: ['projectName', 'environment', 'source', 'level'],
};

export const HIGH_CARDINALITY_FIELDS: readonly string[] = [
  'id',
  'requestId',
  'deploymentId',
  'path',
  'host',
  'traceId',
  'spanId',
  'buildId',
  'trace.id',
  'span.id',
];

export function sanitizeLabelName(name: string): string {
  const replaced = name.replace(/[^a-zA-Z0-9_]/g, '_');
  return /^[0-9]/.test(replaced) ? `_${replaced}` : replaced;
}

export function resolveLabels(event: LogEvent, config: LokiLabelConfig): Record<string, string> {
  const labels: Record<string, string> = {};

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
    stream.values = stream.values.toSorted((left, right) =>
      BigInt(left[0]) < BigInt(right[0]) ? -1 : 1,
    );
  }
  return { streams };
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
