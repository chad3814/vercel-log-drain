import type { LogEvent } from '../vercel/event.js';
import type { RejectedEntry } from '../vercel/decode.js';
import type {
  DrainOutcome,
  DrainRequestCounters,
  ErrorRecord,
  RejectRecord,
  SinkCounters,
  SinkHealth,
} from '../../types/api.js';

const RECENT_EVENT_LIMIT = 200;
const RECENT_RECORD_LIMIT = 100;

export type MetricsSnapshot = {
  uptimeSec: number;
  startedAt: number;
  drains: {
    id: string;
    eventsReceived: number;
    lastEventAt: number | null;
    requests: DrainRequestCounters;
  }[];
  sinkCounters: Record<string, SinkCounters>;
  sinkHealth: Record<string, SinkHealth>;
  recent: { events: LogEvent[]; rejects: RejectRecord[]; errors: ErrorRecord[] };
};

export function initialSinkHealth(): SinkHealth {
  return {
    state: 'ok',
    consecutiveFailures: 0,
    lastError: null,
    lastErrorAt: null,
    lastSuccessAt: null,
    nextRetryAt: null,
  };
}

function emptyRequestCounters(): DrainRequestCounters {
  return { ok: 0, badSignature: 0, notFound: 0, disabled: 0, malformedBody: 0 };
}

function pushBounded<T>(buffer: T[], items: T[], limit: number): void {
  buffer.push(...items);
  if (buffer.length > limit) buffer.splice(0, buffer.length - limit);
}

type DrainCounters = {
  eventsReceived: number;
  lastEventAt: number | null;
  requests: DrainRequestCounters;
};

export class Metrics {
  private readonly startedAt = Date.now();
  private readonly drains = new Map<string, DrainCounters>();
  private readonly sinkCounters = new Map<string, SinkCounters>();
  private readonly sinkHealth = new Map<string, SinkHealth>();
  private readonly recentEvents: LogEvent[] = [];
  private readonly recentRejects: RejectRecord[] = [];
  private readonly recentErrors: ErrorRecord[] = [];

  private drainCounters(drainId: string): DrainCounters {
    const existing = this.drains.get(drainId);
    if (existing !== undefined) return existing;
    const created: DrainCounters = {
      eventsReceived: 0,
      lastEventAt: null,
      requests: emptyRequestCounters(),
    };
    this.drains.set(drainId, created);
    return created;
  }

  private counters(sinkName: string): SinkCounters {
    const existing = this.sinkCounters.get(sinkName);
    if (existing !== undefined) return existing;
    const created: SinkCounters = { delivered: 0, dropped: 0, deadLettered: 0 };
    this.sinkCounters.set(sinkName, created);
    return created;
  }

  recordDrainRequest(drainId: string, outcome: DrainOutcome): void {
    this.drainCounters(drainId).requests[outcome] += 1;
  }

  recordEventsReceived(drainId: string, count: number, latestTimestampMs: number): void {
    const counters = this.drainCounters(drainId);
    counters.eventsReceived += count;
    if (counters.lastEventAt === null || latestTimestampMs > counters.lastEventAt) {
      counters.lastEventAt = latestTimestampMs;
    }
  }

  recordRejected(drainId: string, entries: RejectedEntry[]): void {
    pushBounded(
      this.recentRejects,
      entries.map((entry) => ({ drainId, ...entry })),
      RECENT_RECORD_LIMIT,
    );
  }

  recordDelivered(sinkName: string, count: number): void {
    this.counters(sinkName).delivered += count;
  }

  recordDropped(sinkName: string, count: number): void {
    this.counters(sinkName).dropped += count;
  }

  recordDeadLettered(sinkName: string, count: number): void {
    this.counters(sinkName).deadLettered += count;
  }

  setSinkHealth(sinkName: string, health: SinkHealth): void {
    this.sinkHealth.set(sinkName, health);
  }

  getSinkHealth(sinkName: string): SinkHealth {
    return this.sinkHealth.get(sinkName) ?? initialSinkHealth();
  }

  pushRecentEvents(events: LogEvent[]): void {
    pushBounded(this.recentEvents, events, RECENT_EVENT_LIMIT);
  }

  recordError(scope: string, message: string): void {
    pushBounded(this.recentErrors, [{ scope, message, at: Date.now() }], RECENT_RECORD_LIMIT);
  }

  forgetSink(sinkName: string): void {
    this.sinkCounters.delete(sinkName);
    this.sinkHealth.delete(sinkName);
  }

  snapshot(): MetricsSnapshot {
    return {
      uptimeSec: Math.floor((Date.now() - this.startedAt) / 1000),
      startedAt: this.startedAt,
      drains: [...this.drains.entries()].map(([id, counters]) => ({ id, ...counters })),
      sinkCounters: Object.fromEntries(this.sinkCounters),
      sinkHealth: Object.fromEntries(this.sinkHealth),
      recent: {
        events: [...this.recentEvents],
        rejects: [...this.recentRejects],
        errors: [...this.recentErrors],
      },
    };
  }
}
