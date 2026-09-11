export type DrainOutcome = 'ok' | 'badSignature' | 'disabled' | 'malformedBody';  // notFound is aggregated, not per-drain

export type SinkHealthState = 'ok' | 'retrying' | 'failed';

export type SinkHealth = {
  state: SinkHealthState;
  consecutiveFailures: number;
  lastError: string | null;
  lastErrorAt: number | null;
  lastSuccessAt: number | null;
  nextRetryAt: number | null;
};

export type DrainRequestCounters = {
  ok: number;
  badSignature: number;
  disabled: number;
  malformedBody: number;
};

export type DrainStatus = {
  id: string;
  name: string;
  enabled: boolean;
  eventsReceived: number;
  lastEventAt: number | null;
  requests: DrainRequestCounters;
};

export type SinkCounters = { delivered: number; dropped: number; deadLettered: number };

export type SinkStatus = {
  name: string;
  type: string;
  enabled: boolean;
  health: SinkHealth;
  queue: { files: number; bytes: number; oldestAgeSec: number | null };
  counters: SinkCounters;
};

export type VolumeStatus = { path: string; freeBytes: number; totalBytes: number };

export type OrphanedSpool = { name: string; files: number; bytes: number };

export type RejectRecord = { drainId: string; index: number; reason: string; snippet: string };

export type ErrorRecord = { scope: string; message: string; at: number };

export type StatusSnapshot = {
  service: {
    state: 'ok' | 'degraded';
    uptimeSec: number;
    version: string;
    startedAt: number;
    /**
     * Requests to a drain id that is not configured, aggregated rather than
     * counted per id. A non-zero and climbing value means something is probing
     * the endpoint, or a drain was deleted while Vercel still has its URL.
     */
    unknownDrainRequests: number;
  };
  volumes: { config: VolumeStatus; spool: VolumeStatus };
  drains: DrainStatus[];
  sinks: SinkStatus[];
  orphanedSpools: OrphanedSpool[];
  /**
   * `events` is `unknown[]` on purpose. It is opaque JSON forwarded to the
   * browser for display and never inspected by type-dependent logic.
   *
   * Do NOT "improve" this to `JsonValue[]`: that needs an import, and this
   * file must stay import-free so the Node server and the Vite browser bundle
   * can both consume it without module-resolution friction. Typing it costs
   * the shared contract and buys nothing, since nothing here reads the values.
   */
  recent: { events: unknown[]; rejects: RejectRecord[]; errors: ErrorRecord[] };
};
