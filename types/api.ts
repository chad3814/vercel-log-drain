export type DrainOutcome = 'ok' | 'badSignature' | 'notFound' | 'disabled' | 'malformedBody';

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
  notFound: number;
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
  service: { state: 'ok' | 'degraded'; uptimeSec: number; version: string; startedAt: number };
  volumes: { config: VolumeStatus; spool: VolumeStatus };
  drains: DrainStatus[];
  sinks: SinkStatus[];
  orphanedSpools: OrphanedSpool[];
  /** Opaque JSON passed straight to the browser for display. Never inspected. */
  recent: { events: unknown[]; rejects: RejectRecord[]; errors: ErrorRecord[] };
};
