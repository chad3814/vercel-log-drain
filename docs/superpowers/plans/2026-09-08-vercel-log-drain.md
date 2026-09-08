# Vercel Log Drain Service — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Dockerized service that receives Vercel Drain deliveries, spools each event to disk per sink for durability, and delivers to a local-file sink and a Loki sink, with a browser-managed configuration and a read-only status page.

**Architecture:** A single Node process runs a Hono server (drain endpoint, admin API, static React SPA) plus a Dispatcher owning one async worker per enabled sink. Ingest verifies the drain HMAC, decodes the body, filters per sink, writes a batch file to that sink's spool directory, fsyncs, and only then acknowledges Vercel. Each worker drains its own directory oldest-first and unlinks a batch only after the sink accepts it, which makes restarts and sink outages non-lossy.

**Tech Stack:** TypeScript 7.0 (strict), Node 24, Hono 4.13 + @hono/node-server 2.1, Zod 4.5, pino 10.3, React 19.2 + Vite 8.2, Vitest 5.0, ESLint 10.10 + @typescript-eslint 8.70, Prettier 3.9, Docker (node:24-alpine).

**Spec:** `docs/superpowers/specs/2026-09-08-vercel-log-drain-design.md` — read it before starting. This plan implements that spec and does not restate its rationale.

## Global Constraints

Every task's requirements implicitly include this section.

- **No `any`. No `unknown`.** Where JSON of unknown shape must be typed, use `JsonValue` from `types/json.ts`. This is a project rule enforced by ESLint.
- **No synchronous I/O.** Never `fs.*Sync`, `zlib.*Sync`, or any `*Sync` call in `src/` or `web/`. Use `node:fs/promises` and `promisify`'d `zlib`. Enforced by an ESLint `no-restricted-syntax` rule.
- **Formatting:** 2-space indent; always terminate statements with semicolons, including optional ones. Enforced by Prettier.
- **TypeScript:** `strict`, `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes` all on. Module resolution `nodenext`; server imports use `.js` extensions for local files (compiled ESM output).
- **Node version floor:** 24. `fs.statfs`, `net.BlockList`, and `FileHandle.sync()` are all used and require it.
- **Vercel signature scheme (verified against docs 2026-09-08):** `x-vercel-signature` is the hex `HMAC-SHA1` of the **raw** request body, keyed with the drain secret. 40 hex characters. Verify before decompressing or parsing.
- **Sink name pattern:** `^[a-z0-9][a-z0-9-]{0,63}$`. A sink name is also a directory name; nothing else is acceptable.
- **Spool file naming:** `String(seq).padStart(12, '0') + '.jsonl'`. Twelve digits, so lexicographic order equals numeric order (verified).
- **Durable write protocol:** write `<name>.tmp` → `FileHandle.sync()` → `close()` → `rename()` → open the containing directory and `sync()` it. All four steps, in that order (verified on darwin and required on Linux).
- **Every task ends green:** `npm run lint && npm run typecheck && npm test` must pass before the task's commit.
- **Commit per task**, using Conventional Commit prefixes (`feat:`, `test:`, `chore:`, `docs:`, `fix:`).

## File Structure

| Path | Responsibility |
|---|---|
| `types/json.ts` | `JsonPrimitive`, `JsonValue`. The project's answer to `unknown`. |
| `types/index.ts` | Type-only re-exports consumed by the SPA. Type-only so zod never enters the browser bundle. |
| `src/log.ts` | pino logger factory with secret redaction. |
| `src/vercel/event.ts` | `LogEvent` zod schema (lenient, JSON-validated catchall) and level helpers. |
| `src/vercel/signature.ts` | Timing-safe HMAC-SHA1 verification. Pure. |
| `src/vercel/decode.ts` | gunzip + json/ndjson sniffing + per-entry validation. Pure apart from zlib. |
| `src/pipeline/filter.ts` | Compiles a `SinkFilter` into a predicate. |
| `src/sinks/types.ts` | `Sink`, `SinkType`, `SinkContext`, delivery error classes. No dependencies. |
| `src/sinks/file.ts` | File sink: date partitioning, append+fsync, retention, containment. |
| `src/sinks/loki-payload.ts` | Pure Loki payload/label construction. Separated from transport so it is trivially testable. |
| `src/sinks/loki.ts` | Loki transport: gzip, auth headers, timeout, error classification. |
| `src/sinks/registry.ts` | Maps type name → `SinkType`. Imported by config schema; imports the sinks. |
| `src/config/schema.ts` | Zod schemas and inferred types for the whole config. |
| `src/config/store.ts` | Atomic load/save, default bootstrap, etag concurrency, `.bak` retention. |
| `src/config/redact.ts` | Write-only secret handling: redact on read, restore on write. |
| `src/pipeline/spool.ts` | `SpoolQueue`: durable FIFO of batch files with byte budget, dead-letter, free-space guard. |
| `src/pipeline/dispatcher.ts` | Worker loops, backoff, health, config reconciliation, orphan handling. |
| `src/status/metrics.ts` | Counters, sink health, ring buffers. In-memory only. |
| `src/server/types.ts` | `AppEnv` (Hono bindings + variables). |
| `src/server/middleware/proxy-auth.ts` | Fail-closed reverse-proxy identity check with injected peer resolver. |
| `src/server/routes/drain.ts` | `POST /api/drain/:drainId`. |
| `src/server/routes/status.ts` | `GET /api/status`, `/healthz`, `/readyz`. |
| `src/server/routes/admin.ts` | Config read/write, sink test, orphan discard. |
| `src/server/app.ts` | Hono app assembly and static asset serving. |
| `src/index.ts` | Entrypoint: env parsing, writability probe, boot, graceful shutdown. |
| `web/src/api.ts` | Typed fetch client for the admin API. |
| `web/src/views/{Status,Drains,Sinks}.tsx` | The three SPA views. |
| `Dockerfile`, `docker-compose.example.yml`, `examples/{Caddyfile,nginx.conf}` | Packaging and the documented auth split. |

## Interface Contracts

These are the exact names and types tasks share. A task implementer sees only
their own task, so anything crossing a task boundary is fixed here. Do not
rename or re-shape these.

```ts
// types/json.ts
export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

// src/vercel/event.ts
export const jsonValueSchema: z.ZodType<JsonValue>;
export const logEventSchema;                       // z.object({...}).catchall(jsonValueSchema)
export type LogEvent = z.infer<typeof logEventSchema>;
export type EventLevel = 'info' | 'warning' | 'error';
export function eventLevel(event: LogEvent): EventLevel;
export function levelRank(level: EventLevel): number;

// src/vercel/signature.ts
export function verifySignature(raw: Buffer, header: string | undefined, secret: string): boolean;

// src/vercel/decode.ts
export type RejectedEntry = { index: number; reason: string; snippet: string };
export type DecodeResult = { events: LogEvent[]; rejected: RejectedEntry[] };
export type DecodeOptions = { gzipped: boolean; maxDecompressedBytes: number };
export class PayloadTooLargeError extends Error {}
export async function decodeBody(raw: Buffer, options: DecodeOptions): Promise<DecodeResult>;

// src/pipeline/filter.ts
export type EventPredicate = (event: LogEvent) => boolean;
export function compileFilter(filter: SinkFilter): EventPredicate;

// src/sinks/types.ts
export class RetryableDeliveryError extends Error {}
export class PermanentDeliveryError extends Error {}
export interface SinkContext { log: Logger; }
export interface Sink {
  readonly name: string;
  readonly type: string;
  deliver(events: LogEvent[]): Promise<void>;
  close(): Promise<void>;
}
export interface SinkType<TConfig> {
  readonly type: string;
  readonly configSchema: z.ZodType<TConfig>;
  create(name: string, config: TConfig, ctx: SinkContext): Sink;
  warnings(config: TConfig): string[];
}

// src/config/schema.ts
export const SINK_NAME_PATTERN: RegExp;
export type SinkFilter = { minLevel?: EventLevel; sources?: string[]; environments?: string[]; projectIds?: string[] };
export type DrainEntry = { id: string; name: string; secret: string; enabled: boolean; createdAt: number };
export type SinkEntry = { name: string; enabled: boolean; filter: SinkFilter; maxSpoolBytes: number; maxBatchEvents: number; maxBatchBytes: number; config: FileSinkConfig | LokiSinkConfig };
export type ServerConfig = { maxBodyBytes: number; maxDecompressedBytes: number; spoolFreeSpaceFloorBytes: number };
export type AppConfig = { version: 1; drains: DrainEntry[]; sinks: SinkEntry[]; server: ServerConfig };
export const appConfigSchema: z.ZodType<AppConfig>;
export function defaultAppConfig(): AppConfig;

// src/config/store.ts
export type LoadedConfig = { config: AppConfig; etag: string };
export class ConfigInvalidError extends Error {}
export class EtagMismatchError extends Error {}
export class ConfigStore {
  constructor(dir: string);
  load(): Promise<LoadedConfig>;
  save(config: AppConfig, expectedEtag: string | null): Promise<LoadedConfig>;
}
export function etagOf(config: AppConfig): string;

// src/config/redact.ts
export type RedactedDrain = Omit<DrainEntry, 'secret'> & { secret: null; hasSecret: boolean };
export type RedactedConfig = { version: 1; drains: RedactedDrain[]; sinks: SinkEntry[]; server: ServerConfig };
export function redactConfig(config: AppConfig): RedactedConfig;
export function restoreSecrets(incoming: JsonValue, current: AppConfig): AppConfig;

// src/status/metrics.ts
export type DrainOutcome = 'ok' | 'badSignature' | 'notFound' | 'disabled' | 'malformedBody';
export type SinkHealthState = 'ok' | 'retrying' | 'failed';
export type SinkHealth = { state: SinkHealthState; consecutiveFailures: number; lastError: string | null; lastErrorAt: number | null; lastSuccessAt: number | null; nextRetryAt: number | null };
export class Metrics {
  recordDrainRequest(drainId: string, outcome: DrainOutcome): void;
  recordEventsReceived(drainId: string, count: number, latestTimestampMs: number): void;
  recordRejected(drainId: string, entries: RejectedEntry[]): void;
  recordDelivered(sinkName: string, count: number): void;
  recordDropped(sinkName: string, count: number): void;
  recordDeadLettered(sinkName: string, count: number): void;
  setSinkHealth(sinkName: string, health: SinkHealth): void;
  getSinkHealth(sinkName: string): SinkHealth;
  pushRecentEvents(events: LogEvent[]): void;
  recordError(scope: string, message: string): void;
  forgetSink(sinkName: string): void;
  snapshot(): MetricsSnapshot;
}

// src/pipeline/spool.ts
export type FreeSpaceProbe = (path: string) => Promise<number>;
export const statfsFreeSpace: FreeSpaceProbe;
export type SpoolOptions = { maxSpoolBytes: number; freeSpaceFloorBytes: number; freeSpace?: FreeSpaceProbe };
export type SpoolBatch = { files: string[]; events: LogEvent[]; bytes: number };
export type EnqueueResult = { writtenBytes: number; droppedEvents: number };
export class SpoolQueue {
  static open(dir: string, options: SpoolOptions): Promise<SpoolQueue>;
  enqueue(events: LogEvent[]): Promise<EnqueueResult>;
  nextBatch(maxEvents: number, maxBytes: number): Promise<SpoolBatch | null>;
  ack(batch: SpoolBatch): Promise<void>;
  deadLetter(batch: SpoolBatch): Promise<void>;
  bytes(): number;
  fileCount(): number;
  oldestMtimeMs(): Promise<number | null>;
  discardAll(): Promise<void>;
}

// src/sinks/file.ts
export type FileSinkConfig = { type: 'file'; directory: string; filePrefix: string; retentionDays: number; freeSpaceFloorBytes: number };
export const fileSinkType: SinkType<FileSinkConfig>;
export function utcDateKey(timestampMs: number): string;              // '2026-09-08'
export function dailyFileName(prefix: string, timestampMs: number): string;  // 'events-2026-09-08.jsonl'
export function groupByUtcDate(events: LogEvent[]): Map<string, LogEvent[]>;
export function pruneRetention(dir: string, prefix: string, retentionDays: number, nowMs: number): Promise<string[]>;
export function resolveLogsDirectory(candidate: string, logsRoot: string): string;

// src/sinks/loki-payload.ts
export type LokiLabelConfig = { static: Record<string, string>; fromFields: string[] };
export type LokiStream = { stream: Record<string, string>; values: [string, string][] };
export type LokiPushPayload = { streams: LokiStream[] };
export const HIGH_CARDINALITY_FIELDS: readonly string[];
export function sanitizeLabelName(name: string): string;
export function resolveLabels(event: LogEvent, config: LokiLabelConfig): Record<string, string>;
export function buildPushPayload(events: LogEvent[], config: LokiLabelConfig): LokiPushPayload;
export function labelWarnings(config: LokiLabelConfig): string[];
export function normalizePushUrl(base: string): string;

// src/sinks/loki.ts
export type LokiAuth = { kind: 'none' } | { kind: 'basic'; username: string; password: string } | { kind: 'bearer'; token: string };
export type LokiSinkConfig = { type: 'loki'; url: string; auth: LokiAuth; tenantId: string | null; labels: LokiLabelConfig; timeoutMs: number };
export type LokiClassification = 'ok' | 'permanent' | 'retryable' | 'auth';
export function classifyLokiStatus(status: number): LokiClassification;
export const lokiSinkType: SinkType<LokiSinkConfig>;

// src/sinks/registry.ts
export type AnySinkType = SinkType<FileSinkConfig> | SinkType<LokiSinkConfig>;
export function sinkTypeFor(type: 'file' | 'loki'): AnySinkType;
export function createSink(entry: SinkEntry, ctx: SinkContext): Sink;
export function sinkEntryWarnings(entry: SinkEntry): string[];

// src/pipeline/dispatcher.ts
export type OrphanedSpool = { name: string; files: number; bytes: number };
export type SinkStatus = { name: string; type: string; enabled: boolean; health: SinkHealth; queue: { files: number; bytes: number; oldestAgeSec: number | null }; counters: { delivered: number; dropped: number; deadLettered: number } };
export type TestSinkResult = { ok: boolean; detail: string };
export type DispatcherOptions = { spoolRoot: string; logsRoot: string; metrics: Metrics; log: Logger; now?: () => number; random?: () => number; freeSpace?: FreeSpaceProbe };
export function backoffDelayMs(consecutiveFailures: number, baseMs: number, capMs: number, random: () => number): number;
export class Dispatcher {
  constructor(options: DispatcherOptions);
  applyConfig(config: AppConfig): Promise<void>;
  enqueue(events: LogEvent[]): Promise<void>;
  start(): void;
  stop(deadlineMs: number): Promise<void>;
  listOrphanedSpools(): Promise<OrphanedSpool[]>;
  discardOrphan(name: string): Promise<void>;
  testSink(name: string): Promise<TestSinkResult>;
  snapshotSinks(): Promise<SinkStatus[]>;
  isDegraded(): boolean;
}

// src/server/middleware/proxy-auth.ts
export type AuthConfig =
  | { mode: 'unset' }
  | { mode: 'disabled' }
  | { mode: 'proxy'; trustedProxies: string[]; userHeader: string; allowedUsers: string[] | null };
export type PeerResolver = (c: Context<AppEnv>) => string | undefined;
export const nodePeerResolver: PeerResolver;
export function parseAuthConfig(env: Record<string, string | undefined>): AuthConfig;
export function proxyAuth(config: AuthConfig, resolvePeer: PeerResolver): MiddlewareHandler<AppEnv>;
```

---
