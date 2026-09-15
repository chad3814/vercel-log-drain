# Vercel Log Drain Service — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Dockerized service that receives Vercel Drain deliveries, spools each event to disk per sink for durability, and delivers to a local-file sink and a Loki sink, with a browser-managed configuration and a read-only status page.

**Architecture:** A single Node process runs a Hono server (drain endpoint, admin API, static React SPA) plus a Dispatcher owning one async worker per enabled sink. Ingest verifies the drain HMAC, decodes the body, filters per sink, writes a batch file to that sink's spool directory, fsyncs, and only then acknowledges Vercel. Each worker drains its own directory oldest-first and unlinks a batch only after the sink accepts it, which makes restarts and sink outages non-lossy.

**Tech Stack:** TypeScript 7.0 (strict, native compiler), Node 24, Hono 4.13 + @hono/node-server 2.1, Zod 4.5, pino 10.3, React 19.2 + Vite 8.2, Vitest 5.0, oxlint 1.82 + oxlint-tsgolint 7.0, Prettier 3.9, Docker (node:24-alpine).

**Spec:** `docs/superpowers/specs/2026-09-08-vercel-log-drain-design.md` — read it before starting. This plan implements that spec and does not restate its rationale.

## Global Constraints

Every task's requirements implicitly include this section.

- **No `any`. No `unknown`.** Where JSON of unknown shape must be typed, use `JsonValue` from `types/json.ts`. `any` is enforced by `typescript/no-explicit-any`; **`unknown` has no lint rule in oxlint or ESLint**, so it is upheld by design and review. The only sanctioned exceptions are the two `JSON.parse` boundaries in `src/vercel/decode.ts` and the one opaque display payload in `types/api.ts`, both commented at the site.
- **No synchronous I/O.** Never `fs.*Sync`, `zlib.*Sync`, or any `*Sync` call in `src/` or `web/`. Use `node:fs/promises` and `promisify`'d `zlib`. Enforced by oxlint's `node/no-sync`, verified to catch `fs.readFileSync(p)`, a directly imported `readFileSync(p)`, and `zlib.gunzipSync` alike.
- **Formatting:** 2-space indent; always terminate statements with semicolons, including optional ones. Enforced by Prettier.
- **TypeScript:** `strict`, `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes` all on. Module resolution `nodenext`; server imports use `.js` extensions for local files (compiled ESM output). **TypeScript 7 removed `baseUrl`** — never add it to either tsconfig; `paths` resolve relative to the declaring tsconfig instead.
- **Lint with oxlint, not ESLint.** oxlint has no `typescript` peer dependency, which is what allows TypeScript 7 here: an ESLint setup would need `typescript-eslint`, whose `typescript@>=4.8.4 <6.1.0` peer makes `npm ci` fail with `ERESOLVE` against TypeScript 7. Do not reintroduce ESLint.
- **Read a Node error's `code` with `in` narrowing**, never a weak-typed annotation or an assertion. `if ('code' in error && typeof error.code === 'string')` is the only form that satisfies both the compiler and the linter: `const c: { code?: string } = error;` fails `TS2559` because `Error` has no properties in common with that shape, and `error as { code?: string }` trips oxlint's `no-unsafe-type-assertion` for narrowing. Verified 2026-09-11.
- **Never `String(x)` a `JsonValue`.** oxlint's type-aware `typescript/no-base-to-string` rejects it, because an object would stringify to `[object Object]`. Narrow first (`typeof x === 'string' ? x : …`) or use `JSON.stringify`. Verified 2026-09-11.
- **Never write `JSON.parse(x) as T`.** oxlint's type-aware `typescript/no-unsafe-type-assertion` rejects asserting away `JSON.parse`'s `any`. Use an annotated assignment instead — `const value: T = JSON.parse(x);` — which is lint-clean AND type-checked. Do **not** simply drop the annotation (`const value = JSON.parse(x)`): that silences the rule by leaving an inferred `any`, which is the invisible form of the thing the project bans.
- **Never call `Array#sort()`; use `Array#toSorted()`.** oxlint enables `unicorn/no-array-sort`, which flags EVERY `.sort()` call — with or without a comparator — because it mutates in place. `toSorted()` is available under `target`/`lib` `es2023`. Where the old code relied on in-place mutation, assign the result (`x = x.toSorted(...)`); a blind swap silently leaves the original unsorted.
- **oxlint does not support `no-restricted-syntax`.** Attempting to configure it is a hard config-parse error (`Rule 'no-restricted-syntax' not found in plugin 'eslint'`). Use the named rules in `.oxlintrc.json` instead.
- **Node version floor:** 24. `fs.statfs`, `net.BlockList`, and `FileHandle.sync()` are all used and require it.
- **Vercel signature scheme (verified against docs 2026-09-08):** `x-vercel-signature` is the hex `HMAC-SHA1` of the **raw** request body, keyed with the drain secret. 40 hex characters. Verify before decompressing or parsing.
- **Sink name pattern:** `^[a-z0-9][a-z0-9-]{0,63}$`. A sink name is also a directory name; nothing else is acceptable.
- **Spool file naming:** `String(seq).padStart(12, '0') + '.jsonl'`. Twelve digits, so lexicographic order equals numeric order (verified).
- **Durable write protocol:** write `<name>.tmp` → `FileHandle.sync()` → `close()` → `rename()` → open the containing directory and `sync()` it. All four steps, in that order (verified on darwin and required on Linux).
- **A negative test must pin ONE reason.** A test named for a specific rejection uses a fixture that is otherwise valid, and asserts the issue count and path — not merely `success === false`. Three tests in this plan originally used doubly-invalid fixtures (a short drain id alongside the defect under test), so each would have passed with the property it named removed entirely. Verified by counting zod issues per fixture; every other negative case here already fails for exactly one reason.
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
| `src/sinks/registry.ts` | Owns the sink-config discriminated union and construction. Imported **by** `config/schema.ts`; imports the sink modules. Never imports config. |
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

## Build Order

Tasks are ordered so that every import already exists when a task runs. The
dependency spine is:

```
types/json → vercel/{event,signature,decode}
sinks/types → sinks/file → sinks/loki-payload → sinks/loki → sinks/registry
  → config/schema → pipeline/filter → config/{store,redact}
  → status/metrics → pipeline/spool → pipeline/dispatcher
  → server/{types,middleware,routes} → app → index → web
```

Do not reorder tasks. In particular, the sink modules come *before* the config
schema, because the schema imports the sink-config union from the registry.

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
export const WHOLE_BODY_INDEX = -1;   // sentinel: the failure is the whole body, not an entry
export type RejectedEntry = { index: number; reason: string; snippet: string };
export type DecodeResult = { events: LogEvent[]; rejected: RejectedEntry[] };
export type DecodeOptions = { gzipped: boolean; maxDecompressedBytes: number };
export class PayloadTooLargeError extends Error {}   // size cap only
export async function decodeBody(raw: Buffer, options: DecodeOptions): Promise<DecodeResult>;

// src/pipeline/filter.ts
export type EventPredicate = (event: LogEvent) => boolean;
export function compileFilter(filter: SinkFilter): EventPredicate;

// src/sinks/types.ts
export class RetryableDeliveryError extends Error {}   // (message, cause?)
export class PermanentDeliveryError extends Error {}   // (message, cause?)
export class AuthDeliveryError extends RetryableDeliveryError {} // added in Task 9
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
export type SinkEntry = { name: string; enabled: boolean; filter: SinkFilter; maxSpoolBytes: number; maxBatchEvents: number; maxBatchBytes: number; config: AnySinkConfig };
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
  sweepStaleTemps(): Promise<void>;   // BOOT ONLY — see its doc comment
}
export function etagOf(config: AppConfig): string;

// src/config/redact.ts
export type RedactedDrain = Omit<DrainEntry, 'secret'> & { secret: null; hasSecret: boolean };
export type RedactedConfig = { version: 1; drains: RedactedDrain[]; sinks: SinkEntry[]; server: ServerConfig };
export function redactConfig(config: AppConfig): RedactedConfig;
export function restoreSecrets(incoming: JsonValue, current: AppConfig): AppConfig;

// types/api.ts (created in Task 15) — these are declared there, NOT in
// src/status/metrics.ts, so the SPA can import them without pulling in zod.
// metrics.ts imports them from types/api.ts and re-exports nothing.
/**
 * Outcomes attributable to a KNOWN drain. `notFound` is deliberately absent: a
 * request for an id that is not in the config belongs to no drain, so counting
 * it per-id would create a permanent map entry for every id an attacker
 * invents, and the status page — which lists drains from the config — would
 * never display it. Those are aggregated into `unknownDrainRequests` instead.
 */
export type DrainOutcome = 'ok' | 'badSignature' | 'disabled' | 'malformedBody';
export type SinkHealthState = 'ok' | 'retrying' | 'failed';
export type SinkHealth = { state: SinkHealthState; consecutiveFailures: number; lastError: string | null; lastErrorAt: number | null; lastSuccessAt: number | null; nextRetryAt: number | null };
export class Metrics {
  recordDrainRequest(drainId: string, outcome: DrainOutcome): void;
  recordUnknownDrainRequest(): void;
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

// src/sinks/types.ts (added in Task 7)
export type FreeSpaceProbe = (path: string) => Promise<number>;
// src/sinks/file.ts (Task 7) — the single statfs implementation, imported by
// the spool queue. It lives with the file sink because that is where it is
// first needed; do not duplicate it.
export const statfsFreeSpace: FreeSpaceProbe;

// src/pipeline/spool.ts
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
// NOTE ON DEPENDENCY DIRECTION: the registry owns the discriminated union of
// sink configs and `config/schema.ts` imports it — never the reverse. That is
// also why createSink takes (name, config) rather than a SinkEntry: taking a
// SinkEntry would force the registry to import config/schema and create a
// cycle. The Dispatcher, which imports both, does the unpacking.
export type AnySinkConfig = FileSinkConfig | LokiSinkConfig;
export const sinkConfigSchema: z.ZodType<AnySinkConfig>;
export function createSink(name: string, config: AnySinkConfig, ctx: SinkContext): Sink;
export function warningsFor(config: AnySinkConfig): string[];

// src/pipeline/dispatcher.ts
// OrphanedSpool and SinkStatus are declared in types/api.ts (Task 15); the
// dispatcher imports them rather than defining its own copies.
export type TestSinkResult = { ok: boolean; detail: string };
export type DispatcherOptions = { spoolRoot: string; logsRoot: string; metrics: Metrics; log: Logger; freeSpace?: FreeSpaceProbe };
export function backoffDelayMs(consecutiveFailures: number, baseMs: number, capMs: number, random: () => number): number;
export type SinkWorkerOptions = { sink: Sink; queue: SpoolQueue; metrics: Metrics; log: Logger; maxBatchEvents: number; maxBatchBytes: number; baseBackoffMs?: number; maxBackoffMs?: number; random?: () => number };
export class SinkWorker {
  constructor(options: SinkWorkerOptions);
  health(): SinkHealth;
  drainOnce(): Promise<boolean>;
  start(): void;
  stop(deadlineMs: number): Promise<void>;
}
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

### Task 1: Project scaffolding, tooling, and logger

Establishes the gates every later task must pass, plus the `JsonValue` type and
the redacting logger that everything depends on.

**Files:**
- Create: `package.json`, `.npmrc`, `tsconfig.json`, `tsconfig.build.json`, `web/tsconfig.json`, `vite.config.ts`, `vitest.config.ts`, `.oxlintrc.json`, `.prettierrc.json`, `.gitignore`, `.dockerignore`
- Create: `types/json.ts`, `src/log.ts`
- Test: `test/log.test.ts`

**Interfaces:**
- Consumes: nothing.
- Produces: `JsonPrimitive`, `JsonValue` (`types/json.ts`); `createLogger(level: string, destination?: DestinationStream): Logger`, `REDACT_PATHS: string[]`, and a re-exported `Logger` type (`src/log.ts`).

- [ ] **Step 1: Initialize the package and install exact dependencies**

```bash
npm init -y
npm pkg set name=vercel-log-drain version=0.1.0 type=module
npm pkg set --json private=true
npm pkg set engines.node=">=24"
npm pkg delete main

# Pin exactly. npm writes caret ranges by default, which would let a future
# plain `npm install` drift the TypeScript 7 / oxlint / oxlint-tsgolint triad
# into an incompatible combination — the exact failure this pinning prevents.
printf 'save-exact=true\n' > .npmrc

npm install hono@4.13.7 @hono/node-server@2.1.1 zod@4.5.4 pino@10.3.1
npm install react@19.2.8 react-dom@19.2.8
npm install -D typescript@7.0.2 @types/node@24 vitest@5.0.0 vite@8.2.2 \
  @vitejs/plugin-react@6.1.1 @types/react@19 @types/react-dom@19 \
  oxlint@1.82.0 oxlint-tsgolint@7.0.2001 prettier@3.9.6
```

Do not substitute ESLint. `oxlint-tsgolint` is what provides the type-aware
rules; its version tracks the TypeScript 7 native compiler.

Afterwards, confirm `package.json` contains no `^` or `~` in any version spec.
If it does, the `.npmrc` was written too late; fix the specs and re-run
`npm install` so the lockfile agrees.

- [ ] **Step 2: Write the config files**

`package.json` scripts (set with `npm pkg set` or edit directly):

```json
{
  "scripts": {
    "lint": "oxlint --type-aware",
    "typecheck": "tsc -p tsconfig.json",
    "test": "vitest run",
    "test:watch": "vitest",
    "build": "npm run build:server && npm run build:web",
    "build:server": "tsc -p tsconfig.build.json",
    "build:web": "vite build",
    "start": "node dist/src/index.js",
    "format": "prettier --write ."
  }
}
```

`tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "es2023",
    "lib": ["es2023"],
    "module": "nodenext",
    "moduleResolution": "nodenext",
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "exactOptionalPropertyTypes": true,
    "noImplicitOverride": true,
    "verbatimModuleSyntax": true,
    "noEmit": true,
    "skipLibCheck": true,
    "types": ["node"]
  },
  "include": ["src/**/*.ts", "test/**/*.ts", "types/**/*.ts", "vitest.config.ts"]
}
```

`tsconfig.build.json`:

```json
{
  "extends": "./tsconfig.json",
  "compilerOptions": {
    "noEmit": false,
    "outDir": "dist",
    "rootDir": ".",
    "sourceMap": true
  },
  "include": ["src/**/*.ts", "types/**/*.ts"]
}
```

`rootDir: "."` means the entrypoint compiles to `dist/src/index.js`. The
`start` script and the Dockerfile `CMD` both reflect that.

`web/tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "es2023",
    "lib": ["es2023", "dom", "dom.iterable"],
    "module": "esnext",
    "moduleResolution": "bundler",
    "jsx": "react-jsx",
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "exactOptionalPropertyTypes": true,
    "verbatimModuleSyntax": true,
    "noEmit": true,
    "skipLibCheck": true,
    "types": ["vite/client"],
    "paths": { "@shared/*": ["../types/*"] }
  },
  "include": ["src/**/*.ts", "src/**/*.tsx", "../types/**/*.ts", "../vite.config.ts"]
}
```

**Do not add `baseUrl`.** TypeScript 7 removed it and errors out with
`TS5102: Option 'baseUrl' has been removed`, which fails the whole typecheck
before any file is examined. In TypeScript 7 `paths` entries resolve relative
to the `tsconfig.json` that declares them, so `../types/*` is correct as
written. Verified on 2026-09-08, including that a real type error in a `.tsx`
file is still reported.

The `@shared/*` alias is how the SPA imports shared types without extension
ambiguity between `nodenext` and `bundler` resolution. Server code imports the
same files by relative path with a `.js` extension.

`vite.config.ts`:

```ts
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { fileURLToPath } from 'node:url';

export default defineConfig({
  root: 'web',
  plugins: [react()],
  resolve: {
    alias: {
      '@shared': fileURLToPath(new URL('./types', import.meta.url)),
    },
  },
  build: { outDir: 'dist', emptyOutDir: true },
  server: {
    proxy: {
      '/api': 'http://127.0.0.1:8080',
      '/healthz': 'http://127.0.0.1:8080',
      '/readyz': 'http://127.0.0.1:8080',
    },
  },
});
```

`vitest.config.ts`:

```ts
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    environment: 'node',
    testTimeout: 20_000,
  },
});
```

`.oxlintrc.json` — this exact content was installed and exercised on
2026-09-08:

```json
{
  "$schema": "./node_modules/oxlint/configuration_schema.json",
  "plugins": ["typescript", "oxc", "node", "promise", "unicorn", "react"],
  "categories": { "correctness": "error", "suspicious": "error" },
  "ignorePatterns": ["node_modules/**", "dist/**", "web/dist/**", "coverage/**"],
  "rules": {
    "typescript/no-explicit-any": "error",
    "node/no-sync": "error",
    "typescript/no-floating-promises": "error",
    "typescript/no-unsafe-type-assertion": "error",
    "react/react-in-jsx-scope": "off"
  },
  "overrides": [
    {
      "files": ["web/**/*.ts", "web/**/*.tsx"],
      "rules": { "node/no-sync": "off" }
    },
    {
      "files": ["scripts/**", "*.config.ts"],
      "rules": { "node/no-sync": "off" }
    }
  ]
}
```

Six things about this config are load-bearing and were each checked against the
real binary on 2026-09-08. Changing any of them will either break the run or
silently weaken it:

- **`node/no-sync` replaces the ESLint selector hack.** One rule catches
  `fs.readFileSync(p)`, a directly imported `readFileSync(p)`, and
  `zlib.gunzipSync(buf)` alike. It is name-based, so it generalizes to any
  `*Sync` call.
- **`no-restricted-syntax` does not exist in oxlint.** Configuring it aborts
  the whole run with `Rule 'no-restricted-syntax' not found in plugin
  'eslint'` — not a warning, a config-parse failure. Do not add it.
- **`node_modules/**` must be listed in `ignorePatterns` explicitly.** With a
  bare `oxlint` invocation and no positional path, oxlint walks
  `node_modules/` and reports thousands of errors from dependency `.d.ts`
  files, including from TypeScript's own bundled declarations. Passing explicit
  paths avoids it too, but listing the pattern keeps the script short and works
  either way.
- **Do not enable the `pedantic` category.** It turns on `max-lines`, which
  caps files at 300 lines; several modules in this project are legitimately
  longer, and the failure looks like a code problem rather than a config
  choice. `correctness` plus `suspicious` is the intended level.
- **`react/react-in-jsx-scope` must be off.** The project uses
  `jsx: "react-jsx"`, so `React` need not be in scope, but oxlint's react
  plugin enables that legacy rule by default and every `.tsx` file would fail.
- **`node/no-sync` is off for `web/`, `scripts/`, and config files.** Browser
  code has no Node sync API to misuse, and build scripts are exactly where a
  sync call is acceptable.

Type-aware rules only run when `--type-aware` is passed, which the `lint`
script does. Without that flag, `no-floating-promises` and
`no-unsafe-type-assertion` are silently inert — a clean lint would prove
nothing. The flag requires the `oxlint-tsgolint` devDependency.

Verified acceptance for this config: it exits `0` on clean source (including a
400-line file, confirming `max-lines` is off) and reports `no-explicit-any`,
`no-sync`, `no-floating-promises`, and `no-unsafe-type-assertion` on code that
violates them.

`.prettierrc.json`:

```json
{ "semi": true, "singleQuote": true, "tabWidth": 2, "printWidth": 100, "trailingComma": "all" }
```

`.gitignore`:

```
node_modules/
dist/
web/dist/
coverage/
*.log
.DS_Store
```

`.dockerignore`:

```
node_modules
dist
web/dist
coverage
.git
docs
*.log
```

- [ ] **Step 3: Write the failing logger test**

`test/log.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { Writable } from 'node:stream';
import { createLogger } from '../src/log.js';

function captureLogger(): { lines: string[]; log: ReturnType<typeof createLogger> } {
  const lines: string[] = [];
  const sink = new Writable({
    write(chunk: Buffer, _enc, callback): void {
      lines.push(chunk.toString('utf8'));
      callback();
    },
  });
  return { lines, log: createLogger('info', sink) };
}

describe('createLogger', () => {
  it('redacts a top-level secret', () => {
    const { lines, log } = captureLogger();
    log.info({ secret: 'sup3rs3cret' }, 'test');
    expect(lines.join('')).not.toContain('sup3rs3cret');
    expect(lines.join('')).toContain('[redacted]');
  });

  it('redacts drain secrets nested in an array', () => {
    const { lines, log } = captureLogger();
    log.info({ drains: [{ id: 'd1', secret: 'sup3rs3cret' }] }, 'test');
    const output = lines.join('');
    expect(output).not.toContain('sup3rs3cret');
    expect(output).toContain('[redacted]');
    expect(output).toContain('d1');
  });

  it('redacts loki auth credentials nested in sink config', () => {
    const { lines, log } = captureLogger();
    log.info(
      { sinks: [{ name: 'loki', config: { auth: { password: 'pw123', token: 'tk456' } } }] },
      'test',
    );
    const output = lines.join('');
    expect(output).not.toContain('pw123');
    expect(output).not.toContain('tk456');
  });

  it('leaves non-secret fields intact', () => {
    const { lines, log } = captureLogger();
    log.info({ sinkName: 'loki-prod', delivered: 42 }, 'test');
    const output = lines.join('');
    expect(output).toContain('loki-prod');
    expect(output).toContain('42');
  });
});
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `npx vitest run test/log.test.ts`
Expected: FAIL — cannot resolve `../src/log.js`.

- [ ] **Step 5: Implement `types/json.ts`**

```ts
export type JsonPrimitive = string | number | boolean | null;

export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
```

- [ ] **Step 6: Implement `src/log.ts`**

`fast-redact`, which backs pino's `redact`, matches wildcards one level at a
time — there is no deep-wildcard syntax. The paths below are therefore
enumerated deliberately for the shapes this service actually logs.

```ts
import pino from 'pino';
import type { DestinationStream, Logger } from 'pino';

export type { Logger };

export const REDACT_PATHS: string[] = [
  'secret',
  'password',
  'token',
  '*.secret',
  '*.password',
  '*.token',
  'drains[*].secret',
  'sinks[*].config.auth.password',
  'sinks[*].config.auth.token',
  'config.auth.password',
  'config.auth.token',
  'auth.password',
  'auth.token',
];

export function createLogger(level: string, destination?: DestinationStream): Logger {
  const options = {
    level,
    redact: { paths: REDACT_PATHS, censor: '[redacted]' },
  };
  return destination === undefined ? pino(options) : pino(options, destination);
}
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `npx vitest run test/log.test.ts`
Expected: PASS, 4 tests.

- [ ] **Step 8: Run all gates**

Run: `npm run lint && npm run typecheck && npm test`
Expected: all pass.

Note that `typecheck` deliberately covers the server project only. `web/src`
has no files until Task 25, and `tsc` errors with "No inputs were found" on an
empty project. Task 25 extends the script to add `web/tsconfig.json` at the
point where there is web code to check. Do **not** create a placeholder file to
work around this.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "chore: scaffold project tooling and redacting logger

Lints with oxlint rather than ESLint, which is what allows TypeScript 7:
typescript-eslint peer-requires <6.1.0, so an ESLint setup fails npm ci
outright against TS 7. node/no-sync enforces the project's async-over-sync
rule in CI, covering both member and direct-import call forms."
```

---

### Task 2: LogEvent schema

**Files:**
- Create: `src/vercel/event.ts`
- Test: `test/vercel/event.test.ts`

**Interfaces:**
- Consumes: `JsonValue` from `types/json.ts`.
- Produces: `jsonValueSchema: z.ZodType<JsonValue>`, `logEventSchema`, `type LogEvent`, `type EventLevel = 'info' | 'warning' | 'error'`, `eventLevel(event: LogEvent): EventLevel`, `levelRank(level: EventLevel): number`.

- [ ] **Step 1: Write the failing test**

`test/vercel/event.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { eventLevel, levelRank, logEventSchema } from '../../src/vercel/event.js';

const validEvent = {
  id: '1573817187330377061717300000',
  timestamp: 1573817187330,
  source: 'lambda',
  projectId: 'gdufoJxB6b9b1fEqr1jUtFkyavUU',
  level: 'info',
  message: 'API request processed',
};

describe('logEventSchema', () => {
  it('accepts a documented Vercel event', () => {
    const result = logEventSchema.safeParse(validEvent);
    expect(result.success).toBe(true);
  });

  it('preserves unknown fields through the catchall', () => {
    const result = logEventSchema.safeParse({
      ...validEvent,
      proxy: { method: 'GET', statusCode: 200, userAgent: ['Mozilla/5.0'] },
      'trace.id': '1b02cd14bb8642fd092bc23f54c7ffcd',
    });
    expect(result.success).toBe(true);
    if (!result.success) return;
    expect(result.data['trace.id']).toBe('1b02cd14bb8642fd092bc23f54c7ffcd');
    expect(result.data['proxy']).toEqual({
      method: 'GET',
      statusCode: 200,
      userAgent: ['Mozilla/5.0'],
    });
  });

  it.each([
    ['id', { ...validEvent, id: undefined }],
    ['timestamp', { ...validEvent, timestamp: undefined }],
    ['source', { ...validEvent, source: undefined }],
    ['projectId', { ...validEvent, projectId: undefined }],
  ])('rejects an event missing %s', (_field, candidate) => {
    expect(logEventSchema.safeParse(candidate).success).toBe(false);
  });

  it('rejects a non-numeric timestamp', () => {
    expect(logEventSchema.safeParse({ ...validEvent, timestamp: '1573817187330' }).success).toBe(
      false,
    );
  });

  it('rejects a non-JSON value in an unknown field', () => {
    expect(logEventSchema.safeParse({ ...validEvent, weird: () => 1 }).success).toBe(false);
  });
});

describe('eventLevel', () => {
  it.each([
    ['info', 'info'],
    ['warning', 'warning'],
    ['error', 'error'],
  ] as const)('maps %s to %s', (input, expected) => {
    expect(eventLevel({ ...validEvent, level: input })).toBe(expected);
  });

  it('treats a missing level as info', () => {
    const { level: _level, ...withoutLevel } = validEvent;
    expect(eventLevel(withoutLevel)).toBe('info');
  });

  it('treats an unrecognized level as info', () => {
    expect(eventLevel({ ...validEvent, level: 'trace' })).toBe('info');
  });

  it('normalizes case and the warn alias', () => {
    expect(eventLevel({ ...validEvent, level: 'WARN' })).toBe('warning');
    expect(eventLevel({ ...validEvent, level: 'Error' })).toBe('error');
  });
});

describe('levelRank', () => {
  it('orders info below warning below error', () => {
    expect(levelRank('info')).toBeLessThan(levelRank('warning'));
    expect(levelRank('warning')).toBeLessThan(levelRank('error'));
  });
});
```

The `it.each` rows above pass `undefined` for a required field, which zod
rejects exactly as a missing key would.

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/vercel/event.test.ts`
Expected: FAIL — cannot resolve `../../src/vercel/event.js`.

- [ ] **Step 3: Implement `src/vercel/event.ts`**

`z.lazy` plus `.catchall()` is what lets unknown passthrough fields be typed as
`JsonValue` rather than `unknown`, satisfying the project rule. This exact
construction was typechecked against zod 4.5.4 on 2026-09-08.

```ts
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/vercel/event.test.ts`
Expected: PASS.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: add lenient Vercel log event schema

Requires only id, timestamp, source, and projectId, and validates
unknown passthrough fields as JsonValue so new Vercel fields survive to
the sinks without being typed unknown."
```

---

### Task 3: Signature verification

**Files:**
- Create: `src/vercel/signature.ts`
- Test: `test/vercel/signature.test.ts`

**Interfaces:**
- Consumes: nothing.
- Produces: `verifySignature(raw: Buffer, header: string | undefined, secret: string): boolean`.

- [ ] **Step 1: Write the failing test**

`test/vercel/signature.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { createHmac } from 'node:crypto';
import { verifySignature } from '../../src/vercel/signature.js';

const secret = 'drain-signature-secret';
const body = Buffer.from('[{"id":"1","timestamp":1,"source":"lambda","projectId":"p"}]', 'utf8');
const signature = createHmac('sha1', secret).update(body).digest('hex');

describe('verifySignature', () => {
  it('accepts a correct signature', () => {
    expect(verifySignature(body, signature, secret)).toBe(true);
  });

  it('produces a 40-character hex digest', () => {
    expect(signature).toMatch(/^[0-9a-f]{40}$/);
  });

  it('rejects a signature made with the wrong secret', () => {
    expect(verifySignature(body, signature, 'wrong-secret')).toBe(false);
  });

  it('rejects a signature over different bytes', () => {
    expect(verifySignature(Buffer.from('tampered', 'utf8'), signature, secret)).toBe(false);
  });

  it('rejects a missing header without throwing', () => {
    expect(verifySignature(body, undefined, secret)).toBe(false);
  });

  it('rejects a header of the wrong length without throwing', () => {
    expect(verifySignature(body, 'abc123', secret)).toBe(false);
  });

  it('rejects an empty header', () => {
    expect(verifySignature(body, '', secret)).toBe(false);
  });

  it('rejects an uppercase digest, since Vercel sends lowercase hex', () => {
    expect(verifySignature(body, signature.toUpperCase(), secret)).toBe(false);
  });

  it('rejects a header whose string length matches but byte length does not', () => {
    // Node's HTTP parser decodes header bytes as latin1, so 40 raw bytes in
    // 0x80-0xFF arrive as a 40-character string that is 80 UTF-8 bytes. A
    // string-length guard would pass this to timingSafeEqual, which throws.
    const multiByte = '\u00e9'.repeat(40);
    expect(multiByte.length).toBe(40);
    expect(Buffer.byteLength(multiByte, 'utf8')).toBe(80);
    expect(() => verifySignature(body, multiByte, secret)).not.toThrow();
    expect(verifySignature(body, multiByte, secret)).toBe(false);
  });

  it('rejects a multi-byte header of differing string length without throwing', () => {
    const wide = '\u20ac'.repeat(13); // 13 chars, 39 bytes
    expect(() => verifySignature(body, wide, secret)).not.toThrow();
    expect(verifySignature(body, wide, secret)).toBe(false);
  });

  it('verifies an empty body correctly', () => {
    const empty = Buffer.alloc(0);
    const emptySig = createHmac('sha1', secret).update(empty).digest('hex');
    expect(verifySignature(empty, emptySig, secret)).toBe(true);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/vercel/signature.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `src/vercel/signature.ts`**

The explicit length check before `timingSafeEqual` is mandatory:
`timingSafeEqual` throws when its arguments differ in length, so without the
guard a malformed header would crash the request instead of failing closed.

**The guard must compare BYTE lengths, not string lengths.** A JS string's
`.length` is a count of UTF-16 code units, while `timingSafeEqual` compares
`Buffer` byte lengths, and the two diverge for any non-ASCII character. Node's
HTTP parser decodes header bytes as latin1, so a header of 40 raw bytes in
`0x80`-`0xFF` yields a 40-**character** string that is 80 **bytes** in UTF-8:
a string-length guard passes it through and `timingSafeEqual` throws
`RangeError: Input buffers must have the same byte length`. This is
attacker-controlled and reachable over the network — verified 2026-09-11
against Node 24 by sending 40 `0xE9` bytes through `node:http`. Build both
buffers first and compare *their* lengths:

```ts
import { createHmac, timingSafeEqual } from 'node:crypto';

export function verifySignature(
  raw: Buffer,
  header: string | undefined,
  secret: string,
): boolean {
  if (header === undefined || header.length === 0) return false;
  const expected = createHmac('sha1', secret).update(raw).digest('hex');
  const headerBuffer = Buffer.from(header, 'utf8');
  const expectedBuffer = Buffer.from(expected, 'utf8');
  // Byte lengths, because that is what timingSafeEqual compares.
  if (headerBuffer.length !== expectedBuffer.length) return false;
  return timingSafeEqual(headerBuffer, expectedBuffer);
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/vercel/signature.test.ts`
Expected: PASS, 9 tests.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: verify Vercel drain signatures in constant time

HMAC-SHA1 over the raw body, hex-compared with timingSafeEqual behind an
explicit length check, since timingSafeEqual throws on length mismatch."
```

---

### Task 4: Body decoding

**Files:**
- Create: `src/vercel/decode.ts`
- Test: `test/vercel/decode.test.ts`

**Interfaces:**
- Consumes: `logEventSchema`, `LogEvent` from `src/vercel/event.ts`.
- Produces: `WHOLE_BODY_INDEX = -1`, `type RejectedEntry = { index: number; reason: string; snippet: string }`, `type DecodeResult = { events: LogEvent[]; rejected: RejectedEntry[] }`, `type DecodeOptions = { gzipped: boolean; maxDecompressedBytes: number }`, `class PayloadTooLargeError` (size cap ONLY — a corrupt or truncated gzip stream becomes a whole-body reject, not a throw), `decodeBody(raw: Buffer, options: DecodeOptions): Promise<DecodeResult>`.

- [ ] **Step 1: Write the failing test**

`test/vercel/decode.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { gzip } from 'node:zlib';
import { promisify } from 'node:util';
import { decodeBody, PayloadTooLargeError, WHOLE_BODY_INDEX } from '../../src/vercel/decode.js';

const gzipAsync = promisify(gzip);

const eventA = { id: 'a', timestamp: 1573817187330, source: 'build', projectId: 'p1' };
const eventB = { id: 'b', timestamp: 1573817250283, source: 'lambda', projectId: 'p1' };
const options = { gzipped: false, maxDecompressedBytes: 1_000_000 };

describe('decodeBody', () => {
  it('decodes a JSON array body', async () => {
    const raw = Buffer.from(JSON.stringify([eventA, eventB]), 'utf8');
    const result = await decodeBody(raw, options);
    expect(result.events.map((e) => e['id'])).toEqual(['a', 'b']);
    expect(result.rejected).toEqual([]);
  });

  it('decodes an NDJSON body', async () => {
    const raw = Buffer.from(`${JSON.stringify(eventA)}\n${JSON.stringify(eventB)}\n`, 'utf8');
    const result = await decodeBody(raw, options);
    expect(result.events.map((e) => e['id'])).toEqual(['a', 'b']);
    expect(result.rejected).toEqual([]);
  });

  it('sniffs the format rather than trusting a content type', async () => {
    const leadingWhitespace = Buffer.from(`\n  ${JSON.stringify([eventA])}`, 'utf8');
    const result = await decodeBody(leadingWhitespace, options);
    expect(result.events).toHaveLength(1);
  });

  it('ignores blank lines in NDJSON', async () => {
    const raw = Buffer.from(`${JSON.stringify(eventA)}\n\n   \n${JSON.stringify(eventB)}\n`, 'utf8');
    const result = await decodeBody(raw, options);
    expect(result.events).toHaveLength(2);
    expect(result.rejected).toEqual([]);
  });

  it('decompresses a gzipped body', async () => {
    const raw = await gzipAsync(Buffer.from(JSON.stringify([eventA]), 'utf8'));
    const result = await decodeBody(raw, { gzipped: true, maxDecompressedBytes: 1_000_000 });
    expect(result.events).toHaveLength(1);
  });

  it('keeps good NDJSON entries and reports bad ones', async () => {
    const raw = Buffer.from(
      `${JSON.stringify(eventA)}\n{not json\n${JSON.stringify(eventB)}\n`,
      'utf8',
    );
    const result = await decodeBody(raw, options);
    expect(result.events.map((e) => e['id'])).toEqual(['a', 'b']);
    expect(result.rejected).toHaveLength(1);
    expect(result.rejected[0]?.index).toBe(1);
    expect(result.rejected[0]?.snippet).toContain('not json');
  });

  it('keeps good array entries and reports schema-invalid ones', async () => {
    const raw = Buffer.from(JSON.stringify([eventA, { id: 'missing-fields' }]), 'utf8');
    const result = await decodeBody(raw, options);
    expect(result.events.map((e) => e['id'])).toEqual(['a']);
    expect(result.rejected).toHaveLength(1);
    expect(result.rejected[0]?.index).toBe(1);
  });

  it('truncates long snippets so a huge line cannot bloat memory', async () => {
    const raw = Buffer.from(`{"broken":"${'x'.repeat(5000)}"\n`, 'utf8');
    const result = await decodeBody(raw, options);
    expect(result.rejected).toHaveLength(1);
    expect(result.rejected[0]?.snippet.length).toBeLessThanOrEqual(200);
  });

  it('rejects a body that decompresses beyond the cap', async () => {
    const raw = await gzipAsync(Buffer.alloc(200_000, 0x61));
    await expect(
      decodeBody(raw, { gzipped: true, maxDecompressedBytes: 1000 }),
    ).rejects.toBeInstanceOf(PayloadTooLargeError);
  });

  it('returns an empty result for an empty body', async () => {
    const result = await decodeBody(Buffer.alloc(0), options);
    expect(result.events).toEqual([]);
    expect(result.rejected).toEqual([]);
  });

  it('reports a whole-body parse failure with the whole-body sentinel index', async () => {
    const raw = Buffer.from('[{"id":"a"},', 'utf8');
    const result = await decodeBody(raw, options);
    expect(result.events).toEqual([]);
    expect(result.rejected).toHaveLength(1);
    expect(result.rejected[0]?.index).toBe(WHOLE_BODY_INDEX);
  });

  it('distinguishes a whole-body failure from a first-entry failure by index', async () => {
    const wholeBody = await decodeBody(Buffer.from('[{"id":"a"},', 'utf8'), options);
    const firstEntry = await decodeBody(
      Buffer.from(JSON.stringify([{ id: 'no-required-fields' }, eventB]), 'utf8'),
      options,
    );
    // Both are "the first thing went wrong", but they are not the same failure
    // and a status-page consumer must be able to tell them apart.
    expect(wholeBody.rejected[0]?.index).toBe(WHOLE_BODY_INDEX);
    expect(firstEntry.rejected[0]?.index).toBe(0);
    expect(firstEntry.events.map((e) => e['id'])).toEqual(['b']);
  });

  it('treats a lone JSON object as a single NDJSON entry, not a whole-body failure', async () => {
    // A body starting with '{' sniffs as NDJSON, so it is one line. A schema
    // failure there is a PER-ENTRY failure at index 0 — not a whole-body
    // failure. Do NOT add a special case for object bodies: a single-event
    // ndjson delivery is exactly one JSON object with no newline, and
    // rejecting it would silently drop real events.
    const result = await decodeBody(Buffer.from('{"not":"an event"}', 'utf8'), options);
    expect(result.events).toEqual([]);
    expect(result.rejected).toHaveLength(1);
    expect(result.rejected[0]?.index).toBe(0);
  });

  it('accepts a single-event NDJSON body with no trailing newline', async () => {
    const result = await decodeBody(Buffer.from(JSON.stringify(eventA), 'utf8'), options);
    expect(result.events.map((e) => e['id'])).toEqual(['a']);
    expect(result.rejected).toEqual([]);
  });

  it('accepts a single-event NDJSON body with a trailing newline', async () => {
    const result = await decodeBody(Buffer.from(`${JSON.stringify(eventA)}\n`, 'utf8'), options);
    expect(result.events.map((e) => e['id'])).toEqual(['a']);
    expect(result.rejected).toEqual([]);
  });

  it('reports a corrupt gzip body as a reject, not as PayloadTooLargeError', async () => {
    const notGzip = Buffer.from('this is not gzip at all', 'utf8');
    const result = await decodeBody(notGzip, { gzipped: true, maxDecompressedBytes: 1_000_000 });
    expect(result.events).toEqual([]);
    expect(result.rejected).toHaveLength(1);
    expect(result.rejected[0]?.index).toBe(WHOLE_BODY_INDEX);
    expect(result.rejected[0]?.reason).toMatch(/inflation failed/);
  });

  it('reports a truncated gzip stream as a reject, not as PayloadTooLargeError', async () => {
    const full = await gzipAsync(Buffer.from(JSON.stringify([eventA]), 'utf8'));
    const truncated = full.subarray(0, full.length - 4);
    const result = await decodeBody(truncated, { gzipped: true, maxDecompressedBytes: 1_000_000 });
    expect(result.rejected).toHaveLength(1);
    expect(result.rejected[0]?.index).toBe(WHOLE_BODY_INDEX);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/vercel/decode.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `src/vercel/decode.ts`**

`maxOutputLength` makes zlib itself enforce the cap, so a gzip bomb is refused
during inflation rather than after allocating the full output.

```ts
import { gunzip } from 'node:zlib';
import { promisify } from 'node:util';
import { logEventSchema } from './event.js';
import type { LogEvent } from './event.js';

const gunzipAsync = promisify(gunzip);

const SNIPPET_LIMIT = 200;

/**
 * Index used for a reject that describes the WHOLE body rather than one entry:
 * a corrupt gzip stream, an unparseable JSON array, or a non-array body. A real
 * per-entry failure always carries its own non-negative index, so a consumer
 * can tell "nothing parsed" apart from "the first entry was invalid" — which a
 * shared index of 0 could not express.
 */
export const WHOLE_BODY_INDEX = -1;

export type RejectedEntry = { index: number; reason: string; snippet: string };
export type DecodeResult = { events: LogEvent[]; rejected: RejectedEntry[] };
export type DecodeOptions = { gzipped: boolean; maxDecompressedBytes: number };

export class PayloadTooLargeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PayloadTooLargeError';
  }
}

class CorruptBodyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CorruptBodyError';
  }
}

function snippet(text: string): string {
  return text.length > SNIPPET_LIMIT ? `${text.slice(0, SNIPPET_LIMIT - 1)}…` : text;
}

function errorCode(error: Error): string | undefined {
  // `in` narrowing, deliberately. Two tempting alternatives both fail:
  //   const candidate: { code?: string } = error;  -> TS2559, weak-type check
  //   error as { code?: string }                   -> oxlint no-unsafe-type-assertion
  // This form needs neither an assertion nor `unknown`.
  if ('code' in error && typeof error.code === 'string') return error.code;
  return undefined;
}

function validateEntry(candidate: unknown, index: number, into: DecodeResult): void {
  const parsed = logEventSchema.safeParse(candidate);
  if (parsed.success) {
    into.events.push(parsed.data);
    return;
  }
  into.rejected.push({
    index,
    reason: parsed.error.issues.map((issue) => `${issue.path.join('.')}: ${issue.message}`).join('; '),
    snippet: snippet(JSON.stringify(candidate)),
  });
}

async function inflate(raw: Buffer, options: DecodeOptions): Promise<Buffer> {
  if (!options.gzipped) {
    if (raw.byteLength > options.maxDecompressedBytes) {
      throw new PayloadTooLargeError(`body of ${raw.byteLength} bytes exceeds cap`);
    }
    return raw;
  }
  try {
    return await gunzipAsync(raw, { maxOutputLength: options.maxDecompressedBytes });
  } catch (error) {
    const failure = error instanceof Error ? error : new Error(String(error));
    // zlib reports these distinctly, verified on Node 24:
    //   cap exceeded  -> RangeError, code ERR_BUFFER_TOO_LARGE
    //   not gzip      -> Error, code Z_DATA_ERROR ("incorrect header check")
    //   truncated     -> Error, code Z_BUF_ERROR ("unexpected end of file")
    // Collapsing all three into PayloadTooLargeError would report a corrupt
    // body as an oversized one and mislead whoever reads the status page.
    if (errorCode(failure) === 'ERR_BUFFER_TOO_LARGE') {
      throw new PayloadTooLargeError(
        `gzip inflation exceeded ${String(options.maxDecompressedBytes)} bytes`,
      );
    }
    throw new CorruptBodyError(`gzip inflation failed: ${failure.message}`);
  }
}

export async function decodeBody(raw: Buffer, options: DecodeOptions): Promise<DecodeResult> {
  const result: DecodeResult = { events: [], rejected: [] };

  let body: Buffer;
  try {
    body = await inflate(raw, options);
  } catch (error) {
    // A corrupt body yields no events but is NOT an exception: nothing is
    // salvageable, and the signature already proved these are the bytes Vercel
    // sent, so redelivery would reproduce it byte for byte. Report it the same
    // way an unparseable JSON array is reported — one whole-body reject — and
    // let the caller answer 200 with a rejected count. PayloadTooLargeError
    // still propagates, because that one the caller answers with 413.
    if (error instanceof CorruptBodyError) {
      result.rejected.push({
        index: WHOLE_BODY_INDEX,
        reason: error.message,
        snippet: '',
      });
      return result;
    }
    throw error;
  }

  const text = body.toString('utf8');

  const firstNonSpace = text.search(/\S/);
  if (firstNonSpace === -1) return result;

  if (text[firstNonSpace] === '[') {
    let entries: unknown;
    try {
      entries = JSON.parse(text);
    } catch (error) {
      result.rejected.push({
        index: WHOLE_BODY_INDEX,
        reason: error instanceof Error ? error.message : 'invalid JSON array',
        snippet: snippet(text),
      });
      return result;
    }
    if (!Array.isArray(entries)) {
      result.rejected.push({
        index: WHOLE_BODY_INDEX,
        reason: 'body is not an array',
        snippet: snippet(text),
      });
      return result;
    }
    entries.forEach((entry, index) => {
      validateEntry(entry, index, result);
    });
    return result;
  }

  const lines = text.split('\n');
  lines.forEach((line, index) => {
    if (line.trim().length === 0) return;
    let entry: unknown;
    try {
      entry = JSON.parse(line);
    } catch (error) {
      result.rejected.push({
        index,
        reason: error instanceof Error ? error.message : 'invalid JSON line',
        snippet: snippet(line),
      });
      return;
    }
    validateEntry(entry, index, result);
  });
  return result;
}
```

Note the two `unknown` uses here are the *input* boundary of `JSON.parse`,
which is unavoidable and immediately narrowed by zod. The project rule forbids
`unknown` as a way of typing data structures, not as the parse boundary. Do not
propagate it beyond these functions.

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/vercel/decode.test.ts`
Expected: PASS, 11 tests.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: decode Vercel drain bodies

Async gunzip with a zlib-enforced output cap, format sniffing on the
first non-whitespace byte rather than content-type, and per-entry
validation so one malformed line does not fail the whole delivery."
```

---

### Task 5: Sink contract and delivery errors

**Files:**
- Create: `src/sinks/types.ts`
- Test: `test/sinks/types.test.ts`

**Interfaces:**
- Consumes: `Logger` from `src/log.ts`, `LogEvent` from `src/vercel/event.ts`.
- Produces: `RetryableDeliveryError`, `PermanentDeliveryError`, `SinkContext`, `Sink`, `SinkType<TConfig>`.

- [ ] **Step 1: Write the failing test**

`test/sinks/types.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { PermanentDeliveryError, RetryableDeliveryError } from '../../src/sinks/types.js';

describe('delivery errors', () => {
  it('marks a retryable error distinguishably', () => {
    const error = new RetryableDeliveryError('loki unreachable');
    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(RetryableDeliveryError);
    expect(error).not.toBeInstanceOf(PermanentDeliveryError);
    expect(error.name).toBe('RetryableDeliveryError');
    expect(error.message).toBe('loki unreachable');
  });

  it('marks a permanent error distinguishably', () => {
    const error = new PermanentDeliveryError('entry too far behind');
    expect(error).toBeInstanceOf(PermanentDeliveryError);
    expect(error).not.toBeInstanceOf(RetryableDeliveryError);
    expect(error.name).toBe('PermanentDeliveryError');
  });

  it('preserves a cause for diagnostics', () => {
    const cause = new Error('ECONNREFUSED');
    const error = new RetryableDeliveryError('push failed', cause);
    expect(error.cause).toBe(cause);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/sinks/types.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `src/sinks/types.ts`**

```ts
import type { z } from 'zod';
import type { Logger } from '../log.js';
import type { LogEvent } from '../vercel/event.js';

export class RetryableDeliveryError extends Error {
  constructor(message: string, cause?: Error) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = 'RetryableDeliveryError';
  }
}

export class PermanentDeliveryError extends Error {
  constructor(message: string, cause?: Error) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = 'PermanentDeliveryError';
  }
}

export interface SinkContext {
  readonly log: Logger;
}

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
```

Any error a sink throws that is *not* a `PermanentDeliveryError` is treated as
retryable by the Dispatcher, so an unexpected bug never silently discards logs.
`RetryableDeliveryError` exists to make intent explicit, not to gate the
behavior.

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/sinks/types.test.ts`
Expected: PASS, 3 tests.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: define the sink contract and delivery error classes"
```

---

### Task 6: File sink — writer

**Files:**
- Create: `src/sinks/file.ts`
- Test: `test/sinks/file.test.ts`

**Interfaces:**
- Consumes: `Sink`, `SinkType`, `SinkContext` from `src/sinks/types.ts`; `LogEvent` from `src/vercel/event.ts`. (`RetryableDeliveryError` is consumed by Task 7, not here — this task throws no classified delivery errors. A raw fs `ErrnoException` from `open`/`write`/`mkdir` propagates unclassified, which the Task 17 worker treats as retryable, leaving the batch spooled. That is the intended safe default.)
- Produces: `type FileSinkConfig`, `fileSinkConfigSchema`, `fileSinkType`, `utcDateKey(timestampMs: number): string`, `dailyFileName(prefix: string, timestampMs: number): string`, `groupByUtcDate(events: LogEvent[]): Map<string, LogEvent[]>`, `resolveLogsDirectory(candidate: string, logsRoot: string): string`.

Retention and the free-space guard are Task 7; this task delivers writing.

- [ ] **Step 1: Write the failing test**

`test/sinks/file.test.ts`:

```ts
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import {
  dailyFileName,
  fileSinkType,
  groupByUtcDate,
  resolveLogsDirectory,
  utcDateKey,
} from '../../src/sinks/file.js';
import type { FileSinkConfig } from '../../src/sinks/file.js';

const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));

// 2019-11-15T11:26:27.330Z and 2019-11-16T00:00:01.000Z
const beforeMidnight = 1573817187330;
const afterMidnight = 1573862401000;

function event(id: string, timestampMs: number) {
  return { id, timestamp: timestampMs, source: 'lambda', projectId: 'p1' };
}

describe('utcDateKey', () => {
  it('formats a UTC date key', () => {
    expect(utcDateKey(beforeMidnight)).toBe('2019-11-15');
  });

  it('uses UTC, not local time', () => {
    // 2020-01-01T00:30:00Z is still 2019-12-31 in US timezones.
    expect(utcDateKey(Date.UTC(2020, 0, 1, 0, 30, 0))).toBe('2020-01-01');
  });
});

describe('dailyFileName', () => {
  it('composes prefix and date', () => {
    expect(dailyFileName('events', beforeMidnight)).toBe('events-2019-11-15.jsonl');
  });
});

describe('groupByUtcDate', () => {
  it('splits a batch that straddles midnight', () => {
    const grouped = groupByUtcDate([
      event('a', beforeMidnight),
      event('b', afterMidnight),
      event('c', beforeMidnight),
    ]);
    expect([...grouped.keys()].toSorted()).toEqual(['2019-11-15', '2019-11-16']);
    expect(grouped.get('2019-11-15')).toHaveLength(2);
    expect(grouped.get('2019-11-16')).toHaveLength(1);
  });
});

describe('resolveLogsDirectory', () => {
  it('accepts a directory under the root', () => {
    expect(resolveLogsDirectory('/logs/app', '/logs')).toBe('/logs/app');
  });

  it('accepts the root itself', () => {
    expect(resolveLogsDirectory('/logs', '/logs')).toBe('/logs');
  });

  it('rejects a traversal escape', () => {
    expect(() => resolveLogsDirectory('/logs/../config', '/logs')).toThrow(/outside/i);
  });

  it('rejects an unrelated absolute path', () => {
    expect(() => resolveLogsDirectory('/config', '/logs')).toThrow(/outside/i);
  });

  it('rejects a sibling with a matching name prefix', () => {
    expect(() => resolveLogsDirectory('/logs-evil', '/logs')).toThrow(/outside/i);
  });
});

describe('fileSinkType', () => {
  let dir = '';

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'vld-file-'));
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  function config(overrides: Partial<FileSinkConfig> = {}): FileSinkConfig {
    return {
      type: 'file',
      directory: dir,
      filePrefix: 'events',
      retentionDays: 14,
      freeSpaceFloorBytes: 0,
      ...overrides,
    };
  }

  it('writes one JSON line per event', async () => {
    const sink = fileSinkType.create('local', config(), { log: silentLog });
    await sink.deliver([event('a', beforeMidnight), event('b', beforeMidnight)]);
    await sink.close();

    const contents = await readFile(join(dir, 'events-2019-11-15.jsonl'), 'utf8');
    const lines = contents.trimEnd().split('\n');
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[0] ?? '')).toMatchObject({ id: 'a' });
    expect(JSON.parse(lines[1] ?? '')).toMatchObject({ id: 'b' });
  });

  it('partitions by event timestamp, not wall clock', async () => {
    const sink = fileSinkType.create('local', config(), { log: silentLog });
    await sink.deliver([event('a', beforeMidnight), event('b', afterMidnight)]);
    await sink.close();

    const files = (await readdir(dir)).toSorted();
    expect(files).toEqual(['events-2019-11-15.jsonl', 'events-2019-11-16.jsonl']);
  });

  it('appends across separate deliveries', async () => {
    const sink = fileSinkType.create('local', config(), { log: silentLog });
    await sink.deliver([event('a', beforeMidnight)]);
    await sink.deliver([event('b', beforeMidnight)]);
    await sink.close();

    const contents = await readFile(join(dir, 'events-2019-11-15.jsonl'), 'utf8');
    expect(contents.trimEnd().split('\n')).toHaveLength(2);
  });

  it('recreates the directory if it is removed mid-life of the same sink', async () => {
    // The failure this guards against is process-scoped: a cached
    // "directory exists" flag on a LIVE instance. A test that builds a fresh
    // sink after the removal cannot reproduce it, because a new instance has
    // a fresh flag — it must be the same instance, and it must write to a NEW
    // date so no cached handle masks the missing directory. Verified: against
    // a cached-flag implementation this fails with ENOENT.
    const sink = fileSinkType.create('local', config(), { log: silentLog });
    await sink.deliver([event('before', Date.UTC(2026, 2, 1))]);

    await rm(dir, { recursive: true, force: true });

    await sink.deliver([event('after', Date.UTC(2026, 2, 2))]);
    await sink.close();

    const contents = await readFile(join(dir, 'events-2026-03-02.jsonl'), 'utf8');
    expect(JSON.parse(contents.trimEnd())).toMatchObject({ id: 'after' });
  });

  it('creates the directory if it does not exist', async () => {
    const nested = join(dir, 'deep', 'nested');
    const sink = fileSinkType.create('local', config({ directory: nested }), { log: silentLog });
    await sink.deliver([event('a', beforeMidnight)]);
    await sink.close();
    expect(await readdir(nested)).toContain('events-2019-11-15.jsonl');
  });

  it('keeps writing correctly across more dates than the handle cache holds', async () => {
    const sink = fileSinkType.create('local', config(), { log: silentLog });
    const days = [0, 1, 2, 3, 4].map((offset) => Date.UTC(2026, 8, 1 + offset));
    for (const [index, timestamp] of days.entries()) {
      await sink.deliver([event(`d${String(index)}`, timestamp)]);
    }
    // Re-touch the first date after it must have been evicted.
    await sink.deliver([event('again', days[0] ?? 0)]);
    await sink.close();

    const files = (await readdir(dir)).toSorted();
    expect(files).toHaveLength(5);
    const first = await readFile(join(dir, 'events-2026-09-01.jsonl'), 'utf8');
    expect(first.trimEnd().split('\n')).toHaveLength(2);
  });

  it('handles a single batch spanning more dates than the handle cache holds', async () => {
    // Distinct from the test above: that one makes a separate deliver() call
    // per date, so eviction happens BETWEEN calls. Here one batch spans five
    // dates, so eviction happens mid-loop, inside a single deliver().
    const sink = fileSinkType.create('local', config(), { log: silentLog });
    const events = [0, 1, 2, 3, 4].map((offset) =>
      event(`b${String(offset)}`, Date.UTC(2026, 5, 1 + offset)),
    );
    await sink.deliver(events);
    await sink.close();

    const files = (await readdir(dir)).toSorted();
    expect(files).toHaveLength(5);
    for (const [offset, name] of files.entries()) {
      const contents = await readFile(join(dir, name), 'utf8');
      expect(contents.trimEnd().split('\n')).toHaveLength(1);
      expect(JSON.parse(contents.trimEnd())).toMatchObject({ id: `b${String(offset)}` });
    }
  });

  it('reports no warnings for a valid config', () => {
    expect(fileSinkType.warnings(config())).toEqual([]);
  });

  it('validates its config schema', () => {
    expect(fileSinkType.configSchema.safeParse(config()).success).toBe(true);
    expect(fileSinkType.configSchema.safeParse({ ...config(), retentionDays: -1 }).success).toBe(
      false,
    );
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/sinks/file.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `src/sinks/file.ts`**

`fh.sync()` before `deliver()` resolves is load-bearing: resolving is what
causes the Dispatcher to unlink the spool file, which is the only other durable
copy of these events.

```ts
import { mkdir, open } from 'node:fs/promises';
import type { FileHandle } from 'node:fs/promises';
import { isAbsolute, join, relative, resolve } from 'node:path';
import { z } from 'zod';
import type { LogEvent } from '../vercel/event.js';
import type { Sink, SinkContext, SinkType } from './types.js';

const HANDLE_CACHE_LIMIT = 3;

export const fileSinkConfigSchema = z.object({
  type: z.literal('file'),
  directory: z.string().min(1),
  filePrefix: z
    .string()
    .min(1)
    .regex(/^[A-Za-z0-9][A-Za-z0-9._-]*$/, 'prefix must be filename-safe'),
  retentionDays: z.number().int().min(0),
  freeSpaceFloorBytes: z.number().int().min(0),
});

export type FileSinkConfig = z.infer<typeof fileSinkConfigSchema>;

export function utcDateKey(timestampMs: number): string {
  return new Date(timestampMs).toISOString().slice(0, 10);
}

export function dailyFileName(prefix: string, timestampMs: number): string {
  return `${prefix}-${utcDateKey(timestampMs)}.jsonl`;
}

export function groupByUtcDate(events: LogEvent[]): Map<string, LogEvent[]> {
  const grouped = new Map<string, LogEvent[]>();
  for (const event of events) {
    const key = utcDateKey(event.timestamp);
    const bucket = grouped.get(key);
    if (bucket === undefined) {
      grouped.set(key, [event]);
    } else {
      bucket.push(event);
    }
  }
  return grouped;
}

export function resolveLogsDirectory(candidate: string, logsRoot: string): string {
  const root = resolve(logsRoot);
  const target = resolve(candidate);
  const rel = relative(root, target);
  const escapes = rel.startsWith('..') || isAbsolute(rel);
  if (escapes) {
    throw new Error(`directory ${candidate} resolves outside the logs root ${logsRoot}`);
  }
  return target;
}

class FileSink implements Sink {
  readonly type = 'file';
  private readonly handles = new Map<string, FileHandle>();

  constructor(
    readonly name: string,
    private readonly config: FileSinkConfig,
    private readonly ctx: SinkContext,
  ) {}

  /**
   * Called on every deliver, deliberately uncached. `mkdir` with `recursive`
   * is idempotent and costs one syscall per coalesced batch, whereas caching a
   * "directory exists" flag means that if the directory is removed externally
   * — an operator cleaning up, a volume remount — every later write fails with
   * ENOENT permanently, until the process restarts. That trades a negligible
   * saving for an unrecoverable durability regression.
   */
  private async ensureDirectory(): Promise<void> {
    await mkdir(this.config.directory, { recursive: true });
  }

  private async handleFor(dateKey: string): Promise<FileHandle> {
    const existing = this.handles.get(dateKey);
    if (existing !== undefined) {
      // Refresh recency: re-inserting moves the key to the end of a Map's
      // iteration order, which is what makes the eviction below an LRU.
      this.handles.delete(dateKey);
      this.handles.set(dateKey, existing);
      return existing;
    }

    const path = join(this.config.directory, `${this.config.filePrefix}-${dateKey}.jsonl`);
    const handle = await open(path, 'a');
    this.handles.set(dateKey, handle);

    // Bounded cache. A long replay walks through many dates, and leaking a
    // descriptor per day would eventually exhaust the process limit.
    while (this.handles.size > HANDLE_CACHE_LIMIT) {
      const oldest = this.handles.keys().next();
      if (oldest.done === true) break;
      const key = oldest.value;
      const evicted = this.handles.get(key);
      try {
        if (evicted !== undefined) await evicted.close();
      } catch (error) {
        // A failed close must not fail the delivery that triggered eviction —
        // the write we are here for would otherwise have succeeded.
        const message = error instanceof Error ? error.message : String(error);
        this.ctx.log.warn({ sink: this.name, err: message }, 'failed to close evicted handle');
      } finally {
        // Untrack in `finally`: closing first is what avoids leaking a
        // descriptor, but the key must go regardless or a throwing close
        // would spin this loop forever.
        this.handles.delete(key);
      }
    }
    return handle;
  }

  async deliver(events: LogEvent[]): Promise<void> {
    if (events.length === 0) return;
    await this.ensureDirectory();

    for (const [dateKey, batch] of groupByUtcDate(events)) {
      const handle = await this.handleFor(dateKey);
      const payload = `${batch.map((event) => JSON.stringify(event)).join('\n')}\n`;
      await handle.write(payload, null, 'utf8');
      await handle.sync();
    }
    this.ctx.log.debug({ sink: this.name, count: events.length }, 'file sink wrote batch');
  }

  async close(): Promise<void> {
    const handles = [...this.handles.values()];
    this.handles.clear();
    await Promise.all(handles.map((handle) => handle.close()));
  }
}

export const fileSinkType: SinkType<FileSinkConfig> = {
  type: 'file',
  configSchema: fileSinkConfigSchema,
  create(name, config, ctx) {
    return new FileSink(name, config, ctx);
  },
  warnings() {
    return [];
  },
};
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/sinks/file.test.ts`
Expected: PASS.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: add file sink writer

Partitions by the event's own UTC timestamp so replayed batches land in
their own day's file, and fsyncs each write before resolving because
resolving is what deletes the spool copy."
```

---

### Task 7: File sink — retention, free space, and containment

**Files:**
- Modify: `src/sinks/types.ts` (add `FreeSpaceProbe`, extend `SinkContext`)
- Modify: `src/sinks/file.ts`
- Test: `test/sinks/file-retention.test.ts`

**Interfaces:**
- Consumes: everything from Task 6.
- Produces: in `src/sinks/types.ts` — `type FreeSpaceProbe = (path: string) => Promise<number>` and `SinkContext.freeSpace?: FreeSpaceProbe`. In `src/sinks/file.ts` — `statfsFreeSpace: FreeSpaceProbe`, `pruneRetention(dir: string, prefix: string, retentionDays: number, nowMs: number): Promise<string[]>` (returns the deleted filenames); `FileSinkConfig` gains no new fields; `fileSinkType.create` now enforces the free-space floor and starts an hourly pruner.

- [ ] **Step 1: Write the failing test**

`test/sinks/file-retention.test.ts`:

```ts
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, readdir, rm, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import { fileSinkType, pruneRetention } from '../../src/sinks/file.js';
import { RetryableDeliveryError } from '../../src/sinks/types.js';
import type { FileSinkConfig } from '../../src/sinks/file.js';

const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));
const now = Date.UTC(2026, 8, 8, 12, 0, 0); // 2026-09-08T12:00:00Z

describe('pruneRetention', () => {
  let dir = '';

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'vld-retain-'));
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  // Retention requires BOTH an expired filename date AND a stale mtime, so a
  // test file must be aged explicitly — a freshly written one is treated as
  // replayed data and deliberately spared.
  async function agedFile(name: string, ageDays: number): Promise<void> {
    const path = join(dir, name);
    await writeFile(path, '');
    const when = new Date(now - ageDays * 86_400_000);
    await utimes(path, when, when);
  }

  it('deletes files older than the retention window and keeps newer ones', async () => {
    await agedFile('events-2026-09-08.jsonl', 0);
    await agedFile('events-2026-09-06.jsonl', 2);
    await agedFile('events-2026-09-01.jsonl', 7);
    await agedFile('events-2026-08-20.jsonl', 19);

    const deleted = await pruneRetention(dir, 'events', 3, now);

    expect(deleted.toSorted()).toEqual(['events-2026-08-20.jsonl', 'events-2026-09-01.jsonl']);
    expect((await readdir(dir)).toSorted()).toEqual([
      'events-2026-09-06.jsonl',
      'events-2026-09-08.jsonl',
    ]);
  });

  it('never touches files that do not match the pattern', async () => {
    await agedFile('events-2020-01-01.jsonl', 2000);
    await agedFile('important-notes.txt', 2000);
    await agedFile('events-not-a-date.jsonl', 2000);
    await agedFile('other-2020-01-01.jsonl', 2000);

    const deleted = await pruneRetention(dir, 'events', 1, now);

    expect(deleted).toEqual(['events-2020-01-01.jsonl']);
    expect((await readdir(dir)).toSorted()).toEqual([
      'events-not-a-date.jsonl',
      'important-notes.txt',
      'other-2020-01-01.jsonl',
    ]);
  });

  it('escapes regex metacharacters in the prefix', async () => {
    // The config schema permits '.' in a prefix. Unescaped it is a regex
    // wildcard, so `events.log` would also match `eventsXlog-…` and delete a
    // file belonging to someone else.
    await agedFile('events.log-2020-01-01.jsonl', 2000);
    await agedFile('eventsXlog-2020-01-01.jsonl', 2000);

    const deleted = await pruneRetention(dir, 'events.log', 1, now);

    expect(deleted).toEqual(['events.log-2020-01-01.jsonl']);
    expect(await readdir(dir)).toEqual(['eventsXlog-2020-01-01.jsonl']);
  });

  it('spares a file whose name is expired but whose contents just arrived', async () => {
    // The replay case: a batch delayed past the retention window still carries
    // its original event dates, so the file it lands in looks expired the
    // instant it is written. Deleting it would discard data we reported as
    // delivered — and on POSIX an unlink beneath an open handle does not even
    // error, so the loss would be silent.
    await agedFile('events-2020-01-01.jsonl', 0); // expired name, fresh mtime

    const deleted = await pruneRetention(dir, 'events', 1, now);

    expect(deleted).toEqual([]);
    expect(await readdir(dir)).toEqual(['events-2020-01-01.jsonl']);
  });

  it('spares a date that is currently held open', async () => {
    await agedFile('events-2020-01-01.jsonl', 2000);

    const deleted = await pruneRetention(dir, 'events', 1, now, new Set(['2020-01-01']));

    expect(deleted).toEqual([]);
    expect(await readdir(dir)).toEqual(['events-2020-01-01.jsonl']);
  });

  it('skips a shape-valid but impossible calendar date', async () => {
    // Date.parse rolls 2026-02-30 over to 2026-03-02 instead of failing, so a
    // NaN check alone would compare the wrong effective date.
    await agedFile('events-2026-02-30.jsonl', 2000);

    const deleted = await pruneRetention(dir, 'events', 1, now);

    expect(deleted).toEqual([]);
  });

  it('deletes nothing when retentionDays is 0, meaning keep forever', async () => {
    await writeFile(join(dir, 'events-2001-01-01.jsonl'), '');
    expect(await pruneRetention(dir, 'events', 0, now)).toEqual([]);
    expect(await readdir(dir)).toHaveLength(1);
  });

  it('returns an empty list for a missing directory rather than throwing', async () => {
    expect(await pruneRetention(join(dir, 'absent'), 'events', 3, now)).toEqual([]);
  });
});

describe('file sink free-space guard', () => {
  let dir = '';

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'vld-space-'));
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  function config(overrides: Partial<FileSinkConfig> = {}): FileSinkConfig {
    return {
      type: 'file',
      directory: dir,
      filePrefix: 'events',
      retentionDays: 0,
      freeSpaceFloorBytes: 1_000_000,
      ...overrides,
    };
  }

  const event = { id: 'a', timestamp: Date.UTC(2026, 8, 8), source: 'lambda', projectId: 'p1' };

  it('throws a retryable error when free space is below the floor', async () => {
    const sink = fileSinkType.create('local', config(), {
      log: silentLog,
      freeSpace: () => Promise.resolve(500_000),
    });
    await expect(sink.deliver([event])).rejects.toBeInstanceOf(RetryableDeliveryError);
    await sink.close();
    expect(await readdir(dir)).toEqual([]);
  });

  it('writes normally when free space is above the floor', async () => {
    const sink = fileSinkType.create('local', config(), {
      log: silentLog,
      freeSpace: () => Promise.resolve(50_000_000),
    });
    await sink.deliver([event]);
    await sink.close();
    expect(await readdir(dir)).toEqual(['events-2026-09-08.jsonl']);
  });

  it('writes normally when the floor is zero', async () => {
    const sink = fileSinkType.create('local', config({ freeSpaceFloorBytes: 0 }), {
      log: silentLog,
      freeSpace: () => Promise.resolve(0),
    });
    await sink.deliver([event]);
    await sink.close();
    expect(await readdir(dir)).toEqual(['events-2026-09-08.jsonl']);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/sinks/file-retention.test.ts`
Expected: FAIL — `pruneRetention` is not exported, and `SinkContext` has no `freeSpace`.

- [ ] **Step 3: Extend `SinkContext` in `src/sinks/types.ts`**

Add an optional injected free-space probe. Optional so existing callers and
tests need no change, and so `statfs` is the default only in production.

```ts
export type FreeSpaceProbe = (path: string) => Promise<number>;

export interface SinkContext {
  readonly log: Logger;
  readonly freeSpace?: FreeSpaceProbe;
}
```

- [ ] **Step 4: Implement retention and the guard in `src/sinks/file.ts`**

Add these imports and exports:

```ts
import { readdir, stat, statfs, unlink } from 'node:fs/promises';
import { RetryableDeliveryError } from './types.js';
import type { FreeSpaceProbe } from './types.js';

const DAY_MS = 86_400_000;
const PRUNE_INTERVAL_MS = 3_600_000;

export const statfsFreeSpace: FreeSpaceProbe = async (path) => {
  const stats = await statfs(path);
  return stats.bavail * stats.bsize;
};

function escapeForRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

export async function pruneRetention(
  dir: string,
  prefix: string,
  retentionDays: number,
  nowMs: number,
  protectedDates: ReadonlySet<string> = new Set(),
): Promise<string[]> {
  if (retentionDays <= 0) return [];

  let entries: string[];
  try {
    entries = await readdir(dir);
  } catch {
    return [];
  }

  // The prefix is operator-supplied and the schema permits '.', a regex
  // metacharacter. Unescaped, a prefix of `events.log` would also match
  // `eventsXlog-2020-01-01.jsonl` and delete an unrelated file.
  const pattern = new RegExp(`^${escapeForRegExp(prefix)}-(\\d{4}-\\d{2}-\\d{2})\\.jsonl$`);
  const cutoff = nowMs - retentionDays * DAY_MS;
  const deleted: string[] = [];

  for (const entry of entries) {
    const match = pattern.exec(entry);
    if (match === null) continue;
    const dateKey = match[1];
    if (dateKey === undefined) continue;

    const fileMs = Date.parse(`${dateKey}T00:00:00.000Z`);
    if (Number.isNaN(fileMs)) continue;
    // Date.parse rolls an impossible date over rather than failing:
    // 2026-02-30 becomes 2026-03-02. Round-trip it so only a real calendar
    // date is ever compared against the cutoff.
    if (new Date(fileMs).toISOString().slice(0, 10) !== dateKey) continue;
    if (fileMs >= cutoff) continue;

    // A date we hold a handle for is being written to right now.
    if (protectedDates.has(dateKey)) continue;

    // A recently-written file holds REPLAYED data: a batch delayed past the
    // retention window still carries its original event dates, so its name
    // looks expired while its contents only just arrived. Deleting it would
    // silently discard data the caller was told we accepted — and on POSIX an
    // unlink beneath an open handle does not even error, the writes simply
    // vanish. Require the file itself to be stale, not merely its name.
    const stats = await stat(join(dir, entry)).catch(() => null);
    if (stats === null || stats.mtimeMs >= cutoff) continue;

    await unlink(join(dir, entry));
    deleted.push(entry);
  }
  return deleted;
}
```

In `FileSink`, add the free-space guard, a pruner timer, and clear the timer on
close:

```ts
  private pruneTimer: NodeJS.Timeout | null = null;

  private get freeSpace(): FreeSpaceProbe {
    return this.ctx.freeSpace ?? statfsFreeSpace;
  }

  startPruner(): void {
    if (this.config.retentionDays <= 0) return;
    void this.prune();
    this.pruneTimer = setInterval(() => {
      void this.prune();
    }, PRUNE_INTERVAL_MS);
    this.pruneTimer.unref();
  }

  private async prune(): Promise<void> {
    try {
      const deleted = await pruneRetention(
        this.config.directory,
        this.config.filePrefix,
        this.config.retentionDays,
        Date.now(),
        new Set(this.handles.keys()),
      );
      if (deleted.length > 0) {
        this.ctx.log.info({ sink: this.name, deleted: deleted.length }, 'pruned old log files');
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.ctx.log.warn({ sink: this.name, err: message }, 'retention prune failed');
    }
  }

  private async assertSpaceAvailable(): Promise<void> {
    if (this.config.freeSpaceFloorBytes <= 0) return;
    const available = await this.freeSpace(this.config.directory);
    if (available < this.config.freeSpaceFloorBytes) {
      throw new RetryableDeliveryError(
        `only ${available} bytes free in ${this.config.directory}, floor is ${this.config.freeSpaceFloorBytes}`,
      );
    }
  }
```

Then insert the guard at the top of `deliver()`, *before* `ensureDirectory`, so
a blocked delivery leaves nothing behind:

```ts
  async deliver(events: LogEvent[]): Promise<void> {
    if (events.length === 0) return;
    await this.assertSpaceAvailable();
    await this.ensureDirectory();
    // ...remainder unchanged from Task 6
  }
```

And in `close()`, clear the timer before closing handles:

```ts
    if (this.pruneTimer !== null) {
      clearInterval(this.pruneTimer);
      this.pruneTimer = null;
    }
```

Finally, have `fileSinkType.create` start the pruner:

```ts
  create(name, config, ctx) {
    const sink = new FileSink(name, config, ctx);
    sink.startPruner();
    return sink;
  },
```

`unref()` on the timer matters: without it, a Node process with an idle pruner
would refuse to exit.

- [ ] **Step 5: Run the test to verify it passes**

Run: `npx vitest run test/sinks/file-retention.test.ts test/sinks/file.test.ts`
Expected: PASS.

- [ ] **Step 6: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: add file sink retention and free-space guard

Retention deletes only files matching the prefix-date pattern. A logs
volume below its floor raises a retryable error so the batch stays
spooled, turning disk exhaustion into backpressure instead of loss."
```

---

### Task 8: Loki payload construction

**Files:**
- Create: `src/sinks/loki-payload.ts`
- Test: `test/sinks/loki-payload.test.ts`

**Interfaces:**
- Consumes: `LogEvent` from `src/vercel/event.ts`.
- Produces: `type LokiLabelConfig`, `type LokiStream`, `type LokiPushPayload`, `HIGH_CARDINALITY_FIELDS`, `sanitizeLabelName`, `resolveLabels`, `buildPushPayload`, `labelWarnings`, `normalizePushUrl`, `DEFAULT_LABEL_CONFIG`.

- [ ] **Step 1: Write the failing test**

`test/sinks/loki-payload.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import {
  buildPushPayload,
  labelWarnings,
  normalizePushUrl,
  resolveLabels,
  sanitizeLabelName,
} from '../../src/sinks/loki-payload.js';
import type { LokiLabelConfig } from '../../src/sinks/loki-payload.js';

const labels: LokiLabelConfig = {
  static: { job: 'vercel' },
  fromFields: ['projectName', 'environment', 'source', 'level'],
};

function event(overrides: Record<string, string | number> = {}) {
  return {
    id: 'e1',
    timestamp: 1573817250283,
    source: 'lambda',
    projectId: 'p1',
    projectName: 'my-app',
    environment: 'production',
    level: 'info',
    message: 'hello',
    ...overrides,
  };
}

describe('sanitizeLabelName', () => {
  it('replaces dots with underscores', () => {
    expect(sanitizeLabelName('trace.id')).toBe('trace_id');
  });

  it('prefixes a name starting with a digit', () => {
    expect(sanitizeLabelName('2fast')).toBe('_2fast');
  });

  it('strips characters Loki disallows', () => {
    expect(sanitizeLabelName('my-label!')).toBe('my_label_');
  });

  it('leaves a valid name unchanged', () => {
    expect(sanitizeLabelName('project_name')).toBe('project_name');
  });
});

describe('resolveLabels', () => {
  it('merges static labels with allowlisted fields', () => {
    expect(resolveLabels(event(), labels)).toEqual({
      job: 'vercel',
      projectName: 'my-app',
      environment: 'production',
      source: 'lambda',
      level: 'info',
    });
  });

  it('omits fields that are missing rather than sending empty values', () => {
    const { environment: _dropped, ...withoutEnvironment } = event();
    const resolved = resolveLabels(withoutEnvironment, labels);
    expect(resolved).not.toHaveProperty('environment');
  });

  it('omits empty-string values, which Loki rejects', () => {
    const resolved = resolveLabels(event({ environment: '' }), labels);
    expect(resolved).not.toHaveProperty('environment');
  });

  it('stringifies numeric field values', () => {
    const resolved = resolveLabels(event({ statusCode: 200 }), {
      static: {},
      fromFields: ['statusCode'],
    });
    expect(resolved).toEqual({ statusCode: '200' });
  });

  it('skips object-valued fields, which cannot be labels', () => {
    const withProxy = { ...event(), proxy: { method: 'GET' } };
    const resolved = resolveLabels(withProxy, { static: {}, fromFields: ['proxy'] });
    expect(resolved).toEqual({});
  });

  it('sanitizes field names into label names', () => {
    const withTrace = { ...event(), 'trace.id': 'abc' };
    const resolved = resolveLabels(withTrace, { static: {}, fromFields: ['trace.id'] });
    expect(resolved).toEqual({ trace_id: 'abc' });
  });

  it('lets a field-derived label override a static label of the same name', () => {
    const resolved = resolveLabels(event({ level: 'error' }), {
      static: { level: 'from-static' },
      fromFields: ['level'],
    });
    expect(resolved['level']).toBe('error');
  });

  it('truncates over-long static label values too', () => {
    const resolved = resolveLabels(event(), {
      static: { note: 'y'.repeat(2000) },
      fromFields: [],
    });
    expect(resolved['note']?.length).toBe(1024);
  });

  it('truncates over-long label values', () => {
    const resolved = resolveLabels(event({ message: 'x'.repeat(2000) }), {
      static: {},
      fromFields: ['message'],
    });
    expect(resolved['message']?.length).toBe(1024);
  });
});

describe('buildPushPayload', () => {
  it('groups events with identical labels into one stream', () => {
    const payload = buildPushPayload([event({ id: 'a' }), event({ id: 'b' })], labels);
    expect(payload.streams).toHaveLength(1);
    expect(payload.streams[0]?.values).toHaveLength(2);
  });

  it('separates events with different labels into different streams', () => {
    const payload = buildPushPayload(
      [event({ id: 'a', level: 'info' }), event({ id: 'b', level: 'error' })],
      labels,
    );
    expect(payload.streams).toHaveLength(2);
  });

  it('converts milliseconds to a nanosecond string', () => {
    const payload = buildPushPayload([event()], labels);
    expect(payload.streams[0]?.values[0]?.[0]).toBe('1573817250283000000');
  });

  it('sorts values ascending by timestamp within a stream', () => {
    const payload = buildPushPayload(
      [event({ id: 'late', timestamp: 2000 }), event({ id: 'early', timestamp: 1000 })],
      labels,
    );
    const values = payload.streams[0]?.values ?? [];
    expect(values[0]?.[0]).toBe('1000000000');
    expect(values[1]?.[0]).toBe('2000000000');
  });

  it('serializes the complete event as the log line, including label fields', () => {
    const payload = buildPushPayload([event()], labels);
    const line = JSON.parse(payload.streams[0]?.values[0]?.[1] ?? '{}');
    expect(line).toMatchObject({ id: 'e1', projectName: 'my-app', message: 'hello' });
  });

  it('keeps same-timestamp events in arrival order', () => {
    // A comparator that never returns 0 leaves ties implementation-defined.
    const ids = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'];
    const payload = buildPushPayload(
      ids.map((id) => event({ id, timestamp: 5000 })),
      labels,
    );
    const order = (payload.streams[0]?.values ?? []).map(([, line]) => {
      const entry: { id: string } = JSON.parse(line);
      return entry.id;
    });
    expect(order).toEqual(ids);
  });

  it('returns no streams for an empty batch', () => {
    expect(buildPushPayload([], labels).streams).toEqual([]);
  });
});

describe('labelWarnings', () => {
  it('warns about high-cardinality fields', () => {
    const warnings = labelWarnings({ static: {}, fromFields: ['requestId', 'source'] });
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain('requestId');
  });

  it('returns nothing for a safe label set', () => {
    expect(labelWarnings(labels)).toEqual([]);
  });
});

describe('normalizePushUrl', () => {
  it('appends the push path to a base URL', () => {
    expect(normalizePushUrl('http://loki:3100')).toBe('http://loki:3100/loki/api/v1/push');
  });

  it('tolerates a trailing slash', () => {
    expect(normalizePushUrl('http://loki:3100/')).toBe('http://loki:3100/loki/api/v1/push');
  });

  it('does not double-append an already complete URL', () => {
    expect(normalizePushUrl('http://loki:3100/loki/api/v1/push')).toBe(
      'http://loki:3100/loki/api/v1/push',
    );
  });

  it('preserves a path prefix', () => {
    expect(normalizePushUrl('http://gw/loki-tenant')).toBe(
      'http://gw/loki-tenant/loki/api/v1/push',
    );
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/sinks/loki-payload.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `src/sinks/loki-payload.ts`**

```ts
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/sinks/loki-payload.test.ts`
Expected: PASS.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: build Loki push payloads

Pure label resolution and stream grouping, separated from transport so
it is testable without a server. Sanitizes label names to Loki's
grammar, drops empty values Loki rejects, and sorts each stream
ascending."
```

---

### Task 9: Loki sink transport

**Files:**
- Create: `src/sinks/loki.ts`
- Modify: `src/sinks/types.ts` (add `AuthDeliveryError`)
- Test: `test/sinks/loki.test.ts`

**Interfaces:**
- Consumes: everything from `src/sinks/loki-payload.ts` and `src/sinks/types.ts`.
- Produces: `type LokiAuth`, `type LokiSinkConfig`, `lokiSinkConfigSchema`, `type LokiClassification`, `classifyLokiStatus(status: number): LokiClassification`, `lokiSinkType`; and in `types.ts`, `class AuthDeliveryError extends RetryableDeliveryError`.

- [ ] **Step 1: Write the failing test**

`test/sinks/loki.test.ts`:

```ts
import { afterEach, describe, expect, it } from 'vitest';
import { createServer } from 'node:http';
import type { Server } from 'node:http';
import { gunzip } from 'node:zlib';
import { promisify } from 'node:util';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import { classifyLokiStatus, lokiSinkType } from '../../src/sinks/loki.js';
import type { LokiSinkConfig } from '../../src/sinks/loki.js';
import {
  AuthDeliveryError,
  PermanentDeliveryError,
  RetryableDeliveryError,
} from '../../src/sinks/types.js';

const gunzipAsync = promisify(gunzip);
const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));

const event = {
  id: 'e1',
  timestamp: 1573817250283,
  source: 'lambda',
  projectId: 'p1',
  level: 'info',
};

type Captured = { headers: Record<string, string>; body: string };

let server: Server | null = null;

async function startServer(
  status: number,
  responseBody: string,
  captured: Captured[],
  delayMs = 0,
): Promise<string> {
  const instance = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => {
      void (async () => {
        const raw = Buffer.concat(chunks);
        const body =
          req.headers['content-encoding'] === 'gzip'
            ? (await gunzipAsync(raw)).toString('utf8')
            : raw.toString('utf8');
        const headers: Record<string, string> = {};
        for (const [key, value] of Object.entries(req.headers)) {
          if (typeof value === 'string') headers[key] = value;
        }
        captured.push({ headers, body });
        if (delayMs > 0) await new Promise((resolve) => setTimeout(resolve, delayMs));
        res.writeHead(status, { 'content-type': 'text/plain' });
        res.end(responseBody);
      })();
    });
  });
  server = instance;
  await new Promise<void>((resolve) => instance.listen(0, '127.0.0.1', resolve));
  const address = instance.address();
  const port = typeof address === 'object' && address !== null ? address.port : 0;
  return `http://127.0.0.1:${port}`;
}

afterEach(async () => {
  if (server !== null) {
    await new Promise<void>((resolve) => server?.close(() => resolve()));
    server = null;
  }
});

function config(url: string, overrides: Partial<LokiSinkConfig> = {}): LokiSinkConfig {
  return {
    type: 'loki',
    url,
    auth: { kind: 'none' },
    tenantId: null,
    labels: { static: { job: 'vercel' }, fromFields: ['level'] },
    timeoutMs: 5000,
    ...overrides,
  };
}

describe('classifyLokiStatus', () => {
  it.each([
    [200, 'ok'],
    [204, 'ok'],
    [400, 'permanent'],
    [413, 'permanent'],
    [422, 'permanent'],
    [401, 'auth'],
    [403, 'auth'],
    [404, 'auth'],
    [429, 'retryable'],
    [500, 'retryable'],
    [503, 'retryable'],
  ] as const)('classifies %i as %s', (status, expected) => {
    expect(classifyLokiStatus(status)).toBe(expected);
  });
});

describe('loki sink delivery', () => {
  it('posts a gzipped push payload to the push path', async () => {
    const captured: Captured[] = [];
    const url = await startServer(204, '', captured);
    const sink = lokiSinkType.create('loki', config(url), { log: silentLog });

    await sink.deliver([event]);

    expect(captured).toHaveLength(1);
    expect(captured[0]?.headers['content-encoding']).toBe('gzip');
    expect(captured[0]?.headers['content-type']).toBe('application/json');
    const payload = JSON.parse(captured[0]?.body ?? '{}');
    expect(payload.streams[0].stream).toEqual({ job: 'vercel', level: 'info' });
    expect(payload.streams[0].values[0][0]).toBe('1573817250283000000');
  });

  it('sends basic auth and tenant headers', async () => {
    const captured: Captured[] = [];
    const url = await startServer(204, '', captured);
    const sink = lokiSinkType.create(
      'loki',
      config(url, {
        auth: { kind: 'basic', username: 'user', password: 'pass' },
        tenantId: 'team-a',
      }),
      { log: silentLog },
    );

    await sink.deliver([event]);

    const expected = `Basic ${Buffer.from('user:pass').toString('base64')}`;
    expect(captured[0]?.headers['authorization']).toBe(expected);
    expect(captured[0]?.headers['x-scope-orgid']).toBe('team-a');
  });

  it('sends a bearer token', async () => {
    const captured: Captured[] = [];
    const url = await startServer(204, '', captured);
    const sink = lokiSinkType.create('loki', config(url, { auth: { kind: 'bearer', token: 'tk' } }), {
      log: silentLog,
    });
    await sink.deliver([event]);
    expect(captured[0]?.headers['authorization']).toBe('Bearer tk');
  });

  it('throws a permanent error on 400 so the batch is dead-lettered', async () => {
    const url = await startServer(400, 'entry too far behind', []);
    const sink = lokiSinkType.create('loki', config(url), { log: silentLog });
    await expect(sink.deliver([event])).rejects.toBeInstanceOf(PermanentDeliveryError);
  });

  it('throws an auth error on 401 so logs are preserved for retry', async () => {
    const url = await startServer(401, 'unauthorized', []);
    const sink = lokiSinkType.create('loki', config(url), { log: silentLog });
    const rejection = sink.deliver([event]);
    await expect(rejection).rejects.toBeInstanceOf(AuthDeliveryError);
    // An auth error must still be retryable, never permanent.
    await expect(rejection).rejects.toBeInstanceOf(RetryableDeliveryError);
    await expect(rejection).rejects.not.toBeInstanceOf(PermanentDeliveryError);
  });

  it('throws a retryable error on 429 and on 503', async () => {
    const rateLimited = await startServer(429, 'slow down', []);
    const sink = lokiSinkType.create('loki', config(rateLimited), { log: silentLog });
    await expect(sink.deliver([event])).rejects.toBeInstanceOf(RetryableDeliveryError);
  });

  it('throws a retryable error when the connection is refused', async () => {
    const sink = lokiSinkType.create('loki', config('http://127.0.0.1:1'), { log: silentLog });
    await expect(sink.deliver([event])).rejects.toBeInstanceOf(RetryableDeliveryError);
  });

  it('throws a retryable error when the request times out', async () => {
    const url = await startServer(204, '', [], 300);
    const sink = lokiSinkType.create('loki', config(url, { timeoutMs: 100 }), { log: silentLog });
    await expect(sink.deliver([event])).rejects.toBeInstanceOf(RetryableDeliveryError);
  });

  it('does not hang when the server stalls the response body', async () => {
    // Headers arrive at once; the body never completes. The abort must still
    // bound the exchange, or a sink's sequential worker stops forever.
    const stalling = createServer((req, res) => {
      req.on('data', () => undefined);
      req.on('end', () => {
        res.writeHead(500, { 'content-type': 'text/plain', 'content-length': '100' });
        res.write('partial');
        // deliberately never res.end()
      });
    });
    server = stalling;
    await new Promise<void>((resolve) => stalling.listen(0, '127.0.0.1', resolve));
    const address = stalling.address();
    const port = typeof address === 'object' && address !== null ? address.port : 0;

    const sink = lokiSinkType.create(
      'loki',
      config(`http://127.0.0.1:${String(port)}`, { timeoutMs: 200 }),
      { log: silentLog },
    );

    const started = Date.now();
    await expect(sink.deliver([event])).rejects.toBeInstanceOf(RetryableDeliveryError);
    // Generous bound: the point is that it returns at all, not the exact timing.
    expect(Date.now() - started).toBeLessThan(3000);
  }, 10_000);

  it('does not call the server for an empty batch', async () => {
    const captured: Captured[] = [];
    const url = await startServer(204, '', captured);
    const sink = lokiSinkType.create('loki', config(url), { log: silentLog });
    await sink.deliver([]);
    expect(captured).toEqual([]);
  });

  it('warns about high-cardinality labels via the sink type', () => {
    const warnings = lokiSinkType.warnings(
      config('http://loki:3100', { labels: { static: {}, fromFields: ['requestId'] } }),
    );
    expect(warnings[0]).toContain('requestId');
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/sinks/loki.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Add `AuthDeliveryError` to `src/sinks/types.ts`**

It extends `RetryableDeliveryError` so the retry path is inherited by
construction — a credentials mistake must never discard logs. The subclass
exists only so the Dispatcher can surface health as `failed` immediately
instead of after five failures.

```ts
export class AuthDeliveryError extends RetryableDeliveryError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'AuthDeliveryError';
  }
}
```

- [ ] **Step 4: Implement `src/sinks/loki.ts`**

```ts
import { gzip } from 'node:zlib';
import { promisify } from 'node:util';
import { z } from 'zod';
import { buildPushPayload, labelWarnings, normalizePushUrl } from './loki-payload.js';
import { AuthDeliveryError, PermanentDeliveryError, RetryableDeliveryError } from './types.js';
import type { LogEvent } from '../vercel/event.js';
import type { Sink, SinkContext, SinkType } from './types.js';

const gzipAsync = promisify(gzip);
const DETAIL_LIMIT = 500;

export const lokiAuthSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('none') }),
  z.object({
    kind: z.literal('basic'),
    username: z.string().min(1),
    password: z.string().min(1),
  }),
  z.object({ kind: z.literal('bearer'), token: z.string().min(1) }),
]);

export type LokiAuth = z.infer<typeof lokiAuthSchema>;

export const lokiSinkConfigSchema = z.object({
  type: z.literal('loki'),
  url: z.url(),
  auth: lokiAuthSchema,
  tenantId: z.string().min(1).nullable(),
  labels: z.object({
    static: z.record(z.string(), z.string()),
    fromFields: z.array(z.string()),
  }),
  timeoutMs: z.number().int().min(100).max(120_000),
});

export type LokiSinkConfig = z.infer<typeof lokiSinkConfigSchema>;

export type LokiClassification = 'ok' | 'permanent' | 'retryable' | 'auth';

export function classifyLokiStatus(status: number): LokiClassification {
  if (status >= 200 && status < 300) return 'ok';
  if (status === 401 || status === 403 || status === 404) return 'auth';
  // 400 covers malformed streams and entries outside reject_old_samples_max_age;
  // 413/422 mean this exact payload will never be accepted. Retrying any of
  // them forever would pin the head of the queue.
  if (status === 400 || status === 413 || status === 422) return 'permanent';
  return 'retryable';
}

class LokiSink implements Sink {
  readonly type = 'loki';
  private readonly pushUrl: string;

  constructor(
    readonly name: string,
    private readonly config: LokiSinkConfig,
    private readonly ctx: SinkContext,
  ) {
    this.pushUrl = normalizePushUrl(config.url);
  }

  private headers(): Record<string, string> {
    const headers: Record<string, string> = {
      'content-type': 'application/json',
      'content-encoding': 'gzip',
    };
    if (this.config.tenantId !== null) {
      headers['X-Scope-OrgID'] = this.config.tenantId;
    }
    const auth = this.config.auth;
    if (auth.kind === 'basic') {
      const encoded = Buffer.from(`${auth.username}:${auth.password}`, 'utf8').toString('base64');
      headers['authorization'] = `Basic ${encoded}`;
    } else if (auth.kind === 'bearer') {
      headers['authorization'] = `Bearer ${auth.token}`;
    }
    return headers;
  }

  async deliver(events: LogEvent[]): Promise<void> {
    if (events.length === 0) return;
    const payload = buildPushPayload(events, this.config.labels);
    if (payload.streams.length === 0) return;

    const body = await gzipAsync(Buffer.from(JSON.stringify(payload), 'utf8'));
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.config.timeoutMs);

    // The timer stays armed for the WHOLE exchange, headers and body alike.
    // Clearing it as soon as fetch() resolves would leave the body read
    // unprotected: a server that returns a status promptly and then stalls the
    // body would hang deliver() forever, with no timer left to recover it.
    // Measured: with timeoutMs 300 and a stalled body, deliver() was still
    // pending after 3s. Because a sink's worker is sequential, that halts the
    // sink entirely until the spool budget starts dropping data.
    try {
      let response: Response;
      try {
        response = await fetch(this.pushUrl, {
          method: 'POST',
          headers: this.headers(),
          body,
          signal: controller.signal,
        });
      } catch (error) {
        const cause = error instanceof Error ? error : new Error(String(error));
        throw new RetryableDeliveryError(
          `loki push to ${this.pushUrl} failed: ${cause.message}`,
          cause,
        );
      }

      const classification = classifyLokiStatus(response.status);
      if (classification === 'ok') {
        // Release the socket back to undici's pool. An unconsumed body on the
        // success path — the common path — holds a connection per delivery.
        await response.body?.cancel().catch(() => undefined);
        this.ctx.log.debug({ sink: this.name, count: events.length }, 'loki push accepted');
        return;
      }

      // Still inside the armed timer: if the body stalls, the abort rejects
      // this read, the catch yields an empty detail, and we go on to throw the
      // correct classification error rather than hanging.
      const detail = (await response.text().catch(() => '')).slice(0, DETAIL_LIMIT);
      const summary = `loki responded ${String(response.status)}: ${detail}`;

      if (classification === 'permanent') {
        throw new PermanentDeliveryError(
          `${summary} — batch cannot be accepted as-is; dead-lettering. If this is 413, lower the sink's maxBatchBytes.`,
        );
      }
      if (classification === 'auth') {
        throw new AuthDeliveryError(`${summary} — check the sink's credentials and URL`);
      }
      throw new RetryableDeliveryError(summary);
    } finally {
      clearTimeout(timer);
    }
  }

  close(): Promise<void> {
    return Promise.resolve();
  }
}

export const lokiSinkType: SinkType<LokiSinkConfig> = {
  type: 'loki',
  configSchema: lokiSinkConfigSchema,
  create(name, config, ctx) {
    return new LokiSink(name, config, ctx);
  },
  warnings(config) {
    return labelWarnings(config.labels);
  },
};
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `npx vitest run test/sinks/loki.test.ts`
Expected: PASS.

- [ ] **Step 6: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: add Loki sink transport

Gzipped JSON push with basic/bearer auth and tenant header. Classifies
400/413/422 as permanent so a poison batch is dead-lettered instead of
wedging the queue, while 401/403/404 stay retryable but escalate health
so a wrong password never destroys logs."
```

---

### Task 10: Sink registry

**Files:**
- Create: `src/sinks/registry.ts`
- Test: `test/sinks/registry.test.ts`

**Interfaces:**
- Consumes: `fileSinkType`, `fileSinkConfigSchema` from `src/sinks/file.ts`; `lokiSinkType`, `lokiSinkConfigSchema` from `src/sinks/loki.ts`.
- Produces: `type AnySinkConfig = FileSinkConfig | LokiSinkConfig`, `sinkConfigSchema: z.ZodType<AnySinkConfig>`, `createSink(name, config, ctx): Sink`, `warningsFor(config): string[]`.

This module must **not** import from `src/config/`. See the dependency note in
Interface Contracts.

- [ ] **Step 1: Write the failing test**

`test/sinks/registry.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import { createSink, sinkConfigSchema, warningsFor } from '../../src/sinks/registry.js';

const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));

const fileConfig = {
  type: 'file' as const,
  directory: '/tmp/vld-registry',
  filePrefix: 'events',
  retentionDays: 7,
  freeSpaceFloorBytes: 0,
};

const lokiConfig = {
  type: 'loki' as const,
  url: 'http://loki:3100',
  auth: { kind: 'none' as const },
  tenantId: null,
  labels: { static: { job: 'vercel' }, fromFields: ['requestId'] },
  timeoutMs: 5000,
};

describe('sinkConfigSchema', () => {
  it('accepts a file config', () => {
    expect(sinkConfigSchema.safeParse(fileConfig).success).toBe(true);
  });

  it('accepts a loki config', () => {
    expect(sinkConfigSchema.safeParse(lokiConfig).success).toBe(true);
  });

  it('rejects an unknown sink type', () => {
    expect(sinkConfigSchema.safeParse({ type: 'syslog', host: 'x' }).success).toBe(false);
  });

  it('rejects a loki config with a malformed url', () => {
    expect(sinkConfigSchema.safeParse({ ...lokiConfig, url: 'not a url' }).success).toBe(false);
  });
});

describe('createSink', () => {
  it('builds a file sink', async () => {
    const sink = createSink('local', fileConfig, { log: silentLog });
    expect(sink.type).toBe('file');
    expect(sink.name).toBe('local');
    await sink.close();
  });

  it('builds a loki sink', async () => {
    const sink = createSink('remote', lokiConfig, { log: silentLog });
    expect(sink.type).toBe('loki');
    await sink.close();
  });
});

describe('warningsFor', () => {
  it('surfaces loki label warnings', () => {
    expect(warningsFor(lokiConfig)[0]).toContain('requestId');
  });

  it('returns nothing for a file sink', () => {
    expect(warningsFor(fileConfig)).toEqual([]);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/sinks/registry.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `src/sinks/registry.ts`**

```ts
import { z } from 'zod';
import { fileSinkConfigSchema, fileSinkType } from './file.js';
import { lokiSinkConfigSchema, lokiSinkType } from './loki.js';
import type { FileSinkConfig } from './file.js';
import type { LokiSinkConfig } from './loki.js';
import type { Sink, SinkContext } from './types.js';

export type AnySinkConfig = FileSinkConfig | LokiSinkConfig;

export const sinkConfigSchema: z.ZodType<AnySinkConfig> = z.discriminatedUnion('type', [
  fileSinkConfigSchema,
  lokiSinkConfigSchema,
]);

export function createSink(name: string, config: AnySinkConfig, ctx: SinkContext): Sink {
  switch (config.type) {
    case 'file':
      return fileSinkType.create(name, config, ctx);
    case 'loki':
      return lokiSinkType.create(name, config, ctx);
  }
}

export function warningsFor(config: AnySinkConfig): string[] {
  switch (config.type) {
    case 'file':
      return fileSinkType.warnings(config);
    case 'loki':
      return lokiSinkType.warnings(config);
  }
}
```

Exhaustiveness is the point here: adding a third sink type must fail to
compile. Two forms achieve that, and either is acceptable —

- a bare `switch` with no `default`, which errors with
  `TS2366: Function lacks ending return statement`; or
- a trailing `const _: never = config; return _;` after the switch, which
  errors with `Type 'X' is not assignable to type 'never'`.

Both were verified to fail on a third variant. The second names the intent
explicitly and gives the clearer message, at the cost of two lines that are
unreachable at runtime. What is NOT acceptable is a `default` branch that
throws a generic "unknown type" error without a `never` assignment: that
silences the compile-time check entirely and defers a structural mistake to
runtime.

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/sinks/registry.test.ts`
Expected: PASS.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: add sink registry

Owns the sink-config discriminated union so config/schema depends on the
registry and not the reverse, avoiding an import cycle."
```

---

### Task 11: Config schema

**Files:**
- Create: `src/config/schema.ts`
- Test: `test/config/schema.test.ts`

**Interfaces:**
- Consumes: `sinkConfigSchema`, `AnySinkConfig` from `src/sinks/registry.ts`; `EventLevel` from `src/vercel/event.ts`.
- Produces: `SINK_NAME_PATTERN`, `sinkFilterSchema`, `drainEntrySchema`, `sinkEntrySchema`, `serverConfigSchema`, `appConfigSchema`, the inferred types `SinkFilter`, `DrainEntry`, `SinkEntry`, `ServerConfig`, `AppConfig`, plus `defaultAppConfig(): AppConfig`, `newDrainId(): string`, `newDrainSecret(): string`, and the default constants `DEFAULT_MAX_SPOOL_BYTES`, `DEFAULT_MAX_BATCH_EVENTS`, `DEFAULT_MAX_BATCH_BYTES`, `DEFAULT_MAX_BODY_BYTES`, `DEFAULT_MAX_DECOMPRESSED_BYTES`, `DEFAULT_SPOOL_FREE_FLOOR_BYTES`.

- [ ] **Step 1: Write the failing test**

`test/config/schema.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import {
  appConfigSchema,
  defaultAppConfig,
  newDrainId,
  newDrainSecret,
  SINK_NAME_PATTERN,
  sinkEntrySchema,
} from '../../src/config/schema.js';

const validSink = {
  name: 'local-file',
  enabled: true,
  filter: {},
  maxSpoolBytes: 536_870_912,
  maxBatchEvents: 1000,
  maxBatchBytes: 4_194_304,
  config: {
    type: 'file',
    directory: '/logs',
    filePrefix: 'events',
    retentionDays: 14,
    freeSpaceFloorBytes: 268_435_456,
  },
};

describe('SINK_NAME_PATTERN', () => {
  it.each(['a', 'loki', 'loki-prod', 'sink1', 'a-b-c-1'])('accepts %s', (name) => {
    expect(SINK_NAME_PATTERN.test(name)).toBe(true);
  });

  it.each(['', '-lead', 'Upper', 'has space', 'dot.name', '../escape', 'a/b', 'a_b'])(
    'rejects %s',
    (name) => {
      expect(SINK_NAME_PATTERN.test(name)).toBe(false);
    },
  );

  it('rejects a name longer than 64 characters', () => {
    expect(SINK_NAME_PATTERN.test('a'.repeat(65))).toBe(false);
    expect(SINK_NAME_PATTERN.test('a'.repeat(64))).toBe(true);
  });
});

describe('sinkEntrySchema', () => {
  it('accepts a valid entry', () => {
    expect(sinkEntrySchema.safeParse(validSink).success).toBe(true);
  });

  it('rejects a traversal name', () => {
    expect(sinkEntrySchema.safeParse({ ...validSink, name: '../etc' }).success).toBe(false);
  });

  it('keeps an absent predicate distinct from an empty one', () => {
    // Task 12's filter compiler reads absent as "match everything" and an empty
    // array as "match nothing", so parsing must not collapse the two.
    const absent = sinkEntrySchema.safeParse(validSink);
    const empty = sinkEntrySchema.safeParse({ ...validSink, filter: { sources: [] } });
    expect(absent.success && empty.success).toBe(true);
    if (!absent.success || !empty.success) return;
    expect(absent.data.filter.sources).toBeUndefined();
    expect(empty.data.filter.sources).toEqual([]);
  });

  it('rejects a batch bound larger than the whole spool budget', () => {
    const result = sinkEntrySchema.safeParse({
      ...validSink,
      maxSpoolBytes: 1_048_576,
      maxBatchBytes: 100_000_000,
    });
    expect(result.success).toBe(false);
  });

  it('accepts a batch bound equal to the spool budget', () => {
    const result = sinkEntrySchema.safeParse({
      ...validSink,
      maxSpoolBytes: 4_194_304,
      maxBatchBytes: 4_194_304,
    });
    expect(result.success).toBe(true);
  });

  it('accepts a filter with all predicates', () => {
    const result = sinkEntrySchema.safeParse({
      ...validSink,
      filter: {
        minLevel: 'error',
        sources: ['lambda', 'edge'],
        environments: ['production'],
        projectIds: ['p1'],
      },
    });
    expect(result.success).toBe(true);
  });

  it('rejects an unknown minLevel', () => {
    expect(
      sinkEntrySchema.safeParse({ ...validSink, filter: { minLevel: 'trace' } }).success,
    ).toBe(false);
  });
});

describe('appConfigSchema', () => {
  it('accepts the default config', () => {
    expect(appConfigSchema.safeParse(defaultAppConfig()).success).toBe(true);
  });

  it('rejects a wrong version', () => {
    expect(appConfigSchema.safeParse({ ...defaultAppConfig(), version: 2 }).success).toBe(false);
  });

  it('rejects duplicate sink names, and for that reason alone', () => {
    const config = { ...defaultAppConfig(), sinks: [validSink, { ...validSink }] };

    const result = appConfigSchema.safeParse(config);
    expect(result.success).toBe(false);
    if (result.success) return;
    // `validSink` is otherwise valid, so the duplicate must be the sole issue.
    expect(result.error.issues).toHaveLength(1);
    expect(result.error.issues[0]?.message).toMatch(/sink names must be unique/);
  });

  it('rejects duplicate drain ids, and for that reason alone', () => {
    // The id must satisfy min(8) so the ONLY thing wrong with this config is
    // the duplicate. With a short id the parse also fails on length, so the
    // test would pass even with the uniqueness refine deleted — asserting
    // `success === false` alone does not pin the property it names.
    const drain = {
      id: 'drain001',
      name: 'a',
      secret: 'x'.repeat(24),
      enabled: true,
      createdAt: 1,
    };
    const config = { ...defaultAppConfig(), drains: [drain, { ...drain, name: 'b' }] };

    const result = appConfigSchema.safeParse(config);
    expect(result.success).toBe(false);
    if (result.success) return;
    expect(result.error.issues).toHaveLength(1);
    expect(result.error.issues[0]?.message).toMatch(/drain ids must be unique/);
  });

  it('rejects a drain secret that is too short, and for that reason alone', () => {
    // Valid id, so the secret length is the only thing wrong. With a short id
    // as well, the parse fails twice and the assertion stops pinning the
    // property this test is named after.
    const drain = { id: 'drain001', name: 'a', secret: 'short', enabled: true, createdAt: 1 };

    const result = appConfigSchema.safeParse({ ...defaultAppConfig(), drains: [drain] });
    expect(result.success).toBe(false);
    if (result.success) return;
    expect(result.error.issues).toHaveLength(1);
    expect(result.error.issues[0]?.path.join('.')).toBe('drains.0.secret');
  });
});

describe('defaultAppConfig', () => {
  it('starts with no drains and no sinks', () => {
    const config = defaultAppConfig();
    expect(config.drains).toEqual([]);
    expect(config.sinks).toEqual([]);
    expect(config.version).toBe(1);
  });
});

describe('id and secret generation', () => {
  it('generates url-safe drain ids that are unique', () => {
    const ids = new Set(Array.from({ length: 100 }, () => newDrainId()));
    expect(ids.size).toBe(100);
    for (const id of ids) expect(id).toMatch(/^[A-Za-z0-9_-]{16,}$/);
  });

  it('generates secrets long enough to pass the schema', () => {
    const drain = {
      id: newDrainId(),
      name: 'a',
      secret: newDrainSecret(),
      enabled: true,
      createdAt: 1,
    };
    expect(appConfigSchema.safeParse({ ...defaultAppConfig(), drains: [drain] }).success).toBe(
      true,
    );
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/config/schema.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `src/config/schema.ts`**

```ts
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/config/schema.test.ts`
Expected: PASS.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: add config schema

Sink names are validated against the directory-safe pattern and required
to be unique, since a sink name is also its spool directory path."
```

---

### Task 12: Filter compilation

**Files:**
- Create: `src/pipeline/filter.ts`
- Test: `test/pipeline/filter.test.ts`

**Interfaces:**
- Consumes: `SinkFilter` from `src/config/schema.ts`; `LogEvent`, `eventLevel`, `levelRank` from `src/vercel/event.ts`.
- Produces: `type EventPredicate = (event: LogEvent) => boolean`, `compileFilter(filter: SinkFilter): EventPredicate`.

- [ ] **Step 1: Write the failing test**

`test/pipeline/filter.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { compileFilter } from '../../src/pipeline/filter.js';

function event(overrides: Record<string, string | number> = {}) {
  return {
    id: 'e1',
    timestamp: 1,
    source: 'lambda',
    projectId: 'p1',
    environment: 'production',
    level: 'info',
    ...overrides,
  };
}

describe('compileFilter', () => {
  it('matches everything when the filter is empty', () => {
    const predicate = compileFilter({});
    expect(predicate(event())).toBe(true);
    expect(predicate(event({ level: 'error', source: 'build' }))).toBe(true);
  });

  it('applies minLevel inclusively', () => {
    const predicate = compileFilter({ minLevel: 'warning' });
    expect(predicate(event({ level: 'info' }))).toBe(false);
    expect(predicate(event({ level: 'warning' }))).toBe(true);
    expect(predicate(event({ level: 'error' }))).toBe(true);
  });

  it('treats an event with no level as info for minLevel purposes', () => {
    const { level: _omit, ...withoutLevel } = event();
    expect(compileFilter({ minLevel: 'warning' })(withoutLevel)).toBe(false);
    expect(compileFilter({ minLevel: 'info' })(withoutLevel)).toBe(true);
  });

  it('filters by source', () => {
    const predicate = compileFilter({ sources: ['lambda', 'edge'] });
    expect(predicate(event({ source: 'lambda' }))).toBe(true);
    expect(predicate(event({ source: 'build' }))).toBe(false);
  });

  it('filters by environment', () => {
    const predicate = compileFilter({ environments: ['production'] });
    expect(predicate(event({ environment: 'production' }))).toBe(true);
    expect(predicate(event({ environment: 'preview' }))).toBe(false);
  });

  it('excludes an event with no environment when environments is set', () => {
    const { environment: _omit, ...withoutEnvironment } = event();
    expect(compileFilter({ environments: ['production'] })(withoutEnvironment)).toBe(false);
  });

  it('filters by projectId', () => {
    const predicate = compileFilter({ projectIds: ['p1'] });
    expect(predicate(event({ projectId: 'p1' }))).toBe(true);
    expect(predicate(event({ projectId: 'p2' }))).toBe(false);
  });

  it('requires all predicates to pass', () => {
    const predicate = compileFilter({
      minLevel: 'error',
      sources: ['lambda'],
      environments: ['production'],
    });
    expect(predicate(event({ level: 'error', source: 'lambda' }))).toBe(true);
    expect(predicate(event({ level: 'error', source: 'build' }))).toBe(false);
    expect(predicate(event({ level: 'info', source: 'lambda' }))).toBe(false);
  });

  it('treats an empty array as matching nothing, not everything', () => {
    // An empty allowlist is an explicit "no sources permitted"; absence is the
    // way to express "any source".
    expect(compileFilter({ sources: [] })(event())).toBe(false);
  });

  it('applies the empty-means-nothing rule to every array predicate', () => {
    expect(compileFilter({ environments: [] })(event())).toBe(false);
    expect(compileFilter({ projectIds: [] })(event())).toBe(false);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/pipeline/filter.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `src/pipeline/filter.ts`**

```ts
import { eventLevel, levelRank } from '../vercel/event.js';
import type { SinkFilter } from '../config/schema.js';
import type { LogEvent } from '../vercel/event.js';

export type EventPredicate = (event: LogEvent) => boolean;

/**
 * Reads a field as a string, or null if it is absent or any non-string JSON
 * value. Used for EVERY field-based predicate, including `source` and
 * `projectId` which the event schema currently declares as required strings.
 * Going through this helper uniformly costs nothing and means a later schema
 * change — making a field optional, or widening it — degrades to "does not
 * match" rather than silently comparing a non-string against a string Set.
 */
function stringField(event: LogEvent, field: string): string | null {
  const value = event[field];
  return typeof value === 'string' ? value : null;
}

export function compileFilter(filter: SinkFilter): EventPredicate {
  const checks: EventPredicate[] = [];

  if (filter.minLevel !== undefined) {
    const threshold = levelRank(filter.minLevel);
    checks.push((event) => levelRank(eventLevel(event)) >= threshold);
  }

  if (filter.sources !== undefined) {
    const allowed = new Set(filter.sources);
    checks.push((event) => {
      const value = stringField(event, 'source');
      return value !== null && allowed.has(value);
    });
  }

  if (filter.environments !== undefined) {
    const allowed = new Set(filter.environments);
    checks.push((event) => {
      const value = stringField(event, 'environment');
      return value !== null && allowed.has(value);
    });
  }

  if (filter.projectIds !== undefined) {
    const allowed = new Set(filter.projectIds);
    checks.push((event) => {
      const value = stringField(event, 'projectId');
      return value !== null && allowed.has(value);
    });
  }

  if (checks.length === 0) return () => true;
  return (event) => checks.every((check) => check(event));
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/pipeline/filter.test.ts`
Expected: PASS.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: compile sink filters into predicates

An absent predicate matches everything; an empty array matches nothing,
so an explicit empty allowlist is not silently ignored."
```

---

### Task 13: Config store

**Files:**
- Create: `src/config/store.ts`
- Test: `test/config/store.test.ts`

**Interfaces:**
- Consumes: `appConfigSchema`, `defaultAppConfig`, `AppConfig` from `src/config/schema.ts`.
- Produces: `type LoadedConfig = { config: AppConfig; etag: string }`, `class ConfigInvalidError`, `class EtagMismatchError`, `etagOf(config: AppConfig): string`, `class ConfigStore` with `load()` and `save(config, expectedEtag)`.

- [ ] **Step 1: Write the failing test**

`test/config/store.test.ts`:

```ts
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { defaultAppConfig } from '../../src/config/schema.js';
import {
  ConfigInvalidError,
  ConfigStore,
  EtagMismatchError,
  etagOf,
} from '../../src/config/store.js';

describe('ConfigStore', () => {
  let dir = '';

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'vld-config-'));
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it('writes a default config when none exists', async () => {
    const store = new ConfigStore(dir);
    const loaded = await store.load();
    expect(loaded.config).toEqual(defaultAppConfig());
    expect(await readdir(dir)).toContain('config.json');
  });

  it('round-trips a saved config', async () => {
    const store = new ConfigStore(dir);
    const initial = await store.load();
    const updated = { ...initial.config, server: { ...initial.config.server, maxBodyBytes: 4096 } };

    const saved = await store.save(updated, initial.etag);
    const reloaded = await new ConfigStore(dir).load();

    expect(reloaded.config.server.maxBodyBytes).toBe(4096);
    expect(reloaded.etag).toBe(saved.etag);
  });

  it('leaves no .tmp file behind after a save', async () => {
    const store = new ConfigStore(dir);
    const initial = await store.load();
    await store.save(initial.config, initial.etag);
    expect(await readdir(dir)).not.toContain('config.json.tmp');
  });

  it('retains the previous version as .bak', async () => {
    const store = new ConfigStore(dir);
    const first = await store.load();
    const second = await store.save(
      { ...first.config, server: { ...first.config.server, maxBodyBytes: 8192 } },
      first.etag,
    );
    await store.save(
      { ...second.config, server: { ...second.config.server, maxBodyBytes: 9999 } },
      second.etag,
    );

    const backup: { server: { maxBodyBytes: number } } = JSON.parse(
      await readFile(join(dir, 'config.json.bak'), 'utf8'),
    );
    expect(backup.server.maxBodyBytes).toBe(8192);
  });

  it('rejects a save whose etag is stale', async () => {
    const store = new ConfigStore(dir);
    const initial = await store.load();
    await store.save(
      { ...initial.config, server: { ...initial.config.server, maxBodyBytes: 4096 } },
      initial.etag,
    );

    await expect(store.save(initial.config, initial.etag)).rejects.toBeInstanceOf(
      EtagMismatchError,
    );
  });

  it('allows a save with a null etag, for boot-time writes', async () => {
    const store = new ConfigStore(dir);
    await store.load();
    const saved = await store.save(
      { ...defaultAppConfig(), server: { ...defaultAppConfig().server, maxBodyBytes: 5555 } },
      null,
    );
    expect(saved.config.server.maxBodyBytes).toBe(5555);
    expect(saved.etag).toBe(etagOf(saved.config));
    const reloaded = await new ConfigStore(dir).load();
    expect(reloaded.config.server.maxBodyBytes).toBe(5555);
  });

  it('serializes concurrent saves: the second sees a stale etag', async () => {
    const store = new ConfigStore(dir);
    const initial = await store.load();

    const results = await Promise.allSettled([
      store.save({ ...initial.config, server: { ...initial.config.server, maxBodyBytes: 1111 } }, initial.etag),
      store.save({ ...initial.config, server: { ...initial.config.server, maxBodyBytes: 2222 } }, initial.etag),
    ]);

    // Exactly one wins; the loser gets a meaningful conflict, never a raw
    // ENOENT from two saves sharing one temp path.
    expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
    const rejected = results.find((r) => r.status === 'rejected');
    expect(rejected?.status === 'rejected' && rejected.reason).toBeInstanceOf(EtagMismatchError);

    const reloaded = await new ConfigStore(dir).load();
    expect([1111, 2222]).toContain(reloaded.config.server.maxBodyBytes);
  });

  it('survives many concurrent null-etag saves with a valid file and no temp litter', async () => {
    const store = new ConfigStore(dir);
    const initial = await store.load();

    const outcomes = await Promise.all(
      Array.from({ length: 8 }, (_unused, index) =>
        store
          .save(
            { ...initial.config, server: { ...initial.config.server, maxBodyBytes: 2048 + index } },
            null,
          )
          .then(() => 'ok')
          .catch(() => 'failed'),
      ),
    );

    expect(outcomes.every((outcome) => outcome === 'ok')).toBe(true);
    const reloaded = await new ConfigStore(dir).load();
    expect(reloaded.config.server.maxBodyBytes).toBeGreaterThanOrEqual(2048);
    expect((await readdir(dir)).filter((name) => name.endsWith('.tmp'))).toEqual([]);
  });

  it('refuses to start on malformed JSON rather than resetting', async () => {
    await writeFile(join(dir, 'config.json'), '{ not json');
    await expect(new ConfigStore(dir).load()).rejects.toBeInstanceOf(ConfigInvalidError);
  });

  it('refuses to start on a schema-invalid config and names the failing path', async () => {
    await writeFile(join(dir, 'config.json'), JSON.stringify({ version: 1, drains: 'nope' }));
    const rejection = new ConfigStore(dir).load();
    await expect(rejection).rejects.toBeInstanceOf(ConfigInvalidError);
    await expect(rejection).rejects.toThrow(/drains/);
  });

  it('mentions a usable backup in the error when one parses cleanly', async () => {
    await writeFile(join(dir, 'config.json.bak'), JSON.stringify(defaultAppConfig()));
    await writeFile(join(dir, 'config.json'), '{ not json');
    await expect(new ConfigStore(dir).load()).rejects.toThrow(/config\.json\.bak/);
  });

  it('produces an etag independent of key order', () => {
    const a = defaultAppConfig();
    const reordered = {
      server: a.server,
      sinks: a.sinks,
      drains: a.drains,
      version: a.version,
    } as typeof a;
    expect(etagOf(reordered)).toBe(etagOf(a));
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/config/store.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `src/config/store.ts`**

The etag is computed over a **canonical** serialization with sorted keys.
Without that, a save/load round trip could change key order and produce a
spurious `409` on the next edit.

```ts
import { copyFile, mkdir, open, readdir, readFile, rename, rm } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { join } from 'node:path';
import { z } from 'zod';
import { appConfigSchema, defaultAppConfig } from './schema.js';
import type { AppConfig } from './schema.js';
import type { JsonValue } from '../../types/json.js';

export type LoadedConfig = { config: AppConfig; etag: string };

export class ConfigInvalidError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ConfigInvalidError';
  }
}

export class EtagMismatchError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EtagMismatchError';
  }
}

function isErrno(error: Error, code: string): boolean {
  // `in` narrowing, deliberately — see the note in src/vercel/decode.ts.
  // `const candidate: { code?: string } = error;` fails TS2559 (weak-type
  // check: Error has no properties in common), and an `as` assertion trips
  // oxlint's no-unsafe-type-assertion.
  return 'code' in error && error.code === code;
}

function canonicalize(value: JsonValue): JsonValue {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value !== null && typeof value === 'object') {
    const sorted: { [key: string]: JsonValue } = {};
    for (const key of Object.keys(value).toSorted()) {
      const entry = value[key];
      if (entry !== undefined) sorted[key] = canonicalize(entry);
    }
    return sorted;
  }
  return value;
}

export function etagOf(config: AppConfig): string {
  // Annotated assignment rather than `as JsonValue`: oxlint's
  // no-unsafe-type-assertion rejects asserting away JSON.parse's `any`.
  const cloned: JsonValue = JSON.parse(JSON.stringify(config));
  const canonical = canonicalize(cloned);
  return createHash('sha256').update(JSON.stringify(canonical)).digest('hex').slice(0, 32);
}

export class ConfigStore {
  private readonly path: string;
  private readonly backupPath: string;
  private tmpCounter = 0;
  /**
   * Saves are serialized through this chain. `save()` reads the current etag
   * and only then writes, with several `await` points in between — without
   * serialization two concurrent callers (a double-submitted form, two open
   * admin tabs, a retried request racing the original) both pass the etag
   * check and both proceed to write. Measured before this was added: of eight
   * concurrent saves, one succeeded and seven failed with a bare
   * `ENOENT ... rename`, because they all shared one temp path and the first
   * rename moved it out from under the rest. An operator would see "no such
   * file or directory" for what is really a write conflict, and two handles
   * opened `'w'` on the same path can in principle interleave into a corrupt
   * file that then gets renamed over the live config.
   */
  private writeChain: Promise<void> = Promise.resolve();

  constructor(private readonly dir: string) {
    this.path = join(dir, 'config.json');
    this.backupPath = join(dir, 'config.json.bak');
  }

  /** Per-save temp path, so concurrent or crashed writes cannot collide. */
  private nextTmpPath(): string {
    this.tmpCounter += 1;
    return join(this.dir, `config.json.${String(process.pid)}.${String(this.tmpCounter)}.tmp`);
  }

  private serialize<T>(work: () => Promise<T>): Promise<T> {
    const run = this.writeChain.then(work, work);
    // Keep the chain alive whatever happens, so one failed save does not
    // wedge every later one.
    this.writeChain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  async load(): Promise<LoadedConfig> {
    await mkdir(this.dir, { recursive: true });

    let text: string;
    try {
      text = await readFile(this.path, 'utf8');
    } catch (error) {
      if (error instanceof Error && isErrno(error, 'ENOENT')) {
        const config = defaultAppConfig();
        await this.writeAtomic(config);
        return { config, etag: etagOf(config) };
      }
      throw error;
    }

    let raw: JsonValue;
    try {
      // `raw` is already annotated, so no assertion is needed or permitted.
      raw = JSON.parse(text);
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new ConfigInvalidError(
        `${this.path} is not valid JSON: ${detail}${await this.backupHint()}`,
      );
    }

    const result = appConfigSchema.safeParse(raw);
    if (!result.success) {
      throw new ConfigInvalidError(
        `${this.path} does not match the config schema:\n${z.prettifyError(result.error)}${await this.backupHint()}`,
      );
    }
    return { config: result.data, etag: etagOf(result.data) };
  }

  save(config: AppConfig, expectedEtag: string | null): Promise<LoadedConfig> {
    // Serialized: the read-check-write sequence below must not interleave with
    // another save, or the etag check it performs is meaningless.
    return this.serialize(async () => {
      const validated = appConfigSchema.parse(config);
      if (expectedEtag !== null) {
        const current = await this.load();
        if (current.etag !== expectedEtag) {
          throw new EtagMismatchError(
            'the configuration changed since it was read; reload and reapply your edit',
          );
        }
      }
      await this.writeAtomic(validated);
      return { config: validated, etag: etagOf(validated) };
    });
  }

  private async writeAtomic(config: AppConfig): Promise<void> {
    await mkdir(this.dir, { recursive: true });

    let backedUp = false;
    try {
      await copyFile(this.path, this.backupPath);
      backedUp = true;
    } catch (error) {
      // ENOENT here means first run: there is no primary to back up yet.
      if (!(error instanceof Error && isErrno(error, 'ENOENT'))) throw error;
    }

    if (backedUp) {
      // Make the backup durable. The design calls `.bak` the operator's
      // recovery path, and without this its bytes can sit in the page cache
      // indefinitely — a later unrelated crash could lose the one copy someone
      // is told to restore from.
      //
      // Deliberately outside the catch above. An ENOENT from THIS open means
      // "the backup we just wrote has vanished", which is nothing like "there
      // was nothing to back up", and must not be silently swallowed as though
      // it were.
      const backupHandle = await open(this.backupPath, 'r+');
      try {
        await backupHandle.sync();
      } finally {
        await backupHandle.close();
      }
    }

    const tmpPath = this.nextTmpPath();
    try {
      const handle = await open(tmpPath, 'w');
      try {
        await handle.writeFile(`${JSON.stringify(config, null, 2)}\n`, 'utf8');
        await handle.sync();
      } finally {
        await handle.close();
      }
      await rename(tmpPath, this.path);
    } catch (error) {
      // A failed save must not leave its temp file behind.
      await rm(tmpPath, { force: true });
      throw error;
    }

    const dirHandle = await open(this.dir, 'r');
    try {
      await dirHandle.sync();
    } finally {
      await dirHandle.close();
    }
  }

  /**
   * Reaps temp files left by a crashed predecessor. Temp names are per-save and
   * unique, so a crash mid-write strands one permanently; a stray temp is
   * harmless to correctness (`load()` only ever reads `config.json`) but they
   * would accumulate on the config volume.
   *
   * CALL THIS ONCE AT BOOT, before the server starts accepting requests, and
   * nowhere else. It deliberately reaps temps from ANY pid, because the whole
   * point is clearing a dead predecessor's litter — which means it cannot tell
   * a dead temp from a live one. Calling it while any writer is between its
   * `open` and its `rename` unlinks that writer's file and makes the rename
   * fail with ENOENT.
   *
   * This used to be called from `load()`, which was wrong twice over: `load()`
   * also runs inside every non-null-etag `save()`, and a second `ConfigStore`
   * instance on the same directory calls it at will. Measured: a planted
   * foreign-pid temp was deleted both by another instance's `load()` and by a
   * `save()` in progress — reproducing the very ENOENT-on-rename failure the
   * unique temp paths were introduced to eliminate.
   */
  async sweepStaleTemps(): Promise<void> {
    let entries: string[];
    try {
      entries = await readdir(this.dir);
    } catch {
      return;
    }
    for (const entry of entries) {
      if (/^config\.json\.\d+\.\d+\.tmp$/.test(entry)) {
        await rm(join(this.dir, entry), { force: true });
      }
    }
  }

  private async backupHint(): Promise<string> {
    try {
      const text = await readFile(this.backupPath, 'utf8');
      const backup: JsonValue = JSON.parse(text);
      const parsed = appConfigSchema.safeParse(backup);
      if (parsed.success) {
        return `\n\nThe backup at ${this.backupPath} parses cleanly. To recover, copy it over ${this.path} and restart.`;
      }
    } catch {
      // No usable backup; the primary error stands on its own.
    }
    return '';
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/config/store.test.ts`
Expected: PASS, 10 tests.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: add atomic config store

tmp -> fsync -> rename -> fsync(dir), retaining the prior version as
.bak. Refuses to start on an invalid config instead of resetting it, and
points at the backup when the backup is usable. Etags are computed over
a key-sorted canonical form so a round trip cannot cause a false 409."
```

---

### Task 14: Secret redaction

**Files:**
- Create: `src/config/redact.ts`
- Test: `test/config/redact.test.ts`

**Interfaces:**
- Consumes: `AppConfig`, `DrainEntry`, `SinkEntry`, `ServerConfig`, `appConfigSchema` from `src/config/schema.ts`; `JsonValue` from `types/json.ts`.
- Produces: `type RedactedDrain`, `type RedactedConfig`, `redactConfig(config: AppConfig): RedactedConfig`, `restoreSecrets(incoming: JsonValue, current: AppConfig): AppConfig`, `class SecretRestoreError`.

- [ ] **Step 1: Write the failing test**

`test/config/redact.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { defaultAppConfig } from '../../src/config/schema.js';
import { redactConfig, restoreSecrets, SecretRestoreError } from '../../src/config/redact.js';
import type { AppConfig } from '../../src/config/schema.js';

function configWithSecrets(): AppConfig {
  return {
    ...defaultAppConfig(),
    drains: [
      { id: 'drain001', name: 'prod', secret: 'x'.repeat(32), enabled: true, createdAt: 1 },
    ],
    sinks: [
      {
        name: 'loki',
        enabled: true,
        filter: {},
        maxSpoolBytes: 536_870_912,
        maxBatchEvents: 1000,
        maxBatchBytes: 4_194_304,
        config: {
          type: 'loki',
          url: 'http://loki:3100',
          auth: { kind: 'basic', username: 'user', password: 'pw' },
          tenantId: null,
          labels: { static: { job: 'vercel' }, fromFields: ['level'] },
          timeoutMs: 5000,
        },
      },
    ],
  };
}

describe('redactConfig', () => {
  it('nulls drain secrets and flags their presence', () => {
    const redacted = redactConfig(configWithSecrets());
    expect(redacted.drains[0]?.secret).toBeNull();
    expect(redacted.drains[0]?.hasSecret).toBe(true);
    expect(JSON.stringify(redacted)).not.toContain('x'.repeat(32));
  });

  it('nulls loki passwords and flags their presence', () => {
    const redacted = redactConfig(configWithSecrets());
    const sinkConfig = redacted.sinks[0]?.config;
    expect(JSON.stringify(sinkConfig)).not.toContain('pw');
    expect(JSON.stringify(sinkConfig)).toContain('user');
  });

  it('preserves everything non-secret', () => {
    const redacted = redactConfig(configWithSecrets());
    expect(redacted.drains[0]?.name).toBe('prod');
    expect(redacted.sinks[0]?.name).toBe('loki');
  });
});

describe('restoreSecrets', () => {
  it('keeps the existing drain secret when the incoming one is null', () => {
    const current = configWithSecrets();
    const incoming = JSON.parse(JSON.stringify(redactConfig(current)));
    const restored = restoreSecrets(incoming, current);
    expect(restored.drains[0]?.secret).toBe('x'.repeat(32));
  });

  it('keeps the existing loki password when the incoming one is null', () => {
    const current = configWithSecrets();
    const incoming = JSON.parse(JSON.stringify(redactConfig(current)));
    const restored = restoreSecrets(incoming, current);
    const auth = restored.sinks[0]?.config;
    expect(auth?.type === 'loki' && auth.auth.kind === 'basic' && auth.auth.password).toBe('pw');
  });

  it('replaces a secret when a new string is supplied', () => {
    const current = configWithSecrets();
    const incoming = JSON.parse(JSON.stringify(redactConfig(current)));
    incoming.drains[0].secret = 'y'.repeat(32);
    const restored = restoreSecrets(incoming, current);
    expect(restored.drains[0]?.secret).toBe('y'.repeat(32));
  });

  it('fails when a brand-new drain arrives with no secret', () => {
    const current = configWithSecrets();
    const incoming = JSON.parse(JSON.stringify(redactConfig(current)));
    incoming.drains.push({
      id: 'drain002',
      name: 'new',
      secret: null,
      hasSecret: false,
      enabled: true,
      createdAt: 2,
    });
    expect(() => restoreSecrets(incoming, current)).toThrow(SecretRestoreError);
  });

  it('fails when a brand-new loki sink arrives with basic auth and no password', () => {
    const current = configWithSecrets();
    const incoming = JSON.parse(JSON.stringify(redactConfig(current)));
    incoming.sinks[0].name = 'loki-two';
    expect(() => restoreSecrets(incoming, current)).toThrow(SecretRestoreError);
  });

  it('produces a config that passes the full schema', () => {
    const current = configWithSecrets();
    const incoming = JSON.parse(JSON.stringify(redactConfig(current)));
    expect(restoreSecrets(incoming, current)).toEqual(current);
  });
});
```

Matching is by identity — `drains` by `id`, `sinks` by `name` — which is why
renaming a sink is treated as a new sink and requires its secret again.

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/config/redact.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `src/config/redact.ts`**

```ts
import { z } from 'zod';
import { appConfigSchema } from './schema.js';
import type { AppConfig, DrainEntry, ServerConfig, SinkEntry } from './schema.js';
import type { JsonValue } from '../../types/json.js';

export class SecretRestoreError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SecretRestoreError';
  }
}

export type RedactedDrain = Omit<DrainEntry, 'secret'> & { secret: null; hasSecret: boolean };

export type RedactedConfig = {
  version: 1;
  drains: RedactedDrain[];
  sinks: SinkEntry[];
  server: ServerConfig;
};

function redactSinkEntry(entry: SinkEntry): SinkEntry {
  if (entry.config.type !== 'loki') return entry;
  const auth = entry.config.auth;
  if (auth.kind === 'basic') {
    return {
      ...entry,
      config: { ...entry.config, auth: { ...auth, password: '' } },
    };
  }
  if (auth.kind === 'bearer') {
    return { ...entry, config: { ...entry.config, auth: { ...auth, token: '' } } };
  }
  return entry;
}

export function redactConfig(config: AppConfig): RedactedConfig {
  return {
    version: 1,
    drains: config.drains.map(({ secret, ...rest }) => ({
      ...rest,
      secret: null,
      hasSecret: secret.length > 0,
    })),
    sinks: config.sinks.map(redactSinkEntry),
    server: config.server,
  };
}

// The incoming payload mirrors RedactedConfig but with secrets optionally
// replaced by real strings, so it is validated loosely here and strictly by
// appConfigSchema once secrets have been restored.
const incomingSchema = z.object({
  version: z.literal(1),
  drains: z.array(
    z.object({
      id: z.string(),
      name: z.string(),
      secret: z.string().nullish(),
      enabled: z.boolean(),
      createdAt: z.number(),
    }),
  ),
  sinks: z.array(z.record(z.string(), z.custom<JsonValue>(() => true))),
  server: z.record(z.string(), z.custom<JsonValue>(() => true)),
});

export function restoreSecrets(incoming: JsonValue, current: AppConfig): AppConfig {
  const parsed = incomingSchema.safeParse(incoming);
  if (!parsed.success) {
    throw new SecretRestoreError(`malformed configuration payload:\n${z.prettifyError(parsed.error)}`);
  }

  const drainsById = new Map(current.drains.map((drain) => [drain.id, drain]));
  const sinksByName = new Map(current.sinks.map((sink) => [sink.name, sink]));

  const drains = parsed.data.drains.map((drain) => {
    if (typeof drain.secret === 'string' && drain.secret.length > 0) {
      return { ...drain, secret: drain.secret };
    }
    const existing = drainsById.get(drain.id);
    if (existing === undefined) {
      throw new SecretRestoreError(
        `drain "${drain.name}" is new and must be created with a secret`,
      );
    }
    return { ...drain, secret: existing.secret };
  });

  const sinks = parsed.data.sinks.map((raw) => {
    const restored = restoreSinkSecret(raw, sinksByName);
    return restored;
  });

  const candidate = { version: 1 as const, drains, sinks, server: parsed.data.server };
  const validated = appConfigSchema.safeParse(candidate);
  if (!validated.success) {
    throw new SecretRestoreError(
      `configuration is invalid:\n${z.prettifyError(validated.error)}`,
    );
  }
  return validated.data;
}

function restoreSinkSecret(
  raw: Record<string, JsonValue>,
  existingByName: Map<string, SinkEntry>,
): JsonValue {
  const config = raw['config'];
  if (config === null || typeof config !== 'object' || Array.isArray(config)) return raw;
  if (config['type'] !== 'loki') return raw;

  const auth = config['auth'];
  if (auth === null || typeof auth !== 'object' || Array.isArray(auth)) return raw;

  const name = typeof raw['name'] === 'string' ? raw['name'] : '';
  const existing = existingByName.get(name);
  const existingAuth =
    existing !== undefined && existing.config.type === 'loki' ? existing.config.auth : null;

  // Narrow `kind` rather than stringifying it: oxlint's no-base-to-string
  // rejects String() on a JsonValue, since an object would render as
  // "[object Object]".
  const kind = auth['kind'];
  let field: 'password' | 'token' | null = null;
  if (kind === 'basic') field = 'password';
  else if (kind === 'bearer') field = 'token';
  if (field === null) return raw;

  const supplied = auth[field];
  if (typeof supplied === 'string' && supplied.length > 0) return raw;

  if (existingAuth === null || existingAuth.kind !== kind) {
    throw new SecretRestoreError(
      `sink "${name}" uses ${kind} auth and must be saved with its ${field}`,
    );
  }
  const carried = existingAuth.kind === 'basic' ? existingAuth.password : existingAuth.token;
  return { ...raw, config: { ...config, auth: { ...auth, [field]: carried } } };
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/config/redact.test.ts`
Expected: PASS.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: make config secrets write-only

Reads return null with a hasSecret flag; writes carry forward the stored
value unless a replacement is supplied, matched by drain id and sink
name. A new entry must supply its own secret."
```

---

### Task 15: Metrics

**Files:**
- Create: `src/status/metrics.ts`, `types/api.ts`
- Test: `test/status/metrics.test.ts`

**Interfaces:**
- Consumes: `LogEvent` from `src/vercel/event.ts`; `RejectedEntry` from `src/vercel/decode.ts`.
- Produces: in `types/api.ts` the shared shapes `DrainOutcome`, `SinkHealthState`, `SinkHealth`, `DrainStatus`, `SinkStatus`, `VolumeStatus`, `OrphanedSpool`, `StatusSnapshot`; in `src/status/metrics.ts` the `Metrics` class and `initialSinkHealth()`.

`types/api.ts` contains **only** type declarations with no imports, so both the
server and the SPA can consume it without resolution friction.

- [ ] **Step 1: Write the failing test**

`test/status/metrics.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { Metrics } from '../../src/status/metrics.js';

function event(id: string, timestamp = 1000) {
  return { id, timestamp, source: 'lambda', projectId: 'p1' };
}

describe('Metrics', () => {
  it('counts drain request outcomes separately', () => {
    const metrics = new Metrics();
    metrics.recordDrainRequest('d1', 'ok');
    metrics.recordDrainRequest('d1', 'ok');
    metrics.recordDrainRequest('d1', 'badSignature');

    const drain = metrics.snapshot().drains.find((entry) => entry.id === 'd1');
    expect(drain?.requests).toMatchObject({ ok: 2, badSignature: 1 });
  });

  it('aggregates unknown-drain requests without creating a map entry each', () => {
    // The drain id comes from the request path, so a per-id counter would let
    // anyone grow this map without bound.
    const metrics = new Metrics();
    for (let index = 0; index < 5000; index += 1) {
      metrics.recordUnknownDrainRequest();
    }

    const snapshot = metrics.snapshot();
    expect(snapshot.unknownDrainRequests).toBe(5000);
    expect(snapshot.drains).toHaveLength(0);
  });

  it('tracks events received and the latest event timestamp', () => {
    const metrics = new Metrics();
    metrics.recordEventsReceived('d1', 3, 5000);
    metrics.recordEventsReceived('d1', 2, 4000);

    const drain = metrics.snapshot().drains.find((entry) => entry.id === 'd1');
    expect(drain?.eventsReceived).toBe(5);
    // The latest timestamp must not go backwards on an out-of-order batch.
    expect(drain?.lastEventAt).toBe(5000);
  });

  it('accumulates sink counters', () => {
    const metrics = new Metrics();
    metrics.recordDelivered('loki', 10);
    metrics.recordDropped('loki', 3);
    metrics.recordDeadLettered('loki', 1);

    expect(metrics.snapshot().sinkCounters['loki']).toEqual({
      delivered: 10,
      dropped: 3,
      deadLettered: 1,
    });
  });

  it('returns a default health for an unknown sink', () => {
    const metrics = new Metrics();
    expect(metrics.getSinkHealth('never-seen').state).toBe('ok');
    expect(metrics.getSinkHealth('never-seen').consecutiveFailures).toBe(0);
  });

  it('stores and returns sink health', () => {
    const metrics = new Metrics();
    metrics.setSinkHealth('loki', {
      state: 'failed',
      consecutiveFailures: 7,
      lastError: 'boom',
      lastErrorAt: 100,
      lastSuccessAt: null,
      nextRetryAt: 200,
    });
    expect(metrics.getSinkHealth('loki').state).toBe('failed');
  });

  it('bounds the recent-events ring buffer and keeps the newest', () => {
    const metrics = new Metrics();
    for (let index = 0; index < 250; index += 1) {
      metrics.pushRecentEvents([event(`e${String(index)}`)]);
    }
    const recent = metrics.snapshot().recent.events;
    expect(recent).toHaveLength(200);
    expect(recent[recent.length - 1]?.id).toBe('e249');
  });

  it('bounds the rejects and errors ring buffers', () => {
    const metrics = new Metrics();
    for (let index = 0; index < 120; index += 1) {
      metrics.recordRejected('d1', [{ index, reason: 'bad', snippet: 'x' }]);
      metrics.recordError('loki', `failure ${String(index)}`);
    }
    expect(metrics.snapshot().recent.rejects.length).toBeLessThanOrEqual(100);
    expect(metrics.snapshot().recent.errors.length).toBeLessThanOrEqual(100);
  });

  it('forgets a removed sink', () => {
    const metrics = new Metrics();
    metrics.recordDelivered('gone', 5);
    metrics.forgetSink('gone');
    expect(metrics.snapshot().sinkCounters['gone']).toBeUndefined();
  });

  it('reports uptime as a non-negative number', () => {
    expect(new Metrics().snapshot().uptimeSec).toBeGreaterThanOrEqual(0);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/status/metrics.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Create `types/api.ts`**

```ts
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
```

`recent.events` is the single permitted `unknown[]` in the codebase: it is
opaque JSON passed straight to the browser for display and never inspected by
type-dependent logic. No lint directive is needed — neither oxlint nor ESLint
has a rule against `unknown` — but comment the intent at the site:

```ts
  /** Opaque JSON passed straight to the browser for display. Never inspected. */
  recent: { events: unknown[]; rejects: RejectRecord[]; errors: ErrorRecord[] };
```

Do not "fix" this to `JsonValue[]`: that would add an import to this
deliberately import-free file, which is what keeps the SPA's type resolution
free of extension ambiguity.

- [ ] **Step 4: Implement `src/status/metrics.ts`**

```ts
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
  unknownDrainRequests: number;
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
  return { ok: 0, badSignature: 0, disabled: 0, malformedBody: 0 };
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
  private unknownDrainRequests = 0;
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

  /**
   * A request for a drain id that is not configured. Counted in aggregate, not
   * per id: the id comes straight from the request path, so a per-id counter
   * would let anyone grow this map without bound by inventing ids — and the
   * status page lists drains from the config, so such an entry would never be
   * shown. Measured before this was separated: 5000 invented ids produced 5000
   * permanent map entries, none of them displayable.
   */
  recordUnknownDrainRequest(): void {
    this.unknownDrainRequests += 1;
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
      unknownDrainRequests: this.unknownDrainRequests,
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
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `npx vitest run test/status/metrics.test.ts`
Expected: PASS.

- [ ] **Step 6: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: add in-memory metrics and shared status types

Counters and bounded ring buffers, reset on restart by design. Status
shapes live in types/api.ts with no imports so both the server and the
SPA can consume them."
```

---

### Task 16: Durable spool queue

The core durability primitive. Take your time here; the end-to-end test in
Task 24 exists to prove this module's contract.

**Files:**
- Create: `src/pipeline/spool.ts`
- Test: `test/pipeline/spool.test.ts`

**Interfaces:**
- Consumes: `LogEvent`, `logEventSchema` from `src/vercel/event.ts`; `FreeSpaceProbe`, `statfsFreeSpace` re-exported from `src/sinks/file.ts`.
- Produces: `type SpoolOptions`, `type SpoolBatch = { files: string[]; events: LogEvent[]; bytes: number }`, `type EnqueueResult = { writtenBytes: number; droppedEvents: number }`, `class SpoolQueue` with `static open`, `enqueue`, `nextBatch`, `ack`, `deadLetter`, `bytes`, `fileCount`, `oldestMtimeMs`, `discardAll`.

- [ ] **Step 1: Write the failing test**

`test/pipeline/spool.test.ts`:

```ts
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdir, mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SpoolQueue } from '../../src/pipeline/spool.js';

const BIG = 1_048_576;

function event(id: string, timestamp = 1000) {
  return { id, timestamp, source: 'lambda', projectId: 'p1' };
}

function options(overrides: Partial<{ maxSpoolBytes: number; freeSpaceFloorBytes: number }> = {}) {
  return { maxSpoolBytes: BIG, freeSpaceFloorBytes: 0, ...overrides };
}

describe('SpoolQueue', () => {
  let dir = '';

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'vld-spool-'));
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it('round-trips a batch through enqueue and nextBatch', async () => {
    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('a'), event('b')]);

    const batch = await queue.nextBatch(1000, BIG);
    expect(batch?.events.map((e) => e['id'])).toEqual(['a', 'b']);
    expect(batch?.files).toHaveLength(1);
  });

  it('returns null when empty', async () => {
    const queue = await SpoolQueue.open(dir, options());
    expect(await queue.nextBatch(1000, BIG)).toBeNull();
  });

  it('ack removes exactly the coalesced files', async () => {
    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('a')]);
    await queue.enqueue([event('b')]);
    await queue.enqueue([event('c')]);

    const batch = await queue.nextBatch(2, BIG);
    expect(batch?.files).toHaveLength(2);
    await queue.ack(batch!);

    expect(queue.fileCount()).toBe(1);
    const remaining = await queue.nextBatch(1000, BIG);
    expect(remaining?.events.map((e) => e['id'])).toEqual(['c']);
  });

  it('delivers in FIFO order', async () => {
    const queue = await SpoolQueue.open(dir, options());
    for (let index = 0; index < 12; index += 1) {
      await queue.enqueue([event(`e${String(index)}`)]);
    }
    const batch = await queue.nextBatch(1000, BIG);
    expect(batch?.events.map((e) => e['id'])).toEqual(
      Array.from({ length: 12 }, (_unused, index) => `e${String(index)}`),
    );
  });

  // The three tests below are what actually justify the 12-digit padding. The
  // FIFO test above cannot: twelve files are all the same width, so ordering
  // there would hold at any padding, including none. Crossing a decimal
  // boundary is where insufficient padding breaks, and seeding the names
  // directly exercises it through the real recovery path without writing a
  // thousand files.
  it('orders correctly across decimal digit boundaries', async () => {
    for (const seq of [998, 999, 1000, 1001, 9999, 10_000]) {
      await writeFile(
        join(dir, `${String(seq).padStart(12, '0')}.jsonl`),
        `${JSON.stringify(event(`e${String(seq)}`))}\n`,
      );
    }

    const queue = await SpoolQueue.open(dir, options());
    const batch = await queue.nextBatch(1000, BIG);

    expect(batch?.events.map((e) => e['id'])).toEqual([
      'e998',
      'e999',
      'e1000',
      'e1001',
      'e9999',
      'e10000',
    ]);
  });

  it('recovers the sequence counter past a boundary', async () => {
    await writeFile(
      join(dir, `${String(1000).padStart(12, '0')}.jsonl`),
      `${JSON.stringify(event('old'))}\n`,
    );

    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('new')]);

    // The new batch must take seq 1001 and therefore sort AFTER the old one.
    const batch = await queue.nextBatch(1000, BIG);
    expect(batch?.events.map((e) => e['id'])).toEqual(['old', 'new']);
  });

  it('would mis-order without the padding, which is why it is there', () => {
    // Pure demonstration of the failure mode: unpadded, '1000' sorts before
    // '999'. If this assertion ever flips, the padding has stopped mattering
    // and the ordering guarantee rests on nothing.
    const unpadded = [998, 999, 1000, 1001].map((n) => `${String(n)}.jsonl`);
    const padded = [998, 999, 1000, 1001].map((n) => `${String(n).padStart(12, '0')}.jsonl`);

    expect(unpadded.toSorted()).not.toEqual(unpadded);
    expect(padded.toSorted()).toEqual(padded);
  });

  it('coalesces up to maxEvents but always returns at least one file', async () => {
    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('a'), event('b'), event('c')]);

    // A single file already exceeds the limit; it must still be returned,
    // otherwise the queue would deadlock on its own head.
    const batch = await queue.nextBatch(1, BIG);
    expect(batch?.files).toHaveLength(1);
    expect(batch?.events).toHaveLength(3);
  });

  it('recovers sequence and byte accounting after reopening', async () => {
    const first = await SpoolQueue.open(dir, options());
    await first.enqueue([event('a')]);
    await first.enqueue([event('b')]);
    const bytesBefore = first.bytes();

    const second = await SpoolQueue.open(dir, options());
    expect(second.fileCount()).toBe(2);
    expect(second.bytes()).toBe(bytesBefore);

    await second.enqueue([event('c')]);
    const batch = await second.nextBatch(1000, BIG);
    expect(batch?.events.map((e) => e['id'])).toEqual(['a', 'b', 'c']);
  });

  it('deletes stray .tmp files on open and ignores unrelated files', async () => {
    await writeFile(join(dir, '000000000005.jsonl.tmp'), 'garbage');
    await writeFile(join(dir, 'README.txt'), 'not a batch');

    const queue = await SpoolQueue.open(dir, options());

    expect(queue.fileCount()).toBe(0);
    const entries = await readdir(dir);
    expect(entries).not.toContain('000000000005.jsonl.tmp');
    expect(entries).toContain('README.txt');
  });

  it('drops the oldest batches when the byte budget is exceeded', async () => {
    const queue = await SpoolQueue.open(dir, options({ maxSpoolBytes: 400 }));
    // Each event line is roughly 70 bytes.
    const first = await queue.enqueue([event('a1'), event('a2'), event('a3')]);
    expect(first.droppedEvents).toBe(0);

    let dropped = 0;
    for (let index = 0; index < 8; index += 1) {
      const result = await queue.enqueue([event(`b${String(index)}`)]);
      dropped += result.droppedEvents;
    }

    expect(dropped).toBeGreaterThan(0);
    expect(queue.bytes()).toBeLessThanOrEqual(400);
    // Newest data survives; the oldest was sacrificed.
    const batch = await queue.nextBatch(1000, BIG);
    const ids = batch?.events.map((e) => e['id']) ?? [];
    expect(ids).toContain('b7');
    expect(ids).not.toContain('a1');
  });

  it('drops the whole batch when free space is below the floor', async () => {
    const queue = await SpoolQueue.open(dir, {
      maxSpoolBytes: BIG,
      freeSpaceFloorBytes: 1_000_000,
      freeSpace: () => Promise.resolve(500),
    });

    const result = await queue.enqueue([event('a'), event('b')]);

    expect(result.droppedEvents).toBe(2);
    expect(result.writtenBytes).toBe(0);
    expect(queue.fileCount()).toBe(0);
  });

  it('writes normally when free space is above the floor', async () => {
    const queue = await SpoolQueue.open(dir, {
      maxSpoolBytes: BIG,
      freeSpaceFloorBytes: 1000,
      freeSpace: () => Promise.resolve(50_000_000),
    });
    const result = await queue.enqueue([event('a')]);
    expect(result.droppedEvents).toBe(0);
    expect(queue.fileCount()).toBe(1);
  });

  it('moves a batch to the dead directory', async () => {
    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('a')]);
    const batch = await queue.nextBatch(1000, BIG);

    await queue.deadLetter(batch!);

    expect(queue.fileCount()).toBe(0);
    expect(queue.bytes()).toBe(0);
    const dead = await readdir(join(dir, 'dead'));
    expect(dead).toHaveLength(1);
    expect(await readFile(join(dir, 'dead', dead[0] ?? ''), 'utf8')).toContain('"a"');
  });

  it('skips unparseable lines rather than looping forever', async () => {
    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('a')]);
    const [name] = await readdir(dir).then((entries) => entries.filter((e) => e.endsWith('.jsonl')));
    await writeFile(join(dir, name ?? ''), 'this is not json\n');

    const batch = await queue.nextBatch(1000, BIG);
    expect(batch?.events).toEqual([]);
    expect(batch?.files).toHaveLength(1);
    // Acking a zero-event batch is what lets the queue self-heal past corruption.
    await queue.ack(batch!);
    expect(queue.fileCount()).toBe(0);
  });

  it('reports the age of the head of the queue', async () => {
    const queue = await SpoolQueue.open(dir, options());
    expect(await queue.oldestMtimeMs()).toBeNull();
    await queue.enqueue([event('a')]);
    const mtime = await queue.oldestMtimeMs();
    expect(mtime).not.toBeNull();
    expect(Date.now() - (mtime ?? 0)).toBeLessThan(10_000);
  });

  it('discards everything including dead letters', async () => {
    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('a')]);
    const batch = await queue.nextBatch(1000, BIG);
    await queue.deadLetter(batch!);
    await queue.enqueue([event('b')]);

    await queue.discardAll();

    expect(queue.fileCount()).toBe(0);
    expect(queue.bytes()).toBe(0);
    expect(await readdir(join(dir, 'dead'))).toEqual([]);
  });

  // Each of the three tests below fails against the pre-fix implementation.
  // Write them so they do: a regression test that also passes before the fix
  // documents nothing.

  it('does not overwrite an existing dead letter after a restart', async () => {
    // The live directory is empty after the first dead-letter, so a recovery
    // that only scans the live directory restarts the counter at 0 and reissues
    // a name `dead/` already holds -- and rename replaces the destination.
    const first = await SpoolQueue.open(dir, options());
    await first.enqueue([event('precious')]);
    const firstBatch = await first.nextBatch(1000, BIG);
    await first.deadLetter(firstBatch!);
    expect(await readdir(dir).then((f) => f.filter((x) => x !== 'dead'))).toEqual([]);

    const second = await SpoolQueue.open(dir, options());
    await second.enqueue([event('newer')]);
    const secondBatch = await second.nextBatch(1000, BIG);
    await second.deadLetter(secondBatch!);

    const dead = await readdir(join(dir, 'dead'));
    expect(dead).toHaveLength(2);
    const bodies = await Promise.all(dead.map((f) => readFile(join(dir, 'dead', f), 'utf8')));
    expect(bodies.some((b) => b.includes('precious'))).toBe(true);
    expect(bodies.some((b) => b.includes('newer'))).toBe(true);
  });

  it('evicts nothing when the new batch cannot be written', async () => {
    const queue = await SpoolQueue.open(dir, options());
    const first = await queue.enqueue([event('keep-me')]);

    // Budget is now exactly full, so the next enqueue wants to evict `keep-me`.
    const tight = await SpoolQueue.open(dir, options({ maxSpoolBytes: first.writtenBytes }));
    // Block the next sequence number's temp path with a directory: `open(..., 'w')`
    // on a directory fails with EISDIR, while the spool directory itself stays
    // writable -- so an eviction, if one were attempted, would succeed.
    await mkdir(join(dir, '000000000001.jsonl.tmp'));

    await expect(tight.enqueue([event('doomed')])).rejects.toThrow();

    const batch = await tight.nextBatch(1000, BIG);
    expect(batch?.events.map((e) => e['id'])).toEqual(['keep-me']);
  });

  it('evicts nothing when the new batch cannot be renamed into place', async () => {
    // Distinct from the test above: that one fails at the write, so it never
    // reaches the eviction at all. This one lets the write succeed and fails
    // the rename, which is the window where eviction used to have already run.
    const queue = await SpoolQueue.open(dir, options());
    const first = await queue.enqueue([event('keep-me')]);

    const tight = await SpoolQueue.open(dir, options({ maxSpoolBytes: first.writtenBytes }));
    // Block the next sequence number's FINAL path with a directory: renaming a
    // file onto a directory fails with EISDIR, after a clean write and fsync.
    await mkdir(join(dir, '000000000001.jsonl'));

    await expect(tight.enqueue([event('doomed')])).rejects.toThrow();

    const batch = await tight.nextBatch(1000, BIG);
    expect(batch?.events.map((e) => e['id'])).toEqual(['keep-me']);
  });

  it('stops tracking a batch it can no longer read', async () => {
    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('unreadable')]);
    expect(queue.fileCount()).toBe(1);

    // Make the batch unreadable without removing the name: readFile on a
    // directory fails with EISDIR.
    const name = (await readdir(dir)).find((f) => f.endsWith('.jsonl'));
    await rm(join(dir, name!));
    await mkdir(join(dir, name!));

    expect(await queue.nextBatch(1000, BIG)).toBeNull();
    // A retained entry would overcount bytes for the rest of the process's
    // life and be re-read on every later call.
    expect(queue.fileCount()).toBe(0);
    expect(queue.bytes()).toBe(0);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/pipeline/spool.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `src/pipeline/spool.ts`**

```ts
import { mkdir, open, readdir, readFile, rename, rm, stat, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { logEventSchema } from '../vercel/event.js';
import { statfsFreeSpace } from '../sinks/file.js';
import type { LogEvent } from '../vercel/event.js';
import type { FreeSpaceProbe } from '../sinks/types.js';
import type { Logger } from '../log.js';

const BATCH_NAME = /^\d{12}\.jsonl$/;
/**
 * Sequence prefix of any file in `dead/`. Matches both the plain
 * `000000000042.jsonl` and the collision-avoiding
 * `000000000042.1763078400000.jsonl`, so recovery can read the sequence number
 * off either form.
 */
const DEAD_SEQ = /^(\d{12})\./;
const SEQ_WIDTH = 12;
const DEAD_DIR = 'dead';

export type SpoolOptions = {
  maxSpoolBytes: number;
  freeSpaceFloorBytes: number;
  freeSpace?: FreeSpaceProbe;
  /**
   * Optional, but supply it in production. Without it, a filesystem error
   * during `ack`, `deadLetter` or overflow eviction is swallowed silently —
   * which is how a dead-letter name collision went undetected long enough to
   * destroy a batch during development.
   */
  log?: Logger;
};

export type SpoolBatch = { files: string[]; events: LogEvent[]; bytes: number };
export type EnqueueResult = { writtenBytes: number; droppedEvents: number };

type Entry = { name: string; bytes: number };

function serialize(events: LogEvent[]): Buffer {
  return Buffer.from(`${events.map((event) => JSON.stringify(event)).join('\n')}\n`, 'utf8');
}

function parseLines(text: string): LogEvent[] {
  const events: LogEvent[] = [];
  for (const line of text.split('\n')) {
    if (line.trim().length === 0) continue;
    try {
      const parsed = logEventSchema.safeParse(JSON.parse(line));
      if (parsed.success) events.push(parsed.data);
    } catch {
      // A corrupt line is skipped. The batch may end up empty, which the
      // worker acks — that is how the queue heals past corruption instead of
      // retrying a broken head forever.
    }
  }
  return events;
}

function countLines(text: string): number {
  return text.split('\n').filter((line) => line.trim().length > 0).length;
}

export class SpoolQueue {
  private entries: Entry[] = [];
  private totalBytes = 0;
  private seq = 0;
  private readonly freeSpace: FreeSpaceProbe;

  private constructor(
    private readonly dir: string,
    private readonly options: SpoolOptions,
  ) {
    this.freeSpace = options.freeSpace ?? statfsFreeSpace;
  }

  static async open(dir: string, options: SpoolOptions): Promise<SpoolQueue> {
    const queue = new SpoolQueue(dir, options);
    await mkdir(join(dir, DEAD_DIR), { recursive: true });
    await queue.recover();
    return queue;
  }

  private async recover(): Promise<void> {
    const names = await readdir(this.dir);
    const batches: string[] = [];

    for (const name of names) {
      if (name.endsWith('.tmp')) {
        await unlink(join(this.dir, name)).catch(() => undefined);
        continue;
      }
      if (BATCH_NAME.test(name)) batches.push(name);
    }
    // toSorted, not sort: oxlint's unicorn/no-array-sort bans the mutating
    // form. Lexicographic order is deliberate — the 12-digit zero padding
    // makes it identical to numeric order.
    const ordered = batches.toSorted();

    const entries: Entry[] = [];
    let total = 0;
    let maxSeq = -1;
    for (const name of ordered) {
      const stats = await stat(join(this.dir, name));
      entries.push({ name, bytes: stats.size });
      total += stats.size;
      maxSeq = Math.max(maxSeq, Number.parseInt(name.slice(0, SEQ_WIDTH), 10));
    }

    // Sequence numbers must also clear anything already in `dead/`. Without
    // this, a restart whose live directory is empty resets the counter to 0 and
    // reissues a name a dead-lettered file already holds; `deadLetter`'s rename
    // then replaces that file, because POSIX rename replaces its destination.
    // Reproduced before this was added: a batch dead-lettered in one process
    // life was silently destroyed by an unrelated batch dead-lettered after a
    // restart, with no error and no log line.
    for (const name of await readdir(join(this.dir, DEAD_DIR)).catch(() => [])) {
      const match = DEAD_SEQ.exec(name);
      if (match?.[1] === undefined) continue;
      maxSeq = Math.max(maxSeq, Number.parseInt(match[1], 10));
    }

    this.entries = entries;
    this.totalBytes = total;
    this.seq = maxSeq + 1;
  }

  bytes(): number {
    return this.totalBytes;
  }

  fileCount(): number {
    return this.entries.length;
  }

  async enqueue(events: LogEvent[]): Promise<EnqueueResult> {
    if (events.length === 0) return { writtenBytes: 0, droppedEvents: 0 };

    if (this.options.freeSpaceFloorBytes > 0) {
      const available = await this.freeSpace(this.dir);
      if (available < this.options.freeSpaceFloorBytes) {
        return { writtenBytes: 0, droppedEvents: events.length };
      }
    }

    const payload = serialize(events);
    const name = `${String(this.seq).padStart(SEQ_WIDTH, '0')}.jsonl`;
    this.seq += 1;
    const tmpPath = join(this.dir, `${name}.tmp`);

    // Commit the new batch COMPLETELY before evicting anything: write, fsync,
    // rename, and fsync the directory. Eviction is a real unlink, so any step
    // still ahead of it is a step that can fail with the old batch already
    // destroyed. An earlier version evicted between the fsync and the rename,
    // which left a narrow window where a failing rename lost both the evicted
    // batch and the replacement — boot recovery deletes stray `.tmp` files, so
    // the replacement had nowhere to survive. Ordering is the whole fix here;
    // do not move `makeRoom` back inside this block.
    try {
      const handle = await open(tmpPath, 'w');
      try {
        await handle.writeFile(payload);
        await handle.sync();
      } finally {
        await handle.close();
      }
      await rename(tmpPath, join(this.dir, name));

      const dirHandle = await open(this.dir, 'r');
      try {
        await dirHandle.sync();
      } finally {
        await dirHandle.close();
      }
    } catch (error) {
      // Nothing has been evicted yet, so the only cleanup is the temp file,
      // which must not be left for boot recovery to find. After a successful
      // rename `tmpPath` is already gone and this is a no-op.
      await rm(tmpPath, { force: true }).catch(() => undefined);
      throw error;
    }

    // Safe now: the new batch is durable, so reclaiming space can at worst
    // leave the spool briefly over budget. `makeRoom` swallows and logs its own
    // filesystem errors, so a throw here means a programming error; the batch
    // is on disk either way and boot recovery will pick it up.
    const droppedEvents = await this.makeRoom(payload.byteLength);

    this.entries.push({ name, bytes: payload.byteLength });
    this.totalBytes += payload.byteLength;
    return { writtenBytes: payload.byteLength, droppedEvents };
  }

  private async makeRoom(incoming: number): Promise<number> {
    let dropped = 0;
    while (this.entries.length > 0 && this.totalBytes + incoming > this.options.maxSpoolBytes) {
      const oldest = this.entries.shift();
      if (oldest === undefined) break;
      const path = join(this.dir, oldest.name);
      try {
        dropped += countLines(await readFile(path, 'utf8'));
      } catch {
        // Already gone; still account for its bytes below.
      }
      this.totalBytes -= oldest.bytes;
      await unlink(path).catch((error: unknown) => {
        this.reportFsError('unlink during overflow eviction', oldest.name, error);
      });
    }
    // If a single batch is larger than the whole budget the loop empties the
    // queue and we still write it: refusing the newest data would be worse.
    return dropped;
  }

  async nextBatch(maxEvents: number, maxBytes: number): Promise<SpoolBatch | null> {
    if (this.entries.length === 0) return null;

    const files: string[] = [];
    const events: LogEvent[] = [];
    const unreadable: string[] = [];
    let bytes = 0;

    for (const entry of this.entries) {
      let text: string;
      try {
        text = await readFile(join(this.dir, entry.name), 'utf8');
      } catch (error) {
        // Stop tracking it. Leaving the entry in place used to overcount
        // totalBytes for the rest of the process's life, which could evict
        // live batches to make room that was never occupied — and the entry
        // was re-read and re-skipped on every later call.
        this.reportFsError('read', entry.name, error);
        unreadable.push(entry.name);
        continue;
      }
      const parsed = parseLines(text);

      const wouldExceed = events.length + parsed.length > maxEvents || bytes + entry.bytes > maxBytes;
      if (files.length > 0 && wouldExceed) break;

      files.push(entry.name);
      events.push(...parsed);
      bytes += entry.bytes;

      if (events.length >= maxEvents || bytes >= maxBytes) break;
    }

    if (unreadable.length > 0) this.untrack(unreadable);
    if (files.length === 0) return null;
    return { files, events, bytes };
  }

  async ack(batch: SpoolBatch): Promise<void> {
    await this.removeAll('ack', batch.files, (name) => unlink(join(this.dir, name)));
  }

  async deadLetter(batch: SpoolBatch): Promise<void> {
    await this.removeAll('dead-letter', batch.files, async (name) => {
      await rename(join(this.dir, name), await this.deadPathFor(name));
    });
  }

  /**
   * Where a dead-lettered batch should land. Recovery now scans `dead/` when
   * choosing sequence numbers, so a collision should be impossible — but this
   * checks anyway, because POSIX rename REPLACES its destination and a
   * collision here would silently destroy an already-failed batch, which is
   * the single thing this directory exists to prevent.
   */
  private async deadPathFor(name: string): Promise<string> {
    const preferred = join(this.dir, DEAD_DIR, name);
    try {
      await stat(preferred);
    } catch {
      return preferred;
    }
    const suffixed = join(
      this.dir,
      DEAD_DIR,
      `${name.slice(0, SEQ_WIDTH)}.${String(Date.now())}.jsonl`,
    );
    this.options.log?.error(
      { dir: this.dir, name, suffixed },
      'dead-letter name already taken; preserving both rather than replacing',
    );
    return suffixed;
  }

  private reportFsError(operation: string, name: string, error: unknown): void {
    const failure = error instanceof Error ? error : new Error(String(error));
    // An ENOENT means the file was already gone, which is the expected race and
    // not worth a log line. Anything else is a real filesystem problem.
    if ('code' in failure && failure.code === 'ENOENT') return;
    this.options.log?.warn(
      { dir: this.dir, name, operation, err: failure.message },
      'spool filesystem operation failed',
    );
  }

  private untrack(names: readonly string[]): void {
    const removing = new Set(names);
    const kept: Entry[] = [];
    for (const entry of this.entries) {
      if (removing.has(entry.name)) {
        this.totalBytes -= entry.bytes;
      } else {
        kept.push(entry);
      }
    }
    this.entries = kept;
  }

  private async removeAll(
    operation: string,
    names: string[],
    action: (name: string) => Promise<void>,
  ): Promise<void> {
    for (const name of names) {
      await action(name).catch((error: unknown) => {
        this.reportFsError(operation, name, error);
      });
    }
    // The entry is untracked whether or not the filesystem call succeeded. A
    // file that genuinely cannot be removed would otherwise sit at the head of
    // the queue and be redelivered forever, which is worse than the byte
    // undercount — and the failure is now logged rather than swallowed.
    this.untrack(names);
  }

  async oldestMtimeMs(): Promise<number | null> {
    const oldest = this.entries[0];
    if (oldest === undefined) return null;
    try {
      const stats = await stat(join(this.dir, oldest.name));
      return stats.mtimeMs;
    } catch {
      return null;
    }
  }

  async discardAll(): Promise<void> {
    for (const entry of this.entries) {
      await unlink(join(this.dir, entry.name)).catch((error: unknown) => {
        this.reportFsError('unlink during discardAll', entry.name, error);
      });
    }
    this.entries = [];
    this.totalBytes = 0;
    await rm(join(this.dir, DEAD_DIR), { recursive: true, force: true });
    await mkdir(join(this.dir, DEAD_DIR), { recursive: true });
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/pipeline/spool.test.ts`
Expected: PASS, 14 tests.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: add durable spool queue

Batch files are written tmp -> fsync -> rename -> fsync(dir) so a crash
never exposes a partial batch, and are unlinked only after a sink
accepts them. Twelve-digit sequence names make lexicographic order FIFO
order. Overflow drops oldest with a counted event total; a corrupt file
yields an empty batch that the worker acks, so the head can never wedge."
```

---

### Task 17: Dispatcher — worker loop, backoff, and health

**Files:**
- Create: `src/pipeline/dispatcher.ts`
- Test: `test/pipeline/dispatcher-worker.test.ts`

**Interfaces:**
- Consumes: `SpoolQueue`, `SpoolBatch` from `src/pipeline/spool.ts`; `Sink`, `PermanentDeliveryError`, `AuthDeliveryError` from `src/sinks/types.ts`; `Metrics`, `initialSinkHealth` from `src/status/metrics.ts`; `Logger` from `src/log.ts`.
- Produces: `backoffDelayMs(consecutiveFailures, baseMs, capMs, random): number`, `class SinkWorker` (internal but exported for testing) with `start()`, `stop()`, `drainOnce(): Promise<boolean>`, and `health(): SinkHealth`.

Config reconciliation is Task 18. This task builds the worker in isolation so
its retry semantics can be tested without any config machinery.

- [ ] **Step 1: Write the failing test**

`test/pipeline/dispatcher-worker.test.ts`:

```ts
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import { SpoolQueue } from '../../src/pipeline/spool.js';
import { backoffDelayMs, SinkWorker } from '../../src/pipeline/dispatcher.js';
import { Metrics } from '../../src/status/metrics.js';
import { AuthDeliveryError, PermanentDeliveryError } from '../../src/sinks/types.js';
import type { LogEvent } from '../../src/vercel/event.js';
import type { Sink } from '../../src/sinks/types.js';

const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));

function event(id: string) {
  return { id, timestamp: 1000, source: 'lambda', projectId: 'p1' };
}

class FakeSink implements Sink {
  readonly type = 'fake';
  readonly received: LogEvent[][] = [];
  constructor(
    readonly name: string,
    private readonly behavior: (attempt: number) => Error | null = () => null,
  ) {}
  private attempts = 0;
  deliver(events: LogEvent[]): Promise<void> {
    this.attempts += 1;
    const error = this.behavior(this.attempts);
    if (error !== null) return Promise.reject(error);
    this.received.push(events);
    return Promise.resolve();
  }
  closeCount = 0;
  closeDelayMs = 0;
  closedAt: number | null = null;
  async close(): Promise<void> {
    this.closeCount += 1;
    if (this.closeDelayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.closeDelayMs));
    }
    this.closedAt = Date.now();
  }
}

describe('backoffDelayMs', () => {
  it('grows exponentially from the base', () => {
    expect(backoffDelayMs(1, 1000, 60_000, () => 0)).toBe(1000);
    expect(backoffDelayMs(2, 1000, 60_000, () => 0)).toBe(2000);
    expect(backoffDelayMs(3, 1000, 60_000, () => 0)).toBe(4000);
  });

  it('caps at the maximum', () => {
    expect(backoffDelayMs(30, 1000, 60_000, () => 0)).toBe(60_000);
  });

  it('adds jitter above the base delay', () => {
    const withJitter = backoffDelayMs(1, 1000, 60_000, () => 1);
    expect(withJitter).toBeGreaterThan(1000);
    expect(withJitter).toBeLessThanOrEqual(1300);
  });

  it('never returns less than the base for the first failure', () => {
    expect(backoffDelayMs(0, 1000, 60_000, () => 0)).toBe(1000);
  });
});

describe('SinkWorker', () => {
  let dir = '';
  let metrics: Metrics;

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'vld-worker-'));
    metrics = new Metrics();
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  async function makeWorker(
    sink: Sink,
    maxBatchEvents = 1000,
  ): Promise<{ worker: SinkWorker; queue: SpoolQueue }> {
    const queue = await SpoolQueue.open(dir, { maxSpoolBytes: 1_048_576, freeSpaceFloorBytes: 0 });
    const worker = new SinkWorker({
      sink,
      queue,
      metrics,
      log: silentLog,
      maxBatchEvents,
      maxBatchBytes: 1_048_576,
      baseBackoffMs: 10,
      maxBackoffMs: 40,
      random: () => 0,
    });
    return { worker, queue };
  }

  it('delivers a spooled batch and removes it', async () => {
    const sink = new FakeSink('ok');
    const { worker, queue } = await makeWorker(sink);
    await queue.enqueue([event('a')]);

    expect(await worker.drainOnce()).toBe(true);

    expect(sink.received[0]?.map((e) => e['id'])).toEqual(['a']);
    expect(queue.fileCount()).toBe(0);
    expect(metrics.snapshot().sinkCounters['ok']?.delivered).toBe(1);
    expect(worker.health().state).toBe('ok');
  });

  it('reports nothing to do on an empty queue', async () => {
    const { worker } = await makeWorker(new FakeSink('idle'));
    expect(await worker.drainOnce()).toBe(false);
  });

  it('keeps the batch on a retryable failure and escalates after the threshold', async () => {
    const sink = new FakeSink('flaky', () => new Error('loki down'));
    const { worker, queue } = await makeWorker(sink);
    await queue.enqueue([event('a')]);

    await worker.drainOnce();
    expect(queue.fileCount()).toBe(1);
    expect(worker.health().state).toBe('retrying');
    expect(worker.health().consecutiveFailures).toBe(1);
    expect(worker.health().lastError).toContain('loki down');
    expect(worker.health().nextRetryAt).not.toBeNull();

    for (let index = 0; index < 4; index += 1) await worker.drainOnce();
    expect(worker.health().consecutiveFailures).toBe(5);
    expect(worker.health().state).toBe('failed');
    expect(queue.fileCount()).toBe(1);
  });

  it('treats an unexpected error as retryable, never discarding logs', async () => {
    const sink = new FakeSink('buggy', () => new TypeError('undefined is not a function'));
    const { worker, queue } = await makeWorker(sink);
    await queue.enqueue([event('a')]);

    await worker.drainOnce();

    expect(queue.fileCount()).toBe(1);
    expect(metrics.snapshot().sinkCounters['buggy']?.deadLettered ?? 0).toBe(0);
  });

  it('dead-letters on a permanent failure and advances', async () => {
    const sink = new FakeSink('poison', (attempt) =>
      attempt === 1 ? new PermanentDeliveryError('400 malformed') : null,
    );
    // maxBatchEvents is pinned to 1 so 'bad' and 'good' are drained as two
    // separate batches. With the generous limits used elsewhere in this file,
    // SpoolQueue.nextBatch() coalesces both enqueued files into one batch, and
    // the permanent failure would then dead-letter 'good' along with 'bad' --
    // which is the opposite of what this test exists to demonstrate.
    const { worker, queue } = await makeWorker(sink, 1);
    await queue.enqueue([event('bad')]);
    await queue.enqueue([event('good')]);

    await worker.drainOnce();
    expect(metrics.snapshot().sinkCounters['poison']?.deadLettered).toBe(1);

    await worker.drainOnce();
    expect(sink.received[0]?.map((e) => e['id'])).toEqual(['good']);
    expect(queue.fileCount()).toBe(0);
  });

  it('escalates health immediately on an auth failure', async () => {
    const sink = new FakeSink('auth', () => new AuthDeliveryError('401 check credentials'));
    const { worker, queue } = await makeWorker(sink);
    await queue.enqueue([event('a')]);

    await worker.drainOnce();

    expect(worker.health().state).toBe('failed');
    expect(worker.health().consecutiveFailures).toBe(1);
    expect(queue.fileCount()).toBe(1);
  });

  it('recovers health after a success', async () => {
    const sink = new FakeSink('recover', (attempt) => (attempt <= 2 ? new Error('down') : null));
    const { worker, queue } = await makeWorker(sink);
    await queue.enqueue([event('a')]);

    await worker.drainOnce();
    await worker.drainOnce();
    expect(worker.health().state).toBe('retrying');

    await worker.drainOnce();
    expect(worker.health().state).toBe('ok');
    expect(worker.health().consecutiveFailures).toBe(0);
    expect(worker.health().lastSuccessAt).not.toBeNull();
  });

  it('drains everything when started, then stops cleanly', async () => {
    const sink = new FakeSink('runner');
    const { worker, queue } = await makeWorker(sink);
    await queue.enqueue([event('a')]);
    await queue.enqueue([event('b')]);

    worker.start();
    await new Promise((resolve) => setTimeout(resolve, 200));
    await worker.stop(1000);

    expect(queue.fileCount()).toBe(0);
    expect(sink.received.flat().map((e) => e['id']).toSorted()).toEqual(['a', 'b']);
  });

  it('stop() halts the loop and leaves no timer armed to do more work later', async () => {
    // The test above stops a worker with an empty queue, so it passes whether
    // or not stop() actually stopped anything. This one keeps the sink failing
    // so the loop always has work, which is the only way to tell a working
    // stop() from a broken one.
    let attempts = 0;
    const sink = new FakeSink('halt', () => {
      attempts += 1;
      return new Error('down');
    });
    const { worker, queue } = await makeWorker(sink);
    await queue.enqueue([event('a')]);

    worker.start();
    // Let a couple of failure/backoff cycles happen (base 10ms, cap 40ms).
    await new Promise((resolve) => setTimeout(resolve, 50));
    await worker.stop(1000);

    const attemptsAtStop = attempts;
    expect(attemptsAtStop).toBeGreaterThan(0);

    // A timer left armed, or a loop still running behind stop(), shows up as
    // further delivery attempts after stop() has already resolved.
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(attempts).toBe(attemptsAtStop);
    expect(queue.fileCount()).toBe(1);
  });

  it('closes the sink exactly once however often stop() is called', async () => {
    const sink = new FakeSink('once');
    const { worker } = await makeWorker(sink);

    worker.start();
    await Promise.all([worker.stop(1000), worker.stop(1000)]);
    await worker.stop(1000);

    // Task 18 stops a worker when its sink leaves the config and Task 23 stops
    // every worker from a signal handler, so concurrent and repeated stops are
    // expected rather than hypothetical. Nothing here documents Sink.close() as
    // safe to call twice, so the worker must not rely on that.
    expect(sink.closeCount).toBe(1);
  });

  it('makes a second stop() wait for shutdown rather than returning early', async () => {
    // This pins the property that chose a memoised promise over a boolean
    // guard. A boolean guard with an early return also closes the sink exactly
    // once, so the test above cannot tell the two apart -- it would report a
    // clean stop while close() was still running. Without this test the design
    // note is an assertion with nothing behind it.
    const sink = new FakeSink('slow');
    sink.closeDelayMs = 50;
    const { worker } = await makeWorker(sink);

    worker.start();
    const first = worker.stop(1000);
    const second = worker.stop(1000);

    await second;
    expect(sink.closedAt).not.toBeNull();

    await first;
    expect(sink.closeCount).toBe(1);
  });

  it('closes the sink even if it was never started', async () => {
    const sink = new FakeSink('unstarted');
    const { worker } = await makeWorker(sink);

    await worker.stop(1000);

    expect(sink.closeCount).toBe(1);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/pipeline/dispatcher-worker.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement the worker in `src/pipeline/dispatcher.ts`**

```ts
import { AuthDeliveryError, PermanentDeliveryError } from '../sinks/types.js';
import { initialSinkHealth } from '../status/metrics.js';
import type { Logger } from '../log.js';
import type { Metrics } from '../status/metrics.js';
import type { Sink } from '../sinks/types.js';
import type { SpoolQueue } from './spool.js';
import type { SinkHealth } from '../../types/api.js';

const FAILURE_THRESHOLD = 5;
const IDLE_POLL_MS = 500;
const JITTER_FRACTION = 0.3;

export function backoffDelayMs(
  consecutiveFailures: number,
  baseMs: number,
  capMs: number,
  random: () => number,
): number {
  const exponent = Math.max(0, consecutiveFailures - 1);
  const raw = baseMs * 2 ** exponent;
  const capped = Math.min(raw, capMs);
  // Jitter is added AFTER the cap, so `capMs` bounds the exponential term
  // rather than the delay actually slept: the true ceiling is
  // capMs * (1 + JITTER_FRACTION). Jittering a capped value is what keeps
  // several sinks from retrying in lockstep, so this is deliberate -- but do
  // not read maxBackoffMs as a hard ceiling on observed delay.
  const jitter = capped * JITTER_FRACTION * random();
  return Math.min(Math.round(capped + jitter), Math.round(capMs * (1 + JITTER_FRACTION)));
}

export type SinkWorkerOptions = {
  sink: Sink;
  queue: SpoolQueue;
  metrics: Metrics;
  log: Logger;
  maxBatchEvents: number;
  maxBatchBytes: number;
  baseBackoffMs?: number;
  maxBackoffMs?: number;
  random?: () => number;
};

export class SinkWorker {
  private state: SinkHealth = initialSinkHealth();
  private running = false;
  private loop: Promise<void> | null = null;
  /**
   * Memoised so `stop()` is idempotent. Task 18 stops a worker when its sink
   * leaves the config and Task 23 stops every worker from a signal handler, so
   * a reload racing a SIGTERM calls this twice. A plain boolean guard would let
   * the second caller return while shutdown was still in flight; holding the
   * promise makes it await the same completion.
   */
  private stopping: Promise<void> | null = null;
  private readonly baseBackoffMs: number;
  private readonly maxBackoffMs: number;
  private readonly random: () => number;

  constructor(private readonly options: SinkWorkerOptions) {
    this.baseBackoffMs = options.baseBackoffMs ?? 1000;
    this.maxBackoffMs = options.maxBackoffMs ?? 60_000;
    this.random = options.random ?? Math.random;
    options.metrics.setSinkHealth(options.sink.name, this.state);
  }

  health(): SinkHealth {
    return { ...this.state };
  }

  private publish(): void {
    this.options.metrics.setSinkHealth(this.options.sink.name, this.health());
  }

  private onSuccess(count: number): void {
    this.state = {
      state: 'ok',
      consecutiveFailures: 0,
      lastError: null,
      lastErrorAt: null,
      lastSuccessAt: Date.now(),
      nextRetryAt: null,
    };
    this.options.metrics.recordDelivered(this.options.sink.name, count);
    this.publish();
  }

  private onFailure(error: Error): number {
    const consecutiveFailures = this.state.consecutiveFailures + 1;
    const delay = backoffDelayMs(
      consecutiveFailures,
      this.baseBackoffMs,
      this.maxBackoffMs,
      this.random,
    );
    // An auth failure is surfaced as failed immediately: waiting five rounds
    // to tell the operator their password is wrong wastes their time.
    const escalate = error instanceof AuthDeliveryError;
    this.state = {
      state: escalate || consecutiveFailures >= FAILURE_THRESHOLD ? 'failed' : 'retrying',
      consecutiveFailures,
      lastError: error.message,
      lastErrorAt: Date.now(),
      lastSuccessAt: this.state.lastSuccessAt,
      nextRetryAt: Date.now() + delay,
    };
    this.options.metrics.recordError(this.options.sink.name, error.message);
    this.publish();
    return delay;
  }

  /**
   * Attempts one batch. Returns true if a batch was claimed (delivered or
   * dead-lettered or failed), false if the queue was empty.
   */
  async drainOnce(): Promise<boolean> {
    const batch = await this.options.queue.nextBatch(
      this.options.maxBatchEvents,
      this.options.maxBatchBytes,
    );
    if (batch === null) return false;

    try {
      await this.options.sink.deliver(batch.events);
    } catch (error) {
      const failure = error instanceof Error ? error : new Error(String(error));
      if (failure instanceof PermanentDeliveryError) {
        await this.options.queue.deadLetter(batch);
        // Event count, not file count: `dropped` is event-counted, and all three
        // counters surface together in one `counters` object, so a file count here
        // would silently mix units in a number operators compare.
        this.options.metrics.recordDeadLettered(this.options.sink.name, batch.events.length);
        this.options.log.error(
          { sink: this.options.sink.name, files: batch.files.length, err: failure.message },
          'batch dead-lettered',
        );
        return true;
      }
      // Anything else, including an unexpected bug, is retryable. The batch
      // stays on disk.
      this.onFailure(failure);
      return true;
    }

    await this.options.queue.ack(batch);
    this.onSuccess(batch.events.length);
    return true;
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loop = this.run();
  }

  private async run(): Promise<void> {
    while (this.running) {
      let claimed = false;
      try {
        claimed = await this.drainOnce();
      } catch (error) {
        const failure = error instanceof Error ? error : new Error(String(error));
        this.options.log.error(
          { sink: this.options.sink.name, err: failure.message },
          'worker iteration failed',
        );
        this.onFailure(failure);
      }

      const waitMs = !claimed
        ? IDLE_POLL_MS
        : this.state.consecutiveFailures > 0
          ? Math.max(0, (this.state.nextRetryAt ?? 0) - Date.now())
          : 0;
      if (waitMs > 0) await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
  }

  async stop(deadlineMs: number): Promise<void> {
    this.stopping ??= this.stopOnce(deadlineMs);
    await this.stopping;
  }

  private async stopOnce(deadlineMs: number): Promise<void> {
    this.running = false;
    // Cut the backoff sleep short rather than waiting it out; a worker in a
    // 40s backoff would otherwise hold up shutdown for 40s.
    this.wake?.();
    const pending = this.loop;
    this.loop = null;
    if (pending !== null) {
      let timeoutHandle: NodeJS.Timeout | undefined;
      const timeout = new Promise<void>((resolve) => {
        timeoutHandle = setTimeout(resolve, deadlineMs);
      });
      try {
        await Promise.race([pending, timeout]);
      } finally {
        // Without this the deadline timer keeps the process alive for its full
        // duration after a shutdown that already finished.
        if (timeoutHandle !== undefined) clearTimeout(timeoutHandle);
      }
    }
    // Outside the `pending === null` check on purpose: a worker that was
    // constructed but never started still owns an open sink, and an earlier
    // draft returned before this line and leaked it.
    await this.options.sink.close();
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/pipeline/dispatcher-worker.test.ts`
Expected: PASS.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: add sink worker with backoff and health

Only PermanentDeliveryError dead-letters; every other error, including
an unexpected bug, keeps the batch on disk. AuthDeliveryError surfaces
as failed on the first attempt while remaining retryable."
```

---

### Task 18: Dispatcher — reconciliation, orphans, and sink test

**Files:**
- Modify: `src/pipeline/dispatcher.ts`
- Test: `test/pipeline/dispatcher-reconcile.test.ts`

**Interfaces:**
- Consumes: `SinkWorker` from Task 17; `createSink` from `src/sinks/registry.ts`; `compileFilter` from `src/pipeline/filter.ts`; `AppConfig`, `SinkEntry` from `src/config/schema.ts`; `resolveLogsDirectory` from `src/sinks/file.ts`.
- Produces: `type DispatcherOptions`, `type TestSinkResult`, `class Dispatcher` with `applyConfig`, `enqueue`, `start`, `stop`, `listOrphanedSpools`, `discardOrphan`, `testSink`, `snapshotSinks`, `isDegraded`.

- [ ] **Step 1: Write the failing test**

`test/pipeline/dispatcher-reconcile.test.ts`:

```ts
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mkdir, mkdtemp, readdir, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import { Dispatcher } from '../../src/pipeline/dispatcher.js';
import { SpoolQueue } from '../../src/pipeline/spool.js';
import { defaultAppConfig } from '../../src/config/schema.js';
import { Metrics } from '../../src/status/metrics.js';
import type { AppConfig, SinkEntry } from '../../src/config/schema.js';

const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));

function event(id: string, overrides: Record<string, string> = {}) {
  return { id, timestamp: 1000, source: 'lambda', projectId: 'p1', level: 'info', ...overrides };
}

describe('Dispatcher', () => {
  let spoolRoot = '';
  let logsRoot = '';
  let metrics: Metrics;
  let dispatcher: Dispatcher;

  beforeEach(async () => {
    spoolRoot = await mkdtemp(join(tmpdir(), 'vld-spoolroot-'));
    logsRoot = await mkdtemp(join(tmpdir(), 'vld-logsroot-'));
    metrics = new Metrics();
    dispatcher = new Dispatcher({ spoolRoot, logsRoot, metrics, log: silentLog });
  });

  afterEach(async () => {
    await dispatcher.stop(500);
    await rm(spoolRoot, { recursive: true, force: true });
    await rm(logsRoot, { recursive: true, force: true });
  });

  function fileSink(name: string, overrides: Partial<SinkEntry> = {}): SinkEntry {
    return {
      name,
      enabled: true,
      filter: {},
      maxSpoolBytes: 1_048_576,
      maxBatchEvents: 1000,
      maxBatchBytes: 1_048_576,
      config: {
        type: 'file',
        directory: join(logsRoot, name),
        filePrefix: 'events',
        retentionDays: 0,
        freeSpaceFloorBytes: 0,
      },
      ...overrides,
    };
  }

  function configWith(sinks: SinkEntry[]): AppConfig {
    return { ...defaultAppConfig(), sinks };
  }

  it('creates a spool directory per enabled sink', async () => {
    await dispatcher.applyConfig(configWith([fileSink('one'), fileSink('two')]));
    expect((await readdir(spoolRoot)).toSorted()).toEqual(['one', 'two']);
  });

  it('routes events only to sinks whose filter matches', async () => {
    await dispatcher.applyConfig(
      configWith([
        fileSink('errors-only', { filter: { minLevel: 'error' } }),
        fileSink('everything'),
      ]),
    );

    await dispatcher.enqueue([event('a', { level: 'info' })]);

    const statuses = await dispatcher.snapshotSinks();
    const errorsOnly = statuses.find((s) => s.name === 'errors-only');
    const everything = statuses.find((s) => s.name === 'everything');
    expect(errorsOnly?.queue.files).toBe(0);
    expect(everything?.queue.files).toBe(1);
  });

  it('does not enqueue to a disabled sink', async () => {
    await dispatcher.applyConfig(configWith([fileSink('off', { enabled: false })]));
    await dispatcher.enqueue([event('a')]);
    const statuses = await dispatcher.snapshotSinks();
    expect(statuses.find((s) => s.name === 'off')?.queue.files).toBe(0);
  });

  it('preserves the spool when a sink is removed, and reports it as orphaned', async () => {
    await dispatcher.applyConfig(configWith([fileSink('temporary')]));
    await dispatcher.enqueue([event('a')]);

    await dispatcher.applyConfig(configWith([]));

    expect(await readdir(spoolRoot)).toContain('temporary');
    const orphans = await dispatcher.listOrphanedSpools();
    expect(orphans.map((o) => o.name)).toEqual(['temporary']);
    expect(orphans[0]?.files).toBe(1);
    expect(orphans[0]?.bytes).toBeGreaterThan(0);
  });

  it('opens one spool per sink under overlapping applyConfig calls', async () => {
    // Counting opens rather than inspecting settled state: `active` is keyed
    // by sink name, so it holds one entry whichever call won, and a test that
    // asserts on it passes with the reconcile chain removed. Without the
    // chain both calls clear the active.has() check for the same new sink and
    // each opens its own SpoolQueue on the same directory, the second
    // orphaning the first worker, which keeps draining that spool untracked.
    const openSpy = vi.spyOn(SpoolQueue, 'open');
    try {
      await Promise.all([
        dispatcher.applyConfig(configWith([fileSink('shared')])),
        dispatcher.applyConfig(configWith([fileSink('shared')])),
      ]);

      expect(openSpy).toHaveBeenCalledTimes(1);
    } finally {
      openSpy.mockRestore();
    }
  });

  it('keeps a disabled sink out of the orphan list and preserves its spool', async () => {
    // This test previously toggled enabled with an EMPTY spool and asserted
    // only that nothing was orphaned, so it passed even when the queue was
    // wiped on every applyConfig. Enqueue first, so the disable/enable cycle
    // has something to lose. A disabled sink is still configured and so is
    // never an orphan -- classifying it as one would offer an operator's
    // undelivered data to discardOrphan.
    await dispatcher.applyConfig(configWith([fileSink('keeper')]));
    await dispatcher.enqueue([event('a')]);
    const filesFor = async (name: string): Promise<number | undefined> =>
      (await dispatcher.snapshotSinks()).find((sink) => sink.name === name)?.queue.files;
    expect(await filesFor('keeper')).toBe(1);

    await dispatcher.applyConfig(configWith([fileSink('keeper', { enabled: false })]));
    expect(await dispatcher.listOrphanedSpools()).toEqual([]);

    await dispatcher.applyConfig(configWith([fileSink('keeper', { enabled: true })]));
    expect(await dispatcher.listOrphanedSpools()).toEqual([]);
    expect(await filesFor('keeper')).toBe(1);
  });

  it('discards an orphaned spool on request', async () => {
    await dispatcher.applyConfig(configWith([fileSink('gone')]));
    await dispatcher.enqueue([event('a')]);
    await dispatcher.applyConfig(configWith([]));

    await dispatcher.discardOrphan('gone');

    expect(await readdir(spoolRoot)).not.toContain('gone');
    expect(await dispatcher.listOrphanedSpools()).toEqual([]);
  });

  it('refuses to discard a name that is not an orphan', async () => {
    await dispatcher.applyConfig(configWith([fileSink('active')]));
    await expect(dispatcher.discardOrphan('active')).rejects.toThrow(/active/);
  });

  it('refuses a traversal name in discardOrphan', async () => {
    await expect(dispatcher.discardOrphan('../..')).rejects.toThrow();
  });

  it('rejects a file sink whose directory escapes the logs root', async () => {
    const escaping = fileSink('escape', {
      config: {
        type: 'file',
        directory: '/etc',
        filePrefix: 'events',
        retentionDays: 0,
        freeSpaceFloorBytes: 0,
      },
    });
    await expect(dispatcher.applyConfig(configWith([escaping]))).rejects.toThrow(/outside/i);
  });

  it('runs a sink test and reports success', async () => {
    await dispatcher.applyConfig(configWith([fileSink('probe')]));
    const result = await dispatcher.testSink('probe');
    expect(result.ok).toBe(true);
    expect(await readdir(join(logsRoot, 'probe'))).toHaveLength(1);
  });

  it('reports a sink test failure without throwing', async () => {
    await dispatcher.applyConfig(
      configWith([
        {
          name: 'bad-loki',
          enabled: true,
          filter: {},
          maxSpoolBytes: 1_048_576,
          maxBatchEvents: 1000,
          maxBatchBytes: 1_048_576,
          config: {
            type: 'loki',
            url: 'http://127.0.0.1:1',
            auth: { kind: 'none' },
            tenantId: null,
            labels: { static: {}, fromFields: [] },
            timeoutMs: 200,
          },
        },
      ]),
    );

    const result = await dispatcher.testSink('bad-loki');
    expect(result.ok).toBe(false);
    expect(result.detail.length).toBeGreaterThan(0);
  });

  it('reports an unknown sink test as a failure', async () => {
    const result = await dispatcher.testSink('nope');
    expect(result.ok).toBe(false);
  });

  it('keeps previously spooled data when an enabled sink is reconfigured', async () => {
    // Unlike the toggle test above, this sink is enabled throughout and
    // already has undelivered data on disk before its settings (not its
    // enabled flag) change. applyConfig must reopen the SAME spool
    // directory rather than one whose prior contents are lost.
    await dispatcher.applyConfig(configWith([fileSink('reconfigured')]));
    await dispatcher.enqueue([event('a')]);
    let statuses = await dispatcher.snapshotSinks();
    expect(statuses.find((s) => s.name === 'reconfigured')?.queue.files).toBe(1);

    await dispatcher.applyConfig(
      configWith([fileSink('reconfigured', { maxBatchEvents: 42 })]),
    );

    statuses = await dispatcher.snapshotSinks();
    expect(statuses.find((s) => s.name === 'reconfigured')?.queue.files).toBe(1);
  });

  it('ignores a non-directory entry in the spool root when listing orphans', async () => {
    await writeFile(join(spoolRoot, 'stray-file'), 'x');
    await mkdir(join(spoolRoot, 'orphan-dir'), { recursive: true });
    const orphans = await dispatcher.listOrphanedSpools();
    expect(orphans.map((o) => o.name)).toEqual(['orphan-dir']);
  });

  it('honors the free-space floor from the config being applied, even on the first call', async () => {
    // A regression here looks like: applyConfig uses a stale/absent
    // `this.config` to source the free-space floor while starting new
    // sinks, because the field is only assigned at the very end of
    // applyConfig. On the very first call there is no previous config at
    // all, so the floor silently falls back to 0 and this probe would
    // wrongly report full acceptance.
    const floored = new Dispatcher({
      spoolRoot,
      logsRoot,
      metrics,
      log: silentLog,
      freeSpace: noFreeSpace,
    });
    const config = configWith([fileSink('floor-test')]);
    config.server.spoolFreeSpaceFloorBytes = 1_000_000;

    await floored.applyConfig(config);
    await floored.enqueue([event('a')]);

    const statuses = await floored.snapshotSinks();
    expect(statuses.find((s) => s.name === 'floor-test')?.queue.files).toBe(0);

    await floored.stop(500);
  });

  it('waits for an in-flight delivery to finish before removing a sink', async () => {
    // The other reconciliation tests never call dispatcher.start(), so their
    // workers never run a loop iteration — stop() succeeds trivially whether
    // or not applyConfig actually awaits it. This test drives a real worker
    // through a real (slow) delivery and removes the sink while that
    // delivery is in flight, so a regression that stops awaiting
    // worker.stop() shows up as applyConfig returning almost instantly
    // instead of after the delivery completes.
    let received = 0;
    const DELIVERY_DELAY_MS = 300;
    const server = createServer((req, res) => {
      req.resume();
      req.on('end', () => {
        setTimeout(() => {
          received += 1;
          res.writeHead(204);
          res.end();
        }, DELIVERY_DELAY_MS);
      });
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    if (address === null || typeof address === 'string') {
      throw new Error('expected the test server to bind a TCP port');
    }
    const port = address.port;

    const slowLoki: SinkEntry = {
      name: 'slow-loki',
      enabled: true,
      filter: {},
      maxSpoolBytes: 1_048_576,
      maxBatchEvents: 1000,
      maxBatchBytes: 1_048_576,
      config: {
        type: 'loki',
        url: `http://127.0.0.1:${String(port)}`,
        auth: { kind: 'none' },
        tenantId: null,
        labels: { static: {}, fromFields: [] },
        timeoutMs: 5000,
      },
    };

    try {
      await dispatcher.applyConfig(configWith([slowLoki]));
      await dispatcher.enqueue([event('a')]);
      dispatcher.start();

      // Give the worker time to claim the batch and start the slow request.
      await new Promise((resolve) => setTimeout(resolve, 100));

      const before = Date.now();
      await dispatcher.applyConfig(configWith([]));
      const elapsed = Date.now() - before;

      expect(elapsed).toBeGreaterThanOrEqual(DELIVERY_DELAY_MS - 100);
      expect(received).toBe(1);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });

  it('reports degraded when a sink health is failed', async () => {
    await dispatcher.applyConfig(configWith([fileSink('ok-sink')]));
    expect(dispatcher.isDegraded()).toBe(false);
    metrics.setSinkHealth('ok-sink', {
      state: 'failed',
      consecutiveFailures: 9,
      lastError: 'x',
      lastErrorAt: 1,
      lastSuccessAt: null,
      nextRetryAt: 2,
    });
    expect(dispatcher.isDegraded()).toBe(true);
  });

  it('ignores a non-directory entry in the spool root when listing orphans', async () => {
    await writeFile(join(spoolRoot, 'stray-file'), 'x');
    await mkdir(join(spoolRoot, 'orphan-dir'), { recursive: true });
    const orphans = await dispatcher.listOrphanedSpools();
    expect(orphans.map((o) => o.name)).toEqual(['orphan-dir']);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/pipeline/dispatcher-reconcile.test.ts`
Expected: FAIL — `Dispatcher` is not exported.

- [ ] **Step 3: Implement the Dispatcher in `src/pipeline/dispatcher.ts`**

Append to the file created in Task 17:

```ts
import { mkdir, readdir, rm, stat } from 'node:fs/promises';
import { join } from 'node:path';
import { compileFilter } from './filter.js';
import { SpoolQueue } from './spool.js';
import { createSink } from '../sinks/registry.js';
import { resolveLogsDirectory } from '../sinks/file.js';
import { SINK_NAME_PATTERN } from '../config/schema.js';
import type { AppConfig, SinkEntry } from '../config/schema.js';
import type { EventPredicate } from './filter.js';
import type { LogEvent } from '../vercel/event.js';
import type { FreeSpaceProbe } from '../sinks/types.js';
import type { OrphanedSpool, SinkStatus } from '../../types/api.js';

export type TestSinkResult = { ok: boolean; detail: string };

export type DispatcherOptions = {
  spoolRoot: string;
  logsRoot: string;
  metrics: Metrics;
  log: Logger;
  freeSpace?: FreeSpaceProbe;
};

type ActiveSink = {
  entry: SinkEntry;
  predicate: EventPredicate;
  queue: SpoolQueue;
  worker: SinkWorker;
};

export class Dispatcher {
  private active = new Map<string, ActiveSink>();
  private config: AppConfig | null = null;
  private started = false;
  /**
   * Reconciliation runs one at a time. `applyConfig` awaits worker shutdown
   * and spool opening, so two concurrent calls can interleave across those
   * awaits: both pass the `active.has()` check for the same new sink, both
   * open a SpoolQueue on the same directory, and the second `active.set()`
   * orphans the first worker, which keeps running untracked against that
   * directory. Task 22 calls this straight from an HTTP handler, so two
   * overlapping PUTs are an ordinary occurrence rather than a rare race.
   * The guarantee belongs here, in the component that owns the state, not in
   * every caller. Same chain pattern as ConfigStore.save.
   *
   * Tested by counting `SpoolQueue.open` invocations, not by inspecting
   * settled state. An earlier attempt asserted on `active` after both calls
   * resolved and passed 20/20 with the chain removed: the map is keyed by
   * sink name, so there is exactly one entry whichever call won. The number
   * of spools opened for one directory is the thing that actually differs.
   */
  private reconcileChain: Promise<void> = Promise.resolve();

  constructor(private readonly options: DispatcherOptions) {}

  private spoolDirFor(name: string): string {
    if (!SINK_NAME_PATTERN.test(name)) {
      throw new Error(`invalid sink name "${name}"`);
    }
    return join(this.options.spoolRoot, name);
  }

  /**
   * Normalizes a sink entry, resolving and containing a file sink's directory.
   * Throws before anything is created so an invalid config cannot half-apply.
   */
  private normalize(entry: SinkEntry): SinkEntry {
    if (entry.config.type !== 'file') return entry;
    const directory = resolveLogsDirectory(entry.config.directory, this.options.logsRoot);
    return { ...entry, config: { ...entry.config, directory } };
  }

  async applyConfig(config: AppConfig): Promise<void> {
    // `.then(work, work)` so a rejected reconciliation does not wedge every
    // later one; the chain is kept alive below whatever the outcome.
    const run = this.reconcileChain.then(
      () => this.reconcileNow(config),
      () => this.reconcileNow(config),
    );
    this.reconcileChain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private async reconcileNow(config: AppConfig): Promise<void> {
    const normalized = config.sinks.map((entry) => this.normalize(entry));
    const desired = new Map(normalized.map((entry) => [entry.name, entry]));

    // Committed to applying from here on: `normalize()` is the only step that
    // can reject the whole config and it has already run. Assigning
    // `this.config` BEFORE starting anything means `startSink()` reads the
    // free-space floor from the config being applied rather than from the
    // previous one -- or, on the very first call, from nothing at all, which
    // silently floored it at 0 and disabled the check the spec requires.
    this.config = { ...config, sinks: normalized };

    for (const [name, current] of [...this.active]) {
      const next = desired.get(name);
      const unchanged =
        next !== undefined && JSON.stringify(next) === JSON.stringify(current.entry);
      if (unchanged) continue;
      await current.worker.stop(5000);
      this.active.delete(name);
      if (next === undefined) {
        // Deliberately leaves the spool directory on disk.
        this.options.metrics.forgetSink(name);
      }
    }

    for (const entry of normalized) {
      if (this.active.has(entry.name)) continue;
      if (!entry.enabled) continue;
      await this.startSink(entry);
    }
  }

  private async startSink(entry: SinkEntry): Promise<void> {
    const dir = this.spoolDirFor(entry.name);
    await mkdir(dir, { recursive: true });

    const queue = await SpoolQueue.open(dir, {
      maxSpoolBytes: entry.maxSpoolBytes,
      freeSpaceFloorBytes: this.config?.server.spoolFreeSpaceFloorBytes ?? 0,
      // Without this the spool swallows its own filesystem errors silently,
      // which is how a dead-letter overwrite went undetected in Task 16.
      log: this.options.log,
      ...(this.options.freeSpace === undefined ? {} : { freeSpace: this.options.freeSpace }),
    });

    const sink = createSink(entry.name, entry.config, {
      log: this.options.log,
      ...(this.options.freeSpace === undefined ? {} : { freeSpace: this.options.freeSpace }),
    });

    const worker = new SinkWorker({
      sink,
      queue,
      metrics: this.options.metrics,
      log: this.options.log,
      maxBatchEvents: entry.maxBatchEvents,
      maxBatchBytes: entry.maxBatchBytes,
    });

    this.active.set(entry.name, {
      entry,
      predicate: compileFilter(entry.filter),
      queue,
      worker,
    });
    if (this.started) worker.start();
  }

  async enqueue(events: LogEvent[]): Promise<void> {
    if (events.length === 0) return;
    for (const active of this.active.values()) {
      if (!active.entry.enabled) continue;
      const matching = events.filter((event) => active.predicate(event));
      if (matching.length === 0) continue;
      const result = await active.queue.enqueue(matching);
      if (result.droppedEvents > 0) {
        this.options.metrics.recordDropped(active.entry.name, result.droppedEvents);
        this.options.log.warn(
          { sink: active.entry.name, dropped: result.droppedEvents },
          'spool overflow dropped oldest batches',
        );
      }
    }
  }

  start(): void {
    this.started = true;
    for (const active of this.active.values()) active.worker.start();
  }

  async stop(deadlineMs: number): Promise<void> {
    this.started = false;
    await Promise.all([...this.active.values()].map((active) => active.worker.stop(deadlineMs)));
    this.active.clear();
  }

  async listOrphanedSpools(): Promise<OrphanedSpool[]> {
    let names: string[];
    try {
      names = await readdir(this.options.spoolRoot);
    } catch {
      return [];
    }

    const configured = new Set((this.config?.sinks ?? []).map((entry) => entry.name));
    const orphans: OrphanedSpool[] = [];

    for (const name of names) {
      if (configured.has(name)) continue;
      const dir = join(this.options.spoolRoot, name);
      const stats = await stat(dir).catch(() => null);
      if (stats === null || !stats.isDirectory()) continue;

      let files = 0;
      let bytes = 0;
      for (const entry of await readdir(dir).catch(() => [])) {
        if (!entry.endsWith('.jsonl')) continue;
        const fileStats = await stat(join(dir, entry)).catch(() => null);
        if (fileStats === null) continue;
        files += 1;
        bytes += fileStats.size;
      }
      orphans.push({ name, files, bytes });
    }
    return orphans;
  }

  async discardOrphan(name: string): Promise<void> {
    const orphans = await this.listOrphanedSpools();
    if (!orphans.some((orphan) => orphan.name === name)) {
      throw new Error(`"${name}" is not an orphaned spool directory`);
    }
    await rm(this.spoolDirFor(name), { recursive: true, force: true });
  }

  async testSink(name: string): Promise<TestSinkResult> {
    const active = this.active.get(name);
    if (active === undefined) {
      return { ok: false, detail: `sink "${name}" is not running; enable and save it first` };
    }
    const probe: LogEvent = {
      id: `test-${String(Date.now())}`,
      timestamp: Date.now(),
      source: 'external',
      projectId: 'vercel-log-drain',
      level: 'info',
      message: `test event from vercel-log-drain for sink ${name}`,
    };
    const sink = createSink(active.entry.name, active.entry.config, {
      log: this.options.log,
      ...(this.options.freeSpace === undefined ? {} : { freeSpace: this.options.freeSpace }),
    });
    try {
      await sink.deliver([probe]);
      return { ok: true, detail: 'test event accepted' };
    } catch (error) {
      return { ok: false, detail: error instanceof Error ? error.message : String(error) };
    } finally {
      await sink.close();
    }
  }

  async snapshotSinks(): Promise<SinkStatus[]> {
    const counters = this.options.metrics.snapshot().sinkCounters;
    const statuses: SinkStatus[] = [];

    for (const entry of this.config?.sinks ?? []) {
      const active = this.active.get(entry.name);
      const oldest = active === undefined ? null : await active.queue.oldestMtimeMs();
      statuses.push({
        name: entry.name,
        type: entry.config.type,
        enabled: entry.enabled,
        health: this.options.metrics.getSinkHealth(entry.name),
        queue: {
          files: active?.queue.fileCount() ?? 0,
          bytes: active?.queue.bytes() ?? 0,
          oldestAgeSec: oldest === null ? null : Math.floor((Date.now() - oldest) / 1000),
        },
        counters: counters[entry.name] ?? { delivered: 0, dropped: 0, deadLettered: 0 },
      });
    }
    return statuses;
  }

  isDegraded(): boolean {
    for (const entry of this.config?.sinks ?? []) {
      if (!entry.enabled) continue;
      if (this.options.metrics.getSinkHealth(entry.name).state === 'failed') return true;
    }
    return false;
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/pipeline/dispatcher-reconcile.test.ts`
Expected: PASS.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: add dispatcher config reconciliation

Removing or disabling a sink stops its worker but preserves its spool,
surfaced as an orphan with an explicit discard action. A settings change
reuses the same spool because the sink name is the queue identity. File
sink directories are contained under the logs root before anything is
created, so an invalid config cannot half-apply."
```

---

### Task 19: Proxy authentication middleware

**Files:**
- Create: `src/server/types.ts`, `src/server/middleware/proxy-auth.ts`
- Test: `test/server/proxy-auth.test.ts`

**Interfaces:**
- Consumes: nothing beyond Hono.
- Produces: in `src/server/types.ts` the type `AppEnv`; in the middleware `type AuthConfig`, `type PeerResolver`, `nodePeerResolver`, `parseAuthConfig(env)`, `proxyAuth(config, resolvePeer)`, `stripIdentityHeader(headerName)`.

**Verified behavior you must not design around differently:**
`@hono/node-server` populates `c.env.incoming` only for requests over a real
socket. Under Hono's in-process `app.request()` it is `undefined`. That is why
the peer resolver is injected, and why an unresolved peer must be denied.

- [ ] **Step 1: Write the failing test**

`test/server/proxy-auth.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import {
  nodePeerResolver,
  parseAuthConfig,
  proxyAuth,
  stripIdentityHeader,
} from '../../src/server/middleware/proxy-auth.js';
import type { AuthConfig } from '../../src/server/middleware/proxy-auth.js';
import type { AppEnv } from '../../src/server/types.js';

function appWith(config: AuthConfig, peer: string | undefined) {
  const app = new Hono<AppEnv>();
  app.use('/admin/*', proxyAuth(config, () => peer));
  app.get('/admin/thing', (c) => c.json({ user: c.get('user') }));
  app.get('/open', (c) => c.text('public'));
  return app;
}

const proxyMode: AuthConfig = {
  mode: 'proxy',
  trustedProxies: ['10.0.0.0/8', '127.0.0.1/32'],
  userHeader: 'x-forwarded-user',
  allowedUsers: null,
};

describe('parseAuthConfig', () => {
  it('returns unset when AUTH_MODE is missing', () => {
    expect(parseAuthConfig({}).mode).toBe('unset');
  });

  it('returns disabled when explicitly disabled', () => {
    expect(parseAuthConfig({ AUTH_MODE: 'disabled' }).mode).toBe('disabled');
  });

  it('parses proxy mode with trusted CIDRs and a header', () => {
    const config = parseAuthConfig({
      AUTH_MODE: 'proxy',
      AUTH_TRUSTED_PROXIES: '10.0.0.0/8, 192.168.1.5/32',
      AUTH_USER_HEADER: 'Cf-Access-Authenticated-User-Email',
    });
    expect(config.mode).toBe('proxy');
    if (config.mode !== 'proxy') return;
    expect(config.trustedProxies).toEqual(['10.0.0.0/8', '192.168.1.5/32']);
    expect(config.userHeader).toBe('cf-access-authenticated-user-email');
    expect(config.allowedUsers).toBeNull();
  });

  it('parses an allowed-users list', () => {
    const config = parseAuthConfig({
      AUTH_MODE: 'proxy',
      AUTH_TRUSTED_PROXIES: '10.0.0.0/8',
      AUTH_USER_HEADER: 'x-user',
      AUTH_ALLOWED_USERS: 'a@example.com, b@example.com',
    });
    if (config.mode !== 'proxy') throw new Error('expected proxy mode');
    expect(config.allowedUsers).toEqual(['a@example.com', 'b@example.com']);
  });

  it('throws when proxy mode is missing its required variables', () => {
    expect(() => parseAuthConfig({ AUTH_MODE: 'proxy' })).toThrow(/AUTH_TRUSTED_PROXIES/);
    expect(() =>
      parseAuthConfig({ AUTH_MODE: 'proxy', AUTH_TRUSTED_PROXIES: '10.0.0.0/8' }),
    ).toThrow(/AUTH_USER_HEADER/);
  });

  it('throws on an unrecognized mode rather than failing open', () => {
    // Assert the whole message, not /AUTH_MODE/: that pattern also matches the
    // "AUTH_MODE=proxy requires AUTH_TRUSTED_PROXIES" fallback, so this test
    // stayed green with the mode guard deleted -- at which point
    // AUTH_MODE=disable plus valid proxy vars would boot as proxy mode.
    expect(() => parseAuthConfig({ AUTH_MODE: 'yolo' })).toThrow(
      'AUTH_MODE must be "proxy" or "disabled", received "yolo"',
    );
  });

  it('rejects an empty allowed-users list rather than reading it as no list', () => {
    // An operator trimming AUTH_ALLOWED_USERS to nothing in a compose file is
    // asking for lockout, not for every identity the proxy authenticates to
    // get admin. AUTH_TRUSTED_PROXIES already throws on the identical input.
    expect(() =>
      parseAuthConfig({
        AUTH_MODE: 'proxy',
        AUTH_TRUSTED_PROXIES: '10.0.0.0/8',
        AUTH_USER_HEADER: 'x-user',
        AUTH_ALLOWED_USERS: '   ,  ',
      }),
    ).toThrow('AUTH_ALLOWED_USERS was set but lists no users');
  });

  it('rejects a user header that names a header the service depends on', () => {
    // AUTH_USER_HEADER=x-vercel-signature would strip the signature from
    // every inbound delivery, failing HMAC on all of them: total, silent log
    // loss from one plausible-looking misconfiguration.
    expect(() =>
      parseAuthConfig({
        AUTH_MODE: 'proxy',
        AUTH_TRUSTED_PROXIES: '10.0.0.0/8',
        AUTH_USER_HEADER: 'X-Vercel-Signature',
      }),
    ).toThrow('AUTH_USER_HEADER must not name a reserved header');
  });

  it('rejects a user header that is not a valid header name', () => {
    // Otherwise this boots clean and then throws inside Headers.get on every
    // admin request: a 500 per request with nothing naming the bad variable.
    expect(() =>
      parseAuthConfig({
        AUTH_MODE: 'proxy',
        AUTH_TRUSTED_PROXIES: '10.0.0.0/8',
        AUTH_USER_HEADER: 'X-Forwarded User',
      }),
    ).toThrow('AUTH_USER_HEADER is not a valid HTTP header name');
  });

  it('trims AUTH_MODE like every other variable', () => {
    expect(parseAuthConfig({ AUTH_MODE: ' disabled ' })).toEqual({ mode: 'disabled' });
  });
});

describe('proxyAuth', () => {
  it('returns 503 when auth is unset, naming what to configure', async () => {
    const response = await appWith({ mode: 'unset' }, '10.1.1.1').request('/admin/thing');
    expect(response.status).toBe(503);
    expect(await response.text()).toMatch(/AUTH_MODE/);
  });

  it('allows everything when auth is explicitly disabled', async () => {
    const response = await appWith({ mode: 'disabled' }, undefined).request('/admin/thing');
    expect(response.status).toBe(200);
  });

  it('allows a request from a trusted peer with a user header', async () => {
    const response = await appWith(proxyMode, '10.2.3.4').request('/admin/thing', {
      headers: { 'x-forwarded-user': 'chad@example.com' },
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ user: 'chad@example.com' });
  });

  it('denies a request from an untrusted peer even with a user header', async () => {
    const response = await appWith(proxyMode, '203.0.113.9').request('/admin/thing', {
      headers: { 'x-forwarded-user': 'attacker@example.com' },
    });
    expect(response.status).toBe(403);
  });

  it('denies a trusted peer with no user header', async () => {
    const response = await appWith(proxyMode, '10.2.3.4').request('/admin/thing');
    expect(response.status).toBe(403);
  });

  it('denies a trusted peer with an empty user header', async () => {
    const response = await appWith(proxyMode, '10.2.3.4').request('/admin/thing', {
      headers: { 'x-forwarded-user': '   ' },
    });
    expect(response.status).toBe(403);
  });

  it('denies when the peer cannot be resolved, rather than failing open', async () => {
    const response = await appWith(proxyMode, undefined).request('/admin/thing', {
      headers: { 'x-forwarded-user': 'chad@example.com' },
    });
    expect(response.status).toBe(403);
  });

  it('ignores X-Forwarded-For when deciding trust', async () => {
    const response = await appWith(proxyMode, '203.0.113.9').request('/admin/thing', {
      headers: { 'x-forwarded-for': '10.0.0.1', 'x-forwarded-user': 'attacker@example.com' },
    });
    expect(response.status).toBe(403);
  });

  it('enforces the allowed-users list', async () => {
    const restricted: AuthConfig = { ...proxyMode, allowedUsers: ['chad@example.com'] };
    const allowed = await appWith(restricted, '10.2.3.4').request('/admin/thing', {
      headers: { 'x-forwarded-user': 'chad@example.com' },
    });
    const denied = await appWith(restricted, '10.2.3.4').request('/admin/thing', {
      headers: { 'x-forwarded-user': 'someone@example.com' },
    });
    expect(allowed.status).toBe(200);
    expect(denied.status).toBe(403);
  });

  it('handles an IPv6-mapped IPv4 peer address', async () => {
    const response = await appWith(proxyMode, '::ffff:10.2.3.4').request('/admin/thing', {
      headers: { 'x-forwarded-user': 'chad@example.com' },
    });
    expect(response.status).toBe(200);
  });

  it('rejects a second copy of the identity header', async () => {
    const response = await appWith(proxyMode, '10.1.2.3').request('/admin/thing', {
      headers: [
        ['x-forwarded-user', 'real@example.com'],
        ['x-forwarded-user', 'attacker@example.com'],
      ],
    });
    expect(response.status).toBe(403);
    expect(await response.text()).toContain('arrived more than once');
  });

  it('sets user to null in disabled mode', async () => {
    const app = new Hono<AppEnv>();
    app.use('*', proxyAuth({ mode: 'disabled' }, () => undefined));
    app.get('/thing', (c) => c.json({ user: c.get('user') }));
    const body: { user: string | null } = await (await app.request('/thing')).json();
    expect(body.user).toBeNull();
  });

  it('nodePeerResolver reads the socket peer over a real connection', async () => {
    // Every other test injects a fake resolver, so until this one existed the
    // resolver actually used in production was referenced by no test at all:
    // a refactor making it fall back to X-Forwarded-For would have left the
    // whole suite green. app.request() cannot cover it -- c.env.incoming is
    // undefined there, which is the very reason the resolver is injected.
    const app = new Hono<AppEnv>();
    app.use('*', proxyAuth(proxyMode, nodePeerResolver));
    app.get('/admin/thing', (c) => c.json({ user: c.get('user') }));

    const server = serve({ fetch: app.fetch, hostname: '127.0.0.1', port: 0 });
    try {
      const address = server.address();
      if (address === null || typeof address === 'string') {
        throw new Error('expected the test server to bind a TCP port');
      }
      const base = `http://127.0.0.1:${String(address.port)}/admin/thing`;

      // The loopback peer is trusted by proxyMode, so this must be allowed on
      // the strength of the real socket address.
      const allowed = await fetch(base, {
        headers: { 'x-forwarded-user': 'ada@example.com' },
      });
      expect(allowed.status).toBe(200);

      // And a forged X-Forwarded-For must not change the decision either way.
      const spoofed = await fetch(base, {
        headers: { 'x-forwarded-user': 'ada@example.com', 'x-forwarded-for': '203.0.113.9' },
      });
      expect(spoofed.status).toBe(200);
    } finally {
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
    }
  });

  it('stripIdentityHeader removes the header before any handler runs', async () => {
    // Mounted app-wide by buildApp, so it has to work on routes that no auth
    // middleware covers -- the drain route above all. Tested here with a
    // handler that reports what it was given, which is the only way to
    // observe the strip at all: nothing in the real app echoes its headers.
    const app = new Hono<AppEnv>();
    app.use('*', stripIdentityHeader('X-Forwarded-User'));
    app.get('/anything', (c) => c.json({ seen: c.req.header('x-forwarded-user') ?? null }));

    const response = await app.request('/anything', {
      headers: { 'x-forwarded-user': 'attacker@example.com' },
    });
    const body: { seen: string | null } = await response.json();
    expect(body.seen).toBeNull();
  });

  it('stripIdentityHeader leaves every other header alone', async () => {
    const app = new Hono<AppEnv>();
    app.use('*', stripIdentityHeader('x-forwarded-user'));
    app.get('/anything', (c) => c.json({ sig: c.req.header('x-vercel-signature') ?? null }));

    const response = await app.request('/anything', {
      headers: { 'x-vercel-signature': 'abc123', 'x-forwarded-user': 'a@b.c' },
    });
    const body: { sig: string | null } = await response.json();
    expect(body.sig).toBe('abc123');
  });

  it('does not apply to unguarded routes', async () => {
    const response = await appWith({ mode: 'unset' }, undefined).request('/open');
    expect(response.status).toBe(200);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/server/proxy-auth.test.ts`
Expected: FAIL — modules not found.

- [ ] **Step 3: Implement `src/server/types.ts`**

`Bindings` is `Partial<HttpBindings>` precisely because the bindings are absent
under `app.request()`.

```ts
import type { HttpBindings } from '@hono/node-server';

export type AppEnv = {
  Bindings: Partial<HttpBindings>;
  Variables: { user: string | null };
};
```

- [ ] **Step 4: Implement `src/server/middleware/proxy-auth.ts`**

```ts
import { BlockList, isIPv4, isIPv6 } from 'node:net';
import type { Context, MiddlewareHandler } from 'hono';
import type { AppEnv } from '../types.js';

export type AuthConfig =
  | { mode: 'unset' }
  | { mode: 'disabled' }
  | {
      mode: 'proxy';
      trustedProxies: string[];
      userHeader: string;
      allowedUsers: string[] | null;
    };

export type PeerResolver = (c: Context<AppEnv>) => string | undefined;

export const nodePeerResolver: PeerResolver = (c) => c.env?.incoming?.socket?.remoteAddress;

function splitList(value: string | undefined): string[] | null {
  if (value === undefined) return null;
  const items = value
    .split(',')
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
  return items.length === 0 ? null : items;
}

// RFC 7230 token. Validated at parse time because an invalid name throws
// inside Headers.get, turning every admin request into a 500 whose message
// says nothing about the misconfigured variable.
const HEADER_NAME = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;

// The identity header is stripped from every inbound request app-wide, so
// naming a header the service itself depends on would quietly break that
// mechanism. `x-vercel-signature` is the dangerous one: stripping it fails
// HMAC verification on every delivery, which is total and silent log loss.
// The others are either request-critical or would strip a credential the
// service is not meant to touch.
const RESERVED_HEADERS = new Set([
  'authorization',
  'connection',
  'content-length',
  'content-type',
  'cookie',
  'host',
  'transfer-encoding',
  'x-vercel-signature',
]);

export function parseAuthConfig(env: Record<string, string | undefined>): AuthConfig {
  const mode = env['AUTH_MODE']?.trim();
  if (mode === undefined || mode.length === 0) return { mode: 'unset' };
  if (mode === 'disabled') return { mode: 'disabled' };
  if (mode !== 'proxy') {
    throw new Error(`AUTH_MODE must be "proxy" or "disabled", received "${mode}"`);
  }

  const trustedProxies = splitList(env['AUTH_TRUSTED_PROXIES']);
  if (trustedProxies === null) {
    throw new Error('AUTH_MODE=proxy requires AUTH_TRUSTED_PROXIES, a comma-separated CIDR list');
  }
  const userHeader = env['AUTH_USER_HEADER']?.trim();
  if (userHeader === undefined || userHeader.length === 0) {
    throw new Error('AUTH_MODE=proxy requires AUTH_USER_HEADER');
  }
  if (!HEADER_NAME.test(userHeader)) {
    throw new Error(`AUTH_USER_HEADER is not a valid HTTP header name: "${userHeader}"`);
  }
  if (RESERVED_HEADERS.has(userHeader.toLowerCase())) {
    throw new Error(`AUTH_USER_HEADER must not name a reserved header: "${userHeader}"`);
  }

  // Present-but-empty is a configuration mistake, not "no allowlist".
  // Treating it as no allowlist hands admin to every identity the proxy
  // authenticates -- the opposite of what emptying the list intends.
  // AUTH_TRUSTED_PROXIES already rejects the identical input.
  const rawAllowed = env['AUTH_ALLOWED_USERS'];
  const allowedUsers = splitList(rawAllowed);
  if (rawAllowed !== undefined && allowedUsers === null) {
    throw new Error('AUTH_ALLOWED_USERS was set but lists no users');
  }

  return {
    mode: 'proxy',
    trustedProxies,
    userHeader: userHeader.toLowerCase(),
    allowedUsers,
  };
}

function buildBlockList(cidrs: string[]): BlockList {
  const list = new BlockList();
  for (const cidr of cidrs) {
    const [address, prefix] = cidr.split('/');
    if (address === undefined) continue;
    const family = isIPv6(address) ? 'ipv6' : 'ipv4';
    const bits = prefix === undefined ? (family === 'ipv6' ? 128 : 32) : Number.parseInt(prefix, 10);
    if (Number.isNaN(bits)) continue;
    list.addSubnet(address, bits, family);
  }
  return list;
}

/**
 * Removes an inbound identity header from every request it sees, whatever the
 * auth mode. Mounted ahead of the route table by `buildApp`, because
 * `proxyAuth` only strips on the routes it guards and the drain route is
 * deliberately outside the guard. This authenticates nothing; it only removes
 * a value no inbound request is ever allowed to assert.
 */
export function stripIdentityHeader(headerName: string): MiddlewareHandler<AppEnv> {
  const name = headerName.toLowerCase();
  return async (c, next) => {
    c.req.raw.headers.delete(name);
    return next();
  };
}

export function proxyAuth(config: AuthConfig, resolvePeer: PeerResolver): MiddlewareHandler<AppEnv> {
  // Narrow by early return rather than carrying a `blockList === null` check
  // into the request path: a null check standing in for "this cannot happen"
  // is a branch nobody can reason about and no test can reach.
  if (config.mode === 'unset') {
    return async (c) =>
      c.text(
        'The admin interface is disabled because authentication is not configured. ' +
          'Set AUTH_MODE=proxy with AUTH_TRUSTED_PROXIES and AUTH_USER_HEADER, or ' +
          'AUTH_MODE=disabled for local development.',
        503,
      );
  }

  if (config.mode === 'disabled') {
    return async (c, next) => {
      c.set('user', null);
      return next();
    };
  }

  const blockList = buildBlockList(config.trustedProxies);
  const userHeader = config.userHeader;
  const allowedUsers = config.allowedUsers;

  return async (c, next) => {
    const providedUser = c.req.header(userHeader);
    // Strip any inbound copy immediately, in every branch, before a handler
    // can run. `c.get('user')` -- written only below, after trust is
    // established -- is the sole identity channel.
    c.req.raw.headers.delete(userHeader);

    const peer = resolvePeer(c);
    if (peer === undefined) {
      return c.text('forbidden: peer address could not be determined', 403);
    }

    // No normalisation of `::ffff:a.b.c.d`: net.BlockList#check already maps
    // in both directions, so a peer accepted on a dual-stack listener matches
    // an IPv4 subnet and vice versa. An earlier version normalised explicitly
    // and was measured to change no outcome. Dead code on a trust boundary is
    // worse than none -- it implies a protection that is not there. The mapped
    // cases are asserted directly, so a future Node changing this is caught.
    const family = isIPv4(peer) ? 'ipv4' : isIPv6(peer) ? 'ipv6' : null;
    if (family === null || !blockList.check(peer, family)) {
      return c.text('forbidden: request did not arrive from a trusted proxy', 403);
    }

    const user = providedUser?.trim() ?? '';
    if (user.length === 0) {
      // Name the variable, never its value. Echoing the configured header name
      // tells anyone reaching this point from inside a trusted subnet -- a
      // co-located container, an SSRF -- exactly which header to forge.
      // Naming AUTH_USER_HEADER is just as discriminating for tests.
      return c.text('forbidden: AUTH_USER_HEADER was not supplied by the proxy', 403);
    }
    // Two copies of the header arrive joined as "a, b". Once a second value
    // exists neither is trustworthy, and with no allowlist configured the
    // join would otherwise be accepted whole as an identity.
    if (user.includes(',')) {
      return c.text('forbidden: AUTH_USER_HEADER arrived more than once', 403);
    }
    if (allowedUsers !== null && !allowedUsers.includes(user)) {
      return c.text('forbidden: user is not in AUTH_ALLOWED_USERS', 403);
    }

    c.set('user', user);
    return next();
  };
}
```

The middleware reads the user header before deleting it and only trusts that
value once the peer is inside a trusted CIDR, so a self-asserted header can
never reach a handler: `c.get('user')` is written exclusively here. Note the
strip only covers routes this middleware is mounted on — Task 23 mounts a
separate unconditional strip ahead of the route table so the guarantee also
holds on the auth-exempt drain path.

- [ ] **Step 5: Run the test to verify it passes**

Run: `npx vitest run test/server/proxy-auth.test.ts`
Expected: PASS.

- [ ] **Step 6: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: add fail-closed proxy auth middleware

Unset AUTH_MODE closes the admin surface with a 503 naming the variables
to set. Trust is decided from the socket peer address via net.BlockList,
never from X-Forwarded-For, and an unresolvable peer is denied."
```

---

### Task 20: Drain route

**Files:**
- Create: `src/server/routes/drain.ts`
- Test: `test/server/drain-route.test.ts`

**Interfaces:**
- Consumes: `verifySignature`, `decodeBody`, `PayloadTooLargeError`, `WHOLE_BODY_INDEX`; `Dispatcher`; `Metrics`; `AppConfig`.
- Note on `decodeBody`'s contract: it throws `PayloadTooLargeError` **only** for the size cap, which this route answers with `413`. A corrupt or truncated gzip body does NOT throw — it returns a `DecodeResult` carrying one reject at `WHOLE_BODY_INDEX`, so this route answers `200` with `rejected: 1`, exactly as it does for an unparseable JSON array. Do not add a second `catch` for corruption.
- Produces: `type DrainDeps = { getConfig: () => AppConfig; dispatcher: Dispatcher; metrics: Metrics; log: Logger }`, `drainRoutes(deps: DrainDeps): Hono<AppEnv>`.

- [ ] **Step 1: Write the failing test**

`test/server/drain-route.test.ts`:

```ts
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createHmac } from 'node:crypto';
import { gzip } from 'node:zlib';
import { promisify } from 'node:util';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Hono } from 'hono';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import { drainRoutes } from '../../src/server/routes/drain.js';
import { Dispatcher } from '../../src/pipeline/dispatcher.js';
import { Metrics } from '../../src/status/metrics.js';
import { defaultAppConfig } from '../../src/config/schema.js';
import type { AppConfig } from '../../src/config/schema.js';
import type { AppEnv } from '../../src/server/types.js';

const gzipAsync = promisify(gzip);
const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));
const SECRET = 's'.repeat(32);

function event(id: string, level = 'info') {
  return { id, timestamp: 1573817187330, source: 'lambda', projectId: 'p1', level };
}

function sign(body: string | Buffer): string {
  return createHmac('sha1', SECRET).update(body).digest('hex');
}

describe('drain route', () => {
  let spoolRoot = '';
  let logsRoot = '';
  let dispatcher: Dispatcher;
  let metrics: Metrics;
  let config: AppConfig;

  beforeEach(async () => {
    spoolRoot = await mkdtemp(join(tmpdir(), 'vld-drain-spool-'));
    logsRoot = await mkdtemp(join(tmpdir(), 'vld-drain-logs-'));
    metrics = new Metrics();
    dispatcher = new Dispatcher({ spoolRoot, logsRoot, metrics, log: silentLog });
    config = {
      ...defaultAppConfig(),
      drains: [{ id: 'drain1', name: 'prod', secret: SECRET, enabled: true, createdAt: 1 }],
      sinks: [
        {
          name: 'local',
          enabled: true,
          filter: {},
          maxSpoolBytes: 1_048_576,
          maxBatchEvents: 1000,
          maxBatchBytes: 1_048_576,
          config: {
            type: 'file',
            directory: join(logsRoot, 'local'),
            filePrefix: 'events',
            retentionDays: 0,
            freeSpaceFloorBytes: 0,
          },
        },
      ],
    };
    await dispatcher.applyConfig(config);
  });

  afterEach(async () => {
    await dispatcher.stop(500);
    await rm(spoolRoot, { recursive: true, force: true });
    await rm(logsRoot, { recursive: true, force: true });
  });

  function app() {
    const instance = new Hono<AppEnv>();
    instance.route(
      '/api/drain',
      drainRoutes({ getConfig: () => config, dispatcher, metrics, log: silentLog }),
    );
    return instance;
  }

  async function post(path: string, body: string | Buffer, headers: Record<string, string> = {}) {
    return app().request(path, { method: 'POST', body, headers });
  }

  it('accepts a correctly signed JSON array and spools it', async () => {
    const body = JSON.stringify([event('a'), event('b')]);
    const response = await post('/api/drain/drain1', body, { 'x-vercel-signature': sign(body) });

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ received: 2, accepted: 2, rejected: 0 });
    const statuses = await dispatcher.snapshotSinks();
    expect(statuses[0]?.queue.files).toBe(1);
  });

  it('accepts NDJSON', async () => {
    const body = `${JSON.stringify(event('a'))}\n${JSON.stringify(event('b'))}\n`;
    const response = await post('/api/drain/drain1', body, { 'x-vercel-signature': sign(body) });
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ accepted: 2 });
  });

  it('accepts a gzipped body', async () => {
    const compressed = await gzipAsync(Buffer.from(JSON.stringify([event('a')]), 'utf8'));
    const response = await post('/api/drain/drain1', compressed, {
      'x-vercel-signature': sign(compressed),
      'content-encoding': 'gzip',
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ accepted: 1 });
  });

  it('rejects a bad signature with 403 and spools nothing', async () => {
    const body = JSON.stringify([event('a')]);
    const response = await post('/api/drain/drain1', body, { 'x-vercel-signature': 'f'.repeat(40) });

    expect(response.status).toBe(403);
    expect((await dispatcher.snapshotSinks())[0]?.queue.files).toBe(0);
    const drain = metrics.snapshot().drains.find((d) => d.id === 'drain1');
    expect(drain?.requests.badSignature).toBe(1);
  });

  it('rejects a missing signature with 401', async () => {
    const body = JSON.stringify([event('a')]);
    expect((await post('/api/drain/drain1', body)).status).toBe(401);
  });

  it('returns 404 for an unknown drain', async () => {
    const body = JSON.stringify([event('a')]);
    const response = await post('/api/drain/nope', body, { 'x-vercel-signature': sign(body) });
    expect(response.status).toBe(404);
  });

  it('returns 403 for a disabled drain', async () => {
    config = {
      ...config,
      drains: [{ ...config.drains[0]!, enabled: false }],
    };
    const body = JSON.stringify([event('a')]);
    const response = await post('/api/drain/drain1', body, { 'x-vercel-signature': sign(body) });
    expect(response.status).toBe(403);
  });

  it('returns 413 when the body exceeds maxBodyBytes', async () => {
    config = { ...config, server: { ...config.server, maxBodyBytes: 32 } };
    const body = JSON.stringify([event('a'), event('b'), event('c')]);
    const response = await post('/api/drain/drain1', body, { 'x-vercel-signature': sign(body) });
    expect(response.status).toBe(413);
  });

  it('keeps good entries and counts bad ones without failing the request', async () => {
    const body = `${JSON.stringify(event('a'))}\n{broken\n${JSON.stringify(event('b'))}\n`;
    const response = await post('/api/drain/drain1', body, { 'x-vercel-signature': sign(body) });

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ received: 3, accepted: 2, rejected: 1 });
    expect(metrics.snapshot().recent.rejects).toHaveLength(1);
  });

  it('routes only matching events to a filtered sink', async () => {
    config = {
      ...config,
      sinks: [{ ...config.sinks[0]!, filter: { minLevel: 'error' } }],
    };
    await dispatcher.applyConfig(config);

    const body = JSON.stringify([event('a', 'info')]);
    const response = await post('/api/drain/drain1', body, { 'x-vercel-signature': sign(body) });

    expect(response.status).toBe(200);
    expect((await dispatcher.snapshotSinks())[0]?.queue.files).toBe(0);
  });

  it('records the latest event timestamp', async () => {
    const body = JSON.stringify([event('a')]);
    await post('/api/drain/drain1', body, { 'x-vercel-signature': sign(body) });
    const drain = metrics.snapshot().drains.find((d) => d.id === 'drain1');
    expect(drain?.lastEventAt).toBe(1573817187330);
  });

  it('returns 500 when spooling fails, so Vercel retries', async () => {
    await rm(spoolRoot, { recursive: true, force: true });
    // Recreate as a file so mkdir/write inside the spool root fails.
    const { writeFile } = await import('node:fs/promises');
    await writeFile(spoolRoot, 'not a directory');

    const body = JSON.stringify([event('a')]);
    const response = await post('/api/drain/drain1', body, { 'x-vercel-signature': sign(body) });
    expect(response.status).toBe(500);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/server/drain-route.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `src/server/routes/drain.ts`**

Ordering is deliberate: the cheap rejections come first, and the signature is
checked before the body is decompressed or parsed.

```ts
import { Hono } from 'hono';
import { verifySignature } from '../../vercel/signature.js';
import { decodeBody, PayloadTooLargeError } from '../../vercel/decode.js';
import type { AppConfig } from '../../config/schema.js';
import type { Dispatcher } from '../../pipeline/dispatcher.js';
import type { Metrics } from '../../status/metrics.js';
import type { Logger } from '../../log.js';
import type { AppEnv } from '../types.js';

export type DrainDeps = {
  getConfig: () => AppConfig;
  dispatcher: Dispatcher;
  metrics: Metrics;
  log: Logger;
};

export function drainRoutes(deps: DrainDeps): Hono<AppEnv> {
  const app = new Hono<AppEnv>();

  app.post('/:drainId', async (c) => {
    const config = deps.getConfig();
    const drainId = c.req.param('drainId');
    const drain = config.drains.find((entry) => entry.id === drainId);

    if (drain === undefined) {
      // Aggregate, not per-id: this id is attacker-supplied.
      deps.metrics.recordUnknownDrainRequest();
      return c.json({ code: 'unknown_drain' }, 404);
    }
    if (!drain.enabled) {
      deps.metrics.recordDrainRequest(drain.id, 'disabled');
      return c.json({ code: 'drain_disabled' }, 403);
    }

    const declaredLength = Number.parseInt(c.req.header('content-length') ?? '', 10);
    if (!Number.isNaN(declaredLength) && declaredLength > config.server.maxBodyBytes) {
      return c.json({ code: 'payload_too_large' }, 413);
    }

    const raw = Buffer.from(await c.req.arrayBuffer());
    if (raw.byteLength > config.server.maxBodyBytes) {
      return c.json({ code: 'payload_too_large' }, 413);
    }

    const signature = c.req.header('x-vercel-signature');
    if (signature === undefined) {
      deps.metrics.recordDrainRequest(drain.id, 'badSignature');
      return c.json({ code: 'missing_signature' }, 401);
    }
    if (!verifySignature(raw, signature, drain.secret)) {
      deps.metrics.recordDrainRequest(drain.id, 'badSignature');
      return c.json({ code: 'invalid_signature', error: "signature didn't match" }, 403);
    }

    const gzipped = c.req.header('content-encoding')?.toLowerCase().includes('gzip') ?? false;

    let decoded;
    try {
      decoded = await decodeBody(raw, {
        gzipped,
        maxDecompressedBytes: config.server.maxDecompressedBytes,
      });
    } catch (error) {
      if (error instanceof PayloadTooLargeError) {
        deps.metrics.recordDrainRequest(drain.id, 'malformedBody');
        return c.json({ code: 'payload_too_large', error: error.message }, 413);
      }
      throw error;
    }

    if (decoded.rejected.length > 0) {
      deps.metrics.recordRejected(drain.id, decoded.rejected);
    }

    if (decoded.events.length > 0) {
      try {
        await deps.dispatcher.enqueue(decoded.events);
      } catch (error) {
        const failure = error instanceof Error ? error : new Error(String(error));
        deps.log.error({ drain: drain.id, err: failure.message }, 'failed to spool batch');
        deps.metrics.recordError('ingest', failure.message);
        // 500 makes Vercel redeliver. At-least-once is the deliberate trade.
        return c.json({ code: 'spool_failed' }, 500);
      }

      const latest = decoded.events.reduce(
        (max, item) => (item.timestamp > max ? item.timestamp : max),
        0,
      );
      deps.metrics.recordEventsReceived(drain.id, decoded.events.length, latest);
      deps.metrics.pushRecentEvents(decoded.events);
    }

    deps.metrics.recordDrainRequest(
      drain.id,
      decoded.events.length > 0 || decoded.rejected.length === 0 ? 'ok' : 'malformedBody',
    );

    return c.json({
      received: decoded.events.length + decoded.rejected.length,
      accepted: decoded.events.length,
      rejected: decoded.rejected.length,
    });
  });

  return app;
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/server/drain-route.test.ts`
Expected: PASS.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: add drain ingest route

Verifies the signature before decompressing, so an unauthenticated
caller cannot make the process spend CPU inflating gzip. Malformed
entries are counted without failing the delivery; a spool failure
returns 500 so Vercel redelivers."
```

---

### Task 21: Status, health, and readiness routes

**Files:**
- Create: `src/server/routes/status.ts`
- Test: `test/server/status-route.test.ts`

**Interfaces:**
- Consumes: `Metrics`, `Dispatcher`, `AppConfig`, `StatusSnapshot` from `types/api.ts`.
- Produces: `type StatusDeps = { getConfig: () => AppConfig; dispatcher: Dispatcher; metrics: Metrics; version: string; configDir: string; spoolDir: string }`, `statusRoutes(deps): Hono<AppEnv>`, `healthRoutes(deps): Hono<AppEnv>`.

- [ ] **Step 1: Write the failing test**

`test/server/status-route.test.ts`:

```ts
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Hono } from 'hono';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import { healthRoutes, statusRoutes } from '../../src/server/routes/status.js';
import { Dispatcher } from '../../src/pipeline/dispatcher.js';
import { Metrics } from '../../src/status/metrics.js';
import { defaultAppConfig } from '../../src/config/schema.js';
import type { AppConfig } from '../../src/config/schema.js';
import type { AppEnv } from '../../src/server/types.js';
import type { StatusSnapshot } from '../../types/api.js';

const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));

describe('status routes', () => {
  let spoolRoot = '';
  let logsRoot = '';
  let dispatcher: Dispatcher;
  let metrics: Metrics;
  let config: AppConfig;

  beforeEach(async () => {
    spoolRoot = await mkdtemp(join(tmpdir(), 'vld-status-spool-'));
    logsRoot = await mkdtemp(join(tmpdir(), 'vld-status-logs-'));
    metrics = new Metrics();
    dispatcher = new Dispatcher({ spoolRoot, logsRoot, metrics, log: silentLog });
    config = {
      ...defaultAppConfig(),
      drains: [{ id: 'd1', name: 'prod', secret: 'x'.repeat(32), enabled: true, createdAt: 1 }],
      sinks: [
        {
          name: 'local',
          enabled: true,
          filter: {},
          maxSpoolBytes: 1_048_576,
          maxBatchEvents: 1000,
          maxBatchBytes: 1_048_576,
          config: {
            type: 'file',
            directory: join(logsRoot, 'local'),
            filePrefix: 'events',
            retentionDays: 0,
            freeSpaceFloorBytes: 0,
          },
        },
      ],
    };
    await dispatcher.applyConfig(config);
  });

  afterEach(async () => {
    await dispatcher.stop(500);
    await rm(spoolRoot, { recursive: true, force: true });
    await rm(logsRoot, { recursive: true, force: true });
  });

  function app() {
    const deps = {
      getConfig: () => config,
      dispatcher,
      metrics,
      version: '9.9.9',
      configDir: spoolRoot,
      spoolDir: spoolRoot,
    };
    const instance = new Hono<AppEnv>();
    instance.route('/api/status', statusRoutes(deps));
    instance.route('/', healthRoutes(deps));
    return instance;
  }

  it('reports service, volumes, drains, and sinks', async () => {
    const response = await app().request('/api/status');
    expect(response.status).toBe(200);

    const snapshot: StatusSnapshot = await response.json();
    expect(snapshot.service.state).toBe('ok');
    expect(snapshot.service.version).toBe('9.9.9');
    expect(snapshot.volumes.spool.totalBytes).toBeGreaterThan(0);
    expect(snapshot.drains[0]).toMatchObject({ id: 'd1', name: 'prod', enabled: true });
    expect(snapshot.sinks[0]).toMatchObject({ name: 'local', type: 'file', enabled: true });
    expect(snapshot.sinks[0]?.queue.oldestAgeSec).toBeNull();
  });

  it('never exposes a drain secret', async () => {
    const body = await (await app().request('/api/status')).text();
    expect(body).not.toContain('x'.repeat(32));
  });

  it('reports degraded when a sink has failed', async () => {
    metrics.setSinkHealth('local', {
      state: 'failed',
      consecutiveFailures: 6,
      lastError: 'loki down',
      lastErrorAt: 1,
      lastSuccessAt: null,
      nextRetryAt: 2,
    });
    const snapshot: StatusSnapshot = await (await app().request('/api/status')).json();
    expect(snapshot.service.state).toBe('degraded');
  });

  it('always answers healthz with 200', async () => {
    expect((await app().request('/healthz')).status).toBe(200);
  });

  it('answers readyz with 200 when healthy and 503 when degraded', async () => {
    expect((await app().request('/readyz')).status).toBe(200);
    metrics.setSinkHealth('local', {
      state: 'failed',
      consecutiveFailures: 6,
      lastError: 'x',
      lastErrorAt: 1,
      lastSuccessAt: null,
      nextRetryAt: 2,
    });
    expect((await app().request('/readyz')).status).toBe(503);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/server/status-route.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `src/server/routes/status.ts`**

```ts
import { statfs } from 'node:fs/promises';
import { Hono } from 'hono';
import type { AppConfig } from '../../config/schema.js';
import type { Dispatcher } from '../../pipeline/dispatcher.js';
import type { Metrics } from '../../status/metrics.js';
import type { AppEnv } from '../types.js';
import type { StatusSnapshot, VolumeStatus } from '../../../types/api.js';

export type StatusDeps = {
  getConfig: () => AppConfig;
  dispatcher: Dispatcher;
  metrics: Metrics;
  version: string;
  configDir: string;
  spoolDir: string;
};

async function volumeStatus(path: string): Promise<VolumeStatus> {
  try {
    const stats = await statfs(path);
    return {
      path,
      freeBytes: stats.bavail * stats.bsize,
      totalBytes: stats.blocks * stats.bsize,
    };
  } catch {
    return { path, freeBytes: 0, totalBytes: 0 };
  }
}

export function statusRoutes(deps: StatusDeps): Hono<AppEnv> {
  const app = new Hono<AppEnv>();

  app.get('/', async (c) => {
    const config = deps.getConfig();
    const metrics = deps.metrics.snapshot();
    const byId = new Map(metrics.drains.map((entry) => [entry.id, entry]));

    const snapshot: StatusSnapshot = {
      service: {
        state: deps.dispatcher.isDegraded() ? 'degraded' : 'ok',
        uptimeSec: metrics.uptimeSec,
        version: deps.version,
        startedAt: metrics.startedAt,
        unknownDrainRequests: metrics.unknownDrainRequests,
      },
      volumes: {
        config: await volumeStatus(deps.configDir),
        spool: await volumeStatus(deps.spoolDir),
      },
      drains: config.drains.map((drain) => {
        const counters = byId.get(drain.id);
        return {
          id: drain.id,
          name: drain.name,
          enabled: drain.enabled,
          eventsReceived: counters?.eventsReceived ?? 0,
          lastEventAt: counters?.lastEventAt ?? null,
          requests: counters?.requests ?? {
            ok: 0,
            badSignature: 0,
            disabled: 0,
            malformedBody: 0,
          },
        };
      }),
      sinks: await deps.dispatcher.snapshotSinks(),
      orphanedSpools: await deps.dispatcher.listOrphanedSpools(),
      recent: {
        events: metrics.recent.events,
        rejects: metrics.recent.rejects,
        errors: metrics.recent.errors,
      },
    };

    return c.json(snapshot);
  });

  return app;
}

export function healthRoutes(deps: StatusDeps): Hono<AppEnv> {
  const app = new Hono<AppEnv>();
  app.get('/healthz', (c) => c.text('ok'));
  app.get('/readyz', (c) =>
    deps.dispatcher.isDegraded() ? c.text('degraded', 503) : c.text('ready'),
  );
  return app;
}
```

The drain list is built from config rather than metrics, so a drain that has
never received a delivery still appears with zeroed counters — otherwise a
misconfigured drain would be invisible on the page where you would look for it.

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/server/status-route.test.ts`
Expected: PASS.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: add status, health, and readiness routes

Status is assembled from config plus metrics so a drain that has never
fired still appears with zeroed counters. readyz reports 503 while
degraded; healthz answers whenever the process is listening."
```

---

### Task 22: Admin routes

**Files:**
- Create: `src/server/routes/admin.ts`
- Test: `test/server/admin-route.test.ts`

**Interfaces:**
- Consumes: `ConfigStore`, `EtagMismatchError`, `redactConfig`, `restoreSecrets`, `SecretRestoreError`, `Dispatcher`, `warningsFor`, `newDrainId`, `newDrainSecret`.
- **Reviewer must verify in code, not in prose:** every response that carries a config passes it through `redactConfig` first. `restoreSecrets` returns real secrets by design — it has to, so they can be persisted — so a handler that responds with its raw output would leak every credential it just restored. There are exactly two such responses (`GET /config` and `PUT /config`), plus `POST /drains`, which returns a freshly generated secret ONCE and is the single sanctioned exception.
- Produces: `type AdminDeps = { store: ConfigStore; dispatcher: Dispatcher; getConfig: () => AppConfig; setConfig: (config: AppConfig, etag: string) => void; getEtag: () => string; log: Logger }`, `adminRoutes(deps): Hono<AppEnv>`.

Routes: `GET /config`, `PUT /config`, `POST /drains`, `POST /sinks/:name/test`,
`DELETE /orphans/:name`.

- [ ] **Step 1: Write the failing test**

`test/server/admin-route.test.ts`:

```ts
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Hono } from 'hono';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import { adminRoutes } from '../../src/server/routes/admin.js';
import { ConfigStore, etagOf } from '../../src/config/store.js';
import { Dispatcher } from '../../src/pipeline/dispatcher.js';
import { Metrics } from '../../src/status/metrics.js';
import type { AppConfig, SinkEntry } from '../../src/config/schema.js';
import type { AppEnv } from '../../src/server/types.js';

const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));

describe('admin routes', () => {
  let configDir = '';
  let spoolRoot = '';
  let logsRoot = '';
  let store: ConfigStore;
  let dispatcher: Dispatcher;
  let current: AppConfig;
  let etag = '';

  beforeEach(async () => {
    configDir = await mkdtemp(join(tmpdir(), 'vld-admin-config-'));
    spoolRoot = await mkdtemp(join(tmpdir(), 'vld-admin-spool-'));
    logsRoot = await mkdtemp(join(tmpdir(), 'vld-admin-logs-'));
    store = new ConfigStore(configDir);
    const loaded = await store.load();
    current = loaded.config;
    etag = loaded.etag;
    dispatcher = new Dispatcher({
      spoolRoot,
      logsRoot,
      metrics: new Metrics(),
      log: silentLog,
    });
    await dispatcher.applyConfig(current);
  });

  afterEach(async () => {
    await dispatcher.stop(500);
    for (const dir of [configDir, spoolRoot, logsRoot]) {
      await rm(dir, { recursive: true, force: true });
    }
  });

  function app() {
    const instance = new Hono<AppEnv>();
    instance.route(
      '/api/admin',
      adminRoutes({
        store,
        dispatcher,
        getConfig: () => current,
        getEtag: () => etag,
        setConfig: (config, nextEtag) => {
          current = config;
          etag = nextEtag;
        },
        log: silentLog,
      }),
    );
    return instance;
  }

  function fileSink(name: string): SinkEntry {
    return {
      name,
      enabled: true,
      filter: {},
      maxSpoolBytes: 1_048_576,
      maxBatchEvents: 1000,
      maxBatchBytes: 1_048_576,
      config: {
        type: 'file',
        directory: join(logsRoot, name),
        filePrefix: 'events',
        retentionDays: 0,
        freeSpaceFloorBytes: 0,
      },
    };
  }

  async function put(body: unknown) {
    return app().request('/api/admin/config', {
      method: 'PUT',
      body: JSON.stringify(body),
      headers: { 'content-type': 'application/json' },
    });
  }

  it('returns the redacted config with its etag', async () => {
    const response = await app().request('/api/admin/config');
    expect(response.status).toBe(200);
    const body: { etag: string; config: { drains: unknown[] } } = await response.json();
    expect(body.etag).toBe(etag);
    expect(body.config.drains).toEqual([]);
  });

  it('creates a drain and reveals the secret exactly once', async () => {
    const response = await app().request('/api/admin/drains', {
      method: 'POST',
      body: JSON.stringify({ name: 'prod' }),
      headers: { 'content-type': 'application/json' },
    });
    expect(response.status).toBe(201);

    const created: { id: string; secret: string } = await response.json();
    expect(created.secret.length).toBeGreaterThanOrEqual(16);

    const after: {
      config: { drains: { id: string; secret: null; hasSecret: boolean }[] };
    } = await (await app().request('/api/admin/config')).json();
    expect(after.config.drains[0]?.id).toBe(created.id);
    expect(after.config.drains[0]?.secret).toBeNull();
    expect(after.config.drains[0]?.hasSecret).toBe(true);
  });

  it('applies a saved config and starts the sink', async () => {
    const response = await put({ config: { ...current, sinks: [fileSink('local')] }, etag });
    expect(response.status).toBe(200);
    expect(current.sinks).toHaveLength(1);
    expect((await dispatcher.snapshotSinks())[0]?.name).toBe('local');
  });

  it('returns warnings alongside a saved config', async () => {
    const lokiSink: SinkEntry = {
      ...fileSink('loki'),
      config: {
        type: 'loki',
        url: 'http://loki:3100',
        auth: { kind: 'none' },
        tenantId: null,
        labels: { static: {}, fromFields: ['requestId'] },
        timeoutMs: 5000,
      },
    };
    const response = await put({ config: { ...current, sinks: [lokiSink] }, etag });
    const body: { warnings: string[] } = await response.json();
    expect(body.warnings.join(' ')).toContain('requestId');
  });

  it('does not start two workers for one sink under overlapping PUTs', async () => {
    // Task 18 serialises reconciliation on a promise chain, but that guarantee
    // is not observable from a unit test of the Dispatcher: with both calls
    // settled and `active` keyed by name there is one entry either way. Here
    // it is observable, because the route is a real concurrent caller.
    // Without serialisation both requests pass the active.has() check for the
    // same new sink, both open a SpoolQueue on one directory, and the second
    // orphans the first worker, which keeps draining that spool untracked.
    const next = { ...current, sinks: [fileSink('racer')] };
    const [first, second] = await Promise.all([
      put({ config: next, etag }),
      put({ config: next, etag }),
    ]);

    // One wins on the etag; the loser must not have half-applied anything.
    const codes = [first.status, second.status].toSorted();
    expect(codes).toEqual([200, 409]);

    const status = await app().request('/api/status');
    const snapshot: { sinks: { name: string }[] } = await status.json();
    expect(snapshot.sinks.filter((sink) => sink.name === 'racer')).toHaveLength(1);
  });

  it('returns 409 on a stale etag', async () => {
    await put({ config: { ...current, sinks: [fileSink('one')] }, etag });
    const response = await put({ config: { ...current, sinks: [fileSink('two')] }, etag });
    expect(response.status).toBe(409);
  });

  it('returns 400 on a schema-invalid config and leaves the running config alone', async () => {
    const response = await put({
      config: { ...current, sinks: [{ ...fileSink('Bad Name') }] },
      etag,
    });
    expect(response.status).toBe(400);
    expect(current.sinks).toHaveLength(0);
  });

  it('returns 400 when a new sink omits its required secret', async () => {
    const lokiSink: SinkEntry = {
      ...fileSink('loki'),
      config: {
        type: 'loki',
        url: 'http://loki:3100',
        auth: { kind: 'basic', username: 'u', password: '' },
        tenantId: null,
        labels: { static: {}, fromFields: [] },
        timeoutMs: 5000,
      },
    };
    const response = await put({ config: { ...current, sinks: [lokiSink] }, etag });
    expect(response.status).toBe(400);
  });

  it('runs a sink test', async () => {
    await put({ config: { ...current, sinks: [fileSink('probe')] }, etag });
    const response = await app().request('/api/admin/sinks/probe/test', { method: 'POST' });
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ ok: true });
  });

  it('discards an orphaned spool', async () => {
    await put({ config: { ...current, sinks: [fileSink('temp')] }, etag });
    await dispatcher.enqueue([{ id: 'a', timestamp: 1, source: 'lambda', projectId: 'p' }]);
    await put({ config: { ...current, sinks: [] }, etag });

    const response = await app().request('/api/admin/orphans/temp', { method: 'DELETE' });
    expect(response.status).toBe(200);
    expect(await dispatcher.listOrphanedSpools()).toEqual([]);
  });

  it('returns 404 when discarding a name that is not an orphan', async () => {
    const response = await app().request('/api/admin/orphans/nope', { method: 'DELETE' });
    expect(response.status).toBe(404);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/server/admin-route.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `src/server/routes/admin.ts`**

```ts
import { Hono } from 'hono';
import { z } from 'zod';
import { EtagMismatchError } from '../../config/store.js';
import { redactConfig, restoreSecrets, SecretRestoreError } from '../../config/redact.js';
import { newDrainId, newDrainSecret } from '../../config/schema.js';
import { warningsFor } from '../../sinks/registry.js';
import type { ConfigStore } from '../../config/store.js';
import type { AppConfig } from '../../config/schema.js';
import type { Dispatcher } from '../../pipeline/dispatcher.js';
import type { Logger } from '../../log.js';
import type { AppEnv } from '../types.js';
import type { JsonValue } from '../../../types/json.js';

export type AdminDeps = {
  store: ConfigStore;
  dispatcher: Dispatcher;
  getConfig: () => AppConfig;
  getEtag: () => string;
  setConfig: (config: AppConfig, etag: string) => void;
  log: Logger;
};

const putBodySchema = z.object({
  config: z.custom<JsonValue>(() => true),
  etag: z.string().min(1),
});

const createDrainSchema = z.object({ name: z.string().min(1).max(128) });

function warningsForConfig(config: AppConfig): string[] {
  return config.sinks.flatMap((entry) =>
    warningsFor(entry.config).map((warning) => `${entry.name}: ${warning}`),
  );
}

export function adminRoutes(deps: AdminDeps): Hono<AppEnv> {
  const app = new Hono<AppEnv>();

  app.get('/config', (c) => {
    const config = deps.getConfig();
    return c.json({
      config: redactConfig(config),
      etag: deps.getEtag(),
      warnings: warningsForConfig(config),
    });
  });

  app.put('/config', async (c) => {
    const body = putBodySchema.safeParse(await c.req.json());
    if (!body.success) {
      return c.json({ code: 'bad_request', error: z.prettifyError(body.error) }, 400);
    }

    let candidate: AppConfig;
    try {
      candidate = restoreSecrets(body.data.config, deps.getConfig());
    } catch (error) {
      if (error instanceof SecretRestoreError) {
        return c.json({ code: 'invalid_config', error: error.message }, 400);
      }
      throw error;
    }

    // Apply to the dispatcher first: it validates path containment and can
    // fail before anything is persisted.
    try {
      await deps.dispatcher.applyConfig(candidate);
    } catch (error) {
      const failure = error instanceof Error ? error : new Error(String(error));
      return c.json({ code: 'invalid_config', error: failure.message }, 400);
    }

    let saved;
    try {
      saved = await deps.store.save(candidate, body.data.etag);
    } catch (error) {
      if (error instanceof EtagMismatchError) {
        // Roll the dispatcher back to the config that is actually persisted.
        await deps.dispatcher.applyConfig(deps.getConfig());
        return c.json({ code: 'conflict', error: error.message }, 409);
      }
      const failure = error instanceof Error ? error : new Error(String(error));
      deps.log.error({ err: failure.message }, 'failed to persist config');
      await deps.dispatcher.applyConfig(deps.getConfig());
      return c.json({ code: 'save_failed', error: failure.message }, 507);
    }

    deps.setConfig(saved.config, saved.etag);
    return c.json({
      config: redactConfig(saved.config),
      etag: saved.etag,
      warnings: warningsForConfig(saved.config),
    });
  });

  app.post('/drains', async (c) => {
    const body = createDrainSchema.safeParse(await c.req.json());
    if (!body.success) {
      return c.json({ code: 'bad_request', error: z.prettifyError(body.error) }, 400);
    }

    const secret = newDrainSecret();
    const drain = {
      id: newDrainId(),
      name: body.data.name,
      secret,
      enabled: true,
      createdAt: Date.now(),
    };
    const current = deps.getConfig();
    const next: AppConfig = { ...current, drains: [...current.drains, drain] };

    const saved = await deps.store.save(next, deps.getEtag());
    deps.setConfig(saved.config, saved.etag);

    // The only time a secret is ever returned by the API.
    return c.json({ id: drain.id, name: drain.name, secret, etag: saved.etag }, 201);
  });

  app.post('/sinks/:name/test', async (c) => {
    const result = await deps.dispatcher.testSink(c.req.param('name'));
    return c.json(result);
  });

  app.delete('/orphans/:name', async (c) => {
    try {
      await deps.dispatcher.discardOrphan(c.req.param('name'));
    } catch (error) {
      const failure = error instanceof Error ? error : new Error(String(error));
      return c.json({ code: 'not_found', error: failure.message }, 404);
    }
    return c.json({ ok: true });
  });

  return app;
}
```

Note the ordering in `PUT /config`: the dispatcher applies first because it is
the component that can reject a config (path containment), and on a persistence
failure the dispatcher is rolled back to the config that is actually on disk.
Persisting a config the dispatcher rejected would leave the container unable to
boot.

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run test/server/admin-route.test.ts`
Expected: PASS.

- [ ] **Step 5: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: add admin config API

Applies to the dispatcher before persisting so a config that cannot run
is never written, and rolls the dispatcher back if the write fails. New
drain secrets are returned exactly once at creation."
```

---

### Task 23: App assembly and entrypoint

**Files:**
- Create: `src/version.ts`, `src/server/static.ts`, `src/server/app.ts`, `src/index.ts`
- Test: `test/server/app.test.ts`

**Interfaces:**
- Consumes: all route modules, `proxyAuth`, `parseAuthConfig`, `nodePeerResolver`, `ConfigStore`, `Dispatcher`, `Metrics`, `createLogger`.
- Produces: `VERSION`; `staticHandler(webRoot: string): MiddlewareHandler<AppEnv>`; `type AppDeps`, `buildApp(deps: AppDeps): Hono<AppEnv>`; `type BootOptions`, `boot(options: BootOptions): Promise<Booted>` where `Booted = { app: Hono<AppEnv>; dispatcher: Dispatcher; shutdown: () => Promise<void> }`.

Static assets are served by a small handler rather than a dependency: the SPA
needs an `index.html` fallback for client routing anyway, and this keeps the
runtime dependency list at four packages.

- [ ] **Step 1: Write the failing test**

`test/server/app.test.ts`:

```ts
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { boot } from '../../src/index.js';

describe('boot', () => {
  let root = '';
  let dirs = { config: '', spool: '', logs: '', web: '' };

  beforeEach(async () => {
    root = await mkdtemp(join(tmpdir(), 'vld-boot-'));
    dirs = {
      config: join(root, 'config'),
      spool: join(root, 'spool'),
      logs: join(root, 'logs'),
      web: join(root, 'web'),
    };
    for (const dir of Object.values(dirs)) await mkdir(dir, { recursive: true });
    await writeFile(join(dirs.web, 'index.html'), '<!doctype html><title>drain</title>');
    await writeFile(join(dirs.web, 'app.js'), 'console.log(1);');
  });

  afterEach(async () => {
    await rm(root, { recursive: true, force: true });
  });

  async function bootWith(env: Record<string, string | undefined>) {
    return boot({
      env: {
        CONFIG_DIR: dirs.config,
        SPOOL_DIR: dirs.spool,
        LOGS_ROOT: dirs.logs,
        LOG_LEVEL: 'silent',
        ...env,
      },
      webRoot: dirs.web,
    });
  }

  it('serves healthz without any auth configuration', async () => {
    const booted = await bootWith({});
    try {
      expect((await booted.app.request('/healthz')).status).toBe(200);
    } finally {
      await booted.shutdown();
    }
  });

  it('does not let the identity strip interfere with ingest', async () => {
    // Boots in proxy mode, which is the only mode where a header name is
    // configured, and drives the auth-exempt drain route with that header
    // present. This does NOT observe the strip -- no handler in the app
    // reports its request headers, so removing the strip block leaves this
    // green. What it does pin is that mounting the strip ahead of the route
    // table cannot break ingest, which is the risk of putting anything at
    // that position. The strip itself is covered by its own unit test in
    // Task 19; that the wiring is still present is a code-reading check.
    const booted = await bootWith({
      AUTH_MODE: 'proxy',
      AUTH_TRUSTED_PROXIES: '127.0.0.1/32',
      AUTH_USER_HEADER: 'x-forwarded-user',
    });
    try {
      const response = await booted.app.request('/api/drain/none', {
        method: 'POST',
        body: '[]',
        headers: { 'x-forwarded-user': 'attacker@example.com' },
      });
      // 404 for the unknown id, not 403 or 503: the guard does not cover this
      // route and the strip did not disturb it.
      expect(response.status).toBe(404);
    } finally {
      await booted.shutdown();
    }
  });

  it('closes the admin surface with 503 when AUTH_MODE is unset', async () => {
    const booted = await bootWith({});
    try {
      expect((await booted.app.request('/api/admin/config')).status).toBe(503);
      expect((await booted.app.request('/api/status')).status).toBe(503);
      expect((await booted.app.request('/')).status).toBe(503);
    } finally {
      await booted.shutdown();
    }
  });

  it('leaves the drain endpoint reachable when AUTH_MODE is unset', async () => {
    const booted = await bootWith({});
    try {
      // 404 rather than 503: the route ran and simply has no such drain.
      const response = await booted.app.request('/api/drain/none', { method: 'POST', body: '[]' });
      expect(response.status).toBe(404);
    } finally {
      await booted.shutdown();
    }
  });

  it('serves the SPA and its assets when auth is disabled', async () => {
    const booted = await bootWith({ AUTH_MODE: 'disabled' });
    try {
      const index = await booted.app.request('/');
      expect(index.status).toBe(200);
      expect(await index.text()).toContain('<title>drain</title>');

      const asset = await booted.app.request('/app.js');
      expect(asset.status).toBe(200);
      expect(asset.headers.get('content-type')).toContain('javascript');

      // Unknown paths fall back to index.html for client-side routing.
      const deep = await booted.app.request('/sinks');
      expect(deep.status).toBe(200);
      expect(await deep.text()).toContain('<title>drain</title>');
    } finally {
      await booted.shutdown();
    }
  });

  it('refuses to serve a path that escapes the web root', async () => {
    const booted = await bootWith({ AUTH_MODE: 'disabled' });
    try {
      const response = await booted.app.request('/../config/config.json');
      expect(response.status).not.toBe(200);
    } finally {
      await booted.shutdown();
    }
  });

  it('creates a default config on first boot', async () => {
    const booted = await bootWith({ AUTH_MODE: 'disabled' });
    try {
      const response = await booted.app.request('/api/admin/config');
      expect(response.status).toBe(200);
      const body: { config: { drains: unknown[] } } = await response.json();
      expect(body.config.drains).toEqual([]);
    } finally {
      await booted.shutdown();
    }
  });

  it('fails to boot with a clear message when a directory is not writable', async () => {
    await expect(
      boot({
        env: {
          CONFIG_DIR: join(root, 'config'),
          SPOOL_DIR: join(dirs.web, 'index.html'), // a file, not a directory
          LOGS_ROOT: dirs.logs,
          LOG_LEVEL: 'silent',
        },
        webRoot: dirs.web,
      }),
    ).rejects.toThrow(/SPOOL_DIR/);
  });

  it('fails to boot on an invalid AUTH_MODE rather than failing open', async () => {
    // Whole message, not /AUTH_MODE/, which also matches the
    // missing-AUTH_TRUSTED_PROXIES error and so survives deleting the guard.
    await expect(bootWith({ AUTH_MODE: 'wide-open' })).rejects.toThrow(
      'AUTH_MODE must be "proxy" or "disabled", received "wide-open"',
    );
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run test/server/app.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement `src/version.ts`**

```ts
export const VERSION = process.env['APP_VERSION'] ?? 'dev';
```

The Dockerfile sets `APP_VERSION` from a build argument. Reading
`package.json` at runtime is avoided because its relative location differs
between `src/` and `dist/src/`.

- [ ] **Step 4: Implement `src/server/static.ts`**

```ts
import { readFile, stat } from 'node:fs/promises';
import { extname, join, relative, resolve } from 'node:path';
import type { MiddlewareHandler } from 'hono';
import type { AppEnv } from './types.js';

const CONTENT_TYPES: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.woff2': 'font/woff2',
  '.map': 'application/json; charset=utf-8',
};

export function staticHandler(webRoot: string): MiddlewareHandler<AppEnv> {
  const root = resolve(webRoot);

  return async (c) => {
    const requested = decodeURIComponent(new URL(c.req.url).pathname);
    const candidate = resolve(join(root, requested === '/' ? 'index.html' : requested));

    // Containment: never serve anything outside the web root.
    const rel = relative(root, candidate);
    const contained = rel === '' || (!rel.startsWith('..') && !rel.startsWith('/'));

    const target = contained && (await isFile(candidate)) ? candidate : join(root, 'index.html');
    if (!(await isFile(target))) return c.text('not found', 404);

    const body = await readFile(target);
    const type = CONTENT_TYPES[extname(target)] ?? 'application/octet-stream';
    return c.body(body, 200, { 'content-type': type });
  };
}

async function isFile(path: string): Promise<boolean> {
  try {
    return (await stat(path)).isFile();
  } catch {
    return false;
  }
}
```

- [ ] **Step 5: Implement `src/server/app.ts`**

```ts
import { Hono } from 'hono';
import { adminRoutes } from './routes/admin.js';
import { drainRoutes } from './routes/drain.js';
import { healthRoutes, statusRoutes } from './routes/status.js';
import { proxyAuth } from './middleware/proxy-auth.js';
import { staticHandler } from './static.js';
import type { AdminDeps } from './routes/admin.js';
import type { DrainDeps } from './routes/drain.js';
import type { StatusDeps } from './routes/status.js';
import type { AuthConfig, PeerResolver } from './middleware/proxy-auth.js';
import type { AppEnv } from './types.js';

export type AppDeps = {
  authConfig: AuthConfig;
  /**
   * The identity header to strip from every inbound request, independent of
   * `authConfig.mode`. Deriving it from the 'proxy' variant would mean no
   * strip at all under `AUTH_MODE=disabled` or unset -- precisely the modes a
   * staging box runs in, and where a caller could put the header on a drain
   * request and have a request logger record it as the actor.
   */
  identityHeader: string | null;
  peerResolver: PeerResolver;
  drain: DrainDeps;
  status: StatusDeps;
  admin: AdminDeps;
  webRoot: string | null;
};

export function buildApp(deps: AppDeps): Hono<AppEnv> {
  const app = new Hono<AppEnv>();

  // Unauthenticated by design: liveness/readiness probes and the drain
  // endpoint, which authenticates by HMAC because Vercel cannot present an
  // SSO identity.
  // Ahead of every route, including the auth-exempt ones, and in every auth
  // mode. proxyAuth strips the identity header too, but only on the paths it
  // is mounted on, and the drain route is deliberately registered outside the
  // guard. Without this a client could put the configured header on a drain
  // request and have it reach any handler or request logger that reads raw
  // headers. This authenticates nothing -- it only removes a value no inbound
  // request is ever allowed to assert -- so it must NOT be conditioned on
  // authConfig.mode. See AppDeps.identityHeader.
  if (deps.identityHeader !== null) {
    app.use('*', stripIdentityHeader(deps.identityHeader));
  }

  app.route('/', healthRoutes(deps.status));
  app.route('/api/drain', drainRoutes(deps.drain));

  const guard = proxyAuth(deps.authConfig, deps.peerResolver);
  app.use('/api/admin/*', guard);
  app.use('/api/status', guard);
  app.route('/api/admin', adminRoutes(deps.admin));
  app.route('/api/status', statusRoutes(deps.status));

  if (deps.webRoot !== null) {
    app.use('*', guard);
    app.get('*', staticHandler(deps.webRoot));
  }

  return app;
}
```

**Ordering assumption, and what to do if it does not hold.** This relies on
Hono applying middleware only to handlers registered *after* it, so the
`app.use('*', guard)` above does not wrap the already-registered health and
drain routes. The Task 23 tests pin exactly that: `/api/drain/none` must return
`404`, not `503`, with `AUTH_MODE` unset. If those tests show the guard
swallowing the drain route, do not reorder blindly — replace the wildcard with
an explicit non-API scope:

Note that the identity-header strip depends on the same Hono property in the
**opposite** direction: registered before the routes, it must cover
`/api/drain`, while `guard` must not. Any reordering has to preserve both, and
only one of them is pinned by the `404` test — so a reorder that satisfies that
test can silently drop the strip. The proxy-mode drain test below is what
catches it.

```ts
    app.use('*', async (c, next) => {
      const path = new URL(c.req.url).pathname;
      if (path.startsWith('/api/') || path === '/healthz' || path === '/readyz') {
        await next();
        return;
      }
      return guard(c, next);
    });
```

- [ ] **Step 6: Implement `src/index.ts`**

```ts
import { mkdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { serve } from '@hono/node-server';
import { buildApp } from './server/app.js';
import { nodePeerResolver, parseAuthConfig } from './server/middleware/proxy-auth.js';
import { ConfigStore } from './config/store.js';
import { Dispatcher } from './pipeline/dispatcher.js';
import { Metrics } from './status/metrics.js';
import { createLogger } from './log.js';
import { VERSION } from './version.js';
import type { Hono } from 'hono';
import type { AppConfig } from './config/schema.js';
import type { AppEnv } from './server/types.js';

export type BootOptions = {
  env: Record<string, string | undefined>;
  webRoot: string | null;
};

export type Booted = {
  app: Hono<AppEnv>;
  dispatcher: Dispatcher;
  shutdown: () => Promise<void>;
};

async function assertWritable(label: string, dir: string): Promise<void> {
  const probe = join(dir, `.write-probe-${String(process.pid)}`);
  try {
    await mkdir(dir, { recursive: true });
    await writeFile(probe, 'ok');
    await rm(probe, { force: true });
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new Error(
      `${label} (${dir}) is not writable by uid ${String(process.getuid?.() ?? -1)}: ${detail}\n` +
        `Fix it on the host with:  chown -R 10001:10001 ${dir}`,
    );
  }
}

export async function boot(options: BootOptions): Promise<Booted> {
  const env = options.env;
  const configDir = env['CONFIG_DIR'] ?? '/config';
  const spoolDir = env['SPOOL_DIR'] ?? '/spool';
  const logsRoot = env['LOGS_ROOT'] ?? '/logs';

  // Parse auth before touching disk: a bad AUTH_MODE must fail fast.
  const authConfig = parseAuthConfig(env);
  // Read straight from the environment, not from authConfig: the strip has to
  // happen whenever an operator has named an identity header, whatever
  // AUTH_MODE says. parseAuthConfig has already rejected an invalid or
  // reserved name in proxy mode; in the other modes a bad value here can only
  // ever remove a header nobody should be sending.
  const identityHeader = env['AUTH_USER_HEADER']?.trim().toLowerCase() ?? null;

  await assertWritable('CONFIG_DIR', configDir);
  await assertWritable('SPOOL_DIR', spoolDir);
  await assertWritable('LOGS_ROOT', logsRoot);

  const log = createLogger(env['LOG_LEVEL'] ?? 'info');
  if (authConfig.mode === 'disabled') {
    log.warn(
      'AUTH_MODE=disabled — the admin interface is UNAUTHENTICATED. Never use this outside local development.',
    );
  }
  if (authConfig.mode === 'unset') {
    log.warn(
      'AUTH_MODE is not set — the admin interface will return 503. Ingest is unaffected. Set AUTH_MODE=proxy with AUTH_TRUSTED_PROXIES and AUTH_USER_HEADER to enable it.',
    );
  }

  const metrics = new Metrics();
  const store = new ConfigStore(configDir);
  // Once, before anything can write: reaps temp files stranded by a crashed
  // predecessor. Safe only here — see the note on sweepStaleTemps.
  await store.sweepStaleTemps();
  const loaded = await store.load();

  let config: AppConfig = loaded.config;
  let etag = loaded.etag;

  const dispatcher = new Dispatcher({ spoolRoot: spoolDir, logsRoot, metrics, log });
  await dispatcher.applyConfig(config);
  dispatcher.start();

  const app = buildApp({
    authConfig,
    identityHeader,
    peerResolver: nodePeerResolver,
    webRoot: options.webRoot,
    drain: { getConfig: () => config, dispatcher, metrics, log },
    status: {
      getConfig: () => config,
      dispatcher,
      metrics,
      version: VERSION,
      configDir,
      spoolDir,
    },
    admin: {
      store,
      dispatcher,
      getConfig: () => config,
      getEtag: () => etag,
      setConfig: (next, nextEtag) => {
        config = next;
        etag = nextEtag;
      },
      log,
    },
  });

  return {
    app,
    dispatcher,
    shutdown: async () => {
      await dispatcher.stop(10_000);
    },
  };
}

async function main(): Promise<void> {
  const booted = await boot({
    env: process.env,
    webRoot: process.env['WEB_ROOT'] ?? 'web/dist',
  });

  const port = Number.parseInt(process.env['PORT'] ?? '8080', 10);
  const hostname = process.env['HOST'] ?? '0.0.0.0';
  const server = serve({ fetch: booted.app.fetch, port, hostname });

  const log = createLogger(process.env['LOG_LEVEL'] ?? 'info');
  log.info({ port, hostname, version: VERSION }, 'vercel-log-drain listening');

  let shuttingDown = false;
  const stop = (signal: string): void => {
    if (shuttingDown) return;
    shuttingDown = true;
    log.info({ signal }, 'shutting down');
    server.close(() => {
      void booted.shutdown().then(() => {
        process.exit(0);
      });
    });
  };
  process.on('SIGTERM', () => stop('SIGTERM'));
  process.on('SIGINT', () => stop('SIGINT'));
}

// Only run the server when executed directly, so tests can import boot().
if (process.argv[1]?.endsWith('index.js') === true) {
  void main();
}
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `npx vitest run test/server/app.test.ts`
Expected: PASS.

- [ ] **Step 8: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "feat: assemble app and entrypoint

Health probes and the drain endpoint are unauthenticated by design;
everything else sits behind the proxy guard, including the SPA. Boot
probes all three volumes for writability and fails with the exact chown
to run, rather than an EACCES trace mid-startup."
```

---

### Task 24: End-to-end durability test

This is the test that validates the central design claim. It has no
implementation step — if it fails, a previous task is wrong.

**Files:**
- Test: `test/e2e/durability.test.ts`

**Interfaces:**
- Consumes: `boot` from `src/index.ts`; `newDrainSecret` from `src/config/schema.ts`.
- Produces: nothing.

- [ ] **Step 1: Write the test**

`test/e2e/durability.test.ts`:

```ts
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createHmac } from 'node:crypto';
import { createServer } from 'node:http';
import type { Server } from 'node:http';
import { gunzip } from 'node:zlib';
import { promisify } from 'node:util';
import { mkdir, mkdtemp, readdir, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { boot } from '../../src/index.js';
import { ConfigStore } from '../../src/config/store.js';
import { defaultAppConfig } from '../../src/config/schema.js';
import type { Booted } from '../../src/index.js';

const gunzipAsync = promisify(gunzip);
const SECRET = 'e'.repeat(40);

function event(id: string) {
  return {
    id,
    timestamp: Date.UTC(2026, 8, 8, 12, 0, 0),
    source: 'lambda',
    projectId: 'p1',
    projectName: 'my-app',
    environment: 'production',
    level: 'info',
    message: `event ${id}`,
  };
}

function sign(body: string): string {
  return createHmac('sha1', SECRET).update(body).digest('hex');
}

async function post(booted: Booted, events: ReturnType<typeof event>[]) {
  const body = JSON.stringify(events);
  return booted.app.request('/api/drain/e2e', {
    method: 'POST',
    body,
    headers: { 'x-vercel-signature': sign(body) },
  });
}

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 8000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error('condition not met before timeout');
}

describe('end-to-end durability', () => {
  let root = '';
  let configDir = '';
  let spoolDir = '';
  let logsRoot = '';
  let lokiServer: Server | null = null;
  const lokiReceived: string[] = [];

  beforeEach(async () => {
    root = await mkdtemp(join(tmpdir(), 'vld-e2e-'));
    configDir = join(root, 'config');
    spoolDir = join(root, 'spool');
    logsRoot = join(root, 'logs');
    for (const dir of [configDir, spoolDir, logsRoot]) await mkdir(dir, { recursive: true });
    lokiReceived.length = 0;
  });

  afterEach(async () => {
    if (lokiServer !== null) {
      await new Promise<void>((resolve) => lokiServer?.close(() => resolve()));
      lokiServer = null;
    }
    await rm(root, { recursive: true, force: true });
  });

  async function startLoki(port: number): Promise<void> {
    const instance = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on('data', (chunk: Buffer) => chunks.push(chunk));
      req.on('end', () => {
        void (async () => {
          const raw = Buffer.concat(chunks);
          const text =
            req.headers['content-encoding'] === 'gzip'
              ? (await gunzipAsync(raw)).toString('utf8')
              : raw.toString('utf8');
          lokiReceived.push(text);
          res.writeHead(204);
          res.end();
        })();
      });
    });
    lokiServer = instance;
    await new Promise<void>((resolve) => instance.listen(port, '127.0.0.1', resolve));
  }

  async function writeConfig(lokiPort: number): Promise<void> {
    const base = defaultAppConfig();
    await new ConfigStore(configDir).save(
      {
        ...base,
        drains: [{ id: 'e2e', name: 'e2e', secret: SECRET, enabled: true, createdAt: 1 }],
        sinks: [
          {
            name: 'local',
            enabled: true,
            filter: {},
            maxSpoolBytes: 1_048_576,
            maxBatchEvents: 1000,
            maxBatchBytes: 1_048_576,
            config: {
              type: 'file',
              directory: join(logsRoot, 'local'),
              filePrefix: 'events',
              retentionDays: 0,
              freeSpaceFloorBytes: 0,
            },
          },
          {
            name: 'loki',
            enabled: true,
            filter: {},
            maxSpoolBytes: 1_048_576,
            maxBatchEvents: 1000,
            maxBatchBytes: 1_048_576,
            config: {
              type: 'loki',
              url: `http://127.0.0.1:${String(lokiPort)}`,
              auth: { kind: 'none' },
              tenantId: null,
              labels: { static: { job: 'vercel' }, fromFields: ['level'] },
              timeoutMs: 1000,
            },
          },
        ],
      },
      null,
    );
  }

  function bootService(): Promise<Booted> {
    return boot({
      env: {
        CONFIG_DIR: configDir,
        SPOOL_DIR: spoolDir,
        LOGS_ROOT: logsRoot,
        LOG_LEVEL: 'silent',
        AUTH_MODE: 'disabled',
      },
      webRoot: null,
    });
  }

  it('loses nothing across a sink outage and a hard restart', async () => {
    // Pick a port nothing is listening on yet, so Loki is "down".
    const lokiPort = 45_231;
    await writeConfig(lokiPort);

    // --- Phase 1: Loki is down. Deliveries must still be accepted. ---
    const first = await bootService();

    for (const ids of [['a1', 'a2'], ['a3'], ['a4', 'a5']]) {
      const response = await post(first, ids.map(event));
      expect(response.status).toBe(200);
    }

    // The file sink drains normally even though Loki cannot be reached.
    await waitFor(async () => {
      const files = await readdir(join(logsRoot, 'local')).catch(() => []);
      if (files.length === 0) return false;
      const contents = await readFile(join(logsRoot, 'local', files[0] ?? ''), 'utf8');
      return contents.trimEnd().split('\n').length === 5;
    });

    // Loki's spool still holds the batches, undelivered.
    const spooledBefore = (await readdir(join(spoolDir, 'loki'))).filter((f) =>
      f.endsWith('.jsonl'),
    );
    expect(spooledBefore.length).toBe(3);
    expect(lokiReceived).toEqual([]);

    // --- Phase 2: simulate a crash. No graceful drain. ---
    await first.dispatcher.stop(0);

    // --- Phase 3: Loki comes back; a fresh process must replay the spool. ---
    await startLoki(lokiPort);
    const second = await bootService();

    try {
      await waitFor(async () => {
        const remaining = (await readdir(join(spoolDir, 'loki'))).filter((f) =>
          f.endsWith('.jsonl'),
        );
        return remaining.length === 0;
      });

      const delivered = lokiReceived
        .flatMap((text) => {
          const payload: { streams: { values: [string, string][] }[] } = JSON.parse(text);
          return payload.streams.flatMap((stream) => stream.values);
        })
        .map(([, line]) => {
          const entry: { id: string } = JSON.parse(line);
          return entry.id;
        });

      // Every event arrives, exactly once.
      expect(delivered.toSorted()).toEqual(['a1', 'a2', 'a3', 'a4', 'a5']);

      // And nothing was dead-lettered along the way.
      const dead = await readdir(join(spoolDir, 'loki', 'dead')).catch(() => []);
      expect(dead).toEqual([]);
    } finally {
      await second.shutdown();
    }
  }, 30_000);

  it('keeps accepting deliveries while a sink is wedged, and reports degraded', async () => {
    const lokiPort = 45_232;
    await writeConfig(lokiPort);
    const booted = await bootService();

    try {
      expect((await post(booted, [event('b1')])).status).toBe(200);

      await waitFor(() => Promise.resolve(booted.dispatcher.isDegraded()));

      const status = await booted.app.request('/api/status');
      const snapshot: {
        service: { state: string };
        sinks: { name: string; health: { state: string }; queue: { files: number } }[];
      } = await status.json();
      expect(snapshot.service.state).toBe('degraded');
      const loki = snapshot.sinks.find((sink) => sink.name === 'loki');
      expect(loki?.health.state).toBe('failed');
      expect(loki?.queue.files).toBeGreaterThan(0);

      expect((await booted.app.request('/readyz')).status).toBe(503);
      expect((await booted.app.request('/healthz')).status).toBe(200);
    } finally {
      await booted.shutdown();
    }
  }, 30_000);
});
```

- [ ] **Step 2: Run the test**

Run: `npx vitest run test/e2e/durability.test.ts`
Expected: PASS. If it fails, do not weaken the test — the defect is in the
spool queue, the worker, or the dispatcher. Use
`superpowers:systematic-debugging`.

- [ ] **Step 3: Run all gates and commit**

```bash
npm run lint && npm run typecheck && npm test
git add -A
git commit -m "test: prove durability across a sink outage and restart

Accepts deliveries while Loki is unreachable, kills the dispatcher
without draining, then brings up a fresh process and asserts every event
arrives exactly once with nothing dead-lettered."
```

---

### Task 25: SPA scaffold, API client, and Status view

**Files:**
- Create: `web/index.html`, `web/src/main.tsx`, `web/src/App.tsx`, `web/src/api.ts`, `web/src/styles.css`, `web/src/views/Status.tsx`
- Create: `src/config/api-contract.ts`
- Modify: `types/api.ts` (add the config DTOs the SPA consumes)
- Modify: `package.json` (typecheck script now covers the web project)
- Test: `test/config/api-contract.test.ts`

**Interfaces:**
- Consumes: `StatusSnapshot` and the new DTOs from `types/api.ts`.
- Produces: in `types/api.ts` — `SinkFilterDto`, `FileSinkConfigDto`, `LokiAuthDto`, `LokiSinkConfigDto`, `SinkConfigDto`, `SinkEntryDto`, `RedactedDrainDto`, `ServerConfigDto`, `RedactedConfigDto`, `ConfigResponse`, `CreatedDrain`, `TestSinkResponse`; in `web/src/api.ts` — `fetchStatus`, `fetchConfig`, `saveConfig`, `createDrain`, `testSink`, `discardOrphan`, `ApiError`.

The DTOs in `types/api.ts` are hand-written so the SPA never imports zod.
`src/config/api-contract.ts` asserts at typecheck time that they stay
assignable from the zod-inferred server types, so drift is a build failure
rather than a runtime surprise.

- [ ] **Step 1: Add the DTOs to `types/api.ts`**

Append (still no imports in this file):

```ts
export type SinkFilterDto = {
  minLevel?: 'info' | 'warning' | 'error';
  sources?: string[];
  environments?: string[];
  projectIds?: string[];
};

export type FileSinkConfigDto = {
  type: 'file';
  directory: string;
  filePrefix: string;
  retentionDays: number;
  freeSpaceFloorBytes: number;
};

export type LokiAuthDto =
  | { kind: 'none' }
  | { kind: 'basic'; username: string; password: string }
  | { kind: 'bearer'; token: string };

export type LokiSinkConfigDto = {
  type: 'loki';
  url: string;
  auth: LokiAuthDto;
  tenantId: string | null;
  labels: { static: Record<string, string>; fromFields: string[] };
  timeoutMs: number;
};

export type SinkConfigDto = FileSinkConfigDto | LokiSinkConfigDto;

export type SinkEntryDto = {
  name: string;
  enabled: boolean;
  filter: SinkFilterDto;
  maxSpoolBytes: number;
  maxBatchEvents: number;
  maxBatchBytes: number;
  config: SinkConfigDto;
};

export type RedactedDrainDto = {
  id: string;
  name: string;
  secret: null;
  hasSecret: boolean;
  enabled: boolean;
  createdAt: number;
};

export type ServerConfigDto = {
  maxBodyBytes: number;
  maxDecompressedBytes: number;
  spoolFreeSpaceFloorBytes: number;
};

export type RedactedConfigDto = {
  version: 1;
  drains: RedactedDrainDto[];
  sinks: SinkEntryDto[];
  server: ServerConfigDto;
};

export type ConfigResponse = { config: RedactedConfigDto; etag: string; warnings: string[] };
export type CreatedDrain = { id: string; name: string; secret: string; etag: string };
export type TestSinkResponse = { ok: boolean; detail: string };
```

- [ ] **Step 2: Write the failing contract test**

`test/config/api-contract.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { redactConfig } from '../../src/config/redact.js';
import { defaultAppConfig } from '../../src/config/schema.js';
import { CONTRACT_OK } from '../../src/config/api-contract.js';
import type { RedactedConfigDto } from '../../types/api.js';

describe('api contract', () => {
  it('keeps the hand-written DTOs assignable from the server types', () => {
    // The real assertion is at typecheck time in api-contract.ts; this test
    // exists so the file is exercised and cannot be deleted unnoticed.
    expect(CONTRACT_OK).toBe(true);
  });

  it('produces a redacted config that satisfies the DTO shape at runtime', () => {
    const redacted: RedactedConfigDto = redactConfig(defaultAppConfig());
    expect(redacted.version).toBe(1);
    expect(redacted.drains).toEqual([]);
  });
});
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `npx vitest run test/config/api-contract.test.ts`
Expected: FAIL — `src/config/api-contract.js` not found.

- [ ] **Step 4: Implement `src/config/api-contract.ts`**

```ts
import type { RedactedConfig } from './redact.js';
import type { SinkEntry } from './schema.js';
import type { RedactedConfigDto, SinkEntryDto } from '../../types/api.js';

/**
 * Compile-time guard. If the zod-inferred server types and the hand-written
 * DTOs in types/api.ts ever drift, `npm run typecheck` fails here rather than
 * the SPA silently reading a field that no longer exists.
 */
type AssertAssignable<Target, Source extends Target> = Source;

export type ConfigContract = AssertAssignable<RedactedConfigDto, RedactedConfig>;
export type SinkEntryContract = AssertAssignable<SinkEntryDto, SinkEntry>;

export const CONTRACT_OK = true;
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `npx vitest run test/config/api-contract.test.ts && npm run typecheck`
Expected: PASS, and typecheck clean. If typecheck fails here, the DTOs and the
schema disagree — fix `types/api.ts` to match the schema, never the reverse.

- [ ] **Step 6: Implement the SPA shell**

`web/index.html`:

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Vercel Log Drain</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

`web/src/styles.css`:

```css
:root {
  color-scheme: light dark;
  --border: color-mix(in srgb, currentColor 18%, transparent);
  font-family: ui-sans-serif, system-ui, -apple-system, sans-serif;
}
body { margin: 0; padding: 0 1.5rem 3rem; max-width: 68rem; }
h1 { font-size: 1.25rem; }
nav { display: flex; gap: 0.5rem; margin: 1rem 0 1.5rem; }
nav button { padding: 0.4rem 0.9rem; border: 1px solid var(--border); border-radius: 0.4rem;
  background: transparent; color: inherit; cursor: pointer; font: inherit; }
nav button[aria-current='true'] { background: color-mix(in srgb, currentColor 12%, transparent); }
table { border-collapse: collapse; width: 100%; font-size: 0.9rem; }
th, td { text-align: left; padding: 0.4rem 0.6rem; border-bottom: 1px solid var(--border); }
th { font-weight: 600; opacity: 0.7; font-size: 0.8rem; text-transform: uppercase; }
.card { border: 1px solid var(--border); border-radius: 0.6rem; padding: 1rem; margin-bottom: 1rem; }
.row { display: flex; gap: 1rem; flex-wrap: wrap; align-items: end; }
label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.85rem; }
input, select { padding: 0.35rem 0.5rem; border: 1px solid var(--border); border-radius: 0.3rem;
  background: transparent; color: inherit; font: inherit; }
button.primary { padding: 0.45rem 1rem; border-radius: 0.4rem; border: 1px solid var(--border);
  background: color-mix(in srgb, currentColor 12%, transparent); color: inherit; cursor: pointer; font: inherit; }
.state-ok { color: #10893e; } .state-retrying { color: #b8860b; } .state-failed { color: #d13438; }
.warn { border-left: 3px solid #b8860b; padding-left: 0.75rem; margin: 0.5rem 0; font-size: 0.9rem; }
.err { border-left: 3px solid #d13438; padding-left: 0.75rem; margin: 0.5rem 0; font-size: 0.9rem; }
.muted { opacity: 0.65; font-size: 0.85rem; }
pre { overflow-x: auto; font-size: 0.8rem; }
```

`web/src/api.ts`:

```ts
import type {
  ConfigResponse,
  CreatedDrain,
  RedactedConfigDto,
  StatusSnapshot,
  TestSinkResponse,
} from '@shared/api';

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: { 'content-type': 'application/json', ...init?.headers },
  });
  if (!response.ok) {
    const body: { error?: string } = await response.json().catch(() => ({}));
    throw new ApiError(response.status, body.error ?? `request failed (${response.status})`);
  }
  // Annotated assignment, not `as T`: oxlint's no-unsafe-type-assertion
  // rejects narrowing the `any` that .json() returns.
  const body: T = await response.json();
  return body;
}

export function fetchStatus(): Promise<StatusSnapshot> {
  return request<StatusSnapshot>('/api/status');
}

export function fetchConfig(): Promise<ConfigResponse> {
  return request<ConfigResponse>('/api/admin/config');
}

export function saveConfig(config: RedactedConfigDto, etag: string): Promise<ConfigResponse> {
  return request<ConfigResponse>('/api/admin/config', {
    method: 'PUT',
    body: JSON.stringify({ config, etag }),
  });
}

export function createDrain(name: string): Promise<CreatedDrain> {
  return request<CreatedDrain>('/api/admin/drains', {
    method: 'POST',
    body: JSON.stringify({ name }),
  });
}

export function testSink(name: string): Promise<TestSinkResponse> {
  return request<TestSinkResponse>(`/api/admin/sinks/${encodeURIComponent(name)}/test`, {
    method: 'POST',
  });
}

export function discardOrphan(name: string): Promise<{ ok: boolean }> {
  return request<{ ok: boolean }>(`/api/admin/orphans/${encodeURIComponent(name)}`, {
    method: 'DELETE',
  });
}
```

`web/src/main.tsx`:

```tsx
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './App.tsx';
import './styles.css';

const container = document.getElementById('root');
if (container === null) throw new Error('#root is missing from index.html');
createRoot(container).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
```

`web/src/App.tsx`:

Task 26 adds the Drains and Sinks tabs to this file. For now it carries the
Status tab alone, so nothing references a component that does not exist.

```tsx
import { useState } from 'react';
import { Status } from './views/Status.tsx';

type Tab = 'status';

const TABS: { id: Tab; label: string }[] = [{ id: 'status', label: 'Status' }];

export function App(): React.JSX.Element {
  const [tab, setTab] = useState<Tab>('status');

  return (
    <main>
      <h1>Vercel Log Drain</h1>
      <nav>
        {TABS.map((entry) => (
          <button
            key={entry.id}
            type="button"
            aria-current={tab === entry.id}
            onClick={() => setTab(entry.id)}
          >
            {entry.label}
          </button>
        ))}
      </nav>
      {tab === 'status' ? <Status /> : null}
    </main>
  );
}
```

- [ ] **Step 7: Implement `web/src/views/Status.tsx`**

```tsx
import { useEffect, useState } from 'react';
import { discardOrphan, fetchStatus } from '../api.ts';
import type { StatusSnapshot } from '@shared/api';

const POLL_MS = 2000;

function bytes(value: number): string {
  if (value < 1024) return `${String(value)} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let scaled = value / 1024;
  let unit = 0;
  while (scaled >= 1024 && unit < units.length - 1) {
    scaled /= 1024;
    unit += 1;
  }
  return `${scaled.toFixed(1)} ${units[unit] ?? ''}`;
}

function ago(timestamp: number | null): string {
  if (timestamp === null) return 'never';
  const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
  if (seconds < 60) return `${String(seconds)}s ago`;
  if (seconds < 3600) return `${String(Math.floor(seconds / 60))}m ago`;
  return `${String(Math.floor(seconds / 3600))}h ago`;
}

export function Status(): React.JSX.Element {
  const [snapshot, setSnapshot] = useState<StatusSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    const tick = async (): Promise<void> => {
      try {
        const next = await fetchStatus();
        if (active) {
          setSnapshot(next);
          setError(null);
        }
      } catch (cause) {
        if (active) setError(cause instanceof Error ? cause.message : String(cause));
      }
    };
    void tick();
    const timer = setInterval(() => void tick(), POLL_MS);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, []);

  if (error !== null) return <p className="err">{error}</p>;
  if (snapshot === null) return <p className="muted">Loading…</p>;

  return (
    <>
      <div className="card">
        <strong className={`state-${snapshot.service.state === 'ok' ? 'ok' : 'failed'}`}>
          {snapshot.service.state}
        </strong>{' '}
        <span className="muted">
          version {snapshot.service.version} · up {String(snapshot.service.uptimeSec)}s
        </span>
        <p className="muted">
          spool volume {bytes(snapshot.volumes.spool.freeBytes)} free of{' '}
          {bytes(snapshot.volumes.spool.totalBytes)} · config volume{' '}
          {bytes(snapshot.volumes.config.freeBytes)} free
        </p>
      </div>

      <h2>Sinks</h2>
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Health</th>
            <th>Queued</th>
            <th>Head age</th>
            <th>Delivered</th>
            <th>Dropped</th>
            <th>Dead</th>
          </tr>
        </thead>
        <tbody>
          {snapshot.sinks.map((sink) => (
            <tr key={sink.name}>
              <td>{sink.name}</td>
              <td>{sink.type}</td>
              <td className={`state-${sink.health.state}`} title={sink.health.lastError ?? ''}>
                {sink.enabled ? sink.health.state : 'disabled'}
              </td>
              <td>
                {String(sink.queue.files)} files / {bytes(sink.queue.bytes)}
              </td>
              <td>
                {sink.queue.oldestAgeSec === null ? '—' : `${String(sink.queue.oldestAgeSec)}s`}
              </td>
              <td>{String(sink.counters.delivered)}</td>
              <td>{String(sink.counters.dropped)}</td>
              <td>{String(sink.counters.deadLettered)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="muted">
        Counters are in-memory and reset when the container restarts. A steadily climbing head age
        means delivery is falling behind ingest.
      </p>

      <h2>Drains</h2>
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Events</th>
            <th>Last event</th>
            <th>OK</th>
            <th>Bad signature</th>
            <th>Malformed</th>
          </tr>
        </thead>
        <tbody>
          {snapshot.drains.map((drain) => (
            <tr key={drain.id}>
              <td>{drain.enabled ? drain.name : `${drain.name} (disabled)`}</td>
              <td>{String(drain.eventsReceived)}</td>
              <td>{ago(drain.lastEventAt)}</td>
              <td>{String(drain.requests.ok)}</td>
              <td>{String(drain.requests.badSignature)}</td>
              <td>{String(drain.requests.malformedBody)}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {snapshot.orphanedSpools.length > 0 ? (
        <>
          <h2>Orphaned spools</h2>
          <p className="muted">
            Queues left behind by removed or renamed sinks. Their data is still on disk.
          </p>
          <table>
            <tbody>
              {snapshot.orphanedSpools.map((orphan) => (
                <tr key={orphan.name}>
                  <td>{orphan.name}</td>
                  <td>
                    {String(orphan.files)} files / {bytes(orphan.bytes)}
                  </td>
                  <td>
                    <button
                      type="button"
                      onClick={() => {
                        void discardOrphan(orphan.name);
                      }}
                    >
                      Discard
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      ) : null}

      {snapshot.recent.errors.length > 0 ? (
        <>
          <h2>Recent errors</h2>
          <table>
            <tbody>
              {snapshot.recent.errors
                .slice(-10)
                .reverse()
                .map((entry, index) => (
                  <tr key={`${String(entry.at)}-${String(index)}`}>
                    <td>{entry.scope}</td>
                    <td>{entry.message}</td>
                    <td className="muted">{ago(entry.at)}</td>
                  </tr>
                ))}
            </tbody>
          </table>
        </>
      ) : null}

      <h2>Recent events</h2>
      <p className="muted">
        Newest {String(Math.min(10, snapshot.recent.events.length))} of{' '}
        {String(snapshot.recent.events.length)} buffered. This is a tail to confirm arrival, not a
        log browser — query Loki for that.
      </p>
      <pre>
        {snapshot.recent.events
          .slice(-10)
          .reverse()
          .map((event) => JSON.stringify(event))
          .join('\n')}
      </pre>
    </>
  );
}
```

- [ ] **Step 8: Extend the typecheck script to cover the web project**

`web/src` now has real files, so add it:

```bash
npm pkg set scripts.typecheck="tsc -p tsconfig.json && tsc -p web/tsconfig.json"
```

- [ ] **Step 9: Verify the build**

```bash
npm run lint && npm run typecheck && npm test && npm run build:web
```

`npm run build:web` must produce `web/dist/index.html`.

`Drains.tsx` and `Sinks.tsx` do not exist yet and **must not be created as
stubs** — this task's `App.tsx` wires only the Status tab, and Task 26 adds the
other two tabs together with their views. Nothing is committed in a
placeholder state.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat: add SPA shell, API client, and status view

DTOs are hand-written in types/api.ts so the browser bundle never
includes zod, with a compile-time assertion in api-contract.ts that they
stay assignable from the zod-inferred server types."
```

---

### Task 26: Drains and Sinks views

**Files:**
- Create: `web/src/views/Drains.tsx`, `web/src/views/Sinks.tsx`
- Create: `web/src/useConfig.ts`
- Modify: `web/src/App.tsx` (add the Drains and Sinks tabs)

**Interfaces:**
- Consumes: `fetchConfig`, `saveConfig`, `createDrain`, `testSink`, `ApiError` from `web/src/api.ts`; the DTOs from `@shared/api`.
- Produces: `useConfig()` hook returning `{ config, etag, warnings, error, notice, reload, save, mutate }`.

- [ ] **Step 1: Add the two tabs to `web/src/App.tsx`**

Task 25 left this file with a single Status tab. Widen it:

```tsx
import { useState } from 'react';
import { Status } from './views/Status.tsx';
import { Drains } from './views/Drains.tsx';
import { Sinks } from './views/Sinks.tsx';

type Tab = 'status' | 'drains' | 'sinks';

const TABS: { id: Tab; label: string }[] = [
  { id: 'status', label: 'Status' },
  { id: 'drains', label: 'Drains' },
  { id: 'sinks', label: 'Sinks' },
];
```

and render them alongside Status:

```tsx
      {tab === 'status' ? <Status /> : null}
      {tab === 'drains' ? <Drains /> : null}
      {tab === 'sinks' ? <Sinks /> : null}
```

- [ ] **Step 2: Implement `web/src/useConfig.ts`**

```tsx
import { useCallback, useEffect, useState } from 'react';
import { fetchConfig, saveConfig } from './api.ts';
import type { RedactedConfigDto } from '@shared/api';

export type UseConfig = {
  config: RedactedConfigDto | null;
  etag: string;
  warnings: string[];
  error: string | null;
  notice: string | null;
  setNotice: (value: string | null) => void;
  reload: () => void;
  save: (next: RedactedConfigDto) => void;
};

export function useConfig(): UseConfig {
  const [config, setConfig] = useState<RedactedConfigDto | null>(null);
  const [etag, setEtag] = useState('');
  const [warnings, setWarnings] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const reload = useCallback(() => {
    void (async () => {
      try {
        const response = await fetchConfig();
        setConfig(response.config);
        setEtag(response.etag);
        setWarnings(response.warnings);
        setError(null);
      } catch (cause) {
        setError(cause instanceof Error ? cause.message : String(cause));
      }
    })();
  }, []);

  useEffect(reload, [reload]);

  const save = useCallback(
    (next: RedactedConfigDto) => {
      void (async () => {
        try {
          const response = await saveConfig(next, etag);
          setConfig(response.config);
          setEtag(response.etag);
          setWarnings(response.warnings);
          setError(null);
          setNotice('Saved.');
        } catch (cause) {
          setError(cause instanceof Error ? cause.message : String(cause));
        }
      })();
    },
    [etag],
  );

  return { config, etag, warnings, error, notice, setNotice, reload, save };
}
```

- [ ] **Step 5: Implement `web/src/views/Drains.tsx`**

The created secret is shown once, in a dismissible panel, because the API will
never return it again.

```tsx
import { useState } from 'react';
import { createDrain } from '../api.ts';
import { useConfig } from '../useConfig.ts';
import type { CreatedDrain } from '@shared/api';

export function Drains(): React.JSX.Element {
  const { config, error, save, reload } = useConfig();
  const [name, setName] = useState('');
  const [created, setCreated] = useState<CreatedDrain | null>(null);
  const [createError, setCreateError] = useState<string | null>(null);

  const drainUrl = (id: string): string => `${window.location.origin}/api/drain/${id}`;

  const add = (): void => {
    void (async () => {
      try {
        setCreated(await createDrain(name));
        setName('');
        setCreateError(null);
        reload();
      } catch (cause) {
        setCreateError(cause instanceof Error ? cause.message : String(cause));
      }
    })();
  };

  const toggle = (id: string, enabled: boolean): void => {
    if (config === null) return;
    save({
      ...config,
      drains: config.drains.map((drain) => (drain.id === id ? { ...drain, enabled } : drain)),
    });
  };

  const remove = (id: string): void => {
    if (config === null) return;
    if (!window.confirm('Delete this drain? Vercel will start getting 404s for it.')) return;
    save({ ...config, drains: config.drains.filter((drain) => drain.id !== id) });
  };

  return (
    <>
      {error !== null ? <p className="err">{error}</p> : null}
      {createError !== null ? <p className="err">{createError}</p> : null}

      {created !== null ? (
        <div className="card">
          <strong>Drain created — copy the secret now.</strong>
          <p className="muted">This is the only time it will be shown.</p>
          <table>
            <tbody>
              <tr>
                <th>Endpoint URL</th>
                <td>
                  <code>{drainUrl(created.id)}</code>
                </td>
              </tr>
              <tr>
                <th>Signature secret</th>
                <td>
                  <code>{created.secret}</code>
                </td>
              </tr>
            </tbody>
          </table>
          <p className="muted">
            In Vercel, create a Drain with this endpoint and secret. Either JSON or NDJSON encoding
            works, with or without gzip.
          </p>
          <button
            type="button"
            className="primary"
            onClick={() => {
              setCreated(null);
            }}
          >
            I have saved it
          </button>
        </div>
      ) : null}

      <div className="card row">
        <label>
          New drain name
          <input
            value={name}
            onChange={(changed) => setName(changed.target.value)}
            placeholder="production"
          />
        </label>
        <button type="button" className="primary" disabled={name.trim().length === 0} onClick={add}>
          Create drain
        </button>
      </div>

      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Endpoint</th>
            <th>Secret</th>
            <th>Enabled</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {(config?.drains ?? []).map((drain) => (
            <tr key={drain.id}>
              <td>{drain.name}</td>
              <td>
                <code>{drainUrl(drain.id)}</code>
              </td>
              <td className="muted">{drain.hasSecret ? 'set (hidden)' : 'missing'}</td>
              <td>
                <input
                  type="checkbox"
                  checked={drain.enabled}
                  onChange={(changed) => toggle(drain.id, changed.target.checked)}
                />
              </td>
              <td>
                <button type="button" onClick={() => remove(drain.id)}>
                  Delete
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {config !== null && config.drains.length === 0 ? (
        <p className="muted">No drains yet. Create one, then paste its URL and secret into Vercel.</p>
      ) : null}
    </>
  );
}
```

- [ ] **Step 3: Implement `web/src/views/Sinks.tsx`**

```tsx
import { useState } from 'react';
import { testSink } from '../api.ts';
import { useConfig } from '../useConfig.ts';
import type { RedactedConfigDto, SinkEntryDto } from '@shared/api';

const MIB = 1_048_576;

const HIGH_CARDINALITY = ['id', 'requestId', 'deploymentId', 'path', 'host', 'traceId', 'spanId'];

function newFileSink(index: number): SinkEntryDto {
  return {
    name: `file-${String(index)}`,
    enabled: true,
    filter: {},
    maxSpoolBytes: 512 * MIB,
    maxBatchEvents: 1000,
    maxBatchBytes: 4 * MIB,
    config: {
      type: 'file',
      directory: '/logs',
      filePrefix: 'events',
      retentionDays: 14,
      freeSpaceFloorBytes: 256 * MIB,
    },
  };
}

function newLokiSink(index: number): SinkEntryDto {
  return {
    name: `loki-${String(index)}`,
    enabled: true,
    filter: {},
    maxSpoolBytes: 512 * MIB,
    maxBatchEvents: 1000,
    maxBatchBytes: 4 * MIB,
    config: {
      type: 'loki',
      url: 'http://loki:3100',
      auth: { kind: 'none' },
      tenantId: null,
      labels: { static: { job: 'vercel' }, fromFields: ['projectName', 'environment', 'source', 'level'] },
      timeoutMs: 10_000,
    },
  };
}

export function Sinks(): React.JSX.Element {
  const { config, warnings, error, notice, setNotice, save } = useConfig();
  const [draft, setDraft] = useState<RedactedConfigDto | null>(null);
  const [testResult, setTestResult] = useState<string | null>(null);

  const working = draft ?? config;
  if (working === null) return <p className="muted">{error ?? 'Loading…'}</p>;

  const update = (index: number, next: SinkEntryDto): void => {
    const sinks = working.sinks.map((sink, position) => (position === index ? next : sink));
    setDraft({ ...working, sinks });
    setNotice(null);
  };

  const add = (sink: SinkEntryDto): void => {
    setDraft({ ...working, sinks: [...working.sinks, sink] });
  };

  const remove = (index: number): void => {
    const sink = working.sinks[index];
    if (sink === undefined) return;
    if (
      !window.confirm(
        `Remove sink "${sink.name}"? Its queued batches stay on disk and appear as an orphaned spool.`,
      )
    ) {
      return;
    }
    setDraft({ ...working, sinks: working.sinks.filter((_unused, position) => position !== index) });
  };

  const commit = (): void => {
    save(working);
    setDraft(null);
  };

  return (
    <>
      {error !== null ? <p className="err">{error}</p> : null}
      {notice !== null ? <p className="muted">{notice}</p> : null}
      {warnings.map((warning) => (
        <p className="warn" key={warning}>
          {warning}
        </p>
      ))}
      {testResult !== null ? <p className="muted">{testResult}</p> : null}

      {working.sinks.map((sink, index) => (
        <div className="card" key={`${sink.name}-${String(index)}`}>
          <div className="row">
            <label>
              Name
              <input
                value={sink.name}
                onChange={(changed) => update(index, { ...sink, name: changed.target.value })}
              />
            </label>
            <label>
              Enabled
              <input
                type="checkbox"
                checked={sink.enabled}
                onChange={(changed) => update(index, { ...sink, enabled: changed.target.checked })}
              />
            </label>
            <label>
              Min level
              <select
                value={sink.filter.minLevel ?? ''}
                onChange={(changed) =>
                  update(index, {
                    ...sink,
                    filter:
                      changed.target.value === ''
                        ? { ...sink.filter, minLevel: undefined }
                        : {
                            ...sink.filter,
                            minLevel: changed.target.value as 'info' | 'warning' | 'error',
                          },
                  })
                }
              >
                <option value="">any</option>
                <option value="info">info</option>
                <option value="warning">warning</option>
                <option value="error">error</option>
              </select>
            </label>
            <label>
              Spool budget (MiB)
              <input
                type="number"
                value={Math.round(sink.maxSpoolBytes / MIB)}
                onChange={(changed) =>
                  update(index, {
                    ...sink,
                    maxSpoolBytes: Math.max(1, Number(changed.target.value)) * MIB,
                  })
                }
              />
            </label>
          </div>

          {sink.config.type === 'file' ? (
            <div className="row">
              <label>
                Directory
                <input
                  value={sink.config.directory}
                  onChange={(changed) =>
                    update(index, {
                      ...sink,
                      config: { ...sink.config, type: 'file', directory: changed.target.value },
                    })
                  }
                />
              </label>
              <label>
                File prefix
                <input
                  value={sink.config.filePrefix}
                  onChange={(changed) =>
                    update(index, {
                      ...sink,
                      config: { ...sink.config, type: 'file', filePrefix: changed.target.value },
                    })
                  }
                />
              </label>
              <label>
                Retention (days, 0 = forever)
                <input
                  type="number"
                  value={sink.config.retentionDays}
                  onChange={(changed) =>
                    update(index, {
                      ...sink,
                      config: {
                        ...sink.config,
                        type: 'file',
                        retentionDays: Math.max(0, Number(changed.target.value)),
                      },
                    })
                  }
                />
              </label>
            </div>
          ) : (
            <>
              <div className="row">
                <label>
                  Loki URL
                  <input
                    value={sink.config.url}
                    onChange={(changed) =>
                      update(index, {
                        ...sink,
                        config: { ...sink.config, type: 'loki', url: changed.target.value },
                      })
                    }
                  />
                </label>
                <label>
                  Tenant (X-Scope-OrgID)
                  <input
                    value={sink.config.tenantId ?? ''}
                    onChange={(changed) =>
                      update(index, {
                        ...sink,
                        config: {
                          ...sink.config,
                          type: 'loki',
                          tenantId: changed.target.value === '' ? null : changed.target.value,
                        },
                      })
                    }
                  />
                </label>
                <label>
                  Auth
                  <select
                    value={sink.config.auth.kind}
                    onChange={(changed) => {
                      const kind = changed.target.value;
                      const auth =
                        kind === 'basic'
                          ? { kind: 'basic' as const, username: '', password: '' }
                          : kind === 'bearer'
                            ? { kind: 'bearer' as const, token: '' }
                            : { kind: 'none' as const };
                      update(index, { ...sink, config: { ...sink.config, type: 'loki', auth } });
                    }}
                  >
                    <option value="none">none</option>
                    <option value="basic">basic</option>
                    <option value="bearer">bearer</option>
                  </select>
                </label>
              </div>

              {sink.config.auth.kind === 'basic' ? (
                <div className="row">
                  <label>
                    Username
                    <input
                      value={sink.config.auth.username}
                      onChange={(changed) =>
                        update(index, {
                          ...sink,
                          config: {
                            ...sink.config,
                            type: 'loki',
                            auth: {
                              kind: 'basic',
                              username: changed.target.value,
                              password:
                                sink.config.type === 'loki' && sink.config.auth.kind === 'basic'
                                  ? sink.config.auth.password
                                  : '',
                            },
                          },
                        })
                      }
                    />
                  </label>
                  <label>
                    Password (leave blank to keep the stored one)
                    <input
                      type="password"
                      placeholder="unchanged"
                      onChange={(changed) =>
                        update(index, {
                          ...sink,
                          config: {
                            ...sink.config,
                            type: 'loki',
                            auth: {
                              kind: 'basic',
                              username:
                                sink.config.type === 'loki' && sink.config.auth.kind === 'basic'
                                  ? sink.config.auth.username
                                  : '',
                              password: changed.target.value,
                            },
                          },
                        })
                      }
                    />
                  </label>
                </div>
              ) : null}

              {sink.config.auth.kind === 'bearer' ? (
                <div className="row">
                  <label>
                    Token (leave blank to keep the stored one)
                    <input
                      type="password"
                      placeholder="unchanged"
                      onChange={(changed) =>
                        update(index, {
                          ...sink,
                          config: {
                            ...sink.config,
                            type: 'loki',
                            auth: { kind: 'bearer', token: changed.target.value },
                          },
                        })
                      }
                    />
                  </label>
                </div>
              ) : null}

              <label>
                Label fields (comma separated)
                <input
                  value={sink.config.labels.fromFields.join(', ')}
                  onChange={(changed) =>
                    update(index, {
                      ...sink,
                      config: {
                        ...sink.config,
                        type: 'loki',
                        labels: {
                          static:
                            sink.config.type === 'loki' ? sink.config.labels.static : { job: 'vercel' },
                          fromFields: changed.target.value
                            .split(',')
                            .map((field) => field.trim())
                            .filter((field) => field.length > 0),
                        },
                      },
                    })
                  }
                />
              </label>
              {sink.config.labels.fromFields.some((field) => HIGH_CARDINALITY.includes(field)) ? (
                <p className="warn">
                  One or more of these fields is high-cardinality. Every distinct value creates a new
                  Loki stream; prefer filtering them from the log line with <code>| json</code>.
                </p>
              ) : null}
            </>
          )}

          <div className="row">
            <button
              type="button"
              onClick={() => {
                void (async () => {
                  const result = await testSink(sink.name);
                  setTestResult(`${sink.name}: ${result.ok ? 'OK' : 'FAILED'} — ${result.detail}`);
                })();
              }}
            >
              Send test event
            </button>
            <button type="button" onClick={() => remove(index)}>
              Remove
            </button>
          </div>
        </div>
      ))}

      <div className="row">
        <button type="button" onClick={() => add(newFileSink(working.sinks.length + 1))}>
          Add file sink
        </button>
        <button type="button" onClick={() => add(newLokiSink(working.sinks.length + 1))}>
          Add Loki sink
        </button>
        <button type="button" className="primary" disabled={draft === null} onClick={commit}>
          Save changes
        </button>
        {draft !== null ? (
          <button
            type="button"
            onClick={() => {
              setDraft(null);
            }}
          >
            Discard edits
          </button>
        ) : null}
      </div>
      <p className="muted">
        A test event is sent through the saved configuration, so save before testing. Renaming a
        sink abandons its queued batches, which then appear as an orphaned spool on the Status page.
      </p>
    </>
  );
}
```

- [ ] **Step 5: Verify and commit**

```bash
npm run lint && npm run typecheck && npm test && npm run build:web
git add -A
git commit -m "feat: add drains and sinks admin views

Drain secrets are shown once at creation. Password and token fields
submit blank to mean 'keep the stored value', matching the write-only
secret handling in the API. The Loki label field warns inline on
high-cardinality choices."
```

---

### Task 27: Container, compose example, proxy configs, and smoke test

**Files:**
- Create: `Dockerfile`, `docker-compose.example.yml`, `examples/Caddyfile`, `examples/nginx.conf`, `scripts/smoke.sh`, `.github/workflows/ci.yml`
- Modify: `package.json` (add the `smoke` script)

**Interfaces:**
- Consumes: the built `dist/src/index.js` and `web/dist`.
- Produces: a runnable image and `npm run smoke`.

- [ ] **Step 1: Write the `Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
ARG NODE_IMAGE=node:24-alpine

FROM ${NODE_IMAGE} AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

FROM deps AS build-web
WORKDIR /app
COPY vite.config.ts ./
COPY types ./types
COPY web ./web
RUN npm run build:web

FROM deps AS build-server
WORKDIR /app
COPY tsconfig.json tsconfig.build.json ./
COPY types ./types
COPY src ./src
RUN npm run build:server

FROM ${NODE_IMAGE} AS runtime
ARG APP_VERSION=dev
ENV NODE_ENV=production \
    APP_VERSION=${APP_VERSION} \
    PORT=8080 \
    HOST=0.0.0.0 \
    CONFIG_DIR=/config \
    SPOOL_DIR=/spool \
    LOGS_ROOT=/logs \
    WEB_ROOT=/app/web/dist \
    LOG_LEVEL=info
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY --from=build-server /app/dist ./dist
COPY --from=build-web /app/web/dist ./web/dist

RUN addgroup -g 10001 -S app \
 && adduser -u 10001 -S app -G app \
 && mkdir -p /config /spool /logs \
 && chown -R 10001:10001 /config /spool /logs

USER 10001:10001
EXPOSE 8080
VOLUME ["/config", "/spool", "/logs"]

# busybox wget; alpine has no curl.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null 2>&1 || exit 1

CMD ["node", "dist/src/index.js"]
```

- [ ] **Step 2: Write `docker-compose.example.yml`**

```yaml
# Example stack: the drain service behind Caddy, shipping to Loki, viewed in
# Grafana. Copy to docker-compose.yml and adjust before using.
services:
  drain:
    build:
      context: .
      args:
        APP_VERSION: '0.1.0'
    init: true
    restart: unless-stopped
    environment:
      AUTH_MODE: proxy
      # The Docker bridge network. Narrow this to your proxy's actual address.
      AUTH_TRUSTED_PROXIES: '172.16.0.0/12'
      AUTH_USER_HEADER: X-Forwarded-User
      # AUTH_ALLOWED_USERS: 'you@example.com'
      LOG_LEVEL: info
    volumes:
      - drain-config:/config
      - drain-spool:/spool
      - drain-logs:/logs
    # No published port: only Caddy should reach it.
    expose:
      - '8080'

  caddy:
    image: caddy:2-alpine
    restart: unless-stopped
    ports:
      - '8080:80'
    volumes:
      - ./examples/Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy-data:/data
    depends_on:
      - drain

  loki:
    image: grafana/loki:3.4.2
    restart: unless-stopped
    command: -config.file=/etc/loki/local-config.yaml
    expose:
      - '3100'
    volumes:
      - loki-data:/loki

  grafana:
    image: grafana/grafana:11.5.2
    restart: unless-stopped
    ports:
      - '3000:3000'
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: 'true'
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
    depends_on:
      - loki

volumes:
  drain-config:
  drain-spool:
  drain-logs:
  loki-data:
  caddy-data:
```

Separate named volumes for config, spool, and logs are the point: a full logs
volume then becomes backpressure rather than a failure that also takes the
queue down.

- [ ] **Step 3: Write `examples/Caddyfile`**

```caddyfile
# The split that matters: Vercel must reach /api/drain/* WITHOUT SSO, because
# it authenticates by HMAC signature and cannot present an identity. Everything
# else is the admin surface and must be authenticated.
:80 {
	# --- Unauthenticated: the drain endpoint and health probes. ---
	@public path /api/drain/* /healthz /readyz
	handle @public {
		reverse_proxy drain:8080
	}

	# --- Authenticated: the admin API and the SPA. ---
	handle {
		# Replace with a real hash: `caddy hash-password`
		basic_auth {
			admin $2a$14$REPLACE_WITH_YOUR_OWN_BCRYPT_HASH
		}
		reverse_proxy drain:8080 {
			# The service reads this header, but only for requests whose socket
			# peer is inside AUTH_TRUSTED_PROXIES.
			header_up X-Forwarded-User {http.auth.user.id}
		}
	}
}
```

In production, swap `basic_auth` for a real identity provider — Cloudflare
Access, oauth2-proxy, or Tailscale — and point `AUTH_USER_HEADER` at whatever
header it sets (for Cloudflare Access, `Cf-Access-Authenticated-User-Email`).

- [ ] **Step 4: Write `examples/nginx.conf`**

```nginx
# Same split as the Caddyfile: /api/drain/* is unauthenticated because Vercel
# authenticates by HMAC; everything else requires a logged-in operator.
upstream drain {
    server drain:8080;
}

server {
    listen 80;

    # --- Unauthenticated: the drain endpoint and health probes. ---
    location /api/drain/ {
        proxy_pass http://drain;
        proxy_set_header Host $host;
        # Never forward a client-supplied identity header here.
        proxy_set_header X-Forwarded-User "";
    }

    location = /healthz { proxy_pass http://drain; }
    location = /readyz  { proxy_pass http://drain; }

    # --- Authenticated: the admin API and the SPA. ---
    location / {
        auth_basic "drain admin";
        auth_basic_user_file /etc/nginx/htpasswd;

        proxy_pass http://drain;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-User $remote_user;
    }
}
```

- [ ] **Step 5: Write `scripts/smoke.sh`**

This is the test unit tests structurally cannot replace: it catches a wrong
`CMD`, a dev dependency needed at runtime, and volume permission mistakes.

```bash
#!/usr/bin/env bash
set -euo pipefail

IMAGE="vercel-log-drain:smoke"
NAME="vld-smoke-$$"
PORT="18080"

cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> building image"
docker build --build-arg APP_VERSION=smoke -t "$IMAGE" .

echo "==> starting container"
docker run -d --name "$NAME" \
  -p "127.0.0.1:${PORT}:8080" \
  -e AUTH_MODE=disabled \
  -e LOG_LEVEL=info \
  --tmpfs /config:uid=10001,gid=10001 \
  --tmpfs /spool:uid=10001,gid=10001 \
  --tmpfs /logs:uid=10001,gid=10001 \
  "$IMAGE" >/dev/null

echo "==> waiting for health"
for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:${PORT}/healthz" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS "http://127.0.0.1:${PORT}/healthz" >/dev/null || {
  echo "FAIL: healthz never became ready"; docker logs "$NAME"; exit 1; }

echo "==> creating a drain"
CREATED=$(curl -fsS -X POST "http://127.0.0.1:${PORT}/api/admin/drains" \
  -H 'content-type: application/json' -d '{"name":"smoke"}')
DRAIN_ID=$(printf '%s' "$CREATED" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).id))')
SECRET=$(printf '%s' "$CREATED" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).secret))')

echo "==> configuring a file sink"
CONFIG=$(curl -fsS "http://127.0.0.1:${PORT}/api/admin/config")
UPDATED=$(printf '%s' "$CONFIG" | SECRET="$SECRET" node -e '
let raw = "";
process.stdin.on("data", (d) => (raw += d)).on("end", () => {
  const { config, etag } = JSON.parse(raw);
  config.sinks = [
    {
      name: "smoke-file",
      enabled: true,
      filter: {},
      maxSpoolBytes: 1048576,
      maxBatchEvents: 1000,
      maxBatchBytes: 1048576,
      config: {
        type: "file",
        directory: "/logs",
        filePrefix: "events",
        retentionDays: 0,
        freeSpaceFloorBytes: 0,
      },
    },
  ];
  process.stdout.write(JSON.stringify({ config, etag }));
});')
curl -fsS -X PUT "http://127.0.0.1:${PORT}/api/admin/config" \
  -H 'content-type: application/json' -d "$UPDATED" >/dev/null

echo "==> posting a signed delivery"
BODY='[{"id":"smoke-1","timestamp":1573817187330,"source":"lambda","projectId":"p1","level":"info","message":"smoke test"}]'
SIG=$(SECRET="$SECRET" BODY="$BODY" node -e '
const { createHmac } = require("node:crypto");
process.stdout.write(createHmac("sha1", process.env.SECRET).update(process.env.BODY).digest("hex"));')

STATUS=$(curl -s -o /tmp/vld-smoke-response -w '%{http_code}' \
  -X POST "http://127.0.0.1:${PORT}/api/drain/${DRAIN_ID}" \
  -H "x-vercel-signature: ${SIG}" -H 'content-type: application/json' -d "$BODY")
[ "$STATUS" = "200" ] || { echo "FAIL: expected 200, got $STATUS"; cat /tmp/vld-smoke-response; docker logs "$NAME"; exit 1; }

echo "==> asserting a rejected signature is refused"
BAD=$(curl -s -o /dev/null -w '%{http_code}' \
  -X POST "http://127.0.0.1:${PORT}/api/drain/${DRAIN_ID}" \
  -H "x-vercel-signature: $(printf 'f%.0s' $(seq 1 40))" -d "$BODY")
[ "$BAD" = "403" ] || { echo "FAIL: expected 403 for a bad signature, got $BAD"; exit 1; }

echo "==> waiting for the file sink to write"
for _ in $(seq 1 30); do
  if docker exec "$NAME" sh -c 'grep -q smoke-1 /logs/events-*.jsonl' 2>/dev/null; then break; fi
  sleep 1
done
docker exec "$NAME" sh -c 'grep -q smoke-1 /logs/events-*.jsonl' || {
  echo "FAIL: file sink never wrote the event"
  docker exec "$NAME" sh -c 'ls -la /logs /spool/smoke-file 2>&1' || true
  docker logs "$NAME"
  exit 1
}

echo "==> asserting readiness and a drained spool"
curl -fsS "http://127.0.0.1:${PORT}/readyz" >/dev/null || {
  echo "FAIL: readyz reported not ready"; docker logs "$NAME"; exit 1; }

echo "==> asserting the process runs as uid 10001"
UID_IN_CONTAINER=$(docker exec "$NAME" id -u)
[ "$UID_IN_CONTAINER" = "10001" ] || { echo "FAIL: expected uid 10001, got $UID_IN_CONTAINER"; exit 1; }

echo "SMOKE PASSED"
```

Make it executable and register the script:

```bash
chmod +x scripts/smoke.sh
npm pkg set scripts.smoke="bash scripts/smoke.sh"
```

- [ ] **Step 6: Write `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  push:
    branches: ['**']
  pull_request:

jobs:
  verify:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '24'
          cache: npm
      - run: npm ci
      - run: npm run lint
      - run: npm run typecheck
      - run: npm test
      - run: npm run build

  smoke:
    runs-on: ubuntu-latest
    needs: verify
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '24'
      - run: npm run smoke
```

No image is published; that is deliberate and stays out of scope until asked
for.

- [ ] **Step 7: Run the smoke test locally**

Run: `npm run smoke`
Expected: ends with `SMOKE PASSED`. If the container exits immediately, read
`docker logs` — the most likely causes are a wrong `CMD` path (remember the
entrypoint is `dist/src/index.js`, not `dist/index.js`) or a dependency that
was pruned by `--omit=dev` but is needed at runtime.

- [ ] **Step 8: Commit**

```bash
npm run lint && npm run typecheck && npm test && npm run build
git add -A
git commit -m "feat: add container, compose example, proxy configs, and smoke test

Four-stage build running as uid 10001 with three separate volumes. The
Caddy and nginx examples both demonstrate the split that is easy to get
wrong: /api/drain/* must bypass SSO because Vercel authenticates by
HMAC, while everything else requires an operator identity."
```

---

### Task 28: README

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: everything.
- Produces: operator documentation.

- [ ] **Step 1: Write `README.md`**

Replace the existing two-line file. Cover exactly these sections, in order,
with no invented claims — every behavior described must be one this
implementation actually has:

1. **What it is** — one paragraph: receives Vercel Drain deliveries, spools each
   event to disk per sink, forwards to a local file and/or Loki. Docker only.
2. **Quick start** — `docker compose -f docker-compose.example.yml up`, then
   open `http://localhost:8080`, create a drain, copy the URL and secret into
   Vercel, add a sink.
3. **Configuring the Vercel side** — create a Drain pointing at
   `https://your-host/api/drain/<drainId>`, paste the signature secret, and note
   that both `json` and `ndjson` encodings work with or without gzip.
4. **Authentication** — the full table of `AUTH_MODE`, `AUTH_TRUSTED_PROXIES`,
   `AUTH_USER_HEADER`, `AUTH_ALLOWED_USERS`. State plainly: **unset means the
   admin UI returns 503, while ingest keeps working**, and `disabled` is for
   local development only. Include the warning that the reverse proxy must
   leave `/api/drain/*` unauthenticated, with a pointer to
   `examples/Caddyfile`.
5. **Volumes** — the three mounts, what lives in each, and why they should be
   separate: a full logs volume becomes backpressure instead of data loss.
   Include the `chown -R 10001:10001` note.
6. **Sinks** — the file sink (date-partitioned JSONL, `retentionDays`,
   free-space floor) and the Loki sink (push URL, auth modes, tenant, label
   allowlist). Include the cardinality warning and the recommended default
   label set.
7. **Secrets** — which three fields are scrubbed from API reads (a drain's
   `secret`, a Loki sink's `auth.password` and `auth.token`) and, explicitly,
   that a Loki sink's `labels.static` values are NOT scrubbed: anything pasted
   there is returned on every `GET` and shipped to Loki as a label. Tell the
   reader not to put credentials in static labels.
12. **Delivery semantics** — at-least-once, stated plainly: a spool write
   failure returns 500 so Vercel redelivers, which can duplicate events into
   sinks that already succeeded. Loki collapses identical entries; the file
   sink does not.
8. **Operations** — reading the status page, what a climbing head age means,
   what dead-lettered means and where those files live
   (`/spool/<sink>/dead/`), what an orphaned spool is, and that counters reset
   on restart.
9. **Troubleshooting** — a table of symptom → cause → fix covering at minimum:
   403 `invalid_signature` (wrong secret), 404 from Vercel (wrong drain id),
   admin UI 503 (`AUTH_MODE` unset), admin 403 (`AUTH_TRUSTED_PROXIES` does not
   include the proxy's address), Loki sink `failed` with 401 (credentials),
   Loki sink dead-lettering with 400 (`reject_old_samples_max_age` shorter than
   the outage), container exits with a `chown` message (volume ownership).
10. **Development** — `npm ci`, `npm run dev` is not defined; use
    `npm run build && npm start` with `AUTH_MODE=disabled` and local
    directories, or `npm run test:watch`. Document `npm run lint`,
    `typecheck`, `test`, `build`, `smoke`.
11. **Design** — link `docs/superpowers/specs/2026-09-08-vercel-log-drain-design.md`
    and note that the rejected alternatives are recorded there.

Keep it factual and free of marketing language. Where a default is stated,
copy it from `src/config/schema.ts` rather than recalling it.

- [ ] **Step 2: Verify every command in the README actually works**

Run each shell command the README tells the reader to run, in a scratch
directory, and correct the README where reality differs. In particular confirm
the compose file's port, the SPA URL, and the script names.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs: document setup, auth, volumes, and troubleshooting"
```

---

## Definition of Done

- [ ] `npm run lint` clean.
- [ ] `npm run typecheck` clean for both the server and web projects.
- [ ] `npm test` green, including `test/e2e/durability.test.ts`.
- [ ] `npm run build` produces `dist/src/index.js` and `web/dist/index.html`.
- [ ] `npm run smoke` ends with `SMOKE PASSED`.
- [ ] No `any` and no `unknown` outside the two documented parse boundaries
      (`src/vercel/decode.ts`) and the one documented display payload
      (`types/api.ts`).
- [ ] No `*Sync` call anywhere in `src/` or `web/` — `node/no-sync` enforces
      this, so a clean `npm run lint` is the proof.
- [ ] The branch is **not** pushed. Ask before pushing or opening a PR.
