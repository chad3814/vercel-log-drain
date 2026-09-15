# vercel-log-drain — Design

- **Date:** 2026-09-08
- **Status:** Approved, pending implementation plan
- **Branch:** `feat/log-drain-service`

## 1. Purpose and scope

A self-hosted service, shipped as a Docker container, that receives Vercel Drain
deliveries over HTTP and forwards each log event to one or more configured
sinks. Two sink types ship in v1: append to a local file, and push to Loki.

### Goals

1. Accept Vercel log drain deliveries, authenticated by drain signature.
2. Never lose an accepted event to a sink outage or a process restart.
3. Let an operator manage drains and sinks from a browser, with a read-only
   status page for health.
4. Run as a single container with three mounted volumes and no external
   dependencies.

### Non-goals

- Log querying, searching, or browsing. Loki does that better; the status page
  shows only a short tail to answer "is anything arriving?".
- Multi-replica or clustered operation. Spool directories are owned
  exclusively by one process, and the design depends on that.
- Sink types beyond file and Loki. The sink interface makes a third type a new
  module, but v1 ships two.
- Authentication of the admin UI by the service itself. Identity is delegated
  to a reverse proxy.

## 2. Background: the Vercel Drains protocol

Verified against Vercel documentation on 2026-09-08 (`/docs/drains`,
`/docs/drains/security`, `/docs/drains/reference/logs`), not from recall:

- Deliveries are `POST` requests to a configured endpoint.
- The `x-vercel-signature` header is the **HMAC-SHA1 of the raw request body**,
  hex-encoded, keyed with the drain's signature secret.
- `encoding` is per-drain and is either `json` (a JSON array of events) or
  `ndjson` (newline-delimited JSON objects).
- `compression` is optionally `gzip`.
- Custom headers can be configured per drain.
- There is **no** `x-vercel-verify` ownership challenge in the current Drains
  API. Vercel validates an endpoint via its own `POST /v1/drains/test`, which
  sends sample events to the configured endpoint. Nothing special is required
  of us beyond correctly handling a normal signed delivery.

Event fields observed in the documented payloads: `id`, `deploymentId`,
`source` (`build` | `lambda` | `static` | `edge` | `external`), `host`,
`timestamp` (epoch ms), `projectId`, `projectName`, `level`, `message`,
`buildId`, `type`, `entrypoint`, `requestId`, `statusCode`, `path`,
`executionRegion`, `environment`, `traceId`, `spanId`, `trace.id`, `span.id`,
and a nested `proxy` object.

Vercel retries a failed delivery only a limited number of times, so the service
must acknowledge quickly and take on durability itself rather than relying on
upstream retries.

## 3. Architecture

### 3.1 Process model

One Node process, no clustering. Hono (via `@hono/node-server`) serves three
surfaces:

- `POST /api/drain/:drainId` — the drain endpoint (HMAC-authenticated).
- `/api/admin/*`, `/api/status` — the admin API (proxy-authenticated).
- `/` — the built React SPA, as static assets.

A **Dispatcher** runs alongside the server and owns one async worker loop per
enabled sink. Single-process ownership of each spool directory removes any need
for file locking.

On `SIGTERM`: stop accepting deliveries, allow in-flight `deliver()` calls to
settle within a deadline, then exit. Anything unacknowledged remains on disk
and is redelivered on the next boot.

### 3.2 Data flow

```
Vercel ──POST /api/drain/:drainId──> verify HMAC ──> decode (gunzip? json|ndjson)
                                                          │
                                                    LogEvent[] (validated)
                                                          │
                                      ┌───────────────────┴───────────────────┐
                                 filter(sinkA)                           filter(sinkB)
                                      │                                       │
                        spool/sinkA/<seq>.jsonl                 spool/sinkB/<seq>.jsonl
                                   fsync                                   fsync
                                      └──────────── 200 OK to Vercel ────────┘
                                      │                                       │
                              worker(sinkA) ──> file sink            worker(sinkB) ──> Loki
                              unlink on success                      unlink on success
```

### 3.3 Modules

| Module | Responsibility | Depends on |
|---|---|---|
| `src/vercel/` | `signature.ts` (timing-safe HMAC-SHA1), `decode.ts` (gunzip + json/ndjson → events), `event.ts` (zod schema). Pure functions: bytes in, events out. No I/O. | — |
| `src/config/` | Config zod schema, atomic load/save, secret redaction. Knows sink config *schemas* via the registry, not sink behavior. | `sinks/types` |
| `src/pipeline/spool.ts` | `SpoolQueue`: `enqueue`, `peekOldest`, `ack`, `bytes`, `prune`, `deadLetter`. Generic over payload; knows only files, dirs, byte budgets. | `node:fs/promises` |
| `src/pipeline/filter.ts` | Compiles a `SinkFilter` into a predicate. | `vercel/event` |
| `src/pipeline/dispatcher.ts` | The only module aware of both queues and sinks. Reconciles desired config → running workers. Owns retry/backoff. | spool, sinks, config |
| `src/sinks/` | `types.ts` (interface + type registry), `file.ts`, `loki.ts`. A sink knows nothing about spooling or HTTP ingest. | — |
| `src/server/` | Routing, signature middleware, proxy-auth middleware, JSON in/out. Delegates all logic. | all of the above |
| `src/status/metrics.ts` | Counters, per-sink health, ring buffers of recent events/errors/rejects. | — |
| `src/log.ts` | pino logger with secret redaction. | — |

### 3.4 Sink contract

```ts
interface Sink {
  readonly name: string;
  readonly type: string;
  deliver(events: LogEvent[]): Promise<void>; // throw to retry
  close(): Promise<void>;
}

interface SinkType<TConfig> {
  readonly type: string;
  readonly configSchema: z.ZodType<TConfig>;
  create(name: string, config: TConfig, ctx: SinkContext): Sink;
}
```

A thrown error means retry. A thrown `PermanentDeliveryError` means the batch is
unfixable: it is moved to `spool/<sink>/dead/`, counted, and the queue advances.
Without this distinction, one poison batch at the head of the queue blocks
delivery forever.

`dead/` is terminal storage, never a staging area: nothing the service does may
remove or replace a file already in it. Two consequences bind the
implementation. Batch sequence numbers must be monotonic across the contents of
`dead/` as well as the live directory, because a restart that finds the live
directory empty would otherwise reissue a name a dead-lettered file already
holds — and POSIX `rename` replaces its destination silently. And the move into
`dead/` must not overwrite an existing name even if one somehow appears. This
guarantee is what makes the retryable classification of `401`/`403`/`404` worth
anything: preserving a batch on disk is only a preservation if the next failure
cannot erase it.

`SinkContext` provides the logger and the metrics recorder, so sinks never
import them directly.

### 3.5 Repository layout

```
src/
  index.ts              entrypoint: load config → build app → start server + dispatcher
  server/
    app.ts
    routes/{drain,admin,status}.ts
    middleware/proxy-auth.ts
  vercel/{signature,decode,event}.ts
  config/{schema,store,redact}.ts
  pipeline/{filter,spool,dispatcher}.ts
  sinks/{types,file,loki}.ts
  status/metrics.ts
  log.ts
types/                  config + status shapes shared by server and SPA
web/                    Vite + React admin SPA (own tsconfig)
test/                   vitest, mirrors src/
Dockerfile
docker-compose.example.yml
examples/{Caddyfile,nginx.conf}
```

Two `tsconfig`s (server = Node ESM, web = DOM) with a shared `types/`
directory, so the API contract is typed on both ends without codegen.

## 4. Volume model

Three independent mounts, each checked against its own filesystem with
`fs.statfs`:

| Mount | Default | Contents | Behavior on low free space |
|---|---|---|---|
| config | `/config` | `config.json`, `config.json.bak` | Writes are tiny. A full volume fails the admin save with `507` and a clear message; the running config is unaffected because it is held in memory. |
| spool | `/spool` | `<sink>/*.jsonl`, `<sink>/dead/` | `statfs(SPOOL_DIR)` before enqueue. Below the floor: discard the incoming batch, increment `droppedEvents`, mark the service `degraded`, and record it in `recent.errors`. **See the open question below.** |
| logs | `/logs` | `events-YYYY-MM-DD.jsonl` | `statfs` on the file sink's own directory before appending. Below the floor: `deliver()` throws a **retryable** error. |

Because the logs volume is separate, a full logs volume loses nothing: the
batch stays in the spool and backs off exactly as it would during a Loki
outage. Events are dropped only when the **spool** volume hits its floor —
a single, clearly defined loss point.

**Path containment.** The file sink's `directory` is editable from the browser,
so the server resolves it with `realpath` and rejects anything that does not
land under `LOGS_ROOT` (default `/logs`). This prevents an admin session from
pointing a sink at `/config` or `/spool` and corrupting them.

## 5. Ingest path

`POST /api/drain/:drainId`:

1. Read the raw body into a `Buffer`, capped at `server.maxBodyBytes`
   (default 16 MiB); beyond that, respond `413`.
2. Look up `drainId` in the in-memory config. Unknown → `404`. Disabled →
   `403`. IDs are randomly generated, so enumeration is not a concern.
3. **Verify the signature before interpreting the body.** Compute
   `HMAC-SHA1(rawBytes, drain.secret)` as hex and compare with
   `crypto.timingSafeEqual` after an explicit **byte**-length check — a JS
   string's `.length` counts UTF-16 code units, and a header of non-ASCII
   bytes can match on code units while differing in bytes, which makes
   `timingSafeEqual` throw instead of returning false. Missing header →
   `401`. Mismatch → `403 {"code":"invalid_signature"}`. Verifying before
   decompression means an unauthenticated caller can never make the service
   spend CPU on gzip expansion.
4. If `content-encoding: gzip`, decompress with `promisify(zlib.gunzip)` —
   never `gunzipSync`, which would block the same event loop serving every
   other request — subject to a decompressed-size cap.
5. Parse by sniffing the first non-whitespace byte: `[` means a JSON array,
   anything else means NDJSON. Vercel does set `content-type` from the drain's
   `encoding`, but sniffing costs nothing and removes a class of
   misconfiguration bug.
6. Validate each event with a **lenient** zod schema: require `id`,
   `timestamp`, `source`, and `projectId`; `.passthrough()` everything else so
   `proxy`, `traceId`, and any field Vercel adds later reach the sinks
   untouched. A malformed entry is counted and pushed onto the `rejects` ring
   buffer, but does **not** fail the request — one bad line must not cause
   Vercel to redeliver the other 999 good ones.
7. For each enabled sink, apply its compiled filter and enqueue the surviving
   events into that sink's spool.
8. Respond `200 {"received":n,"accepted":n,"rejected":n}` only after every
   spool write has been fsynced.

**Delivery semantics: at-least-once, by design.** If any spool write fails, the
response is `500` so Vercel redelivers the entire batch, which may duplicate
events into sinks that already succeeded. For logs this is the correct trade —
duplication is recoverable, loss is not — and Loki collapses identical
`(labels, timestamp, line)` entries on its own. This is documented behavior,
not a deduplication cache.

## 6. Spool queue

**Naming.** `spool/<sink>/000000000042.jsonl`, a zero-padded 12-digit monotonic
sequence, so lexicographic order is FIFO order. Content is one JSON event per
line; batch metadata is carried by the filename and directory, keeping the file
itself pure JSONL.

**Open question — what the spool free-space floor should do.** An earlier draft
of the table above said "drop oldest within that sink's budget". That is wrong
for this condition and has been corrected to describe what the code does:
dropping one sink's oldest batch cannot free a volume that some other sink, or
something outside the service entirely, has filled. Two defensible behaviours
remain, and the choice is an operational trade rather than a technical one:

- **Shed (current).** Discard the incoming batch, count it, report `degraded`,
  and record it in `recent.errors`. Ingest stays up for every sink, including
  healthy ones, and a full volume costs the newest events.
- **Backpressure.** Refuse the delivery so Vercel retries. This makes "no
  acknowledged delivery is ever lost" true without qualification — the service
  never acknowledges what it cannot store — at the cost of coupling ingest to
  disk pressure: one stuck sink filling the volume would stop deliveries that a
  healthy sink could have taken.

The logs volume already uses backpressure (§4), so the two volumes are
currently inconsistent, which is the strongest argument for changing this. It
is deliberately unresolved pending the operator's judgement; the current
behaviour is at least honest and visible, which the original was not.

**Write protocol.** `<name>.tmp` → `fsync` → `rename()` → fsync the directory.
`rename` is atomic, so a crash mid-write can never expose a partial batch.
Budget eviction runs only after all four steps have completed: eviction is an
unlink, so any step still ahead of it can fail with the old batch already
destroyed and the replacement not yet committed. Boot recovery deletes stray
`.tmp` files, which leaves such a replacement nowhere to survive. The service
therefore accepts a transient peak of the sink's byte budget plus one payload,
and a transient over-budget spool if it crashes between the two — both
self-heal on the next enqueue, whereas the lost batch does not.

**Boot recovery.** Ensure directories exist; delete stray `.tmp` files; recover
each sequence counter as `max(existing) + 1`; tally directory sizes. Any
pre-existing batch files are simply delivered — replay needs no special path.

**Worker loop**, one per sink:

```
oldest file → coalesce consecutive files up to maxBatchEvents / maxBatchBytes
           → sink.deliver(events)
   success → unlink all coalesced files, reset failure count, set lastSuccessAt
 retryable → leave files in place; backoff 1s → 2s → 4s … capped at 60s with
             jitter; health = 'retrying', then 'failed' after 5 consecutive
permanent → move the batch to spool/<sink>/dead/, count it, advance
     empty → await an enqueue signal, with a 500 ms poll as a safety net
```

`maxBatchEvents` and `maxBatchBytes` are properties of the sink *entry*, not of
the sink type, because coalescing is the worker's behavior rather than the
sink's. Coalescing keeps Loki pushes efficient during a burst. Acknowledging the
whole coalesced group together is safe because a Loki push is all-or-nothing.

**Overflow.** Each sink has `maxSpoolBytes` (default 512 MiB). If an enqueue
would exceed it, delete oldest files until the batch fits and increment
`droppedEvents`. Drop-oldest rather than drop-newest: the freshest logs are the
operationally useful ones, and the drop counter is displayed prominently.

## 7. Sinks

### 7.1 File sink

```ts
{ type: 'file',
  directory: string,            // must resolve under LOGS_ROOT
  filePrefix: string,           // default 'events'
  retentionDays: number,        // default 14; 0 = keep forever
  freeSpaceFloorBytes: number } // default 256 MiB
```

Files are partitioned by **the event's own UTC timestamp**, not wall clock.
This matters on replay: after a two-day outage, the draining spool writes
events into their own day's file rather than today's. A batch that straddles
midnight is grouped by date, with one append per date.

File handles are cached in a small LRU. Each batch write is `fsync`ed before
`deliver()` resolves, because resolving is what deletes the spool file — the
only durable copy. The cost is acceptable because the worker already coalesced
the batch.

Retention runs at boot and hourly. A file is deleted only when **all** of
these hold: its name matches `<prefix>-YYYY-MM-DD.jsonl` (with the prefix
regex-escaped, since the schema permits `.`); the date in the name is a real
calendar date older than `retentionDays`; that date is not currently held open
by the handle cache; and the file's own `mtime` is also older than
`retentionDays`.

The `mtime` condition is what makes retention safe under replay. A batch
delayed past the retention window still carries its original event
timestamps, so the file it lands in looks expired the moment it is written.
Deleting on the filename alone would discard data the service has already
reported as delivered — and because a POSIX `unlink` beneath an open handle
neither fails nor stops subsequent writes, that loss would be completely
silent. Requiring the file itself to be stale, not merely its name, is what
closes that hole.

The open-handle check narrows the residual race but does not eliminate it.
`protectedDates` is snapshotted once when a prune pass begins, so a delivery
that opens a *new* handle for a stale-looking date mid-pass is invisible to
it; the live `mtime` check is the backstop, and that is a stat-then-act
sequence. A delivery landing in the gap between one file's `stat` and its
`unlink` can still lose data. Reaching it requires a replay delayed longer
than the entire retention window arriving inside that sub-millisecond gap,
against a previously certain loss for any such replay during the hour between
prune passes. That residual is accepted rather than solved: eliminating it
would need locking between the pruner and the writer, which is not worth the
complexity at this scale.

Before appending, `statfs` on `directory`; below `freeSpaceFloorBytes`, throw a
retryable error so the batch stays spooled.

### 7.2 Loki sink

```ts
{ type: 'loki',
  url: string,                  // e.g. http://loki:3100
  auth: { kind: 'none' }
      | { kind: 'basic', username: string, password: string }
      | { kind: 'bearer', token: string },
  tenantId?: string,            // X-Scope-OrgID
  labels: { static: Record<string, string>, fromFields: string[] },
  timeoutMs: number }           // default 10000
```

**Request.** `POST ${url}/loki/api/v1/push` with
`{"streams":[{"stream":{…},"values":[[nanos, line], …]}]}`, gzipped with async
`zlib.gzip` (log batches compress roughly tenfold, which is the difference
between a fine and a painful push over a WAN). The URL is normalized so a
value already ending in `/loki/api/v1/push` is not double-suffixed.

**Timestamps.** Vercel's epoch milliseconds become nanosecond strings:
`String(BigInt(ms) * 1_000_000n)`.

**Streams.** Events are grouped by resolved label set, one stream per set, with
values sorted ascending by timestamp within each stream.

**Line body.** The complete event serialized as JSON, including fields promoted
to labels, so LogQL `| json` sees every field.

**Labels.** `labels.static` (default `{"job":"vercel"}`) merged with fields
named in `labels.fromFields` (default `projectName`, `environment`, `source`,
`level` — `projectName` rather than `projectId` because these are read by
humans in Grafana). Label names are sanitized to Loki's
`[a-zA-Z_][a-zA-Z0-9_]*`, so `trace.id` becomes `trace_id`. Empty or missing
values are omitted rather than sent, since Loki rejects empty label values.
Label values are truncated at 1024 characters.

The UI warns when `fromFields` includes a known high-cardinality field (`id`,
`requestId`, `deploymentId`, `path`, `host`, `traceId`, `spanId`, `buildId`)
and the server returns those warnings alongside the validated config, but
neither blocks the choice.

**Error classification.**

| Response | Treatment | Rationale |
|---|---|---|
| `2xx` | success; unlink spool files | — |
| `429`, `5xx`, timeout, `ECONNREFUSED`, `ENOTFOUND` | retryable, normal backoff | transient |
| `400` | **permanent**; dead-letter the batch | Loki returns 400 for malformed streams and for entries outside `reject_old_samples_max_age`. Replaying a week-old spool would otherwise return 400 forever and the queue head would never advance. |
| `401`, `403`, `404` | retryable, but health jumps straight to `failed` with "check credentials / URL" | Dead-lettering on a wrong password would silently destroy logs. This preserves them on disk until the operator fixes the config. |
| `413`, `422` | retryable, health jumps straight to `failed` with "lower `maxBatchBytes`, or raise the proxy's body limit" | Added 2026-09-15, after the implementation was found classifying both as **permanent** — which this table never said, and which contradicts the row above. A `413` is config-fixable from either end (an nginx in front of Loki defaults to `client_max_body_size 1m`, against a 4 MiB default `maxBatchBytes`), and the same batch succeeds unchanged once it is fixed, so the argument for keeping `401`/`403`/`404` retryable applies verbatim. Loki does not itself use `422`; arriving from something in front of Loki it is no more inherently unfixable, and making it the one undocumented permanent status is exactly the code/spec disagreement about which statuses destroy data that this table exists to prevent. `400` is therefore the only permanent status. |

**Test action.** `POST /api/admin/sinks/:name/test` pushes one synthetic event
through the live sink configuration and returns the actual response or error,
which makes first-time Loki setup tractable.

## 8. Configuration

### 8.1 Schema

```ts
{
  version: 1,
  drains: [{ id, name, secret, enabled, createdAt }],
  sinks: [{
    name,           // ^[a-z0-9][a-z0-9-]{0,63}$  — also the spool directory name
    enabled,
    filter,
    maxSpoolBytes,      // default 512 MiB
    maxBatchEvents,     // default 1000  — worker coalescing bound
    maxBatchBytes,      // default 4 MiB — worker coalescing bound
    config              // discriminated union on `type`: file | loki
  }],
  server: { maxBodyBytes, spoolFreeSpaceFloorBytes }
}
```

`sinks[].name` doubles as a directory name, so it is validated strictly: no
dots, no slashes, no traversal sequences.

`filter` holds four optional structured predicates — `minLevel`, `sources`,
`environments`, `projectIds` — where an absent predicate matches everything.
Level ordering is `info < warning < error`; an unrecognized level is treated as
`info`. A message-substring filter is deliberately excluded: it is the field
that invites regex creep, and LogQL does it better downstream.

### 8.2 Persistence

Saves are written `config.json.tmp` → `fsync` → `rename`, retaining the prior
version as `config.json.bak`.

On boot:

- Missing file → write an empty default config and log a first-run notice.
- Invalid file → **refuse to start**, reporting the exact zod error path, and
  note whether `config.json.bak` parses cleanly. Silently resetting or
  auto-recovering an operator's config is worse than a loud failure that names
  the fix.

External edits are picked up at boot, so a prepared `config.json` can be
dropped into the volume and the container restarted. Otherwise the browser is
the single writer, which keeps conflict semantics trivial.

Admin `PUT` carries the etag (content hash) of the config it was based on; a
mismatch returns `409`, so two open browser tabs cannot clobber each other.

### 8.3 Reconciliation

After a successful save the Dispatcher diffs old against new:

- **Added or enabled** → create the sink, ensure its spool directory, start a
  worker.
- **Removed or disabled** → stop the worker, `close()` the sink, stop
  enqueuing, and **leave the spool on disk**. A config edit must never silently
  destroy queued logs.
  - A **removed** sink's directory appears as `orphanedSpools` on the status
    page, with an explicit discard action.
  - A **disabled** sink is still configured, so it is NOT an orphan: it keeps
    its own row with its real queue figures. This is a correction to an
    earlier draft of this section. Orphans are offered to `discardOrphan`,
    and listing a sink an operator has merely paused among them would offer
    its undelivered backlog for deletion — which is the one thing this bullet
    exists to prevent.
- **Settings changed** → stop, recreate, restart against the *same* spool
  directory, since `name` is the queue's identity. Correcting a wrong Loki URL
  therefore resumes delivery of everything that accumulated.

Renaming a sink is a remove plus an add, which abandons the old queue. The UI
states this before confirming.

### 8.4 Secrets are write-only

`GET` returns `secret: null` with `hasSecret: true`. `PUT` treats an absent or
null secret as "keep existing" and a string as "replace".

**What is and is not treated as a secret.** Exactly three fields are scrubbed:
a drain's `secret`, and a Loki sink's `auth.password` and `auth.token`. Nothing
else is. In particular a Loki sink's `labels.static` values are free-form
strings that are never scrubbed, so a credential pasted there would be returned
on every `GET` *and* shipped to Loki as a label value. That is a real residual
exposure, accepted rather than solved: static labels exist to be read, and
guessing at which of them might be a secret would be both unreliable and
surprising. The admin UI should not encourage putting anything sensitive
there, and the README says so. A drain secret is
displayed exactly once, at creation, with a copy button; afterwards it can be
regenerated but never revealed. Loki passwords and bearer tokens follow the
same rule. This keeps the admin API from becoming a credential-exfiltration
endpoint if the proxy is ever misconfigured.

## 9. Authentication

Identity is delegated to a reverse proxy. The service ships **fail-closed**.

| Variable | Meaning |
|---|---|
| `AUTH_MODE` | `proxy` or `disabled`. **No default.** Unset means `/api/admin/*`, `/api/status`, and the SPA all return `503` with a message naming the variables to set. |
| `AUTH_TRUSTED_PROXIES` | Comma-separated CIDRs. Required when `AUTH_MODE=proxy`. |
| `AUTH_USER_HEADER` | Header carrying the authenticated identity, e.g. `Cf-Access-Authenticated-User-Email`. Required when `AUTH_MODE=proxy`. |
| `AUTH_ALLOWED_USERS` | Optional comma-separated allowlist of identities. |

Two further variables tune retry cadence: `RETRY_BASE_MS` (default `1000`) and
`RETRY_MAX_MS` (default `60000`), the base and cap of the per-sink exponential
backoff. How hard to retry a sink that has been down for hours is a deployment
decision, and the defaults suit a brief outage. Both must parse as positive
integers or the boot fails, like every other malformed variable. Note the cap
bounds the exponential term rather than the delay slept, since jitter is
applied after capping.

`AUTH_MODE=disabled` exists for local development and logs a loud warning on
every boot.

Ingest is unaffected by `AUTH_MODE`. With it unset, the drain endpoint still
accepts and spools signed deliveries and the workers still deliver; only the
admin surface is closed. A running deployment therefore cannot lose logs
because of an auth misconfiguration.

In `proxy` mode a request to an admin route passes only if both hold:

1. The **socket peer address** falls inside a trusted CIDR — checked with
   Node's built-in `net.BlockList`, and never derived from `X-Forwarded-For`,
   which the client controls.
2. The user header is present, non-empty, and in `AUTH_ALLOWED_USERS` when that
   is set.

Any inbound copy of `AUTH_USER_HEADER` is stripped before routing, on every
route and in every auth mode, so a direct caller cannot self-assert an
identity. Two layers do this: the auth middleware strips it on the routes it
guards, and the app mounts `stripIdentityHeader` ahead of the route table so
the guarantee also holds on the auth-exempt drain path. That second layer reads
the header name straight from the environment rather than from the parsed auth
config — deriving it from the `proxy` variant would leave no strip at all under
`AUTH_MODE=disabled` or unset, which are exactly the modes a staging box runs
in. `c.get('user')` is the only channel a handler may treat as identity, and it
is written only after peer trust is established.

Because that strip is app-wide, `AUTH_USER_HEADER` may not name a header the
service itself depends on. `x-vercel-signature` is the dangerous case —
stripping it would fail HMAC verification on every delivery, losing all logs
silently — so a reserved-name list is rejected at startup alongside the
header-name syntax check.

**Peer resolution is injected.** The middleware receives a `PeerResolver`
function rather than reading the server binding directly. `@hono/node-server`
populates `c.env.incoming` only for requests that arrive over a real socket; it
is `undefined` under Hono's in-process `app.request()`, which is how the
route-level tests in §12 run. Injection is what makes the auth matrix testable
without opening sockets, and an unresolvable peer is treated as untrusted and
denied — verified 2026-09-08 against `@hono/node-server` 2.1.1.

**The drain endpoint is exempt**, because it authenticates by HMAC and Vercel
cannot present an SSO identity. This inversion is the deployment footgun worth
documenting prominently: the proxy must enforce SSO on `/` and `/api/admin/*`
while leaving `/api/drain/*` open. `examples/Caddyfile` and
`examples/nginx.conf` demonstrate exactly that split.

## 10. Status and observability

`GET /api/status`, polled every 2 seconds by the SPA:

```
service  { state: ok|degraded, uptimeSec, version, startedAt }
volumes  { config: {path, freeBytes, totalBytes}, spool: {…} }
drains[] { id, name, enabled, eventsReceived, lastEventAt,
           requests: { ok, badSignature, malformed } }
sinks[]  { name, type, enabled,
           health: { state, consecutiveFailures, lastError, lastErrorAt,
                     lastSuccessAt, nextRetryAt },
           queue:  { files, bytes, oldestAgeSec },
           dead:   { files, bytes },
           counters: { delivered, dropped, deadLettered } }
orphanedSpools[] { name, files, bytes, dead: { files, bytes } }
recent   { events: […200], errors: […], rejects: […] }
```

All three of `counters.delivered`, `counters.dropped` and `counters.deadLettered`
count **events**, never batches and never spool files. They appear together in
one object and operators compare them against each other and against a drain's
`eventsReceived`, so a file count among them would mix units in a number that
looks directly comparable. File counts belong in `queue.files`, which reports
them separately and for a different purpose.

**Readiness must not gate the surface that fixes it.** `/readyz` reports 503
only when the spool cannot accept a write — the free-space floor. It
deliberately does NOT report 503 for a failed sink or for a config with no
sinks yet, even though both are `degraded` on the status page. An orchestrator
that removes a pod from rotation on a failing readiness probe also removes the
admin UI and API, which are served by the same process; and a failed sink's URL
and a missing sink are both fixed *through that UI*. Gating on them deadlocks:
a fresh deployment starts with no sinks, so it would never become ready, and
an operator could never reach the page that would fix it. The free-space floor
is exempt because it is not fixed through the UI — an operator frees or resizes
the volume — and refusing traffic there is honest, since the service genuinely
cannot store what it would be handed.

`queue.oldestAgeSec` is derived from the `mtime` of the oldest spool file — the
age of the batch at the head of the queue, not of the events inside it. It is
the single most informative number: a steadily climbing
value means delivery is losing to ingest. Counters are in-memory and reset on
restart, which the page states plainly so they are not misread as historical
totals. `recent.events` is a short tail to confirm arrival, not a log browser.

`/healthz` returns 200 whenever the process is listening (liveness).
`/readyz` returns 503 while the spool volume is below its free-space floor, so
an orchestrator can react to a spool that can no longer accept a write — and
only for that, per the readiness note above. (This sentence previously said
"while the service is `degraded` … a wedged sink or a full spool volume",
which that note supersedes.)

The SPA is React + Vite with three views — Status, Drains, Sinks — polling with
`setInterval` and `fetch`. No websockets.

Service logs go to stdout as JSON via pino, with `secret`, `password`, and
`token` keys redacted at the serializer, so a config-dump log line cannot leak
a credential.

## 11. Container and runtime

Four-stage `Dockerfile` on `node:24-alpine`:

| Stage | Work |
|---|---|
| `deps` | `npm ci` including dev dependencies, from `package.json` + lockfile only |
| `build-web` | `vite build` → `web/dist` |
| `build-server` | `tsc` → `dist/` |
| `runtime` | `npm ci --omit=dev`, copy `dist/` and `web/dist/`, run as uid 10001 |

Plain `tsc` rather than a bundler: a long-running server gains nothing from
bundling, and it is two fewer dependencies.

`EXPOSE 8080`. `HEALTHCHECK` uses busybox `wget -qO- http://127.0.0.1:8080/healthz`
because alpine has no curl. Compose sets `init: true` so `SIGTERM` reaches Node
and the graceful drain runs.

Running as non-root produces one predictable operational papercut: volumes
owned by root are not writable by uid 10001. Boot therefore probes all three
directories for writability and, on failure, exits with a message naming the
uid and the exact `chown` command — rather than an `EACCES` stack trace forty
lines into startup.

Environment carries only what must be known before config can be read:

```
PORT, HOST                             8080, 0.0.0.0
CONFIG_DIR, SPOOL_DIR, LOGS_ROOT       /config, /spool, /logs
AUTH_MODE, AUTH_TRUSTED_PROXIES,
  AUTH_USER_HEADER, AUTH_ALLOWED_USERS no defaults; unset ⇒ admin 503
LOG_LEVEL                              info
```

Everything else lives in `config.json`.

`docker-compose.example.yml` runs the service plus Loki, Grafana, and Caddy —
Caddy specifically to demonstrate the SSO split described in §9.

Runtime dependencies: `hono` 4.13, `@hono/node-server` 2.1, `zod` 4.5, `pino`
10.3. CIDR matching uses the built-in `net.BlockList`; ID generation uses
`node:crypto`. Front end: `react` 19.2, built by `vite` 8.2. Tooling:
`typescript` 7.0 (the native compiler), `vitest` 5.0, `oxlint` 1.82 with
`oxlint-tsgolint` 7.0 for type-aware rules, `prettier` 3.9. Versions verified
against the registry on 2026-09-08.

Linting is oxlint rather than ESLint. Beyond being far faster, it is what makes
TypeScript 7 usable here: `typescript-eslint` 8.70 declares
`typescript@>=4.8.4 <6.1.0` as a peer dependency, so an ESLint-based setup
fails `npm ci` outright with `ERESOLVE` against TypeScript 7. oxlint has no
`typescript` peer dependency, and its optional `oxlint-tsgolint` companion is
versioned against the 7.x native compiler. The TypeScript 7.0.2 / oxlint 1.82 /
oxlint-tsgolint 7.0.2001 combination was installed and exercised on 2026-09-08,
including emit and type-aware linting.

Because `zod` 4 can express recursive JSON with `z.lazy` plus `.catchall()`, the
event schema validates unknown passthrough fields as a `JsonValue` union rather
than typing them `unknown` — which also satisfies the project's prohibition on
`any` and `unknown`.

## 12. Testing strategy

Vitest, in layers.

1. **Pure units** (the bulk of the suite): HMAC verification against known
   vectors, including the wrong-length-header path; decode across
   json-array / ndjson / gzip / bad-line / oversize / content-type sniffing;
   the event schema preserving unknown fields through `.passthrough()`; each
   filter predicate plus "absent means match-all"; Loki label sanitization,
   empty-value dropping, and high-cardinality warnings; push-body construction
   (nanosecond conversion, stream grouping, ascending sort); file path
   derivation from event timestamp including a midnight-straddling batch; sink
   name validation against traversal attempts; config redaction.
2. **Spool queue against a real temporary directory**: FIFO ordering across
   more than 1000 files (which is what proves the zero-padding width), stray
   `.tmp` files ignored and then cleaned at boot, sequence recovery, byte
   accounting, drop-oldest on overflow, acknowledgment removing exactly the
   coalesced set, dead-letter moves.
3. **Sink integration**: the file sink writing real files, appending across
   dates, retention deleting only pattern-matching names, and low free space
   throwing a retryable error via an injected `statfs`. The Loki sink against a
   stub HTTP server asserting the exact request body, then the full error
   matrix — 200 / 400 / 401 / 429 / 500 / timeout / `ECONNREFUSED` — mapped to
   success, permanent, and retryable-with-failed-health.
4. **Route tests** through `app.request()`, requiring no real socket:
   signature and drain-state outcomes, gzip bodies, malformed lines counted but
   not fatal, and the proxy-auth matrix — untrusted peer, spoofed user header,
   `AUTH_MODE` unset returning 503, allowlist rejection.
5. **One end-to-end durability test**, which validates the central design
   claim: run the dispatcher against a Loki stub that refuses connections, POST
   several batches, assert `200`s and files on disk; destroy the dispatcher
   without draining, simulating a crash; start a fresh dispatcher with the stub
   now accepting; assert every event arrives exactly once and the spool empties.
6. **Docker smoke test** in CI: build the image, run it with tmpfs mounts and
   `AUTH_MODE=disabled`, POST a correctly-signed batch, and assert the file
   sink wrote the expected line and `/healthz` returns 200. This catches what
   unit tests structurally cannot — a wrong `CMD`, a dev dependency needed at
   runtime, volume permissions.

**Tooling.** oxlint with the typescript, node, promise, unicorn, and react
plugins, run with `--type-aware` so the type-dependent rules
(`no-floating-promises`, `no-unsafe-type-assertion`, `require-await`) are
available. Two rules carry project policy: `typescript/no-explicit-any` bans
`any`, and `node/no-sync` bans every `*Sync` call — verified to catch both
`fs.readFileSync(p)` and a directly imported `readFileSync(p)`, and to extend
to `zlib.gunzipSync`, so the async-over-sync rule is enforced in CI rather than
in review. `react/react-in-jsx-scope` is disabled because the project uses the
modern JSX transform. Prettier at two-space indentation with semicolons.
`tsc --noEmit` over both projects with `strict`, `noUncheckedIndexedAccess`,
and `exactOptionalPropertyTypes`. GitHub Actions runs lint → typecheck → test →
build → docker smoke. No image is published.

There is no lint rule banning `unknown`; oxlint has no such rule and neither
does ESLint. That prohibition is upheld by design — `JsonValue` exists so no
module needs `unknown` for data — and by review, with the two parse boundaries
in `vercel/decode.ts` and the one opaque display payload in `types/api.ts` as
the documented exceptions.

Implementation is test-driven.

**Negative assertions pin a single reason.** A test named for a specific
rejection uses an otherwise-valid fixture and asserts the issue count and
path, rather than only that validation failed. Three tests written during
implementation used doubly-invalid fixtures and so would have passed with the
very rule they named deleted; they were found by counting validator issues per
fixture rather than by reading the tests.

**Deliberate omission:** no component tests for the SPA in v1. It is forms over
a typed API, and the risk concentrates in the API and the queue. This is a
choice, not an oversight.

## 13. Decisions and rejected alternatives

| Decision | Alternatives rejected | Reason |
|---|---|---|
| Per-sink spool directories, delete-on-success | Shared WAL with per-sink cursors; SQLite queue | The WAL design needs cursor durability, min-cursor GC, and torn-write recovery — roughly double the queue code — and pays off only at volumes this will not see. `better-sqlite3` and `node:sqlite` are both synchronous, so every enqueue would block the loop serving the drain endpoint. Write amplification across two or three sinks is irrelevant here. |
| Disk-backed durability | In-memory queue; fire-and-forget; synchronous ack after all sinks | Vercel retries only a few times, so a restart or a long Loki outage would lose data in every other option. |
| Delegated proxy auth, fail-closed | Built-in admin password; OIDC | Operator's choice. Fail-closed defaults plus socket-peer CIDR checks mitigate the "deployed bare" risk that delegation normally carries. |
| Date-partitioned files with day retention | Size rotation with a file cap; no rotation | Operator's choice, for grep-by-date ergonomics. The unbounded-size risk is mitigated by the separate logs volume plus the free-space floor, which converts disk exhaustion into backpressure rather than loss. |
| Named sinks with per-sink filters | One sink per type; no filtering | Enables "errors to Loki, everything to file" without a second deployment. |
| Configurable label allowlist with safe defaults | Fixed labels; free-form templates | Avoids a templating layer while still allowing a needed label such as `branch`, with warnings on the cardinality footgun. |
| At-least-once delivery | Deduplication cache | Duplicate logs are recoverable; lost logs are not. Loki collapses identical entries anyway. |
| File and Loki sinks only | Adding stdout or a generic HTTP webhook | Keeps the v1 test surface small; the registry makes a third type a single new module. |

## 14. Out of scope for v1

- A third sink type (stdout, generic HTTP webhook, S3).
- Log search or a browsing UI.
- Multi-replica operation.
- Persisted historical metrics or a Prometheus endpoint.
- Automatic recovery from a corrupt `config.json`.
- Watching `config.json` for external edits while running.
- SPA component tests.
