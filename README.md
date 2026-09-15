# vercel-log-drain

## What it is

`vercel-log-drain` receives Vercel Log Drain deliveries over HTTP, verifies
each delivery's signature, and durably spools every accepted event to disk —
per sink — before forwarding it on to a local file and/or a Loki push
endpoint. It ships as a Docker image; that image is the only supported way to
run it as a service. (There is also a `npm run build && npm start` path, but
that's for development — see Development below.)

## Quick start

```
docker build -t vercel-log-drain .
docker run -d --name vercel-log-drain -p 8080:8080 \
  -e AUTH_MODE=disabled \
  -v vld-config:/config -v vld-spool:/spool -v vld-logs:/logs \
  vercel-log-drain
```

`AUTH_MODE=disabled` leaves the admin API and UI open with **no**
authentication. It exists for exactly this kind of local evaluation and for
development — never run it this way on a host or network you don't already
trust. See Authentication below for the modes meant for a real deployment.

Open <http://localhost:8080>. On the **Drains** tab, create a drain and copy
its id and secret — the secret is shown exactly once (see Secrets below). In
Vercel, create a Log Drain pointing at `https://<your-host>/api/drain/<drainId>`
and paste in that secret. Back in the UI, on the **Sinks** tab, add a file
and/or Loki sink and enable it.

For a fuller example — the service behind a reverse proxy with SSO, plus Loki
and Grafana — see `docker-compose.example.yml`. **As shipped, that example
cannot be used to reach the admin UI at all**, for two separate reasons:

1. `examples/Caddyfile` ships with a placeholder password hash
   (`$2a$14$REPLACE_WITH_YOUR_OWN_BCRYPT_HASH`) that cannot authenticate
   anyone. Generate a real one and paste it in:

   ```
   docker run --rm caddy:2-alpine caddy hash-password --plaintext '<a password>'
   ```

2. `AUTH_MODE=proxy` is the mode to deploy with. The `docker run` line above
   uses `AUTH_MODE=disabled` only because it has no proxy in front of it;
   that mode leaves the admin API open to anyone who can reach the port, so
   use it for a local look and nothing else.

## Configuring the Vercel side

In your Vercel project, add a Log Drain pointing at:

```
https://<your-host>/api/drain/<drainId>
```

using the drain id and secret created in the admin UI (or via
`POST /api/admin/drains`). Vercel signs each delivery with HMAC-SHA1 over the
raw request body, keyed by that secret, and sends the signature in the
`x-vercel-signature` header; the service verifies it before doing anything
else with the body — before decompression, before JSON parsing.

Both of Vercel's delivery encodings work, with or without gzip: a JSON array
of events, or newline-delimited JSON (one event per line). The service tells
them apart by looking at the first non-whitespace byte of the (decompressed)
body, not by a header.

## Authentication

| Variable               | Required when                    | Purpose                                                                                                                                               |
| ---------------------- | -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `AUTH_MODE`            | always — **no default**          | `proxy` or `disabled`. Unset closes the admin surface with `503`; `disabled` opens it with no authentication at all.                                  |
| `AUTH_TRUSTED_PROXIES` | `AUTH_MODE=proxy`                | Comma-separated CIDR list (IPv4 and/or IPv6) of your reverse proxy's own address(es).                                                                 |
| `AUTH_USER_HEADER`     | `AUTH_MODE=proxy`                | The header name your proxy sets with the authenticated user's identity (e.g. `X-Forwarded-User`).                                                     |
| `AUTH_ALLOWED_USERS`   | optional, `AUTH_MODE=proxy` only | Comma-separated allowlist of identities. If set, only these are admitted; if set but blank, boot fails rather than being treated as "no restriction". |

Two things worth stating plainly, because they surprise people:

- **`AUTH_MODE` has no default.** Leaving it unset does not open the admin
  surface — it makes the admin API, `/api/status`, and the SPA all return
  `503`. It also does **not** affect ingest: `/api/drain/*`, `/healthz`, and
  `/readyz` keep working with `AUTH_MODE` unset, because a deployment must
  never lose Vercel deliveries to an auth misconfiguration. `disabled` is for
  local development only — it removes authentication entirely.
- **Never put your reverse proxy's SSO in front of `/api/drain/*`.** Vercel
  authenticates a delivery by HMAC signature (see above); it cannot log in
  through a proxy, present an SSO identity, or forward a cookie. If your
  proxy puts the drain route behind the same login as the admin UI, every
  delivery from Vercel fails and you lose logs — this is the single most
  consequential mistake available when deploying this service. Both example
  configs (`examples/Caddyfile`, `examples/nginx.conf`) carve `/api/drain/*`
  (and `/healthz`, `/readyz`) out of the authenticated block for exactly this
  reason; copy that split, don't remove it.

Trust in `proxy` mode is based **only** on the TCP peer address of the socket
that reached the process, never on a header — so `AUTH_TRUSTED_PROXIES` must
name your proxy's real address, not anything a client could claim to be.

`AUTH_MODE`, `AUTH_TRUSTED_PROXIES`, `AUTH_USER_HEADER`, `RETRY_BASE_MS`, and
`RETRY_MAX_MS` are all validated once at boot, before the service opens a
socket or touches a volume. A malformed value throws, which under Docker's
restart policy becomes a crash loop rather than a service that limps along on
a guessed default. If a container is crash-looping, `docker logs` will show
the boot error, and that error names the exact variable and value it
rejected — that's the first place to look.

### Why the identity header is stripped per-route, not globally

Worth knowing before changing `buildApp`, because the obvious simplification
breaks authentication completely.

An inbound copy of `AUTH_USER_HEADER` must never reach a handler, so that a
client cannot assert its own identity. Two things enforce that: `proxyAuth`
strips the header itself on every route it guards, and `buildApp` mounts a
strip on the routes the guard does _not_ cover — `/healthz`, `/readyz` and
`/api/drain/*`.

Mounting that strip globally with `app.use('*', ...)` looks safer and is
catastrophic: `proxyAuth` **reads** that header to establish identity, so a
global strip registered ahead of the route table deletes it first and every
admin request is refused with `403 forbidden: AUTH_USER_HEADER was not
supplied by the proxy` — on a header the proxy did set. That was a real
defect in this codebase, and it survived 355 passing tests because no
in-process test can reach it: under Hono's `app.request()` there is no
socket, so the peer resolver returns `undefined` and a request is refused at
the peer check before the header is ever read. The regression test for it
uses a real listener for exactly that reason.

If you add a route the guard does not cover, add it to that strip list.

## Volumes

Three separate mounts, each with its own free-space floor, and they can live
on different physical devices:

| Path      | Env var      | Default   | Holds                                                                                                                                                     |
| --------- | ------------ | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `/config` | `CONFIG_DIR` | `/config` | `config.json` (plus a `.bak` copy) — drains, sinks, and server settings. Drain secrets live here in plaintext.                                            |
| `/spool`  | `SPOOL_DIR`  | `/spool`  | One subdirectory per sink, holding events accepted but not yet delivered, plus a `dead/` folder per sink for permanently rejected batches.                |
| `/logs`   | `LOGS_ROOT`  | `/logs`   | Output of file sinks — date-partitioned `.jsonl` files. A file sink's `directory` must resolve inside this root; the service refuses to write outside it. |

They are kept separate on purpose. A full **logs** volume makes a file
sink's write fail, but that failure is backpressure, not loss: the batch
stays in `/spool` and is retried, `/api/drain/*` keeps accepting new
deliveries, and nothing is dropped until space is freed or the sink's
`freeSpaceFloorBytes` is raised. A full **spool** volume is different: once
free space on it drops below the server-wide floor
(`spoolFreeSpaceFloorBytes`, default 256 MiB), newly enqueued events for a
sink are dropped outright rather than written, and a per-sink `maxSpoolBytes`
budget independently evicts the _oldest_ on-disk batches to make room for new
ones. Collapsing all three mounts into one would tie a logs-volume outage
directly to ingest, which is exactly what keeping them apart avoids.

Dropping at the spool floor is the **one loss point in the design**, so it is
reported on every surface an operator might be looking at: `service.state`
becomes `degraded`, `/readyz` answers `503`, each drop appends an entry to
`recent.errors` naming the sink and the bytes free, and `counters.dropped`
climbs. Vercel is still answered `200` for a batch dropped this way, which is
what the design chose: Vercel retries only a few times, and each retry would
arrive at the same full volume, so the honest signal is local rather than a
`500` that loses the data a few seconds later anyway. That makes these
signals the only warning you get — treat a `degraded` service with a climbing
`dropped` count as data loss in progress, not as a slow sink.

On a Linux host, a **bind mount** of a pre-existing host directory (as
opposed to a named volume) does not carry the right ownership: the container
runs as uid/gid `10001`, and Docker does not chown a bind-mounted directory
into a container's user for you. Fix it on the host before starting the
container:

```
chown -R 10001:10001 /path/on/host
```

A _named_ volume needs no manual chown — Docker seeds it from the image's own
`/config`, `/spool`, `/logs`, which the Dockerfile already chowns to
`10001:10001`. Only a bind mount of existing host content needs the fix
above. Skip it and the container fails a startup write-check and crash-loops,
with the boot error naming the directory and this exact fix.

(This specific failure was not reproduced in this project — Docker Desktop
for macOS does not enforce Linux uid/gid semantics on bind mounts, so it
could not be. It's standard Linux behavior, documented here because it's the
kind of thing that only shows up once someone deploys to a real Linux host.)

## Sinks

Two sink types, configured on the **Sinks** tab or via
`PUT /api/admin/config`. Every sink has its own spool directory under
`/spool/<name>`, its own `maxSpoolBytes` budget, its own
`maxBatchEvents`/`maxBatchBytes` (how much one delivery attempt carries), and
an optional `filter` (minimum level, allowed sources/environments/project
ids).

### File sink

Writes date-partitioned JSONL: `<directory>/<filePrefix>-YYYY-MM-DD.jsonl`,
one line per event, grouped by the event's own UTC date rather than arrival
time — a batch delayed by an outage still lands in the file for the date it
actually happened. `retentionDays` (`0` disables pruning; the UI defaults new
sinks to `14`) deletes files whose _name_ and _last-modified time_ are both
older than the window, checked hourly; a file still being written, or one
that only just arrived via a delayed retry, is never deleted purely because
its name looks old. `freeSpaceFloorBytes` (UI default 256 MiB) is the point
below which a delivery attempt fails rather than writes — see Volumes above
for what that means for `/logs` specifically.

### Loki sink

Pushes to `<url>` (an implicit `/loki/api/v1/push` is appended if not already
present), gzip-compressed, with an optional `X-Scope-OrgID` tenant header.
Three auth modes: `none`, `basic` (username/password), and `bearer` (token).
Labels come from two places:

- `labels.static` — fixed key/value labels applied to every stream (UI
  default: `job: vercel`).
- `labels.fromFields` — event fields promoted to labels (UI default:
  `projectName`, `environment`, `source`, `level`).

**Cardinality warning:** every distinct combination of label values becomes a
new Loki stream. Promoting a high-cardinality field — `id`, `requestId`,
`deploymentId`, `path`, `host`, `traceId`, `spanId`, `buildId`, `trace.id`,
`span.id` — to a label fans out streams without bound. The admin API and the
Sinks UI both warn when `fromFields` includes one of these, but neither
blocks saving it. Query those fields from the log line with LogQL's `| json`
instead of turning them into labels.

## Secrets

Three fields are scrubbed from every API read:

- A drain's `secret` — `GET`/`PUT /api/admin/config` always return
  `secret: null` with a boolean `hasSecret` flag alongside it. **The real
  secret is returned exactly once: in the response to
  `POST /api/admin/drains`, at creation.** There is no way to recover it
  afterward. If it isn't copied into Vercel at that moment, the drain has to
  be rotated — there is no in-place secret rotation, only deleting the drain
  and creating a new one.
- A Loki sink's `auth.password` (basic auth).
- A Loki sink's `auth.token` (bearer auth).

**`labels.static` on a Loki sink is _not_ scrubbed.** Whatever is pasted
there comes back on every `GET /api/admin/config` and is shipped to Loki as a
literal label value on every log line it applies to. Don't put a credential,
token, or anything else sensitive in a static label — use the sink's own
`auth` fields instead.

## Delivery semantics

Delivery is at-least-once, both from Vercel into this service and from this
service out to each sink. The drain route only acknowledges Vercel — with a
`200` — after the batch has been durably written to `/spool`; if that write
fails, the route answers `500`, and Vercel retries the same delivery. That
retry can duplicate events into a sink that already received and delivered
the earlier attempt. A consumer of these events needs to tolerate duplicates:
Loki collapses identical entries (same labels, same timestamp, same line)
into one, but the file sink writes exactly what it's given and does not
deduplicate.

## Operations

The **Status** tab (and `GET /api/status`) is a read-only snapshot: overall
service state (`ok`/`degraded`), free/total bytes for the `/config` and
`/spool` volumes, per-drain request and event counters, per-sink health and
queue depth, any orphaned spools, and a bounded rolling window of recent
events, rejects, and errors. None of it is persisted — **every counter resets
to zero on restart** — so it reflects the current process's lifetime only,
not history.

- **Head age** (a sink's `queue.oldestAgeSec`) is the age of the oldest batch
  still waiting in that sink's spool. A small, steady number is normal — a
  batch arrives and is delivered within a second or two. A **climbing** head
  age means the sink isn't keeping up: check its `health.state` for errors,
  or consider whether `maxBatchEvents`/`maxBatchBytes` are too small for the
  incoming rate.
- Sink health has three states: `ok`, `retrying` (fewer than five consecutive
  failures), and `failed` (five or more consecutive failures, or a single
  authentication failure — a `401`/`403`/`404` from Loki escalates straight
  to `failed` rather than waiting out several backoff rounds to say so).
  `/readyz` answers `503` while any _enabled_ sink is `failed`, while the
  spool volume is below its free-space floor (events are being dropped), or
  while **no** sink is enabled at all — a service with nowhere to put a
  delivery is not ready, even though ingest still answers `200`. Retry backoff
  doubles from `RETRY_BASE_MS` (default `1000`) up to `RETRY_MAX_MS` (default
  `60000`), plus up to 30% jitter — so the real ceiling on any single wait is
  about 1.3× `RETRY_MAX_MS`, not `RETRY_MAX_MS` itself.
- **Dead-lettered** means a batch failed in a way that retrying it can never
  fix, so it was moved out of the live queue into `/spool/<sink>/dead/`
  rather than blocking everything behind it. For a Loki sink that means
  exactly one response: `400`, which Loki returns for a malformed stream and
  for entries older than its `reject_old_samples_max_age`. Everything else,
  including `413` (payload too large) and `422`, is **retried** and left on
  disk, because those are fixable from the config and destroying the logs
  would be the wrong answer to a wrong setting — health goes straight to
  `failed` so you are told at once. Nothing deletes a dead letter
  automatically; find and clear them by hand.
- An **orphaned spool** is a directory under `/spool` for a sink name that no
  longer appears in the current config — typically left behind after
  renaming or deleting a sink. It is never removed automatically, since it
  may still hold undelivered data. The Status page lists it with its file
  count and byte size; `DELETE /api/admin/orphans/<name>` removes it for
  good.
- A **disabled** sink keeps its spool, and `/api/status` reports that spool's
  real file count and byte size — a disabled sink is not an orphan (it is
  still configured, and a one-click discard must not be offered for it), but
  its backlog is still on the volume and still counts against the spool
  free-space floor, so it is reported rather than shown as zero.
- **Dead-letter size** is reported per sink and per orphan as `dead.files` /
  `dead.bytes`, separately from `queue.*`. It is deliberately outside
  `maxSpoolBytes`: `dead/` is terminal storage, and the budget reclaims space
  by deleting, which is the one thing nothing may do to a dead letter. So it
  only ever grows until you clear it by hand — watch it, because a large
  `dead/` is what takes the whole spool volume below its floor, at which
  point new events are dropped.

## Troubleshooting

| Symptom                                                                                                                                | Cause                                                                                                                                                                                              | Fix                                                                                                                          |
| -------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Vercel reports delivery failures; drain answers `403 invalid_signature`                                                                | The secret configured in Vercel doesn't match the drain's                                                                                                                                          | Re-copy the secret from drain creation, or rotate: delete the drain and create a new one                                     |
| Vercel reports delivery failures; drain answers `404`                                                                                  | The drain id in the Vercel destination URL doesn't match any configured drain                                                                                                                      | Check the id in the URL against the **Drains** tab                                                                           |
| Admin UI / `/api/status` answers `503`                                                                                                 | `AUTH_MODE` is unset                                                                                                                                                                               | Set `AUTH_MODE=proxy` (with `AUTH_TRUSTED_PROXIES`/`AUTH_USER_HEADER`), or `AUTH_MODE=disabled` for local development        |
| Admin UI answers `403 forbidden: request did not arrive from a trusted proxy`                                                          | The proxy's real peer address isn't covered by `AUTH_TRUSTED_PROXIES`                                                                                                                              | Correct or widen the CIDR list; trust is based on the TCP peer, never a header                                               |
| Admin UI answers `403 forbidden: AUTH_USER_HEADER was not supplied by the proxy`                                                       | The proxy is not setting the header named by `AUTH_USER_HEADER`, or is setting a different one                                                                                                     | Compare the header your proxy sets against `AUTH_USER_HEADER`; both are case-insensitive but the name must otherwise match   |
| Admin UI answers `403 forbidden: request did not arrive from a trusted proxy`                                                          | The socket peer is outside `AUTH_TRUSTED_PROXIES`. Trust is decided from the connecting address, never from `X-Forwarded-For`                                                                      | Set `AUTH_TRUSTED_PROXIES` to the CIDR the proxy actually connects from — on Docker that is the bridge network, not the host |
| A Loki sink's health is `failed`, last error mentions `401`/`403`/`404`                                                                | Bad Loki credentials, tenant, or URL                                                                                                                                                               | Fix the sink's `auth`/`url`/`tenantId` and save; the worker resumes on its own once the credential is valid                  |
| A Loki sink is dead-lettering batches, last error mentions `400`                                                                       | Batch timestamps are older than Loki's `reject_old_samples_max_age`, typically because the sink was down longer than that window                                                                   | Raise `reject_old_samples_max_age` on the Loki side, or accept that outages longer than it will dead-letter                  |
| A Loki sink's health is `failed`, last error mentions `413`                                                                            | Something in front of Loki has a body-size limit below the sink's `maxBatchBytes` (nginx defaults to `client_max_body_size 1m`; the sink defaults to 4 MiB)                                        | Lower the sink's `maxBatchBytes` or raise the proxy's limit; the batches are still on disk and delivery resumes on its own   |
| Container exits immediately, message mentions `chown`                                                                                  | A bind-mounted volume isn't owned by uid/gid `10001`                                                                                                                                               | `chown -R 10001:10001` the host directory (see Volumes)                                                                      |
| Container exits immediately, message mentions `EACCES` and a `/spool/<sink>/dead` path                                                 | That sink's dead-letter directory cannot be read, so batch sequence numbers cannot be kept monotonic across it — the service refuses to start rather than risk overwriting an already-failed batch | Fix the permissions on the spool volume (`chown -R 10001:10001`); nothing in `dead/` is lost by the restart                  |
| Container exits immediately, message names `AUTH_MODE`, `AUTH_TRUSTED_PROXIES`, `AUTH_USER_HEADER`, `RETRY_BASE_MS`, or `RETRY_MAX_MS` | That variable is malformed                                                                                                                                                                         | The boot error names the variable and the rejected value — fix it and restart                                                |

## Development

Requires Node.js 24+ (see `engines` in `package.json`).

```
npm ci
npm run lint        # oxlint, type-aware
npm run typecheck   # tsc for the server, then separately for the web project
npm test            # vitest run, includes test/e2e/durability.test.ts
npm run test:watch  # vitest in watch mode
npm run build       # server -> dist/src/index.js, web -> web/dist/index.html
npm run smoke       # builds the Docker image and exercises it end to end; needs Docker running
```

There is no `npm run dev`. To run the built service locally against local
directories instead of `/config`, `/spool`, `/logs`:

```
npm run build
mkdir -p .local/config .local/spool .local/logs
CONFIG_DIR=.local/config SPOOL_DIR=.local/spool LOGS_ROOT=.local/logs \
  WEB_ROOT=web/dist AUTH_MODE=disabled npm start
```

`AUTH_MODE=disabled` here has the same caveat as in Quick start: no
authentication at all, local use only.

## Design

The full design — including the alternatives that were considered and
rejected — is recorded in
[`docs/superpowers/specs/2026-09-08-vercel-log-drain-design.md`](docs/superpowers/specs/2026-09-08-vercel-log-drain-design.md).
