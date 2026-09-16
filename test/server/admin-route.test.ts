import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mkdir, mkdtemp, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Hono } from 'hono';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import { adminRoutes } from '../../src/server/routes/admin.js';
import { ConfigStore, EtagMismatchError } from '../../src/config/store.js';
import type { LoadedConfig } from '../../src/config/store.js';
import { Dispatcher } from '../../src/pipeline/dispatcher.js';
import { readSpoolDirStats, SpoolQueue } from '../../src/pipeline/spool.js';
import { Metrics } from '../../src/status/metrics.js';
import type { AppConfig, SinkEntry } from '../../src/config/schema.js';
import type { AppEnv } from '../../src/server/types.js';

const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));

/**
 * Reads a response body as `T`. This is an UNCHECKED cast, concentrated in one
 * place on purpose: the server tsconfig has no "dom" lib, so
 * `Response.json()` is typed `Promise<unknown>` and cannot be landed in a
 * typed binding without one. Prefer asserting directly --
 * `expect(await response.json()).toMatchObject({...})` takes `unknown` and
 * needs no cast. Reach for this only where a test genuinely has to read a
 * value out of the body: reuse it in a later request, filter a list, or
 * compare a number. Never launder the cast through
 * `JSON.parse(await response.text())`, which hides it behind `any`.
 */
async function jsonBody<T>(response: Response): Promise<T> {
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion
  return (await response.json()) as T;
}

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

  /**
   * Sends a literal, unencoded body -- unlike `put()`, which always
   * `JSON.stringify`s its argument. Malformed and empty-body cases need the
   * raw bytes on the wire, not a validly re-encoded string, so this is the
   * only way to exercise the `c.req.json()` parse failure itself rather than
   * the schema check one line after it.
   */
  async function putRaw(rawBody: string) {
    return app().request('/api/admin/config', {
      method: 'PUT',
      body: rawBody,
      headers: { 'content-type': 'application/json' },
    });
  }

  function postDrain(name: string) {
    return app().request('/api/admin/drains', {
      method: 'POST',
      body: JSON.stringify({ name }),
      headers: { 'content-type': 'application/json' },
    });
  }

  async function postDrainRaw(rawBody: string) {
    return app().request('/api/admin/drains', {
      method: 'POST',
      body: rawBody,
      headers: { 'content-type': 'application/json' },
    });
  }

  it('returns the redacted config with its etag', async () => {
    const response = await app().request('/api/admin/config');
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ etag, config: { drains: [] } });
  });

  it('creates a drain and reveals the secret exactly once', async () => {
    const response = await postDrain('prod');
    expect(response.status).toBe(201);

    const created = await jsonBody<{ id: string; secret: string }>(response);
    expect(created.secret.length).toBeGreaterThanOrEqual(16);

    const after = await jsonBody<{
      config: { drains: { id: string; secret: null; hasSecret: boolean }[] };
    }>(await app().request('/api/admin/config'));
    expect(after.config.drains[0]?.id).toBe(created.id);
    expect(after.config.drains[0]?.secret).toBeNull();
    expect(after.config.drains[0]?.hasSecret).toBe(true);
  });

  it('returns 400 for a malformed JSON drain-creation body instead of a 500', async () => {
    const response = await postDrainRaw('{nope');
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({ code: 'bad_request' });
  });

  it('returns 400 for an empty drain-creation body instead of a 500', async () => {
    const response = await postDrainRaw('');
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({ code: 'bad_request' });
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
    expect(await response.json()).toMatchObject({
      warnings: expect.arrayContaining([expect.stringContaining('requestId')]),
    });
  });

  it('redacts a real secret in the PUT response that just restored it', async () => {
    // Distinct from the GET-side check in "creates a drain and reveals the
    // secret exactly once": that test only exercises the redactConfig call
    // inside the GET /config handler. PUT /config redacts its own response
    // independently, and a config-carrying response passes if the secret
    // field is merely absent -- for any reason, including a typo'd field
    // name or a fixture that never had a secret. So this plants a real
    // password via restoreSecrets (the exact codepath that hands back real
    // secrets), confirms it landed in the persisted store, and only then
    // asserts it is absent from the HTTP response body -- on the literal
    // secret value, not merely a null check.
    const secretValue = 'super-secret-password-value';
    const lokiSink: SinkEntry = {
      ...fileSink('loki-secret'),
      config: {
        type: 'loki',
        url: 'http://loki:3100',
        auth: { kind: 'basic', username: 'operator', password: secretValue },
        tenantId: null,
        labels: { static: { job: 'vercel' }, fromFields: [] },
        timeoutMs: 5000,
      },
    };
    const response = await put({ config: { ...current, sinks: [lokiSink] }, etag });
    expect(response.status).toBe(200);

    const bodyText = await response.clone().text();
    expect(bodyText).not.toContain(secretValue);
    expect(await response.json()).toMatchObject({
      config: { sinks: [{ config: { auth: { password: '' } } }] },
    });

    // The secret really is there in the persisted store -- proving the
    // response redaction above is hiding a genuine credential, not asserting
    // against a fixture that never had one.
    const stored = await store.load();
    const storedSink = stored.config.sinks[0];
    expect(storedSink?.config.type).toBe('loki');
    expect(
      storedSink?.config.type === 'loki' && storedSink.config.auth.kind === 'basic'
        ? storedSink.config.auth.password
        : null,
    ).toBe(secretValue);
  });

  it('does not start two workers for one sink under overlapping PUTs', async () => {
    // Task 18 serialises reconciliation on a promise chain, but that
    // guarantee is not observable by inspecting settled state: `active` is
    // keyed by sink name, so it holds one entry whichever call won, and
    // `/api/status`/`snapshotSinks()` are both built from that same
    // name-keyed map, so a count of "sinks named racer" is 1 either way --
    // it cannot tell "serialised" from "raced and orphaned a worker".
    // Counting `SpoolQueue.open` calls is what actually distinguishes them
    // (see the identical reasoning and technique in
    // dispatcher-reconcile.test.ts's "opens one spool per sink under
    // overlapping applyConfig calls"). Without serialisation both requests
    // pass the active.has() check for the same new sink, both open a
    // SpoolQueue on one directory, and the second orphans the first worker,
    // which keeps draining that spool untracked.
    const next = { ...current, sinks: [fileSink('racer')] };
    const openSpy = vi.spyOn(SpoolQueue, 'open');
    try {
      const [first, second] = await Promise.all([
        put({ config: next, etag }),
        put({ config: next, etag }),
      ]);

      // One wins on the etag; the loser must not have half-applied anything.
      const codes = [first.status, second.status].toSorted((a, b) => a - b);
      expect(codes).toEqual([200, 409]);
      expect(openSpy).toHaveBeenCalledTimes(1);
    } finally {
      openSpy.mockRestore();
    }
  });

  it('a losing PUT that only touches an unrelated sink still churns it, but loses no data, and a losing rename leaves no phantom orphan', async () => {
    // Pins the three things issue #6 asked to confirm or fix, in one race
    // so the interleaving is the real one and not three separately staged
    // approximations of it:
    //
    // 1. A losing PUT that never named "unrelated" in its own diff still
    //    gets it torn down and reopened, because `Dispatcher.applyConfig`
    //    reconciles in arrival order, not etag-win order, and the rollback
    //    reconciles a second time. Pinned by counting `SpoolQueue.open`
    //    calls for "unrelated" specifically.
    // 2. Every event enqueued to "mover" before the race is still there
    //    afterward -- the rollback reopens the same spool directory rather
    //    than losing what was queued to it.
    // 3. The losing PUT renames "mover" to "mover2"; that briefly creates
    //    "mover2"'s spool directory, and the rollback undoes the rename
    //    without deleting the now-empty directory itself (reconcileNow
    //    deliberately leaves a removed sink's directory on disk). Without
    //    `pruneEmptyOrphans` in admin.ts, "mover2" would sit forever in
    //    `listOrphanedSpools` holding nothing. This asserts it is gone.
    const baseline = { ...current, sinks: [fileSink('unrelated'), fileSink('mover')] };
    const setupResponse = await put({ config: baseline, etag });
    expect(setupResponse.status).toBe(200);
    const raceEtag = etag;

    await dispatcher.enqueue([{ id: 'queued-1', timestamp: 1, source: 'lambda', projectId: 'p' }]);
    const moverDir = join(spoolRoot, 'mover');
    const beforeMover = await readSpoolDirStats(moverDir);
    expect(beforeMover.files).toBeGreaterThan(0);

    const openSpy = vi.spyOn(SpoolQueue, 'open');
    try {
      // Winner: edits only "unrelated". Loser: leaves "unrelated" alone but
      // renames "mover" -> "mover2".
      const winner = {
        ...current,
        sinks: [{ ...fileSink('unrelated'), maxBatchEvents: 42 }, fileSink('mover')],
      };
      const loser = {
        ...current,
        sinks: [fileSink('unrelated'), fileSink('mover2')],
      };

      const [first, second] = await Promise.all([
        put({ config: winner, etag: raceEtag }),
        put({ config: loser, etag: raceEtag }),
      ]);

      const codes = [first.status, second.status].toSorted((a, b) => a - b);
      expect(codes).toEqual([200, 409]);

      // Effect 1: "unrelated" was reopened more than once, though neither
      // PUT's own diff ever named it as the thing being changed.
      const unrelatedDir = join(spoolRoot, 'unrelated');
      const unrelatedOpens = openSpy.mock.calls.filter((call) => call[0] === unrelatedDir);
      expect(unrelatedOpens.length).toBeGreaterThan(1);

      // Effect 2 (survival, not a residual cost): the queued event is still
      // on disk under "mover" -- the rollback did not drop it.
      const afterMover = await readSpoolDirStats(moverDir);
      expect(afterMover.files).toBe(beforeMover.files);
      expect(afterMover.bytes).toBe(beforeMover.bytes);

      // Effect 3, fixed: the phantom "mover2" directory the losing rename
      // created does not linger as a permanent empty orphan.
      const orphans = await dispatcher.listOrphanedSpools();
      expect(orphans.find((orphan) => orphan.name === 'mover2')).toBeUndefined();
      const mover2Exists = await stat(join(spoolRoot, 'mover2')).then(
        () => true,
        () => false,
      );
      expect(mover2Exists).toBe(false);
    } finally {
      openSpy.mockRestore();
    }
  });

  it('returns 409 on a stale etag', async () => {
    // Captured before the first write, and reused verbatim for the second:
    // `etag` itself is mutated by `setConfig` as soon as the first `put()`
    // resolves, so passing the (shared, closed-over) `etag` variable to the
    // second call -- as opposed to this snapshot -- would silently send the
    // now-current value and could never exercise the conflict path.
    const staleEtag = etag;
    await put({ config: { ...current, sinks: [fileSink('one')] }, etag });
    const response = await put({
      config: { ...current, sinks: [fileSink('two')] },
      etag: staleEtag,
    });
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

  it('returns 400 for a malformed JSON body instead of a 500', async () => {
    // Issue #1: `c.req.json()` throws a SyntaxError from JSON.parse before
    // the schema check ever runs, and that error used to reach Hono's
    // default handler unhandled -- an unlogged 500. A truncated PUT from a
    // flaky browser connection should read as a bad request, not a server
    // fault, so this must match the shape the schema-failure path one line
    // below already uses.
    const response = await putRaw('{not json');
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({ code: 'bad_request' });
    expect(current.sinks).toHaveLength(0);
  });

  it('returns 400 for an empty PUT body instead of a 500', async () => {
    const response = await putRaw('');
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({ code: 'bad_request' });
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
        labels: { static: { job: 'vercel' }, fromFields: [] },
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

  it('returns 400 for a syntactically invalid orphan name', async () => {
    // Separate from the 404 above on purpose. Both guards -- the orphan-list
    // membership check and SINK_NAME_PATTERN in spoolDirFor -- reject a
    // traversal name, so while both answered 404 no test could tell which
    // one fired, and removing either left the other covering for it. Two
    // distinct status codes pin them individually.
    const response = await app().request('/api/admin/orphans/Not_A_Valid_Name', {
      method: 'DELETE',
    });
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({ code: 'bad_request' });
  });

  it('rejects a path-traversal name instead of touching anything outside the spool root', async () => {
    // `:name` is a single Hono path segment, but a client that percent-encodes
    // its slashes (`..%2F..%2Fetc`) gets them decoded back into literal `../`
    // by the time `c.req.param('name')` returns -- confirmed by probing Hono
    // directly, since the browser-level path-dot-segment normalisation that
    // would otherwise catch this does not apply to a raw `app.request()`
    // call. Sink names are a security boundary precisely because a name
    // becomes a directory under the spool root, so this must be rejected the
    // same as any other syntactically invalid name -- not 500, and certainly
    // not a successful delete of something outside spoolRoot. Expects 400
    // (InvalidSinkNameError from spoolDirFor's pattern check), not 404: the
    // canary-survival assertion is the important half and is unchanged.
    const target = join(spoolRoot, '..', 'traversal-canary');
    await mkdir(target, { recursive: true });
    try {
      const response = await app().request('/api/admin/orphans/..%2Ftraversal-canary', {
        method: 'DELETE',
      });
      expect(response.status).toBe(400);
      const survived = await stat(target).then(
        () => true,
        () => false,
      );
      expect(survived).toBe(true);
    } finally {
      await rm(target, { recursive: true, force: true });
    }
  });

  it('answers 507 only when the config volume is full, and 500 otherwise', async () => {
    // The spec reserves 507 for one case, a full config volume. Sending it
    // for every save failure sends an operator to free disk space that was
    // never the problem, so the two are pinned apart here.
    // `app()` reads `store` when it is called, so swapping the binding is
    // enough to inject a failure -- no change to the route's dependencies.
    class FailingStore extends ConfigStore {
      constructor(private readonly failure: Error) {
        super(configDir);
      }
      override save(): Promise<LoadedConfig> {
        return Promise.reject(this.failure);
      }
    }

    const real = store;
    try {
      store = new FailingStore(
        Object.assign(new Error('no space left on device'), { code: 'ENOSPC' }),
      );
      expect((await put({ config: current, etag })).status).toBe(507);

      store = new FailingStore(new Error('permission denied'));
      expect((await put({ config: current, etag })).status).toBe(500);
    } finally {
      store = real;
    }
  });

  it('returns 409 on a stale etag when creating a drain', async () => {
    // Unlike PUT /config, POST /drains never takes an etag in its request
    // body -- it always reads the live `deps.getEtag()` -- so a client can
    // never send a stale one. The conflict path is still reachable, though:
    // `config.json` can change on disk between boot and this request (a
    // second process, a manual edit), which is exactly what a rejected
    // `EtagMismatchError` from `store.save` models. Same store-swapping
    // technique as the 507-vs-500 test below.
    class FailingStore extends ConfigStore {
      constructor() {
        super(configDir);
      }
      override save(): Promise<LoadedConfig> {
        return Promise.reject(new EtagMismatchError('etag mismatch'));
      }
    }

    const real = store;
    try {
      store = new FailingStore();
      const response = await postDrain('prod');
      expect(response.status).toBe(409);
    } finally {
      store = real;
    }
  });

  it('answers 507 only when the config volume is full, and 500 otherwise, when creating a drain', async () => {
    class FailingStore extends ConfigStore {
      constructor(private readonly failure: Error) {
        super(configDir);
      }
      override save(): Promise<LoadedConfig> {
        return Promise.reject(this.failure);
      }
    }

    const real = store;
    try {
      store = new FailingStore(
        Object.assign(new Error('no space left on device'), { code: 'ENOSPC' }),
      );
      expect((await postDrain('prod')).status).toBe(507);

      store = new FailingStore(new Error('permission denied'));
      expect((await postDrain('prod')).status).toBe(500);
    } finally {
      store = real;
    }
  });
});
