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
import type { LogEvent } from '../../src/vercel/event.js';

const gzipAsync = promisify(gzip);
const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));
const SECRET = 's'.repeat(32);

function event(id: string, level = 'info') {
  return { id, timestamp: 1573817187330, source: 'lambda', projectId: 'p1', level };
}

function sign(body: string | Buffer): string {
  return createHmac('sha1', SECRET).update(body).digest('hex');
}

function noop(): void {
  // Placeholder default, overwritten synchronously by the Promise executor
  // below before it is ever called.
}

/**
 * Detects whether `promise` has already settled, without consuming it.
 * Racing it against `Promise.resolve(marker)` is NOT reliable: `.then()`
 * adds an extra microtask hop, so the marker can win even when the promise
 * settled long ago. A macrotask-based marker is reliable instead -- the
 * microtask queue, including any settled promise's `.then()` chain however
 * many hops deep, always fully drains before Node runs a `setImmediate`
 * callback.
 */
function settledYet(promise: Promise<unknown>): Promise<boolean> {
  const pendingMarker = Symbol('pending');
  return Promise.race([
    promise.then(() => 'settled' as const),
    new Promise<typeof pendingMarker>((resolve) => setImmediate(() => resolve(pendingMarker))),
  ]).then((value) => value !== pendingMarker);
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

  // Every rejection path must leave the spool untouched. A status code alone
  // does not show that: a 404 that spooled the batch anyway would still be a
  // 404, and a rejection test passes when the request fails for any reason.
  async function spooledFiles(): Promise<number | undefined> {
    const statuses = await dispatcher.snapshotSinks();
    return statuses.find((sink) => sink.name === 'local')?.queue.files;
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
    const response = await post('/api/drain/drain1', body, {
      'x-vercel-signature': 'f'.repeat(40),
    });

    expect(response.status).toBe(403);
    expect((await dispatcher.snapshotSinks())[0]?.queue.files).toBe(0);
    const drain = metrics.snapshot().drains.find((d) => d.id === 'drain1');
    expect(drain?.requests.badSignature).toBe(1);
  });

  it('rejects a missing signature with 401', async () => {
    const body = JSON.stringify([event('a')]);
    expect((await post('/api/drain/drain1', body)).status).toBe(401);
    expect(await spooledFiles()).toBe(0);
  });

  it('returns 413 from the declared content-length, before reading the body', async () => {
    // The other 413 test sends an oversized body, so it only ever exercises
    // the raw.byteLength fallback -- which runs after the whole body has been
    // buffered. The pre-check exists precisely to reject without buffering,
    // and was untested. A small body here means the fallback cannot fire, so
    // deleting the pre-check turns this into a 200.
    const body = JSON.stringify([event('a')]);
    const response = await post('/api/drain/drain1', body, {
      'x-vercel-signature': sign(body),
      'content-length': String(config.server.maxBodyBytes + 1),
    });
    expect(response.status).toBe(413);
    // Not `const json: { code: string } = await response.json();` as in the
    // brief: in this project's fetch typings, Response.json() returns
    // Promise<unknown>, not Promise<any>, so that direct assignment does not
    // typecheck -- and an assertion (`as { code: string }`) is banned by
    // oxlint's no-unsafe-type-assertion. toMatchObject accepts `unknown`
    // and asserts the same thing.
    expect(await response.json()).toMatchObject({ code: 'payload_too_large' });
    expect(await spooledFiles()).toBe(0);
  });

  it('returns 404 for an unknown drain', async () => {
    const body = JSON.stringify([event('a')]);
    const response = await post('/api/drain/nope', body, { 'x-vercel-signature': sign(body) });
    expect(response.status).toBe(404);
    expect(await spooledFiles()).toBe(0);
  });

  it('returns 403 for a disabled drain', async () => {
    config = {
      ...config,
      drains: [{ ...config.drains[0]!, enabled: false }],
    };
    const body = JSON.stringify([event('a')]);
    const response = await post('/api/drain/drain1', body, { 'x-vercel-signature': sign(body) });
    expect(response.status).toBe(403);
    expect(await spooledFiles()).toBe(0);
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

  it('returns 200 with rejected: 1 for a corrupt gzip body, not a thrown error', async () => {
    // decodeBody's contract: corruption is reported as a WHOLE_BODY_INDEX
    // reject, not a thrown PayloadTooLargeError. Only the size cap throws.
    // A second catch here that treated corruption as a hard failure was a
    // real defect in an earlier task.
    const corrupt = Buffer.from('this is not gzip data at all, just plain bytes', 'utf8');
    const response = await post('/api/drain/drain1', corrupt, {
      'x-vercel-signature': sign(corrupt),
      'content-encoding': 'gzip',
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ received: 1, accepted: 0, rejected: 1 });
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

  it('does not create per-id metrics state for an unknown drain id', async () => {
    // recordUnknownDrainRequest() is a single aggregate counter precisely
    // because a per-id map entry let an attacker grow memory without bound
    // by posting to random ids. Assert the aggregate moved and that no
    // per-drain entry was created for the unknown id.
    const body = JSON.stringify([event('a')]);
    await post('/api/drain/totally-made-up-id', body, { 'x-vercel-signature': sign(body) });

    const snapshot = metrics.snapshot();
    expect(snapshot.unknownDrainRequests).toBe(1);
    expect(snapshot.drains.find((d) => d.id === 'totally-made-up-id')).toBeUndefined();
  });

  it('acknowledges Vercel only after enqueue() has resolved', async () => {
    // The discriminating shape: a test that posts and then checks the spool
    // passes whether or not the ack races the write, because by the time we
    // look, enqueue has almost certainly already finished. To actually catch
    // an inverted ordering, we make enqueue observably slow (gated on a
    // promise we control) and assert the HTTP response has not been
    // produced while it is still pending -- then release the gate and
    // confirm the response only appears once enqueue has truly resolved.
    const realEnqueue = dispatcher.enqueue.bind(dispatcher);
    let releaseGate: () => void = noop;
    const gate = new Promise<void>((resolve) => {
      releaseGate = resolve;
    });
    let enqueueResolved = false;
    dispatcher.enqueue = async (events: LogEvent[]) => {
      await gate;
      await realEnqueue(events);
      enqueueResolved = true;
    };

    const body = JSON.stringify([event('a')]);
    const responsePromise = post('/api/drain/drain1', body, { 'x-vercel-signature': sign(body) });

    // Let the request pipeline run as far as it can: signature check, body
    // decode, everything up to the enqueue call, which is now blocked on
    // the gate.
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(enqueueResolved).toBe(false);
    expect(await settledYet(responsePromise)).toBe(false);

    releaseGate();
    const response = await responsePromise;
    expect(enqueueResolved).toBe(true);
    expect(response.status).toBe(200);
  });
});
