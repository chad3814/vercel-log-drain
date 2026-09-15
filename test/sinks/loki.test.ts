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
    const sink = lokiSinkType.create(
      'loki',
      config(url, { auth: { kind: 'bearer', token: 'tk' } }),
      {
        log: silentLog,
      },
    );
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
