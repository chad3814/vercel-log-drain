import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { serve } from '@hono/node-server';
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

  async function bootWith(
    env: Record<string, string | undefined>,
    webRoot: string | null = dirs.web,
  ) {
    return boot({
      env: {
        CONFIG_DIR: dirs.config,
        SPOOL_DIR: dirs.spool,
        LOGS_ROOT: dirs.logs,
        LOG_LEVEL: 'silent',
        ...env,
      },
      webRoot,
    });
  }

  const IDENTITY = 'x-forwarded-user';

  /**
   * Boots, then registers a probe on each route the app-wide strip is
   * mounted on, and reports what each probe saw in the identity header.
   *
   * Two things make this observable at all. The probes are registered after
   * `buildApp` has run, so they come after its `app.use()` calls in
   * registration order and the strip composes ahead of them -- and each uses
   * a method the real route for that path does not (`healthRoutes` registers
   * GET /healthz and GET /readyz; `drainRoutes` registers POST
   * /api/drain/:drainId), so no real handler answers first. `webRoot` is
   * null so the `/api/*` 404 catch-all is not registered either.
   *
   * No production handler reports its own request headers, which is why the
   * pre-existing test for this block could not fail: it asserted a drain
   * request's status code, which is the same with the strip deleted.
   */
  async function probeIdentityStrip(env: Record<string, string | undefined>) {
    const booted = await bootWith(env, null);
    booted.app.post('/healthz', (c) => c.json({ identity: c.req.header(IDENTITY) ?? null }));
    booted.app.post('/readyz', (c) => c.json({ identity: c.req.header(IDENTITY) ?? null }));
    booted.app.get('/api/drain/probe', (c) => c.json({ identity: c.req.header(IDENTITY) ?? null }));

    const seen: Record<string, unknown> = {};
    try {
      for (const [path, method] of [
        ['/healthz', 'POST'],
        ['/readyz', 'POST'],
        ['/api/drain/probe', 'GET'],
      ] as const) {
        const response = await booted.app.request(path, {
          method,
          headers: { [IDENTITY]: 'attacker@example.com' },
        });
        expect(response.status).toBe(200);
        seen[path] = await response.json();
      }
    } finally {
      await booted.shutdown();
    }
    return seen;
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
    // Boots in proxy mode and drives the auth-exempt drain route with the
    // identity header present. This does NOT observe the strip -- the drain
    // handler never reports its request headers -- and it is not trying to:
    // what it pins is that mounting the strip ahead of the route table
    // cannot break ingest, which is the risk of putting anything at that
    // position. That the strip actually removes the header is pinned
    // behaviourally by the two probe tests above, which is what this test
    // used to concede it could not do.
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

  it('strips an inbound identity header on the routes the guard does not cover', async () => {
    // Spec §9's second layer: "the app mounts stripIdentityHeader ahead of
    // the route table so the guarantee also holds on the auth-exempt drain
    // path". Deleting that whole block left 356/356 tests green, and it
    // matters concretely -- examples/Caddyfile proxies /api/drain/* WITHOUT
    // clearing an inbound X-Forwarded-User (examples/nginx.conf does clear
    // it), so under the Caddy example this strip is the only thing removing
    // a client-supplied identity from a drain request.
    const seen = await probeIdentityStrip({
      AUTH_MODE: 'disabled',
      AUTH_USER_HEADER: IDENTITY,
    });

    expect(seen).toEqual({
      '/healthz': { identity: null },
      '/readyz': { identity: null },
      '/api/drain/probe': { identity: null },
    });
  });

  it('leaves the header alone when no identity header is configured', async () => {
    // The control the assertion above needs: it proves the probes CAN see an
    // inbound header, so `identity: null` there is the strip working rather
    // than a request that never carried the header or a probe that cannot
    // read one. Without this, the test above would also pass against an app
    // that dropped every header, or against a broken probe.
    const seen = await probeIdentityStrip({ AUTH_MODE: 'disabled' });

    expect(seen).toEqual({
      '/healthz': { identity: 'attacker@example.com' },
      '/readyz': { identity: 'attacker@example.com' },
      '/api/drain/probe': { identity: 'attacker@example.com' },
    });
  });

  it.each([
    ['', 'AUTH_MODE=disabled'],
    ['   ', 'AUTH_MODE=disabled'],
  ])('keeps healthz and ingest working with AUTH_USER_HEADER=%j under %s', async (headerName) => {
    // Measured before the fix: AUTH_MODE=disabled with an empty or
    // whitespace-only AUTH_USER_HEADER produced healthz 500 and drain 500,
    // because boot() read the variable raw and mounted the strip on both,
    // and Headers.delete('') throws. A compose file carrying
    // `AUTH_USER_HEADER: ${SOMEVAR}` with SOMEVAR missing therefore lost
    // every log AND failed the container HEALTHCHECK forever, while boot
    // logged success.
    const booted = await bootWith({ AUTH_MODE: 'disabled', AUTH_USER_HEADER: headerName });
    try {
      expect((await booted.app.request('/healthz')).status).toBe(200);
      expect((await booted.app.request('/readyz')).status).not.toBe(500);
      const drain = await booted.app.request('/api/drain/none', { method: 'POST', body: '[]' });
      // 404 for the unknown id: the route ran rather than throwing inside
      // the strip.
      expect(drain.status).toBe(404);
    } finally {
      await booted.shutdown();
    }
  });

  it('keeps healthz and ingest working with an empty AUTH_USER_HEADER and AUTH_MODE unset', async () => {
    const booted = await bootWith({ AUTH_USER_HEADER: '' });
    try {
      expect((await booted.app.request('/healthz')).status).toBe(200);
      const drain = await booted.app.request('/api/drain/none', { method: 'POST', body: '[]' });
      expect(drain.status).toBe(404);
    } finally {
      await booted.shutdown();
    }
  });

  it('fails the boot on a malformed AUTH_USER_HEADER even when AUTH_MODE is disabled', async () => {
    // The docs promise boot "fails fast (and loudly) on a malformed
    // AUTH_USER_HEADER"; that was true in proxy mode only. Whole message, so
    // this cannot pass on the reserved-header error instead.
    await expect(bootWith({ AUTH_MODE: 'disabled', AUTH_USER_HEADER: 'X User' })).rejects.toThrow(
      'AUTH_USER_HEADER is not a valid HTTP header name: "X User"',
    );
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

  it('authenticates an admin request in proxy mode over a real socket', async () => {
    // The ONLY test that exercises the auth feature in its production mode
    // end to end, and it needs a real listener: under app.request() there is
    // no socket, so nodePeerResolver returns undefined and the request is
    // refused at the peer check before the identity header is ever read.
    // That is why every other test in this file boots unset or disabled, and
    // why a global identity strip that broke proxy mode completely survived
    // two reviews.
    const booted = await bootWith({
      AUTH_MODE: 'proxy',
      AUTH_TRUSTED_PROXIES: '127.0.0.1/32',
      AUTH_USER_HEADER: 'x-forwarded-user',
    });
    const server = serve({ fetch: booted.app.fetch, hostname: '127.0.0.1', port: 0 });
    // serve() returns before the underlying listen() has actually bound a
    // port -- server.address() is null until the 'listening' event fires --
    // so this waits for it rather than reading the address immediately.
    await new Promise<void>((resolve) => server.once('listening', resolve));
    try {
      const address = server.address();
      if (address === null || typeof address === 'string') {
        throw new Error('expected the test server to bind a TCP port');
      }
      const base = `http://127.0.0.1:${String(address.port)}`;

      // A trusted peer presenting an identity reaches the admin API.
      const allowed = await fetch(`${base}/api/admin/config`, {
        headers: { 'x-forwarded-user': 'ada@example.com' },
      });
      expect(allowed.status).toBe(200);

      // The same peer without one is refused, so the 200 above is not merely
      // an unguarded route answering everybody.
      const refused = await fetch(`${base}/api/admin/config`);
      expect(refused.status).toBe(403);

      // And the drain route stays reachable, since it sits outside the guard:
      // the strip covering it must not have been dropped to fix the above.
      const drain = await fetch(`${base}/api/drain/none`, { method: 'POST', body: '[]' });
      expect(drain.status).toBe(404);
    } finally {
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
      await booted.shutdown();
    }
  });

  it('answers 404 for an unmatched API path instead of the SPA shell', async () => {
    // The SPA catch-all is registered last so client-side routes load the
    // shell, which means an /api path that matched no route would otherwise
    // come back 200 with HTML. A mistyped or withdrawn endpoint then looks
    // alive to a client and to monitoring.
    const booted = await bootWith({ AUTH_MODE: 'disabled' });
    try {
      const response = await booted.app.request('/api/unknown-endpoint');
      expect(response.status).toBe(404);
      // Assert on the body too: a 404 that still carried the shell would
      // mean the catch-all ran and merely relabelled the status.
      expect(await response.text()).not.toContain('<html');
    } finally {
      await booted.shutdown();
    }
  });

  it('answers 404 for a malformed percent-escape rather than 500', async () => {
    // decodeURIComponent throws URIError on `/%ZZ`. Uncaught that is an
    // unlogged 500, which also invites a caller to retry a request that can
    // never succeed.
    const booted = await bootWith({ AUTH_MODE: 'disabled' });
    try {
      for (const bad of ['/%ZZ', '/%c0%ae%c0%ae%2fconfig']) {
        expect((await booted.app.request(bad)).status).toBe(404);
      }
    } finally {
      await booted.shutdown();
    }
  });

  it('refuses to serve a path that escapes the web root', async () => {
    // PERCENT-ENCODED, not literal `../`. A literal `..` is removed by the
    // WHATWG URL parser before any app code runs, so asserting on
    // `/../config/config.json` tests the URL parser, not this service -- it
    // cannot fail and cannot pass for the right reason. `%2e%2e%2f` survives
    // normalization and is only turned back into `../` by the handler's own
    // decodeURIComponent, which is what makes the containment check
    // load-bearing rather than defensive.
    //
    // Measured with the containment check neutralised: the literal form did
    // not leak, while both `%2e%2e%2f` and `..%2f` served the target file
    // with a 200. The target here is config.json, which holds drain secrets,
    // and /config is a mounted volume in the container -- so without
    // containment this is credential disclosure, not just a file read.
    const booted = await bootWith({ AUTH_MODE: 'disabled' });
    try {
      const secret = 'canary-secret-value';
      await writeFile(join(dirs.config, 'canary.txt'), secret, 'utf8');

      for (const attack of [
        '/%2e%2e%2fconfig%2fcanary.txt',
        '/..%2fconfig%2fcanary.txt',
        '/%2e%2e%2f%2e%2e%2fetc%2fpasswd',
      ]) {
        const response = await booted.app.request(attack);
        // The SPA fallback answering 200 with index.html is fine; serving
        // the target's CONTENTS is not. Assert on the body, because the
        // status alone cannot tell those two apart.
        expect(await response.text()).not.toContain(secret);
        const second = await booted.app.request(attack);
        expect(await second.text()).not.toContain('root:');
      }
    } finally {
      await booted.shutdown();
    }
  });

  it('creates a default config on first boot', async () => {
    const booted = await bootWith({ AUTH_MODE: 'disabled' });
    try {
      const response = await booted.app.request('/api/admin/config');
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({ config: { drains: [] } });
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
