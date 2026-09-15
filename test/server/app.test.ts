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
