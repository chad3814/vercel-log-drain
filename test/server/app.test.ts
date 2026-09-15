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
