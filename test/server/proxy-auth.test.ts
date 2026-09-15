import type { AddressInfo } from 'node:net';
import { describe, expect, it } from 'vitest';
import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { nodePeerResolver, parseAuthConfig, proxyAuth } from '../../src/server/middleware/proxy-auth.js';
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

  it('throws on an empty AUTH_TRUSTED_PROXIES rather than trusting nothing silently', () => {
    expect(() =>
      parseAuthConfig({ AUTH_MODE: 'proxy', AUTH_TRUSTED_PROXIES: '   ,  ', AUTH_USER_HEADER: 'x-user' }),
    ).toThrow(/AUTH_TRUSTED_PROXIES/);
  });

  it('throws on a malformed CIDR entry rather than silently dropping it', () => {
    expect(() =>
      parseAuthConfig({
        AUTH_MODE: 'proxy',
        AUTH_TRUSTED_PROXIES: '10.0.0.0/8, not-an-address',
        AUTH_USER_HEADER: 'x-user',
      }),
    ).toThrow(/AUTH_TRUSTED_PROXIES/);
  });

  it('throws on a prefix length out of range for the address family', () => {
    expect(() =>
      parseAuthConfig({
        AUTH_MODE: 'proxy',
        AUTH_TRUSTED_PROXIES: '10.0.0.0/33',
        AUTH_USER_HEADER: 'x-user',
      }),
    ).toThrow(/AUTH_TRUSTED_PROXIES/);
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
    expect(await response.text()).toMatch(/did not arrive from a trusted proxy/);
  });

  it('denies a trusted peer with no user header', async () => {
    const response = await appWith(proxyMode, '10.2.3.4').request('/admin/thing');
    expect(response.status).toBe(403);
    expect(await response.text()).toBe('forbidden: AUTH_USER_HEADER was not supplied by the proxy');
  });

  it('denies a trusted peer with an empty user header', async () => {
    const response = await appWith(proxyMode, '10.2.3.4').request('/admin/thing', {
      headers: { 'x-forwarded-user': '   ' },
    });
    expect(response.status).toBe(403);
    expect(await response.text()).toBe('forbidden: AUTH_USER_HEADER was not supplied by the proxy');
  });

  it('denies when the peer cannot be resolved, rather than failing open', async () => {
    const response = await appWith(proxyMode, undefined).request('/admin/thing', {
      headers: { 'x-forwarded-user': 'chad@example.com' },
    });
    expect(response.status).toBe(403);
    expect(await response.text()).toMatch(/peer address could not be determined/);
  });

  it('ignores X-Forwarded-For when deciding trust', async () => {
    const response = await appWith(proxyMode, '203.0.113.9').request('/admin/thing', {
      headers: { 'x-forwarded-for': '10.0.0.1', 'x-forwarded-user': 'attacker@example.com' },
    });
    expect(response.status).toBe(403);
    expect(await response.text()).toMatch(/did not arrive from a trusted proxy/);
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
    expect(await denied.text()).toBe('forbidden: user is not in AUTH_ALLOWED_USERS');
  });

  it('handles an IPv6-mapped IPv4 peer address', async () => {
    const response = await appWith(proxyMode, '::ffff:10.2.3.4').request('/admin/thing', {
      headers: { 'x-forwarded-user': 'chad@example.com' },
    });
    expect(response.status).toBe(200);
  });

  it('denies an IPv6-mapped address whose IPv4 form is outside every trusted CIDR', async () => {
    const response = await appWith(proxyMode, '::ffff:203.0.113.9').request('/admin/thing', {
      headers: { 'x-forwarded-user': 'chad@example.com' },
    });
    expect(response.status).toBe(403);
    expect(await response.text()).toMatch(/did not arrive from a trusted proxy/);
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
    const response = await app.request('/thing');
    expect(await response.text()).toBe('{"user":null}');
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

    // `serve()`'s listener starts asynchronously; the port is only known once
    // it fires the listening callback, not synchronously after the call.
    let server: ReturnType<typeof serve> | undefined;
    const address = await new Promise<AddressInfo>((resolve) => {
      server = serve({ fetch: app.fetch, hostname: '127.0.0.1', port: 0 }, resolve);
    });
    try {
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
        if (server === undefined) {
          resolve();
          return;
        }
        server.close(() => resolve());
      });
    }
  });

  it('does not apply to unguarded routes', async () => {
    const response = await appWith({ mode: 'unset' }, undefined).request('/open');
    expect(response.status).toBe(200);
  });

  it('strips any inbound copy of the user header so only c.get("user") can carry identity', async () => {
    const app = new Hono<AppEnv>();
    app.use('/admin/*', proxyAuth(proxyMode, () => '10.2.3.4'));
    app.get('/admin/thing', (c) =>
      c.json({ user: c.get('user'), rawHeader: c.req.header('x-forwarded-user') ?? null }),
    );
    const response = await app.request('/admin/thing', {
      headers: { 'x-forwarded-user': 'chad@example.com' },
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ user: 'chad@example.com', rawHeader: null });
  });
});
