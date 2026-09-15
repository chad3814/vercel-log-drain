import { BlockList, isIPv4, isIPv6 } from 'node:net';
import type { Context, MiddlewareHandler } from 'hono';
import type { AppEnv } from '../types.js';

// Identity is delegated entirely to a reverse proxy in front of this
// service. There is deliberately no default for `mode`: an unset AUTH_MODE
// must close the admin surface (503), never open it. `disabled` exists only
// for local development, where there is no proxy to trust.
export type AuthConfig =
  | { mode: 'unset' }
  | { mode: 'disabled' }
  | {
      mode: 'proxy';
      trustedProxies: string[];
      userHeader: string;
      allowedUsers: string[] | null;
    };

// Injected rather than read off the server binding: `@hono/node-server`
// populates `c.env.incoming` only for requests that arrived over a real
// socket. Under Hono's in-process `app.request()` (used pervasively in
// tests, and by anything that talks to this app without a real listener)
// `c.env.incoming` is `undefined` — verified against `@hono/node-server`
// 2.1.1 on 2026-09-08. A resolver that cannot determine a peer must return
// `undefined`, and callers must treat that as untrusted, never as trusted.
export type PeerResolver = (c: Context<AppEnv>) => string | undefined;

// The ONLY source of peer identity. Never read X-Forwarded-For or any other
// request header here — the client fully controls those, so deriving trust
// from them would let any direct caller impersonate a trusted proxy.
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

type ParsedCidr = { address: string; prefix: number; family: 'ipv4' | 'ipv6' };

// Validated once, at config-parse time, so a typo in AUTH_TRUSTED_PROXIES
// fails the boot loudly instead of silently narrowing (or worse, silently
// failing to narrow) the trusted set at request time.
function parseCidr(raw: string): ParsedCidr {
  const slash = raw.lastIndexOf('/');
  const address = slash === -1 ? raw : raw.slice(0, slash);
  const family = isIPv4(address) ? 'ipv4' : isIPv6(address) ? 'ipv6' : null;
  if (family === null) {
    throw new Error(`AUTH_TRUSTED_PROXIES contains an invalid address: "${raw}"`);
  }
  const maxPrefix = family === 'ipv4' ? 32 : 128;
  const prefix = slash === -1 ? maxPrefix : Number.parseInt(raw.slice(slash + 1), 10);
  if (!Number.isInteger(prefix) || prefix < 0 || prefix > maxPrefix) {
    throw new Error(`AUTH_TRUSTED_PROXIES contains an invalid prefix length: "${raw}"`);
  }
  return { address, prefix, family };
}

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
  // Fail loudly on the first bad entry rather than deferring to the
  // middleware, which must never be left to "cope" with a malformed value
  // per request.
  for (const cidr of trustedProxies) {
    parseCidr(cidr);
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
  // authenticates — the opposite of what emptying the list intends.
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
    const { address, prefix, family } = parseCidr(cidr);
    list.addSubnet(address, prefix, family);
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

  // `config` is narrowed to the 'proxy' variant for the rest of this
  // function by the two early returns above, so everything below reads
  // straight off it with no null checks standing in for "this cannot
  // happen" — an absent positive decision falls through to a denial by
  // construction, not because every failure mode was enumerated.
  const blockList = buildBlockList(config.trustedProxies);
  const userHeader = config.userHeader;
  const allowedUsers = config.allowedUsers;

  return async (c, next) => {
    const providedUser = c.req.header(userHeader);
    // Strip any inbound copy of the identity header immediately, in every
    // branch, before any downstream handler can run. `c.get('user')` — set
    // exclusively below, after trust is established — is the only channel
    // a handler may treat as identity; a direct caller supplying this
    // header itself must never have it echo through to a handler. Note the
    // strip only covers routes this middleware is mounted on — Task 23
    // mounts a separate unconditional strip ahead of the route table so the
    // guarantee also holds on the auth-exempt drain path.
    c.req.raw.headers.delete(userHeader);

    const peer = resolvePeer(c);
    if (peer === undefined) {
      return c.text('forbidden: peer address could not be determined', 403);
    }

    // No normalisation of `::ffff:a.b.c.d`: net.BlockList#check already maps
    // in both directions, so a peer accepted on a dual-stack listener matches
    // an IPv4 subnet and vice versa. An earlier version normalised
    // explicitly and was measured to change no outcome. Dead code on a
    // trust boundary is worse than none — it implies a protection that is
    // not there. The mapped cases are asserted directly below, so a future
    // Node changing this behaviour is caught rather than masked.
    const family = isIPv4(peer) ? 'ipv4' : isIPv6(peer) ? 'ipv6' : null;
    if (family === null || !blockList.check(peer, family)) {
      return c.text('forbidden: request did not arrive from a trusted proxy', 403);
    }

    const user = providedUser?.trim() ?? '';
    if (user.length === 0) {
      // Name the variable, never its value. Echoing the configured header
      // name tells anyone reaching this point from inside a trusted subnet
      // — a co-located container, an SSRF — exactly which header to forge.
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
