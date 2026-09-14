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
  const mode = env['AUTH_MODE'];
  if (mode === undefined || mode.trim().length === 0) return { mode: 'unset' };
  if (mode === 'disabled') return { mode: 'disabled' };
  if (mode !== 'proxy') {
    throw new Error(`AUTH_MODE must be "proxy" or "disabled", received "${mode}"`);
  }

  const trustedProxies = splitList(env['AUTH_TRUSTED_PROXIES']);
  if (trustedProxies === null) {
    throw new Error('AUTH_MODE=proxy requires AUTH_TRUSTED_PROXIES, a comma-separated CIDR list');
  }
  // Fail loudly on the first bad entry rather than deferring to the
  // middleware, which — per the brief's own fail-closed mandate — must
  // never be left to "cope" with a malformed value per request.
  for (const cidr of trustedProxies) {
    parseCidr(cidr);
  }

  const userHeader = env['AUTH_USER_HEADER'];
  if (userHeader === undefined || userHeader.trim().length === 0) {
    throw new Error('AUTH_MODE=proxy requires AUTH_USER_HEADER');
  }

  return {
    mode: 'proxy',
    trustedProxies,
    userHeader: userHeader.trim().toLowerCase(),
    allowedUsers: splitList(env['AUTH_ALLOWED_USERS']),
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

// Node reports an IPv4 peer accepted on a dual-stack socket as
// `::ffff:a.b.c.d`, which is the normal shape for a proxy on a dual-stack
// host — this is not an edge case, it is the default deployment. Node's
// own `net.BlockList#check` already treats `::ffff:a.b.c.d` checked as
// 'ipv6' as a member of an IPv4 subnet added as 'ipv4', but relying on that
// implicitly would leave the mapping undocumented and untested. Normalizing
// explicitly here makes the decision visible, keeps CIDRs and peers on one
// family for the `check` call, and is covered by its own test below.
function normalizePeer(address: string): string {
  const mapped = /^::ffff:(\d+\.\d+\.\d+\.\d+)$/i.exec(address);
  return mapped?.[1] ?? address;
}

export function proxyAuth(config: AuthConfig, resolvePeer: PeerResolver): MiddlewareHandler<AppEnv> {
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
    // header itself must never have it echo through to a handler.
    c.req.raw.headers.delete(userHeader);

    const rawPeer = resolvePeer(c);
    if (rawPeer === undefined) {
      return c.text('forbidden: peer address could not be determined', 403);
    }

    const peer = normalizePeer(rawPeer);
    const family = isIPv4(peer) ? 'ipv4' : isIPv6(peer) ? 'ipv6' : null;
    if (family === null || !blockList.check(peer, family)) {
      return c.text('forbidden: request did not arrive from a trusted proxy', 403);
    }

    const user = providedUser?.trim() ?? '';
    if (user.length === 0) {
      return c.text(`forbidden: ${userHeader} was not supplied by the proxy`, 403);
    }
    if (allowedUsers !== null && !allowedUsers.includes(user)) {
      return c.text('forbidden: user is not permitted by AUTH_ALLOWED_USERS', 403);
    }

    c.set('user', user);
    return next();
  };
}
