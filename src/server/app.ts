import { Hono } from 'hono';
import { adminRoutes } from './routes/admin.js';
import { drainRoutes } from './routes/drain.js';
import { healthRoutes, statusRoutes } from './routes/status.js';
import { proxyAuth, stripIdentityHeader } from './middleware/proxy-auth.js';
import { staticHandler } from './static.js';
import type { AdminDeps } from './routes/admin.js';
import type { DrainDeps } from './routes/drain.js';
import type { StatusDeps } from './routes/status.js';
import type { AuthConfig, PeerResolver } from './middleware/proxy-auth.js';
import type { AppEnv } from './types.js';

export type AppDeps = {
  authConfig: AuthConfig;
  /**
   * The identity header to strip from every inbound request, independent of
   * `authConfig.mode`. Deriving it from the 'proxy' variant would mean no
   * strip at all under `AUTH_MODE=disabled` or unset -- precisely the modes a
   * staging box runs in, and where a caller could put the header on a drain
   * request and have a request logger record it as the actor.
   */
  identityHeader: string | null;
  peerResolver: PeerResolver;
  drain: DrainDeps;
  status: StatusDeps;
  admin: AdminDeps;
  webRoot: string | null;
};

/**
 * Middleware registration order carries three separate guarantees, and Hono
 * applies `app.use()` only to routes registered *after* it -- which is why
 * this function's shape below matters as much as its contents:
 *
 * 1. `stripIdentityHeader` is mounted with `app.use('*', ...)` FIRST, ahead
 *    of the entire route table (including the auth-exempt drain route), and
 *    unconditionally on `AppDeps.identityHeader` rather than on
 *    `authConfig.mode`. It authenticates nothing -- it only removes a value
 *    no inbound request may assert -- so it must run in every auth mode,
 *    including `disabled` and `unset`, which are exactly the modes a
 *    staging box runs in.
 * 2. `proxyAuth`'s guard must NOT cover `/api/drain` or the health routes:
 *    Vercel authenticates by HMAC, not by proxy identity, and a deployment
 *    must never lose ingest to an auth misconfiguration. Both are therefore
 *    registered on `app` BEFORE `guard` exists at all.
 * 3. The guard DOES cover `/api/admin`, `/api/status`, and the static/SPA
 *    routes, so `app.use(..., guard)` is registered before each of those
 *    route tables.
 */
export function buildApp(deps: AppDeps): Hono<AppEnv> {
  const app = new Hono<AppEnv>();

  // (1) Ahead of every route, including the auth-exempt ones, and in every
  // auth mode. See AppDeps.identityHeader for why this reads the environment
  // directly rather than authConfig.mode.
  if (deps.identityHeader !== null) {
    app.use('*', stripIdentityHeader(deps.identityHeader));
  }

  // (2) Unauthenticated by design: liveness/readiness probes and the drain
  // endpoint, which authenticates by HMAC because Vercel cannot present an
  // SSO identity. Registered before `guard` is even constructed, so no
  // later `app.use(..., guard)` call can wrap them.
  app.route('/', healthRoutes(deps.status));
  app.route('/api/drain', drainRoutes(deps.drain));

  // (3) Everything else sits behind the proxy guard.
  const guard = proxyAuth(deps.authConfig, deps.peerResolver);
  app.use('/api/admin/*', guard);
  app.use('/api/status', guard);
  app.route('/api/admin', adminRoutes(deps.admin));
  app.route('/api/status', statusRoutes(deps.status));

  if (deps.webRoot !== null) {
    app.use('*', guard);
    app.get('*', staticHandler(deps.webRoot));
  }

  return app;
}
