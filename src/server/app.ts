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
   * The identity header to strip from every route the proxy guard does NOT
   * already cover, independent of `authConfig.mode`. Deriving it from the
   * 'proxy' variant would mean no strip at all under `AUTH_MODE=disabled` or
   * unset -- precisely the modes a staging box runs in, and where a caller
   * could put the header on a drain request and have a request logger
   * record it as the actor.
   *
   * NOT stripped globally. `proxyAuth` reads this same header to establish
   * identity, so a global strip ahead of the route table deletes it before
   * the guard ever sees it, and every admin request under `AUTH_MODE=proxy`
   * is rejected with "AUTH_USER_HEADER was not supplied by the proxy" --
   * measured over a real socket from a trusted loopback peer, on a header
   * the client had in fact supplied. See the strip's call sites in
   * `buildApp` for the exact, deliberately non-exhaustive list of routes
   * this covers.
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
 * 1. `stripIdentityHeader` is mounted, per route group, ahead of every
 *    route the proxy guard does not already cover (health and drain), and
 *    unconditionally on `AppDeps.identityHeader` rather than on
 *    `authConfig.mode`, so it strips in every auth mode including
 *    `disabled` and `unset`. It is deliberately NOT a single global
 *    `app.use('*', ...)`: `proxyAuth` reads this same header to establish
 *    identity, so a global strip ahead of the route table would delete it
 *    before the guard ever sees it and reject every admin request under
 *    `AUTH_MODE=proxy` -- see `AppDeps.identityHeader` for the incident.
 *    `proxyAuth` already strips internally on the paths it guards (the SPA
 *    catch-all included), so those need no separate strip here; a future
 *    route added outside the guard's coverage must be added to this list.
 * 2. `proxyAuth`'s guard must NOT cover `/api/drain` or the health routes:
 *    Vercel authenticates by HMAC, not by proxy identity, and a deployment
 *    must never lose ingest to an auth misconfiguration. Both are therefore
 *    registered on `app` BEFORE `guard` exists at all.
 * 3. The guard DOES cover `/api/admin`, `/api/status`, and the static/SPA
 *    routes, so `app.use(..., guard)` is registered before each of those
 *    route tables.
 * 4. The SPA catch-all must not shadow the API: an `/api/*` 404 is
 *    registered after every real API route and before `staticHandler`'s
 *    `app.get('*', ...)`, so an unmatched `/api/...` path answers 404
 *    instead of falling through to the SPA shell with a 200. It sits
 *    outside the guard for the same reason the drain and health routes do
 *    -- a routing typo must read as "not found", not as a 503 from an
 *    auth-configuration problem.
 */
export function buildApp(deps: AppDeps): Hono<AppEnv> {
  const app = new Hono<AppEnv>();

  // (1) Stripped on exactly the routes the guard does NOT cover, and never
  // globally -- see AppDeps.identityHeader for why a global strip is
  // catastrophic here (it deletes the header proxyAuth needs to read). A new
  // route the guard does not cover MUST be added to this list; that is the
  // price of not being able to do it globally.
  if (deps.identityHeader !== null) {
    const strip = stripIdentityHeader(deps.identityHeader);
    app.use('/healthz', strip);
    app.use('/readyz', strip);
    app.use('/api/drain/*', strip);
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
    // (4) Registered before the SPA catch-all below, and after every real
    // API route above: any /api path arriving here matched nothing, so it is
    // a 404. Without this the catch-all serves the SPA shell with a 200, and
    // a mistyped or withdrawn endpoint looks alive to a client and to
    // monitoring. Deliberately outside the guard -- answering 404 to an
    // unauthenticated prober discloses only that the path does not exist,
    // whereas routing it through the guard would turn every typo into a 503.
    app.all('/api/*', (c) => c.json({ code: 'not_found' }, 404));

    app.use('*', guard);
    app.get('*', staticHandler(deps.webRoot));
  }

  return app;
}
