import { statfs } from 'node:fs/promises';
import { Hono } from 'hono';
import type { AppConfig } from '../../config/schema.js';
import type { Dispatcher } from '../../pipeline/dispatcher.js';
import type { Metrics } from '../../status/metrics.js';
import type { AppEnv } from '../types.js';
import type { StatusSnapshot, VolumeStatus } from '../../../types/api.js';

export type StatusDeps = {
  getConfig: () => AppConfig;
  dispatcher: Dispatcher;
  metrics: Metrics;
  version: string;
  configDir: string;
  spoolDir: string;
};

async function volumeStatus(path: string): Promise<VolumeStatus> {
  try {
    const stats = await statfs(path);
    return {
      path,
      freeBytes: stats.bavail * stats.bsize,
      totalBytes: stats.blocks * stats.bsize,
    };
  } catch {
    // A volume that cannot be statted -- an unmounted volume, a permissions
    // change -- is reported as zeroed rather than failing the whole
    // snapshot. The rest of the page (drains, sinks, recents) is still
    // useful even when one filesystem probe cannot answer.
    return { path, freeBytes: 0, totalBytes: 0 };
  }
}

/**
 * GET /api/status -- a read-only snapshot for the SPA. It must never
 * mutate metrics, the spool, or the config: a monitoring dashboard polls
 * this every few seconds for as long as the process lives, and a snapshot
 * with side effects would make behavior depend on polling frequency.
 *
 * Per-sink failures are handled inside `Dispatcher.snapshotSinks()`, which
 * isolates one sink's stat failure (a spool directory removed, a
 * permissions change) from the rest of the list and reports it as a
 * synthesized `failed` health entry rather than throwing. This route
 * trusts that contract rather than re-guarding it here, so the isolation
 * logic and its test live in exactly one place.
 */
export function statusRoutes(deps: StatusDeps): Hono<AppEnv> {
  const app = new Hono<AppEnv>();

  app.get('/', async (c) => {
    const config = deps.getConfig();
    const metrics = deps.metrics.snapshot();
    const byId = new Map(metrics.drains.map((entry) => [entry.id, entry]));

    const snapshot: StatusSnapshot = {
      service: {
        state: deps.dispatcher.isDegraded() ? 'degraded' : 'ok',
        uptimeSec: metrics.uptimeSec,
        version: deps.version,
        startedAt: metrics.startedAt,
        unknownDrainRequests: metrics.unknownDrainRequests,
      },
      volumes: {
        config: await volumeStatus(deps.configDir),
        spool: await volumeStatus(deps.spoolDir),
      },
      // Built from config, not from the metrics map: a drain removed from
      // config must stop appearing here even though metrics may still hold
      // its historical counters, and a drain that has never received a
      // delivery still appears with zeroed counters -- otherwise a
      // misconfigured drain would be invisible on the page where an
      // operator would look for it.
      drains: config.drains.map((drain) => {
        const counters = byId.get(drain.id);
        return {
          id: drain.id,
          name: drain.name,
          enabled: drain.enabled,
          eventsReceived: counters?.eventsReceived ?? 0,
          lastEventAt: counters?.lastEventAt ?? null,
          requests: counters?.requests ?? {
            ok: 0,
            badSignature: 0,
            disabled: 0,
            malformedBody: 0,
          },
        };
      }),
      // Likewise built from config via the dispatcher, so a sink that has
      // left the config does not linger in the response.
      sinks: await deps.dispatcher.snapshotSinks(),
      orphanedSpools: await deps.dispatcher.listOrphanedSpools(),
      recent: {
        events: metrics.recent.events,
        rejects: metrics.recent.rejects,
        errors: metrics.recent.errors,
      },
    };

    return c.json(snapshot);
  });

  return app;
}

/**
 * GET /healthz -- liveness. Answers 200 unconditionally, the instant the
 * process is accepting connections at all. It reads no config, touches no
 * disk, and calls nothing that can fail, by design: liveness exists so an
 * orchestrator can decide whether to kill and restart the container, and a
 * transient disk hiccup -- or `AUTH_MODE` being unset -- must never trigger
 * that. Restarting a process that is otherwise fine only turns one
 * transient problem into an outage.
 *
 * GET /readyz -- readiness. Unlike liveness, readiness is ALLOWED to
 * report not-ready: it answers whether the service can durably persist a
 * delivery right now, not merely whether the process exists. It reports 503
 * for exactly one condition -- `dispatcher.spoolBelowFloor()`, the spool
 * volume below its free-space floor, where acknowledged deliveries are
 * being discarded (spec §4).
 *
 * Deliberately NARROWER than `isDegraded()`, which also covers a failed
 * sink and a config with no sinks. Spec §10: "readiness must not gate the
 * surface that fixes it." An orchestrator that pulls a pod out of rotation
 * on a failing readiness probe also pulls the admin UI and API, which this
 * same process serves, and both of those conditions are fixed THROUGH that
 * UI -- a Loki URL typo, or adding the first sink. Gating on them
 * deadlocks: `defaultAppConfig()` ships no sinks, so a fresh deployment
 * would be permanently not-ready and an operator could never reach the page
 * that would make it ready. Both still show on `/api/status` as
 * `service.state: degraded`, which is where an operator reads them.
 *
 * The free-space floor is exempt from that reasoning because it is not
 * fixed through the browser -- an operator frees or resizes the volume --
 * and refusing traffic there is honest: the service genuinely cannot store
 * what it would be handed.
 *
 * Readiness still does not probe the filesystem itself (that is
 * `/api/status`'s job, and duplicating it here would make readyz slow and
 * disk-dependent, which is exactly what liveness above is not allowed to
 * be either) -- it reports what the last enqueue actually observed.
 */
export function healthRoutes(deps: StatusDeps): Hono<AppEnv> {
  const app = new Hono<AppEnv>();
  app.get('/healthz', (c) => c.text('ok'));
  app.get('/readyz', (c) =>
    deps.dispatcher.spoolBelowFloor() ? c.text('degraded', 503) : c.text('ready'),
  );
  return app;
}
