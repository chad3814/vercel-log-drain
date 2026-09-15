import { Hono } from 'hono';
import { z } from 'zod';
import { EtagMismatchError } from '../../config/store.js';
import { redactConfig, restoreSecrets, SecretRestoreError } from '../../config/redact.js';
import { newDrainId, newDrainSecret } from '../../config/schema.js';
import { warningsFor } from '../../sinks/registry.js';
import type { ConfigStore } from '../../config/store.js';
import type { AppConfig } from '../../config/schema.js';
import type { Dispatcher } from '../../pipeline/dispatcher.js';
import type { Logger } from '../../log.js';
import type { AppEnv } from '../types.js';
import type { JsonValue } from '../../../types/json.js';

export type AdminDeps = {
  store: ConfigStore;
  dispatcher: Dispatcher;
  getConfig: () => AppConfig;
  getEtag: () => string;
  setConfig: (config: AppConfig, etag: string) => void;
  log: Logger;
};

const putBodySchema = z.object({
  config: z.custom<JsonValue>(() => true),
  etag: z.string().min(1),
});

const createDrainSchema = z.object({ name: z.string().min(1).max(128) });

function warningsForConfig(config: AppConfig): string[] {
  return config.sinks.flatMap((entry) =>
    warningsFor(entry.config).map((warning) => `${entry.name}: ${warning}`),
  );
}

/**
 * Admin API. Handles secrets, so the one rule that matters everywhere in
 * this file is: every response that carries a config is built with
 * `redactConfig`, never with the config value passed straight through.
 * `restoreSecrets` hands back REAL secrets on purpose -- they have to be
 * real to be persisted -- so a handler that echoes its output verbatim
 * would leak every credential it just restored.
 *
 * There are exactly two config-carrying responses in this router --
 * `GET /config` and `PUT /config` -- and both call `redactConfig` on the
 * value they send, inline in the `c.json(...)` call, so the redaction is
 * visible at the same call site as the response rather than one hop away
 * in a helper a reviewer would have to go find. `POST /drains` is the
 * single sanctioned exception: it hands back a freshly generated secret
 * exactly once, at creation, because that is the only way for an operator
 * to ever see it.
 */
export function adminRoutes(deps: AdminDeps): Hono<AppEnv> {
  const app = new Hono<AppEnv>();

  app.get('/config', (c) => {
    const config = deps.getConfig();
    return c.json({
      config: redactConfig(config),
      etag: deps.getEtag(),
      warnings: warningsForConfig(config),
    });
  });

  app.put('/config', async (c) => {
    const body = putBodySchema.safeParse(await c.req.json<JsonValue>());
    if (!body.success) {
      return c.json({ code: 'bad_request', error: z.prettifyError(body.error) }, 400);
    }

    let candidate: AppConfig;
    try {
      candidate = restoreSecrets(body.data.config, deps.getConfig());
    } catch (error) {
      if (error instanceof SecretRestoreError) {
        return c.json({ code: 'invalid_config', error: error.message }, 400);
      }
      throw error;
    }

    // Apply to the dispatcher first: it validates path containment and can
    // fail before anything is persisted. A schema-invalid or
    // path-escaping config must never reach disk, because a partially
    // applied config is worse than a rejected one.
    try {
      await deps.dispatcher.applyConfig(candidate);
    } catch (error) {
      const failure = error instanceof Error ? error : new Error(String(error));
      return c.json({ code: 'invalid_config', error: failure.message }, 400);
    }

    let saved;
    try {
      saved = await deps.store.save(candidate, body.data.etag);
    } catch (error) {
      if (error instanceof EtagMismatchError) {
        // Roll the dispatcher back to the config that is actually persisted.
        await deps.dispatcher.applyConfig(deps.getConfig());
        return c.json({ code: 'conflict', error: error.message }, 409);
      }
      const failure = error instanceof Error ? error : new Error(String(error));
      deps.log.error({ err: failure.message }, 'failed to persist config');
      await deps.dispatcher.applyConfig(deps.getConfig());
      return c.json({ code: 'save_failed', error: failure.message }, 507);
    }

    deps.setConfig(saved.config, saved.etag);
    return c.json({
      config: redactConfig(saved.config),
      etag: saved.etag,
      warnings: warningsForConfig(saved.config),
    });
  });

  app.post('/drains', async (c) => {
    const body = createDrainSchema.safeParse(await c.req.json<JsonValue>());
    if (!body.success) {
      return c.json({ code: 'bad_request', error: z.prettifyError(body.error) }, 400);
    }

    const secret = newDrainSecret();
    const drain = {
      id: newDrainId(),
      name: body.data.name,
      secret,
      enabled: true,
      createdAt: Date.now(),
    };
    const current = deps.getConfig();
    const next: AppConfig = { ...current, drains: [...current.drains, drain] };

    const saved = await deps.store.save(next, deps.getEtag());
    deps.setConfig(saved.config, saved.etag);

    // The only time a secret is ever returned by the API.
    return c.json({ id: drain.id, name: drain.name, secret, etag: saved.etag }, 201);
  });

  app.post('/sinks/:name/test', async (c) => {
    const result = await deps.dispatcher.testSink(c.req.param('name'));
    return c.json(result);
  });

  app.delete('/orphans/:name', async (c) => {
    try {
      await deps.dispatcher.discardOrphan(c.req.param('name'));
    } catch (error) {
      const failure = error instanceof Error ? error : new Error(String(error));
      return c.json({ code: 'not_found', error: failure.message }, 404);
    }
    return c.json({ ok: true });
  });

  return app;
}
