import { Hono } from 'hono';
import { z } from 'zod';
import { EtagMismatchError } from '../../config/store.js';
import { redactConfig, restoreSecrets, SecretRestoreError } from '../../config/redact.js';
import { newDrainId, newDrainSecret } from '../../config/schema.js';
import { warningsFor } from '../../sinks/registry.js';
import { InvalidSinkNameError } from '../../pipeline/dispatcher.js';
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

type SaveFailure =
  | { code: 'conflict'; error: string; status: 409 }
  | { code: 'save_failed'; error: string; status: 500 | 507 };

/**
 * Maps a `ConfigStore.save` rejection onto the two failure shapes the spec
 * distinguishes. Shared by every route that calls `store.save`, so a fix or
 * a new case only has to happen once.
 */
function mapSaveError(error: unknown): SaveFailure {
  if (error instanceof EtagMismatchError) {
    return { code: 'conflict', error: error.message, status: 409 };
  }
  const failure = error instanceof Error ? error : new Error(String(error));
  // The spec reserves 507 for one case: a full config volume. Everything
  // else -- a permission change, a failed rename, an unexpected throw -- is
  // a plain 500. Answering 507 for those sends an operator to go free disk
  // space that was never the problem.
  const status: 500 | 507 = 'code' in failure && failure.code === 'ENOSPC' ? 507 : 500;
  return { code: 'save_failed', error: failure.message, status };
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
    let payload: JsonValue;
    try {
      payload = await c.req.json<JsonValue>();
    } catch (error) {
      if (error instanceof SyntaxError) {
        return c.json({ code: 'bad_request', error: error.message }, 400);
      }
      throw error;
    }

    const body = putBodySchema.safeParse(payload);
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
    //
    // Do NOT "optimise" this by saving first and reconciling second, even
    // though that would look like it removes the two costs below -- it
    // would reintroduce the hazard this ordering exists to prevent, a bad
    // config landing on disk. Those costs are accepted, not fixed, because
    // reordering is worse:
    //
    // - `Dispatcher.applyConfig` reconciles in the order calls ARRIVE, not
    //   the order the matching `store.save` calls land (see
    //   `reconcileChain` on Dispatcher). So a losing PUT's own reconcile
    //   can touch a sink it never asked to change, merely because the
    //   winner's reconcile already changed that sink's entry and the
    //   loser's (older) copy now reads as different; the rollback below
    //   then touches that same sink AGAIN to restore the persisted state.
    //   Measured: two concurrent PUTs, one touching only sink A, the other
    //   only sink B, and sink B's `SpoolQueue.open` is called three times
    //   before the request settles, even though neither PUT's own diff
    //   ever named it.
    // - If the losing PUT renamed a sink, that reconcile briefly creates a
    //   spool directory for the new name (`startSink`'s `mkdir`) before the
    //   rollback tears it down again. Measured: the directory survives as a
    //   permanently empty orphan, because a removed sink's directory is
    //   deliberately left on disk and nothing ever routed a delivery to a
    //   sink that existed for one reconcile cycle. `pruneEmptyOrphans()`
    //   below cleans up exactly that -- and only that: an orphan with zero
    //   files, zero bytes, zero dead letters. A real orphan (any content at
    //   all) is untouched and stays a `discardOrphan` decision.
    //
    // Queued data is never at risk from any of this: the rollback
    // reconciles against the config that is actually on disk, which
    // reopens the same spool directories every affected sink was already
    // using, so nothing enqueued before or during the race is lost.
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
      // mapSaveError is shared with POST /drains, so the two routes cannot
      // drift on which failure means what. It collapsed what used to be two
      // rollback paths into one, so the orphan cleanup below is needed in
      // only one place rather than two.
      const failure = mapSaveError(error);
      if (failure.code === 'save_failed') {
        deps.log.error({ err: failure.error }, 'failed to persist config');
      }
      // Roll the dispatcher back to the config that is actually persisted.
      await deps.dispatcher.applyConfig(deps.getConfig());
      // See pruneEmptyOrphans' doc comment: cleans up an empty spool
      // directory the losing reconcile above may have just created for a
      // renamed sink. Never touches an orphan that holds any data.
      await deps.dispatcher.pruneEmptyOrphans();
      return c.json({ code: failure.code, error: failure.error }, failure.status);
    }

    deps.setConfig(saved.config, saved.etag);
    return c.json({
      config: redactConfig(saved.config),
      etag: saved.etag,
      warnings: warningsForConfig(saved.config),
    });
  });

  app.post('/drains', async (c) => {
    let payload: JsonValue;
    try {
      payload = await c.req.json<JsonValue>();
    } catch (error) {
      if (error instanceof SyntaxError) {
        return c.json({ code: 'bad_request', error: error.message }, 400);
      }
      throw error;
    }

    const body = createDrainSchema.safeParse(payload);
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

    let saved;
    try {
      saved = await deps.store.save(next, deps.getEtag());
    } catch (error) {
      const failure = mapSaveError(error);
      if (failure.code === 'save_failed') {
        deps.log.error({ err: failure.error }, 'failed to persist config');
      }
      // Unlike PUT /config, this route never called dispatcher.applyConfig
      // -- drains do not feed the dispatcher -- so there is no speculative
      // dispatcher state to roll back here.
      return c.json({ code: failure.code, error: failure.error }, failure.status);
    }
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
      // Two distinct rejections, kept distinct. A name that could never be
      // valid is a bad request; a well-formed name that is not an orphan is a
      // miss. Collapsing both into 404 also collapsed the two guards into one
      // observable outcome, so no test could pin either individually.
      if (failure instanceof InvalidSinkNameError) {
        return c.json({ code: 'bad_request', error: failure.message }, 400);
      }
      return c.json({ code: 'not_found', error: failure.message }, 404);
    }
    return c.json({ ok: true });
  });

  return app;
}
