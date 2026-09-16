import { Hono } from 'hono';
import { verifySignature } from '../../vercel/signature.js';
import { decodeBody, PayloadTooLargeError } from '../../vercel/decode.js';
import { SpoolFloorError } from '../../pipeline/spool.js';
import type { DecodeResult } from '../../vercel/decode.js';
import type { AppConfig } from '../../config/schema.js';
import type { Dispatcher } from '../../pipeline/dispatcher.js';
import type { Metrics } from '../../status/metrics.js';
import type { Logger } from '../../log.js';
import type { AppEnv } from '../types.js';

export type DrainDeps = {
  getConfig: () => AppConfig;
  dispatcher: Dispatcher;
  metrics: Metrics;
  log: Logger;
};

/**
 * The one route Vercel itself calls, so it is deliberately outside
 * `proxyAuth`: Vercel cannot present an SSO identity, and this is the only
 * route reachable by an unauthenticated caller. `verifySignature` against
 * the drain's own secret is the entire authentication story here — nothing
 * in this handler ever reads identity off a request header.
 */
export function drainRoutes(deps: DrainDeps): Hono<AppEnv> {
  const app = new Hono<AppEnv>();

  app.post('/:drainId', async (c) => {
    const config = deps.getConfig();
    const drainId = c.req.param('drainId');
    const drain = config.drains.find((entry) => entry.id === drainId);

    if (drain === undefined) {
      // Aggregate only. A per-id counter here would let anyone grow this
      // service's memory without bound just by posting to invented ids.
      deps.metrics.recordUnknownDrainRequest();
      return c.json({ code: 'unknown_drain' }, 404);
    }
    if (!drain.enabled) {
      deps.metrics.recordDrainRequest(drain.id, 'disabled');
      return c.json({ code: 'drain_disabled' }, 403);
    }

    // Cheap size checks before the body is even read fully: a declared
    // Content-Length over the cap is rejected without buffering anything.
    const declaredLength = Number.parseInt(c.req.header('content-length') ?? '', 10);
    if (!Number.isNaN(declaredLength) && declaredLength > config.server.maxBodyBytes) {
      return c.json({ code: 'payload_too_large' }, 413);
    }

    const raw = Buffer.from(await c.req.arrayBuffer());
    if (raw.byteLength > config.server.maxBodyBytes) {
      return c.json({ code: 'payload_too_large' }, 413);
    }

    // Signature verification happens before anything expensive: no
    // decompression, no JSON parsing, until the caller has proven it holds
    // this drain's secret.
    const signature = c.req.header('x-vercel-signature');
    if (signature === undefined) {
      deps.metrics.recordDrainRequest(drain.id, 'badSignature');
      return c.json({ code: 'missing_signature' }, 401);
    }
    if (!verifySignature(raw, signature, drain.secret)) {
      deps.metrics.recordDrainRequest(drain.id, 'badSignature');
      return c.json({ code: 'invalid_signature', error: "signature didn't match" }, 403);
    }

    const gzipped = c.req.header('content-encoding')?.toLowerCase().includes('gzip') ?? false;

    let decoded: DecodeResult;
    try {
      decoded = await decodeBody(raw, {
        gzipped,
        maxDecompressedBytes: config.server.maxDecompressedBytes,
      });
    } catch (error) {
      // decodeBody throws ONLY for the size cap (PayloadTooLargeError).
      // A corrupt or truncated gzip body does not throw at all -- it comes
      // back as a normal DecodeResult carrying one WHOLE_BODY_INDEX reject,
      // handled below like any other unparseable body. Adding a second
      // catch for corruption here would resurrect a defect from an earlier
      // task: it would turn a durable-but-unusable delivery into a 5xx that
      // Vercel redelivers forever, since redelivery reproduces the same
      // corrupt bytes every time.
      if (error instanceof PayloadTooLargeError) {
        deps.metrics.recordDrainRequest(drain.id, 'malformedBody');
        return c.json({ code: 'payload_too_large', error: error.message }, 413);
      }
      throw error;
    }

    if (decoded.rejected.length > 0) {
      deps.metrics.recordRejected(drain.id, decoded.rejected);
    }

    if (decoded.events.length > 0) {
      try {
        // The entire crash-safety story rests on this await: Vercel is
        // acknowledged only once enqueue() has resolved, never before and
        // never concurrently. A failure here surfaces as 500 below, so
        // Vercel redelivers -- a tolerable duplicate. Acking before this
        // resolves would turn any in-flight batch into silent loss the
        // moment the process dies, and nothing in the spool's own tests can
        // catch that, because the ordering lives here, not in the spool.
        //
        // This catch is also the whole of the backpressure decision at the
        // HTTP layer (spec §4): a spool volume below its free-space floor,
        // and a config with no enabled sink, both throw rather than letting
        // the handler fall through to the 200 below. Nothing is acknowledged
        // that could not be stored.
        await deps.dispatcher.enqueue(decoded.events);
      } catch (error) {
        const failure = error instanceof Error ? error : new Error(String(error));
        // A full volume is called out separately because it is the one
        // refusal an operator acts on differently -- they free or resize the
        // volume, and /readyz is already answering 503 for it -- and because
        // it is expected to repeat for every delivery until they do, so a log
        // line that reads like an unexpected write failure buries it.
        const belowFloor = error instanceof SpoolFloorError;
        deps.log.error(
          { drain: drain.id, err: failure.message, belowFloor },
          belowFloor
            ? 'refused delivery: spool volume is below its free-space floor'
            : 'failed to spool batch',
        );
        deps.metrics.recordError('ingest', failure.message);
        // 500, deliberately, not 503: spec §4 and §5 pin 500 as the status
        // Vercel redelivers on, and nothing here has established that it
        // treats 503 the same way. At-least-once is the accepted trade -- the
        // redelivery may duplicate into a sink that already committed this
        // batch before a later one refused.
        return c.json({ code: belowFloor ? 'spool_below_floor' : 'spool_failed' }, 500);
      }

      const latest = decoded.events.reduce(
        (max, item) => (item.timestamp > max ? item.timestamp : max),
        0,
      );
      deps.metrics.recordEventsReceived(drain.id, decoded.events.length, latest);
      deps.metrics.pushRecentEvents(decoded.events);
    }

    deps.metrics.recordDrainRequest(
      drain.id,
      decoded.events.length > 0 || decoded.rejected.length === 0 ? 'ok' : 'malformedBody',
    );

    return c.json({
      received: decoded.events.length + decoded.rejected.length,
      accepted: decoded.events.length,
      rejected: decoded.rejected.length,
    });
  });

  return app;
}
