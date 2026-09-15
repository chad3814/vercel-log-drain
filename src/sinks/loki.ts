import { gzip } from 'node:zlib';
import { promisify } from 'node:util';
import { z } from 'zod';
import { buildPushPayload, labelWarnings, normalizePushUrl } from './loki-payload.js';
import {
  AuthDeliveryError,
  ConfigDeliveryError,
  PermanentDeliveryError,
  RetryableDeliveryError,
} from './types.js';
import type { LogEvent } from '../vercel/event.js';
import type { Sink, SinkContext, SinkType } from './types.js';

const gzipAsync = promisify(gzip);
const DETAIL_LIMIT = 500;

export const lokiAuthSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('none') }),
  z.object({
    kind: z.literal('basic'),
    username: z.string().min(1),
    password: z.string().min(1),
  }),
  z.object({ kind: z.literal('bearer'), token: z.string().min(1) }),
]);

export type LokiAuth = z.infer<typeof lokiAuthSchema>;

export const lokiSinkConfigSchema = z.object({
  type: z.literal('loki'),
  url: z.url(),
  auth: lokiAuthSchema,
  tenantId: z.string().min(1).nullable(),
  labels: z.object({
    static: z.record(z.string(), z.string()),
    fromFields: z.array(z.string()),
  }),
  timeoutMs: z.number().int().min(100).max(120_000),
});

export type LokiSinkConfig = z.infer<typeof lokiSinkConfigSchema>;

export type LokiClassification = 'ok' | 'permanent' | 'retryable' | 'auth' | 'config';

export function classifyLokiStatus(status: number): LokiClassification {
  if (status >= 200 && status < 300) return 'ok';
  if (status === 401 || status === 403 || status === 404) return 'auth';
  // The ONLY permanent status, as spec §7.2's table has it: Loki returns 400
  // for a malformed stream and for entries outside
  // reject_old_samples_max_age, and replaying a week-old spool would
  // otherwise return 400 forever with the queue head never advancing.
  // Permanence destroys data — it moves the batch to dead/, out of the live
  // queue, for an operator to find by hand — so it stays reserved for that.
  if (status === 400) return 'permanent';
  // 413 and 422 were permanent here, which the spec never said and which
  // contradicts its own reasoning: a 413 is CONFIG-FIXABLE (lower the sink's
  // maxBatchBytes, or raise the body limit on the proxy in front of Loki)
  // and the same batch would then succeed unchanged. The error text even
  // said so, about a batch it had already dead-lettered. An nginx in front
  // of Loki with the default client_max_body_size 1m against the 4 MiB
  // default maxBatchBytes turns every push into a 413, so this was
  // reachable by default configuration on both sides.
  //
  // 422 is not a status Loki uses; it can only arrive from something in
  // front of it, where it is no more inherently unfixable than a 413. Spec
  // §7.2 lists it nowhere, and the spec is explicit that the code and it
  // must not disagree about which statuses destroy data, so it is treated
  // the same way rather than being the one undocumented permanent status.
  // The residual risk is a genuinely unfixable 422 pinning the queue head,
  // which escalated health surfaces immediately instead of hiding.
  if (status === 413 || status === 422) return 'config';
  return 'retryable';
}

class LokiSink implements Sink {
  readonly type = 'loki';
  private readonly pushUrl: string;

  constructor(
    readonly name: string,
    private readonly config: LokiSinkConfig,
    private readonly ctx: SinkContext,
  ) {
    this.pushUrl = normalizePushUrl(config.url);
  }

  private headers(): Record<string, string> {
    const headers: Record<string, string> = {
      'content-type': 'application/json',
      'content-encoding': 'gzip',
    };
    if (this.config.tenantId !== null) {
      headers['X-Scope-OrgID'] = this.config.tenantId;
    }
    const auth = this.config.auth;
    if (auth.kind === 'basic') {
      const encoded = Buffer.from(`${auth.username}:${auth.password}`, 'utf8').toString('base64');
      headers['authorization'] = `Basic ${encoded}`;
    } else if (auth.kind === 'bearer') {
      headers['authorization'] = `Bearer ${auth.token}`;
    }
    return headers;
  }

  async deliver(events: LogEvent[]): Promise<void> {
    if (events.length === 0) return;
    const payload = buildPushPayload(events, this.config.labels);
    if (payload.streams.length === 0) return;

    const body = await gzipAsync(Buffer.from(JSON.stringify(payload), 'utf8'));
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.config.timeoutMs);

    // The timer stays armed for the WHOLE exchange, headers and body alike.
    // Clearing it as soon as fetch() resolves would leave the body read
    // unprotected: a server that returns a status promptly and then stalls the
    // body would hang deliver() forever, with no timer left to recover it.
    // Measured: with timeoutMs 300 and a stalled body, deliver() was still
    // pending after 3s. Because a sink's worker is sequential, that halts the
    // sink entirely until the spool budget starts dropping data.
    try {
      let response: Response;
      try {
        response = await fetch(this.pushUrl, {
          method: 'POST',
          headers: this.headers(),
          body,
          signal: controller.signal,
        });
      } catch (error) {
        const cause = error instanceof Error ? error : new Error(String(error));
        throw new RetryableDeliveryError(
          `loki push to ${this.pushUrl} failed: ${cause.message}`,
          cause,
        );
      }

      const classification = classifyLokiStatus(response.status);
      if (classification === 'ok') {
        // Release the socket back to undici's pool. An unconsumed body on the
        // success path — the common path — holds a connection per delivery.
        await response.body?.cancel().catch(() => undefined);
        this.ctx.log.debug({ sink: this.name, count: events.length }, 'loki push accepted');
        return;
      }

      // Still inside the armed timer: if the body stalls, the abort rejects
      // this read, the catch yields an empty detail, and we go on to throw the
      // correct classification error rather than hanging.
      const detail = (await response.text().catch(() => '')).slice(0, DETAIL_LIMIT);
      const summary = `loki responded ${String(response.status)}: ${detail}`;

      if (classification === 'permanent') {
        throw new PermanentDeliveryError(
          `${summary} — Loki will never accept this batch as-is; dead-lettering it to spool/${this.name}/dead/. Entries older than Loki's reject_old_samples_max_age are the usual cause.`,
        );
      }
      if (classification === 'auth') {
        throw new AuthDeliveryError(`${summary} — check the sink's credentials and URL`);
      }
      if (classification === 'config') {
        throw new ConfigDeliveryError(
          `${summary} — the batch is kept on disk and retried. Lower the sink's maxBatchBytes, or raise the body-size limit of whatever sits in front of Loki.`,
        );
      }
      throw new RetryableDeliveryError(summary);
    } finally {
      clearTimeout(timer);
    }
  }

  close(): Promise<void> {
    return Promise.resolve();
  }
}

export const lokiSinkType: SinkType<LokiSinkConfig> = {
  type: 'loki',
  configSchema: lokiSinkConfigSchema,
  create(name, config, ctx) {
    return new LokiSink(name, config, ctx);
  },
  warnings(config) {
    return labelWarnings(config.labels);
  },
};
