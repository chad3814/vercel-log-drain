import { gzip } from 'node:zlib';
import { promisify } from 'node:util';
import { z } from 'zod';
import { buildPushPayload, labelWarnings, normalizePushUrl } from './loki-payload.js';
import { AuthDeliveryError, PermanentDeliveryError, RetryableDeliveryError } from './types.js';
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

export type LokiClassification = 'ok' | 'permanent' | 'retryable' | 'auth';

export function classifyLokiStatus(status: number): LokiClassification {
  if (status >= 200 && status < 300) return 'ok';
  if (status === 401 || status === 403 || status === 404) return 'auth';
  // 400 covers malformed streams and entries outside reject_old_samples_max_age;
  // 413/422 mean this exact payload will never be accepted. Retrying any of
  // them forever would pin the head of the queue.
  if (status === 400 || status === 413 || status === 422) return 'permanent';
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
          `${summary} — batch cannot be accepted as-is; dead-lettering. If this is 413, lower the sink's maxBatchBytes.`,
        );
      }
      if (classification === 'auth') {
        throw new AuthDeliveryError(`${summary} — check the sink's credentials and URL`);
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
