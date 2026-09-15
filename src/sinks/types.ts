import type { z } from 'zod';
import type { Logger } from '../log.js';
import type { LogEvent } from '../vercel/event.js';

export class RetryableDeliveryError extends Error {
  constructor(message: string, cause?: Error) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = 'RetryableDeliveryError';
  }
}

export class PermanentDeliveryError extends Error {
  constructor(message: string, cause?: Error) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = 'PermanentDeliveryError';
  }
}

/**
 * Retryable — the batch stays on disk — but the operator has to do something
 * before it can succeed, so sink health jumps straight to `failed` instead of
 * waiting out five backoff rounds to say so. The distinction that matters is
 * retryable vs permanent: permanence destroys data by moving the batch to
 * `dead/`, and is reserved for a batch that can never be accepted as-is.
 * Anything an operator can fix by changing configuration belongs here.
 */
export class EscalatingDeliveryError extends RetryableDeliveryError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'EscalatingDeliveryError';
  }
}

export class AuthDeliveryError extends EscalatingDeliveryError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'AuthDeliveryError';
  }
}

/**
 * The sink's own configuration, or that of something in front of it, refuses
 * this batch — a body-size limit, say. Same treatment as an auth failure:
 * the logs stay on disk until the configuration changes.
 */
export class ConfigDeliveryError extends EscalatingDeliveryError {
  constructor(message: string, cause?: Error) {
    super(message, cause);
    this.name = 'ConfigDeliveryError';
  }
}

export type FreeSpaceProbe = (path: string) => Promise<number>;

export interface SinkContext {
  readonly log: Logger;
  readonly freeSpace?: FreeSpaceProbe;
}

export interface Sink {
  readonly name: string;
  readonly type: string;
  deliver(events: LogEvent[]): Promise<void>;
  close(): Promise<void>;
}

export interface SinkType<TConfig> {
  readonly type: string;
  readonly configSchema: z.ZodType<TConfig>;
  create(name: string, config: TConfig, ctx: SinkContext): Sink;
  warnings(config: TConfig): string[];
}
