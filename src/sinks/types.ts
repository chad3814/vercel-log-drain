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

export interface SinkContext {
  readonly log: Logger;
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
