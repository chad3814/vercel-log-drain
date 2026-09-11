import { z } from 'zod';
import { fileSinkConfigSchema, fileSinkType } from './file.js';
import { lokiSinkConfigSchema, lokiSinkType } from './loki.js';
import type { FileSinkConfig } from './file.js';
import type { LokiSinkConfig } from './loki.js';
import type { Sink, SinkContext } from './types.js';

export type AnySinkConfig = FileSinkConfig | LokiSinkConfig;

export const sinkConfigSchema: z.ZodType<AnySinkConfig> = z.discriminatedUnion('type', [
  fileSinkConfigSchema,
  lokiSinkConfigSchema,
]);

export function createSink(name: string, config: AnySinkConfig, ctx: SinkContext): Sink {
  switch (config.type) {
    case 'file':
      return fileSinkType.create(name, config, ctx);
    case 'loki':
      return lokiSinkType.create(name, config, ctx);
  }
  const _: never = config;
  return _;
}

export function warningsFor(config: AnySinkConfig): string[] {
  switch (config.type) {
    case 'file':
      return fileSinkType.warnings(config);
    case 'loki':
      return lokiSinkType.warnings(config);
  }
  const _: never = config;
  return _;
}
