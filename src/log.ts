import pino from 'pino';
import type { DestinationStream, Logger } from 'pino';

export type { Logger };

export const REDACT_PATHS: string[] = [
  'secret',
  'password',
  'token',
  '*.secret',
  '*.password',
  '*.token',
  'drains[*].secret',
  'sinks[*].config.auth.password',
  'sinks[*].config.auth.token',
  'config.auth.password',
  'config.auth.token',
  'auth.password',
  'auth.token',
];

export function createLogger(level: string, destination?: DestinationStream): Logger {
  const options = {
    level,
    redact: { paths: REDACT_PATHS, censor: '[redacted]' },
  };
  return destination === undefined ? pino(options) : pino(options, destination);
}
