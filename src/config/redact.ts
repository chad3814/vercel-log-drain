import { z } from 'zod';
import { appConfigWriteSchema } from './schema.js';
import type { AppConfig, DrainEntry, ServerConfig, SinkEntry } from './schema.js';
import type { JsonValue } from '../../types/json.js';

export class SecretRestoreError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SecretRestoreError';
  }
}

export type RedactedDrain = Omit<DrainEntry, 'secret'> & { secret: null; hasSecret: boolean };

export type RedactedConfig = {
  version: 1;
  drains: RedactedDrain[];
  sinks: SinkEntry[];
  server: ServerConfig;
};

function redactSinkEntry(entry: SinkEntry): SinkEntry {
  if (entry.config.type !== 'loki') return entry;
  const auth = entry.config.auth;
  if (auth.kind === 'basic') {
    return {
      ...entry,
      config: { ...entry.config, auth: { ...auth, password: '' } },
    };
  }
  if (auth.kind === 'bearer') {
    return { ...entry, config: { ...entry.config, auth: { ...auth, token: '' } } };
  }
  return entry;
}

export function redactConfig(config: AppConfig): RedactedConfig {
  return {
    version: 1,
    drains: config.drains.map(({ secret, ...rest }) => ({
      ...rest,
      secret: null,
      hasSecret: secret.length > 0,
    })),
    sinks: config.sinks.map(redactSinkEntry),
    server: config.server,
  };
}

// The incoming payload mirrors RedactedConfig but with secrets optionally
// replaced by real strings, so it is validated loosely here and strictly by
// appConfigWriteSchema once secrets have been restored. appConfigWriteSchema,
// not appConfigSchema: this is the PUT /api/admin/config write boundary, the
// one place that can still afford to reject a loki sink with no usable
// labels outright, rather than the load-time tolerance appConfigSchema
// provides. See appConfigWriteSchema's doc comment in schema.ts.
const incomingSchema = z.object({
  version: z.literal(1),
  drains: z.array(
    z.object({
      id: z.string(),
      name: z.string(),
      secret: z.string().nullish(),
      enabled: z.boolean(),
      createdAt: z.number(),
    }),
  ),
  sinks: z.array(
    z.record(
      z.string(),
      z.custom<JsonValue>(() => true),
    ),
  ),
  server: z.record(
    z.string(),
    z.custom<JsonValue>(() => true),
  ),
});

export function restoreSecrets(incoming: JsonValue, current: AppConfig): AppConfig {
  const parsed = incomingSchema.safeParse(incoming);
  if (!parsed.success) {
    throw new SecretRestoreError(
      `malformed configuration payload:\n${z.prettifyError(parsed.error)}`,
    );
  }

  const drainsById = new Map(current.drains.map((drain) => [drain.id, drain]));
  const sinksByName = new Map(current.sinks.map((sink) => [sink.name, sink]));

  const drains = parsed.data.drains.map((drain) => {
    if (typeof drain.secret === 'string' && drain.secret.length > 0) {
      return { ...drain, secret: drain.secret };
    }
    const existing = drainsById.get(drain.id);
    if (existing === undefined) {
      throw new SecretRestoreError(
        `drain "${drain.name}" is new and must be created with a secret`,
      );
    }
    return { ...drain, secret: existing.secret };
  });

  const sinks = parsed.data.sinks.map((raw) => {
    const restored = restoreSinkSecret(raw, sinksByName);
    return restored;
  });

  const candidate = { version: 1 as const, drains, sinks, server: parsed.data.server };
  const validated = appConfigWriteSchema.safeParse(candidate);
  if (!validated.success) {
    throw new SecretRestoreError(`configuration is invalid:\n${z.prettifyError(validated.error)}`);
  }
  return validated.data;
}

function restoreSinkSecret(
  raw: Record<string, JsonValue>,
  existingByName: Map<string, SinkEntry>,
): JsonValue {
  const config = raw['config'];
  if (config === null || typeof config !== 'object' || Array.isArray(config)) return raw;
  if (config['type'] !== 'loki') return raw;

  const auth = config['auth'];
  if (auth === null || typeof auth !== 'object' || Array.isArray(auth)) return raw;

  const name = typeof raw['name'] === 'string' ? raw['name'] : '';
  const existing = existingByName.get(name);
  const existingAuth =
    existing !== undefined && existing.config.type === 'loki' ? existing.config.auth : null;

  const kind = auth['kind'];
  let field: 'password' | 'token';
  if (kind === 'basic') {
    field = 'password';
  } else if (kind === 'bearer') {
    field = 'token';
  } else {
    return raw;
  }

  const supplied = auth[field];
  if (typeof supplied === 'string' && supplied.length > 0) return raw;

  if (existingAuth === null || existingAuth.kind !== kind) {
    throw new SecretRestoreError(
      `sink "${name}" uses ${kind} auth and must be saved with its ${field}`,
    );
  }
  const carried = existingAuth.kind === 'basic' ? existingAuth.password : existingAuth.token;
  return { ...raw, config: { ...config, auth: { ...auth, [field]: carried } } };
}
