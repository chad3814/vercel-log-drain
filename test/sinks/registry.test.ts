import { describe, expect, it } from 'vitest';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import { createSink, sinkConfigSchema, warningsFor } from '../../src/sinks/registry.js';

const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));

const fileConfig = {
  type: 'file' as const,
  directory: '/tmp/vld-registry',
  filePrefix: 'events',
  retentionDays: 7,
  freeSpaceFloorBytes: 0,
};

const lokiConfig = {
  type: 'loki' as const,
  url: 'http://loki:3100',
  auth: { kind: 'none' as const },
  tenantId: null,
  labels: { static: { job: 'vercel' }, fromFields: ['requestId'] },
  timeoutMs: 5000,
};

describe('sinkConfigSchema', () => {
  it('accepts a file config', () => {
    expect(sinkConfigSchema.safeParse(fileConfig).success).toBe(true);
  });

  it('accepts a loki config', () => {
    expect(sinkConfigSchema.safeParse(lokiConfig).success).toBe(true);
  });

  it('rejects an unknown sink type', () => {
    expect(sinkConfigSchema.safeParse({ type: 'syslog', host: 'x' }).success).toBe(false);
  });

  it('rejects a loki config with a malformed url', () => {
    expect(sinkConfigSchema.safeParse({ ...lokiConfig, url: 'not a url' }).success).toBe(false);
  });
});

describe('createSink', () => {
  it('builds a file sink', async () => {
    const sink = createSink('local', fileConfig, { log: silentLog });
    expect(sink.type).toBe('file');
    expect(sink.name).toBe('local');
    await sink.close();
  });

  it('builds a loki sink', async () => {
    const sink = createSink('remote', lokiConfig, { log: silentLog });
    expect(sink.type).toBe('loki');
    await sink.close();
  });
});

describe('warningsFor', () => {
  it('surfaces loki label warnings', () => {
    expect(warningsFor(lokiConfig)[0]).toContain('requestId');
  });

  it('returns nothing for a file sink', () => {
    expect(warningsFor(fileConfig)).toEqual([]);
  });
});
