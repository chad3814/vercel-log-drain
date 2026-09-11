import { describe, expect, it } from 'vitest';
import {
  appConfigSchema,
  defaultAppConfig,
  newDrainId,
  newDrainSecret,
  SINK_NAME_PATTERN,
  sinkEntrySchema,
} from '../../src/config/schema.js';

const validSink = {
  name: 'local-file',
  enabled: true,
  filter: {},
  maxSpoolBytes: 536_870_912,
  maxBatchEvents: 1000,
  maxBatchBytes: 4_194_304,
  config: {
    type: 'file',
    directory: '/logs',
    filePrefix: 'events',
    retentionDays: 14,
    freeSpaceFloorBytes: 268_435_456,
  },
};

describe('SINK_NAME_PATTERN', () => {
  it.each(['a', 'loki', 'loki-prod', 'sink1', 'a-b-c-1'])('accepts %s', (name) => {
    expect(SINK_NAME_PATTERN.test(name)).toBe(true);
  });

  it.each(['', '-lead', 'Upper', 'has space', 'dot.name', '../escape', 'a/b', 'a_b'])(
    'rejects %s',
    (name) => {
      expect(SINK_NAME_PATTERN.test(name)).toBe(false);
    },
  );

  it('rejects a name longer than 64 characters', () => {
    expect(SINK_NAME_PATTERN.test('a'.repeat(65))).toBe(false);
    expect(SINK_NAME_PATTERN.test('a'.repeat(64))).toBe(true);
  });
});

describe('sinkEntrySchema', () => {
  it('accepts a valid entry', () => {
    expect(sinkEntrySchema.safeParse(validSink).success).toBe(true);
  });

  it('rejects a traversal name', () => {
    expect(sinkEntrySchema.safeParse({ ...validSink, name: '../etc' }).success).toBe(false);
  });

  it('keeps an absent predicate distinct from an empty one', () => {
    // Task 12's filter compiler reads absent as "match everything" and an empty
    // array as "match nothing", so parsing must not collapse the two.
    const absent = sinkEntrySchema.safeParse(validSink);
    const empty = sinkEntrySchema.safeParse({ ...validSink, filter: { sources: [] } });
    expect(absent.success && empty.success).toBe(true);
    if (!absent.success || !empty.success) return;
    expect(absent.data.filter.sources).toBeUndefined();
    expect(empty.data.filter.sources).toEqual([]);
  });

  it('rejects a batch bound larger than the whole spool budget', () => {
    const result = sinkEntrySchema.safeParse({
      ...validSink,
      maxSpoolBytes: 1_048_576,
      maxBatchBytes: 100_000_000,
    });
    expect(result.success).toBe(false);
  });

  it('accepts a batch bound equal to the spool budget', () => {
    const result = sinkEntrySchema.safeParse({
      ...validSink,
      maxSpoolBytes: 4_194_304,
      maxBatchBytes: 4_194_304,
    });
    expect(result.success).toBe(true);
  });

  it('accepts a filter with all predicates', () => {
    const result = sinkEntrySchema.safeParse({
      ...validSink,
      filter: {
        minLevel: 'error',
        sources: ['lambda', 'edge'],
        environments: ['production'],
        projectIds: ['p1'],
      },
    });
    expect(result.success).toBe(true);
  });

  it('rejects an unknown minLevel', () => {
    expect(
      sinkEntrySchema.safeParse({ ...validSink, filter: { minLevel: 'trace' } }).success,
    ).toBe(false);
  });
});

describe('appConfigSchema', () => {
  it('accepts the default config', () => {
    expect(appConfigSchema.safeParse(defaultAppConfig()).success).toBe(true);
  });

  it('rejects a wrong version', () => {
    expect(appConfigSchema.safeParse({ ...defaultAppConfig(), version: 2 }).success).toBe(false);
  });

  it('rejects duplicate sink names, and for that reason alone', () => {
    const config = { ...defaultAppConfig(), sinks: [validSink, { ...validSink }] };

    const result = appConfigSchema.safeParse(config);
    expect(result.success).toBe(false);
    if (result.success) return;
    // `validSink` is otherwise valid, so the duplicate must be the sole issue.
    expect(result.error.issues).toHaveLength(1);
    expect(result.error.issues[0]?.message).toMatch(/sink names must be unique/);
  });

  it('rejects duplicate drain ids, and for that reason alone', () => {
    // The id must satisfy min(8) so the ONLY thing wrong with this config is
    // the duplicate. With a short id the parse also fails on length, so the
    // test would pass even with the uniqueness refine deleted — asserting
    // `success === false` alone does not pin the property it names.
    const drain = {
      id: 'drain001',
      name: 'a',
      secret: 'x'.repeat(24),
      enabled: true,
      createdAt: 1,
    };
    const config = { ...defaultAppConfig(), drains: [drain, { ...drain, name: 'b' }] };

    const result = appConfigSchema.safeParse(config);
    expect(result.success).toBe(false);
    if (result.success) return;
    expect(result.error.issues).toHaveLength(1);
    expect(result.error.issues[0]?.message).toMatch(/drain ids must be unique/);
  });

  it('rejects a drain secret that is too short to be meaningful', () => {
    const drain = { id: 'd1', name: 'a', secret: 'short', enabled: true, createdAt: 1 };
    expect(appConfigSchema.safeParse({ ...defaultAppConfig(), drains: [drain] }).success).toBe(
      false,
    );
  });
});

describe('defaultAppConfig', () => {
  it('starts with no drains and no sinks', () => {
    const config = defaultAppConfig();
    expect(config.drains).toEqual([]);
    expect(config.sinks).toEqual([]);
    expect(config.version).toBe(1);
  });
});

describe('id and secret generation', () => {
  it('generates url-safe drain ids that are unique', () => {
    const ids = new Set(Array.from({ length: 100 }, () => newDrainId()));
    expect(ids.size).toBe(100);
    for (const id of ids) expect(id).toMatch(/^[A-Za-z0-9_-]{16,}$/);
  });

  it('generates secrets long enough to pass the schema', () => {
    const drain = {
      id: newDrainId(),
      name: 'a',
      secret: newDrainSecret(),
      enabled: true,
      createdAt: 1,
    };
    expect(appConfigSchema.safeParse({ ...defaultAppConfig(), drains: [drain] }).success).toBe(
      true,
    );
  });
});
