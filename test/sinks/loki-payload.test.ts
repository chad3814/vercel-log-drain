import { describe, expect, it } from 'vitest';
import {
  buildPushPayload,
  labelWarnings,
  normalizePushUrl,
  resolveLabels,
  sanitizeLabelName,
} from '../../src/sinks/loki-payload.js';
import type { LokiLabelConfig } from '../../src/sinks/loki-payload.js';

const labels: LokiLabelConfig = {
  static: { job: 'vercel' },
  fromFields: ['projectName', 'environment', 'source', 'level'],
};

function event(overrides: Record<string, string | number> = {}) {
  return {
    id: 'e1',
    timestamp: 1573817250283,
    source: 'lambda',
    projectId: 'p1',
    projectName: 'my-app',
    environment: 'production',
    level: 'info',
    message: 'hello',
    ...overrides,
  };
}

describe('sanitizeLabelName', () => {
  it('replaces dots with underscores', () => {
    expect(sanitizeLabelName('trace.id')).toBe('trace_id');
  });

  it('prefixes a name starting with a digit', () => {
    expect(sanitizeLabelName('2fast')).toBe('_2fast');
  });

  it('strips characters Loki disallows', () => {
    expect(sanitizeLabelName('my-label!')).toBe('my_label_');
  });

  it('leaves a valid name unchanged', () => {
    expect(sanitizeLabelName('project_name')).toBe('project_name');
  });
});

describe('resolveLabels', () => {
  it('merges static labels with allowlisted fields', () => {
    expect(resolveLabels(event(), labels)).toEqual({
      job: 'vercel',
      projectName: 'my-app',
      environment: 'production',
      source: 'lambda',
      level: 'info',
    });
  });

  it('omits fields that are missing rather than sending empty values', () => {
    const { environment: _dropped, ...withoutEnvironment } = event();
    const resolved = resolveLabels(withoutEnvironment, labels);
    expect(resolved).not.toHaveProperty('environment');
  });

  it('omits empty-string values, which Loki rejects', () => {
    const resolved = resolveLabels(event({ environment: '' }), labels);
    expect(resolved).not.toHaveProperty('environment');
  });

  it('stringifies numeric field values', () => {
    const resolved = resolveLabels(event({ statusCode: 200 }), {
      static: {},
      fromFields: ['statusCode'],
    });
    expect(resolved).toEqual({ statusCode: '200' });
  });

  it('skips object-valued fields, which cannot be labels', () => {
    const withProxy = { ...event(), proxy: { method: 'GET' } };
    const resolved = resolveLabels(withProxy, { static: {}, fromFields: ['proxy'] });
    expect(resolved).toEqual({});
  });

  it('sanitizes field names into label names', () => {
    const withTrace = { ...event(), 'trace.id': 'abc' };
    const resolved = resolveLabels(withTrace, { static: {}, fromFields: ['trace.id'] });
    expect(resolved).toEqual({ trace_id: 'abc' });
  });

  it('truncates over-long label values', () => {
    const resolved = resolveLabels(event({ message: 'x'.repeat(2000) }), {
      static: {},
      fromFields: ['message'],
    });
    expect(resolved['message']?.length).toBe(1024);
  });
});

describe('buildPushPayload', () => {
  it('groups events with identical labels into one stream', () => {
    const payload = buildPushPayload([event({ id: 'a' }), event({ id: 'b' })], labels);
    expect(payload.streams).toHaveLength(1);
    expect(payload.streams[0]?.values).toHaveLength(2);
  });

  it('separates events with different labels into different streams', () => {
    const payload = buildPushPayload(
      [event({ id: 'a', level: 'info' }), event({ id: 'b', level: 'error' })],
      labels,
    );
    expect(payload.streams).toHaveLength(2);
  });

  it('converts milliseconds to a nanosecond string', () => {
    const payload = buildPushPayload([event()], labels);
    expect(payload.streams[0]?.values[0]?.[0]).toBe('1573817250283000000');
  });

  it('sorts values ascending by timestamp within a stream', () => {
    const payload = buildPushPayload(
      [event({ id: 'late', timestamp: 2000 }), event({ id: 'early', timestamp: 1000 })],
      labels,
    );
    const values = payload.streams[0]?.values ?? [];
    expect(values[0]?.[0]).toBe('1000000000');
    expect(values[1]?.[0]).toBe('2000000000');
  });

  it('serializes the complete event as the log line, including label fields', () => {
    const payload = buildPushPayload([event()], labels);
    const line = JSON.parse(payload.streams[0]?.values[0]?.[1] ?? '{}');
    expect(line).toMatchObject({ id: 'e1', projectName: 'my-app', message: 'hello' });
  });

  it('returns no streams for an empty batch', () => {
    expect(buildPushPayload([], labels).streams).toEqual([]);
  });
});

describe('labelWarnings', () => {
  it('warns about high-cardinality fields', () => {
    const warnings = labelWarnings({ static: {}, fromFields: ['requestId', 'source'] });
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain('requestId');
  });

  it('returns nothing for a safe label set', () => {
    expect(labelWarnings(labels)).toEqual([]);
  });
});

describe('normalizePushUrl', () => {
  it('appends the push path to a base URL', () => {
    expect(normalizePushUrl('http://loki:3100')).toBe('http://loki:3100/loki/api/v1/push');
  });

  it('tolerates a trailing slash', () => {
    expect(normalizePushUrl('http://loki:3100/')).toBe('http://loki:3100/loki/api/v1/push');
  });

  it('does not double-append an already complete URL', () => {
    expect(normalizePushUrl('http://loki:3100/loki/api/v1/push')).toBe(
      'http://loki:3100/loki/api/v1/push',
    );
  });

  it('preserves a path prefix', () => {
    expect(normalizePushUrl('http://gw/loki-tenant')).toBe(
      'http://gw/loki-tenant/loki/api/v1/push',
    );
  });
});
