import { describe, expect, it } from 'vitest';
import { eventLevel, levelRank, logEventSchema } from '../../src/vercel/event.js';

const validEvent = {
  id: '1573817187330377061717300000',
  timestamp: 1573817187330,
  source: 'lambda',
  projectId: 'gdufoJxB6b9b1fEqr1jUtFkyavUU',
  level: 'info',
  message: 'API request processed',
};

describe('logEventSchema', () => {
  it('accepts a documented Vercel event', () => {
    const result = logEventSchema.safeParse(validEvent);
    expect(result.success).toBe(true);
  });

  it('preserves unknown fields through the catchall', () => {
    const result = logEventSchema.safeParse({
      ...validEvent,
      proxy: { method: 'GET', statusCode: 200, userAgent: ['Mozilla/5.0'] },
      'trace.id': '1b02cd14bb8642fd092bc23f54c7ffcd',
    });
    expect(result.success).toBe(true);
    if (!result.success) return;
    expect(result.data['trace.id']).toBe('1b02cd14bb8642fd092bc23f54c7ffcd');
    expect(result.data['proxy']).toEqual({
      method: 'GET',
      statusCode: 200,
      userAgent: ['Mozilla/5.0'],
    });
  });

  it.each([
    ['id', { ...validEvent, id: undefined }],
    ['timestamp', { ...validEvent, timestamp: undefined }],
    ['source', { ...validEvent, source: undefined }],
    ['projectId', { ...validEvent, projectId: undefined }],
  ])('rejects an event missing %s', (_field, candidate) => {
    expect(logEventSchema.safeParse(candidate).success).toBe(false);
  });

  it('rejects a non-numeric timestamp', () => {
    expect(logEventSchema.safeParse({ ...validEvent, timestamp: '1573817187330' }).success).toBe(
      false,
    );
  });

  it.each([
    ['above the Date range', 1e21],
    ['just above the Date range', 8_640_000_000_000_001],
    ['negative', -1],
  ])('rejects a timestamp %s with one issue on timestamp', (_label, timestamp) => {
    // An otherwise-valid fixture and an issue count, per spec §12: a
    // doubly-invalid fixture, or a bare `success === false`, would pass with
    // this very bound deleted. `new Date(1e21).toISOString()` throws
    // RangeError, which used to escape the file sink as a plain retryable
    // error and pin the head of that sink's queue forever.
    const result = logEventSchema.safeParse({ ...validEvent, timestamp });
    expect(result.success).toBe(false);
    if (result.success) return;
    expect(result.error.issues).toHaveLength(1);
    expect(result.error.issues[0]?.path).toEqual(['timestamp']);
  });

  it.each([
    ['the epoch', 0],
    ['the largest representable instant', 8_640_000_000_000_000],
  ])('still accepts a timestamp at %s', (_label, timestamp) => {
    // The other direction: the bound must not reject a timestamp the sinks
    // can in fact render, or a lenient schema has quietly become a strict
    // one. Both of these round-trip through new Date(ms).toISOString().
    expect(logEventSchema.safeParse({ ...validEvent, timestamp }).success).toBe(true);
    expect(() => new Date(timestamp).toISOString()).not.toThrow();
  });

  it('rejects a non-JSON value in an unknown field', () => {
    expect(logEventSchema.safeParse({ ...validEvent, weird: () => 1 }).success).toBe(false);
  });
});

describe('eventLevel', () => {
  it.each([
    ['info', 'info'],
    ['warning', 'warning'],
    ['error', 'error'],
  ] as const)('maps %s to %s', (input, expected) => {
    expect(eventLevel({ ...validEvent, level: input })).toBe(expected);
  });

  it('treats a missing level as info', () => {
    const { level: _level, ...withoutLevel } = validEvent;
    expect(eventLevel(withoutLevel)).toBe('info');
  });

  it('treats an unrecognized level as info', () => {
    expect(eventLevel({ ...validEvent, level: 'trace' })).toBe('info');
  });

  it('normalizes case and the warn alias', () => {
    expect(eventLevel({ ...validEvent, level: 'WARN' })).toBe('warning');
    expect(eventLevel({ ...validEvent, level: 'Error' })).toBe('error');
  });
});

describe('levelRank', () => {
  it('orders info below warning below error', () => {
    expect(levelRank('info')).toBeLessThan(levelRank('warning'));
    expect(levelRank('warning')).toBeLessThan(levelRank('error'));
  });
});
