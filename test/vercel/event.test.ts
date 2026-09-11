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
