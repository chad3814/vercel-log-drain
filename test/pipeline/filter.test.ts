import { describe, expect, it } from 'vitest';
import { compileFilter } from '../../src/pipeline/filter.js';

function event(overrides: Record<string, string | number> = {}) {
  return {
    id: 'e1',
    timestamp: 1,
    source: 'lambda',
    projectId: 'p1',
    environment: 'production',
    level: 'info',
    ...overrides,
  };
}

describe('compileFilter', () => {
  it('matches everything when the filter is empty', () => {
    const predicate = compileFilter({});
    expect(predicate(event())).toBe(true);
    expect(predicate(event({ level: 'error', source: 'build' }))).toBe(true);
  });

  it('applies minLevel inclusively', () => {
    const predicate = compileFilter({ minLevel: 'warning' });
    expect(predicate(event({ level: 'info' }))).toBe(false);
    expect(predicate(event({ level: 'warning' }))).toBe(true);
    expect(predicate(event({ level: 'error' }))).toBe(true);
  });

  it('treats an event with no level as info for minLevel purposes', () => {
    const { level: _omit, ...withoutLevel } = event();
    expect(compileFilter({ minLevel: 'warning' })(withoutLevel)).toBe(false);
    expect(compileFilter({ minLevel: 'info' })(withoutLevel)).toBe(true);
  });

  it('filters by source', () => {
    const predicate = compileFilter({ sources: ['lambda', 'edge'] });
    expect(predicate(event({ source: 'lambda' }))).toBe(true);
    expect(predicate(event({ source: 'build' }))).toBe(false);
  });

  it('filters by environment', () => {
    const predicate = compileFilter({ environments: ['production'] });
    expect(predicate(event({ environment: 'production' }))).toBe(true);
    expect(predicate(event({ environment: 'preview' }))).toBe(false);
  });

  it('excludes an event with no environment when environments is set', () => {
    const { environment: _omit, ...withoutEnvironment } = event();
    expect(compileFilter({ environments: ['production'] })(withoutEnvironment)).toBe(false);
  });

  it('filters by projectId', () => {
    const predicate = compileFilter({ projectIds: ['p1'] });
    expect(predicate(event({ projectId: 'p1' }))).toBe(true);
    expect(predicate(event({ projectId: 'p2' }))).toBe(false);
  });

  it('requires all predicates to pass', () => {
    const predicate = compileFilter({
      minLevel: 'error',
      sources: ['lambda'],
      environments: ['production'],
    });
    expect(predicate(event({ level: 'error', source: 'lambda' }))).toBe(true);
    expect(predicate(event({ level: 'error', source: 'build' }))).toBe(false);
    expect(predicate(event({ level: 'info', source: 'lambda' }))).toBe(false);
  });

  it('treats an empty array as matching nothing, not everything', () => {
    // An empty allowlist is an explicit "no sources permitted"; absence is the
    // way to express "any source".
    expect(compileFilter({ sources: [] })(event())).toBe(false);
  });

  it('applies the empty-means-nothing rule to every array predicate', () => {
    expect(compileFilter({ environments: [] })(event())).toBe(false);
    expect(compileFilter({ projectIds: [] })(event())).toBe(false);
  });
});
