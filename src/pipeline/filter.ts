import { eventLevel, levelRank } from '../vercel/event.js';
import type { SinkFilter } from '../config/schema.js';
import type { LogEvent } from '../vercel/event.js';

export type EventPredicate = (event: LogEvent) => boolean;

function stringField(event: LogEvent, field: string): string | null {
  const value = event[field];
  return typeof value === 'string' ? value : null;
}

export function compileFilter(filter: SinkFilter): EventPredicate {
  const checks: EventPredicate[] = [];

  if (filter.minLevel !== undefined) {
    const threshold = levelRank(filter.minLevel);
    checks.push((event) => levelRank(eventLevel(event)) >= threshold);
  }

  if (filter.sources !== undefined) {
    const allowed = new Set(filter.sources);
    checks.push((event) => allowed.has(event.source));
  }

  if (filter.environments !== undefined) {
    const allowed = new Set(filter.environments);
    checks.push((event) => {
      const value = stringField(event, 'environment');
      return value !== null && allowed.has(value);
    });
  }

  if (filter.projectIds !== undefined) {
    const allowed = new Set(filter.projectIds);
    checks.push((event) => allowed.has(event.projectId));
  }

  if (checks.length === 0) return () => true;
  return (event) => checks.every((check) => check(event));
}
