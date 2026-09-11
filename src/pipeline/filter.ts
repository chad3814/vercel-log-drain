import { eventLevel, levelRank } from '../vercel/event.js';
import type { SinkFilter } from '../config/schema.js';
import type { LogEvent } from '../vercel/event.js';

export type EventPredicate = (event: LogEvent) => boolean;

/**
 * Reads a field as a string, or null if it is absent or any non-string JSON
 * value. Used for EVERY field-based predicate, including `source` and
 * `projectId` which the event schema currently declares as required strings.
 * Going through this helper uniformly costs nothing and means a later schema
 * change — making a field optional, or widening it — degrades to "does not
 * match" rather than silently comparing a non-string against a string Set.
 */
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
    checks.push((event) => {
      const value = stringField(event, 'source');
      return value !== null && allowed.has(value);
    });
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
    checks.push((event) => {
      const value = stringField(event, 'projectId');
      return value !== null && allowed.has(value);
    });
  }

  if (checks.length === 0) return () => true;
  return (event) => checks.every((check) => check(event));
}
