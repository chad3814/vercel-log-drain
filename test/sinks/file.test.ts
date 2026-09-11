import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import {
  dailyFileName,
  fileSinkType,
  groupByUtcDate,
  resolveLogsDirectory,
  utcDateKey,
} from '../../src/sinks/file.js';
import type { FileSinkConfig } from '../../src/sinks/file.js';

const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));

// 2019-11-15T11:26:27.330Z and 2019-11-16T00:00:01.000Z
const beforeMidnight = 1573817187330;
const afterMidnight = 1573862401000;

function event(id: string, timestampMs: number) {
  return { id, timestamp: timestampMs, source: 'lambda', projectId: 'p1' };
}

describe('utcDateKey', () => {
  it('formats a UTC date key', () => {
    expect(utcDateKey(beforeMidnight)).toBe('2019-11-15');
  });

  it('uses UTC, not local time', () => {
    // 2020-01-01T00:30:00Z is still 2019-12-31 in US timezones.
    expect(utcDateKey(Date.UTC(2020, 0, 1, 0, 30, 0))).toBe('2020-01-01');
  });
});

describe('dailyFileName', () => {
  it('composes prefix and date', () => {
    expect(dailyFileName('events', beforeMidnight)).toBe('events-2019-11-15.jsonl');
  });
});

describe('groupByUtcDate', () => {
  it('splits a batch that straddles midnight', () => {
    const grouped = groupByUtcDate([
      event('a', beforeMidnight),
      event('b', afterMidnight),
      event('c', beforeMidnight),
    ]);
    expect([...grouped.keys()].toSorted()).toEqual(['2019-11-15', '2019-11-16']);
    expect(grouped.get('2019-11-15')).toHaveLength(2);
    expect(grouped.get('2019-11-16')).toHaveLength(1);
  });
});

describe('resolveLogsDirectory', () => {
  it('accepts a directory under the root', () => {
    expect(resolveLogsDirectory('/logs/app', '/logs')).toBe('/logs/app');
  });

  it('accepts the root itself', () => {
    expect(resolveLogsDirectory('/logs', '/logs')).toBe('/logs');
  });

  it('rejects a traversal escape', () => {
    expect(() => resolveLogsDirectory('/logs/../config', '/logs')).toThrow(/outside/i);
  });

  it('rejects an unrelated absolute path', () => {
    expect(() => resolveLogsDirectory('/config', '/logs')).toThrow(/outside/i);
  });

  it('rejects a sibling with a matching name prefix', () => {
    expect(() => resolveLogsDirectory('/logs-evil', '/logs')).toThrow(/outside/i);
  });
});

describe('fileSinkType', () => {
  let dir = '';

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'vld-file-'));
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  function config(overrides: Partial<FileSinkConfig> = {}): FileSinkConfig {
    return {
      type: 'file',
      directory: dir,
      filePrefix: 'events',
      retentionDays: 14,
      freeSpaceFloorBytes: 0,
      ...overrides,
    };
  }

  it('writes one JSON line per event', async () => {
    const sink = fileSinkType.create('local', config(), { log: silentLog });
    await sink.deliver([event('a', beforeMidnight), event('b', beforeMidnight)]);
    await sink.close();

    const contents = await readFile(join(dir, 'events-2019-11-15.jsonl'), 'utf8');
    const lines = contents.trimEnd().split('\n');
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[0] ?? '')).toMatchObject({ id: 'a' });
    expect(JSON.parse(lines[1] ?? '')).toMatchObject({ id: 'b' });
  });

  it('partitions by event timestamp, not wall clock', async () => {
    const sink = fileSinkType.create('local', config(), { log: silentLog });
    await sink.deliver([event('a', beforeMidnight), event('b', afterMidnight)]);
    await sink.close();

    const files = (await readdir(dir)).toSorted();
    expect(files).toEqual(['events-2019-11-15.jsonl', 'events-2019-11-16.jsonl']);
  });

  it('appends across separate deliveries', async () => {
    const sink = fileSinkType.create('local', config(), { log: silentLog });
    await sink.deliver([event('a', beforeMidnight)]);
    await sink.deliver([event('b', beforeMidnight)]);
    await sink.close();

    const contents = await readFile(join(dir, 'events-2019-11-15.jsonl'), 'utf8');
    expect(contents.trimEnd().split('\n')).toHaveLength(2);
  });

  it('creates the directory if it does not exist', async () => {
    const nested = join(dir, 'deep', 'nested');
    const sink = fileSinkType.create('local', config({ directory: nested }), { log: silentLog });
    await sink.deliver([event('a', beforeMidnight)]);
    await sink.close();
    expect(await readdir(nested)).toContain('events-2019-11-15.jsonl');
  });

  it('keeps writing correctly across more dates than the handle cache holds', async () => {
    const sink = fileSinkType.create('local', config(), { log: silentLog });
    const days = [0, 1, 2, 3, 4].map((offset) => Date.UTC(2026, 8, 1 + offset));
    for (const [index, timestamp] of days.entries()) {
      await sink.deliver([event(`d${String(index)}`, timestamp)]);
    }
    // Re-touch the first date after it must have been evicted.
    await sink.deliver([event('again', days[0] ?? 0)]);
    await sink.close();

    const files = (await readdir(dir)).toSorted();
    expect(files).toHaveLength(5);
    const first = await readFile(join(dir, 'events-2026-09-01.jsonl'), 'utf8');
    expect(first.trimEnd().split('\n')).toHaveLength(2);
  });

  it('handles a single batch spanning more dates than the handle cache holds', async () => {
    // Distinct from the test above: that one makes a separate deliver() call
    // per date, so eviction happens BETWEEN calls. Here one batch spans five
    // dates, so eviction happens mid-loop, inside a single deliver().
    const sink = fileSinkType.create('local', config(), { log: silentLog });
    const events = [0, 1, 2, 3, 4].map((offset) =>
      event(`b${String(offset)}`, Date.UTC(2026, 5, 1 + offset)),
    );
    await sink.deliver(events);
    await sink.close();

    const files = (await readdir(dir)).toSorted();
    expect(files).toHaveLength(5);
    for (const [offset, name] of files.entries()) {
      const contents = await readFile(join(dir, name), 'utf8');
      expect(contents.trimEnd().split('\n')).toHaveLength(1);
      expect(JSON.parse(contents.trimEnd())).toMatchObject({ id: `b${String(offset)}` });
    }
  });

  it('recreates the directory if it is removed between deliveries', async () => {
    const sink = fileSinkType.create('local', config(), { log: silentLog });
    await sink.deliver([event('first', beforeMidnight)]);
    await sink.close();

    // Simulate an operator cleanup or a volume remount.
    await rm(dir, { recursive: true, force: true });

    const revived = fileSinkType.create('local', config(), { log: silentLog });
    await revived.deliver([event('second', beforeMidnight)]);
    await revived.close();

    const contents = await readFile(join(dir, 'events-2019-11-15.jsonl'), 'utf8');
    expect(JSON.parse(contents.trimEnd())).toMatchObject({ id: 'second' });
  });

  it('reports no warnings for a valid config', () => {
    expect(fileSinkType.warnings(config())).toEqual([]);
  });

  it('validates its config schema', () => {
    expect(fileSinkType.configSchema.safeParse(config()).success).toBe(true);
    expect(fileSinkType.configSchema.safeParse({ ...config(), retentionDays: -1 }).success).toBe(
      false,
    );
  });
});
