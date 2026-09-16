import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdir, mkdtemp, readFile, readdir, realpath, rm, symlink } from 'node:fs/promises';
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

  // Issue #8: `toISOString` switches to ISO 8601's expanded-year form
  // (`+YYYYYY-...`) once the year no longer fits in four digits, and the
  // 10-character slice then lands on `+YYYYYY-MM` instead of `YYYY-MM-DD`.
  // `pruneRetention`'s filename pattern has to recognize exactly this shape,
  // so the boundary matters to the millisecond: one side must still be the
  // four-digit form, the other must already be expanded.
  it.each([
    ['the last four-digit-year instant', 253_402_300_799_999, '9999-12-31'],
    ['a mid-range far-future instant', 1e15, '+033658-09'],
  ])('formats %s as %s', (_label, timestamp, expected) => {
    expect(utcDateKey(timestamp)).toBe(expected);
  });

  it('switches to expanded-year notation one millisecond after the last four-digit year', () => {
    expect(utcDateKey(253_402_300_799_999)).toBe('9999-12-31');
    expect(utcDateKey(253_402_300_800_000)).toBe('+010000-01');
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
  // Real directories, not string fixtures: containment is resolved with
  // realpath (spec §4), so it has to run against a filesystem that can
  // actually hold a symlink. `root` is itself realpath'd, because on macOS
  // the temp directory is reached through a symlink (/var -> /private/var)
  // and every expectation below would otherwise compare the two spellings.
  let base = '';
  let root = '';

  beforeEach(async () => {
    base = await realpath(await mkdtemp(join(tmpdir(), 'vld-logsroot-')));
    root = join(base, 'logs');
    await mkdir(root, { recursive: true });
  });

  afterEach(async () => {
    await rm(base, { recursive: true, force: true });
  });

  it('accepts a directory under the root', async () => {
    await mkdir(join(root, 'app'), { recursive: true });
    expect(await resolveLogsDirectory(join(root, 'app'), root)).toBe(join(root, 'app'));
  });

  it('accepts a directory under the root that does not exist yet', async () => {
    // The common case when saving a new sink: `deliver()` mkdirs it later.
    // Plain realpath would throw ENOENT here and reject a valid config.
    expect(await resolveLogsDirectory(join(root, 'not-created-yet', 'deeper'), root)).toBe(
      join(root, 'not-created-yet', 'deeper'),
    );
  });

  it('accepts the root itself', async () => {
    expect(await resolveLogsDirectory(root, root)).toBe(root);
  });

  it('rejects a traversal escape', async () => {
    await expect(resolveLogsDirectory(join(root, '..', 'config'), root)).rejects.toThrow(
      /outside/i,
    );
  });

  it('rejects an unrelated absolute path', async () => {
    await expect(resolveLogsDirectory(join(base, 'config'), root)).rejects.toThrow(/outside/i);
  });

  it('rejects a sibling with a matching name prefix', async () => {
    await expect(resolveLogsDirectory(`${root}-evil`, root)).rejects.toThrow(/outside/i);
  });

  it('rejects a symlink under the root that points outside it', async () => {
    // The lexical check accepted this, and the sink then wrote outside the
    // logs volume: /logs is a shared mount (a shipping sidecar, a backup
    // agent, an operator with host shell access), so a symlink appearing in
    // it is not exotic. Measured before the fix: both the link and a path
    // under it were ACCEPTED.
    const outside = join(base, 'outside');
    await mkdir(outside, { recursive: true });
    await symlink(outside, join(root, 'escape'));

    await expect(resolveLogsDirectory(join(root, 'escape'), root)).rejects.toThrow(/outside/i);
    await expect(resolveLogsDirectory(join(root, 'escape', 'sub'), root)).rejects.toThrow(
      /outside/i,
    );
  });

  it('accepts a symlink that resolves back inside the root', async () => {
    // The other direction, so the fix is containment rather than a blanket
    // ban on symlinks: a link pointing at a real directory inside the root
    // is fine, and resolves to its target.
    const real = join(root, 'real');
    await mkdir(real, { recursive: true });
    await symlink(real, join(root, 'alias'));

    expect(await resolveLogsDirectory(join(root, 'alias'), root)).toBe(real);
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

  it('recreates the directory if it is removed mid-life of the same sink', async () => {
    // The failure this guards against is process-scoped: a cached
    // "directory exists" flag on a LIVE instance. A test that builds a fresh
    // sink after the removal cannot reproduce it, because a new instance has
    // a fresh flag — it must be the same instance, and it must write to a NEW
    // date so no cached handle masks the missing directory. Verified: against
    // a cached-flag implementation this fails with ENOENT.
    const sink = fileSinkType.create('local', config(), { log: silentLog });
    await sink.deliver([event('before', Date.UTC(2026, 2, 1))]);

    await rm(dir, { recursive: true, force: true });

    await sink.deliver([event('after', Date.UTC(2026, 2, 2))]);
    await sink.close();

    const contents = await readFile(join(dir, 'events-2026-03-02.jsonl'), 'utf8');
    expect(JSON.parse(contents.trimEnd())).toMatchObject({ id: 'after' });
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
