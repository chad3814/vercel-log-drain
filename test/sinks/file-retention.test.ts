import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, readdir, rm, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Writable } from 'node:stream';
import { createLogger } from '../../src/log.js';
import { fileSinkType, pruneRetention } from '../../src/sinks/file.js';
import { RetryableDeliveryError } from '../../src/sinks/types.js';
import type { FileSinkConfig } from '../../src/sinks/file.js';

const silentLog = createLogger('silent', new Writable({ write: (_c, _e, cb) => cb() }));
const now = Date.UTC(2026, 8, 8, 12, 0, 0); // 2026-09-08T12:00:00Z

describe('pruneRetention', () => {
  let dir = '';

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'vld-retain-'));
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  // Retention requires BOTH an expired filename date AND a stale mtime, so a
  // test file must be aged explicitly — a freshly written one is treated as
  // replayed data and deliberately spared.
  async function agedFile(name: string, ageDays: number): Promise<void> {
    const path = join(dir, name);
    await writeFile(path, '');
    const when = new Date(now - ageDays * 86_400_000);
    await utimes(path, when, when);
  }

  it('deletes files older than the retention window and keeps newer ones', async () => {
    await agedFile('events-2026-09-08.jsonl', 0);
    await agedFile('events-2026-09-06.jsonl', 2);
    await agedFile('events-2026-09-01.jsonl', 7);
    await agedFile('events-2026-08-20.jsonl', 19);

    const deleted = await pruneRetention(dir, 'events', 3, now);

    expect(deleted.toSorted()).toEqual(['events-2026-08-20.jsonl', 'events-2026-09-01.jsonl']);
    expect((await readdir(dir)).toSorted()).toEqual([
      'events-2026-09-06.jsonl',
      'events-2026-09-08.jsonl',
    ]);
  });

  it('never touches files that do not match the pattern', async () => {
    await agedFile('events-2020-01-01.jsonl', 2000);
    await agedFile('important-notes.txt', 2000);
    await agedFile('events-not-a-date.jsonl', 2000);
    await agedFile('other-2020-01-01.jsonl', 2000);

    const deleted = await pruneRetention(dir, 'events', 1, now);

    expect(deleted).toEqual(['events-2020-01-01.jsonl']);
    expect((await readdir(dir)).toSorted()).toEqual([
      'events-not-a-date.jsonl',
      'important-notes.txt',
      'other-2020-01-01.jsonl',
    ]);
  });

  it('escapes regex metacharacters in the prefix', async () => {
    // The config schema permits '.' in a prefix. Unescaped it is a regex
    // wildcard, so `events.log` would also match `eventsXlog-…` and delete a
    // file belonging to someone else.
    await agedFile('events.log-2020-01-01.jsonl', 2000);
    await agedFile('eventsXlog-2020-01-01.jsonl', 2000);

    const deleted = await pruneRetention(dir, 'events.log', 1, now);

    expect(deleted).toEqual(['events.log-2020-01-01.jsonl']);
    expect(await readdir(dir)).toEqual(['eventsXlog-2020-01-01.jsonl']);
  });

  it('spares a file whose name is expired but whose contents just arrived', async () => {
    // The replay case: a batch delayed past the retention window still carries
    // its original event dates, so the file it lands in looks expired the
    // instant it is written. Deleting it would discard data we reported as
    // delivered — and on POSIX an unlink beneath an open handle does not even
    // error, so the loss would be silent.
    await agedFile('events-2020-01-01.jsonl', 0); // expired name, fresh mtime

    const deleted = await pruneRetention(dir, 'events', 1, now);

    expect(deleted).toEqual([]);
    expect(await readdir(dir)).toEqual(['events-2020-01-01.jsonl']);
  });

  it('spares a date that is currently held open', async () => {
    await agedFile('events-2020-01-01.jsonl', 2000);

    const deleted = await pruneRetention(dir, 'events', 1, now, new Set(['2020-01-01']));

    expect(deleted).toEqual([]);
    expect(await readdir(dir)).toEqual(['events-2020-01-01.jsonl']);
  });

  it('skips a shape-valid but impossible calendar date', async () => {
    // Date.parse rolls 2026-02-30 over to 2026-03-02 instead of failing, so a
    // NaN check alone would compare the wrong effective date.
    await agedFile('events-2026-02-30.jsonl', 2000);

    const deleted = await pruneRetention(dir, 'events', 1, now);

    expect(deleted).toEqual([]);
  });

  it('deletes nothing when retentionDays is 0, meaning keep forever', async () => {
    await writeFile(join(dir, 'events-2001-01-01.jsonl'), '');
    expect(await pruneRetention(dir, 'events', 0, now)).toEqual([]);
    expect(await readdir(dir)).toHaveLength(1);
  });

  it('returns an empty list for a missing directory rather than throwing', async () => {
    expect(await pruneRetention(join(dir, 'absent'), 'events', 3, now)).toEqual([]);
  });
});

describe('file sink free-space guard', () => {
  let dir = '';

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'vld-space-'));
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  function config(overrides: Partial<FileSinkConfig> = {}): FileSinkConfig {
    return {
      type: 'file',
      directory: dir,
      filePrefix: 'events',
      retentionDays: 0,
      freeSpaceFloorBytes: 1_000_000,
      ...overrides,
    };
  }

  const event = { id: 'a', timestamp: Date.UTC(2026, 8, 8), source: 'lambda', projectId: 'p1' };

  it('throws a retryable error when free space is below the floor', async () => {
    const sink = fileSinkType.create('local', config(), {
      log: silentLog,
      freeSpace: () => Promise.resolve(500_000),
    });
    await expect(sink.deliver([event])).rejects.toBeInstanceOf(RetryableDeliveryError);
    await sink.close();
    expect(await readdir(dir)).toEqual([]);
  });

  it('writes normally when free space is above the floor', async () => {
    const sink = fileSinkType.create('local', config(), {
      log: silentLog,
      freeSpace: () => Promise.resolve(50_000_000),
    });
    await sink.deliver([event]);
    await sink.close();
    expect(await readdir(dir)).toEqual(['events-2026-09-08.jsonl']);
  });

  it('writes normally when the floor is zero', async () => {
    const sink = fileSinkType.create('local', config({ freeSpaceFloorBytes: 0 }), {
      log: silentLog,
      freeSpace: () => Promise.resolve(0),
    });
    await sink.deliver([event]);
    await sink.close();
    expect(await readdir(dir)).toEqual(['events-2026-09-08.jsonl']);
  });
});
