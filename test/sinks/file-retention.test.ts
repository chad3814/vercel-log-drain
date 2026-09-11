import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, readdir, rm, writeFile } from 'node:fs/promises';
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

  it('deletes files older than the retention window and keeps newer ones', async () => {
    await writeFile(join(dir, 'events-2026-09-08.jsonl'), '');
    await writeFile(join(dir, 'events-2026-09-06.jsonl'), '');
    await writeFile(join(dir, 'events-2026-09-01.jsonl'), '');
    await writeFile(join(dir, 'events-2026-08-20.jsonl'), '');

    const deleted = await pruneRetention(dir, 'events', 3, now);

    expect(deleted.toSorted()).toEqual(['events-2026-08-20.jsonl', 'events-2026-09-01.jsonl']);
    expect((await readdir(dir)).toSorted()).toEqual([
      'events-2026-09-06.jsonl',
      'events-2026-09-08.jsonl',
    ]);
  });

  it('never touches files that do not match the pattern', async () => {
    await writeFile(join(dir, 'events-2020-01-01.jsonl'), '');
    await writeFile(join(dir, 'important-notes.txt'), '');
    await writeFile(join(dir, 'events-not-a-date.jsonl'), '');
    await writeFile(join(dir, 'other-2020-01-01.jsonl'), '');

    const deleted = await pruneRetention(dir, 'events', 1, now);

    expect(deleted).toEqual(['events-2020-01-01.jsonl']);
    expect((await readdir(dir)).toSorted()).toEqual([
      'events-not-a-date.jsonl',
      'important-notes.txt',
      'other-2020-01-01.jsonl',
    ]);
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
