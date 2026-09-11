import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SpoolQueue } from '../../src/pipeline/spool.js';

const BIG = 1_048_576;

function event(id: string, timestamp = 1000) {
  return { id, timestamp, source: 'lambda', projectId: 'p1' };
}

function options(overrides: Partial<{ maxSpoolBytes: number; freeSpaceFloorBytes: number }> = {}) {
  return { maxSpoolBytes: BIG, freeSpaceFloorBytes: 0, ...overrides };
}

describe('SpoolQueue', () => {
  let dir = '';

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'vld-spool-'));
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it('round-trips a batch through enqueue and nextBatch', async () => {
    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('a'), event('b')]);

    const batch = await queue.nextBatch(1000, BIG);
    expect(batch?.events.map((e) => e['id'])).toEqual(['a', 'b']);
    expect(batch?.files).toHaveLength(1);
  });

  it('returns null when empty', async () => {
    const queue = await SpoolQueue.open(dir, options());
    expect(await queue.nextBatch(1000, BIG)).toBeNull();
  });

  it('ack removes exactly the coalesced files', async () => {
    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('a')]);
    await queue.enqueue([event('b')]);
    await queue.enqueue([event('c')]);

    const batch = await queue.nextBatch(2, BIG);
    expect(batch?.files).toHaveLength(2);
    await queue.ack(batch!);

    expect(queue.fileCount()).toBe(1);
    const remaining = await queue.nextBatch(1000, BIG);
    expect(remaining?.events.map((e) => e['id'])).toEqual(['c']);
  });

  it('delivers in FIFO order across a padding boundary', async () => {
    const queue = await SpoolQueue.open(dir, options());
    for (let index = 0; index < 12; index += 1) {
      await queue.enqueue([event(`e${String(index)}`)]);
    }
    const batch = await queue.nextBatch(1000, BIG);
    expect(batch?.events.map((e) => e['id'])).toEqual(
      Array.from({ length: 12 }, (_unused, index) => `e${String(index)}`),
    );
  });

  it('coalesces up to maxEvents but always returns at least one file', async () => {
    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('a'), event('b'), event('c')]);

    // A single file already exceeds the limit; it must still be returned,
    // otherwise the queue would deadlock on its own head.
    const batch = await queue.nextBatch(1, BIG);
    expect(batch?.files).toHaveLength(1);
    expect(batch?.events).toHaveLength(3);
  });

  it('recovers sequence and byte accounting after reopening', async () => {
    const first = await SpoolQueue.open(dir, options());
    await first.enqueue([event('a')]);
    await first.enqueue([event('b')]);
    const bytesBefore = first.bytes();

    const second = await SpoolQueue.open(dir, options());
    expect(second.fileCount()).toBe(2);
    expect(second.bytes()).toBe(bytesBefore);

    await second.enqueue([event('c')]);
    const batch = await second.nextBatch(1000, BIG);
    expect(batch?.events.map((e) => e['id'])).toEqual(['a', 'b', 'c']);
  });

  it('deletes stray .tmp files on open and ignores unrelated files', async () => {
    await writeFile(join(dir, '000000000005.jsonl.tmp'), 'garbage');
    await writeFile(join(dir, 'README.txt'), 'not a batch');

    const queue = await SpoolQueue.open(dir, options());

    expect(queue.fileCount()).toBe(0);
    const entries = await readdir(dir);
    expect(entries).not.toContain('000000000005.jsonl.tmp');
    expect(entries).toContain('README.txt');
  });

  it('drops the oldest batches when the byte budget is exceeded', async () => {
    const queue = await SpoolQueue.open(dir, options({ maxSpoolBytes: 400 }));
    // Each event line is roughly 70 bytes.
    const first = await queue.enqueue([event('a1'), event('a2'), event('a3')]);
    expect(first.droppedEvents).toBe(0);

    let dropped = 0;
    for (let index = 0; index < 8; index += 1) {
      const result = await queue.enqueue([event(`b${String(index)}`)]);
      dropped += result.droppedEvents;
    }

    expect(dropped).toBeGreaterThan(0);
    expect(queue.bytes()).toBeLessThanOrEqual(400);
    // Newest data survives; the oldest was sacrificed.
    const batch = await queue.nextBatch(1000, BIG);
    const ids = batch?.events.map((e) => e['id']) ?? [];
    expect(ids).toContain('b7');
    expect(ids).not.toContain('a1');
  });

  it('drops the whole batch when free space is below the floor', async () => {
    const queue = await SpoolQueue.open(dir, {
      maxSpoolBytes: BIG,
      freeSpaceFloorBytes: 1_000_000,
      freeSpace: () => Promise.resolve(500),
    });

    const result = await queue.enqueue([event('a'), event('b')]);

    expect(result.droppedEvents).toBe(2);
    expect(result.writtenBytes).toBe(0);
    expect(queue.fileCount()).toBe(0);
  });

  it('writes normally when free space is above the floor', async () => {
    const queue = await SpoolQueue.open(dir, {
      maxSpoolBytes: BIG,
      freeSpaceFloorBytes: 1000,
      freeSpace: () => Promise.resolve(50_000_000),
    });
    const result = await queue.enqueue([event('a')]);
    expect(result.droppedEvents).toBe(0);
    expect(queue.fileCount()).toBe(1);
  });

  it('moves a batch to the dead directory', async () => {
    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('a')]);
    const batch = await queue.nextBatch(1000, BIG);

    await queue.deadLetter(batch!);

    expect(queue.fileCount()).toBe(0);
    expect(queue.bytes()).toBe(0);
    const dead = await readdir(join(dir, 'dead'));
    expect(dead).toHaveLength(1);
    expect(await readFile(join(dir, 'dead', dead[0] ?? ''), 'utf8')).toContain('"a"');
  });

  it('skips unparseable lines rather than looping forever', async () => {
    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('a')]);
    const [name] = await readdir(dir).then((entries) => entries.filter((e) => e.endsWith('.jsonl')));
    await writeFile(join(dir, name ?? ''), 'this is not json\n');

    const batch = await queue.nextBatch(1000, BIG);
    expect(batch?.events).toEqual([]);
    expect(batch?.files).toHaveLength(1);
    // Acking a zero-event batch is what lets the queue self-heal past corruption.
    await queue.ack(batch!);
    expect(queue.fileCount()).toBe(0);
  });

  it('reports the age of the head of the queue', async () => {
    const queue = await SpoolQueue.open(dir, options());
    expect(await queue.oldestMtimeMs()).toBeNull();
    await queue.enqueue([event('a')]);
    const mtime = await queue.oldestMtimeMs();
    expect(mtime).not.toBeNull();
    expect(Date.now() - (mtime ?? 0)).toBeLessThan(10_000);
  });

  it('discards everything including dead letters', async () => {
    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('a')]);
    const batch = await queue.nextBatch(1000, BIG);
    await queue.deadLetter(batch!);
    await queue.enqueue([event('b')]);

    await queue.discardAll();

    expect(queue.fileCount()).toBe(0);
    expect(queue.bytes()).toBe(0);
    expect(await readdir(join(dir, 'dead'))).toEqual([]);
  });
});
