import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdir, mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { readSpoolDirStats, SpoolQueue } from '../../src/pipeline/spool.js';

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

  it('delivers in FIFO order', async () => {
    const queue = await SpoolQueue.open(dir, options());
    for (let index = 0; index < 12; index += 1) {
      await queue.enqueue([event(`e${String(index)}`)]);
    }
    const batch = await queue.nextBatch(1000, BIG);
    expect(batch?.events.map((e) => e['id'])).toEqual(
      Array.from({ length: 12 }, (_unused, index) => `e${String(index)}`),
    );
  });

  // The three tests below are what actually justify the 12-digit padding. The
  // FIFO test above cannot: twelve files are all the same width, so ordering
  // there would hold at any padding, including none. Crossing a decimal
  // boundary is where insufficient padding breaks, and seeding the names
  // directly exercises it through the real recovery path without writing a
  // thousand files.
  it('orders correctly across decimal digit boundaries', async () => {
    for (const seq of [998, 999, 1000, 1001, 9999, 10_000]) {
      await writeFile(
        join(dir, `${String(seq).padStart(12, '0')}.jsonl`),
        `${JSON.stringify(event(`e${String(seq)}`))}\n`,
      );
    }

    const queue = await SpoolQueue.open(dir, options());
    const batch = await queue.nextBatch(1000, BIG);

    expect(batch?.events.map((e) => e['id'])).toEqual([
      'e998',
      'e999',
      'e1000',
      'e1001',
      'e9999',
      'e10000',
    ]);
  });

  it('recovers the sequence counter past a boundary', async () => {
    await writeFile(
      join(dir, `${String(1000).padStart(12, '0')}.jsonl`),
      `${JSON.stringify(event('old'))}\n`,
    );

    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('new')]);

    // The new batch must take seq 1001 and therefore sort AFTER the old one.
    const batch = await queue.nextBatch(1000, BIG);
    expect(batch?.events.map((e) => e['id'])).toEqual(['old', 'new']);
  });

  it('would mis-order without the padding, which is why it is there', () => {
    // Pure demonstration of the failure mode: unpadded, '1000' sorts before
    // '999'. If this assertion ever flips, the padding has stopped mattering
    // and the ordering guarantee rests on nothing.
    const unpadded = [998, 999, 1000, 1001].map((n) => `${String(n)}.jsonl`);
    const padded = [998, 999, 1000, 1001].map((n) => `${String(n).padStart(12, '0')}.jsonl`);

    expect(unpadded.toSorted()).not.toEqual(unpadded);
    expect(padded.toSorted()).toEqual(padded);
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
      // Evicting to stay inside a sink's own budget is not a volume-level
      // loss, and must not be reported as one: the incoming batch was
      // written.
      expect(result.floorDrop).toBeNull();
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
    // The caller degrades the service on THIS, not on droppedEvents, which a
    // routine overflow eviction also moves -- see the assertion in the
    // overflow test above that floorDrop stays null there.
    expect(result.floorDrop).toEqual({ freeBytes: 500, floorBytes: 1_000_000 });
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
    const [name] = await readdir(dir).then((entries) =>
      entries.filter((e) => e.endsWith('.jsonl')),
    );
    await writeFile(join(dir, name ?? ''), 'this is not json\n');

    const batch = await queue.nextBatch(1000, BIG);
    expect(batch?.events).toEqual([]);
    expect(batch?.files).toHaveLength(1);
    // Acking a zero-event batch is what lets the queue self-heal past corruption.
    await queue.ack(batch!);
    expect(queue.fileCount()).toBe(0);
  });

  it('drops an already-spooled event whose timestamp no sink can render', async () => {
    // Heals a spool poisoned by an earlier build. One event with
    // `timestamp: 1e21` validated at ingest, reached the file sink, and threw
    // RangeError from new Date(ts).toISOString() -- classified retryable, so
    // the batch stayed at the head of the queue and was re-attempted forever,
    // surviving every restart. nextBatch re-validates each line it reads, so
    // bounding the schema means the poison line is now skipped and the good
    // one is still delivered.
    await writeFile(
      join(dir, '000000000000.jsonl'),
      `${JSON.stringify({ id: 'poison', timestamp: 1e21, source: 'lambda', projectId: 'p1' })}\n${JSON.stringify(event('good'))}\n`,
    );

    const queue = await SpoolQueue.open(dir, options());
    const batch = await queue.nextBatch(1000, BIG);

    expect(batch?.events.map((e) => e['id'])).toEqual(['good']);
    // Every event that survives is one the file sink can derive a date from,
    // which is the property the wedge violated.
    for (const parsed of batch?.events ?? []) {
      expect(() => new Date(parsed.timestamp).toISOString()).not.toThrow();
    }
  });

  it('reports the age of the head of the queue', async () => {
    const queue = await SpoolQueue.open(dir, options());
    expect(await queue.oldestMtimeMs()).toBeNull();
    await queue.enqueue([event('a')]);
    const mtime = await queue.oldestMtimeMs();
    expect(mtime).not.toBeNull();
    expect(Date.now() - (mtime ?? 0)).toBeLessThan(10_000);
  });

  it('reports dead-letter files and bytes, which no other figure includes', async () => {
    // `dead/` sits outside maxSpoolBytes by design (it is terminal storage
    // and makeRoom evicts by unlinking), and it was also excluded from
    // queue.bytes and from orphanedSpools[].bytes -- so it could grow
    // without bound with `counters.deadLettered`, an event count, as the only
    // trace. That is the chain that takes the spool volume below its floor.
    const queue = await SpoolQueue.open(dir, options());
    expect(await queue.deadStats()).toEqual({ files: 0, bytes: 0 });

    await queue.enqueue([event('a')]);
    const batch = await queue.nextBatch(1000, BIG);
    await queue.deadLetter(batch!);

    const dead = await queue.deadStats();
    expect(dead.files).toBe(1);
    expect(dead.bytes).toBe(batch?.bytes);
    // And the live figures still exclude them, so the two are not confused.
    expect(queue.fileCount()).toBe(0);
    expect(queue.bytes()).toBe(0);
  });

  it('reads live and dead figures off a spool directory with no queue open', async () => {
    // What a DISABLED sink's status is built from: there is no SpoolQueue for
    // one, so its whole backlog used to be reported as zero.
    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('doomed')]);
    const batch = await queue.nextBatch(1000, BIG);
    await queue.deadLetter(batch!);
    await queue.enqueue([event('waiting')]);
    const liveBytes = queue.bytes();

    const stats = await readSpoolDirStats(dir);

    expect(stats.files).toBe(1);
    expect(stats.bytes).toBe(liveBytes);
    expect(stats.dead.files).toBe(1);
    expect(stats.dead.bytes).toBeGreaterThan(0);
    expect(stats.oldestMtimeMs).not.toBeNull();
  });

  it('ignores names that are not batch files when reading a directory', async () => {
    // The live count must match what the queue itself tracks: a stray
    // README, a .tmp left by a crash, and the dead/ directory entry are all
    // things a naive readdir would count as undelivered batches.
    await writeFile(join(dir, 'README.txt'), 'not a batch');
    await writeFile(join(dir, '000000000009.jsonl.tmp'), 'not a batch either');
    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('real')]);

    const stats = await readSpoolDirStats(dir);
    expect(stats.files).toBe(1);
    expect(stats.bytes).toBe(queue.bytes());
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

  // Each of the three tests below fails against the pre-fix implementation.
  // Write them so they do: a regression test that also passes before the fix
  // documents nothing.

  it('does not overwrite an existing dead letter after a restart', async () => {
    // The live directory is empty after the first dead-letter, so a recovery
    // that only scans the live directory restarts the counter at 0 and reissues
    // a name `dead/` already holds -- and rename replaces the destination.
    const first = await SpoolQueue.open(dir, options());
    await first.enqueue([event('precious')]);
    const firstBatch = await first.nextBatch(1000, BIG);
    await first.deadLetter(firstBatch!);
    expect(await readdir(dir).then((f) => f.filter((x) => x !== 'dead'))).toEqual([]);

    const second = await SpoolQueue.open(dir, options());
    await second.enqueue([event('newer')]);
    const secondBatch = await second.nextBatch(1000, BIG);
    await second.deadLetter(secondBatch!);

    const dead = await readdir(join(dir, 'dead'));
    expect(dead).toHaveLength(2);
    const bodies = await Promise.all(dead.map((f) => readFile(join(dir, 'dead', f), 'utf8')));
    expect(bodies.some((b) => b.includes('precious'))).toBe(true);
    expect(bodies.some((b) => b.includes('newer'))).toBe(true);
  });

  it('evicts nothing when the new batch cannot be written', async () => {
    const queue = await SpoolQueue.open(dir, options());
    const first = await queue.enqueue([event('keep-me')]);

    // Budget is now exactly full, so the next enqueue wants to evict `keep-me`.
    const tight = await SpoolQueue.open(dir, options({ maxSpoolBytes: first.writtenBytes }));
    // Block the next sequence number's temp path with a directory: `open(..., 'w')`
    // on a directory fails with EISDIR, while the spool directory itself stays
    // writable -- so an eviction, if one were attempted, would succeed.
    await mkdir(join(dir, '000000000001.jsonl.tmp'));

    await expect(tight.enqueue([event('doomed')])).rejects.toThrow();

    const batch = await tight.nextBatch(1000, BIG);
    expect(batch?.events.map((e) => e['id'])).toEqual(['keep-me']);
  });

  it('evicts nothing when the new batch cannot be renamed into place', async () => {
    // Distinct from the test above: that one fails at the write, so it never
    // reaches the eviction at all. This one lets the write succeed and fails
    // the rename, which is the window where eviction used to have already run.
    const queue = await SpoolQueue.open(dir, options());
    const first = await queue.enqueue([event('keep-me')]);

    const tight = await SpoolQueue.open(dir, options({ maxSpoolBytes: first.writtenBytes }));
    // Block the next sequence number's FINAL path with a directory: renaming a
    // file onto a directory fails with EISDIR, after a clean write and fsync.
    await mkdir(join(dir, '000000000001.jsonl'));

    await expect(tight.enqueue([event('doomed')])).rejects.toThrow();

    const batch = await tight.nextBatch(1000, BIG);
    expect(batch?.events.map((e) => e['id'])).toEqual(['keep-me']);
  });

  it('stops tracking a batch it can no longer read', async () => {
    const queue = await SpoolQueue.open(dir, options());
    await queue.enqueue([event('unreadable')]);
    expect(queue.fileCount()).toBe(1);

    // Make the batch unreadable without removing the name: readFile on a
    // directory fails with EISDIR.
    const name = (await readdir(dir)).find((f) => f.endsWith('.jsonl'));
    await rm(join(dir, name!));
    await mkdir(join(dir, name!));

    expect(await queue.nextBatch(1000, BIG)).toBeNull();
    // A retained entry would overcount bytes for the rest of the process's
    // life and be re-read on every later call.
    expect(queue.fileCount()).toBe(0);
    expect(queue.bytes()).toBe(0);
  });
});
