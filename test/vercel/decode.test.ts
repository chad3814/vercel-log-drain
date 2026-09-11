import { describe, expect, it } from 'vitest';
import { gzip } from 'node:zlib';
import { promisify } from 'node:util';
import { decodeBody, PayloadTooLargeError, WHOLE_BODY_INDEX } from '../../src/vercel/decode.js';

const gzipAsync = promisify(gzip);

const eventA = { id: 'a', timestamp: 1573817187330, source: 'build', projectId: 'p1' };
const eventB = { id: 'b', timestamp: 1573817250283, source: 'lambda', projectId: 'p1' };
const options = { gzipped: false, maxDecompressedBytes: 1_000_000 };

describe('decodeBody', () => {
  it('decodes a JSON array body', async () => {
    const raw = Buffer.from(JSON.stringify([eventA, eventB]), 'utf8');
    const result = await decodeBody(raw, options);
    expect(result.events.map((e) => e['id'])).toEqual(['a', 'b']);
    expect(result.rejected).toEqual([]);
  });

  it('decodes an NDJSON body', async () => {
    const raw = Buffer.from(`${JSON.stringify(eventA)}\n${JSON.stringify(eventB)}\n`, 'utf8');
    const result = await decodeBody(raw, options);
    expect(result.events.map((e) => e['id'])).toEqual(['a', 'b']);
    expect(result.rejected).toEqual([]);
  });

  it('sniffs the format rather than trusting a content type', async () => {
    const leadingWhitespace = Buffer.from(`\n  ${JSON.stringify([eventA])}`, 'utf8');
    const result = await decodeBody(leadingWhitespace, options);
    expect(result.events).toHaveLength(1);
  });

  it('ignores blank lines in NDJSON', async () => {
    const raw = Buffer.from(`${JSON.stringify(eventA)}\n\n   \n${JSON.stringify(eventB)}\n`, 'utf8');
    const result = await decodeBody(raw, options);
    expect(result.events).toHaveLength(2);
    expect(result.rejected).toEqual([]);
  });

  it('decompresses a gzipped body', async () => {
    const raw = await gzipAsync(Buffer.from(JSON.stringify([eventA]), 'utf8'));
    const result = await decodeBody(raw, { gzipped: true, maxDecompressedBytes: 1_000_000 });
    expect(result.events).toHaveLength(1);
  });

  it('keeps good NDJSON entries and reports bad ones', async () => {
    const raw = Buffer.from(
      `${JSON.stringify(eventA)}\n{not json\n${JSON.stringify(eventB)}\n`,
      'utf8',
    );
    const result = await decodeBody(raw, options);
    expect(result.events.map((e) => e['id'])).toEqual(['a', 'b']);
    expect(result.rejected).toHaveLength(1);
    expect(result.rejected[0]?.index).toBe(1);
    expect(result.rejected[0]?.snippet).toContain('not json');
  });

  it('keeps good array entries and reports schema-invalid ones', async () => {
    const raw = Buffer.from(JSON.stringify([eventA, { id: 'missing-fields' }]), 'utf8');
    const result = await decodeBody(raw, options);
    expect(result.events.map((e) => e['id'])).toEqual(['a']);
    expect(result.rejected).toHaveLength(1);
    expect(result.rejected[0]?.index).toBe(1);
  });

  it('truncates long snippets so a huge line cannot bloat memory', async () => {
    const raw = Buffer.from(`{"broken":"${'x'.repeat(5000)}"\n`, 'utf8');
    const result = await decodeBody(raw, options);
    expect(result.rejected).toHaveLength(1);
    expect(result.rejected[0]?.snippet.length).toBeLessThanOrEqual(200);
  });

  it('rejects a body that decompresses beyond the cap', async () => {
    const raw = await gzipAsync(Buffer.alloc(200_000, 0x61));
    await expect(
      decodeBody(raw, { gzipped: true, maxDecompressedBytes: 1000 }),
    ).rejects.toBeInstanceOf(PayloadTooLargeError);
  });

  it('returns an empty result for an empty body', async () => {
    const result = await decodeBody(Buffer.alloc(0), options);
    expect(result.events).toEqual([]);
    expect(result.rejected).toEqual([]);
  });

  it('reports a whole-body parse failure with the whole-body sentinel index', async () => {
    const raw = Buffer.from('[{"id":"a"},', 'utf8');
    const result = await decodeBody(raw, options);
    expect(result.events).toEqual([]);
    expect(result.rejected).toHaveLength(1);
    expect(result.rejected[0]?.index).toBe(WHOLE_BODY_INDEX);
  });

  it('distinguishes a whole-body failure from a first-entry failure by index', async () => {
    const wholeBody = await decodeBody(Buffer.from('[{"id":"a"},', 'utf8'), options);
    const firstEntry = await decodeBody(
      Buffer.from(JSON.stringify([{ id: 'no-required-fields' }, eventB]), 'utf8'),
      options,
    );
    expect(wholeBody.rejected[0]?.index).toBe(WHOLE_BODY_INDEX);
    expect(firstEntry.rejected[0]?.index).toBe(0);
    expect(firstEntry.events.map((e) => e['id'])).toEqual(['b']);
  });

  it('reports a non-array JSON body with the whole-body sentinel index', async () => {
    const result = await decodeBody(Buffer.from('{"not":"an array"}', 'utf8'), options);
    expect(result.rejected[0]?.index).toBe(WHOLE_BODY_INDEX);
  });

  it('reports a corrupt gzip body as a reject, not as PayloadTooLargeError', async () => {
    const notGzip = Buffer.from('this is not gzip at all', 'utf8');
    const result = await decodeBody(notGzip, { gzipped: true, maxDecompressedBytes: 1_000_000 });
    expect(result.events).toEqual([]);
    expect(result.rejected).toHaveLength(1);
    expect(result.rejected[0]?.index).toBe(WHOLE_BODY_INDEX);
    expect(result.rejected[0]?.reason).toMatch(/inflation failed/);
  });

  it('reports a truncated gzip stream as a reject, not as PayloadTooLargeError', async () => {
    const full = await gzipAsync(Buffer.from(JSON.stringify([eventA]), 'utf8'));
    const truncated = full.subarray(0, full.length - 4);
    const result = await decodeBody(truncated, { gzipped: true, maxDecompressedBytes: 1_000_000 });
    expect(result.rejected).toHaveLength(1);
    expect(result.rejected[0]?.index).toBe(WHOLE_BODY_INDEX);
  });
});
