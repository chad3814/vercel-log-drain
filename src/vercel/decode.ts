import { gunzip } from 'node:zlib';
import { promisify } from 'node:util';
import { logEventSchema } from './event.js';
import type { LogEvent } from './event.js';

const gunzipAsync = promisify(gunzip);

const SNIPPET_LIMIT = 200;

/**
 * Index used for a reject that describes the WHOLE body rather than one entry:
 * a corrupt gzip stream, an unparseable JSON array, or a non-array body. A real
 * per-entry failure always carries its own non-negative index, so a consumer
 * can tell "nothing parsed" apart from "the first entry was invalid" — which a
 * shared index of 0 could not express.
 */
export const WHOLE_BODY_INDEX = -1;

export type RejectedEntry = { index: number; reason: string; snippet: string };
export type DecodeResult = { events: LogEvent[]; rejected: RejectedEntry[] };
export type DecodeOptions = { gzipped: boolean; maxDecompressedBytes: number };

export class PayloadTooLargeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PayloadTooLargeError';
  }
}

class CorruptBodyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CorruptBodyError';
  }
}

function errorCode(error: Error): string | undefined {
  // zlib errors have a .code property for error classification.
  const candidate: unknown = error;
  if (candidate && typeof candidate === 'object' && 'code' in candidate) {
    const code: unknown = (candidate as Record<string, unknown>).code;
    if (typeof code === 'string') {
      return code;
    }
  }
  return undefined;
}

function snippet(text: string): string {
  return text.length > SNIPPET_LIMIT ? `${text.slice(0, SNIPPET_LIMIT - 1)}…` : text;
}

function validateEntry(candidate: unknown, index: number, into: DecodeResult): void {
  const parsed = logEventSchema.safeParse(candidate);
  if (parsed.success) {
    into.events.push(parsed.data);
    return;
  }
  into.rejected.push({
    index,
    reason: parsed.error.issues.map((issue) => `${issue.path.join('.')}: ${issue.message}`).join('; '),
    snippet: snippet(JSON.stringify(candidate)),
  });
}

async function inflate(raw: Buffer, options: DecodeOptions): Promise<Buffer> {
  if (!options.gzipped) {
    if (raw.byteLength > options.maxDecompressedBytes) {
      throw new PayloadTooLargeError(`body of ${raw.byteLength} bytes exceeds cap`);
    }
    return raw;
  }
  try {
    return await gunzipAsync(raw, { maxOutputLength: options.maxDecompressedBytes });
  } catch (error) {
    const failure = error instanceof Error ? error : new Error(String(error));
    // zlib reports these distinctly, verified on Node 24:
    //   cap exceeded  -> RangeError, code ERR_BUFFER_TOO_LARGE
    //   not gzip      -> Error, code Z_DATA_ERROR ("incorrect header check")
    //   truncated     -> Error, code Z_BUF_ERROR ("unexpected end of file")
    // Collapsing all three into PayloadTooLargeError would report a corrupt
    // body as an oversized one and mislead whoever reads the status page.
    if (errorCode(failure) === 'ERR_BUFFER_TOO_LARGE') {
      throw new PayloadTooLargeError(
        `gzip inflation exceeded ${String(options.maxDecompressedBytes)} bytes`,
      );
    }
    throw new CorruptBodyError(`gzip inflation failed: ${failure.message}`);
  }
}

export async function decodeBody(raw: Buffer, options: DecodeOptions): Promise<DecodeResult> {
  const result: DecodeResult = { events: [], rejected: [] };

  let body: Buffer;
  try {
    body = await inflate(raw, options);
  } catch (error) {
    // A corrupt body yields no events but is NOT an exception: nothing is
    // salvageable, and the signature already proved these are the bytes Vercel
    // sent, so redelivery would reproduce it byte for byte. Report it the same
    // way an unparseable JSON array is reported — one whole-body reject — and
    // let the caller answer 200 with a rejected count. PayloadTooLargeError
    // still propagates, because that one the caller answers with 413.
    if (error instanceof CorruptBodyError) {
      result.rejected.push({ index: WHOLE_BODY_INDEX, reason: error.message, snippet: '' });
      return result;
    }
    throw error;
  }

  const text = body.toString('utf8');

  const firstNonSpace = text.search(/\S/);
  if (firstNonSpace === -1) return result;

  if (text[firstNonSpace] === '{' && !text.includes('\n')) {
    // Attempting to send a single JSON object instead of an array or NDJSON is a
    // misconfiguration. Try to parse it to confirm, then reject the whole body.
    let obj: unknown;
    try {
      obj = JSON.parse(text);
    } catch (error) {
      result.rejected.push({
        index: WHOLE_BODY_INDEX,
        reason: error instanceof Error ? error.message : 'invalid JSON',
        snippet: snippet(text),
      });
      return result;
    }
    if (typeof obj === 'object' && obj !== null && !Array.isArray(obj)) {
      result.rejected.push({
        index: WHOLE_BODY_INDEX,
        reason: 'body is a JSON object, not an array or newline-delimited entries',
        snippet: snippet(text),
      });
      return result;
    }
  }

  if (text[firstNonSpace] === '[') {
    let entries: unknown;
    try {
      entries = JSON.parse(text);
    } catch (error) {
      result.rejected.push({
        index: WHOLE_BODY_INDEX,
        reason: error instanceof Error ? error.message : 'invalid JSON array',
        snippet: snippet(text),
      });
      return result;
    }
    if (!Array.isArray(entries)) {
      result.rejected.push({ index: WHOLE_BODY_INDEX, reason: 'body is not an array', snippet: snippet(text) });
      return result;
    }
    entries.forEach((entry, index) => {
      validateEntry(entry, index, result);
    });
    return result;
  }

  const lines = text.split('\n');
  lines.forEach((line, index) => {
    if (line.trim().length === 0) return;
    let entry: unknown;
    try {
      entry = JSON.parse(line);
    } catch (error) {
      result.rejected.push({
        index,
        reason: error instanceof Error ? error.message : 'invalid JSON line',
        snippet: snippet(line),
      });
      return;
    }
    validateEntry(entry, index, result);
  });
  return result;
}
