import { gunzip } from 'node:zlib';
import { promisify } from 'node:util';
import { logEventSchema } from './event.js';
import type { LogEvent } from './event.js';

const gunzipAsync = promisify(gunzip);

const SNIPPET_LIMIT = 200;

export type RejectedEntry = { index: number; reason: string; snippet: string };
export type DecodeResult = { events: LogEvent[]; rejected: RejectedEntry[] };
export type DecodeOptions = { gzipped: boolean; maxDecompressedBytes: number };

export class PayloadTooLargeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PayloadTooLargeError';
  }
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
    snippet: snippet(JSON.stringify(candidate) ?? String(candidate)),
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
    const message = error instanceof Error ? error.message : String(error);
    throw new PayloadTooLargeError(`gzip inflation refused: ${message}`);
  }
}

export async function decodeBody(raw: Buffer, options: DecodeOptions): Promise<DecodeResult> {
  const body = await inflate(raw, options);
  const text = body.toString('utf8');
  const result: DecodeResult = { events: [], rejected: [] };

  const firstNonSpace = text.search(/\S/);
  if (firstNonSpace === -1) return result;

  if (text[firstNonSpace] === '[') {
    let entries: unknown;
    try {
      entries = JSON.parse(text);
    } catch (error) {
      result.rejected.push({
        index: 0,
        reason: error instanceof Error ? error.message : 'invalid JSON array',
        snippet: snippet(text),
      });
      return result;
    }
    if (!Array.isArray(entries)) {
      result.rejected.push({ index: 0, reason: 'body is not an array', snippet: snippet(text) });
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
