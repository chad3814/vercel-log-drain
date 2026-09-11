import { describe, expect, it } from 'vitest';
import { Writable } from 'node:stream';
import { createLogger } from '../src/log.js';

function captureLogger(): { lines: string[]; log: ReturnType<typeof createLogger> } {
  const lines: string[] = [];
  const sink = new Writable({
    write(chunk: Buffer, _enc, callback): void {
      lines.push(chunk.toString('utf8'));
      callback();
    },
  });
  return { lines, log: createLogger('info', sink) };
}

describe('createLogger', () => {
  it('redacts a top-level secret', () => {
    const { lines, log } = captureLogger();
    log.info({ secret: 'sup3rs3cret' }, 'test');
    expect(lines.join('')).not.toContain('sup3rs3cret');
    expect(lines.join('')).toContain('[redacted]');
  });

  it('redacts drain secrets nested in an array', () => {
    const { lines, log } = captureLogger();
    log.info({ drains: [{ id: 'd1', secret: 'sup3rs3cret' }] }, 'test');
    const output = lines.join('');
    expect(output).not.toContain('sup3rs3cret');
    expect(output).toContain('[redacted]');
    expect(output).toContain('d1');
  });

  it('redacts loki auth credentials nested in sink config', () => {
    const { lines, log } = captureLogger();
    log.info(
      { sinks: [{ name: 'loki', config: { auth: { password: 'pw123', token: 'tk456' } } }] },
      'test',
    );
    const output = lines.join('');
    expect(output).not.toContain('pw123');
    expect(output).not.toContain('tk456');
  });

  it('leaves non-secret fields intact', () => {
    const { lines, log } = captureLogger();
    log.info({ sinkName: 'loki-prod', delivered: 42 }, 'test');
    const output = lines.join('');
    expect(output).toContain('loki-prod');
    expect(output).toContain('42');
  });
});
