import { describe, expect, it } from 'vitest';
import { createHmac } from 'node:crypto';
import { verifySignature } from '../../src/vercel/signature.js';

const secret = 'drain-signature-secret';
const body = Buffer.from('[{"id":"1","timestamp":1,"source":"lambda","projectId":"p"}]', 'utf8');
const signature = createHmac('sha1', secret).update(body).digest('hex');

describe('verifySignature', () => {
  it('accepts a correct signature', () => {
    expect(verifySignature(body, signature, secret)).toBe(true);
  });

  it('produces a 40-character hex digest', () => {
    expect(signature).toMatch(/^[0-9a-f]{40}$/);
  });

  it('rejects a signature made with the wrong secret', () => {
    expect(verifySignature(body, signature, 'wrong-secret')).toBe(false);
  });

  it('rejects a signature over different bytes', () => {
    expect(verifySignature(Buffer.from('tampered', 'utf8'), signature, secret)).toBe(false);
  });

  it('rejects a missing header without throwing', () => {
    expect(verifySignature(body, undefined, secret)).toBe(false);
  });

  it('rejects a header of the wrong length without throwing', () => {
    expect(verifySignature(body, 'abc123', secret)).toBe(false);
  });

  it('rejects an empty header', () => {
    expect(verifySignature(body, '', secret)).toBe(false);
  });

  it('rejects an uppercase digest, since Vercel sends lowercase hex', () => {
    expect(verifySignature(body, signature.toUpperCase(), secret)).toBe(false);
  });

  it('verifies an empty body correctly', () => {
    const empty = Buffer.alloc(0);
    const emptySig = createHmac('sha1', secret).update(empty).digest('hex');
    expect(verifySignature(empty, emptySig, secret)).toBe(true);
  });
});
