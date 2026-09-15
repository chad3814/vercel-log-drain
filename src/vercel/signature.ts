import { createHmac, timingSafeEqual } from 'node:crypto';

export function verifySignature(raw: Buffer, header: string | undefined, secret: string): boolean {
  if (header === undefined || header.length === 0) return false;
  const expected = createHmac('sha1', secret).update(raw).digest('hex');
  const headerBuffer = Buffer.from(header, 'utf8');
  const expectedBuffer = Buffer.from(expected, 'utf8');
  // Byte lengths, because that is what timingSafeEqual compares.
  if (headerBuffer.length !== expectedBuffer.length) return false;
  return timingSafeEqual(headerBuffer, expectedBuffer);
}
