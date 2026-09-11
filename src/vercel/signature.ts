import { createHmac, timingSafeEqual } from 'node:crypto';

export function verifySignature(
  raw: Buffer,
  header: string | undefined,
  secret: string,
): boolean {
  if (header === undefined || header.length === 0) return false;
  const expected = createHmac('sha1', secret).update(raw).digest('hex');
  if (header.length !== expected.length) return false;
  return timingSafeEqual(Buffer.from(header, 'utf8'), Buffer.from(expected, 'utf8'));
}
