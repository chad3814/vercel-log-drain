import { describe, expect, it } from 'vitest';
import { PermanentDeliveryError, RetryableDeliveryError } from '../../src/sinks/types.js';

describe('delivery errors', () => {
  it('marks a retryable error distinguishably', () => {
    const error = new RetryableDeliveryError('loki unreachable');
    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(RetryableDeliveryError);
    expect(error).not.toBeInstanceOf(PermanentDeliveryError);
    expect(error.name).toBe('RetryableDeliveryError');
    expect(error.message).toBe('loki unreachable');
  });

  it('marks a permanent error distinguishably', () => {
    const error = new PermanentDeliveryError('entry too far behind');
    expect(error).toBeInstanceOf(PermanentDeliveryError);
    expect(error).not.toBeInstanceOf(RetryableDeliveryError);
    expect(error.name).toBe('PermanentDeliveryError');
  });

  it('preserves a cause for diagnostics', () => {
    const cause = new Error('ECONNREFUSED');
    const error = new RetryableDeliveryError('push failed', cause);
    expect(error.cause).toBe(cause);
  });
});
