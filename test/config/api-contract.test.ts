import { describe, expect, it } from 'vitest';
import { redactConfig } from '../../src/config/redact.js';
import { defaultAppConfig } from '../../src/config/schema.js';
import { CONTRACT_OK } from '../../src/config/api-contract.js';
import type { RedactedConfigDto } from '../../types/api.js';

describe('api contract', () => {
  it('keeps the hand-written DTOs assignable from the server types', () => {
    // The real assertion is at typecheck time in api-contract.ts; this test
    // exists so the file is exercised and cannot be deleted unnoticed.
    expect(CONTRACT_OK).toBe(true);
  });

  it('produces a redacted config that satisfies the DTO shape at runtime', () => {
    const redacted: RedactedConfigDto = redactConfig(defaultAppConfig());
    expect(redacted.version).toBe(1);
    expect(redacted.drains).toEqual([]);
  });
});
