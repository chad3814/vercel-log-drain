import { expect } from '@playwright/test';
import type { APIRequestContext } from '@playwright/test';

/**
 * Empties the sink list. Every browser spec must call this before each test.
 *
 * The Playwright webServer boots once for the whole run and its config volume
 * persists across specs, so any test that saves a sink leaves it behind for
 * everything that follows. That is not a cosmetic problem: a second sink card
 * makes `getByLabel('Name')` and every source checkbox ambiguous, so
 * unrelated specs start failing on strict-mode violations whose cause is in a
 * different file. Shared here rather than copied into each spec so the two
 * cannot drift.
 */
export async function resetSinks(request: APIRequestContext): Promise<void> {
  const current: { config: Record<string, unknown>; etag: string } = await (
    await request.get('/api/admin/config')
  ).json();
  const response = await request.put('/api/admin/config', {
    data: { config: { ...current.config, sinks: [] }, etag: current.etag },
  });
  expect(response.status()).toBe(200);
}
