import { expect, test } from '@playwright/test';
import type { APIRequestContext } from '@playwright/test';
import { resetSinks } from './reset.ts';

/**
 * Regression coverage for issue #4: the orphan "Discard" button on the
 * Status page destroyed an orphaned spool -- undelivered log batches, the
 * data this whole service exists to preserve -- on a single unconfirmed
 * click. `Sinks.tsx`'s "Remove" button, by contrast, confirms before
 * removing a sink even though that action destroys nothing (the spool is
 * deliberately left on disk). This file pins the fix: Discard now asks
 * first, and the cancel path is the one that matters most.
 *
 * Playwright dismisses `window.confirm` by default, which is the cancel
 * path for free; the accept path needs an explicit `dialog.accept()`.
 */

test.beforeEach(async ({ request }) => {
  await resetSinks(request);
});

type ConfigResponse = { config: { sinks: unknown[] }; etag: string };
type StatusResponse = { orphanedSpools: { name: string; files: number; bytes: number }[] };

/**
 * Creates an orphaned spool the way the design spec says one comes to
 * exist: save a sink, then save a config that no longer has it. The
 * dispatcher `mkdir`s the sink's spool directory the moment it becomes
 * active (`SpoolQueue.open`) and the removal path never deletes it -- that
 * leftover directory IS the orphan. `applyConfig` is awaited inside the PUT
 * handler before it responds, so the directory exists by the time this
 * function returns.
 */
async function createOrphan(request: APIRequestContext, name: string): Promise<void> {
  const before: ConfigResponse = await (await request.get('/api/admin/config')).json();

  const sink = {
    name,
    enabled: true,
    filter: {},
    maxSpoolBytes: 512 * 1024 * 1024,
    maxBatchEvents: 1000,
    maxBatchBytes: 4 * 1024 * 1024,
    config: {
      type: 'file',
      directory: `.playwright-state/logs/${name}`,
      filePrefix: 'events',
      retentionDays: 14,
      freeSpaceFloorBytes: 256 * 1024 * 1024,
    },
  };

  const added = await request.put('/api/admin/config', {
    data: { config: { ...before.config, sinks: [sink] }, etag: before.etag },
  });
  expect(added.status()).toBe(200);
  const afterAdd: ConfigResponse = await added.json();

  const removed = await request.put('/api/admin/config', {
    data: { config: { ...afterAdd.config, sinks: [] }, etag: afterAdd.etag },
  });
  expect(removed.status()).toBe(200);
}

test('cancelling the confirm leaves the orphaned spool intact', async ({ page, request }) => {
  await createOrphan(request, 'cancel-orphan');

  await page.goto('/');
  const row = page.getByRole('row', { name: /cancel-orphan/ });
  await expect(row).toBeVisible();

  // No dialog handler registered: Playwright's default is to dismiss, which
  // is exactly the operator clicking "Cancel".
  await row.getByRole('button', { name: 'Discard' }).click();

  // The row survives the round trip a confirmed discard would have removed
  // it in, and the server-side state agrees: the directory was never
  // touched.
  await expect(row).toBeVisible();
  const status: StatusResponse = await (await request.get('/api/status')).json();
  expect(status.orphanedSpools.some((orphan) => orphan.name === 'cancel-orphan')).toBe(true);
});

test('accepting the confirm discards the orphaned spool', async ({ page, request }) => {
  await createOrphan(request, 'accept-orphan');

  await page.goto('/');
  const row = page.getByRole('row', { name: /accept-orphan/ });
  await expect(row).toBeVisible();

  page.once('dialog', (dialog) => {
    void dialog.accept();
  });
  await row.getByRole('button', { name: 'Discard' }).click();

  // The row disappears on the next status poll, and the directory is
  // actually gone server-side, not just missing from one snapshot.
  await expect(row).not.toBeVisible();
  const status: StatusResponse = await (await request.get('/api/status')).json();
  expect(status.orphanedSpools.some((orphan) => orphan.name === 'accept-orphan')).toBe(false);
});

test('the confirm names the orphan and states its file and byte counts', async ({
  page,
  request,
}) => {
  await createOrphan(request, 'message-orphan');

  await page.goto('/');
  const row = page.getByRole('row', { name: /message-orphan/ });
  await expect(row).toBeVisible();

  let message = '';
  page.once('dialog', (dialog) => {
    message = dialog.message();
    void dialog.dismiss();
  });
  await row.getByRole('button', { name: 'Discard' }).click();

  await expect.poll(() => message).not.toBe('');
  // An operator must be able to tell an empty directory from a real
  // backlog straight from the prompt, using the same figures the row
  // already shows -- not just a bare "are you sure?".
  expect(message).toContain('message-orphan');
  expect(message).toContain('0 file(s)');
  expect(message).toContain('0 B');
});
