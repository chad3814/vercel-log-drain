import { expect, test } from '@playwright/test';
import { resetSinks } from './reset.ts';

/**
 * The sink filter UI, and specifically the one mistake it must never make.
 *
 * `compileFilter` adds no check for an absent list field, so the sink matches
 * everything; for `[]` it adds a check against an empty Set, so the sink
 * matches NOTHING. `sinkFilterSchema` accepts both, so an operator who clears
 * a filter box and gets `[]` on the wire has silently muted their sink with
 * nothing anywhere reporting it. These tests assert the WIRE FORMAT, not the
 * on-screen wording: the description is a convenience, the saved JSON is what
 * the dispatcher acts on.
 */

test.beforeEach(async ({ request }) => {
  await resetSinks(request);
});

/**
 * Waits for a save to actually land.
 *
 * Do NOT wait on `getByText(/saved/i)`: the always-present help text under
 * the buttons reads "...sent through the saved configuration...", so that
 * matcher passes the instant the page renders and the wait becomes a no-op --
 * which made the config fetch below race the PUT and read the pre-save
 * state. The Save button is disabled whenever `draft === null`, and `draft`
 * is only cleared by a successful save, so this is tied to the outcome
 * rather than to wording that happens to contain the word.
 */
async function expectSaved(page: import('@playwright/test').Page): Promise<void> {
  await expect(page.getByText('Saved.', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Save changes' })).toBeDisabled();
}

async function addFileSinkNamed(page: import('@playwright/test').Page, name: string) {
  await page.goto('/');
  await page.getByRole('button', { name: 'Sinks' }).click();
  await page.getByRole('button', { name: 'Add file sink' }).click();
  const field = page.getByLabel('Name');
  await field.fill('');
  await field.click();
  await page.keyboard.type(name, { delay: 10 });
  await expect(field).toHaveValue(name);

  // A new file sink defaults to `/logs/<name>`, the production path, which
  // this test server rejects because it runs with LOGS_ROOT pointed at a
  // throwaway directory -- the containment check in resolveLogsDirectory
  // doing exactly its job. Point it inside that root so the save can
  // succeed, since what these tests are about is the filter, not the path.
  await page.getByLabel('Directory').fill(`.playwright-state/logs/${name}`);
}

test('selecting sources saves them in the declared order', async ({ page, request }) => {
  await addFileSinkNamed(page, 'sources-sink');

  // Tick out of order to prove the saved order is the declared one, so the
  // config diff does not churn based on which box was clicked first.
  await page.getByRole('checkbox', { name: 'edge', exact: true }).check();
  await page.getByRole('checkbox', { name: 'build', exact: true }).check();

  await expect(page.getByText(/Matches when source is one of/)).toBeVisible();
  await page.getByRole('button', { name: 'Save changes' }).click();
  await expectSaved(page);

  const body: { config: { sinks: { name: string; filter: { sources?: string[] } }[] } } = await (
    await request.get('/api/admin/config')
  ).json();
  const sink = body.config.sinks.find((entry) => entry.name === 'sources-sink');
  expect(sink?.filter.sources).toEqual(['build', 'edge']);
});

test('clearing every source omits the field rather than saving an empty list', async ({
  page,
  request,
}) => {
  await addFileSinkNamed(page, 'cleared-sink');

  // Select, then deselect: the state an operator reaches by changing their
  // mind. This is the case that used to be impossible to get right, because
  // `[]` and "absent" look identical in the UI and mean opposite things.
  await page.getByRole('checkbox', { name: 'lambda', exact: true }).check();
  await expect(page.getByText(/Matches when source is one of lambda/)).toBeVisible();
  await page.getByRole('checkbox', { name: 'lambda', exact: true }).uncheck();

  // The description must go back to "everything", not to "nothing".
  await expect(page.getByText('Matches every event this drain receives.')).toBeVisible();

  await page.getByRole('button', { name: 'Save changes' }).click();
  await expectSaved(page);

  const body: { config: { sinks: { name: string; filter: Record<string, unknown> }[] } } = await (
    await request.get('/api/admin/config')
  ).json();
  const sink = body.config.sinks.find((entry) => entry.name === 'cleared-sink');
  expect(sink).toBeDefined();
  // The assertion that matters: the key is ABSENT, not present-and-empty.
  expect(Object.keys(sink?.filter ?? {})).not.toContain('sources');
});

test('emptying the environments text box omits the field too', async ({ page, request }) => {
  await addFileSinkNamed(page, 'env-sink');

  const environments = page.getByLabel('Environments');
  await environments.fill('production, preview');
  await expect(page.getByText(/environment is one of production, preview/)).toBeVisible();

  await environments.fill('');
  await expect(page.getByText('Matches every event this drain receives.')).toBeVisible();

  await page.getByRole('button', { name: 'Save changes' }).click();
  await expectSaved(page);

  const body: { config: { sinks: { name: string; filter: Record<string, unknown> }[] } } = await (
    await request.get('/api/admin/config')
  ).json();
  const sink = body.config.sinks.find((entry) => entry.name === 'env-sink');
  expect(Object.keys(sink?.filter ?? {})).not.toContain('environments');
});
