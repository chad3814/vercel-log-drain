import { expect, test } from '@playwright/test';

/**
 * Regression test for a bug a human had to report, because nothing in this
 * repo could see it: the sink card was keyed by `${sink.name}-${index}`, and
 * `sink.name` is edited by an input inside that card. Every keystroke changed
 * the key, so React unmounted the card and mounted a fresh one, and the
 * focused input went with it.
 *
 * The assertion that matters is the one on the field's VALUE, not the one on
 * focus. Losing focus is the symptom an operator notices, but the damage is
 * that characters land in a remounted element and the name ends up truncated
 * to whatever was typed last -- so typing "local-files" into a re-created
 * input produces "l", or "s", not the name anybody intended.
 */
test('typing a sink name keeps focus and keeps every character', async ({ page }) => {
  await page.goto('/');
  await page.getByRole('button', { name: 'Sinks' }).click();
  await page.getByRole('button', { name: 'Add file sink' }).click();

  const name = page.getByLabel('Name');
  await expect(name).toBeVisible();

  // Click in, then type character by character the way a person does.
  // `fill()` would set the value in one operation and could not reproduce
  // this bug at all -- it is the per-keystroke re-render that breaks.
  // fill('') to clear, then type for real. Clearing is not what reproduces
  // the bug, so it does not need to be keystroke-level -- and `Control+a` is
  // "move to line start" on macOS rather than select-all, which silently
  // left characters behind when this test was first written.
  await name.fill('');
  await name.click();
  await page.keyboard.type('local-files', { delay: 20 });

  // Every character survived: the input was never replaced mid-typing.
  await expect(name).toHaveValue('local-files');

  // And it is still the focused element, so the operator can keep typing.
  await expect(name).toBeFocused();
});

/**
 * The same property for the second sink in the list, which is where an
 * index-based key could plausibly go wrong if the list were ever reordered
 * or an entry removed while editing.
 */
test('typing in the second sink does not disturb the first', async ({ page }) => {
  await page.goto('/');
  await page.getByRole('button', { name: 'Sinks' }).click();
  await page.getByRole('button', { name: 'Add file sink' }).click();
  await page.getByRole('button', { name: 'Add Loki sink' }).click();

  const names = page.getByLabel('Name');
  await expect(names).toHaveCount(2);

  await names.nth(0).fill('');
  await names.nth(0).click();
  await page.keyboard.type('first-sink', { delay: 10 });

  await names.nth(1).fill('');
  await names.nth(1).click();
  await page.keyboard.type('second-sink', { delay: 10 });

  await expect(names.nth(0)).toHaveValue('first-sink');
  await expect(names.nth(1)).toHaveValue('second-sink');
  await expect(names.nth(1)).toBeFocused();
});
