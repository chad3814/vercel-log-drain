import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { defaultAppConfig } from '../../src/config/schema.js';
import {
  ConfigInvalidError,
  ConfigStore,
  EtagMismatchError,
  etagOf,
} from '../../src/config/store.js';

describe('ConfigStore', () => {
  let dir = '';

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'vld-config-'));
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it('writes a default config when none exists', async () => {
    const store = new ConfigStore(dir);
    const loaded = await store.load();
    expect(loaded.config).toEqual(defaultAppConfig());
    expect(await readdir(dir)).toContain('config.json');
  });

  it('round-trips a saved config', async () => {
    const store = new ConfigStore(dir);
    const initial = await store.load();
    const updated = { ...initial.config, server: { ...initial.config.server, maxBodyBytes: 4096 } };

    const saved = await store.save(updated, initial.etag);
    const reloaded = await new ConfigStore(dir).load();

    expect(reloaded.config.server.maxBodyBytes).toBe(4096);
    expect(reloaded.etag).toBe(saved.etag);
  });

  it('leaves no .tmp file behind after a save', async () => {
    const store = new ConfigStore(dir);
    const initial = await store.load();
    await store.save(initial.config, initial.etag);
    expect(await readdir(dir)).not.toContain('config.json.tmp');
  });

  it('retains the previous version as .bak', async () => {
    const store = new ConfigStore(dir);
    const first = await store.load();
    const second = await store.save(
      { ...first.config, server: { ...first.config.server, maxBodyBytes: 8192 } },
      first.etag,
    );
    await store.save(
      { ...second.config, server: { ...second.config.server, maxBodyBytes: 9999 } },
      second.etag,
    );

    const backup: { server: { maxBodyBytes: number } } = JSON.parse(
      await readFile(join(dir, 'config.json.bak'), 'utf8'),
    );
    expect(backup.server.maxBodyBytes).toBe(8192);
  });

  it('rejects a save whose etag is stale', async () => {
    const store = new ConfigStore(dir);
    const initial = await store.load();
    await store.save(
      { ...initial.config, server: { ...initial.config.server, maxBodyBytes: 4096 } },
      initial.etag,
    );

    await expect(store.save(initial.config, initial.etag)).rejects.toBeInstanceOf(
      EtagMismatchError,
    );
  });

  it('allows a save with a null etag, for boot-time writes', async () => {
    const store = new ConfigStore(dir);
    await store.load();
    const saved = await store.save(
      { ...defaultAppConfig(), server: { ...defaultAppConfig().server, maxBodyBytes: 5555 } },
      null,
    );
    expect(saved.config.server.maxBodyBytes).toBe(5555);
    expect(saved.etag).toBe(etagOf(saved.config));
    const reloaded = await new ConfigStore(dir).load();
    expect(reloaded.config.server.maxBodyBytes).toBe(5555);
  });

  it('serializes concurrent saves: the second sees a stale etag', async () => {
    const store = new ConfigStore(dir);
    const initial = await store.load();

    const results = await Promise.allSettled([
      store.save({ ...initial.config, server: { ...initial.config.server, maxBodyBytes: 1111 } }, initial.etag),
      store.save({ ...initial.config, server: { ...initial.config.server, maxBodyBytes: 2222 } }, initial.etag),
    ]);

    // Exactly one wins; the loser gets a meaningful conflict, never a raw
    // ENOENT from two saves sharing one temp path.
    expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
    const rejected = results.find((r) => r.status === 'rejected');
    expect(rejected?.status === 'rejected' && rejected.reason).toBeInstanceOf(EtagMismatchError);

    const reloaded = await new ConfigStore(dir).load();
    expect([1111, 2222]).toContain(reloaded.config.server.maxBodyBytes);
  });

  it('survives many concurrent null-etag saves with a valid file and no temp litter', async () => {
    const store = new ConfigStore(dir);
    const initial = await store.load();

    const outcomes = await Promise.all(
      Array.from({ length: 8 }, (_unused, index) =>
        store
          .save(
            { ...initial.config, server: { ...initial.config.server, maxBodyBytes: 2048 + index } },
            null,
          )
          .then(() => 'ok')
          .catch(() => 'failed'),
      ),
    );

    expect(outcomes.every((outcome) => outcome === 'ok')).toBe(true);
    const reloaded = await new ConfigStore(dir).load();
    expect(reloaded.config.server.maxBodyBytes).toBeGreaterThanOrEqual(2048);
    expect((await readdir(dir)).filter((name) => name.endsWith('.tmp'))).toEqual([]);
  });

  it('refuses to start on malformed JSON rather than resetting', async () => {
    await writeFile(join(dir, 'config.json'), '{ not json');
    await expect(new ConfigStore(dir).load()).rejects.toBeInstanceOf(ConfigInvalidError);
  });

  it('refuses to start on a schema-invalid config and names the failing path', async () => {
    await writeFile(join(dir, 'config.json'), JSON.stringify({ version: 1, drains: 'nope' }));
    const rejection = new ConfigStore(dir).load();
    await expect(rejection).rejects.toBeInstanceOf(ConfigInvalidError);
    await expect(rejection).rejects.toThrow(/drains/);
  });

  it('mentions a usable backup in the error when one parses cleanly', async () => {
    await writeFile(join(dir, 'config.json.bak'), JSON.stringify(defaultAppConfig()));
    await writeFile(join(dir, 'config.json'), '{ not json');
    await expect(new ConfigStore(dir).load()).rejects.toThrow(/config\.json\.bak/);
  });

  it('produces an etag independent of key order', () => {
    const a = defaultAppConfig();
    const reordered = {
      server: a.server,
      sinks: a.sinks,
      drains: a.drains,
      version: a.version,
    } as typeof a;
    expect(etagOf(reordered)).toBe(etagOf(a));
  });
});
