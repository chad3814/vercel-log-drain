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

    const backup = JSON.parse(await readFile(join(dir, 'config.json.bak'), 'utf8'));
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
    await expect(store.save(defaultAppConfig(), null)).resolves.toBeDefined();
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
