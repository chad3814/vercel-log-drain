import { copyFile, mkdir, open, readdir, readFile, rename, rm } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { join } from 'node:path';
import { z } from 'zod';
import { appConfigSchema, defaultAppConfig } from './schema.js';
import type { AppConfig } from './schema.js';
import type { JsonValue } from '../../types/json.js';

export type LoadedConfig = { config: AppConfig; etag: string };

export class ConfigInvalidError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ConfigInvalidError';
  }
}

export class EtagMismatchError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EtagMismatchError';
  }
}

function isErrno(error: Error, code: string): boolean {
  // `in` narrowing, deliberately — see the note in src/vercel/decode.ts.
  // `const candidate: { code?: string } = error;` fails TS2559 (weak-type
  // check: Error has no properties in common), and an `as` assertion trips
  // oxlint's no-unsafe-type-assertion.
  return 'code' in error && error.code === code;
}

function canonicalize(value: JsonValue): JsonValue {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value !== null && typeof value === 'object') {
    const sorted: { [key: string]: JsonValue } = {};
    for (const key of Object.keys(value).toSorted()) {
      const entry = value[key];
      if (entry !== undefined) sorted[key] = canonicalize(entry);
    }
    return sorted;
  }
  return value;
}

export function etagOf(config: AppConfig): string {
  // Annotated assignment rather than `as JsonValue`: oxlint's
  // no-unsafe-type-assertion rejects asserting away JSON.parse's `any`.
  const cloned: JsonValue = JSON.parse(JSON.stringify(config));
  const canonical = canonicalize(cloned);
  return createHash('sha256').update(JSON.stringify(canonical)).digest('hex').slice(0, 32);
}

export class ConfigStore {
  private readonly path: string;
  private readonly backupPath: string;
  private tmpCounter = 0;
  /**
   * Saves are serialized through this chain. `save()` reads the current etag
   * and only then writes, with several `await` points in between — without
   * serialization two concurrent callers (a double-submitted form, two open
   * admin tabs, a retried request racing the original) both pass the etag
   * check and both proceed to write. Measured before this was added: of eight
   * concurrent saves, one succeeded and seven failed with a bare
   * `ENOENT ... rename`, because they all shared one temp path and the first
   * rename moved it out from under the rest. An operator would see "no such
   * file or directory" for what is really a write conflict, and two handles
   * opened `'w'` on the same path can in principle interleave into a corrupt
   * file that then gets renamed over the live config.
   */
  private writeChain: Promise<void> = Promise.resolve();

  constructor(private readonly dir: string) {
    this.path = join(dir, 'config.json');
    this.backupPath = join(dir, 'config.json.bak');
  }

  /** Per-save temp path, so concurrent or crashed writes cannot collide. */
  private nextTmpPath(): string {
    this.tmpCounter += 1;
    return join(this.dir, `config.json.${String(process.pid)}.${String(this.tmpCounter)}.tmp`);
  }

  private serialize<T>(work: () => Promise<T>): Promise<T> {
    const run = this.writeChain.then(work, work);
    // Keep the chain alive whatever happens, so one failed save does not
    // wedge every later one.
    this.writeChain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  async load(): Promise<LoadedConfig> {
    await mkdir(this.dir, { recursive: true });
    await this.sweepStaleTemps();

    let text: string;
    try {
      text = await readFile(this.path, 'utf8');
    } catch (error) {
      if (error instanceof Error && isErrno(error, 'ENOENT')) {
        const config = defaultAppConfig();
        await this.writeAtomic(config);
        return { config, etag: etagOf(config) };
      }
      throw error;
    }

    let raw: JsonValue;
    try {
      // `raw` is already annotated, so no assertion is needed or permitted.
      raw = JSON.parse(text);
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new ConfigInvalidError(
        `${this.path} is not valid JSON: ${detail}${await this.backupHint()}`,
      );
    }

    const result = appConfigSchema.safeParse(raw);
    if (!result.success) {
      throw new ConfigInvalidError(
        `${this.path} does not match the config schema:\n${z.prettifyError(result.error)}${await this.backupHint()}`,
      );
    }
    return { config: result.data, etag: etagOf(result.data) };
  }

  save(config: AppConfig, expectedEtag: string | null): Promise<LoadedConfig> {
    // Serialized: the read-check-write sequence below must not interleave with
    // another save, or the etag check it performs is meaningless.
    return this.serialize(async () => {
      const validated = appConfigSchema.parse(config);
      if (expectedEtag !== null) {
        const current = await this.load();
        if (current.etag !== expectedEtag) {
          throw new EtagMismatchError(
            'the configuration changed since it was read; reload and reapply your edit',
          );
        }
      }
      await this.writeAtomic(validated);
      return { config: validated, etag: etagOf(validated) };
    });
  }

  private async writeAtomic(config: AppConfig): Promise<void> {
    await mkdir(this.dir, { recursive: true });

    try {
      await copyFile(this.path, this.backupPath);
      // Make the backup durable too. The design calls `.bak` the operator's
      // recovery path, and without this its bytes can sit in the page cache
      // indefinitely — a later unrelated crash could lose the one copy someone
      // is told to restore from.
      const backupHandle = await open(this.backupPath, 'r+');
      try {
        await backupHandle.sync();
      } finally {
        await backupHandle.close();
      }
    } catch (error) {
      if (!(error instanceof Error && isErrno(error, 'ENOENT'))) throw error;
    }

    const tmpPath = this.nextTmpPath();
    try {
      const handle = await open(tmpPath, 'w');
      try {
        await handle.writeFile(`${JSON.stringify(config, null, 2)}\n`, 'utf8');
        await handle.sync();
      } finally {
        await handle.close();
      }
      await rename(tmpPath, this.path);
    } catch (error) {
      // A failed save must not leave its temp file behind.
      await rm(tmpPath, { force: true });
      throw error;
    }

    const dirHandle = await open(this.dir, 'r');
    try {
      await dirHandle.sync();
    } finally {
      await dirHandle.close();
    }
  }

  /**
   * Temp files are per-save and unique, so a crash mid-write leaves one behind
   * forever. `load()` runs at boot, which is the natural place to clear them.
   * A stray temp is harmless to correctness — `load()` only ever reads
   * `config.json` — but they would accumulate on the config volume.
   */
  private async sweepStaleTemps(): Promise<void> {
    let entries: string[];
    try {
      entries = await readdir(this.dir);
    } catch {
      return;
    }
    for (const entry of entries) {
      if (/^config\.json\.\d+\.\d+\.tmp$/.test(entry)) {
        await rm(join(this.dir, entry), { force: true });
      }
    }
  }

  private async backupHint(): Promise<string> {
    try {
      const text = await readFile(this.backupPath, 'utf8');
      const backup: JsonValue = JSON.parse(text);
      const parsed = appConfigSchema.safeParse(backup);
      if (parsed.success) {
        return `\n\nThe backup at ${this.backupPath} parses cleanly. To recover, copy it over ${this.path} and restart.`;
      }
    } catch {
      // No usable backup; the primary error stands on its own.
    }
    return '';
  }
}
