import { copyFile, mkdir, open, readFile, rename } from 'node:fs/promises';
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
  // Error has no declared `code` property, so narrow through `unknown` rather
  // than assigning into a weak `{ code?: string }` type (TS2559) — the same
  // pattern src/vercel/decode.ts uses for zlib's error codes.
  const candidate: unknown = error;
  if (candidate && typeof candidate === 'object' && 'code' in candidate) {
    const value: unknown = (candidate as Record<string, unknown>).code;
    return value === code;
  }
  return false;
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
  private readonly tmpPath: string;

  constructor(private readonly dir: string) {
    this.path = join(dir, 'config.json');
    this.backupPath = join(dir, 'config.json.bak');
    this.tmpPath = join(dir, 'config.json.tmp');
  }

  async load(): Promise<LoadedConfig> {
    await mkdir(this.dir, { recursive: true });

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

  async save(config: AppConfig, expectedEtag: string | null): Promise<LoadedConfig> {
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
  }

  private async writeAtomic(config: AppConfig): Promise<void> {
    await mkdir(this.dir, { recursive: true });

    try {
      await copyFile(this.path, this.backupPath);
    } catch (error) {
      if (!(error instanceof Error && isErrno(error, 'ENOENT'))) throw error;
    }

    const handle = await open(this.tmpPath, 'w');
    try {
      await handle.writeFile(`${JSON.stringify(config, null, 2)}\n`, 'utf8');
      await handle.sync();
    } finally {
      await handle.close();
    }
    await rename(this.tmpPath, this.path);

    const dirHandle = await open(this.dir, 'r');
    try {
      await dirHandle.sync();
    } finally {
      await dirHandle.close();
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
