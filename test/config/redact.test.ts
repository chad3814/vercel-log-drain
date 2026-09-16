import { describe, expect, it } from 'vitest';
import { defaultAppConfig } from '../../src/config/schema.js';
import { redactConfig, restoreSecrets, SecretRestoreError } from '../../src/config/redact.js';
import type { AppConfig } from '../../src/config/schema.js';

function configWithSecrets(): AppConfig {
  return {
    ...defaultAppConfig(),
    drains: [{ id: 'drain001', name: 'prod', secret: 'x'.repeat(32), enabled: true, createdAt: 1 }],
    sinks: [
      {
        name: 'loki',
        enabled: true,
        filter: {},
        maxSpoolBytes: 536_870_912,
        maxBatchEvents: 1000,
        maxBatchBytes: 4_194_304,
        config: {
          type: 'loki',
          url: 'http://loki:3100',
          auth: { kind: 'basic', username: 'user', password: 'pw' },
          tenantId: null,
          labels: { static: { job: 'vercel' }, fromFields: ['level'] },
          timeoutMs: 5000,
        },
      },
    ],
  };
}

describe('redactConfig', () => {
  it('nulls drain secrets and flags their presence', () => {
    const redacted = redactConfig(configWithSecrets());
    expect(redacted.drains[0]?.secret).toBeNull();
    expect(redacted.drains[0]?.hasSecret).toBe(true);
    expect(JSON.stringify(redacted)).not.toContain('x'.repeat(32));
  });

  it('nulls loki passwords and flags their presence', () => {
    const redacted = redactConfig(configWithSecrets());
    const sinkConfig = redacted.sinks[0]?.config;
    expect(JSON.stringify(sinkConfig)).not.toContain('pw');
    expect(JSON.stringify(sinkConfig)).toContain('user');
  });

  it('preserves everything non-secret', () => {
    const redacted = redactConfig(configWithSecrets());
    expect(redacted.drains[0]?.name).toBe('prod');
    expect(redacted.sinks[0]?.name).toBe('loki');
  });
});

describe('restoreSecrets', () => {
  it('keeps the existing drain secret when the incoming one is null', () => {
    const current = configWithSecrets();
    const incoming = JSON.parse(JSON.stringify(redactConfig(current)));
    const restored = restoreSecrets(incoming, current);
    expect(restored.drains[0]?.secret).toBe('x'.repeat(32));
  });

  it('keeps the existing loki password when the incoming one is null', () => {
    const current = configWithSecrets();
    const incoming = JSON.parse(JSON.stringify(redactConfig(current)));
    const restored = restoreSecrets(incoming, current);
    const auth = restored.sinks[0]?.config;
    expect(auth?.type === 'loki' && auth.auth.kind === 'basic' && auth.auth.password).toBe('pw');
  });

  it('replaces a secret when a new string is supplied', () => {
    const current = configWithSecrets();
    const incoming = JSON.parse(JSON.stringify(redactConfig(current)));
    incoming.drains[0].secret = 'y'.repeat(32);
    const restored = restoreSecrets(incoming, current);
    expect(restored.drains[0]?.secret).toBe('y'.repeat(32));
  });

  it('replaces a loki password when a new one is supplied', () => {
    const current = configWithSecrets();
    const incoming = JSON.parse(JSON.stringify(redactConfig(current)));
    incoming.sinks[0].config.auth.password = 'replacement-pw';
    const restored = restoreSecrets(incoming, current);
    const auth = restored.sinks[0]?.config;
    expect(auth?.type === 'loki' && auth.auth.kind === 'basic' && auth.auth.password).toBe(
      'replacement-pw',
    );
  });

  it('round-trips a bearer token unchanged', () => {
    const current: AppConfig = {
      ...configWithSecrets(),
      sinks: [
        {
          ...configWithSecrets().sinks[0]!,
          name: 'loki-bearer',
          config: {
            type: 'loki',
            url: 'http://loki:3100',
            auth: { kind: 'bearer', token: 'tok-abcdefghij' },
            tenantId: null,
            labels: { static: { job: 'vercel' }, fromFields: [] },
            timeoutMs: 5000,
          },
        },
      ],
    };
    const incoming = JSON.parse(JSON.stringify(redactConfig(current)));
    expect(JSON.stringify(incoming)).not.toContain('tok-abcdefghij');
    expect(restoreSecrets(incoming, current)).toEqual(current);
  });

  it('requires a fresh secret when the auth kind changes', () => {
    // basic -> bearer is a different credential entirely; there is no stored
    // token to carry forward, so the caller must supply one.
    const current = configWithSecrets();
    const incoming = JSON.parse(JSON.stringify(redactConfig(current)));
    incoming.sinks[0].config.auth = { kind: 'bearer', token: '' };
    expect(() => restoreSecrets(incoming, current)).toThrow(SecretRestoreError);
  });

  it('fails when a brand-new drain arrives with no secret', () => {
    const current = configWithSecrets();
    const incoming = JSON.parse(JSON.stringify(redactConfig(current)));
    incoming.drains.push({
      id: 'drain002',
      name: 'new',
      secret: null,
      hasSecret: false,
      enabled: true,
      createdAt: 2,
    });
    expect(() => restoreSecrets(incoming, current)).toThrow(SecretRestoreError);
  });

  it('fails when a brand-new loki sink arrives with basic auth and no password', () => {
    const current = configWithSecrets();
    const incoming = JSON.parse(JSON.stringify(redactConfig(current)));
    incoming.sinks[0].name = 'loki-two';
    expect(() => restoreSecrets(incoming, current)).toThrow(SecretRestoreError);
  });

  it('produces a config that passes the full schema', () => {
    const current = configWithSecrets();
    const incoming = JSON.parse(JSON.stringify(redactConfig(current)));
    expect(restoreSecrets(incoming, current)).toEqual(current);
  });
});
