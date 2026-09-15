import { useState } from 'react';
import { createDrain } from '../api.ts';
import { useConfig } from '../useConfig.ts';
import type { CreatedDrain } from '@shared/api';

function drainUrl(id: string): string {
  return `${window.location.origin}/api/drain/${id}`;
}

export function Drains(): React.JSX.Element {
  const { config, error, save, reload } = useConfig();
  const [name, setName] = useState('');
  const [created, setCreated] = useState<CreatedDrain | null>(null);
  const [createError, setCreateError] = useState<string | null>(null);

  const add = (): void => {
    void (async () => {
      try {
        setCreated(await createDrain(name));
        setName('');
        setCreateError(null);
        reload();
      } catch (cause) {
        setCreateError(cause instanceof Error ? cause.message : String(cause));
      }
    })();
  };

  const toggle = (id: string, enabled: boolean): void => {
    if (config === null) return;
    void save({
      ...config,
      drains: config.drains.map((drain) => (drain.id === id ? { ...drain, enabled } : drain)),
    });
  };

  const remove = (id: string): void => {
    if (config === null) return;
    if (!window.confirm('Delete this drain? Vercel will start getting 404s for it.')) return;
    void save({ ...config, drains: config.drains.filter((drain) => drain.id !== id) });
  };

  return (
    <>
      {error !== null ? <p className="err">{error}</p> : null}
      {createError !== null ? <p className="err">{createError}</p> : null}

      {created !== null ? (
        <div className="card">
          <strong>Drain created — copy the secret now.</strong>
          <p className="muted">This is the only time it will be shown.</p>
          <table>
            <tbody>
              <tr>
                <th>Endpoint URL</th>
                <td>
                  <code>{drainUrl(created.id)}</code>
                </td>
              </tr>
              <tr>
                <th>Signature secret</th>
                <td>
                  <code>{created.secret}</code>
                </td>
              </tr>
            </tbody>
          </table>
          <p className="muted">
            In Vercel, create a Drain with this endpoint and secret. Either JSON or NDJSON encoding
            works, with or without gzip.
          </p>
          <button
            type="button"
            className="primary"
            onClick={() => {
              setCreated(null);
            }}
          >
            I have saved it
          </button>
        </div>
      ) : null}

      <div className="card row">
        <label>
          New drain name
          <input
            value={name}
            onChange={(changed) => setName(changed.target.value)}
            placeholder="production"
          />
        </label>
        <button type="button" className="primary" disabled={name.trim().length === 0} onClick={add}>
          Create drain
        </button>
      </div>

      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Endpoint</th>
            <th>Secret</th>
            <th>Enabled</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {(config?.drains ?? []).map((drain) => (
            <tr key={drain.id}>
              <td>{drain.name}</td>
              <td>
                <code>{drainUrl(drain.id)}</code>
              </td>
              <td className="muted">{drain.hasSecret ? 'set (hidden)' : 'missing'}</td>
              <td>
                <input
                  type="checkbox"
                  checked={drain.enabled}
                  onChange={(changed) => toggle(drain.id, changed.target.checked)}
                />
              </td>
              <td>
                <button type="button" onClick={() => remove(drain.id)}>
                  Delete
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {config !== null && config.drains.length === 0 ? (
        <p className="muted">
          No drains yet. Create one, then paste its URL and secret into Vercel.
        </p>
      ) : null}
    </>
  );
}
