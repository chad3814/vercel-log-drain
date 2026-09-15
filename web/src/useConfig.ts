import { useCallback, useEffect, useState } from 'react';
import { fetchConfig, saveConfig } from './api.ts';
import type { RedactedConfigDto } from '@shared/api';

export type UseConfig = {
  config: RedactedConfigDto | null;
  etag: string;
  warnings: string[];
  error: string | null;
  notice: string | null;
  setNotice: (value: string | null) => void;
  reload: () => void;
  // Resolves `true` once the save has actually landed, `false` on failure
  // (including a 409 conflict). Callers that hold unsaved edits in local
  // state -- Sinks does, via its `draft` -- must await this and only
  // discard that state on `true`. Discarding it unconditionally would throw
  // away the operator's typing the moment a conflict message appears,
  // which is exactly the failure mode this hook exists to prevent.
  save: (next: RedactedConfigDto) => Promise<boolean>;
};

export function useConfig(): UseConfig {
  const [config, setConfig] = useState<RedactedConfigDto | null>(null);
  const [etag, setEtag] = useState('');
  const [warnings, setWarnings] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const reload = useCallback(() => {
    void (async () => {
      try {
        const response = await fetchConfig();
        setConfig(response.config);
        setEtag(response.etag);
        setWarnings(response.warnings);
        setError(null);
      } catch (cause) {
        setError(cause instanceof Error ? cause.message : String(cause));
      }
    })();
  }, []);

  useEffect(reload, [reload]);

  const save = useCallback(
    (next: RedactedConfigDto): Promise<boolean> => {
      return (async () => {
        try {
          const response = await saveConfig(next, etag);
          setConfig(response.config);
          setEtag(response.etag);
          setWarnings(response.warnings);
          setError(null);
          setNotice('Saved.');
          return true;
        } catch (cause) {
          setError(cause instanceof Error ? cause.message : String(cause));
          return false;
        }
      })();
    },
    [etag],
  );

  return { config, etag, warnings, error, notice, setNotice, reload, save };
}
