import { useState } from 'react';
import { testSink } from '../api.ts';
import { useConfig } from '../useConfig.ts';
import { HIGH_CARDINALITY_FIELDS } from '@shared/api';
import type {
  FileSinkConfigDto,
  LokiAuthDto,
  LokiSinkConfigDto,
  RedactedConfigDto,
  SinkEntryDto,
  SinkFilterDto,
} from '@shared/api';

const MIB = 1_048_576;

type Level = 'info' | 'warning' | 'error';

function parseLevel(value: string): Level | undefined {
  switch (value) {
    case 'info':
    case 'warning':
    case 'error':
      return value;
    default:
      return undefined;
  }
}

function newFileSink(index: number): SinkEntryDto {
  return {
    name: `file-${String(index)}`,
    enabled: true,
    filter: {},
    maxSpoolBytes: 512 * MIB,
    maxBatchEvents: 1000,
    maxBatchBytes: 4 * MIB,
    config: {
      type: 'file',
      directory: '/logs',
      filePrefix: 'events',
      retentionDays: 14,
      freeSpaceFloorBytes: 256 * MIB,
    },
  };
}

function newLokiSink(index: number): SinkEntryDto {
  return {
    name: `loki-${String(index)}`,
    enabled: true,
    filter: {},
    maxSpoolBytes: 512 * MIB,
    maxBatchEvents: 1000,
    maxBatchBytes: 4 * MIB,
    config: {
      type: 'loki',
      url: 'http://loki:3100',
      auth: { kind: 'none' },
      tenantId: null,
      labels: {
        static: { job: 'vercel' },
        fromFields: ['projectName', 'environment', 'source', 'level'],
      },
      timeoutMs: 10_000,
    },
  };
}

/**
 * The `source` values Vercel actually sends (design spec section 2). Offered
 * as checkboxes rather than a text box on purpose: a mistyped source is not a
 * validation error, it is a filter that silently matches nothing, and the
 * operator would see a healthy sink receiving no events with no indication
 * why.
 */
const KNOWN_SOURCES = ['build', 'lambda', 'static', 'edge', 'external'] as const;

/**
 * Writes one of the list fields, OMITTING it when the list is empty rather
 * than storing `[]`.
 *
 * That distinction is the entire reason this helper exists. `compileFilter`
 * adds no check for an absent field, so the sink matches everything; for `[]`
 * it adds a check against an empty Set, so the sink matches NOTHING. An
 * operator who clears a box means "stop filtering on this", never "drop every
 * event" -- and `sinkFilterSchema` accepts `[]`, so nothing downstream would
 * have caught the difference.
 */
function withListField(
  filter: SinkFilterDto,
  field: 'sources' | 'environments' | 'projectIds',
  values: string[],
): SinkFilterDto {
  const next: SinkFilterDto = { ...filter };
  if (values.length === 0) {
    delete next[field];
    return next;
  }
  next[field] = values;
  return next;
}

function parseList(raw: string): string[] {
  return raw
    .split(',')
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

/**
 * Plain-language description of what a filter admits, shown live under the
 * controls. This is the real defence against the empty-list footgun: it is
 * the one place an operator can see "matches nothing" before saving, and it
 * also surfaces an empty list that arrived from a hand-edited config.json or
 * a raw PUT, which the controls above cannot produce but the schema permits.
 */
function describeFilter(filter: SinkFilterDto): string {
  const parts: string[] = [];
  if (filter.minLevel !== undefined) parts.push(`level is ${filter.minLevel} or higher`);

  const lists: [string, string[] | undefined][] = [
    ['source', filter.sources],
    ['environment', filter.environments],
    ['project', filter.projectIds],
  ];
  for (const [label, values] of lists) {
    if (values === undefined) continue;
    if (values.length === 0) {
      return `Matches nothing: the ${label} list is empty, so every event is filtered out.`;
    }
    parts.push(`${label} is one of ${values.join(', ')}`);
  }

  if (parts.length === 0) return 'Matches every event this drain receives.';
  return `Matches when ${parts.join(', and ')}.`;
}

export function Sinks(): React.JSX.Element {
  const { config, warnings, error, notice, setNotice, save } = useConfig();
  const [draft, setDraft] = useState<RedactedConfigDto | null>(null);
  const [testResult, setTestResult] = useState<string | null>(null);

  const working = draft ?? config;
  if (working === null) return <p className="muted">{error ?? 'Loading…'}</p>;

  const update = (index: number, next: SinkEntryDto): void => {
    const sinks = working.sinks.map((sink, position) => (position === index ? next : sink));
    setDraft({ ...working, sinks });
    setNotice(null);
  };

  const add = (sink: SinkEntryDto): void => {
    setDraft({ ...working, sinks: [...working.sinks, sink] });
  };

  const remove = (index: number): void => {
    const sink = working.sinks[index];
    if (sink === undefined) return;
    if (
      !window.confirm(
        `Remove sink "${sink.name}"? Its queued batches stay on disk and appear as an orphaned spool.`,
      )
    ) {
      return;
    }
    setDraft({
      ...working,
      sinks: working.sinks.filter((_unused, position) => position !== index),
    });
  };

  // `save` only resolves `true` once the write has actually landed. On a 409
  // conflict (or any other failure) it resolves `false` and leaves `draft`
  // alone, so the operator's edits stay on screen next to the error message
  // instead of vanishing the instant a conflict is reported.
  const commit = (): void => {
    void (async () => {
      const ok = await save(working);
      if (ok) setDraft(null);
    })();
  };

  const runTest = (name: string): void => {
    void (async () => {
      const result = await testSink(name);
      setTestResult(`${name}: ${result.ok ? 'OK' : 'FAILED'} — ${result.detail}`);
    })();
  };

  // Field renderers take an already-narrowed, concretely-typed `sinkConfig`
  // rather than re-reading `sink.config` inside their nested `onChange`
  // closures. A discriminant check on a property chain (`sink.config.type
  // === 'file'`) only narrows within the synchronous scope it's written in;
  // it does not survive into a closure created inside that scope, so a
  // spread of `sink.config` inside an `onChange` handler is still typed as
  // the full `SinkConfigDto` union. Passing the narrowed value in as a
  // same-scope function argument narrows it at the call site instead, which
  // does hold, and each renderer then works with one concrete shape.

  const renderFileFields = (
    sink: SinkEntryDto,
    index: number,
    sinkConfig: FileSinkConfigDto,
  ): React.JSX.Element => (
    <div className="row">
      <label>
        Directory
        <input
          value={sinkConfig.directory}
          onChange={(changed) =>
            update(index, { ...sink, config: { ...sinkConfig, directory: changed.target.value } })
          }
        />
      </label>
      <label>
        File prefix
        <input
          value={sinkConfig.filePrefix}
          onChange={(changed) =>
            update(index, { ...sink, config: { ...sinkConfig, filePrefix: changed.target.value } })
          }
        />
      </label>
      <label>
        Retention (days, 0 = forever)
        <input
          type="number"
          value={sinkConfig.retentionDays}
          onChange={(changed) =>
            update(index, {
              ...sink,
              config: { ...sinkConfig, retentionDays: Math.max(0, Number(changed.target.value)) },
            })
          }
        />
      </label>
    </div>
  );

  const renderLokiAuthFields = (
    sink: SinkEntryDto,
    index: number,
    sinkConfig: LokiSinkConfigDto,
  ): React.JSX.Element | null => {
    const auth = sinkConfig.auth;
    if (auth.kind === 'basic') {
      return (
        <div className="row">
          <label>
            Username
            <input
              value={auth.username}
              onChange={(changed) =>
                update(index, {
                  ...sink,
                  config: {
                    ...sinkConfig,
                    auth: {
                      kind: 'basic',
                      username: changed.target.value,
                      password: auth.password,
                    },
                  },
                })
              }
            />
          </label>
          <label>
            Password (leave blank to keep the stored one)
            <input
              type="password"
              placeholder="unchanged"
              onChange={(changed) =>
                update(index, {
                  ...sink,
                  config: {
                    ...sinkConfig,
                    auth: {
                      kind: 'basic',
                      username: auth.username,
                      password: changed.target.value,
                    },
                  },
                })
              }
            />
          </label>
        </div>
      );
    }
    if (auth.kind === 'bearer') {
      return (
        <div className="row">
          <label>
            Token (leave blank to keep the stored one)
            <input
              type="password"
              placeholder="unchanged"
              onChange={(changed) =>
                update(index, {
                  ...sink,
                  config: { ...sinkConfig, auth: { kind: 'bearer', token: changed.target.value } },
                })
              }
            />
          </label>
        </div>
      );
    }
    return null;
  };

  const renderLokiFields = (
    sink: SinkEntryDto,
    index: number,
    sinkConfig: LokiSinkConfigDto,
  ): React.JSX.Element => (
    <>
      <div className="row">
        <label>
          Loki URL
          <input
            value={sinkConfig.url}
            onChange={(changed) =>
              update(index, { ...sink, config: { ...sinkConfig, url: changed.target.value } })
            }
          />
        </label>
        <label>
          Tenant (X-Scope-OrgID)
          <input
            value={sinkConfig.tenantId ?? ''}
            onChange={(changed) =>
              update(index, {
                ...sink,
                config: {
                  ...sinkConfig,
                  tenantId: changed.target.value === '' ? null : changed.target.value,
                },
              })
            }
          />
        </label>
        <label>
          Auth
          <select
            value={sinkConfig.auth.kind}
            onChange={(changed) => {
              const kind = changed.target.value;
              const auth: LokiAuthDto =
                kind === 'basic'
                  ? { kind: 'basic', username: '', password: '' }
                  : kind === 'bearer'
                    ? { kind: 'bearer', token: '' }
                    : { kind: 'none' };
              update(index, { ...sink, config: { ...sinkConfig, auth } });
            }}
          >
            <option value="none">none</option>
            <option value="basic">basic</option>
            <option value="bearer">bearer</option>
          </select>
        </label>
      </div>

      {renderLokiAuthFields(sink, index, sinkConfig)}

      <label>
        Label fields (comma separated)
        <input
          value={sinkConfig.labels.fromFields.join(', ')}
          onChange={(changed) =>
            update(index, {
              ...sink,
              config: {
                ...sinkConfig,
                labels: {
                  static: sinkConfig.labels.static,
                  fromFields: changed.target.value
                    .split(',')
                    .map((field) => field.trim())
                    .filter((field) => field.length > 0),
                },
              },
            })
          }
        />
      </label>
      {sinkConfig.labels.fromFields.some((field) => HIGH_CARDINALITY_FIELDS.includes(field)) ? (
        <p className="warn">
          One or more of these fields is high-cardinality. Every distinct value creates a new Loki
          stream; prefer filtering them from the log line with <code>| json</code>.
        </p>
      ) : null}
    </>
  );

  return (
    <>
      {error !== null ? <p className="err">{error}</p> : null}
      {notice !== null ? <p className="muted">{notice}</p> : null}
      {warnings.map((warning) => (
        <p className="warn" key={warning}>
          {warning}
        </p>
      ))}
      {testResult !== null ? <p className="muted">{testResult}</p> : null}

      {working.sinks.map((sink, index) => (
        // Keyed by position, NOT by name. `name` is edited in the input
        // below, so including it here changed the key on every keystroke,
        // which made React unmount the whole card and mount a fresh one --
        // the focused input went with it, so typing a name lost focus after
        // every single character. Position is safe because nothing in this
        // card holds local state: every field is controlled from
        // `working.sinks[index]`, so React has no per-card state to
        // mis-associate when a sink is added or removed.
        <div className="card" key={index}>
          <div className="row">
            <label>
              Name
              <input
                value={sink.name}
                onChange={(changed) => update(index, { ...sink, name: changed.target.value })}
              />
            </label>
            <label>
              Enabled
              <input
                type="checkbox"
                checked={sink.enabled}
                onChange={(changed) => update(index, { ...sink, enabled: changed.target.checked })}
              />
            </label>
            <label>
              Min level
              <select
                value={sink.filter.minLevel ?? ''}
                onChange={(changed) =>
                  update(index, {
                    ...sink,
                    filter: { ...sink.filter, minLevel: parseLevel(changed.target.value) },
                  })
                }
              >
                <option value="">any</option>
                <option value="info">info</option>
                <option value="warning">warning</option>
                <option value="error">error</option>
              </select>
            </label>
            <label>
              Spool budget (MiB)
              <input
                type="number"
                value={Math.round(sink.maxSpoolBytes / MIB)}
                onChange={(changed) =>
                  update(index, {
                    ...sink,
                    maxSpoolBytes: Math.max(1, Number(changed.target.value)) * MIB,
                  })
                }
              />
            </label>
          </div>

          <fieldset className="filters">
            <legend>Filter</legend>

            {/* A nested fieldset, NOT a label. A <label> must label exactly
                one control, so wrapping five checkboxes in one made the
                browser associate it with the first of them: that checkbox's
                accessible name became "Sources build lambda static edge
                external", which a screen reader would read out verbatim.
                Caught by a Playwright selector matching two elements. */}
            <fieldset className="group">
              <legend>Sources</legend>
              <span className="checks">
                {KNOWN_SOURCES.map((source) => {
                  const selected = sink.filter.sources ?? [];
                  return (
                    <label key={source} className="check">
                      <input
                        type="checkbox"
                        checked={selected.includes(source)}
                        onChange={(changed) => {
                          const next = changed.target.checked
                            ? [...selected, source]
                            : selected.filter((item) => item !== source);
                          update(index, {
                            ...sink,
                            // Keep the declared order rather than click order,
                            // so the saved config does not churn its diff
                            // depending on which box was ticked first.
                            filter: withListField(
                              sink.filter,
                              'sources',
                              KNOWN_SOURCES.filter((item) => next.includes(item)),
                            ),
                          });
                        }}
                      />
                      {source}
                    </label>
                  );
                })}
              </span>
            </fieldset>

            <label>
              Environments
              <input
                placeholder="production, preview — blank for any"
                value={(sink.filter.environments ?? []).join(', ')}
                onChange={(changed) =>
                  update(index, {
                    ...sink,
                    filter: withListField(
                      sink.filter,
                      'environments',
                      parseList(changed.target.value),
                    ),
                  })
                }
              />
            </label>

            <label>
              Project IDs
              <input
                placeholder="blank for any"
                value={(sink.filter.projectIds ?? []).join(', ')}
                onChange={(changed) =>
                  update(index, {
                    ...sink,
                    filter: withListField(
                      sink.filter,
                      'projectIds',
                      parseList(changed.target.value),
                    ),
                  })
                }
              />
            </label>

            <p
              className={
                describeFilter(sink.filter).startsWith('Matches nothing') ? 'warn' : 'muted'
              }
            >
              {describeFilter(sink.filter)}
            </p>
          </fieldset>

          {sink.config.type === 'file'
            ? renderFileFields(sink, index, sink.config)
            : renderLokiFields(sink, index, sink.config)}

          <div className="row">
            <button type="button" onClick={() => runTest(sink.name)}>
              Send test event
            </button>
            <button type="button" onClick={() => remove(index)}>
              Remove
            </button>
          </div>
        </div>
      ))}

      <div className="row">
        <button type="button" onClick={() => add(newFileSink(working.sinks.length + 1))}>
          Add file sink
        </button>
        <button type="button" onClick={() => add(newLokiSink(working.sinks.length + 1))}>
          Add Loki sink
        </button>
        <button type="button" className="primary" disabled={draft === null} onClick={commit}>
          Save changes
        </button>
        {draft !== null ? (
          <button
            type="button"
            onClick={() => {
              setDraft(null);
            }}
          >
            Discard edits
          </button>
        ) : null}
      </div>
      <p className="muted">
        A test event is sent through the saved configuration, so save before testing. Renaming a
        sink abandons its queued batches, which then appear as an orphaned spool on the Status page.
      </p>
    </>
  );
}
