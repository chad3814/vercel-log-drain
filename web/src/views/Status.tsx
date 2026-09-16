import { useEffect, useState } from 'react';
import { discardOrphan, fetchStatus } from '../api.ts';
import type { OrphanedSpool, StatusSnapshot } from '@shared/api';

const POLL_MS = 2000;

function bytes(value: number): string {
  if (value < 1024) return `${String(value)} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let scaled = value / 1024;
  let unit = 0;
  while (scaled >= 1024 && unit < units.length - 1) {
    scaled /= 1024;
    unit += 1;
  }
  return `${scaled.toFixed(1)} ${units[unit] ?? ''}`;
}

function ago(timestamp: number | null): string {
  if (timestamp === null) return 'never';
  const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
  if (seconds < 60) return `${String(seconds)}s ago`;
  if (seconds < 3600) return `${String(Math.floor(seconds / 60))}m ago`;
  return `${String(Math.floor(seconds / 3600))}h ago`;
}

export function Status(): React.JSX.Element {
  const [snapshot, setSnapshot] = useState<StatusSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    const tick = async (): Promise<void> => {
      try {
        const next = await fetchStatus();
        if (active) {
          setSnapshot(next);
          setError(null);
        }
      } catch (cause) {
        if (active) setError(cause instanceof Error ? cause.message : String(cause));
      }
    };
    void tick();
    const timer = setInterval(() => void tick(), POLL_MS);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, []);

  const handleDiscard = (orphan: OrphanedSpool): void => {
    if (
      !window.confirm(
        `Discard orphaned spool "${orphan.name}"? This permanently destroys ` +
          `${String(orphan.files)} file(s) / ${bytes(orphan.bytes)} of undelivered log batches. ` +
          'This cannot be undone.',
      )
    ) {
      return;
    }
    void (async () => {
      try {
        await discardOrphan(orphan.name);
      } catch (cause) {
        setError(cause instanceof Error ? cause.message : String(cause));
      }
    })();
  };

  if (error !== null) return <p className="err">{error}</p>;
  if (snapshot === null) return <p className="muted">Loading…</p>;

  return (
    <>
      <div className="card">
        <strong className={`state-${snapshot.service.state === 'ok' ? 'ok' : 'failed'}`}>
          {snapshot.service.state}
        </strong>{' '}
        <span className="muted">
          version {snapshot.service.version} · up {String(snapshot.service.uptimeSec)}s
        </span>
        <p className="muted">
          spool volume {bytes(snapshot.volumes.spool.freeBytes)} free of{' '}
          {bytes(snapshot.volumes.spool.totalBytes)} · config volume{' '}
          {bytes(snapshot.volumes.config.freeBytes)} free
        </p>
      </div>

      <h2>Sinks</h2>
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Health</th>
            <th>Queued</th>
            <th>Head age</th>
            <th>Delivered</th>
            <th>Dropped</th>
            <th>Dead (events / on disk)</th>
          </tr>
        </thead>
        <tbody>
          {snapshot.sinks.map((sink) => (
            <tr key={sink.name}>
              <td>{sink.name}</td>
              <td>{sink.type}</td>
              <td className={`state-${sink.health.state}`} title={sink.health.lastError ?? ''}>
                {sink.enabled ? sink.health.state : 'disabled'}
              </td>
              <td>
                {String(sink.queue.files)} files / {bytes(sink.queue.bytes)}
              </td>
              <td>
                {sink.queue.oldestAgeSec === null ? '—' : `${String(sink.queue.oldestAgeSec)}s`}
              </td>
              <td>{String(sink.counters.delivered)}</td>
              <td>{String(sink.counters.dropped)}</td>
              {/* Events dead-lettered by THIS process, plus the size of the
                  dead/ directory, which survives restarts and which nothing
                  removes automatically -- the counter alone resets to zero on
                  every restart, so it cannot tell an operator how much is
                  sitting there. The README tells them to watch this figure. */}
              <td title={`${String(sink.dead.files)} file(s) in spool/${sink.name}/dead/`}>
                {String(sink.counters.deadLettered)} / {bytes(sink.dead.bytes)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="muted">
        Counters are in-memory and reset when the container restarts. A steadily climbing head age
        means delivery is falling behind ingest.
      </p>

      <h2>Drains</h2>
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Events</th>
            <th>Last event</th>
            <th>OK</th>
            <th>Bad signature</th>
            <th>Malformed</th>
          </tr>
        </thead>
        <tbody>
          {snapshot.drains.map((drain) => (
            <tr key={drain.id}>
              <td>{drain.enabled ? drain.name : `${drain.name} (disabled)`}</td>
              <td>{String(drain.eventsReceived)}</td>
              <td>{ago(drain.lastEventAt)}</td>
              <td>{String(drain.requests.ok)}</td>
              <td>{String(drain.requests.badSignature)}</td>
              <td>{String(drain.requests.malformedBody)}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {snapshot.orphanedSpools.length > 0 ? (
        <>
          <h2>Orphaned spools</h2>
          <p className="muted">
            Queues left behind by removed or renamed sinks. Their data is still on disk.
          </p>
          <table>
            <tbody>
              {snapshot.orphanedSpools.map((orphan) => (
                <tr key={orphan.name}>
                  <td>{orphan.name}</td>
                  <td>
                    {String(orphan.files)} files / {bytes(orphan.bytes)}
                  </td>
                  <td>
                    <button type="button" onClick={() => handleDiscard(orphan)}>
                      Discard
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      ) : null}

      {snapshot.recent.errors.length > 0 ? (
        <>
          <h2>Recent errors</h2>
          <table>
            <tbody>
              {snapshot.recent.errors
                .slice(-10)
                .toReversed()
                .map((entry, index) => (
                  <tr key={`${String(entry.at)}-${String(index)}`}>
                    <td>{entry.scope}</td>
                    <td>{entry.message}</td>
                    <td className="muted">{ago(entry.at)}</td>
                  </tr>
                ))}
            </tbody>
          </table>
        </>
      ) : null}

      <h2>Recent events</h2>
      <p className="muted">
        Newest {String(Math.min(10, snapshot.recent.events.length))} of{' '}
        {String(snapshot.recent.events.length)} buffered. This is a tail to confirm arrival, not a
        log browser — query Loki for that.
      </p>
      <pre>
        {snapshot.recent.events
          .slice(-10)
          .toReversed()
          .map((event) => JSON.stringify(event))
          .join('\n')}
      </pre>
    </>
  );
}
