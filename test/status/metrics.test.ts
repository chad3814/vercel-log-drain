import { describe, expect, it } from 'vitest';
import { Metrics } from '../../src/status/metrics.js';

function event(id: string, timestamp = 1000) {
  return { id, timestamp, source: 'lambda', projectId: 'p1' };
}

describe('Metrics', () => {
  it('counts drain request outcomes separately', () => {
    const metrics = new Metrics();
    metrics.recordDrainRequest('d1', 'ok');
    metrics.recordDrainRequest('d1', 'ok');
    metrics.recordDrainRequest('d1', 'badSignature');

    const drain = metrics.snapshot().drains.find((entry) => entry.id === 'd1');
    expect(drain?.requests).toMatchObject({ ok: 2, badSignature: 1 });
  });

  it('aggregates unknown-drain requests without creating a map entry each', () => {
    // The drain id comes from the request path, so a per-id counter would let
    // anyone grow this map without bound.
    const metrics = new Metrics();
    for (let index = 0; index < 5000; index += 1) {
      metrics.recordUnknownDrainRequest();
    }

    const snapshot = metrics.snapshot();
    expect(snapshot.unknownDrainRequests).toBe(5000);
    expect(snapshot.drains).toHaveLength(0);
  });

  it('tracks events received and the latest event timestamp', () => {
    const metrics = new Metrics();
    metrics.recordEventsReceived('d1', 3, 5000);
    metrics.recordEventsReceived('d1', 2, 4000);

    const drain = metrics.snapshot().drains.find((entry) => entry.id === 'd1');
    expect(drain?.eventsReceived).toBe(5);
    // The latest timestamp must not go backwards on an out-of-order batch.
    expect(drain?.lastEventAt).toBe(5000);
  });

  it('accumulates sink counters', () => {
    const metrics = new Metrics();
    metrics.recordDelivered('loki', 10);
    metrics.recordDropped('loki', 3);
    metrics.recordDeadLettered('loki', 1);

    expect(metrics.snapshot().sinkCounters['loki']).toEqual({
      delivered: 10,
      dropped: 3,
      deadLettered: 1,
    });
  });

  it('returns a default health for an unknown sink', () => {
    const metrics = new Metrics();
    expect(metrics.getSinkHealth('never-seen').state).toBe('ok');
    expect(metrics.getSinkHealth('never-seen').consecutiveFailures).toBe(0);
  });

  it('stores and returns sink health', () => {
    const metrics = new Metrics();
    metrics.setSinkHealth('loki', {
      state: 'failed',
      consecutiveFailures: 7,
      lastError: 'boom',
      lastErrorAt: 100,
      lastSuccessAt: null,
      nextRetryAt: 200,
    });
    expect(metrics.getSinkHealth('loki').state).toBe('failed');
  });

  it('bounds the recent-events ring buffer and keeps the newest', () => {
    const metrics = new Metrics();
    for (let index = 0; index < 250; index += 1) {
      metrics.pushRecentEvents([event(`e${String(index)}`)]);
    }
    const recent = metrics.snapshot().recent.events;
    expect(recent).toHaveLength(200);
    expect(recent[recent.length - 1]?.id).toBe('e249');
  });

  it('bounds the rejects and errors ring buffers', () => {
    const metrics = new Metrics();
    for (let index = 0; index < 120; index += 1) {
      metrics.recordRejected('d1', [{ index, reason: 'bad', snippet: 'x' }]);
      metrics.recordError('loki', `failure ${String(index)}`);
    }
    expect(metrics.snapshot().recent.rejects.length).toBeLessThanOrEqual(100);
    expect(metrics.snapshot().recent.errors.length).toBeLessThanOrEqual(100);
  });

  it('forgets a removed sink', () => {
    const metrics = new Metrics();
    metrics.recordDelivered('gone', 5);
    metrics.forgetSink('gone');
    expect(metrics.snapshot().sinkCounters['gone']).toBeUndefined();
  });

  it('reports uptime as a non-negative number', () => {
    expect(new Metrics().snapshot().uptimeSec).toBeGreaterThanOrEqual(0);
  });
});
