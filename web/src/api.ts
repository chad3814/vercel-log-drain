import type {
  ConfigResponse,
  CreatedDrain,
  RedactedConfigDto,
  StatusSnapshot,
  TestSinkResponse,
} from '@shared/api';

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  // Merge through `Headers` rather than an object spread: `RequestInit['headers']`
  // may be a `string[][]` at the type level, and spreading an array into an
  // object literal yields index keys ("0", "1", ...) instead of header names.
  const headers = new Headers(init?.headers);
  headers.set('content-type', 'application/json');
  const response = await fetch(path, { ...init, headers });
  if (!response.ok) {
    const body: { error?: string } = await response.json().catch(() => ({}));
    throw new ApiError(response.status, body.error ?? `request failed (${response.status})`);
  }
  // Annotated assignment, not `as T`. This file is compiled by
  // `web/tsconfig.json`, whose `lib` includes "dom", so `Response.json()`
  // resolves to `Promise<any>` here and the annotation is the sanctioned way
  // to land it in a typed binding. Note the SERVER tsconfig has no "dom" and
  // types the same call as `Promise<unknown>`, where this form would not
  // compile -- see Global Constraints.
  const body: T = await response.json();
  return body;
}

export function fetchStatus(): Promise<StatusSnapshot> {
  return request<StatusSnapshot>('/api/status');
}

export function fetchConfig(): Promise<ConfigResponse> {
  return request<ConfigResponse>('/api/admin/config');
}

export function saveConfig(config: RedactedConfigDto, etag: string): Promise<ConfigResponse> {
  return request<ConfigResponse>('/api/admin/config', {
    method: 'PUT',
    body: JSON.stringify({ config, etag }),
  });
}

export function createDrain(name: string): Promise<CreatedDrain> {
  return request<CreatedDrain>('/api/admin/drains', {
    method: 'POST',
    body: JSON.stringify({ name }),
  });
}

export function testSink(name: string): Promise<TestSinkResponse> {
  return request<TestSinkResponse>(`/api/admin/sinks/${encodeURIComponent(name)}/test`, {
    method: 'POST',
  });
}

export function discardOrphan(name: string): Promise<{ ok: boolean }> {
  return request<{ ok: boolean }>(`/api/admin/orphans/${encodeURIComponent(name)}`, {
    method: 'DELETE',
  });
}
