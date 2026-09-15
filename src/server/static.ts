import { readFile, stat } from 'node:fs/promises';
import { extname, join, relative, resolve } from 'node:path';
import type { MiddlewareHandler } from 'hono';
import type { AppEnv } from './types.js';

const CONTENT_TYPES: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.woff2': 'font/woff2',
  '.map': 'application/json; charset=utf-8',
};

/**
 * Serves the built SPA out of `webRoot`, falling back to `index.html` for
 * any path that is not a real file on disk -- required for client-side
 * routing, where e.g. `/sinks` is not a file but must still load the app
 * shell. The containment check below is defense in depth for a request path
 * that reaches this handler with an unresolved `..` segment (for example
 * from a listener that does not normalize the request line the way the
 * WHATWG URL parser does); it never fires on a request that Hono's own
 * `app.request()` and `@hono/node-server` construct, both of which resolve
 * `..` before this handler ever sees the path.
 */
export function staticHandler(webRoot: string): MiddlewareHandler<AppEnv> {
  const root = resolve(webRoot);

  return async (c) => {
    const requested = decodeURIComponent(new URL(c.req.url).pathname);
    const candidate = resolve(join(root, requested === '/' ? 'index.html' : requested));

    // Containment: never serve anything outside the web root.
    const rel = relative(root, candidate);
    const contained = rel === '' || (!rel.startsWith('..') && !rel.startsWith('/'));

    const target = contained && (await isFile(candidate)) ? candidate : join(root, 'index.html');
    if (!(await isFile(target))) return c.text('not found', 404);

    const body = await readFile(target);
    const type = CONTENT_TYPES[extname(target)] ?? 'application/octet-stream';
    return c.body(body, 200, { 'content-type': type });
  };
}

async function isFile(path: string): Promise<boolean> {
  try {
    return (await stat(path)).isFile();
  } catch {
    return false;
  }
}
