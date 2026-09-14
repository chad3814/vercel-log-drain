import type { HttpBindings } from '@hono/node-server';

// `Bindings` is `Partial<HttpBindings>` precisely because the bindings are
// absent under Hono's in-process `app.request()` — only a real
// `@hono/node-server` listener populates `incoming`/`outgoing`. Code that
// reads `c.env.incoming` must treat it as possibly undefined.
export type AppEnv = {
  Bindings: Partial<HttpBindings>;
  Variables: { user: string | null };
};
