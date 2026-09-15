// Read from the environment rather than package.json: the Dockerfile sets
// APP_VERSION from a build argument, and package.json's relative location
// differs between `src/` (ts-node-free dev) and `dist/src/` (the built
// output), so reading it at runtime would need a resolution rule that
// changes depending on how this module is loaded.
export const VERSION = process.env['APP_VERSION'] ?? 'dev';
