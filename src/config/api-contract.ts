import type { RedactedConfig } from './redact.js';
import type { SinkEntry } from './schema.js';
import type { RedactedConfigDto, SinkEntryDto } from '../../types/api.js';

/**
 * Compile-time guard. If the zod-inferred server types and the hand-written
 * DTOs in types/api.ts ever drift, `npm run typecheck` fails here rather than
 * the SPA silently reading a field that no longer exists.
 */
type AssertAssignable<Target, Source extends Target> = Source;

export type ConfigContract = AssertAssignable<RedactedConfigDto, RedactedConfig>;
export type SinkEntryContract = AssertAssignable<SinkEntryDto, SinkEntry>;

export const CONTRACT_OK = true;
