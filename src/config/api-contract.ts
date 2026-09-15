import type { RedactedConfig } from './redact.js';
import type { SinkEntry } from './schema.js';
import type { RedactedConfigDto, SinkEntryDto } from '../../types/api.js';

/**
 * Compile-time guard. If the zod-inferred server types and the hand-written
 * DTOs in types/api.ts ever drift, `npm run typecheck` fails here rather than
 * the SPA silently reading a field that no longer exists.
 *
 * Checked in both directions on purpose. `Assignable<Target, Source>` alone
 * (Source assignable into Target) only catches a DTO that claims something
 * false about the server shape -- a wrong field type, or a field that
 * doesn't exist on the server. A structurally wider Source is always
 * assignable to a narrower Target, so it says nothing when a DTO simply
 * drops a field the server still sends: that under-declaration compiles
 * clean with only the forward check. The reverse direction (Target
 * assignable into Source) is what catches that case, by requiring every DTO
 * field to actually exist on the server type.
 */
type Assignable<Target, Source extends Target> = Source;

export type ConfigContract = Assignable<RedactedConfigDto, RedactedConfig>;
export type ConfigContractReverse = Assignable<RedactedConfig, RedactedConfigDto>;
export type SinkEntryContract = Assignable<SinkEntryDto, SinkEntry>;
export type SinkEntryContractReverse = Assignable<SinkEntry, SinkEntryDto>;

export const CONTRACT_OK = true;
