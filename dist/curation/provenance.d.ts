import type { CompletionProvenance } from "./llm.js";
export declare const CURATION_PROPOSAL_SCHEMA: "curation_proposal_v1";
export interface CurationProposalEnvelope {
    readonly schema: typeof CURATION_PROPOSAL_SCHEMA;
    readonly items: readonly unknown[];
    readonly provenance: CompletionProvenance;
}
/**
 * Parse the strict proposal envelope from an owned canonical snapshot.  The
 * canonical snapshot is taken before reading any field, so a backend object
 * with accessors/proxies or a mutation during validation can never be partly
 * trusted or frozen in place.
 */
export declare function parseCurationProposalEnvelope(value: unknown): CurationProposalEnvelope | null;
export declare function provenanceMatches(provenance: CompletionProvenance, expected: Partial<CompletionProvenance>): boolean;
