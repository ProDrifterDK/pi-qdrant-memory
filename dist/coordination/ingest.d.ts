import { BoundIngestControlReader, ProductionCoordinationStore } from "./control.js";
import { BoundIngestTombstoneReader } from "./tombstones.js";
import { BoundQdrantDestination } from "../qdrant/write.js";
import { BoundEmbeddingDestination } from "../clients/embeddings.js";
export interface BoundIngestRuntimeInput {
    store: ProductionCoordinationStore;
    qdrant: BoundQdrantDestination;
    embedding: BoundEmbeddingDestination;
}
/**
 * ONE nominal, frozen production ingest bundle over the SAME real store.
 * Issued only by `bindIngestRuntime`, which requires the privately branded
 * ProductionCoordinationStore plus nominal Qdrant and embedding destinations
 * and verifies exact identity equality (endpoint/owner/collection and
 * coordination hash+epoch). It internally creates and owns BOTH the control
 * reader and the tombstone reader from that exact store, so there is no
 * mix-and-match endpoint/store path and no independently structural
 * qdrant/embedding/control/tombstones inputs.
 */
export declare class BoundIngestRuntime {
    #private;
    readonly store: ProductionCoordinationStore;
    readonly qdrant: BoundQdrantDestination;
    readonly embedding: BoundEmbeddingDestination;
    readonly control: BoundIngestControlReader;
    readonly tombstones: BoundIngestTombstoneReader;
    /** Public constructor is unusable without the module-private issuer symbol. */
    constructor(input: BoundIngestRuntimeInput, issuer: symbol);
    /** Exposed validating operation only; issuance stays module-private. */
    static isValid(value: unknown): value is BoundIngestRuntime;
}
/** The only issuer of a production ingest bundle. */
export declare function bindIngestRuntime(input: BoundIngestRuntimeInput): BoundIngestRuntime;
