import { BoundIngestControlReader, ProductionCoordinationStore, createIngestControlReader } from "./control.js";
import { BoundIngestTombstoneReader, createIngestTombstoneReader } from "./tombstones.js";
import { BoundQdrantDestination } from "../qdrant/write.js";
import { BoundEmbeddingDestination } from "../clients/embeddings.js";
/** Module-private unexported issuer: ingest runtimes are constructed only through `bindIngestRuntime`. */
const INGEST_RUNTIME_ISSUER = Symbol("pi-qdrant-memory-v2.ingest-runtime-issuer");
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
export class BoundIngestRuntime {
    #issuer;
    store;
    qdrant;
    embedding;
    control;
    tombstones;
    /** Public constructor is unusable without the module-private issuer symbol. */
    constructor(input, issuer) {
        if (issuer !== INGEST_RUNTIME_ISSUER)
            throw new TypeError("Ingest runtime requires the module issuer");
        // GLOBAL RULE: snapshot every untrusted input EXACTLY ONCE into locals; all
        // brand checks, token/scalar comparisons, assignments and reader issuance
        // use ONLY the locals. A Proxy swap genuine->fake cannot mint a runtime.
        const store = input.store;
        const qdrant = input.qdrant;
        const embedding = input.embedding;
        if (!ProductionCoordinationStore.isValid(store))
            throw new TypeError("Ingest runtime requires the branded production store");
        if (!BoundQdrantDestination.isValid(qdrant))
            throw new TypeError("Ingest runtime requires a bound Qdrant destination");
        if (!BoundEmbeddingDestination.isValid(embedding))
            throw new TypeError("Ingest runtime requires a bound embedding destination");
        // Exact identity chain: ONE exact Qdrant writer/transport (token object
        // identity ===), plus scalar endpoint/owner/collection equality, and the
        // embedding must carry the exact same coordination policy identity.
        const storeTransport = store.transport;
        const qdrantTransport = qdrant.transport;
        const storeEndpoint = store.endpoint;
        const storeOwnerHost = store.ownerHost;
        const storeCollection = store.collection;
        const qdrantEndpoint = qdrant.endpoint;
        const qdrantOwnerHost = qdrant.ownerHost;
        const qdrantCollection = qdrant.collection;
        const qdrantPolicyHash = qdrant.coordination.policyHash;
        const qdrantPolicyEpoch = qdrant.coordination.policyEpoch;
        const embeddingPolicyHash = embedding.coordination.policyHash;
        const embeddingPolicyEpoch = embedding.coordination.policyEpoch;
        if (storeTransport !== qdrantTransport)
            throw new TypeError("Ingest runtime store and Qdrant destination must share the exact writer transport");
        if (storeEndpoint !== qdrantEndpoint || storeOwnerHost !== qdrantOwnerHost || storeCollection !== qdrantCollection)
            throw new TypeError("Ingest runtime store and Qdrant destination identity mismatch");
        if (embeddingPolicyHash !== qdrantPolicyHash || embeddingPolicyEpoch !== qdrantPolicyEpoch)
            throw new TypeError("Ingest runtime coordination identity mismatch");
        this.#issuer = issuer;
        this.store = store;
        this.qdrant = qdrant;
        this.embedding = embedding;
        this.control = createIngestControlReader(store, { policyHash: qdrantPolicyHash, policyEpoch: qdrantPolicyEpoch });
        this.tombstones = createIngestTombstoneReader(store, storeOwnerHost);
        Object.freeze(this);
    }
    /** Exposed validating operation only; issuance stays module-private. */
    static isValid(value) {
        if (typeof value !== "object" || value === null || !(#issuer in value))
            return false;
        return value instanceof BoundIngestRuntime && value.#issuer === INGEST_RUNTIME_ISSUER;
    }
}
Object.freeze(BoundIngestRuntime);
Object.freeze(BoundIngestRuntime.prototype);
/** The only issuer of a production ingest bundle. */
export function bindIngestRuntime(input) {
    return new BoundIngestRuntime(input, INGEST_RUNTIME_ISSUER);
}
//# sourceMappingURL=ingest.js.map