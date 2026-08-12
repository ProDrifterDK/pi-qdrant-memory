/** Type-only barrel: keeps the pure outbox module free of production egress client imports. */
export type { BoundQdrantDestination } from "../qdrant/write.js";
export type { BoundEmbeddingDestination } from "../clients/embeddings.js";
