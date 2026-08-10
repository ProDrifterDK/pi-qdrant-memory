# Security model

**Implementation status:** this document records the approved v2 target security contract. Task 1 removes retired executable/configuration surfaces and establishes credential/config boundaries, but does not yet implement capture, redaction, outbox delivery, Qdrant writes, curation, RAPTOR, or privacy deletion.

## Current Task 1 boundaries

The transitional loader reads only user configuration, rejects unknown/retired fields and file credentials, accepts only allowlisted environment settings, fixes embeddings at 1024 dimensions, and keeps the administrative credential outside `RuntimeConfig`. The destination-only CLI shell does not claim to initialize or inspect a live service.

## Locked target behavior for later tasks

The target pipeline treats records as untrusted data. It requires capture opt-in and explicit retention/egress disclosure, selects finalized persisted entries after activation, structurally redacts sensitive fields, runs a final scanner, and allows only a final `passed` result to become durable. Redaction must precede outbox storage, embeddings, Qdrant, and model egress.

The target uses collection-scoped runtime credentials, a separate human-only administrative credential, exact destination allowlists, residency/data-use labels, policy epochs, expiry deadlines, immutable records, tombstones, and at-least-once outbox delivery. Model tools cannot choose endpoints, collections, credentials, or infrastructure. These controls are not available from the Task 1 shell yet.

The target retrieval envelope marks memory as untrusted and ephemeral. Child sessions may emit tagged leaves but cannot perform root work, automatic recall, curation, or RAPTOR. Curation and RAPTOR summaries must preserve provenance and evidence descent. Human privacy and forget commands provide the later logical deletion barriers.

## Operator responsibility

Use loopback services where practical, TLS and least-privilege collection-scoped credentials for remote services, and separate service identities when stronger process isolation is required. Redaction is not anonymization and does not prove proprietary or personal data is absent. Review the approved target design before enabling later tasks in a production deployment.
