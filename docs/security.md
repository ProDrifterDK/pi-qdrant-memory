# Security model

`pi-qdrant-memory` is a privileged extension that treats memory as optional, untrusted data. Its controls reduce risk; they do not make recalled text true, safe, complete, or instruction-free.

## Trust boundary and prompt injection

Qdrant records may contain stale claims, adversarial text, copied instructions, poisoned provenance, or ordinary mistakes. Semantic similarity ranks vector proximity; it is not evidence of truth, relevance, completeness, authorization, or current validity.

Both auto-recall and `memory_search` place represented excerpts inside a complete:

```text
<memory-context trust="untrusted">
...
</memory-context>
```

The fixed header tells the model that excerpts are background rather than instructions and that current user instructions/repository state take precedence. Delimiter-like text in memory and provenance is escaped, each excerpt/provenance value is bounded, and the complete result is character-capped. These are defense-in-depth measures: a model can still be influenced by text inside a delimiter. Operators should curate access, review suspicious results, and never use recall as an authorization or safety oracle.

## Ephemeral auto-recall

Auto-recall is added only to a copied message array returned by the host's `context` hook. The extension does not use `before_agent_start.message`, append the recalled custom message to the session manager branch, or write it into session JSONL. Existing extension-owned custom messages are removed before at most one fresh block is appended.

This non-persistence guarantee applies to the extension's automatic context block. Explicit tool calls follow the host's ordinary tool-result/session behavior. The extension does not write either form back to Qdrant.

Prime sessions with resolved `rlmDepth > 0` receive no automatic recall; their explicit tool remains available. Missing depth defaults to root only after Prime has been identified. Invalid/ambiguous host or depth disables auto-recall rather than guessing.

## Fail-open agent turns

Runtime retrieval, embedding, parsing, timeout, cancellation, formatting, cache, host, and configuration failures do not abort the agent turn. No failure text is injected into model context. The first warning per category is a fixed redacted message; repeats are suppressed for the extension session.

Fail-open means the agent continues **without memory**. It does not mean the memory service is healthy. Operators should use read-only `status --json` and service-side monitoring rather than infer health from a completed turn.

## Runtime read-only endpoint contract

The extension imports only runtime modules. Its capability-limited Qdrant client exposes:

- `GET /healthz`;
- `GET /collections/{collection}`; and
- `POST /collections/{collection}/points/search` with `with_payload: true` and `with_vector: false`.

It has no create, index, upsert, delete, generic request, scroll, or update method. Embeddings use `POST {baseUrl}/embeddings`. Every search filter is constructed internally and contains exact `host`, `status="active"`, and `secret_scan="passed"`; the project lane additionally requires the current project, and the host lane excludes it. Returned payloads are revalidated against the same positive allowlist. Model arguments can change only query and bounded result limit.

Qdrant `POST .../points/search` is read-only by contract despite using HTTP POST. Enforce the boundary again with a Qdrant credential that cannot mutate.

Administrative code lives under a separate entry point and is not imported by the extension or registered as a model tool. The human-operated CLI does contain mutation methods for `init` and approved destination upsert.

## Credential separation

Credentials are accepted only through environment variables:

- `PI_QDRANT_MEMORY_QDRANT_API_KEY`: runtime destination read-only role;
- `PI_QDRANT_MEMORY_EMBEDDING_API_KEY`: embeddings query role;
- `PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY`: destination administration/write role; and
- `PI_QDRANT_MEMORY_SOURCE_QDRANT_API_KEY`: Hermes source read-only role.

Do not reuse the admin key for runtime. Do not grant the source key write/delete permission. The administrative status path uses the destination admin and source keys because it inspects those roles; runtime health uses the runtime key. Environment values that are empty/whitespace are treated as absent.

The JSON loader recursively rejects secret-like key names, and configured URLs reject embedded usernames/passwords. Normal diagnostics/CLI projections omit endpoints, headers, response bodies, queries, memory text, vectors, absolute paths, and credentials. Nonetheless, environment variables are visible to sufficiently privileged same-user processes; use process/service isolation appropriate to the deployment.

## Endpoint exposure

Prefer loopback-bound Qdrant and embeddings services (`127.0.0.1`) on the same trusted machine. For remote services, use authenticated `https://` endpoints, certificate verification, network allowlists/firewalls, and least-privilege credentials. The configuration loader validates URL syntax and rejects userinfo but does **not** enforce loopback, TLS, private-network location, or certificate pinning; those remain operator responsibilities.

Do not put credentials in URL query strings, userinfo, shell history, config JSON, repository files, or command-line source URLs. Import `--source-url` rejects userinfo, query, fragment, raw control characters, non-HTTP(S) schemes, and recognized secret patterns.

## Eligibility allowlist

A runtime candidate is eligible only when all of these are exact:

- payload `host` equals the identified current host (`prime` or `pi`);
- payload `status` equals `active`;
- payload `secret_scan` equals `passed`;
- payload `text` is a non-empty string; and
- required provenance fields are well formed.

Missing/unknown values are ineligible. Project scoping is added internally and cannot be overridden by model input. This limits accidental cross-host/stale exposure but cannot validate the meaning of an eligible record.

## Secret scanner limitations

The Hermes importer runs a local, precision-oriented pattern scan over the complete memory text plus mapped metadata/provenance values. It recognizes selected high-confidence token prefixes, private-key headers, credential-bearing URLs, JWT-like forms, bearer values, and credential assignments. It also bounds tags, source types, labels, collection/model inputs, and source IDs.

The scanner is neither a secret detector of record nor a proof that accepted content is safe. It can miss unknown, encoded, split, encrypted, novel, short, context-dependent, or deliberately obfuscated credentials. It can also reject benign strings. `secret_scan="passed"` means only that the v1 local allowlist/scan accepted that import input at that time. It does not claim completeness, absence of secrets, or semantic safety. Use upstream secret management, source access controls, review, and independent scanners where appropriate.

## Import approval, canonical scope, and TOCTOU

Dry-run reads the source without mutation, validates collection dimension/distance/model and the fixed Hermes payload/safety contract, normalizes accepted records, and computes a SHA-256 plan ID over a canonical manifest. The manifest covers:

- transform version and target host;
- source endpoint identity, collection, dimension, and distance;
- destination collection, dimension, and distance;
- configured/declared model inputs;
- every selected source ID and full finite vector;
- every relevant source payload value; and
- every normalized destination point/payload.

Canonicalization rejects cycles, accessors, sparse/non-plain arrays, symbol/extra properties, non-plain objects, and non-finite numbers. Apply validates a 64-lowercase-hex approval, repeats the complete read/validation/normalization/hash, and compares decoded 32-byte digests with `timingSafeEqual`. A mismatch returns `source changed; run dry-run again` before any destination upsert.

The approval is not a database lock or distributed transaction. Source data can change after the repeated read, Qdrant can fail between batches, and another administrator can mutate either collection concurrently. The approved destination points correspond to the in-memory snapshot that was hashed, but v1 cannot eliminate that residual TOCTOU window or guarantee all batches commit atomically. Restrict concurrent administration, review partial-failure state, and rerun dry-run after any uncertainty.

## Source preservation and idempotence

The importer receives a source projection with only collection metadata and scroll; no source mutation method is available. It copies existing vectors and an explicit payload allowlist and drops all other source fields. Destination IDs are deterministic from target host + source collection + source point ID, so repeating an unchanged approved apply overwrites the same points rather than multiplying them. `import_run_id` records the approved plan ID.

Idempotence is not rollback. v1 provides no automated delete/reset command, and a transport failure may leave already completed batches. The dedicated destination must be inspected/reconciled by a Qdrant administrator. Package uninstall preserves both source and destination Qdrant data.

## No automatic import or mutation

Installation, extension load, session hooks, `memory_search`, and `status` do not create collections or import points. `init` is explicitly mutating: it may create the destination and five keyword indexes. `import-hermes --dry-run` writes zero points. Only `import-hermes --approve <matching-plan-id>` can upsert normalized destination points. No approved import is run automatically.

The source is never intentionally modified. A full reset, deletion, or rollback is outside this CLI and requires a separate Qdrant-administrator decision.

## Extension privilege and supply chain

Prime/Pi extensions run in the host process with that user's full effective system privileges. The extension can, in principle, access files, environment variables, network services, subprocess APIs, and host extension APIs even though this package's runtime client is intentionally narrow. Host installation is therefore a privileged code-execution decision.

Before installing:

1. review the exact `v1.0.0` source/commit and `compatibility.json`;
2. review checked-in `dist/` and verify it rebuilds without drift;
3. review dependencies/lockfile and package file allowlist;
4. use least-privilege service identities and network controls; and
5. update only after reviewing release changes.

The compatibility smokes prove loading and selected behavior against pinned hosts with local stubs; they do not prove absence of malicious code or all possible host interactions.
