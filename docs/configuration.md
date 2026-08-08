# Configuration reference

This document describes the behavior implemented by v1.0.0. Configuration comes only from the user-level JSON file and process environment. Project-local configuration is not read.

## File location

The only JSON file read is:

```text
${XDG_CONFIG_HOME:-$HOME/.config}/pi-qdrant-memory/config.json
```

More exactly, a non-empty `XDG_CONFIG_HOME` is used verbatim; an absent or empty value falls back to `$HOME/.config`. Only an `ENOENT` file error means “no file.” Invalid JSON, a non-object root, unreadable files, invalid values, and forbidden secret-like keys disable the configured runtime service. No repository file is consulted.

## Precedence

For fields that support all levels:

```text
environment override > active host section > shared section > built-in default
```

Important exceptions:

- `prime.enabled`/`pi.enabled` and `prime.autoRecall`/`pi.autoRecall` exist only in the active host section; there is no shared top-level `enabled` or `autoRecall` field.
- `PI_QDRANT_MEMORY_AUTO_RECALL` overrides the active host's `autoRecall`.
- `hardContextCharBudget` is always the literal `16000`; file values cannot change it and there is no environment override.
- `embeddings.queryPrefix` and `admin.source.schema` have file/default values but no environment override.
- At each admin level, `hermesSource` is an alias of `source`; if both supply the same field, `hermesSource` wins. The active host admin source then wins over the shared admin source.
- API keys come only from environment variables and are never accepted from JSON.

Unknown non-secret JSON keys are ignored. A known section (`qdrant`, `embeddings`, `retrieval`, `admin`, `prime`, `pi`, `source`, or `hermesSource`) must be an object when used. The loader does not silently coerce invalid booleans or out-of-range numbers.

## Complete JSON shape

```json
{
  "qdrant": {
    "url": "http://127.0.0.1:6333",
    "collection": "pi_memory"
  },
  "embeddings": {
    "baseUrl": "http://127.0.0.1:8080/v1",
    "model": "bge-m3",
    "dimension": 1024,
    "queryPrefix": "search_query: "
  },
  "retrieval": {
    "topK": 5,
    "candidatesPerLane": 20,
    "minScore": 0.35,
    "projectBoost": 0.05,
    "contextBudgetChars": 1200,
    "toolResultBudgetChars": 8000,
    "hardContextCharBudget": 16000,
    "timeoutMs": 2500
  },
  "admin": {
    "source": {
      "url": "http://127.0.0.1:6333",
      "collection": "hermes_memory",
      "schema": "hermes-qdrant-memory-v0.9-compatible"
    }
  },
  "prime": {
    "enabled": true,
    "autoRecall": true,
    "qdrant": {},
    "embeddings": {},
    "retrieval": {},
    "admin": { "hermesSource": {} }
  },
  "pi": {
    "enabled": true,
    "autoRecall": true,
    "qdrant": {},
    "embeddings": {},
    "retrieval": {},
    "admin": { "source": {} }
  }
}
```

Every field shown under shared `qdrant`, `embeddings`, `retrieval`, and `admin.source`/`admin.hermesSource` may be repeated under `prime` or `pi`. Only the detected active host section participates in the merge.

### JSON field contracts

| Field | JSON type | Default | Validation and effective behavior |
|---|---:|---:|---|
| `<host>.enabled` | boolean | `true` | Exact JSON boolean. `false` leaves `memory_search` registered but unavailable and disables lifecycle recall. |
| `<host>.autoRecall` | boolean | `true` | Exact JSON boolean. Disables lifecycle recall only; explicit tool remains available when the service is enabled. |
| `qdrant.url` | string | `http://127.0.0.1:6333` | Non-empty valid absolute URL, no embedded username/password. Trailing slashes are removed. Runtime health/metadata/search target this endpoint. |
| `qdrant.collection` | string | `pi_memory` | Non-empty string. Runtime retrieval and destination administration use it. Only `import-hermes` applies the stricter `^[A-Za-z0-9_-]{1,255}$` identifier check; `init` and `status` use the loader's non-empty-string contract. |
| `embeddings.baseUrl` | string | `http://127.0.0.1:8080/v1` | Non-empty valid absolute URL, no embedded username/password; trailing slashes removed. Client appends `/embeddings`. |
| `embeddings.model` | string | `bge-m3` | Non-empty string. Sent verbatim in embedding requests and used for import compatibility. CLI import validation additionally caps it at 256 and rejects controls/secret patterns. |
| `embeddings.dimension` | integer or numeric string | `1024` | Integer `1..65536`. Returned query vectors and collection/import dimensions must match exactly. |
| `embeddings.queryPrefix` | string | `search_query: ` | Non-empty string. Prepended verbatim to every runtime/health query. No environment override. |
| `retrieval.topK` | integer or numeric string | `5` | Integer `1..10`. Default merged result count and tool default. |
| `retrieval.candidatesPerLane` | integer or numeric string | `20` | Integer `1..100`. Exact candidate count requested independently from project and host lanes. |
| `retrieval.minScore` | finite number or numeric string | `0.35` | Inclusive `-1..1`. Applied to raw cosine scores before project boost. |
| `retrieval.projectBoost` | finite number or numeric string | `0.05` | Inclusive `0..0.25`. Added only after a project candidate passes `minScore`. |
| `retrieval.contextBudgetChars` | integer or numeric string | `1200` | Integer `1..16000`. Exact JavaScript-string-unit cap for auto-recall envelope. If the fixed envelope cannot fit, nothing is injected. |
| `retrieval.toolResultBudgetChars` | integer or numeric string | `8000` | Integer `1..16000`. Cap for explicit tool text/details; still subject to the hard ceiling. |
| `retrieval.hardContextCharBudget` | fixed literal | `16000` | Effective value is always `16000`, even if JSON supplies another value. No environment override. |
| `retrieval.timeoutMs` | integer or numeric string | `2500` | Integer `100..30000`. Each product HTTP request, including complete response-body consumption, is bounded by this timer. |
| `admin.source.url` / `admin.hermesSource.url` | string | `http://127.0.0.1:6333` | Same URL validation/trailing-slash behavior. Source collection reads use it. CLI `--source-url` wins for import and additionally requires HTTP(S), no query/fragment/controls/userinfo. |
| `admin.source.collection` / `admin.hermesSource.collection` | string | `hermes_memory` | Non-empty string. `import-hermes --source-collection` wins for import and that command alone applies the stricter `^[A-Za-z0-9_-]{1,255}$` identifier rule. `status` uses the loader contract. |
| `admin.source.schema` / `admin.hermesSource.schema` | string literal | `hermes-qdrant-memory-v0.9-compatible` | Must equal the literal exactly. No environment override. |

JSON numeric strings are passed to JavaScript `Number(raw)` after rejecting empty/whitespace-only strings. The effective syntax therefore includes finite forms accepted by `Number`, such as base-10 integers/decimals, exponent notation, surrounding whitespace around numeric content, and `0x`/`0b`/`0o` prefixes. Conversion must still be finite and satisfy the field's inclusive range/integer rule; `NaN`, infinities, fractions for integer fields, and out-of-range values are rejected. Operators should use ordinary base-10 forms for clarity. Other strings are not trimmed by the JSON loader: they must be non-empty, and URLs receive only trailing-slash removal after validation.

## Complete environment reference

### Host/config/detection inputs

| Variable/input | Allowed value | Precedence / behavior |
|---|---|---|
| `PI_QDRANT_MEMORY_HOST` | exact `prime` or `pi` | Highest host signal. An invalid value fails closed; a valid explicit value intentionally overrides marker/argv conflicts. `init` and `status` require it explicitly. |
| `XDG_CONFIG_HOME` | path string | Selects the user config root when non-empty; otherwise `$HOME/.config`. |
| `PRIME_AGENT_CODING_AGENT_DIR` | any non-whitespace string | Prime marker when there is no explicit host. |
| `PI_CODING_AGENT_DIR` | any non-whitespace string | Pi marker when there is no explicit host. |
| process argv basename | exact `prime-agent` or `pi` | Fallback host marker. Any argv element whose path basename is exact `prime-agent` signals Prime; exact `pi` signals Pi. |
| Prime session header `rlmDepth` | number | First depth source: a non-negative safe integer. `0` allows auto-recall; `>0` disables it. Invalid persisted depth fails auto-recall closed. |
| `RLM_DEPTH` | decimal integer string | Prime-only fallback when the header has no `rlmDepth`. Trimmed ASCII digits only, non-negative safe integer. Missing header and environment default to depth `0` only after Prime is identified. Invalid values disable auto-recall. Pi does not consult it. |

Without an explicit host, exactly one family of marker/argv signals must be present. Both families are a conflict; neither is unknown. Unknown, conflict, and invalid-explicit cases fail closed: no auto-recall or network retrieval is started, and the registered tool returns its fixed unavailable result. One redacted host warning is delivered on session start. Runtime never defaults an unknown host to Prime or Pi.

`import-hermes --target-host prime|pi` selects which host configuration is loaded and which host value is written. Unlike `init`/`status`, import does not use marker inference and requires the flag.

### Non-secret overrides

| Variable | Type/range | Overrides |
|---|---|---|
| `PI_QDRANT_MEMORY_QDRANT_URL` | valid non-empty URL without userinfo | `qdrant.url` |
| `PI_QDRANT_MEMORY_QDRANT_COLLECTION` | non-empty string | `qdrant.collection` |
| `PI_QDRANT_MEMORY_EMBEDDING_BASE_URL` | valid non-empty URL without userinfo | `embeddings.baseUrl` |
| `PI_QDRANT_MEMORY_EMBEDDING_MODEL` | non-empty string | `embeddings.model` |
| `PI_QDRANT_MEMORY_EMBEDDING_DIMENSION` | integer `1..65536` | `embeddings.dimension` |
| `PI_QDRANT_MEMORY_TOP_K` | integer `1..10` | `retrieval.topK` |
| `PI_QDRANT_MEMORY_CANDIDATES_PER_LANE` | integer `1..100` | `retrieval.candidatesPerLane` |
| `PI_QDRANT_MEMORY_MIN_SCORE` | finite number `-1..1` | `retrieval.minScore` |
| `PI_QDRANT_MEMORY_PROJECT_BOOST` | finite number `0..0.25` | `retrieval.projectBoost` |
| `PI_QDRANT_MEMORY_CONTEXT_BUDGET_CHARS` | integer `1..16000` | `retrieval.contextBudgetChars` |
| `PI_QDRANT_MEMORY_TOOL_RESULT_BUDGET_CHARS` | integer `1..16000` | `retrieval.toolResultBudgetChars` |
| `PI_QDRANT_MEMORY_TIMEOUT_MS` | integer `100..30000` | `retrieval.timeoutMs` |
| `PI_QDRANT_MEMORY_AUTO_RECALL` | exact `true` or `false` | active host `autoRecall` |
| `PI_QDRANT_MEMORY_SOURCE_QDRANT_URL` | valid non-empty URL without userinfo | admin source URL |
| `PI_QDRANT_MEMORY_SOURCE_QDRANT_COLLECTION` | non-empty string | admin source collection |

Environment numerics must be non-empty strings for which JavaScript `Number(raw)` produces a finite number, then satisfy the same inclusive integer/range rules. Effective accepted syntax includes ordinary base-10, exponent notation, surrounding whitespace around numeric content, and `0x`/`0b`/`0o` prefixes; use plain base-10 values operationally. Booleans are lowercase exact strings. URL trailing slashes are removed.

There is no environment variable for `enabled`, `queryPrefix`, `hardContextCharBudget`, or source `schema` in v1.

### Secret-only environment variables

| Variable | Consumer | Recommended role |
|---|---|---|
| `PI_QDRANT_MEMORY_QDRANT_API_KEY` | runtime Qdrant health/metadata/search | read-only destination credential |
| `PI_QDRANT_MEMORY_EMBEDDING_API_KEY` | embeddings endpoint | query/embedding permission only |
| `PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY` | destination `status`, `init`, and import apply | destination administrative/write credential |
| `PI_QDRANT_MEMORY_SOURCE_QDRANT_API_KEY` | Hermes source metadata/scroll | read-only source credential |

An absent, empty, or whitespace-only secret environment value is treated as not configured. JSON is scanned recursively before merging: keys equal to or containing normalized forms of `apiKey`, `authorization`, `token`, `password`, or `secret` are rejected even in unknown sections. URLs with embedded userinfo are also rejected. Secrets are not serialized into runtime config files or CLI output.

## Effective retrieval behavior

The project identity is SHA-256 of the canonical Git top-level path. Git failure falls back to canonicalized current working directory. Only its hash and basename label are eligible for output; the raw path is not recalled.

Every retrieval embeds one normalized query and performs exactly two searches:

1. project lane: exact host + `active` + `passed` + current `project_id`;
2. host lane: exact host + `active` + `passed`, excluding current `project_id`.

Each lane requests `candidatesPerLane` with payloads and no vectors. Runtime revalidates the same allowlist in every hit. Raw thresholding precedes boost; destination ID deduplication and adjusted-score/ID ordering precede the `topK`/tool limit.

Auto-recall handles only non-empty prompts not starting with `/`. A prompt with fewer than 20 non-whitespace characters uses the latest prior substantive natural-language user prompt when available. The final effective query is capped at 4,000 characters. Cache keys include stable session ID, project ID, effective query, and a non-secret configuration revision; entries live five minutes with at most 32 total entries in the extension service and are cleared on session start/shutdown/reload.

The formatter uses complete untrusted-context delimiters, preserves a complete footer, caps attacker-controlled provenance fields, and escapes delimiter-like memory text. Character counts are JavaScript string units, not guaranteed tokenizer token counts.
