# Pi Qdrant Memory v2: memoria autónoma híbrida y RAPTOR

**Fecha:** 2026-08-09

**Estado:** diseño aprobado para planificación; implementación no iniciada

**Revisión:** auto-revisión de Sol y reviewer independiente completadas sin hallazgos Critical/Important abiertos

**Repositorio:** `ProDrifterDK/pi-qdrant-memory`

**Release objetivo:** `v2.0.0`

**Hosts soportados:** Pi y Prime Agent

## 1. Resumen

La v2 convierte el runtime read-only de `pi-qdrant-memory` v1 en un motor de memoria autónoma integrado en los lifecycle hooks de Pi y Prime Agent. Cada host captura sus conversaciones futuras, guarda episodios redacted, extrae memoria duradera y construye un índice RAPTOR generacional. El mismo paquete TypeScript soporta múltiples procesos y máquinas concurrentes sin daemon, Python ni una base coordinadora adicional.

Pi y Prime no comparten datos:

- Pi usa `pi_memory` por defecto.
- Prime Agent usa `prime_memory` por defecto.
- Cada colección declara en metadata un único `owner_host`.
- El runtime rechaza una colección cuyo propietario no coincide con el host detectado.
- El modelo nunca puede elegir host, colección, endpoint ni credenciales.

La v2 no importa ni consulta recuerdos de Hermes. Se eliminan de la distribución activa el comando `import-hermes`, la configuración `admin.source`, los clientes de origen y las credenciales relacionadas. Ambas colecciones nuevas comienzan vacías y solo contienen recuerdos generados por su propio host después de activar v2.

## 2. Decisiones aprobadas

1. Memoria híbrida: episodios relativamente fieles más memoria curada.
2. Captura episódica de mensajes de usuario/asistente y extractos relevantes de herramientas; no un volcado completo de outputs.
3. RAPTOR híbrido: hojas disponibles inmediatamente y generaciones jerárquicas reconstruidas en background, publicadas atómicamente.
4. El LLM de memoria se resuelve primero como modelo dedicado y después como modelo activo del host si el fallback está permitido. BGE-M3 se usa únicamente para embeddings.
5. Los children/subagentes escriben episodios etiquetados; solo una instancia root con un claim válido ejecuta curación o RAPTOR.
6. Un único plugin TypeScript integrado; sin sidecar, daemon o runtime Python.
7. Los recuerdos obsoletos se conservan con temporalidad y relaciones de reemplazo. No se eliminan automáticamente.
8. Pi y Prime usan colecciones físicas distintas y privadas.
9. Se soportan simultáneamente múltiples procesos en una máquina y múltiples máquinas contra la colección privada del mismo host.
10. La corrección distribuida se basa en IDs idempotentes, datos inmutables, optimistic concurrency control, fencing tokens y publicación compare-and-swap. Los locks locales solo optimizan coste.

## 3. Objetivos

- Capturar automáticamente conocimiento futuro sin una herramienta model-callable de escritura.
- Recuperar detalles exactos de episodios, incluidos fallos y fragmentos relevantes de herramientas.
- Extraer preferencias, correcciones, convenciones, hechos, fallos y aprendizajes duraderos.
- Recuperar contexto en varios niveles mediante episodios, memoria curada y resúmenes RAPTOR.
- Conservar la historia aunque un recuerdo deje de ser vigente.
- Mantener las conversaciones fail-open cuando memoria, embeddings o LLM fallen.
- Evitar lecturas cruzadas entre Pi y Prime por contrato, metadata, credenciales y filtros defensivos.
- Soportar reintentos, crashes, particiones y workers concurrentes sin corrupción ni publicación parcial.
- Mantener trazabilidad desde cada memoria o resumen hasta episodios concretos.

## 4. No objetivos

- Importar, migrar, consultar o re-embebir `hermes_memory`.
- Indexar retrospectivamente sesiones anteriores a v2 en el primer release.
- Compartir recuerdos entre Pi y Prime.
- Convertir memoria recuperada en instrucciones de autoridad.
- Dar al modelo acceso directo a create/update/delete de Qdrant.
- Garantizar exactamente una llamada LLM bajo una partición arbitraria. El protocolo garantiza efectos idempotentes y una única publicación válida; una llamada perdida puede repetirse.
- Ofrecer alta disponibilidad de Qdrant por sí mismo. Replicación, snapshots y operación del cluster siguen siendo responsabilidad del operador.
- Cifrar el payload dentro de Qdrant. TLS, RBAC y cifrado de discos pertenecen al despliegue.

## 5. Arquitectura

```text
Pi / Prime lifecycle events
        |
        v
Episode capture -> redaction/secret scan -> per-instance durable outbox
        |                                      |
        +---------------- retry ---------------+
        |
        v
host-private Qdrant collection
  - episode leaves
  - curated memories
  - RAPTOR summaries
  - control/job/coverage/tombstone points
        ^                 ^
        |                 |
curation worker      RAPTOR builder
(dedicated/active LLM, distributed claims and fencing)
        |
        v
hybrid retriever -> untrusted ephemeral memory context / memory_search
```

Módulos previstos:

- `capture`: normalización de lifecycle events y selección de tool excerpts.
- `redaction`: límites, scanner de secretos y canonicalización segura.
- `outbox`: entrega durable local, reintentos y adopción de colas abandonadas.
- `qdrant-write`: insert-only, conditional update, read-back verification y strong ordering para control.
- `coordination`: jobs, leases, fencing y publicación CAS.
- `curation`: prompts estructurados, validación y materialización de memoria duradera.
- `raptor`: clustering, resumen recursivo, manifest y publicación de generaciones.
- `retrieval`: carriles semántico, exacto, curado y jerárquico.
- `operations`: init, status, inspect, curate, rebuild y forget.

## 6. Configuración e identificación

La única configuración de archivo continúa en:

```text
${XDG_CONFIG_HOME:-$HOME/.config}/pi-qdrant-memory/config.json
```

### 6.1 Precedencia

El host se resuelve fail-closed como en v1. La precedencia es environment allowlisted > sección del host > sección compartida > default compilado. Un repositorio o su contenido no puede aportar configuración. Keys desconocidas, valores fuera de rango y overrides de secreto en archivo son errores. Los defaults de colección dejan de ser compartidos:

```json
{
  "pi": {
    "qdrant": { "url": "http://127.0.0.1:6333", "collection": "pi_memory" }
  },
  "prime": {
    "qdrant": { "url": "http://127.0.0.1:6333", "collection": "prime_memory" }
  }
}
```

El loader rechaza:

- una sección activa sin colección explícita cuando se deshabilitan defaults;
- una colección cuya metadata tiene otro `owner_host`;
- dos secciones que resuelven al mismo endpoint canónico y colección;
- metadata, dimensión, distancia, esquema o modelo incompatibles;
- campos v1 retirados como `admin.source` o variables `SOURCE_QDRANT_*`;
- host ambiguo o contradictorio.

### 6.2 Contrato v2

Los nombres y defaults nuevos quedan fijados; el plan de implementación puede estrechar rangos, pero no ampliarlos sin revisar esta spec:

| Campo XDG, compartido u host-specific | Default | Rango/política |
| --- | --- | --- |
| `enabled` | `true` para lectura | boolean |
| `capture.enabled` | `false` | opt-in explícito por host después de mostrar política de egress/retención |
| `projects.registrations` | `{}` | bindings XDG de path+fingerprint a alias estable; creados por comando humano |
| `capture.projectAllowlist` / `projectDenylist` | `[]` | aliases/IDs no secretos; deny gana; no se leen archivos del repo |
| `capture.episodeRetentionDays` | requerido al activar | entero 1..3650 o `"indefinite"`; `status` muestra la elección |
| `capture.toolArgsChars` / `toolResultChars` | 2000 / 4000 | enteros 0..16000 antes del hard budget |
| `privacy.egressMode` | `"local_only"` | `local_only|allowlist`; remote requiere destino canónico explícito |
| `privacy.allowedQdrantDestinations` / `allowedEmbeddingDestinations` / `allowedLlmDestinations` | `[]` | entradas exactas con destination ID, residency/data-use declarados por operador; sin wildcards |
| `privacy.allowActiveModelFallback` | `false` | boolean y solo sesiones del proveedor activo salvo permiso siguiente |
| `privacy.allowCrossProviderReplay` | `false` | permite que contenido de otra sesión/proveedor salga al destino allowlisted |
| `coordination.maxClockSkewMs` | 300000 | 0..3600000; expiración se aplica conservadoramente con este margen |
| `retrieval.rootScope` | `"project"` | `project|project_and_global` |
| `retrieval.childSearch` | `true` | child siempre project-only, aunque root permita global |
| `outbox.maxJobs` / `maxBytes` | 10000 / 268435456 | 1..100000 / 1 MiB..1 GiB |
| `outbox.retryBaseMs` / `retryMaxMs` | 500 / 30000 | 100..10000 / 1000..300000; base <= max |
| `outbox.nodeId` / `sharedFilesystem` | derivado / `false` | node ID explícito obligatorio para home compartido; no contiene hostname raw |
| `qdrant.replicationFactor` / `writeConsistencyFactor` | 1 / 1 | enteros 1..7; en cluster write >= ceil((replication+1)/2) |
| `coordination.readConsistency` | `1` single / `"majority"` cluster | `majority|quorum|all` o entero; control nunca menor que majority |
| `coordination.leaseMs` / `reconcileIntervalMs` | 30000 / 900000 | 5000..300000 / 60000..86400000 |
| `curation.turnTrigger` / `toolTrigger` | 10 / 15 | enteros 1..1000 |
| `curation.maxInputTokens` | 12000 | 512..65536 y <= context window del modelo |
| `memoryModel.modelId` | ausente | ID exacto del model registry del host; sin ID no hay LLM dedicado |
| `memoryModel.timeoutMs` / `maxOutputTokens` | 30000 / 2048 | 1000..120000 / 128..8192 |
| `raptor.rebuildEpisodeDelta` / `maxLevels` | 64 / 5 | 2..10000 / 1..10 |
| `raptor.summaryInputTokens` | 12000 | 512..65536 y <= context window del modelo |
| `raptor.umapDimensions` / `localNeighbors` | 10 / 10 | 1..64 / 2..200, clamp efectivo por N |
| `raptor.gmmMaxClusters` / `membershipThreshold` | 50 / 0.10 | 1..200 / 0.01..1 |
| `raptor.seed` | derivada | uint32 explícito o hash de colección+policy revision |

Se conservan los límites v1 de retrieval/context/timeout salvo cambios enumerados. Solo se permiten los environment overrides operacionales cuyos sufijos son `QDRANT_URL`, `QDRANT_COLLECTION`, `EMBEDDING_BASE_URL`, `EMBEDDING_MODEL`, `EMBEDDING_DIMENSION`, `AUTO_RECALL`, `TOP_K`, `CANDIDATES_PER_LANE`, `MIN_SCORE`, `PROJECT_BOOST`, `CONTEXT_BUDGET_CHARS`, `TOOL_RESULT_BUDGET_CHARS` y `TIMEOUT_MS`, todos bajo el prefijo `PI_QDRANT_MEMORY_`. Los secretos se llaman `PI_QDRANT_MEMORY_QDRANT_API_KEY`, `PI_QDRANT_MEMORY_ADMIN_QDRANT_API_KEY` y `PI_QDRANT_MEMORY_EMBEDDING_API_KEY`; credenciales LLM siguen en el model registry del host. Las variables `SOURCE_QDRANT_*` son errores retirados, no aliases.

### 6.3 Credenciales y amenaza

Las credenciales solo se aceptan por environment. El runtime nunca recibe el admin key de Qdrant. `init` usa una variable administrativa separada proporcionada solo al proceso CLI; las sesiones Pi/Prime reciben exclusivamente su JWT collection-scoped. En Qdrant remoto o compartido se requieren TLS y un JWT RBAC de lectura/escritura limitado a la colección del host. En self-hosted, granular access keys están disponibles desde Qdrant 1.9; requieren `api_key` y `jwt_rbac=true`. Un endpoint loopback sin autenticación puede usarse para desarrollo local, pero `status` lo describe como aislamiento funcional, no privacidad criptográfica.

El threat model evita accesos accidentales o no autorizados a través del plugin y, con RBAC, a través de la API Qdrant. No pretende aislar procesos hostiles que comparten el mismo usuario Unix, pueden leer sus environments o controlan el servidor Qdrant. El despliegue debe separar cuentas/secret delivery si necesita defenderse de ese adversario.

## 7. Contrato de colección

### 7.1 Versión mínima

La v2 requiere **Qdrant >=1.17.0** y se valida contra 1.17.1.

La razón no es solo RAPTOR. Conditional updates existen desde 1.16, pero `update_mode=insert_only|update_only`, necesario para creación idempotente y CAS seguro sobre puntos ausentes/existentes, está disponible desde 1.17.

### 7.2 Metadata

Cada colección contiene metadata verificada en startup:

```json
{
  "schema": "pi-qdrant-memory-v2",
  "schema_revision": 1,
  "owner_host": "pi",
  "dense_vector": "semantic",
  "embedding_model": "bge-m3",
  "embedding_dimension": 1024,
  "distance": "Dot"
}
```

`owner_host` es `pi` o `prime` y es inmutable para el plugin. Un init concurrente del mismo nombre por hosts distintos deja ganar a un creador; el otro reread detecta el propietario incompatible y aborta sin escribir puntos.

### 7.3 Vectores e índices

La colección usa un vector dense nombrado `semantic`, 1024/Dot. Cada embedding se valida en la frontera, se normaliza por norma L2 y se canoniza a componentes float32; Dot sobre vectores normalizados conserva la semántica de Cosine y permite hashes/readback estables frente a Qdrant 1.17. Los puntos payload-only llevan explícitamente `vector: {}` porque Qdrant 1.17 requiere el campo de named vectors aunque no haya vector semántico.

Payload indexes mínimos:

- keyword: `record_type`, `owner_host`, `project_id`, `project_identity_kind`, `scope`, `status`, `resolution`, `state_key`, `content_id`, `observation_id`, `secret_scan`, `session_id`, `turn_id`, `agent_role`, `generation_id`, `job_id`, `category`, `tool_name`, `error_fingerprint`;
- integer/datetime: `event_at`, `effective_at`, `created_at`, `lease_expires_at`, `expires_at`, `privacy_epoch`, `coordination_policy_epoch`, `version`, `level`;
- full text: `text` para candidatos de coincidencia precisa.

Toda consulta incluye `owner_host`, aunque la colección ya sea privada, además de `status` y `secret_scan` cuando corresponda.

## 8. Tipos de punto

Todo punto v2 incluye `record_type`, `owner_host`, `schema_revision`, `created_at`, `privacy_epoch` observado, `processing_policy_id`, `expires_at|null` y un content hash canónico. Todo derivado incluye además `coordination_policy_hash` y su epoch monotónico, más source/provenance IDs acotados o un manifest content-addressed; ausencia o truncación no verificable lo vuelve no recuperable. Un derivado hereda el deadline más temprano y la intersección de permisos de sus fuentes.

### 8.1 `episode`

Registro inmutable de un evento finalizado:

- `owner_host`, `project_id`, `session_id`, `turn_id`;
- `agent_role=root|child`, profundidad cuando exista y producer/node IDs redacted;
- `event_kind=user|assistant|tool_call|tool_result|tool_error`;
- texto redacted y acotado;
- `tool_name`, argumentos redacted y `error_fingerprint` cuando apliquen;
- `event_at`, `created_at`, `content_hash`;
- modelo/dimensión de embeddings, origin provider/destination IDs redacted y `schema_revision`;
- `status=active`, `secret_scan=passed`.

El ID se deriva de namespace de esquema + host + identidad estable de sesión/mensaje/parte. Se codifica como UUID determinista. `content_hash` cubre identidad, texto redacted, provenance y schema, pero excluye timestamps de entrega, producer IDs y floats del vector. `insert_only` impide reemplazar una hoja existente. Tras un insert o un ignore, el writer reread el punto: mismo hash significa idempotencia; hash distinto es una colisión crítica y falla cerrado.

### 8.2 `curated_memory`

La memoria curada separa contenido, observación temporal y vista current:

- `state_key=H(schema,host,scope,project,category,subject,predicate)` identifica la variable lógica;
- `content_id=H(coordination_policy_hash,state_key,canonical_value)` identifica contenido reutilizable dentro de una política, no una vigencia;
- `observation_id=H(policy_epoch,content_id,primary_evidence_episode_id,effective_order)` identifica una ocurrencia inmutable;
- `effective_order` usa secuencia causal de sesión cuando existe y, entre sesiones, `(evidence_event_at, primary_episode_id, content_id)`, nunca `created_at` del worker;
- `curated_memory` guarda category/scope, sujeto/predicado/valor, texto redacted, observation/content/state IDs, confianza, productor y provenance;
- `curated_current` tiene ID determinista por policy epoch+`state_key` y es una vista mutable mediante OCC con versión, `resolution=resolved|conflict`, current observation/content o manifest de conflicto, texto/vector y effective order.

Observations se crean con `insert_only`; `content_id` es su identidad lógica reutilizable y `evidence_link` es determinista por observation+episode+extractor revision. Un materializer usa orden causal dentro de la misma sesión. Entre sesiones, solo declara posterior un `event_at` separado por más de `coordination.maxClockSkewMs`; contenido distinto dentro de la ventana crea por CAS un conflict manifest content-addressed en vez de elegir por reloj. Una vista conflict se excluye de auto-recall/current como verdad y solo se muestra etiquetada en búsqueda histórica; una evidencia causal o claramente posterior la resuelve. Evidencia tardía más antigua entra en historia sin rebobinar current. Historia se deriva ordenando observations y colapsando contenidos consecutivos iguales: A→B→A conserva dos intervalos A distintos sin reutilizar un estado superseded ni crear ciclos. `valid_from` es el inicio de cada tramo y `valid_to` el siguiente cambio derivado; no se mutan observations históricas.

Proposals concurrentes con igual content/primary evidence convergen. Memberships solapados pueden crear observations adicionales del mismo contenido; el fold temporal evita un efecto lógico duplicado. Duplicados semánticos que no canonicalizan igual se consolidan best-effort; no se promete deduplicación semántica perfecta.

### 8.3 `raptor_summary`

Nodo inmutable de una generación:

- `generation_id`, `level`, `cluster_id`;
- IDs de miembros y hash canónico del membership;
- resumen, rango temporal y proyectos cubiertos;
- embedding del resumen;
- modelo, prompt revision, algoritmo, parámetros y seed;
- job/fencing epoch que lo construyó.

El nodo no muta de staged a active/orphaned. Solo IDs alcanzables desde el manifest que referencia el único `active_generation` del control point son recuperables. “Orphan” es una condición derivada (`generation_id` no activo y fuera de la retention window), no un payload mutable; GC reread el control antes de delete.

### 8.4 `control`, `processing_policy`, `job`, `coverage`, `evidence_link` y `tombstone`

No participan en búsqueda semántica:

- `control`: active generation, versión CAS, `privacy_epoch`, coordination policy state/hash/epoch, último forget barrier, schema revision y scan cursor optimista.
- `processing_policy`: snapshot content-addressed, no secreto, de destinos permitidos, origin provider, cross-provider flag y retention deadline policy del productor.
- `job`: membership explícito, owner, lease, fencing token, estado, policy intersection y propuesta aceptada.
- `coverage`: confirma que un episodio fue procesado por una versión de curación.
- `evidence_link`: relación content-addressed assertion/summary -> episodio, con job y revision.
- `tombstone`: exclusión lógica fuerte por target/provenance y activación de cleanup derivado.

## 9. Captura episódica

### 9.1 Contenido incluido

- mensajes finalizados de usuario y asistente;
- nombre y argumentos redacted de tool calls;
- extractos de tool results que contienen un resultado relevante;
- errores, stderr acotado, códigos, identificadores y firmas de fallo.

### 9.2 Contenido excluido

- system/developer prompts;
- custom messages internos;
- `<memory-context>` recuperado o cualquier memoria reinyectada;
- thinking privado y firmas del proveedor;
- vectores, auth metadata y credenciales;
- outputs completos sin selección;
- mensajes abortados o parciales salvo un error final seguro y explícito.

Un resultado de herramienta se selecciona por error, estado no exitoso o allowlist de campos de resultado útiles. Los campos sensibles se eliminan por nombre antes del scanner textual. Paths del home se canonicalizan a `$HOME`; URLs se redactedan si contienen userinfo o query secrets.

### 9.3 Children

Children de Prime y subagentes Pi escriben hojas con rol/profundidad. No ejecutan auto-recall, curación ni RAPTOR. Prime resuelve child primero desde el session header y después desde `RLM_DEPTH`; Pi usa los marcadores heredados `PI_SUBAGENT_CHILD=1` y `PI_SUBAGENT_DEPTH>0`. Un marcador inválido o contradictorio deshabilita las tareas root fail-closed, pero no convierte un child en root. Pueden usar `memory_search` contra la colección de su host. Cualquier root autorizado puede procesar posteriormente sus episodios.

## 10. Identidad de proyecto multi-máquina

Remote Git, root commits y el contenido del checkout son señales de relevancia, no credenciales. Un repositorio puede modificar `.git/config`; por tanto nunca obtiene acceso a memoria de otro proyecto solo por imitar su origin.

v2 distingue:

1. **registro operador**: `project register --path <canonical-path> --alias <stable-id>` escribe en XDG un binding entre path canónico, fingerprint VCS observado y alias; produce `project_identity_kind=registered`. Cada máquina que deba compartir scope registra explícitamente el mismo alias;
2. **no registrado**: `project_id=H(installation_salt, canonical_path, vcs_fingerprint)` y `project_identity_kind=local_only`. No converge entre instalaciones aunque origin/commits coincidan.

El fingerprint de relevancia prefiere origin canónico y usa root commits ordenados como fallback; elimina protocolo, userinfo, credentials, query y sufijo `.git`. Se muestra redacted y permite advertir mismatches, pero no cambia autorización. Runtime exige que path y fingerprint sigan coincidiendo con el binding XDG; cambio o symlink escape deshabilita recall/capture fail-closed hasta re-registro humano. Un checkout en otra ruta con origin spoofed permanece local-only. El repositorio nunca puede aportar el alias, endpoints o permisos.

## 11. Redacción y tratamiento no confiable

`capture.enabled=false` es el default de upgrade. `init` exige que el operador elija retención y confirme destinos de egress antes de activarla. Allow/deny de proyecto se evalúa antes de capturar; deny gana y ninguna configuración del repositorio puede habilitar captura.

El texto se redacted antes de tocar disco local, embeddings, Qdrant o el LLM de memoria. El pipeline aplica:

1. redacción estructural de headers, campos sensibles, bearer tokens y URL credentials;
2. scanner de secretos conocido y detector de alta entropía;
3. límites por tipo y presupuesto total;
4. canonicalización de Unicode y eliminación de control characters;
5. hash sobre la representación final almacenada.

Si un secreto puede eliminarse sin destruir la utilidad, se reemplaza por una marca tipada. Si no, se descarta el fragmento textual y solo se registra un evento no recuperable con categoría redacted. Nunca se marca `secret_scan=passed` por defecto tras un error del scanner. `passed` significa “ningún detector configurado encontró un secreto”; no demuestra ausencia de secretos, PII o información propietaria. `status` y `init` muestran este riesgo residual.

Ningún texto se envía a Qdrant, embeddings o LLM si el destino canónico no satisface `privacy.egressMode`. En `local_only` solo se aceptan loopback/Unix sockets y sus destination IDs efectivos se incorporan al processing policy aunque las listas estén vacías; esos IDs incluyen `node_id`, de modo que loopback de otra máquina no hereda permiso. En `allowlist`, Qdrant, cada endpoint de embeddings y cada provider/model LLM deben coincidir exactamente. El fallback al modelo activo requiere `allowActiveModelFallback=true`; por defecto solo procesa episodios de la sesión actual y del mismo provider. Reprocesar sesiones de otro provider exige además `allowCrossProviderReplay=true`. El registro conserva únicamente un destination ID redacted, nunca credenciales. Cada allowlist entry declara residency y data-use aprobados; `init` advierte que el plugin puede hacer match y auditar esa política, pero no verificar las prácticas reales del proveedor. PII/proprietary data no se considera anonimizados por redacción; el operador debe excluir el proyecto o usar destinos aprobados.

La expiración episódica sigue la política elegida. Un sweeper usa el mismo privacy-epoch/tombstone protocol de `forget`, reconstruye derivados y después borra texto; una curated memory sin evidencia recuperable deja de ser current, y un summary sin miembros recuperables se excluye. `indefinite` es una elección explícita, no un default oculto.

Conversaciones y tool outputs pueden contener instrucciones legítimas o maliciosas. No se consideran autoridad. Curación y RAPTOR reciben contenido dentro de un envelope de datos no confiables; las salidas se validan contra JSON estricto. La recuperación usa `<memory-context trust="untrusted">` y siempre muestra temporalidad/provenance.

## 12. Outbox durable

Después de redacción, cada proceso escribe jobs atómicos en:

```text
~/.pi/agent/pi-qdrant-memory/outbox/<node-id>/<producer-uuid>/
~/.prime/agent/pi-qdrant-memory/outbox/<node-id>/<producer-uuid>/
```

Los directorios son `0700`, archivos `0600`. Cada job usa write-temp, fsync, rename y fsync del directorio. Solo contiene payload redacted pendiente; no es una segunda base de recuerdos.

Características:

- entrega at-least-once e IDs idempotentes;
- backoff con jitter y deadline por intento;
- máximo default de 10.000 jobs o 256 MiB por host;
- nunca descarta silenciosamente un job ya aceptado;
- al llenarse, la conversación continúa, captura nueva se detiene y se notifica al operador;
- shutdown intenta un flush acotado y deja el resto durable;
- otra instancia puede adoptar una producer outbox cerrada o con heartbeat vencido; el lock local solo reduce duplicación;
- locks locales nunca deciden qué entrega o publicación es válida en Qdrant.

`node_id` se deriva de un override operador o de machine ID + salt de instalación; se muestra redacted. Cada proceso genera un `producer_uuid` CSPRNG de 128 bits y crea su directorio de forma exclusiva; ningún proceso comparte un directorio de escritura. Homes clonados o compartidos no son aislamiento: se soportan solo con `outbox.sharedFilesystem=true` y un `outbox.nodeId` único por máquina; `status` advierte si detecta un filesystem remoto no declarado, sin afirmar que pueda detectar todos. En NFS, adopción puede ocurrir por más de un nodo porque locks/heartbeats no son una barrera distribuida; los archivos son inmutables, solo se eliminan tras reread de Qdrant, y entregas duplicadas convergen por IDs idempotentes. Tests cubren salt duplicado, node IDs iguales, home compartido y adopters concurrentes.

## 13. Coordinación distribuida

### 13.1 Garantías usadas

La implementación se limita a garantías documentadas:

- upserts con el mismo ID son idempotentes;
- `insert_only` ignora IDs existentes;
- `update_only` no crea puntos ausentes;
- `update_filter` permite optimistic concurrency control sobre payload versionado;
- `wait=true` espera a que la operación termine, no sustituye la configuración de replicas;
- `ordering=strong` serializa writes a través del leader permanente y puede quedar indisponible si ese leader cae;
- read consistency y `write_consistency_factor` son controles separados en un cluster replicado.

No se afirma que Qdrant ofrezca transacciones multipunto. El protocolo evita depender de ellas.

### 13.2 Lease con fencing

Un job tiene un punto de control determinista. Adquisición inicial usa `insert_only`. Si ya existe, el contender lee con consistencia configurada y solo roba un lease vencido mediante:

- `update_mode=update_only`;
- `update_filter` sobre `version`, owner/estado esperado y expiración;
- `ordering=strong&wait=true`;
- incremento monotónico de `version` y `fencing_token`.

Después de acquire, renew, steal o release, el cliente reread y verifica owner, versión y token. Una respuesta HTTP exitosa no basta porque un `insert_only` ignorado también es una operación válida.

Los relojes pueden tener skew. El lease controla coste, no corrección. Un worker con reloj adelantado puede provocar trabajo duplicado, pero al incrementar el fencing token invalida publicaciones del owner anterior. La publicación CAS es la barrera de corrección.

### 13.3 Curación at-least-once con efectos idempotentes

La unidad de curación contiene una lista explícita y ordenada de episode IDs. Su `job_id` cubre membership, extractor revision, coordination policy hash+epoch y processing-policy intersection. El sistema no afirma exactly-once processing ni atomicidad multipunto; afirma una propuesta aceptada por job y materialización reanudable con IDs deterministas.

1. Un worker reclama el job solo si privacy y coordination epochs/hashes coinciden y `policy_state=active` en collection control.
2. Inmediatamente antes de cualquier egress, reread ambos epochs y processing-policy intersection; después genera una propuesta LLM ligada a fencing/policy epochs.
3. Escribe la propuesta como punto inmutable separado.
4. Reread control; CAS del job selecciona exactamente una `accepted_proposal_id` solo para el hash/epochs del job. Por la ausencia de CAS cross-point, una race puede aceptar físicamente una propuesta retirada, pero nunca hacerla active.
5. Antes y después de materializar, reread control. Solo la policy activa crea observation/evidence points y `curated_current` bajo su policy epoch; outputs viejos quedan `retired`/invisibles por filtro aunque un stale write llegue.
6. Tras materializar observations, actualiza cada `curated_current` mediante OCC solo si el effective order es posterior; después escribe coverage policy-specific y marca el job completo.

El primary evidence es la evidencia directa más reciente por `(event_at, episode_id)` dentro del item aceptado; `content_id` completa el orden del estado. La visibilidad de observations no es atómica, pero current cambia en un único point CAS y nunca apunta a una observation no materializada. El reconciler termina propuestas aceptadas; nunca materializa una propuesta no aceptada. Si el proceso cae antes de actualizar current, la vista anterior permanece segura y el retry avanza idempotentemente.

Una propuesta de un worker stale puede quedar almacenada, pero no puede convertirse en aceptada tras perder el token. Si el proceso muere después del LLM y antes de guardar la propuesta, la llamada puede repetirse; no se promete exactly-once billing.

Discovery y reconciliation pueden crear jobs con memberships solapados. `content_id`, `observation_id` y `evidence_link` no dependen del job y convergen por `insert_only` + reread/hash cuando identidad/evidencia coinciden; observations de igual contenido se pliegan temporalmente y un solo `curated_current` se serializa por OCC. Duplicados semánticos no idénticos son posibles y se etiquetan/consolidan después; la garantía no los llama exactly-once.

### 13.4 Descubrimiento sin perder episodios tardíos

No existe un sequence global de ingest confiable entre máquinas. Por tanto, un timestamp cursor no es una prueba de coverage.

- El cursor y buckets temporales solo optimizan scans normales.
- Cada extractor revision escribe coverage por episode ID.
- Un reconciler periódico recorre episodios por slices, batch-retrieve sus coverage IDs y encola faltantes.
- Scans usan overlap para capturar writes tardíos.
- El operador puede forzar reconciliación completa.

Así, clock skew o una máquina offline no pueden excluir permanentemente un episodio.

### 13.5 Publicación RAPTOR

Cada build fija un manifest ordenado de hojas elegibles y un `manifest_hash`. El manifest se almacena como chunks content-addressed acotados y una raíz Merkle; nunca como un payload único ilimitado. Episodios que llegan después siguen buscables directamente y entran en la generación siguiente.

El builder escribe nodos inmutables bajo un generation ID ligado al job, fencing epoch y `privacy_epoch`. Tras validarlos, intenta actualizar el mismo collection control con CAS sobre versión, base generation y privacy epoch observados. Solo una publicación gana; un forget compite sobre ese mismo point y no puede intercalarse silenciosamente.

Si otro builder publicó primero, el CAS falla. Los nodos perdedores quedan invisibles; se consideran orphaned por comparación con el control y entran en garbage collection sin mutar sus payloads. Un builder viejo nunca reemplaza una generación más nueva porque su base version ya no coincide.

### 13.6 Replicas y disponibilidad

Control, claims, fencing y publicación siempre usan `ordering=strong`, `wait=true` y read consistency configurable, con `majority` como default de cluster. En single-node, consistency efectiva es una réplica.

Para cluster, init exige replication factor >=2 y `write_consistency_factor=ceil((replicationFactor+1)/2)` como mínimo; single-node usa 1/1. El operador puede exigir más, no menos. Si no se alcanzan acknowledgements, Qdrant puede devolver error después de aplicar parcialmente; todos los writes se reintentan idempotentemente y se verifican por reread. Esto, no `wait=true` por sí solo, permite que el batch-read `majority` final observe tombstones confirmados.

La caída del leader puede hacer strong writes temporalmente indisponibles. Captura queda en outbox y la conversación sigue; el protocolo no degrada silenciosamente a weak ordering para control.

### 13.7 Política multi-máquina

El collection control publica un `coordination_policy_hash` y epoch monotónico sobre schema/vector contract, canonicalization, curation y parámetros/algorithm revisions RAPTOR. Captura puede continuar si schema/vector coinciden, pero un root solo reclama curación/RAPTOR cuando su policy hash+epoch coincide.

Schema/vector contract dentro del hash es inmutable para esa colección; cambiarlo requiere una colección/version nueva. Cambiar canonicalization/curation/RAPTOR policy usa dos CAS sobre el mismo control:

1. pasa `policy_state=active` a `draining`, fija `active_generation=null` y deshabilita visibilidad derived-current; workers no reclaman jobs ni comienzan egress si el state no es active;
2. espera release/expiración de leases antiguos más el máximo timeout de llamada configurado; llamadas ya iniciadas pueden terminar y se declaran en el plan;
3. CAS incrementa epoch, instala el hash nuevo y vuelve a `active`.

Desde draining retrieval current deja de aceptar views antiguas. Reconciliation re-extrae episodios bajo IDs/views del epoch nuevo; canonicalizer/state keys nuevos no compiten con los antiguos. Un worker conforme que reaparece tarde solo puede escribir outputs retired/invisibles. Dos archivos XDG distintos no hacen oscilar generaciones.

Cada productor persiste un `processing_policy` content-addressed y lo referencia desde outbox/episode:

- exact destination IDs autorizados para Qdrant, embeddings y LLM;
- origin provider y permiso cross-provider;
- `expires_at` calculado desde `event_at` y la retención elegida, o null explícito para indefinite;
- policy revision y residency/data-use labels redacted.

Un worker procesa un conjunto solo con la intersección de permisos de todas las fuentes, revocaciones collection-wide y su propia política local. Sin un LLM/embedding destination común, divide el job por grupos compatibles o deja esas hojas flat/pending; un RAPTOR global multi-máquina requiere un destino dedicado explícitamente permitido por todos los producer policies, y loopback node-bound no cuenta como común; nunca usa el destino de la máquina coordinadora como permiso implícito. Todo derivado hereda la intersección y el `expires_at` más temprano.

Outbox guarda policy ID y deadline inmutables. Antes de entregar, writer usa `now + coordination.maxClockSkewMs`: un job vencido elimina su payload local, conserva solo un audit hash no reversible y nunca se inserta como fresco. Toda búsqueda filtra `expires_at=null OR expires_at>now+skew`; un stale/offline writer que inserte tarde permanece invisible y el sweeper lo elimina. Un cambio XDG aplica a datos futuros y puede estrechar qué procesa esa máquina; revocar retroactivamente un destination/policy requiere `privacy revoke`, que entra en draining/quiescence, escribe control global, incrementa privacy epoch y reconcilia derivados como forget; no revoca bytes de una llamada ya iniciada y lo declara. Así ninguna máquina decide por sí sola la retención o egress de datos producidos por otra. Ningún software puede borrar una outbox mientras su máquina/disco está offline; el límite se declara y el primer startup elimina el payload vencido antes de cualquier egress/delivery.

## 14. Curación autónoma

Defaults:

- trigger cada 10 turns root o 15 tool calls;
- revisión antes de compaction;
- shutdown solo persiste el trabajo pendiente;
- máximo un claim efectivo de curación por batch/host;
- ventana acotada por tokens y recent messages, siempre con membership explícito.

Resolución LLM:

1. modelo dedicado configurado en el host/model registry;
2. modelo activo de la sesión root si `privacy.allowActiveModelFallback=true`;
3. sin modelo válido, episodios continúan disponibles y el job queda pendiente.

No se usa el endpoint BGE-M3 para generación. El prompt de curación no expone herramientas. Usa temperatura baja, JSON schema estricto, timeout, cancelación y budgets. Cada resultado registra proveedor/model ID, prompt revision y fecha.

El validador solo acepta categorías y scopes conocidos. Una preferencia o corrección requiere evidencia directa del usuario; un tool output no puede inventar standing instructions. La memoria curada permanece no confiable durante recall.

## 15. RAPTOR generacional

### 15.1 Núcleo

La implementación conserva los elementos esenciales descritos por Sarthi et al.:

1. embeddings de chunks/hojas;
2. reducción UMAP;
3. clustering global y local;
4. GMM con selección del número de clusters mediante BIC;
5. soft membership;
6. resúmenes model-based;
7. embedding y clustering recursivos hasta niveles superiores.

Defaults v2:

- dimensión UMAP 10;
- seed persistida por colección/policy revision;
- global neighbors derivados de la raíz de N y local neighbors 10, ambos clampados al tamaño del corpus;
- candidatos GMM `1..min(50, N-1)`;
- membership probability >=0,10;
- máximo 5 niveles;
- clusters que exceden el presupuesto del summarizer se reclusterizan antes de llamar al LLM;
- rebuild tras 64 episodios nuevos o comando administrativo.

La implementación seleccionada usa `umap-js@1.4.0` (tratar como Apache-2.0 según el `LICENSE` del tarball) con su `random` inyectable y un GMM diagonal regularizado/EM+BIC escrito y probado en TypeScript dentro del paquete; no depende de una librería GMM externa. El PRNG interno es `xoshiro128**`, inicializado con 128 bits de SHA-256 de la seed versionada e inyectado tanto en UMAP como en inicialización k-means++ del EM. GMM usa covarianza diagonal, variance floor `1e-6` tras estandarización, máximo 100 iteraciones y tolerancia `1e-4`; `BIC=-2*logLikelihood+p*ln(N)` con `p=K*(2D)+(K-1)`. Estas fórmulas se validan contra fixtures congeladas. No se introduce Python; cualquier cambio de algoritmo o dependencia cambia `algorithm_revision`.

Base cases y terminación:

- `N=0`: no se publica una generación nueva;
- `N=1`: el episodio/nodo único queda como raíz sin ejecutar UMAP/GMM ni forzar un resumen;
- `N=2`: se usa un cluster determinista si cabe en budget; si no, quedan dos raíces;
- para `N>=3`, `nComponents=min(configured, N-2, embeddingDimension)` y neighbors se clampan a `2..N-1`;
- dimensiones con varianza <= floor se fijan a cero/excluyen antes de UMAP/GMM; si todos los embeddings son iguales se omiten ambos y se usa partición estable token-aware;
- covarianzas usan variance floor versionado; fits no finitos/singulares se descartan del BIC;
- si ningún GMM es válido, se usa una partición estable token-aware por IDs; ningún turno falla por clustering;
- cada punto entra al menos en el cluster de máxima probabilidad, memberships idénticos se deduplican;
- si un nivel no reduce el número de nodos, no cambia membership o alcanza el máximo, el build termina sin bucle.

Soft membership produce un DAG, no un árbol estricto. Cada edge aumenta exactamente el nivel, se rechazan ciclos, y retrieval deduplica nodos y episodios evidencia por ID durante descent. Los parámetros son configurables, versionados y forman parte de generation identity.

### 15.2 Reutilización

El DAG lógico cubre todo el manifest. El contenido reutilizable se identifica mediante membership hash + prompt revision + modelo + algoritmo. Cada generación conserva sus propios puntos `raptor_summary`; cuando el hash coincide, el builder copia summary/vector validados desde un nodo anterior en vez de repetir LLM o embeddings. Así se mantiene el filtro simple por `generation_id` sin hacer mutable un nodo antiguo. Solo clusters modificados y sus ancestros consumen llamadas nuevas.

Una generación antigua puede limpiarse después de una ventana de seguridad; episodios y curated memories no se eliminan por esa limpieza.

### 15.3 Calidad y evidencia

El paper documenta que summaries pueden alucinar. Por tanto:

- un hit de resumen siempre desciende a miembros concretos;
- el contexto entregado incluye episodios evidencia;
- summaries no cambian validity de memorias;
- toda afirmación se etiqueta como summary derivado;
- un nodo sin miembros recuperables se excluye.

## 16. Recuperación

Antes de puntuar, todos los carriles aplican `owner_host`, project/root policy y `expires_at=null OR expires_at>now+skew`; carriles derivados current/RAPTOR exigen además el coordination-policy epoch activo. Antes de devolver `memory_search` o inyectar auto-recall, el destination ID del modelo activo debe pertenecer a la intersección del processing policy de cada resultado, revocaciones globales y policy local; un hit no autorizado se excluye, porque un tool result también es egress al modelo. El tombstone/provenance check final sigue siendo obligatorio.

Se ejecutan carriles independientes:

1. `curated_current` para current y `curated_memory` observations para historical;
2. episodios dense BGE-M3;
3. candidatos precisos por full-text, tool name, error code/fingerprint e identificadores;
4. nodos RAPTOR de la generación activa.

Scores no comparables se normalizan por carril y se combinan con reciprocal-rank fusion. Después se aplica diversidad para evitar duplicados. El scope default es exactamente el proyecto actual; material de otro proyecto nunca es elegible. Memoria `global` same-host solo entra para una sesión root cuando el operador fija `retrieval.rootScope=project_and_global` en XDG. Children permanecen project-only y `memory_search` no expone un argumento para elevar scope.

Para RAPTOR se combina collapsed candidate search sobre niveles activos con DAG descent deduplicado para aportar evidencia. Curated memories también expanden sus episode IDs.

La identidad `local_only` aísla el scope a esa instalación; no se trata como global entre máquinas. Si no hay identidad de proyecto válida, auto-recall y child search se deshabilitan fail-closed; una root puede seguir consultando memoria global solo bajo la política explícita anterior.

`memory_search` acepta solamente:

```text
query, limit?, mode?, after?, before?
```

`mode` es `all|current|historical|episodes|curated|raptor`. No existen argumentos de host, colección, endpoint, status o credenciales.

Modo current busca solo vistas `curated_current`. Modo historical etiqueta el policy epoch, ordena observations dentro de cada epoch, colapsa contenidos consecutivos iguales y deriva `valid_from`/`valid_to`; puede incluir el tramo previo/siguiente para explicar una transición, siempre etiquetado como histórico.

Auto-recall:

- habilitado en Pi root y Prime root;
- deshabilitado en children/subagentes;
- efímero y ausente del JSONL;
- nunca se recaptura como episodio;
- fail-open ante cualquier fallo de memoria.

## 17. Olvido y obsolescencia

No hay borrado model-callable.

Una corrección crea una observation posterior y hace avanzar `curated_current`; la historia previa permanece inmutable. `forget` es un comando humano con barrera lógica, no un simple delete multipunto:

1. crea un plan content-addressed que muestra targets redacted, source/provenance closure, generaciones, observations y vistas current derivadas; una selección `curated_current` se resuelve a su observation, nunca tombstonea por defecto el ID estable de la vista; los scopes `occurrence` (default), `content` y `state` se muestran por separado, y solo los dos últimos bloquean recurrence futura;
2. requiere selección y confirmación del plan exacto;
3. inserta con strong ordering/wait tombstones inmutables para targets y provenance source IDs; cada ID es `H(owner_host, "tombstone", target_id)` para batch retrieve directo, y la recuperación los lee con consistency `majority` como último filtro antes de devolver cualquier hit;
4. CAS del collection control incrementa `privacy_epoch`, registra el forget ID y fija conservadoramente `active_generation=null` en el mismo point; todo forget exige rebuild antes de volver a servir summaries;
5. reread confirma tombstones y epoch; solo entonces el comando puede informar éxito lógico;
6. writers, curators y builders etiquetan todo write/job con el epoch observado, reread el control antes de accept/publish y abandonan si cambió;
7. outboxes y proposals del epoch antiguo se reevalúan: targets tombstoned se cuarentenan; materialización derivada conserva provenance y queda excluida aunque un stale worker logre escribir físicamente;
8. un reconciler elimina targets y todos los derivados alcanzables, invalida coverage, recalcula `curated_current` desde observations no olvidadas y encola una generación limpia. Bloquear para siempre un `state_key` es una opción separada y explícita del plan.

Qdrant no permite condicionar un point write a otro control point de forma transaccional. Por eso `privacy_epoch` reduce carreras, pero la barrera de privacidad real es el filtro de tombstones/provenance aplicado después de cada búsqueda y antes de context injection. Un retrieval que empezó antes de `forget` también hace ese batch-check final. Episode IDs, `evidence_link`, curated assertions y manifests RAPTOR conservan provenance suficiente para cierre transitivo; un derivado sin provenance verificable se excluye fail-closed.

Éxito significa que el contenido y sus derivados ya no pueden ser recuperados por el plugin, incluso si un retry stale los reinserta temporalmente. Cleanup físico es eventual e idempotente. El comando no puede revocar contexto ya devuelto al host o enviado a un modelo antes de la barrera; cada retrieval nuevo o todavía no finalizado compara epoch y tombstones justo antes de construir su respuesta. No se promete borrar bytes de snapshots, backups o storage segments ya administrados por Qdrant; esas limitaciones se muestran antes de confirmar.

## 18. Operación y observabilidad

`status --json` incluye:

- host, endpoint canónico y colección efectiva;
- metadata/owner/schema/dimensión/distancia;
- auth mode sin revelar keys;
- capture opt-in, project registration, producer retention/egress destination IDs y active revocations redacted;
- local/active `coordination_policy_hash` y mismatch state;
- retrieval root/child scope y outbox shared-filesystem mode;
- conteos exactos por record type/estado;
- outbox size, oldest job y failed attempts;
- curation coverage y reconciliation age;
- active generation, manifest, niveles, orphan count y `privacy_epoch`;
- jobs/leases con timestamps y owners redacted;
- embeddings health y modelo;
- disponibilidad del LLM dedicado/fallback;
- última categoría de error redacted.

Comandos administrativos:

- `init --json` por host;
- `project register|unregister|status` con confirmación humana;
- `privacy revoke` con plan/approve y privacy-epoch barrier;
- `status --json`;
- `curate --enqueue|--wait`;
- `raptor rebuild --enqueue|--wait`;
- `reconcile --enqueue|--wait`;
- `inspect` con resultados acotados/redacted;
- `forget` interactivo o con plan/approve para modo headless.

No existe `import-hermes` en v2.

## 19. Error handling

- Captura/redacción falla: no se almacena el fragmento; warning redacted una vez por categoría.
- Outbox/Qdrant falla: turn fail-open, job durable y retry.
- Egress no autorizado: no se hace la llamada; episodio/job queda local o pendiente y `status` explica policy mismatch.
- Embeddings falla: episodio queda pendiente, no se escribe con vector inválido.
- Curación falla: episodios siguen recuperables; lease expira y job se reintenta.
- RAPTOR falla: generación activa anterior sigue sirviendo, salvo que forget/retention la haya invalidado; entonces el carril queda vacío.
- CAS falla: el worker reread y abandona o replantea; nunca hace unconditional overwrite.
- Tombstone/epoch final check falla: retrieval devuelve cero memoria fail-closed y el turno continúa fail-open.
- Forget parcial: tombstones ya insertados siguen ocultando targets; el plan reanudable completa epoch, cleanup y rebuild.
- Config/owner mismatch: lectura y escritura deshabilitadas fail-closed.
- Auth/TLS falla: no se intenta otro endpoint o colección.
- Cola llena: no se descartan jobs aceptados; se detiene captura nueva y se notifica.
- Dispose: flush y release best-effort acotados; correctness depende de expiración/fencing.

Logs omiten texto de conversación, query, memory, argumentos completos, paths absolutos, headers, payloads, responses y keys.

## 20. Pruebas

### 20.1 Unitarias

- defaults y overrides host-specific;
- rechazo de colección compartida y owner mismatch;
- project registration estable entre máquinas; path/fingerprint/symlink mismatch y checkout origin-spoofed quedan local-only/fail-closed;
- config v2 precedence/ranges, capture opt-in, producer-policy intersections, retention y egress allowlist;
- project-only default, root global opt-in, child sin escalada y recall bloqueado hacia model destination no autorizado;
- parser rechaza configuración Hermes retirada;
- IDs deterministas de content/observation/evidence, `curated_current` OCC y collision handling;
- selección de eventos y exclusión de contexto inyectado;
- redacción, secret scan y budgets;
- outbox atomicidad, límites, retry/adopción y expiry tras productor offline;
- schemas de todos los record types;
- prompts/validación de curación y migration de policy epoch con misma evidencia/state-key nuevo;
- temporalidad con evidencia tardía, skew/conflict cross-machine, jobs solapados y recurrencia A→B→A sin ciclos;
- clustering determinista, soft membership, BIC, `N=0|1|2`, embeddings idénticos/zero-variance, singularidad y fallback/termination;
- benchmark congelado de clustering/retrieval con umbrales de cobertura y estabilidad frente a baseline flat;
- manifest/node/generation hashing, invariantes DAG y payloads summary inmutables;
- fusion de retrieval, evidence descent y dedup DAG;
- tombstones, privacy epoch, closure transitiva y forget planning.

### 20.2 Qdrant real 1.17.1

Cada test usa una instancia Qdrant 1.17.1 efímera/aislada, nombres aleatorios, allowlist de colecciones creadas por el proceso y cleanup verificado. El harness rechaza el endpoint configurado de producción/live.

- init exacto de `pi_memory`-like y `prime_memory`-like aisladas;
- named vector + payload-only control points;
- `insert_only`, `update_only`, `update_filter`, wait y strong ordering;
- read-back de operaciones ignoradas;
- 20 writers concurrentes con IDs iguales y distintos;
- claim/renew/steal/release;
- fencing de worker stale;
- dos curadores para el mismo membership/memberships solapados y policy-CAS durante LLM/accept/materialize;
- accepted proposal única, content/observation IDs convergentes y `curated_current` serializado;
- dos rebuilds desde la misma base y un único publish, sin mutar nodos perdedores;
- crash antes/después de cada paso;
- network partition/late response simulation;
- reconciliation de episodio tardío;
- dos máquinas con policy hashes/egress/retention iguales y divergentes, outboxes independientes y home compartido/IDs duplicados;
- productor offline más allá de expiry: cero delivery/recall al volver;
- forget intercalado antes/después de ingest, proposal accept, materialización, publish y retrieval final check;
- stale reinsertion físicamente posible pero lógicamente invisible tras la barrera;
- cero resultados cross-host o cross-project sin opt-in root global;
- ninguna request a una colección/source Hermes.

### 20.3 Hosts

La primera implementación apunta exactamente a Pi `@earendil-works/pi-coding-agent@0.84.1` y Prime Agent commit `a18809e00ea30638584d87b3afea7285a9d7296c`, coincidentes con `compatibility.json` v1. Antes del release, `compatibility.json` v2 mantiene `minimum` y `latestTested` exactos; solo se añade un sucesor después de ejecutar todo este contrato contra su artifact/commit público. No se acepta “latest” flotante.

Pruebas por cada target exacto:

- carga sin errores;
- lifecycle capture real;
- child episode tagging;
- root-only curation/RAPTOR;
- dedicated/active model resolution con stubs;
- memory_search y auto-recall efímero;
- shutdown/reload y outbox recovery;
- múltiples instancias simultáneas.

No se usan APIs pagadas en CI. El smoke live usa una conversación sintética no sensible y requiere autorización operacional al momento de activación.

## 21. Release y activación

La ruptura de contrato justifica `v2.0.0`. v1 permanece reproducible por tag, pero no se migra automáticamente.

Secuencia:

1. escribir plan de implementación desde esta spec;
2. TDD en worktree feature/v2;
3. revisión independiente y aceptación directa de Sol;
4. suite completa, tarball/smokes Pi/Prime y auditoría de paquete;
5. release GitHub exacta, sin npm salvo instrucción posterior;
6. backup de settings/configuración;
7. actualizar ambos package pins a v2;
8. configurar `pi_memory` y `prime_memory` por separado;
9. configurar tokens Qdrant por colección salvo loopback de desarrollo explícito, y seleccionar capture, retención, scope y egress por host;
10. ejecutar `init` una vez por host, confirmar el disclosure y verificar colecciones vacías;
11. registrar explícitamente en XDG cada proyecto que deba compartir scope entre máquinas;
12. abrir sesiones nuevas;
13. conversación sintética -> episode write -> curation -> RAPTOR build;
14. reiniciar y verificar recuperación;
15. demostrar que Pi no lee Prime, Prime no lee Pi y nadie consulta Hermes.

No se crea ni modifica ninguna colección live durante diseño, planificación o pruebas no-live. Init live requiere que la implementación y sus dos revisiones hayan sido aceptadas.

## 22. Rollback

- restaurar settings/config backup y pin anterior;
- detener workers v2;
- conservar colecciones v2 por defecto para evitar pérdida;
- cualquier delete de colecciones requiere aprobación separada;
- outboxes quedan preservadas para diagnóstico o se eliminan solo con confirmación;
- nunca tocar colecciones Hermes durante rollback.

## 23. Criterios de aceptación

1. Pi y Prime tienen colecciones físicas diferentes con owner metadata verificable.
2. El paquete v2 no contiene una ruta ejecutable de import/lectura Hermes.
3. Cada evento elegible y aceptado por captura produce un job redacted con efecto de ingest idempotente.
4. Fallos relevantes de herramientas son localizables sin guardar outputs completos.
5. Curación autónoma produce memoria trazable y temporal; content/observation IDs convergen con jobs solapados y A→B→A conserva intervalos correctos.
6. RAPTOR publica generaciones completas atómicamente y cada summary servido aporta evidencia recuperable deduplicada desde el DAG.
7. Recuerdos obsoletos siguen disponibles como historia, no como verdad vigente sin etiqueta.
8. Children escriben hojas y buscan solo su proyecto, pero no ejecutan auto-recall/curation/RAPTOR.
9. Múltiples procesos/máquinas no duplican efectos con la misma identidad determinista ni permiten publish stale; near-duplicates semánticos se reconocen como best-effort.
10. Particiones/crashes no bloquean turns ni corrompen active generation; un coordination-policy CAS oculta views/generación viejas antes de reconstruir.
11. System/developer content, memoria inyectada y fragmentos rechazados/detectados como secretos no entran en storage; el riesgo de secretos/PII no detectados queda explícito.
12. Capture/Qdrant/embeddings/curation/RAPTOR/recall-to-model respetan opt-in y la intersección de políticas producer/collection/worker; retención y egress no se relajan entre máquinas ni hay fallback cross-provider implícito.
13. El scope default impide recall cross-project; global requiere opt-in root y children no pueden elevarlo.
14. Tras el éxito lógico de `forget`, targets y derivados son invisibles aunque un stale writer los reinserte físicamente; cleanup y rebuild son idempotentes.
15. Status permite auditar backlog, coverage, jobs, privacy epoch y generaciones sin filtrar contenido.
16. Tests unitarios, Qdrant real y compatibilidad contra targets exactos de ambos hosts están verdes.
17. Activación live comienza desde cero y prueba aislamiento bidireccional y cero acceso Hermes.

## 24. Hechos oficiales y límites epistemológicos

- RAPTOR introduce embedding, clustering y summarization recursivos y presenta tree traversal y collapsed-tree retrieval. La implementación v2 añade generaciones, caching y coordinación; esas extensiones no forman parte del paper.
- El metadata de licencia de `umap-js@1.4.0` es inconsistente; distribución debe conservar el `LICENSE` Apache-2.0 y pasar auditoría de notices antes del release.
- `umap-js` más GMM diagonal TypeScript no es numéricamente idéntico al stack Python/full-covariance de la implementación de referencia; fixtures prueban determinismo/corrección y benchmarks separados deben validar calidad.
- Qdrant documenta conditional updates desde 1.16 y update modes desde 1.17. Por eso el mínimo real es 1.17, no 1.16.
- Strong write ordering serializa mediante el permanent leader y sacrifica disponibilidad si cae.
- `wait=true` espera completion de la operación; no equivale a quorum.
- `write_consistency_factor` y read consistency solo aportan garantías de replicas cuando la colección está replicada.
- Qdrant no ofrece aquí una transacción cross-point; jobs y publication se diseñan alrededor de esa ausencia.
- Leases con timestamps no eliminan clock skew. Fencing + CAS protegen publication; leases reducen duplicación de trabajo.

## 25. Fuentes primarias

- Sarthi et al., “RAPTOR: Recursive Abstractive Processing for Tree-Organized Retrieval”: https://arxiv.org/abs/2401.18059
- Implementación de referencia RAPTOR: https://github.com/parthsarthi03/raptor
- `umap-js` 1.4.0, implementación JavaScript con PRNG inyectable; el tarball contiene `LICENSE` Apache-2.0 aunque `package.json` declara MIT: https://github.com/PAIR-code/umap-js
- Qdrant Points — idempotence, update modes, conditional updates y wait: https://qdrant.tech/documentation/concepts/points/
- Qdrant Consistency Guarantees — write consistency, read consistency y ordering: https://qdrant.tech/documentation/scaling/consistency-guarantees/
- Qdrant Collections — metadata, named vectors y aliases: https://qdrant.tech/documentation/concepts/collections/
- Qdrant Security — authentication y granular per-collection JWT RBAC: https://qdrant.tech/documentation/guides/security/
