# Enterprise RAG Platform on Databricks + Azure — Locked Design

## Guiding principle

The platform owns the **mechanism** — anything that determines whether something
is done correctly and safely. A team owns the **judgment and content** — anything
that genuinely varies by domain. Everything below is organized around that split,
and every layer follows the same shape: sensible platform-provided default,
explicit team override where it's genuinely needed, nothing silently dropped,
nothing silently ambiguous.

---

## Prerequisites (per application team)

- Dedicated Azure resource group and ADLS Gen2 storage account/directory.
- ADLS Gen2 registered with Unity Catalog as an **External Location**.
- File events enabled on the external location (prerequisite for Autoloader
  file-notification mode — required permissions granted up front).
- Dedicated Unity Catalog **catalog** and **schema** for the team.
- An **External Volume** created on the schema, pointing at the team's ADLS Gen2
  directory — this is a zero-copy governed abstraction. Files are never copied
  or moved; UC governs read/audit against the real files in place.
- A separate **Managed Volume** on the same schema, reserved for pipeline
  internals (checkpoints) — never nested inside the external/source volume.
  Rationale: source data and pipeline operational metadata are different
  governance domains with different lifecycles; mixing them risks checkpoint
  corruption and conflates two things that should stay separable.

---

## Bronze Layer

### Ingestion

- **Autoloader (`cloudFiles`)**, format = `binaryFile` — captures every file's
  raw bytes; no structural parsing happens at this stage, by design.
- **File-notification mode**, not directory listing — scales with files
  arriving, not with total directory size; required for cost-efficient
  ingestion at real volume.
- Defined as a **Lakeflow Declarative Pipeline** (current name; formerly Delta
  Live Tables / DLT) — streaming table, append-only semantics.
- Checkpoint location: a unique path per pipeline inside the team's dedicated
  Managed Volume. Never shared between pipelines. Never nested inside the
  external volume being scanned.

### Bronze table schema

```
content                    binary    -- full raw file content, always captured,
                                          regardless of file type (see rationale)
source_path                 string
file_name                    string
file_size                    long
file_modification_time       timestamp
ingestion_timestamp           timestamp
content_hash                  string   -- SHA-256; stable identity independent
                                             of path or filename
file_type                     string   -- detected from content signature/magic
                                             bytes, NOT from file extension alone
source_classification_tag     string   -- raw signal only (e.g. folder path);
                                             resolved into an enforced ACL group
                                             at silver via a join, not baked in here
processing_status             string   -- e.g. "valid", "corrupt",
                                             "unsupported_file_type" (content is
                                             STILL captured in this case -- see
                                             rationale), "no_extraction_rule_matched"
_rescued_data                  string   -- Autoloader default; kept, never dropped
```

**Key rationale, worth preserving verbatim for the article/repo README:**

- **Every file, every type, full content, unconditionally captured.** Storage
  is a small one-time cost; failing to capture an unsupported file type risks
  a much larger, uncertain future cost (re-fetching from source that may have
  moved or changed, or blocking a team's onboarding on old files). Bronze
  judges nothing; it captures everything. All type-support policy is a
  silver-layer decision.
- **`file_type` is detected from actual content, not the file extension.**
  Extensions lie (renamed files, corrupted uploads). This is the same
  principle as using a content hash instead of trusting a path.
- **ACL resolution is deliberately NOT baked into bronze as a literal value.**
  Access policy changes over time (reclassification, corrected mappings).
  Baking it into bronze would mean re-ingesting PDFs just to fix an access
  decision. Instead: bronze captures a raw signal, a centrally-owned mapping
  table (owned by whoever owns access policy, not the ingestion pipeline)
  resolves it into an enforced group at silver via a join.

### Observability (platform-provided, zero team effort)

- **Backlog dashboard**: a standard Lakeview dashboard built on
  `cloud_files_state(checkpoint_path)` (per-file discovery/processing state,
  queryable SQL, available DBR 11.3 LTS+) and the Lakeflow pipeline's own
  event log (`numFilesOutstanding`, `numBytesOutstanding` from the Streaming
  Query Listener). Parameterized only by checkpoint path — a team supplies
  nothing else.
- **`approximateQueueSize`** (file-notification queue depth) is the more
  honest number in the first minutes after a bulk file drop, since
  `numFilesOutstanding` only updates as of the last completed micro-batch.
- **Alerting**: a Databricks SQL Alert, defined once in the shared bundle
  template, running on a schedule (not instantaneous/real-time) against the
  same source. A team supplies only a threshold number and a distribution
  list/email.
- Both dashboard and alert are definable directly inside a Declarative
  Automation Bundle (DAB; formerly Databricks Asset Bundles) alongside the
  pipeline itself — one deployable unit per team, not several manually wired
  pieces.

---

## Silver Layer

### Extraction

Two-tier resolution, checked in order:
1. **Path-prefix override**, if a team has declared one for a specific
   sub-directory (e.g. `contracts_scanned/` needs OCR, `reports_digital/`
   doesn't).
2. **File-type default**, if no prefix rule matches — a platform-provided
   baseline per type, so a team with simple needs can onboard with zero
   extraction config and just work correctly.

```yaml
extraction:
  defaults:
    pdf:  "pymupdf_standard"
    docx: "python_docx_standard"
    xlsx: "openpyxl_standard"
  overrides:
    - path_prefix: "contracts_scanned/"
      file_type: pdf
      method: "pymupdf_ocr"
      on_failure: "atomic"   # or "partial" -- team-declared policy per rule
```

- Each approved method is a **platform-owned, versioned wrapper function** in
  a dispatch registry (`(file_type, method_name) -> function`). Adding a new
  method later means adding one registry entry — no existing team's config
  needs to change.
- **`on_failure` policy — atomic is the default, partial is an explicit
  opt-in.** Atomic means one failed page invalidates the entire document
  (nothing partial reaches chunking) — the safer default, since a chunk from
  page 46 with no signal that page 45 failed is silently-wrong, which is worse
  than visibly-missing. Partial is available for teams with genuinely
  independent sections (e.g. unrelated chapters) where one failure shouldn't
  block everything else.
- **`"no_extraction_rule_matched"`** (bronze `processing_status`) is now rare
  under the two-tier model — only occurs for a genuinely unsupported file
  type, not a missing team override.

### Chunking

Same two-tier default/override shape, keyed the same way as extraction.
Approved strategies: fixed-size w/ overlap, recursive/structure-aware,
semantic (embedding-similarity boundary detection), table-aware/row-based
(for XLSX), heading-hierarchy split (for DOCX/structured PDFs).

```yaml
chunking:
  defaults:
    pdf:  { method: "recursive_structure_aware", chunk_size: 512, overlap: 50 }
    docx: { method: "heading_hierarchy_split", chunk_size: 512, overlap: 50 }
    xlsx: { method: "table_aware_row_based", chunk_size: null }
  overrides:
    - path_prefix: "contracts_scanned/"
      method: "semantic_split"
      chunk_size: 256
      overlap: 25
```

### Silver schema — split into content and instances (critical design point)

Physical chunk content and logical "a file pointed here" records are
deliberately separated, because the same content can be referenced by more
than one source file (a rename, a copy, a move) — see rationale below.

```
-- silver_chunk_content: one row per unique (content_hash, chunk_index)
chunk_id                 string   -- deterministic hash(content_hash + chunk_index);
                                        NOT a random UUID -- makes silver reruns
                                        idempotent rather than duplicating rows
content_hash               string
chunk_text                 string
section_heading            string   -- nullable; populated only when the method
                                          captures document structure
chunking_method_used       string   -- recorded PER CHUNK, not per team, since
                                          overrides mean this varies within one
                                          team's corpus
extraction_method_used     string   -- same reasoning
token_count                 int
is_active                   boolean  -- false once no active instance references
                                          this content (see cleanup rule below)

-- silver_document_instances: one row per (source_path) ever seen
source_path                 string
file_name                    string
content_hash                 string  -- FK to silver_chunk_content
ingestion_timestamp           timestamp
file_type                     string
resolved_acl_group            string  -- output of the bronze-tag join; if a
                                            content_hash's instances resolve to
                                            more than one distinct ACL group,
                                            the MOST RESTRICTIVE wins, and the
                                            conflict is flagged for human review
                                            -- never silently picked either way
is_active                     boolean
deactivated_at                timestamp  -- nullable
```

**Key rationale, worth preserving verbatim:**

- **Deduplication by content hash avoids reprocessing identical content that
  arrives under a different name/path** (a real, expected scenario — a file
  renamed with no content change). Silver checks whether `content_hash`
  already exists in `silver_chunk_content` before running extraction/chunking
  again; if it does, only a new instance row is inserted.
- **The unifying invalidation rule**: a chunk's content stays valid (and stays
  in gold) only while at least one active instance still references it. A
  rename, a move, and a true source deletion are the SAME event from this
  rule's perspective — an instance stops being live, with or without a
  replacement instance appearing.
- **Deletion detection requires a separate, scheduled reconciliation job** —
  Autoloader cannot natively detect that a file was removed from source; this
  is a confirmed, structural limitation of the tool, not a gap in this design.
  The reconciliation job (platform-owned, scheduled) lists each team's live
  Volume contents and compares against currently-active `document_instances`
  rows; anything active-but-missing gets `is_active = false,
  deactivated_at = now()`. This has an inherent latency window between
  reconciliation runs — a real, named tradeoff, not hidden.
- **Silver-level observability**: Lakeflow `expectations` (data-quality
  constraints declared on the pipeline) flag rows with failed extraction or
  implausible token counts without necessarily failing the whole run; their
  pass/fail counts land automatically in the same pipeline event log bronze's
  dashboard already reads. A standard silver dashboard extends the bronze
  pattern: failure rate by method, method-usage distribution, token-count
  outliers. Same SQL Alert mechanism, reused.

---

## Gold Layer

### Embedding model selection

Same two-tier pattern again. Models are registered in **MLflow Model
Registry** under a stable alias (e.g. `prod`), not a raw version number, so
the platform can promote a new model version without every team's config
changing.

```yaml
embedding:
  default:
    model: "gte-large-en"
  overrides:
    - path_prefix: "financial_exports/"
      model: "code-aware-embed-v2"
```

**Critical property, not shared with extraction/chunking**: two different
embedding models produce vectors in incomparable spaces. Changing a team's
model is a **migration**, not a config edit — it requires either a full
re-embedding backfill or a parallel index cutover, and should go through the
same reviewed deployment path as any production change.

### Gold table schema

```
chunk_id                 string    -- PK; required by Vector Search
chunk_text                string
embedding_vector           array<float>  -- dimensionality fixed by embedding_model
embedding_model             string       -- critical during a migration
embedded_at                  timestamp

source_path                  string    -- carried forward, avoids a join for citation
file_name                     string
content_hash                  string    -- traceability back to silver/bronze
section_heading                string    -- nullable

resolved_acl_group             string   -- THE query-time enforcement column;
                                              filtered against the requester's
                                              group membership as a pre-filter
                                              on search, not a post-hoc check
chunking_method_used            string    -- carried forward, useful for debugging
extraction_method_used          string
```

**Key rationale:**

- **Only truly active chunks belong in gold — no soft-delete flag here.**
  When a chunk becomes orphaned (per the silver invalidation rule), it must be
  a genuine `DELETE` against this table, not a flag. **Databricks Vector
  Search's Delta Sync Index automatically propagates deletes from its source
  Delta table into the live index** — this is the actual, native mechanism
  that solves "vector database cleanup," widely regarded as one of the
  hardest parts of running a RAG system. A lingering soft-deleted row would
  still be a live, searchable vector in the index — the exact problem this
  design is meant to prevent.
- **Vector index config is thin by design**: only `chunk_id` (primary key)
  and `embedding_vector` (vector source) need declaring; every other gold
  column becomes automatically available as filterable/returnable metadata,
  `resolved_acl_group` included. The governance work is already done by the
  time gold exists; the index just serves it.
- **Index scoping is per-team, not a preference — it's forced.** A vector
  index has one fixed dimensionality tied to one embedding model. Once a team
  can choose its own model, per-team indexing is the necessary consequence,
  not a separate design choice.
- **Embedding-model migration = blue-green, not dual-write.** A single table
  can't hold two models' vectors for one `chunk_id` under one primary key.
  Migration means standing up a second gold table + index, re-running
  embedding only (silver's chunk content is reused untouched — zero
  re-extraction, zero re-chunking), validating via the MLflow eval harness,
  then cutting the application over and decommissioning the old index.
  Real complexity here is cost (running two indexes during validation) and
  operational discipline (a real production observation window before
  decommissioning), not new algorithmic logic.

---

## Retrieval and Serving

**Naming note:** Databricks Vector Search has been renamed **AI Search** in
current documentation — same product, referenced as AI Search from here on.

### Two layers of access control — only one is automatic

- **Coarse-grained (automatic):** the index itself is a Unity Catalog object.
  UC grants determine whether a calling principal can query the index at all
  — inherited for free, same as everything else in gold.
- **Fine-grained (NOT automatic):** whether a specific *row* within an index a
  principal can query is one they're actually allowed to see — the
  `resolved_acl_group` filter designed into gold. This must be explicitly
  applied at query time; it does not happen just because the column exists.

### Enforcement mechanism — locked decision

**On-behalf-of-user (OBO) execution is the default**, for any request with a
real, signed-in end-user identity. The query runs as that actual user, so
Unity Catalog checks their real, centrally-managed permissions directly —
the same source of truth used everywhere else in this platform. This avoids
a second, informal copy of access logic living inside application code that
can silently drift out of sync with the official UC record over time.

**A server-side-verified explicit filter** (`filters='resolved_acl_group IN
(...)'`) is the fallback, used only where no real end-user identity exists in
the request context — a scheduled batch job, an automated report with nobody
"logged in." In this case, the filter value must come from a trusted,
server-verified lookup of what that specific process is authorized to touch
— **never accepted as a value the caller supplies directly.** This is the
same info-vs-authority distinction underlying every governance decision
elsewhere in this design: a request should never be trusted to state its own
authorization.

### A concrete operational risk, worth an explicit guard

**The query-time embedding must be generated by the exact same model recorded
in gold's `embedding_model` column.** A mismatch doesn't error — it silently
returns poor results, since vectors are being compared in incorrect,
mismatched spaces. The serving layer should read `embedding_model` from the
target index's own metadata and refuse to proceed if the configured
query-embedding model doesn't match, rather than trusting convention to keep
them aligned.

### Retrieval quality — dense vs. sparse vs. hybrid

**Hybrid search is the platform default, not pure dense.** Dense (embedding
similarity) is what everything above assumes, and it's genuinely good at
matching meaning across different wording. But it has a real, known failure
mode for exactly the kind of content this platform holds: a specific policy
ID, a regulation number, an acronym. Semantic similarity can miss an exact
match on something like "POL-2024-117," since embedding space clusters by
meaning, not precise tokens, and two distinct IDs can sit close together in
that space. Sparse (keyword-based, e.g. BM25) catches exact-term matches
dense retrieval can blur past. AI Search supports hybrid natively via
`query_type`, combining both in one call.

Same two-tier pattern as extraction and chunking: hybrid is the default;
a team can override toward pure dense for content where exact terms
genuinely don't matter (general narrative content).

### `top_k` — two distinct values, easy to conflate

- **Retrieval `top_k`** — how many candidates AI Search pulls initially.
  Wider and cheaper per item (e.g. 20).
- **Final `top_k`** — how many of those actually reach the prompt after
  reranking. Narrower and more expensive per item, since each one costs real
  context tokens (e.g. 5).

A wide-retrieve, narrow-rerank shape is the concrete retrieval-layer version
of a principle already established elsewhere in this design: more retrieved
context is not automatically better — irrelevant candidates compete with
relevant ones for the model's attention rather than just failing to help.

### Reranking

Initial similarity search is fast but comparatively crude, optimized for
speed across a large index. A reranker is a smaller, separate model that
re-scores only the already-narrowed candidate set directly against the
query — more expensive per item, but run against a handful of candidates,
not the whole index. AI Search's built-in reranking is the default rather
than something built in-house.

### Query rewriting

Raw user input is often a poor search query on its own — vague references to
earlier conversation turns, typos, or two questions bundled into one. Query
rewriting is a small LLM call that transforms raw input into a better-formed
search query **before embedding happens** — a real step with real added
latency and cost, not a simplification to skip.

### Retrieval config — keyed by endpoint, not by path or file type

Extraction, chunking, and embedding are all keyed by path-prefix or file
type — properties of the document being ingested. Retrieval has no document
at query time, only a request hitting a serving endpoint, so that key
doesn't apply here. What actually varies is the **use case consuming
retrieval**, not the source content — so overrides are keyed by
`endpoint_name` instead.

```yaml
retrieval:
  defaults:
    query_type: hybrid
    retrieval_top_k: 20
    final_top_k: 5
    rerank: true
    query_rewrite: true
  overrides:
    - endpoint_name: "quick-lookup-bot"
      final_top_k: 3
      query_rewrite: false
    - endpoint_name: "deep-research-tool"
      retrieval_top_k: 50
      final_top_k: 10
```

**One index can back more than one serving endpoint.** A team's gold index
doesn't have to map 1:1 to a single application — a fast, narrow-`top_k`
endpoint for a latency-sensitive chat bot and a wider, higher-rerank endpoint
for a research tool can both query the same underlying vectors, since the
index itself doesn't change, only how a given application chooses to search
it. Same two-tier resolution logic as everywhere else. Mosaic AI Model
Serving endpoints are themselves a definable resource inside a Declarative
Automation Bundle, alongside the pipeline, dashboard, and index — one more
config block in the same bundle a team already deploys, not a new,
separately-managed artifact.


1. Receive query + calling identity (real user via OBO, or service principal
   for the fallback case).
2. **Query rewrite** — transform raw input into an optimized search query.
3. Embed the rewritten query using the model matching the target index —
   verified per the guard above, not assumed.
4. **Hybrid search** (default), wide retrieval `top_k` (e.g. 20), with the
   OBO or verified-filter access control applied.
5. **Rerank**, trim to final `top_k` (e.g. 5).
6. Return `chunk_text` plus the citation fields deliberately kept in gold —
   `source_path`, `file_name`, `section_heading` — so every answer can cite
   exactly where it came from.

Every knob above — hybrid vs. dense, both `top_k` values, rerank on/off,
query rewriting on/off — is team-configurable with a platform default, the
same two-tier shape used throughout this design, since retrieval quality
genuinely varies by domain the same way extraction and chunking do.

### Where this runs

A **Mosaic AI Model Serving** endpoint wrapping query-embedding, the AI
Search call, reranking, and final generation as one deployed unit — not
logic scattered across an external application. Instrumented with **MLflow
tracing** on every call, which is also what gives the still-undesigned
evaluation harness something real to run against.

---

## Locked so far — end to end

Bronze → Silver → Gold → Retrieval/Serving, from a PDF landing in ADLS Gen2
to a governed vector, to an access-controlled, cited answer — with every
real decision's reasoning captured above.

## Evaluation and Monitoring

### Golden dataset

```
question               string
expected_answer          string    -- optional; unlocks context_sufficiency and
                                          correctness judges when present
expected_chunk_ids        array<string>   -- optional, for retrieval-specific grading
category                  string
```

**Built via MLflow's Review App**, not invented from scratch. Domain experts
review real production traces (already captured via serving-layer tracing)
and provide feedback — approve, correct, flag. That feedback becomes a
*candidate* golden-set entry, never auto-merged: a required human approval
step promotes it into the actual enforced set. This matters because the
golden dataset is the thing blocking production deploys, not just a
reference — a single mistaken piece of reviewer feedback flowing straight
through, unreviewed, could wrongly block a good deploy or wave through a bad
one. Delta-backed and versioned, same as everything else in this platform,
for a real audit history of what "golden" meant at any point in time.

### Scorers

Current MLflow behavior: **without** `expected_answer`, available built-in
judges are `chunk_relevance`, `groundedness`, `relevance_to_query`, `safety`,
`guideline_adherence`. **With** `expected_answer` present, two more become
available: `context_sufficiency` and `correctness` — a concrete, real reason
to push teams toward including expected answers wherever possible.

```yaml
evaluation:
  golden_dataset: "catalog.schema.golden_eval_set"
  scorers:
    default: [chunk_relevance, groundedness, relevance_to_query, safety, guideline_adherence]
    with_ground_truth: [context_sufficiency, correctness]
    custom: []   # team-registered custom LLM judges or code-based scorers
  thresholds:
    correctness: 0.85
    groundedness: 0.90
    safety: 1.0
```

Same two-tier default/override shape as everywhere else: platform-provided
default judge set, team can register a custom judge for domain-specific
checks (e.g. verifying a policy-ID citation format).

### The gate — one mechanism, reused everywhere

`mlflow.genai.evaluate()` runs against the golden dataset as a CI/CD step
before any `--target prod` deploy is allowed. Every scorer's aggregate score
is checked against team-configured thresholds; any miss blocks the deploy.
This is **one gate, not several** — reused identically for initial
onboarding, extraction/chunking config changes, embedding-model migrations,
and retrieval config changes. Anything that could affect answer quality goes
through it.

### Production monitoring

**Must be asynchronous, never inline with a live request.** Each judge call
is itself a real LLM invocation; stacking several onto every live response
would make users wait on judging before seeing their answer. Monitoring is a
separate, scheduled batch job reading already-logged traces after the fact —
decoupled entirely from what the user experiences. This is the concrete
mechanism satisfying "drift detection on embeddings and retrieval quality
over time" from the original reference architecture: the same judges used
for the pre-deployment gate, reused on a schedule against sampled production
traffic — no separate monitoring system to build.

**Sampling strategy, not 100% coverage** — judging every request scales
judge-call cost directly with traffic volume:
- **Stratified baseline sampling** (e.g. 5–10%) across categories and ACL
  groups, so monitoring doesn't only ever see the most common query type and
  miss a problem in a rarer, high-stakes category.
- **Always-evaluate triggers**, bypassing sampling entirely: a user
  thumbs-down, a flagged response, an unusually long or short generation (a
  cheap proxy for something having gone wrong).
- Sample rate is a direct, real cost lever — tighter production visibility is
  an explicit tradeoff against ongoing judge-call spend, connecting directly
  to the still-open cost-governance item.

**Differentiated response to a threshold breach, since production traffic
already happened and can't be "blocked" the way a deploy can:**
- **Safety breaches** — near-immediate alerting, reusing the same SQL-Alert
  mechanism from bronze, at effectively zero tolerance.
- **Correctness/groundedness drift** — a trend signal for a dashboard, not an
  immediate page. A single dip may be noise; only a sustained trend is
  actionable.

**Dashboard** — same zero-effort, platform-owned pattern as bronze and
silver: score trends per judge over time, broken down by category and by
which serving endpoint (tying back to the endpoint-keyed retrieval config)
the trace came from.

---

## Locked so far — end to end

Bronze → Silver → Gold → Retrieval/Serving → Evaluation/Monitoring, from a
PDF landing in ADLS Gen2 to a governed vector, to an access-controlled,
cited answer, to a continuously monitored quality signal — with every real
decision's reasoning captured above.

## Cost Governance and Chargeback

### Attribution mechanism — automatic, not team-configured

**Naming note:** the current mechanism is `usage_policy_id`, which replaced
the now-deprecated `budget_policy_id` — same kind of naming shift as AI
Search and Declarative Automation Bundles noted elsewhere in this doc.

Every serverless resource a team's bundle deploys — Lakeflow pipeline, SQL
warehouse, AI Search index, Model Serving endpoint — is tagged with the
team's `usage_policy_id` **as part of the shared bundle template itself**,
the same way ACL bindings and checkpoint paths already are. Left as a
team-remembered convention instead, tagging would degrade the way any
unenforced convention does — inconsistently, and unreliable exactly when it
matters most.

Source of truth: Databricks' built-in `system.billing.usage` system table —
DBU consumption already captured with tags, workspace, and resource
identifiers, queryable directly. No custom cost-tracking system to build;
the chargeback dashboard is a Lakeview dashboard querying this table grouped
by `usage_policy_id`, same zero-effort pattern as every other dashboard in
this design.

### Cost visibility at the point of decision, not just after the fact

A real design principle worth stating explicitly: cost governance done well
makes the cost consequence visible at the exact point a team makes the
choice that causes it, not only in a retrospective report. Every layer
locked above has at least one knob with a real, attributable cost shape:

- **Bronze** — unconditional full-content capture for every file was a
  deliberate tradeoff (cheap now, avoids expensive re-fetching later);
  surfaced as raw storage volume per team on the dashboard, so the tradeoff
  stays visible.
- **Silver** — OCR costs meaningfully more than standard extraction;
  semantic chunking (itself an embedding-model call) costs more than
  fixed-size. A team's path-prefix overrides have direct, attributable cost
  — one more reason those choices are explicit config, not a black box.
- **Gold** — embedding generation is the single biggest line-item risk,
  especially during a migration backfill; the blue-green re-embedding
  process should have cost estimated *before* a team commits, not discovered
  after. Index-serving DBU cost is charged by the hour an endpoint runs
  regardless of query volume — making "one index, many endpoints" a direct
  cost optimization, not just an architectural convenience.
- **Retrieval** — query rewriting and reranking are small per-request LLM
  costs, negligible individually, multiplied at production volume; shown
  per-endpoint, tied to the same `endpoint_name` key retrieval config uses.
- **Evaluation** — production monitoring's sample rate is the same
  quality-visibility lever from the previous section, seen from the cost
  side rather than a new fact.

---
