# Database Selection Implementation Plan

## Goal

Add a `source_database` field to tables in the metadata registry so the compiler pipeline
can scope the schema it sends to the LLM to only the tables belonging to a single logical
database. For the BIRD benchmark, each of the 11 logical databases (financial, formula_1,
etc.) is entirely self-contained; sending 75 tables to the LLM when a question is only
about 8 is wasteful and degrades routing accuracy.

The implementation has two modes:

- **Explicit mode** — caller passes `source_database` in the request; the filter pre-restricts
  to that database before any token matching occurs.
- **Auto-detect mode** — caller omits `source_database`; the filter scores all tables,
  aggregates per database, and selects the winner if the signal is unambiguous. If ambiguous,
  the response surfaces candidate databases and the caller can retry with the explicit field.

---

## Data flow (for reference)

```
MetadataTable (ORM / PostgreSQL)
  → MetadataCompiler.compile() → JSON blob in CompiledRegistryArtifact
    → RegistryLoader.load_active_schema() → RegistrySchema (in-memory)
      → DeterministicSchemaFilter.filter_schema(intent, schema) → FilteredSchema
        → PromptBuilder → PromptEnvelope → LLM → SQL → Execution
```

`source_database` must travel this entire path to be usable at filter time.

---

## Step 1 — Alembic migration: add `source_database` to `metadata_tables`

**File:** `backend_migrations/versions/<new_hash>_add_source_database_to_tables.py`

Add a single nullable `TEXT` column to `aegis_meta.metadata_tables`:

```python
op.add_column(
    "metadata_tables",
    sa.Column("source_database", sa.Text(), nullable=True),
    schema="aegis_meta",
)
op.create_index(
    "ix_metadata_tables_source_database",
    "metadata_tables",
    ["source_database"],
    schema="aegis_meta",
)
```

Nullable so existing rows and non-BIRD schemas require no backfill to stay valid.
The index is for future queries filtering tables by source database, not for this
implementation's correctness — but cheap to add now.

**Downgrade:** `op.drop_index` + `op.drop_column`.

---

## Step 2 — ORM model: add `source_database` to `MetadataTable`

**File:** `app/api/meta_models.py`

Add to the `MetadataTable` mapped class, after the `active` column:

```python
source_database: Mapped[str | None] = mapped_column(Text, nullable=True)
```

No constraint, no default. `None` means "not assigned" and is valid for any non-BIRD
or general-purpose schema.

---

## Step 3 — Compiled artifact: include `source_database` in the JSON blob

**File:** `app/api/compiler.py`

In the section that builds `tbl_dict` from a `MetadataTable` row (currently around line 83),
add `source_database` to the dictionary:

```python
tbl_dict: dict[str, Any] = {
    ...
    "source_database": tbl.source_database,   # new
}
```

This embeds the field into the signed artifact blob. Existing compiled artifacts will
not have this key; the loader (Step 4) must treat its absence as `None`.

**Important:** any active compiled artifact created before this change is deployed must
be recompiled via `POST /api/v1/metadata/compile/{version_id}` to pick up the new field.
The deploy checklist should include this step.

---

## Step 4 — Registry models: add `source_database` to `AbstractTableDef`

**File:** `app/steward/models.py`

```python
class AbstractTableDef(BaseModel):
    alias: str
    description: str
    columns: list[AbstractColumnDef]
    physical_target: str
    source_database: str | None = None   # new
```

Default `None` preserves backward compatibility — existing code that constructs
`AbstractTableDef` without this field continues to work.

---

## Step 5 — Registry loader: propagate `source_database` when hydrating the blob

**File:** `app/steward/loader.py`

In the loop that calls `AbstractTableDef(...)` (currently around line 140–147), pass the
new field:

```python
tables_def.append(
    AbstractTableDef(
        alias=tbl_dict["alias"],
        description=tbl_dict.get("description", ""),
        physical_target=tbl_dict["name"],
        columns=columns_def,
        source_database=tbl_dict.get("source_database"),   # new
    )
)
```

`.get("source_database")` returns `None` for old artifacts without the key — safe.

---

## Step 6 — Population script: write `source_database` for all BIRD tables

**File:** `scripts/populate_source_databases.py`

A standalone script (similar in structure to `populate_descriptions.py`) that connects
to the database via `DB_URL_STEWARD`, finds the latest `MetadataVersion`, and bulk-updates
`source_database` on `metadata_tables` rows whose `real_name` appears in the known mapping.

The mapping is declared as a module-level constant in the script:

```python
SOURCE_DATABASE_MAP: dict[str, str] = {
    "customers": "debit_card_specializing",
    "gasstations": "debit_card_specializing",
    # ... all 75 tables
}
```

Logic:
1. Load all `MetadataTable` rows for the latest version.
2. For each row, look up `real_name` in the map.
3. Set `source_database` if found; leave `None` if not (unknown table).
4. Commit in one transaction, log counts of updated vs skipped rows.

This script is idempotent — re-running it overwrites with the same values.

**Also update `docker-compose.bird.yml`:** add `populate_source_databases.py` to the
`discover` service command so it runs automatically after `populate_descriptions.py`
in the BIRD boot sequence:

```yaml
command:
  - /bin/sh
  - -c
  - |
    uv run python scripts/discover_metadata.py &&
    uv run python scripts/populate_descriptions.py &&
    uv run python scripts/populate_source_databases.py
```

---

## Step 7 — API surface: add `source_database` to `QueryRequest` and `UserIntent`

**File:** `app/api/models.py` — `QueryRequest`:

```python
source_database: str | None = Field(
    default=None,
    description=(
        "Optional logical database name to restrict schema filtering. "
        "If omitted, the pipeline attempts to auto-detect the relevant database. "
        "Pass explicitly for benchmarks or when the target database is known."
    ),
)

@field_validator("source_database", mode="before")
@classmethod
def _normalise_source_database(cls, v: object) -> object:
    if isinstance(v, str):
        return v.strip().lower()
    return v
```

The normaliser runs before Pydantic type-coerces the value, so `"Financial"`, `" financial "`,
and `"FINANCIAL"` all become `"financial"`. The population script already writes lowercase
keys, so this makes the match deterministic regardless of how the caller capitalises the field.

**File:** `app/compiler/models.py` — `UserIntent`:

```python
class UserIntent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    natural_language_query: str
    source_database: str | None = None   # new
```

`UserIntent` does not need a normaliser — it receives values from `QueryRequest`, which has
already been normalised by Pydantic. The constraint lives at the API boundary, not inside
the compiler.

---

## Step 8 — Router wiring: pass `source_database` from request to `UserIntent`

**File:** `app/api/router.py`

In both the `/generate` and `/execute` handlers, where `UserIntent` is constructed from
the request payload, add the field:

```python
intent = UserIntent(
    natural_language_query=payload.intent,
    source_database=payload.source_database,   # new
)
```

No other router changes are needed at this step.

---

## Step 9 — Filter: explicit pre-filter by `source_database`

**File:** `app/compiler/filter.py`

Add a private method that restricts the table pool before any token scoring:

```python
@staticmethod
def _apply_database_scope(
    schema: RegistrySchema,
    source_database: str,
) -> list[AbstractTableDef]:
    """Returns only tables belonging to the specified database."""
    return [t for t in schema.tables if t.source_database == source_database]
```

In `filter_schema()`, before computing `intent_tokens`, check if a database is
explicitly provided and narrow the table list. **Fail fast if the value is unknown:**

```python
def filter_schema(
    self,
    intent: UserIntent,
    schema: RegistrySchema,
    included_columns: RAGIncludedColumns | None = None,
) -> FilteredSchema:
    # --- Step 9: explicit database scope ---
    if intent.source_database:
        candidate_tables = self._apply_database_scope(
            schema, intent.source_database
        )
        if not candidate_tables:
            raise UnknownSourceDatabaseError(intent.source_database)
        scoped_schema = RegistrySchema(
            version=schema.version,
            tables=candidate_tables,
            relationships=schema.relationships,
        )
    else:
        scoped_schema = schema

    # rest of the method operates on scoped_schema instead of schema
    intent_tokens = self._tokenize(intent.natural_language_query)
    ...
```

`UnknownSourceDatabaseError` is a new domain exception defined in
`app/compiler/exceptions.py`. It propagates up to the FastAPI exception handler
registered in Step 12, which returns a `JSONResponse(status_code=400, ...)` directly.

**Why fail fast:** paying a commercial LLM to generate SQL without any schema is
wasteful and will produce garbage. An empty table list is always a caller error
(typo, stale value) — never a valid input. The `.lower().strip()` normaliser in Step 7
eliminates most innocent capitalisation mistakes before the exception can fire.

At this step, auto-detection is not yet implemented — if `source_database` is `None`,
the filter runs exactly as before.

**Verify:** run the existing test suite. All tests pass because they construct `UserIntent`
without `source_database`, which defaults to `None`, hitting the unchanged code path.

---

## ~~Step 10~~ — ~~Filter: enforce strict relationship expansion within the same database~~ (DROPPED)

**Rationale for removal:** In BIRD, logical databases are physically disjoint — no
cross-database foreign keys exist, so the standard expansion algorithm will never cross
boundaries organically anyway. In a real enterprise registry, a cross-database foreign
key edge represents a deliberately modelled join path (e.g., `orders` in `DB_A` joining
`users` in `DB_B`). Severing that edge artificially would break legitimate queries that
the schema author explicitly intended.

`_augment_with_relationships()` is not modified. The existing logic is correct as-is:
it follows edges that exist in `RegistrySchema.relationships` and no spurious edges
will exist between BIRD databases because none were discovered.

---

## Step 11 — Filter: auto-detection algorithm

**File:** `app/compiler/filter.py`

Add a method that scores all tables, groups by `source_database`, and returns the winning
database name or raises on ambiguity.

### Why MAX not SUM

`token_match_score` is a ratio bounded in [0, 1] (Jaccard-like). Summing across all
tables in a database gives a score proportional to the number of tables, not the quality
of the best match. A 100-table database with 50 weak 0.1 hits scores 5.0; a 2-table
database with a perfect 1.0 hit scores 1.0 — the massive database wins despite being
the wrong answer. Instead, use the **MAX** per-table score as each database's
representative score. A single strong signal in a small database beats many weak signals
in a large one.

```python
def _detect_source_database(
    self,
    schema: RegistrySchema,
    intent_tokens: frozenset[str],
) -> tuple[str | None, list[str]]:
    """
    Returns (winner, candidates).

    winner     — the database name if detection is unambiguous, else None.
    candidates — all databases with score > 0, sorted by descending score.
                 Populated even when winner is None (used to populate 400 response).

    Algorithm:
    - For each table, compute a per-table score = max(
          token_match_score(intent_tokens, table_tokens),
          max(token_match_score(intent_tokens, col_tokens) for col in table.columns)
      )  — the best single signal from that table.
      _tokenize() is @lru_cache-wrapped, so each unique alias/description string
      is split into tokens exactly once per process lifetime (see token caching below).
    - For each database, db_score = MAX per-table score across all its tables.
      (MAX prevents large databases from winning by volume of weak matches.)
    - Winner requires: db_score > 0  AND
      db_score >= 2 × second_place_score  (or second_place is 0).
    - If all scores are zero → no candidates, return (None, []).
    - If tied → return (None, sorted candidates). Caller must raise 400.
    """
    db_scores: dict[str, float] = {}
    for table in schema.tables:
        db = table.source_database
        if db is None:
            continue
        table_tokens = self._tokenize(table.alias) | self._tokenize(table.description)
        table_score = float(self._token_match_score(intent_tokens, table_tokens))
        col_scores = [
            float(self._token_match_score(
                intent_tokens,
                self._tokenize(col.alias) | self._tokenize(col.description),
            ))
            for col in table.columns
        ]
        best_for_table = max([table_score] + col_scores)
        db_scores[db] = max(db_scores.get(db, 0.0), best_for_table)

    if not db_scores or max(db_scores.values()) == 0.0:
        return None, []

    ranked = sorted(db_scores.items(), key=lambda x: x[1], reverse=True)
    best_name, best_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0
    candidates = [name for name, score in ranked if score > 0]

    if best_score >= 2.0 * second_score or second_score == 0.0:
        return best_name, candidates
    return None, candidates
```

### Token caching: `@staticmethod` + `@lru_cache` on `_tokenize`

Re-tokenizing a static alias like `"race_results"` on every query call is O(N columns)
work on static data. With 500 BIRD columns this is ~1–3 ms; with 100,000 enterprise
columns it blocks the asyncio event loop entirely. **No model changes are needed.**

The fix is to convert `_tokenize` in `filter.py` from a plain instance method to a
`@staticmethod` decorated with `@functools.lru_cache`:

```python
import functools

@staticmethod
@functools.lru_cache(maxsize=8192)
def _tokenize(text: str) -> frozenset[str]:
    # existing implementation unchanged
    ...
```

**Why `@staticmethod` is required:** `@lru_cache` on an instance method (`self._tokenize`)
includes `self` in the cache key, making the cache per-instance. If `DeterministicSchemaFilter`
is ever reinstantiated (e.g., in tests), the cache is discarded and the next call re-tokenizes
from scratch. A `@staticmethod` has no `self` — the cache is truly global (per-process),
so every unique string is tokenized exactly once across all calls and all instances. This is
the correct semantics for a pure function operating on static schema data.

No changes to `app/steward/models.py` or `app/steward/loader.py` are required. Steps 4 and 5
only add `source_database` — they do not add token fields.

**Integrate into `filter_schema()`:** when `intent.source_database` is `None`, call
`_detect_source_database()`. If a winner is found, proceed with the scoped schema. If
detection returns `(None, candidates)` with a non-empty candidate list, raise
`AmbiguousSourceDatabaseError(candidates)` — **do not fall back to the full schema**.
If detection returns `(None, [])` (no signal at all), proceed with the full schema
unchanged (the query has no database-specific tokens and the normal table-scoring logic
will handle it).

The router catches `AmbiguousSourceDatabaseError` and returns:

```
HTTP 400 Bad Request
{
  "detail": "Query matches multiple databases; retry with source_database set to one of the candidates.",
  "candidate_databases": ["financial", "formula_1"]
}
```

**Why hard-fail on ambiguity:** silently falling back to 75 tables defeats the
purpose of the feature. The LLM will receive a massive confusing schema, hallucinate
SQL, and return HTTP 200 with garbage. The caller has no way to distinguish a good
result from a degraded one unless they manually inspect `candidate_databases` metadata
in every response. Failing loudly forces the caller to provide intent, which produces
a better answer on retry.

---

## Step 12 — Response: surface `source_database_used` on success; `candidate_databases` on error

**Success path** — the caller needs to know which database was selected (useful for
the benchmark harness and debugging). Add `source_database_used` to the success
response only.

**File:** `app/compiler/models.py` — add to `FilteredSchema`:

```python
class FilteredSchema(BaseModel):
    version: str
    tables: list[AbstractTableDef]
    relationships: list[AbstractRelationshipDef]
    omitted_columns: dict[str, str]
    source_database_used: str | None = None   # new — None means no scope was applied
```

**File:** `app/api/models.py` — add to `QueryGenerateResponse` and `QueryExecuteResponse`:

```python
source_database_used: str | None = Field(
    default=None,
    description="The logical database the schema was scoped to for this query.",
)
```

**Error path** — ambiguity and unknown-database errors surface `candidate_databases`
in the HTTP 400 response body (not in the success payload). Define two domain exceptions:

```python
# app/compiler/exceptions.py
class UnknownSourceDatabaseError(Exception):
    def __init__(self, name: str) -> None:
        self.name = name

class AmbiguousSourceDatabaseError(Exception):
    def __init__(self, candidates: list[str]) -> None:
        self.candidates = candidates
```

**File:** `app/main.py` — install exception handlers on the FastAPI `app` instance:

```python
from fastapi.responses import JSONResponse

@app.exception_handler(UnknownSourceDatabaseError)
async def unknown_db_handler(
    request: Request, exc: UnknownSourceDatabaseError
) -> JSONResponse:
    return JSONResponse(
        status_code=400,
        content={"detail": f"Unknown source_database: '{exc.name}'"},
    )

@app.exception_handler(AmbiguousSourceDatabaseError)
async def ambiguous_db_handler(
    request: Request, exc: AmbiguousSourceDatabaseError
) -> JSONResponse:
    return JSONResponse(
        status_code=400,
        content={
            "detail": (
                "Query matches multiple databases; retry with "
                "source_database set to one of the candidates."
            ),
            "candidate_databases": exc.candidates,
        },
    )
```

**Why `app/main.py`, not `app/api/router.py`:** `@app.exception_handler` must be called
on the FastAPI `app` instance (created in `main.py` via `app = FastAPI(...)`). The router
module uses an `APIRouter`, which does not support exception handler registration.
Handlers registered on an `APIRouter` are silently ignored by Starlette's exception
middleware.

**Why `return JSONResponse` and not `raise HTTPException`:** raising `HTTPException`
inside a Starlette exception handler is an anti-pattern. Depending on the middleware
stack, the second exception is not caught by the same handler and can propagate as a
raw 500. Exception handlers must always return a `Response` object directly.

Audit logging should record `source_database_used` alongside `tenant_id` on the
success path.

---

## Step 12a — Query Console UI: handle structured 400 errors

**File:** `static/app.js`

The Query Console currently expects `error.detail` to be a flat string. The
`AmbiguousSourceDatabaseError` response produces a structured body:

```json
{
  "detail": "Query matches multiple databases...",
  "candidate_databases": ["financial", "formula_1"]
}
```

If this lands unhandled, the UI will display `[object Object]` or crash silently —
the user has no way to act on it.

**Context:** The console exposes three user-facing fields: Provider, Model, and Natural
Language Intent. There is no `source_database` input and none should be added. When
the user clicks a candidate pill, `source_database` is injected programmatically into
the JSON payload of the re-submitted fetch — the user sees a seamless retry with no
visible form change.

**Change:** read `app.js` before implementing. Locate the existing fetch call that
posts to `/api/v1/query/execute` (or `/generate`) and the existing error rendering
function. Apply the following changes:

```javascript
// Pseudocode — adapt to the actual structure found in app.js

// 1. Track any pending source_database override at module scope.
let _pendingSourceDatabase = null;

// 2. In the fetch call, include source_database in the payload if set:
const payload = {
    intent: intentInput.value,
    provider_id: providerSelect.value,
    // ... other existing fields
    ...((_pendingSourceDatabase !== null) && { source_database: _pendingSourceDatabase }),
};
// Reset after use so the next fresh query does not carry it forward.
_pendingSourceDatabase = null;

// 3. In the error handler, branch on structured 400 responses:
function renderError(responseBody, statusCode) {
    if (statusCode === 400 && Array.isArray(responseBody.candidate_databases)) {
        const pills = responseBody.candidate_databases
            .map(db => `<button class="db-pill" data-db="${db}">${db}</button>`)
            .join(" ");
        showError(`${responseBody.detail}<br>Choose a database: ${pills}`);
        document.querySelectorAll(".db-pill").forEach(btn => {
            btn.addEventListener("click", () => {
                _pendingSourceDatabase = btn.dataset.db;
                submitQuery();  // re-invoke the existing submit function
            });
        });
    } else {
        // Existing path — detail is always a string here.
        showError(typeof responseBody.detail === "string"
            ? responseBody.detail
            : JSON.stringify(responseBody.detail));
    }
}
```

The key invariants are:

1. No new DOM input field is added. `source_database` is invisible to the user.
2. `detail` is always rendered as a string (never coerced to `[object Object]`).
3. After a pill click re-submits, `_pendingSourceDatabase` is cleared so the next
   independent query does not accidentally carry the previous database scope.
4. The existing submit function is reused — no duplicate fetch logic.

---

## Step 13 — Tests

Add tests across three files:

### `tests/test_schema_filter_database.py` (new file)

| Test | Description |
|---|---|
| `test_explicit_scope_restricts_tables` | When `source_database="financial"` is set, only financial tables appear in `FilteredSchema`. |
| `test_explicit_scope_unknown_database_raises_400` | When `source_database="nonexistent"` is set, `UnknownSourceDatabaseError` is raised immediately — the PromptBuilder is never called. |
| `test_autodetect_clear_winner` | Query "list all races in the 2009 season" → auto-detects `formula_1` unambiguously. |
| `test_autodetect_large_db_does_not_win_by_volume` | A large database with 50 weak (0.1) column hits loses to a small database with one strong (1.0) hit, confirming MAX not SUM. |
| `test_autodetect_ambiguous_raises_400` | Query generic enough to match two databases raises `AmbiguousSourceDatabaseError` with both names — does NOT fall back to full schema. |
| `test_autodetect_no_signal_uses_full_schema` | Query with zero token overlap on any `source_database`-tagged table proceeds with the full schema unchanged. |
| `test_source_database_none_unchanged_behavior` | `source_database=None` with no BIRD tables in the schema behaves exactly as before (no regression). |

### Updates to `tests/test_compiler_engine.py`

Add a case where `UserIntent.source_database` is set and verify the compiled SQL only
references tables from that database.

### Updates to `tests/test_prompting_gateway.py`

Verify the `PromptEnvelope.system_instruction` only contains table aliases from the
scoped database when `source_database` is set.

---

## Step 14 — BIRD benchmark evaluation harness

**File:** `scripts/run_bird_benchmark.py`

A standalone script that drives the Aegis API against the BIRD mini-dev question set and
computes execution accuracy (EX): the fraction of questions where Aegis returns a result
set that exactly matches the gold SQL result set.

### Inputs

- `--questions` — path to `mini_dev_postgresql.json` (BIRD mini-dev question file; each
  entry has `question`, `db_id`, `SQL`, `evidence`)
- `--base-url` — Aegis API base URL (default `http://localhost:8000`)
- `--api-key` — Bearer token for authentication
- `--provider-id` — LLM provider to use (e.g. `anthropic:claude-sonnet-4-6`)
- `--db-url` — direct PostgreSQL URL to run gold SQL for comparison
- `--output` — path to write results JSON (default `bird_results.json`)
- `--limit` — stop after N questions (optional, for quick smoke tests)

### Per-question logic

**Critical:** each question must use a **fresh session**. Do not pass a `session_id`
between questions. If question N is about "financial" and question N+1 is about
"formula_1", a shared session would replay the cached `FilteredSchema` from question N
and scope question N+1 to the wrong database, producing subtly wrong SQL with no error.
The benchmark harness must omit `session_id` on every request (or generate a new UUID
per question) so that each question compiles against a clean, freshly scoped schema.

```
1. POST /api/v1/query/execute with:
   intent           = question["question"]
   source_database  = question["db_id"]
   provider_id      = args.provider_id
   session_id       = <new UUID per question — never reused>

2. If response is an error (4xx/5xx):
   record result = "error", reason = response body

3. Run question["SQL"] directly against PostgreSQL:
   rows_gold = execute_direct(question["SQL"])

4. Compare rows_aegis vs rows_gold:
   - Sort both result sets (order-insensitive comparison)
   - exact_match = (rows_aegis == rows_gold)

5. Append to results:
   { db_id, question, gold_sql, aegis_sql, exact_match, error, latency_ms }
```

### Output

Summary printed to stdout:
```
Total questions : 500
Exact match     : 312  (62.4%)
Errors          : 48   (9.6%)
  Safety blocks :  31
  LLM refusals  :  9
  Timeouts      :  8
Skipped         : 0
```

Full per-question results written to `--output` JSON for analysis.

### Why execution accuracy over string matching

Two SQL queries can produce identical results with different syntax. String comparison
would penalise Aegis unfairly for differences in alias names, column ordering, or
equivalent rewrites. Executing both and comparing result sets is the canonical BIRD
evaluation methodology.

---

## Emergent benefit: session continuity handles follow-up queries for free

The `SessionQueryContext` caching implemented in the compiler pipeline stores the
`FilteredSchema` produced by the first query in a session and reuses it for all
subsequent queries in the same session.

This means `source_database` detection is a **one-time cost per conversation**:

1. User asks "Show me the sales by region" with no `source_database` set.
2. Auto-detection selects `financial`, compiles against 8 tables. `FilteredSchema`
   (scoped to `financial`) is cached in the session.
3. User asks "What about the previous year?" — no `source_database` in this request.
4. The compiler retrieves the cached `FilteredSchema` and compiles against the same
   8-table financial scope. Auto-detection does not run again.

No extra API parameters, no repeated detection cost, no risk of a follow-up query
accidentally scoping to a different database. The existing session architecture handles
multi-turn database-scoped conversations natively and without modification.

---

## Deployment notes

1. Run the migration: `uv run alembic upgrade head`
2. Run the population script: `uv run python scripts/populate_source_databases.py`
3. Recompile the active MetadataVersion: `POST /api/v1/metadata/compile/{version_id}`
   This is required because the compiled artifact JSON blob is what the loader reads;
   existing artifacts pre-dating this change will not have `source_database` in them.
4. Restart the application (or wait for hot-reload) so `RegistryLoader` picks up the
   new artifact.

For Docker deployments using the BIRD overlay, steps 2 and 3 are automated via
`docker-compose.bird.yml` (Step 6 above).

---

## What this plan explicitly defers

- **Steward UI changes** — displaying or editing `source_database` in the web console.
  The field is visible via `GET /api/v1/metadata/versions/{id}/schema` already; a UI
  panel can be added independently.
- **Multi-database queries** — queries that genuinely span two logical databases are
  out of scope. The current model selects exactly one database or none.
- **LLM-based database routing** — using the LLM itself as the routing signal. Token
  scoring is deterministic, free, and fast; LLM routing adds latency and cost for
  marginal gain given good table descriptions.
- **Threshold calibration** — the `>= 2×` second-place threshold in `_detect_source_database`
  is a starting point. After the benchmark harness produces data (Step 14), the threshold
  can be tuned empirically against real BIRD questions.
