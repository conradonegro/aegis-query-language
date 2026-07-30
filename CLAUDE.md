# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

**Setup** (uses `uv` for dependency management):
```bash
uv sync
```

**Run the server**:
```bash
uv run uvicorn app.main:app --reload
```

**Linting and type checking**:
```bash
uv run lint-imports   # Enforce architectural module boundaries (import-linter)
uv run ruff check .   # Code style
uv run mypy .         # Static type checking (strict mode)
```

**Tests**:
```bash
uv run pytest -v                          # Full suite
uv run pytest tests/test_compiler_engine.py -v  # Single test file
uv run pytest -k "test_name" -v           # Single test by name
```

Tests run against an in-memory SQLite database automatically (`TESTING=true` is set in `conftest.py`). The `LLM_PROVIDER` is forced to `mock` during tests.

## Architecture

Aegis is a **semantic translation and security proxy** between natural language queries and PostgreSQL. Users submit natural language; Aegis compiles it to parameterized SQL via an LLM, validates it against safety rules, and executes it. Users never write SQL directly.

### Bounded Contexts (strictly enforced by import-linter)

1. **`app.steward`** — Schema and policy definition. Owns `RegistrySchema`, the only cross-context shared object. No other module may invoke the steward's data-loading mechanism directly.

2. **`app.compiler`** — The compilation pipeline. Receives `UserIntent`, filters the schema, builds prompts (without physical DB targets), calls the LLM gateway, parses the response into an AST via `sqlglot`, runs safety validation, and translates to a fully-resolved `ExecutableQuery`. **Never connects to a database.**

3. **`app.execution`** — The only layer permitted to open database connections. Executes raw parameterized SQL against PostgreSQL using `asyncpg`. Appends `SET LOCAL statement_timeout` on every query. No ORM expression builders.

4. **`app.audit`** — Out-of-band telemetry sink. Consumes `QueryAuditEvent` structs and logs them asynchronously. Must never raise exceptions that block API responses.

5. **`app.rag`** — In-memory store for column/value matching, used to inject hints into the compiler pipeline before schema filtering. The class is `InMemoryVectorStore`, but matching is **lexical** — word-boundary substring plus a `difflib` fuzzy fallback, no embeddings. Search is scoped to the resolved `source_database`.

6. **`app.api`** — FastAPI routes and the Steward UI. Routes: `POST /api/v1/query/generate`, `POST /api/v1/query/execute`, and a full metadata CRUD API under `/api/v1/metadata/`. Exposes a web console served from `static/`.

### Compiler Pipeline (`app/compiler/engine.py`)

The `CompilerEngine.compile()` method runs this pipeline in order:
1. Source-database resolution — resolves `source_database` so the RAG value search can be scoped to it. Detection reads only the intent and schema, never RAG output, so there is no circular dependency. Not confidently detected → unscoped search rather than no hints
2. RAG lookup — optionally injects column/value hints
3. Schema filter — scopes the `RegistrySchema` to relevant tables/columns (`DeterministicSchemaFilter`)
4. Prompt builder — builds a `PromptEnvelope` with **no physical schema targets** exposed to the LLM
5. LLM gateway — calls the configured provider; returns abstract SQL
6. AST parser — `sqlglot`-based parser that rejects DDL/DML, multi-statement payloads, and non-whitelisted functions. **CTEs, subqueries and UNION are permitted** — they were blocked in v1, and their resource cost is bounded by `statement_timeout` at execution
7. Safety engine — validates the AST against per-column `SafetyClassification` rules
8. Translator — maps abstract aliases to physical targets, binds literals as `$1`-style parameters

### LLM Providers (`app/compiler/llm_factory.py`)

`get_llm_gateway(provider_id)` resolves providers from the `LLM_PROVIDER` env var or per-request `provider_id`. Format: `ollama`, `ollama:llama3`, `openai:gpt-4o`, `anthropic:claude-3-opus-20240229`, `google:gemini-pro`, `xai:grok-1`. Default is `ollama` pointing to `http://localhost:11434`.

### Secrets / Vault (`app/vault.py`)

`SECRETS_PROVIDER=env` (default, dev-only) reads passwords from env vars. `SECRETS_PROVIDER=vault` uses HashiCorp Vault via AppRole auth (requires `VAULT_ADDR`, `VAULT_APPROLE_ROLE_ID`, `VAULT_APPROLE_SECRET_ID`). The `env` provider is blocked entirely in `ENVIRONMENT=production`.

### Database Connection Roles

The app uses four least-privilege PostgreSQL roles, each with its own connection URL:
- `DB_URL_RUNTIME` — query execution
- `DB_URL_REGISTRY_RUNTIME` — reading compiled registry artifacts / chat history
- `DB_URL_STEWARD` — schema authoring (tables, columns, relationships)
- `DB_URL_REGISTRY_ADMIN` — compiling/promoting metadata versions

### Key Invariants (Non-Negotiable)

**This list is canonical.** `docs/ARCHITECTURE_CLAUDE.md` §14 elaborates on it;
if the two ever disagree, this one wins and the other is stale.

1. `PromptEnvelope` → LLM: **zero physical targets**. No `physical_target` value is ever exposed.
2. `ExecutableQuery` output: **zero abstract aliases**. All resolved to physical targets before execution.
3. `ValidatedAST` is **immutable** — the translator produces a copy and never mutates in place, preserving provenance.
4. All LLM-generated literals become **DB driver parameters** (`:p1`, `:p2`…), never string-interpolated.
5. `app.execution` is the **only** layer that opens database connections.
6. `app.audit.JSONAuditLogger` **never raises** — wrapped in a broad `try/except Exception`.
7. `MetadataAudit` rows are **write-once** — a PostgreSQL trigger raises on UPDATE/DELETE.
8. At most **one** `CompiledRegistryArtifact` with `status = active` **per tenant** at any time.

### Schema Lifecycle

The `app/api/meta_models.py` SQLAlchemy ORM manages `MetadataVersion`, `MetadataTable`, `MetadataColumn`, and `MetadataRelationship`. The `MetadataCompiler` (`app/api/compiler.py`) compiles a draft version into a signed `RegistrySchema` artifact, which `RegistryLoader` (`app/steward/loader.py`) loads at startup and hot-reloads on `POST /api/v1/metadata/compile/{version_id}`.

## Working agreements

Standing instructions from the repository owner. They apply to every session
and override default behaviour.

### 1. Solution quality

**No workarounds or local patches.** A one-off patch is never the default path.
If the right resolution is an architectural decision, stop and raise it rather
than patching around it. The test for "is this a workaround?" is **does it make
the proper fix harder later?** If no, it's a legitimate increment. If yes, stop.

**Never relax a rule to make a problem go away.** No `# noqa`, no
`[[tool.mypy.overrides]]`, no bumping `line-length`, and no loosening a safety
control (the multi-statement parser rejection, JOIN-relationship validation,
the row cap) to turn a failure into a pass. Fix the code, or accept the failure
and say so. C901 → extract helpers; E501 → wrap; `no-untyped-def` → add the
annotation. The only acceptable suppressions are the architectural ones already
in the repo: `B008` for FastAPI `Depends()` defaults, `E402` on `app/main.py`
for `load_dotenv()`, and `type: ignore` on hvac imports (no published stubs)
and dynamic exception attributes.

**Prefer the best general solution.** Choose what is right for the project
long-term over what resolves the immediate case. Prefer the boring, conventional
solution over the clever one, and match the idiom of the surrounding code.

**Reject changes that increase ambiguity**, blur context boundaries, or weaken
maintainability — even when the immediate behaviour is correct. This applies to
your own work as much as to review of someone else's.

### 2. When to stop, and when to just decide

**Stop on ambiguity.** If progress requires an architectural decision — the
architecture is silent or ambiguous and the choice changes behaviour — stop
before coding and wait for an explicit decision. **Always give a recommendation
at the stop point**; a stop without one just moves the work.

**"Analyse" and "propose a fix" mean stop at the proposal.** Don't edit files
until asked with an explicit "implement", "go ahead", or "do it".

**Local judgement is expected**, and this is the counterweight to the two rules
above: internal package structure, helper extraction, naming and library
selection are yours to decide, provided specified behaviour does not change.
Don't stop for these.

### 3. Source of truth

For what the system **should** do, the architecture invariants win — code that
contradicts them is a bug. For what the system **does** do, the code wins — a
document that contradicts it is stale. Never silently pick a side: fix the
loser, and say which it was.

**Verify against the code, not from notes, docs or recollection.** Documents
and memory go stale silently; several claims in this repo were months out of
date before anyone noticed.

### 4. Process

**TDD for all app code.** Write the failing test first and watch it fail.
Commit per logical change with `pytest`, `ruff`, `mypy` and `lint-imports`
green.

**Don't create new Alembic migrations.** Edit
`backend_migrations/versions/0001_initial_schema.py` directly. The database is
dropped and recreated freely, so `0001` is effectively the init script rather
than the first of many. Table DDL belongs there, not in `docker/initdb/*.sql`
(those handle roles, grants and schemas only).

**Execute plans inline in the current session.** Don't offer the
subagent-vs-inline choice — that decision is already made. Still stop at task
boundaries for review, and still stop when genuinely blocked.

### Code-level gotchas worth remembering

- FastAPI exception handlers must `return JSONResponse(...)`, never `raise
  HTTPException` — raising inside a Starlette handler can produce a raw 500
  depending on the middleware stack.
- Exception handlers must be registered on the `app` instance in `main.py`.
  Handlers registered on an `APIRouter` are silently ignored by Starlette.

## Where initiative context lives

Per-initiative state lives in `docs/initiatives/`, one file per initiative —
not in this file and not in memory. See `docs/initiatives/README.md` for the
index and the convention. Load only the initiative you are working on.
