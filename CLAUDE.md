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

5. **`app.rag`** — In-memory vector store for semantic column/value matching, used to inject hints into the compiler pipeline before schema filtering.

6. **`app.api`** — FastAPI routes and the Steward UI. Routes: `POST /api/v1/query/generate`, `POST /api/v1/query/execute`, and a full metadata CRUD API under `/api/v1/metadata/`. Exposes a web console served from `static/`.

### Compiler Pipeline (`app/compiler/engine.py`)

The `CompilerEngine.compile()` method runs this pipeline in order:
1. RAG lookup — optionally injects column hints
2. Schema filter — scopes the `RegistrySchema` to relevant tables/columns (`DeterministicSchemaFilter`)
3. Prompt builder — builds a `PromptEnvelope` with **no physical schema targets** exposed to the LLM
4. LLM gateway — calls the configured provider; returns abstract SQL
5. AST parser — `sqlglot`-based parser that rejects DDL/DML, CTEs, subqueries, and non-whitelisted functions
6. Safety engine — validates the AST against per-column `SafetyClassification` rules
7. Translator — maps abstract aliases to physical targets, binds literals as `$1`-style parameters

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

### Key Invariants

- `PromptEnvelope` sent to the LLM must **never** contain physical DB targets (`physical_target` fields).
- `ExecutableQuery` output must **never** contain abstract aliases — all must be resolved to physical targets.
- AST rewrites use immutable copy-on-write (`ValidatedAST`) to preserve provenance.
- All literal values from LLM output are bound as DB-driver parameters, not interpolated into SQL strings.

### Schema Lifecycle

The `app/api/meta_models.py` SQLAlchemy ORM manages `MetadataVersion`, `MetadataTable`, `MetadataColumn`, and `MetadataRelationship`. The `MetadataCompiler` (`app/api/compiler.py`) compiles a draft version into a signed `RegistrySchema` artifact, which `RegistryLoader` (`app/steward/loader.py`) loads at startup and hot-reloads on `POST /api/v1/metadata/compile/{version_id}`.

## Working agreements

These are standing instructions from the repository owner. They apply to every
session and override default behaviour.

**Never relax a rule to make a problem go away.** No `# noqa`, no
`[[tool.mypy.overrides]]`, no bumping `line-length`, and no loosening a safety
control (the multi-statement parser rejection, JOIN-relationship validation,
the row cap) to turn a failure into a pass. Fix the code, or accept the
failure and say so. C901 → extract helpers; E501 → wrap; `no-untyped-def` → add
the annotation. The only acceptable suppressions are the architectural ones
already in the repo: `B008` for FastAPI `Depends()` defaults, `E402` on
`app/main.py` for `load_dotenv()`, and `type: ignore` on hvac imports (no
published stubs) and dynamic exception attributes.

**"Analyse" and "propose a fix" mean stop at the proposal.** Do not edit files
until asked with an explicit "implement", "go ahead", or "do it".

**Execute plans inline in the current session.** Don't offer the
subagent-vs-inline choice — that decision is already made. Still stop at task
boundaries for review, and still stop when genuinely blocked.

**Don't create new Alembic migrations.** Edit
`backend_migrations/versions/0001_initial_schema.py` directly. The database is
dropped and recreated freely, so `0001` is effectively the init script rather
than the first of many. Table DDL belongs there, not in `docker/initdb/*.sql`
(those handle roles, grants and schemas only).

**TDD for all app code.** Write the failing test first and watch it fail.
Commit per logical change with `pytest`, `ruff`, `mypy` and `lint-imports`
green.

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
