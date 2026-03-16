# Aegis

Aegis is a **semantic translation and security proxy** between natural language and SQL databases. Users submit natural language queries; Aegis compiles them to parameterized SQL via an LLM, validates the result against a safety policy you control, and executes it. The physical database schema is never exposed to the LLM or the caller.

> **Database support:** The current implementation targets PostgreSQL. The query execution layer uses SQLAlchemy and the SQL generation uses [sqlglot](https://github.com/tobymao/sqlglot), which supports many dialects (MySQL, BigQuery, Snowflake, DuckDB, Spark, and more). Making the dialect configurable is a scoped effort — see the [Database Compatibility](#database-compatibility) section for details.

---

## How it works

```
User intent (natural language)
        ↓
  Schema filter — scopes the registry to relevant tables
        ↓
  Prompt builder — builds a prompt with abstract aliases only (no physical names)
        ↓
  LLM gateway — returns abstract SQL
        ↓
  AST parser — rejects DDL/DML, CTEs, subqueries, non-whitelisted functions
        ↓
  Safety engine — validates every column against per-column policy rules
        ↓
  Translator — maps abstract aliases → physical targets, binds literals as $1 params
        ↓
  Execution engine — runs parameterized SQL with statement_timeout
        ↓
Query results
```

The LLM only ever sees abstract table/column aliases. Physical names, sensitive columns, and join constraints are enforced by the safety engine — not by prompt instructions.

---

## Quickstart

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or Docker Engine + Compose v2)
- Python 3.12+ (for generating secrets and running scripts locally)

### 1. Clone and configure

```bash
git clone <repo-url>
cd aegis

cp .env.example .env
```

Open `.env` and set `API_KEY_HMAC_SECRET`:

```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
# paste the output as the value of API_KEY_HMAC_SECRET in .env
```

### 2. Start the stack

**Option A — Core stack (empty schema, bring your own data)**

Best for connecting Aegis to your own database schema.

```bash
docker compose up --build
```

Boot sequence (~1 minute on first build):
1. `db` — Postgres starts and runs init scripts
2. `migrate` — Alembic creates the `aegis_meta` schema
3. `discover` — introspects the public schema (empty → 0-table draft)
4. `aegis` — app starts on `http://localhost:8000`

**Option B — BIRD stack (75-table demo dataset)**

Best for exploring Aegis with real data immediately.

Uses the [BIRD-SQL Mini-Dev](https://bird-bench.github.io) benchmark dataset
(Jinyang Li et al., NeurIPS 2023), licensed under
[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).
See [`docs/BIRD_SETUP.md`](docs/BIRD_SETUP.md) for download instructions and
full attribution details.

```bash
docker compose -f docker-compose.yml -f docker-compose.bird.yml up --build
```

Boot sequence (~2 minutes on first run):
1. `db` — Postgres starts
2. `migrate` + `bird-loader` — run in parallel (schema setup + BIRD data load)
3. `discover` — reverse-engineers 75 tables, populates semantic descriptions
4. `aegis` — app starts on `http://localhost:8000`

### 3. Create an API key

```bash
docker compose exec aegis sh -c "cd /app && PYTHONPATH=/app .venv/bin/python \
  scripts/create_admin_key.py \
  --tenant-id my-tenant \
  --user-id me@example.com \
  --scope admin \
  --description 'local dev key'"
```

Save the printed key:

```bash
export RAW_KEY=<paste key here>
```

### 4. Verify the stack is up

```bash
curl -s http://localhost:8000/health
# {"status": "ok"}

curl -s -H "Authorization: Bearer $RAW_KEY" \
  http://localhost:8000/api/v1/metadata/versions | python3 -m json.tool
# Returns the draft MetadataVersion created by discover
```

### 5. Open the Steward UI

Navigate to `http://localhost:8000/steward` in your browser.

- Authenticate with your API key
- Review the discovered tables and columns
- Adjust aliases, descriptions, and safety classifications as needed
- Set the version status to **Active**, then click **Compile**

Compiling freezes the schema into a signed registry artifact. The app hot-reloads it — no restart needed.

### 6. Run a query

```bash
# Start a session with the first query
FIRST=$(curl -s -X POST http://localhost:8000/api/v1/query/execute \
  -H "Authorization: Bearer $RAW_KEY" \
  -H "Content-Type: application/json" \
  -d '{"intent": "how many records are in the largest table?"}')

echo $FIRST | python3 -m json.tool

# Capture the session ID for follow-up queries
SESSION_ID=$(echo $FIRST | python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")

# Follow-up using session context
curl -s -X POST http://localhost:8000/api/v1/query/execute \
  -H "Authorization: Bearer $RAW_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"intent\": \"break that down by category\", \"session_id\": \"$SESSION_ID\"}" \
  | python3 -m json.tool
```

---

## Using a real LLM

The default `LLM_PROVIDER=mock` picks the first schema-relevant table and generates a `COUNT(*)` query. It is sufficient for infrastructure testing but does not perform real natural language understanding.

To use a real model, add credentials to `.env` and restart:

```bash
# Anthropic (recommended)
LLM_PROVIDER=anthropic:claude-sonnet-4-6
ANTHROPIC_API_KEY=sk-ant-...

# OpenAI
LLM_PROVIDER=openai:gpt-4o
OPENAI_API_KEY=sk-...

# Local (Ollama — also requires the local-llm profile)
LLM_PROVIDER=ollama:llama3
```

```bash
docker compose restart aegis
```

You can also override the provider per request:

```bash
curl -s -X POST http://localhost:8000/api/v1/query/execute \
  -H "Authorization: Bearer $RAW_KEY" \
  -H "Content-Type: application/json" \
  -d '{"intent": "how many users signed up last month?", "provider_id": "anthropic:claude-sonnet-4-6"}'
```

---

## Local development (without Docker)

Requires a running PostgreSQL instance. Point the env vars at it.

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Run the server
uv run uvicorn app.main:app --reload
```

**Linting and type checking:**

```bash
uv run lint-imports   # enforce architectural module boundaries
uv run ruff check .   # code style
uv run mypy .         # strict type checking
```

**Tests** (run against an in-memory SQLite DB automatically):

```bash
uv run pytest -v
uv run pytest tests/test_compiler_engine.py -v   # single file
uv run pytest -k "test_name" -v                  # single test
```

---

## Project structure

```
app/
  api/          FastAPI routes, Steward UI, metadata CRUD, auth
  compiler/     NL→SQL pipeline: filter, prompt, LLM, parse, safety, translate
  execution/    Database connection layer (only module that touches the DB)
  steward/      Registry loader — boots the compiled schema artifact into memory
  audit/        Async telemetry sink
  rag/          In-memory vector store for column/value semantic hints
  vault.py      Secrets provider (env vars in dev, HashiCorp Vault in prod)

scripts/
  create_admin_key.py     Create an API key for a tenant
  discover_metadata.py    Reverse-engineer a PostgreSQL schema into a draft version
  populate_descriptions.py  Load BIRD semantic column descriptions from CSVs
  apply_grants.py         Apply post-migration PostgreSQL role grants

docker/
  initdb/       SQL scripts run by Postgres on first start (roles, sentinel)
  bird_data/    Place 01_BIRD_dev.sql here (gitignored) for the BIRD overlay
  bird_descriptions/  Committed BIRD column description CSVs (320KB)
  pgadmin/      pgAdmin pre-configured server definition

docs/
  BIRD_SETUP.md         BIRD dataset download and setup instructions
  Test_IMP-013.md       Full Docker integration test guide
```

---

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `API_KEY_HMAC_SECRET` | Yes | — | 32-byte hex secret for API key signing |
| `LLM_PROVIDER` | No | `mock` | LLM provider and model (see `.env.example`) |
| `ANTHROPIC_API_KEY` | If using Anthropic | — | Anthropic API key |
| `OPENAI_API_KEY` | If using OpenAI | — | OpenAI API key |
| `GOOGLE_API_KEY` | If using Google | — | Google AI API key |
| `XAI_API_KEY` | If using xAI | — | xAI API key |
| `BIRD_SQL_PATH` | No | `./docker/bird_data/01_BIRD_dev.sql` | Path to BIRD SQL dump (overlay only) |
| `REDIS_URL` | No | in-memory | Redis URL for multi-worker session continuity |
| `ENVIRONMENT` | No | `development` | Set to `production` to block the `env` secrets provider |
| `SECRETS_PROVIDER` | No | `env` | `env` (dev) or `vault` (production HashiCorp Vault) |

Database connection URLs are set automatically in Docker. For local development, set:

| Variable | Role | Used by |
|---|---|---|
| `DB_URL_RUNTIME` | Query execution | `app.execution` |
| `DB_URL_REGISTRY_RUNTIME` | Read compiled artifacts | `app.steward`, registry endpoints |
| `DB_URL_STEWARD` | Schema authoring | Steward UI CRUD |
| `DB_URL_REGISTRY_ADMIN` | Compile/promote versions | Compile endpoint |

---

## Database compatibility

Aegis currently targets PostgreSQL. The table below shows what would need to change to support another database.

| Component | Current | Effort to change |
|---|---|---|
| SQL dialect (parsing + generation) | `sqlglot` with `dialect="postgres"` | **Low** — two lines in `parser.py` and `translator.py`; sqlglot supports MySQL, BigQuery, Snowflake, DuckDB, Spark, and more |
| Query execution | SQLAlchemy async + `asyncpg` driver | **Low** — swap driver in connection string (`aiomysql`, `asyncmy`, etc.); SQLAlchemy abstracts the rest |
| Statement timeout | `SET LOCAL statement_timeout` (Postgres-only) | **Low** — already gated on `engine.name == "postgresql"`; other engines skip it and lose the timeout protection |
| Schema discovery | `pg_constraint`, `pg_class`, `pg_attribute` catalog tables | **Medium** — `discover_metadata.py` uses Postgres-specific catalog queries; needs DB-specific introspection |
| Meta schema types | `JSONB` and `UUID` from `sqlalchemy.dialects.postgresql` | **Medium** — requires replacing with generic `JSON` + `VARCHAR(36)` and a migration; works on MySQL 5.7+, SQLite, etc. |

The architecture is cleanly separated for this work — there is no raw SQL scattered across the codebase. The execution layer, compiler pipeline, and schema registry are already independent of each other.

---

## API reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness check |
| `POST` | `/api/v1/query/generate` | Compile NL → SQL (no execution) |
| `POST` | `/api/v1/query/execute` | Compile + execute |
| `GET` | `/api/v1/metadata/versions` | List metadata versions |
| `GET` | `/api/v1/metadata/versions/{id}/schema` | Get full schema for a version |
| `POST` | `/api/v1/metadata/versions/{id}/status` | Transition version status |
| `POST` | `/api/v1/metadata/compile/{id}` | Compile version → registry artifact |
| `GET` | `/steward` | Steward UI (browser) |
| `GET` | `/docs` | Auto-generated OpenAPI docs |

All endpoints except `/health` require `Authorization: Bearer <key>`.

---

## pgAdmin

A pgAdmin instance is available at `http://localhost:5050`.

- Email: `admin@admin.com`
- Password: `admin`
- The `aegis` server (connecting to the Docker Postgres) is pre-registered.

> **Tip:** If the login page is unresponsive, open it in an incognito window.
