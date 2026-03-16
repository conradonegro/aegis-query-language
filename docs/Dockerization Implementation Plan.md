# Docker Containerization Plan (IMP-013)

## Core Architecture Decisions

- **Uv Built Image:** The application image is built via a `uv sync --frozen --no-dev --compile-bytecode` cache sequence. All services (`migrate`, `discover`, `aegis`) share this single image. Core service exposes `8000`.
- **Decoupled Boot Sequence:** Alembic races are eliminated using a blocking `migrate` init container. `discover` blocks behind it. `aegis` blocks behind both.
- **Data Load Synchronization:** `pg_isready` is blind to in-progress `initdb.d` scripts. A sentinel `99_ready_signal.sql` (alphabetically last) creates `public._aegis_docker_ready` after the 1GB BIRD dump commits. The `db` healthcheck queries this table, blocking all dependents until the full dataset is loaded.
- **Auto-Discovery:** `discover_metadata.py` runs once on a clean database to draft `aegis_meta`. Grants are revoked only on success (via `else`, not `finally`) so that a mid-run crash leaves grants intact for the retry attempt.
- **Secret Delivery:** `API_KEY_HMAC_SECRET` is injected into `aegis` via Compose variable substitution from a local `.env` file. `migrate` and `discover` do not require it — `get_credential_hmac_secret()` is only called per auth request, never at startup or during migrations.
- **LLM Provider:** Default is `mock`. When the `local-llm` profile is active, the user sets `LLM_PROVIDER=ollama` in their `.env` file, which Compose resolves via `${LLM_PROVIDER:-mock}`.
- **Services:** `db` (Postgres), `redis` (Session Store), `migrate` (Alembic), `discover` (Schema Loader), `aegis` (API), `ollama` (Optional — profile `local-llm`).

---

## Boot Sequence

```
db                                          redis
  initdb (first boot only):                  healthcheck: redis-cli ping
    00_init_roles.sql                                 |
    01_BIRD_dev.sql (~minutes)                        |
    99_ready_signal.sql                               |
  healthcheck: SELECT 1 FROM                          |
    public._aegis_docker_ready LIMIT 1                |
    (interval:10s  retries:60  start_period:10s)      |
        |                                             |
        ↓                                             |
migrate (depends_on: db: service_healthy)             |
  uv run alembic upgrade head → exits 0               |
        |                                             |
        ↓                                             |
discover (depends_on: migrate: service_completed_successfully)
  uv run python scripts/discover_metadata.py → exits 0
  [on success only via else:]: REVOKE public schema grants
        |                                             |
        └──────────────────┬──────────────────────────┘
                           ↓
aegis (depends_on: discover: service_completed_successfully
                   redis:    service_healthy)
  uv run uvicorn app.main:app --host 0.0.0.0 --port 8000
```

---

## Step-by-Step Implementation Plan

### Step 1: SQL Bootstrapping Scripts

**`scripts/init_roles.sql`** — append after the existing role/grant block:
```sql
-- Temporary elevated grants for discover_metadata.py (revoked after first run)
GRANT USAGE ON SCHEMA public TO role_aegis_meta_owner;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO role_aegis_meta_owner;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO role_aegis_meta_owner;
```
- `REFERENCES` privilege is explicitly omitted — read-only discovery only.
- `ALTER DEFAULT PRIVILEGES` covers tables created by `postgres` (the initdb superuser) AFTER this script runs — which is exactly when `01_BIRD_dev.sql` executes. The `GRANT SELECT ON ALL TABLES` handles any tables that already exist at script time.
- These grants are revoked by `discover_metadata.py` on successful completion.

**`scripts/99_ready_signal.sql`** — new file:
```sql
CREATE TABLE public._aegis_docker_ready (id INT);
```
- Plain `CREATE TABLE` (no `IF NOT EXISTS`) — initdb only ever runs once on a fresh volume.
- Alphabetically sorts after `01_BIRD_dev.sql`, guaranteeing it executes last.

---

### Step 2: The Core Dockerfile

**`Dockerfile`** — new file at project root:
- Base: `ghcr.io/astral-sh/uv:python3.12-bookworm-slim`
- Layer 1: `COPY pyproject.toml uv.lock ./` → `uv sync --frozen --no-dev --compile-bytecode`
  - `--no-dev` drops mypy, pytest, ruff, import-linter from the image.
  - `--compile-bytecode` pre-compiles `.py → .pyc` for faster startup.
  - This layer is cached independently of source code — code changes do not trigger a dep reinstall.
- Layer 2: `COPY . .`
- `EXPOSE 8000`
- `CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]`
  - `migrate` and `discover` override this `CMD` in `docker-compose.yml`.

---

### Step 3: Idempotency & Privilege Revocation in `discover_metadata.py`

Two changes to `scripts/discover_metadata.py`:

**1. Idempotency guard** — at the top of `discover_and_draft_metadata()`, before any `try` block:
```python
result = await session.execute(select(MetadataVersion.version_id).limit(1))
if result.scalar_one_or_none():
    print("Metadata registry already populated. Skipping discovery.")
    return
```
This `return` is structurally above the `try/else` block. Early return on second boot does NOT trigger the `else` (revoke) block — correctly, because grants were already revoked on the first successful run.

**2. `try/else` for privilege revocation** — wrap the discovery logic:
```python
try:
    # ... all discovery logic (tables, columns, relationships) ...
except Exception:
    raise  # exit non-zero → Docker retries; grants remain intact for retry
else:
    # Runs ONLY on success (no exception raised)
    await session.execute(text(
        "REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM role_aegis_meta_owner;"
    ))
    await session.execute(text(
        "REVOKE USAGE ON SCHEMA public FROM role_aegis_meta_owner;"
    ))
    await session.execute(text(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        "REVOKE SELECT ON TABLES FROM role_aegis_meta_owner;"
    ))
    await session.commit()
```

Why `else` and not `finally`:
- `finally` runs on both success AND failure. If discovery crashes mid-run and revokes grants, the `restart: on-failure` retry attempt will fail immediately (no `USAGE` on `public`) — a self-defeating loop.
- `else` runs only on success. Failure leaves grants intact so retries can proceed.
- Limitation: SIGKILL (OOM killer, forced `docker compose down`) bypasses both `else` and `finally`. Grants would remain until the next successful run revokes them. This is acceptable — the grants are SELECT-only, and a SIGKILL scenario requires operator intervention anyway.

---

### Step 4: System Orchestration (`docker-compose.yml`)

Full service definitions:

**`db`**
- Image: `postgres:15-alpine`
- Volumes:
  - `./data/processed:/docker-entrypoint-initdb.d` (existing — `01_BIRD_dev.sql`)
  - `./scripts/init_roles.sql:/docker-entrypoint-initdb.d/00_init_roles.sql:ro` (new)
  - `./scripts/99_ready_signal.sql:/docker-entrypoint-initdb.d/99_ready_signal.sql:ro` (new)
- Healthcheck:
  ```yaml
  test: ["CMD-SHELL", "psql -U postgres -d aegis_data_warehouse -c 'SELECT 1 FROM public._aegis_docker_ready LIMIT 1;'"]
  interval: 10s
  timeout: 5s
  retries: 60        # 10-minute budget for 1GB BIRD load
  start_period: 10s  # grace period before retries count
  ```
  Without `01_BIRD_dev.sql` (user doesn't have the dataset), the healthcheck resolves in seconds.

**`redis`**
- Image: `redis:7-alpine`
- No host port exposed (internal mesh only)
- Healthcheck: `redis-cli ping` / `interval: 5s, retries: 5`

**`migrate`**
- `build: .`
- `command: ["uv", "run", "alembic", "upgrade", "head"]`
- `depends_on: db: condition: service_healthy`
- `restart: on-failure` (Alembic is idempotent — safe to retry)
- Environment:
  - `DATABASE_URL: postgresql+asyncpg://user_aegis_meta_owner:meta_owner_pass@db:5432/aegis_data_warehouse`
  - `SECRETS_PROVIDER: env`
  - Note: `API_KEY_HMAC_SECRET` is NOT required — `get_credential_hmac_secret()` is never called during migrations.

**`discover`**
- `build: .`
- `command: ["uv", "run", "python", "scripts/discover_metadata.py"]`
- `depends_on: migrate: condition: service_completed_successfully`
- `restart: on-failure` (safe — grants remain on failure, idempotency guard prevents double-run on success)
- Environment:
  - `DATABASE_URL: postgresql+asyncpg://user_aegis_meta_owner:meta_owner_pass@db:5432/aegis_data_warehouse`
  - `SECRETS_PROVIDER: env`

**`aegis`**
- `build: .`
- `ports: ["8000:8000"]`
- `depends_on:`
  - `discover: condition: service_completed_successfully`
  - `redis: condition: service_healthy`
- Environment:
  - `SECRETS_PROVIDER: env`
  - `ENVIRONMENT: development`
  - `LLM_PROVIDER: ${LLM_PROVIDER:-mock}` (user sets `LLM_PROVIDER=ollama` in `.env` for local LLM)
  - `REDIS_URL: redis://redis:6379/0`
  - `API_KEY_HMAC_SECRET: ${API_KEY_HMAC_SECRET}` (resolved from `.env` via Compose variable substitution)
  - `DB_URL_RUNTIME: postgresql+asyncpg://user_aegis_runtime:runtime_pass@db:5432/aegis_data_warehouse`
  - `DB_URL_REGISTRY_RUNTIME: postgresql+asyncpg://user_aegis_registry_runtime:registry_pass@db:5432/aegis_data_warehouse`
  - `DB_URL_STEWARD: postgresql+asyncpg://user_aegis_steward:steward_pass@db:5432/aegis_data_warehouse`
  - `DB_URL_REGISTRY_ADMIN: postgresql+asyncpg://user_aegis_registry_admin:admin_pass@db:5432/aegis_data_warehouse`
  - Note: All `DB_URL_*` values override any `localhost`-based values that may exist in `.env`.

**`ollama`** (optional)
- `image: ollama/ollama`
- `profiles: ["local-llm"]`
- `volumes: ollama_data:/root/.ollama`
- To use: run `docker compose --profile local-llm up` and set `LLM_PROVIDER=ollama` in `.env`.

**Top-level `volumes:`**
```yaml
volumes:
  ollama_data:
```

---

### Step 5: `.env.example`

A new `.env.example` at project root documents what the user must supply:
```bash
# Required: generate with:
# python3 -c "import secrets; print(secrets.token_hex(32))"
API_KEY_HMAC_SECRET=

# Optional: set to "ollama" when using --profile local-llm
LLM_PROVIDER=mock
```
All other env vars use defaults that work out-of-the-box for the Docker dev stack.
