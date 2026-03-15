# Authentication Implementation Plan

## Architecture Decisions
- **API keys only, no JWTs.** Single `TenantCredential` table with a `scope` column: "query" or "admin".
- **HMAC-SHA256 hashing** (stdlib `hmac` + `hashlib`). Constant-time comparison using `hmac.compare_digest`.
- **Steward UI** static files remain public; all `/api/v1/metadata/*` API calls require "admin" scope.
- **Tracking:** No `last_used_at` to avoid write contention. Instead, `credential_id` is tracked in `QueryAuditEvent` and `MetadataAudit`.
- **Session Isolation:** explicitly enforce `tenant_id` boundaries during chat session lookup to prevent IDOR.

---

## Step-by-Step Execution Plan
*These steps are strictly ordered to guarantee the test suite remains completely green at every stage until the final router cutover.*

### Step 1: Vault & Secrets Manager
- **`app/vault.py`**: Add `get_credential_hmac_secret()` to `SecretsManager` ABC.
- Provide implementations in `EnvFallbackProvider` (reads `API_KEY_HMAC_SECRET`) and `HashiCorpVaultProvider`. 
*(No runtime impact)*

### Step 2: ORM Models
- **`app/api/meta_models.py`**: Add `TenantCredential` ORM class following the existing `_SCHEMA` conditional namespace pattern.
*(No runtime impact)*

### Step 3: Database Migration
- **[`backend_migrations/versions/...`]**: Generate Alembic migration creating the `credential_scope` enum, the `tenant_credentials` table (without `last_used_at`), and a UNIQUE index on `key_hash`.
- Add raw SQL grants: `SELECT` to `user_aegis_registry_runtime`, `SELECT/INSERT/UPDATE` to `user_aegis_registry_admin`.
- Include `ALTER TABLE aegis_meta.metadata_audit ADD COLUMN credential_id TEXT;` so the ORM mapping holds true against the physical database correctly.
*(Prepares DB state)*

### Step 4: Pydantic & Audit Models
- **`app/api/models.py`**: Add `CredentialCreateRequest`, `CredentialCreateResponse`, and `CredentialListItem` Pydantic models.
- **`app/audit/models.py`**: Add `credential_id: str | None` to the Pydantic model `QueryAuditEvent`.
- **`app/api/meta_models.py`**: Add `credential_id = Column(String)` to the ORM model `MetadataAudit`.
*(No runtime impact)*

### Step 5: Extract Dependencies (Avoid Circular Import)
- **`app/api/dependencies.py`**: Create new file.
- Extract all four session factory dependencies (`get_registry_runtime_db_session`, `get_registry_admin_db_session`, `get_runtime_db_session`, `get_steward_db_session`) from `app/api/router.py` into this new file.
- Update `app/api/router.py` to import these dependencies from `app/api/dependencies.py`.

### Step 6: Core Auth Logic (`auth.py`)
- **`app/api/auth.py`**: Create new file.
  - Implement `generate_api_key()`.
  - Implement `_hash_api_key(raw, secret)`.
  - Implement `verify_api_key(raw, stored_hash, secret)` using `hmac.compare_digest`.
  - Define `ResolvedCredential` frozen dataclass (with `credential_id`, `tenant_id`, `user_id`, `scope`).
  - Implement `require_query_credential` dependency (imports `get_registry_runtime_db_session` from `dependencies.py`).
  - Implement `require_admin_credential` dependency (chains query dep, enforces scope).

### Step 7: Test Infrastructure
- **`tests/conftest.py`**: Set `API_KEY_HMAC_SECRET` env var. **Blocker Fix:** Add `CREATE TABLE IF NOT EXISTS aegis_meta.tenant_credentials` and `CREATE TYPE` raw DDL into `engine_init` before applying the seeds. Seed the `tenant_credentials` table with two pre-hashed test keys.
- **`tests/test_api.py`**: Update existing tests to use `dependency_overrides` to inject a fake `ResolvedCredential` into the pipeline.
- **`tests/test_auth.py`**: Write auth-specific tests for hashing and dependency verification.
*(Prepares tests for the breaking router change)*

### Step 8: Session Boundary Hardening
- **`app/api/router.py`**: Refactor `_resolve_session` to accept a new `tenant_id: str` parameter and apply the filter (`WHERE tenant_id = :tenant_id`) to the `ChatSession` DB lookup so a session UUID from tenant A cannot be resumed by tenant B.
- Callers temporarily pass `"default_tenant"`. Tests stay green.

### Step 9: Router Integration (Breaking Change)
- **`app/api/router.py`**: 
  - Wrap all 15 handlers with `Depends(require_query_credential)` or `Depends(require_admin_credential)`.
  - Replace the temporary `"default_tenant"` and `"api_user"` literals with `cred.tenant_id` and `cred.user_id` across all endpoints, including inside `_resolve_session` calls.
  - Populate `credential_id` onto audit events.
  - Add the 3 new admin key-management endpoints using `get_registry_admin_db_session()`.

### Step 10: Bootstrapping CLI
- **`scripts/create_admin_key.py`**: Write a CLI script that connects to the database, constructs the first admin token, hashes it, inserts it, and securely prints the raw key to stdout once.

### Step 11: Steward & Query UI Modifications
- **`static/steward.html`**: Inject `<dialog>` for the modal DOM and add `<script type="module" src="/static/js/auth-modal.js"></script>`.
- **`static/js/api.js`**: Edit the existing file to add a centralized fetch interceptor that attaches the `Authorization: Bearer <key>` header from `localStorage`. 
    - **401**: Key missing/invalid → clear `localStorage`, show modal.
    - **403**: Insufficient scope → show inline "Admin access required" message, do NOT clear the key.
- **`static/js/auth-modal.js`**: Create new file to implement the modal logic (prompt input -> test `/api/v1/metadata/versions` -> on 200 store).
- **`static/app.js`**: Update the core query console interceptors to attach `Authorization: Bearer <key>` using a straightforward key-entry hook (`prompt` or HTML input).

### Step 12: Final Verification
- Run `pytest`, `ruff check`, `mypy`, and `uv run lint-imports`. Verify no imports are cyclical and all protections hold.
