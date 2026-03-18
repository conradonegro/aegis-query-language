# Tenant-Scoped Registry Implementation Plan (Option B)

## Background

`MetadataVersion` and `CompiledRegistryArtifact` have no `tenant_id`. `RegistryLoader.load_active_schema()` selects the latest active artifact globally, and the result is stored as a single `app.state.registry` singleton shared across all tenants. A tenant with valid credentials can query tables belonging to another tenant's namespace if the query tokens match those tables' descriptions or aliases.

The fix enforces isolation at the database layer: every version and artifact is owned by a tenant. The in-memory state becomes a `dict[str, T]` keyed by `tenant_id` rather than a singleton.

**Invariant:** every DB query touching a version or artifact must be filtered by `tenant_id`. The compiled schema blob for tenant A must never reach tenant B's compile or query path.

---

## Step 1 — Clean Slate: Consolidate Migration History

The existing seven iterative migrations represent the project's evolution and carry historical baggage (e.g. `ALTER TABLE` operations, the TIMESTAMP→TIMESTAMPTZ correction applied after the fact). Since the database is being dropped, consolidate everything into a single clean baseline that bakes all fixes and the new `tenant_id` columns natively into the initial `CREATE TABLE` statements. The new `tenant_id` columns on `MetadataVersion` and `CompiledRegistryArtifact` are strict `NOT NULL` with no default. The same principle applies to `ChatSession.tenant_id`, which currently defaults to `"default_tenant"` in the ORM model — that default must be removed too (see Step 2). Every `tenant_id` column in the schema must be explicitly supplied by the application layer; the database never invents one.

### 1a. Update ORM models first

Update `app/api/meta_models.py` with the `tenant_id` fields (see Step 2 below) **before** autogenerating. The ORM is the source of truth the tool introspects.

### 1b. Delete the migration history

```bash
rm backend_migrations/versions/*.py
```

### 1c. Drop the PostgreSQL database and recreate it

Drop and recreate the database (or just the `aegis_meta` schema) so Alembic sees a completely empty target.

### 1d. Verify `env.py` is configured for schema introspection

`backend_migrations/env.py` must have `include_schemas=True` in the `configure()` call, otherwise the `aegis_meta` schema tables will not be introspected:

```python
context.configure(
    ...
    include_schemas=True,
)
```

### 1e. Autogenerate the baseline migration

```bash
uv run alembic revision --autogenerate -m "initial_schema"
```

### 1f. Review and correct the generated migration by hand

Alembic autogenerate has known blind spots with PostgreSQL. **Do not run the migration before reviewing it.** Known issues to check:

- **Custom Enum types** with `schema=` (e.g. `version_status`, `rag_cardinality`, `rel_type`, `credential_scope`, `audit_action`, `chat_role`) — autogenerate may render these incorrectly or omit the `schema=` argument
- **`JSONB` columns** — may be rendered as `VARCHAR` or `JSON`; must be `postgresql.JSONB`
- **`UUID(as_uuid=True)`** — verify rendered as `UUID` not `VARCHAR`
- **Named constraints** — verify all `UniqueConstraint` and `ForeignKeyConstraint` names are preserved exactly (they are used by downgrade operations)
- **`include_schemas=True` interaction** — confirm the `aegis_meta` schema itself is created before the tables that live in it
- **`tenant_id` consistency constraint** — autogenerate will not produce this automatically; it must be added by hand (see below)

### 1f-i. Add a `tenant_id` consistency constraint to the migration

Denormalizing `tenant_id` onto `CompiledRegistryArtifact` is only safe if the DB enforces that it always matches the owning `MetadataVersion.tenant_id`. Autogenerate will not produce this. Add it manually to the migration.

The cleanest enforcement is a **composite foreign key** from `(compiled_registry_artifacts.version_id, compiled_registry_artifacts.tenant_id)` to `(metadata_versions.version_id, metadata_versions.tenant_id)`. This requires a unique constraint on the target side first:

```sql
-- On metadata_versions (target side of the composite FK)
ALTER TABLE aegis_meta.metadata_versions
  ADD CONSTRAINT uq_version_tenant UNIQUE (version_id, tenant_id);

-- Composite FK on compiled_registry_artifacts
ALTER TABLE aegis_meta.compiled_registry_artifacts
  ADD CONSTRAINT fk_artifact_version_tenant
  FOREIGN KEY (version_id, tenant_id)
  REFERENCES aegis_meta.metadata_versions (version_id, tenant_id);
```

This means a row in `compiled_registry_artifacts` with `(version_id=X, tenant_id=B)` will be rejected by the DB if `metadata_versions` has `(version_id=X, tenant_id=A)`. A bad write — whether from a bug or a direct DB mutation — is impossible.

In the Alembic migration this translates to:
```python
op.create_unique_constraint(
    "uq_version_tenant", "metadata_versions",
    ["version_id", "tenant_id"], schema="aegis_meta"
)
op.create_foreign_key(
    "fk_artifact_version_tenant",
    "compiled_registry_artifacts", "metadata_versions",
    ["version_id", "tenant_id"], ["version_id", "tenant_id"],
    source_schema="aegis_meta", referent_schema="aegis_meta",
)
```

### 1g. Run the migration

```bash
uv run alembic upgrade head
```

`tenant_id` is denormalized onto `CompiledRegistryArtifact` (not just derived via JOIN from `MetadataVersion`) so artifact lookups can filter without a JOIN and the isolation guarantee is auditable at the artifact table level directly.

---

## Step 2 — ORM Model Updates (`app/api/meta_models.py`)

**`MetadataVersion`** — add:
```python
tenant_id: Mapped[str] = mapped_column(Text, nullable=False)
```

**`CompiledRegistryArtifact`** — add:
```python
tenant_id: Mapped[str] = mapped_column(Text, nullable=False)
```

No `default=` value. The application layer is responsible for always supplying `tenant_id` explicitly — the database enforces this with `NOT NULL`.

**`ChatSession`** — remove the existing default:
```python
# before
tenant_id: Mapped[str] = mapped_column(Text, nullable=False, default="default_tenant")

# after
tenant_id: Mapped[str] = mapped_column(Text, nullable=False)
```

`_resolve_session()` in the router already creates every `ChatSession` with `tenant_id=tenant_id` (the resolved credential's tenant), so no production call site relies on this default. Removing it makes the contract explicit.

After removing the default, grep for any direct `ChatSession(...)` construction outside `_resolve_session()` — in tests, scripts, or fixtures — and update each to supply `tenant_id=` explicitly:
```bash
grep -rn "ChatSession(" tests/ app/ scripts/
```

`AbstractTableDef` in `app/steward/models.py` is **not** touched. The isolation is fully at the loading and storage boundary, not inside the compiled schema object itself.

---

## Step 3 — RegistryLoader (`app/steward/loader.py`)

Change the signature of `load_active_schema`:
```python
@staticmethod
async def load_active_schema(
    session: AsyncSession, tenant_id: str
) -> RegistrySchema | None:
```

Add `MetadataVersion.tenant_id == tenant_id` to the existing WHERE clause. All hydration logic below the query is unchanged.

---

## Step 4 — MetadataCompiler (`app/api/compiler.py`)

**4a. Stamp `tenant_id` on the artifact**

In `compile_version()`, copy `tenant_id` from the loaded version onto the new artifact:

```python
artifact = CompiledRegistryArtifact(
    ...
    tenant_id=version.tenant_id,  # copy from version
)
```

No signature change required — `version.tenant_id` is available on the already-loaded ORM object.

**4b. Fix per-table `tenant_id` fallback in the compiled blob**

`compile_version()` currently writes each table's `tenant_id` into the blob as:
```python
"tenant_id": tbl.tenant_id or "default_tenant",
```

With strict multi-tenancy this fallback is wrong — a table with `tenant_id=None` would be indexed into the RAG store under `"default_tenant"` regardless of which tenant owns the version, causing RAG misses for all legitimate tenants.

Since Option B enforces one tenant per version, reject any table row whose `tenant_id` is set to a different value than the version's `tenant_id`. A silent override would allow a bad DB row to produce a mixed-tenant artifact, with the RAG builder indexing some tables under the wrong tenant.

Introduce a dedicated domain exception in `app/api/compiler.py` so the router can surface it as a 422 rather than a generic 500:

```python
class MixedTenantArtifactError(ValueError):
    """Raised when a table's tenant_id conflicts with its owning version's tenant_id."""
```

Raise it during the table iteration:
```python
if tbl.tenant_id is not None and tbl.tenant_id != version.tenant_id:
    raise MixedTenantArtifactError(
        f"Table '{tbl.alias}' has tenant_id='{tbl.tenant_id}' "
        f"which conflicts with version tenant_id='{version.tenant_id}'. "
        f"All tables in a version must belong to the same tenant."
    )
"tenant_id": version.tenant_id,  # always use the version owner
```

Catch it in the `compile_metadata_version` router handler and return a 422:
```python
except MixedTenantArtifactError as exc:
    raise HTTPException(status_code=422, detail=str(exc)) from exc
```

This removes the `tbl.tenant_id or ...` fallback entirely. The RAG builder in `app/rag/builder.py:193` reads `tbl_dict.get("tenant_id", tenant_id)` — with the artifact blob always supplying `version.tenant_id`, the builder's fallback becomes dead code and the two paths are guaranteed to agree.

---

## Step 5 — CompilerEngine (`app/compiler/engine.py`)

`CompilerEngine` currently stores a single `self.vector_store: VectorStoreProtocol | None`. Change it to a per-tenant dict:

```python
# __init__
self._vector_stores: dict[str, VectorStoreProtocol] = {}

# signature change
def set_vector_store(self, store: VectorStoreProtocol, tenant_id: str) -> None:
    self._vector_stores[tenant_id] = store
```

In `_apply_rag_hints`, replace the single-store lookup:
```python
store = self._vector_stores.get(tenant_id)
if not store:
    return
rag_result = store.search(...)
```

`tenant_id` is already a parameter of `compile()` and is already forwarded to `_apply_rag_hints`, so no further signature changes are needed.

---

## Step 6 — App State Refactoring (`app/main.py`)

Replace singletons with per-tenant dicts:

| Before | After |
|---|---|
| `app.state.registry: RegistrySchema` | `app.state.registries: dict[str, RegistrySchema]` |
| `app.state.vector_store: InMemoryVectorStore` | `app.state.vector_stores: dict[str, InMemoryVectorStore]` |

**Testing mode boot:**
```python
# before
app.state.registry = schema
app.state.vector_store = vector_store
compiler.set_vector_store(vector_store)

# after
app.state.registries = {"test_tenant": schema}
app.state.vector_stores = {"test_tenant": vector_store}
compiler.set_vector_store(vector_store, "test_tenant")
```

**Production boot** — use the same selection logic as `RegistryLoader.load_active_schema()` so the registry and the RAG index are guaranteed to point at the same artifact version per tenant. Fetch one artifact per tenant (the most recently compiled one with an active version) using `DISTINCT ON`:

```python
# One latest artifact per tenant, matching RegistryLoader's ordering.
# artifact_id DESC is a deterministic tie-breaker for the rare case where
# two artifacts share the same compiled_at timestamp.
stmt = (
    sa_select(CompiledRegistryArtifact, MetadataVersion.tenant_id)
    .join(
        MetadataVersion,
        CompiledRegistryArtifact.version_id == MetadataVersion.version_id,
    )
    .where(MetadataVersion.status == "active")
    .distinct(MetadataVersion.tenant_id)
    .order_by(
        MetadataVersion.tenant_id,
        CompiledRegistryArtifact.compiled_at.desc(),
        CompiledRegistryArtifact.artifact_id.desc(),
    )
)
result = await session.execute(stmt)
artifact_rows = result.all()  # materialise once; reused by both loops below
```

Using `RegistryLoader.load_active_schema()` for the registry half (rather than re-hydrating from the artifact inline) guarantees both the registry dict and the RAG store are derived from the exact same artifact row selected by the same predicate.

**Registry loop:**
```python
registries: dict[str, RegistrySchema] = {}
for _artifact, tid in artifact_rows:
    loaded = await RegistryLoader.load_active_schema(session, tid)
    if loaded:
        registries[tid] = loaded
app.state.registries = registries
```

**`_boot_rag_index(app)`** — reuse the same materialised list to build per-tenant RAG stores:
```python
for artifact, tid in artifact_rows:
    col_values = await _fetch_rag_column_values_for_version(artifact.version_id, session)
    new_store = await build_from_artifact(
        artifact_blob=artifact.artifact_blob,
        version_id=str(artifact.version_id),
        tenant_id=tid,
        artifact_version=artifact.artifact_hash,
        column_values=col_values,
    )
    app.state.vector_stores[tid] = new_store
    app.state.compiler.set_vector_store(new_store, tid)
```

**Boot observability** — log tenant count and total boot duration so there is a hard signal when eager loading becomes a bottleneck and lazy loading should be considered:
```python
logger.info(
    "Registry boot complete: %d tenant(s) loaded in %.1fms",
    len(registries),
    elapsed_ms,
)
```
This is the current strategy. If the tenant count grows large or boot time regresses, move to lazy loading (load each tenant's schema on first authenticated request and cache).

**`/health` endpoint** — update the `index_ready` check:
```python
stores = getattr(request.app.state, "vector_stores", {})
index_ready = any(s.index_ready for s in stores.values()) if stores else False
```

---

## Step 7 — Router Changes (`app/api/router.py`)

### 7a. `get_registry` dependency

Make it tenant-aware. Because FastAPI memoizes `require_query_credential` per request, endpoints that already depend on `cred` will not trigger a second auth DB query.

```python
def get_registry(
    request: Request,
    cred: Annotated[ResolvedCredential, Depends(require_query_credential)],
) -> RegistrySchema:
    schema = request.app.state.registries.get(cred.tenant_id)
    if schema is None:
        raise HTTPException(
            status_code=503,
            detail="No compiled schema available for this tenant. "
                   "Compile a registry artifact first.",
        )
    return schema
```

### 7b. `generate_query` and `execute_query`

The router already passes `tenant_id=cred.tenant_id` into `compiler.compile()` — no additional wiring needed here. However, remove the `tenant_id="default_tenant"` default value from `CompilerEngine.compile()`'s signature (`engine.py:74`). The parameter must remain required (no default) so that future call sites cannot silently omit it and fall through to a wrong tenant's store.

### 7c. `compile_metadata_version`

Four fixes required:

1. Load the version and assert ownership — prevents tenant A's admin from compiling tenant B's version:
   ```python
   if version.tenant_id != cred.tenant_id:
       raise HTTPException(status_code=403, detail="Version does not belong to your tenant.")
   ```

2. Hot-reload only the requesting tenant's slot:
   ```python
   request.app.state.registries[cred.tenant_id] = schema
   ```

3. RAG rebuild targets the tenant's slot:
   ```python
   request.app.state.vector_stores[cred.tenant_id] = new_store
   compiler.set_vector_store(new_store, cred.tenant_id)
   ```

4. Fix hardcoded `tenant_id="default_tenant"` in `build_from_artifact(...)` and pass `tenant_id=cred.tenant_id` to `RegistryLoader.load_active_schema(rt_session, cred.tenant_id)`.

### 7d. `create_metadata_version`

Stamp the new version with the requesting tenant:
```python
new_version = MetadataVersion(
    ...
    tenant_id=cred.tenant_id,
)
```

For baseline cloning: assert `baseline.tenant_id == cred.tenant_id` before cloning — prevents tenant A's admin from cloning tenant B's version as a starting point.

### 7e. `list_metadata_versions`

Add tenant filter to the query:
```python
.where(MetadataVersion.tenant_id == cred.tenant_id)
```

### 7f. `update_version_status`

Add ownership check after loading the version:
```python
if version.tenant_id != cred.tenant_id:
    raise HTTPException(status_code=403, detail="Version does not belong to your tenant.")
```

### 7g. `get_metadata_schema` (GET `.../schema`)

Same ownership check after loading the version.

### 7h. `obfuscate_schema`

Same ownership check after loading the version.

### 7i. `update_metadata_table` and `update_metadata_column`

These take a bare `table_id`/`column_id`. Enforce ownership by joining through to `MetadataVersion.tenant_id == cred.tenant_id` in the SELECT rather than fetching then checking in Python.

### 7j. `create_column_value`, `bulk_import_column_values`, `deactivate_column_value`

Join through `MetadataColumn → MetadataVersion` and assert tenant ownership before mutating.

### 7k. `list_credentials`, `create_credential`, and `revoke_credential`

`list_credentials` currently returns all credentials globally — filter to `cred.tenant_id`.

`create_credential` accepts an arbitrary `payload.tenant_id` — validate it equals `cred.tenant_id` to prevent an admin of tenant A from provisioning credentials for tenant B.

`revoke_credential` (`DELETE /auth/credentials/{credential_id}`) is currently unscoped — it loads a credential by ID with no tenant check, meaning tenant A's admin can revoke tenant B's credentials. Add a tenant ownership check after loading:
```python
if target_cred.tenant_id != cred.tenant_id:
    raise HTTPException(status_code=403, detail="Credential does not belong to your tenant.")
```

### 7l. `list_column_values`

`GET /metadata/columns/{column_id}/values` is currently unscoped — it loads values for any column ID without verifying the column's owning version belongs to the requesting tenant. Fix by joining through `MetadataColumn → MetadataVersion` and asserting `MetadataVersion.tenant_id == cred.tenant_id`, consistent with the approach in 7i and 7j.

---

## Step 8 — Test Updates

### 8a. Sweep all `compiler.compile(...)` call sites

Removing `tenant_id="default_tenant"` from `CompilerEngine.compile()`'s signature makes `tenant_id` a required keyword argument. Every call site that currently omits it will become a `TypeError` at runtime and a mypy error at type-check time. Before removing the default, grep for all invocations:

```bash
grep -rn "\.compile(" tests/ app/
```

Every call site that does not already pass `tenant_id=` must be updated. Known locations beyond the router (which is already wired):
- `tests/test_compiler_engine.py` — likely multiple `engine.compile(...)` calls
- Any other test files that construct a `CompilerEngine` directly and call `compile()`

Update each to pass an explicit `tenant_id="test_tenant"` (or the appropriate tenant for the test's scenario).

### 8b. Fix `tests/test_worm_security.py`

`RegistryLoader.load_active_schema()` is called directly in `tests/test_worm_security.py:189`. Adding the required `tenant_id` parameter will break that call. Update it to pass an explicit tenant:
```python
# before
schema = await RegistryLoader.load_active_schema(session)

# after
schema = await RegistryLoader.load_active_schema(session, "test_tenant")
```

### 8c. `set_vector_store` call sites

Update every `engine.set_vector_store(store)` call to `engine.set_vector_store(store, tenant_id)`. The primary location is `tests/test_compiler_engine.py`:
```python
# before
engine.set_vector_store(build_test_store())

# after
engine.set_vector_store(build_test_store(), "test_tenant")
```

Grep for any other call sites:
```bash
grep -rn "set_vector_store(" tests/ app/
```

### 8d. `app.state.registry` references

If any test reads `app.state.registry` directly, update to `app.state.registries["test_tenant"]`. Grep:
```bash
grep -rn "app\.state\.registry\b" tests/ app/
```

### 8e. New test: `tests/test_registry_tenant_isolation.py`

Cover:
- `get_registry` returns the correct schema for the requesting tenant.
- `get_registry` raises HTTP 503 for a tenant with no loaded schema.
- `compile_metadata_version` returns 403 when `version.tenant_id != cred.tenant_id`.
- `list_metadata_versions` returns only versions owned by the requesting tenant.
- `create_metadata_version` stamps `tenant_id` from the credential.
- Mixed-tenant artifact compile raises `ValueError` (Step 4b enforcement).

---

## Change Surface Summary

| File | Type of change |
|---|---|
| `backend_migrations/versions/*.py` | Delete all; replace with single autogenerated `001_initial_schema.py` (manually corrected) |
| `backend_migrations/env.py` | Verify `include_schemas=True` |
| `app/api/meta_models.py` | Add strict `NOT NULL` `tenant_id` to 2 ORM models; remove `default="default_tenant"` from `ChatSession` |
| `app/api/compiler.py` | Stamp `tenant_id` on artifact; add `MixedTenantArtifactError`; enforce single-tenant-per-version invariant |
| `app/steward/loader.py` | Add `tenant_id` param to `load_active_schema` |
| `app/compiler/engine.py` | `vector_store` singleton → per-tenant dict; remove `tenant_id` default value |
| `app/main.py` | Singleton state → per-tenant dicts; `DISTINCT ON` boot query; observability logging |
| `app/api/router.py` | `get_registry` tenant-aware; ownership checks on ~12 endpoints including `revoke_credential` and `list_column_values` |
| `tests/test_compiler_engine.py` | Update `set_vector_store` and all `compile(...)` call sites |
| `tests/test_worm_security.py` | Update `load_active_schema()` call to pass `tenant_id` |
| `tests/` (sweep) | All other `compile(...)` call sites missing `tenant_id=` |
| `tests/test_registry_tenant_isolation.py` | New isolation test suite |
