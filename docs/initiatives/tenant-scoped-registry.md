# Tenant-scoped registry (Option B)

**Status:** **shipped.** Verified in code on 2026-07-30 — `tenant_id` is on
`MetadataVersion` and `CompiledRegistryArtifact` (`app/api/meta_models.py`),
and `app/main.py` boots per-tenant via `_load_tenant_registries` into a
`registries` dict.

> Note: this was recorded for months as "IN-PROGRESS (on hold)" in memory while
> it was in fact complete. Status lives here now, next to the code.

Original plan: `docs/Tenant-Scoped Registry Implementation Plan.md`.

## The problem it solved

`MetadataVersion` and `CompiledRegistryArtifact` carried no `tenant_id`, and a
single global `RegistrySchema` singleton was shared across all tenants — so any
tenant could query another tenant's physical tables.

## Architectural decisions

- **`tenant_id` is `NOT NULL` with no default anywhere.** The application layer
  always supplies it explicitly. A default is how cross-tenant leakage creeps
  back in.
- **Composite FK `(version_id, tenant_id)`** from `compiled_registry_artifacts`
  → `metadata_versions`, plus a `uq_version_tenant` unique constraint.
  *Why:* database-layer enforcement means even a direct SQL write cannot create
  a mixed-tenant artifact.
- **`MixedTenantArtifactError(ValueError)`** raised in `app/api/compiler.py`,
  caught as HTTP 422 in the router.
- **Boot uses `DISTINCT ON (tenant_id) ORDER BY compiled_at DESC, artifact_id
  DESC`**, and `artifact_rows` is materialised once and reused by both the
  registry loop and the RAG loop.
- **Per-tenant RAG stores**: `engine._vector_stores: dict[str, VectorStoreProtocol]`,
  and `set_vector_store(store, tenant_id)` requires the tenant explicitly.
- **`compile(..., tenant_id: str, ...)`** — positioned before optional params
  with no default, so a missing tenant is a type error rather than a silent
  fallback.

## Enforcement surface

Every metadata and credential route performs an ownership check, joining
through `MetadataVersion` where the entity doesn't carry `tenant_id` directly
(table, column, column-value routes join through their parent). Mismatch → 403.
`get_registry` resolves `registries.get(cred.tenant_id)` and returns 503 when
absent. `list_*` routes filter by `cred.tenant_id`.

## Gotcha for future work

`ChatSession` is constructed in exactly one place (`router.py`), which already
passes `tenant_id`. If a second construction site appears, it must pass it too —
the column has no default by design.
