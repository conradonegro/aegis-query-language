## **Core tables**

All tables below live in a dedicated schema `aegis_meta`. Primary keys are `uuid` (use `gen_random_uuid()` / `uuid_generate_v4()`).

The listed columns are not exhaustive, you can suggest additions/removals/changes.

1. **metadata\_version/s**  
* `version_id UUID PRIMARY KEY`  
* `registry_hash TEXT` — SHA256 of compiled artifact  
* `status ENUM('draft','pending_review','active','archived')`  
* `created_by TEXT`  
* `created_at TIMESTAMP`  
* `approved_by TEXT NULL`  
* `approved_at TIMESTAMP NULL`  
* `change_reason TEXT NULL`  
* `signed_by TEXT NULL` — optional cryptographic signer id  
* `signed_at TIMESTAMP NULL`

2. **metadata\_tables**  
* `table_id UUID PK`  
* `version_id UUID NOT NULL REFERENCES metadata_versions(version_id)`  
* `real_name TEXT NOT NULL` (schema.table)  
* `alias TEXT NOT NULL`  
* `description TEXT`  
* `tenant_id TEXT NULL` — multi-tenant support  
* `active BOOL DEFAULT TRUE`  
* `created_by`, `created_at`, `updated_by`, `updated_at`  
* Unique constraint: `(version_id, tenant_id, alias)` and `(version_id, tenant_id, real_name)`

3. **metadata\_columns**  
* `column_id UUID PK`  
* `version_id UUID NOT NULL REFERENCES metadata_versions(version_id)`  
* `table_id UUID NOT NULL REFERENCES metadata_tables(table_id)` — binds to the versioned table row  
* `real_name TEXT NOT NULL`  
* `alias TEXT NOT NULL`  
* `description TEXT`  
* `data_type TEXT` (normalized types)  
* `is_nullable BOOL`  
* `is_primary_key BOOL`  
* `is_unique BOOL`  
* `is_sensitive BOOL` (PII classification)  
* `allowed_in_select BOOL`  
* `allowed_in_filter BOOL`  
* `allowed_in_join BOOL`  
* `safety_classification JSONB` — structured finer-grain policy  
* `created/updated metadata`  
* Unique constraint: `(version_id, table_id, alias)` and `(version_id, table_id, real_name)`

4. **metadata\_relationships**  
* `relationship_id UUID PK`  
* `version_id UUID NOT NULL REFERENCES metadata_versions(version_id)`  
* `source_table_id UUID NOT NULL REFERENCES metadata_tables(table_id)`  
* `source_column_id UUID NOT NULL REFERENCES metadata_columns(column_id)`  
* `target_table_id UUID NOT NULL REFERENCES metadata_tables(table_id)`  
* `target_column_id UUID NOT NULL REFERENCES metadata_columns(column_id)`  
* `relationship_type ENUM('fk','logical','denormalized')`  
* `bidirectional BOOL`  
* `active BOOL`  
* `created/updated metadata`  
* Add constraint: `source_table.version_id = version_id` and `target_table.version_id = version_id` (see Implementation notes)

5. **metadata\_audit**  
* `audit_id UUID PK`  
* `version_id UUID`  
* `actor TEXT`  
* `action ENUM('create','update','approve','deploy','revoke')`  
* `payload JSONB`  
* `timestamp`

6. **compiled\_registry\_artifacts**  
* `artifact_id UUID PK`  
* `version_id UUID NOT NULL REFERENCES metadata_versions(version_id)`  
* `artifact_blob JSONB` (or TEXT)  
* `artifact_hash TEXT`  
* `compiled_at TIMESTAMP`  
* `compiler_version TEXT`  
* `signature TEXT` (optional)  
* unique on `version_id`

---

# **Important schema & integrity rules**

The following must be enforced at DB \+ app layer:

* **Version scoping:** every metadata\_tables/columns/relationships row is tied to a `version_id`. The compiler only loads rows for one active `version_id`.  
* **Immutability post-activation:** when `metadata_versions.status = 'active'`, rows associated with that version are conceptually immutable — any change creates a new `version_id`.  
* **Cross-row FK checks:** relationships must reference `table_id` and `column_id` rows belonging to the *same* `version_id`. Enforce with triggers and application-level validation at compile time.  
* **Unique alias constraints:** prevent alias collisions inside a version+tenant; this simplifies translator mapping.  
* **Safety flags default to conservative:** default `allowed_in_select=false` etc., and require explicit enablement.  
* **Audit trail mandatory:** Every mutation must create a `metadata_audit` entry.

---

# **Implementation notes**

1. **Version consistency checks**  
   * DB constraints alone may not enforce `table.version_id == column.version_id`. Use application-level validators during compilation; add triggers if you prefer DB-side enforcement.  
2. **Compiling the artifact**  
   * The compile step reads `metadata_*` rows for a chosen `version_id` that are active and approved, then validates signatures/consistency, produces a `registry_schema.json`, computes `registry_hash`, stores it in `compiled_registry_artifacts`, and flips `metadata_versions.status` to `active` only when the operator approves.  
3. **Race conditions**  
   * Lock the version during compilation (advisable use `SELECT FOR UPDATE` on the metadata\_versions row).  
4. **Multi-tenant**  
   * Include `tenant_id` in every row; compile per-tenant artifacts. Or allow global schemas \+ tenant overlays. Prefer per-tenant compiled artifacts if you require strict isolation.  
5. **Performance**  
   * Index `alias` lookups: index `(version_id, tenant_id, alias)` for fast SchemaFilter.  
   * Relationship lookups should be indexed on `source_table_id` and `target_table_id`.  
6. **Storage of artifact**  
   * JSONB in DB is fine for small registries; for larger, store artifact in object storage and store a reference+hash in DB.  
7. **Secret info & encryption**  
   * Mark PII columns with `is_sensitive`; redact in explainability.  
   * Stored artifacts should be access-protected. Consider encrypting `artifact_blob` at rest if required.  
8. **Schema migrations**  
   * Add Liquibase / Alembic migrations and migration tests.

---

# **Ingestion & editorial workflow**

1. **Discovery / Draft**  
   * Script reads DB information\_schema and proposes `metadata_tables`/`metadata_columns` prefilled with `real_name` and suggested alias (table1, table2, etc. or column1, column2, …).  
   * It also auto-discovers FKs but flags them as `relationship_type='discovered'` (requires human approval).  
2. **Enrichment**  
   * Steward UI allows reviewers to:  
     * See all inactive rows to approve or not  
     * edit alias, descriptions  
     * set `allowed_in_select`, `is_sensitive`, `safety_classification`  
     * approve or reject discovered relationships  
     * Steward UI must show `version_id` context (draft vs active) and require explicit “Compile & Activate” action.  
     * API to propose changes: returns `version_id` draft id  
     * API for reviewers: list pending versions, diff view, approve/reject  
     * Show compiled artifact hash and signature in UI  
3. **Validation run**  
   * Runner applies a `validate()` process:  
     * alias uniqueness  
     * pk existence  
     * no ambiguous column aliases in single table scope  
     * relationship source/target exist  
     * compile-registry validation (dry-run translator, run safety checks over sample queries)  
     * unit tests for joins/translation  
4. **Approval & Compile**  
   * On approval commit, `metadata_versions` entry created and compiled artifact generated.  
   * Only compiled artifact is loaded by runtime Aegis instances.  
5. **Deploy**  
   * Promote `metadata_versions` to `active` after approval; communicate to Aegis runtime to reload artifact or restart cleanly.  
6. **Rollback**  
   * Keep older compiled artifact rows for rollback.

---

# **Security & governance recommendations**

1. **Least privilege**: Execution database role must **not** have write access to `aegis_meta`. Metadata DB user is separate and limited to steward UI or batch jobs.  
2. **Approval workflow**: Changes require at least 1 reviewer \+ 1 approver in critical tables (columns/relationships). Record in `metadata_audit`.  
3. **Read-only runtime**: Aegis only reads compiled artifact; runtime cannot alter artifact.  
4. **Signing**: Sign compiled artifacts (HMAC or RSA) and verify at load time.  
5. **Retention & retention policy**: Keep artifacts and audit logs indefinitely (or per policy); purge drafts older than N days.  
6. **Secrets & PII handling**: Columns with `is_sensitive` should be redacted in explainability unless a privileged role requests it (and that must go to audit).  
7. **Monitoring**: Emit metrics for: compile\_time, number of relationships, alias collisions flagged, number of active versions.

---

# **Testing**

* **Unit**  
  * alias uniqueness  
  * relationship validation  
  * PK presence  
  * safety flags defaults  
* **Integration**  
  * ingest sample BIRD DB \-\> draft metadata \-\> compile artifact \-\> translator integration tests  
  * run translation tests against compiled artifact  
  * explainability shows provenance for repairs  
* **Regression**  
  * golden tests with seeded Postgres (docker-compose) using current artifact  
  * tests that ensure runtime loads only compiled artifact, not live tables  
* **Security**  
  * try to inject columns via API/UI and assert system rejects without RAG-generated wrapper

---

