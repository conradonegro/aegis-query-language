# 2026-04-07 Code Review

## 1. Follow-up sessions can bypass newly deployed schema and safety changes
- Severity: High
- Confidence: High
- Code location: `app/compiler/engine.py:93`, `app/compiler/engine.py:118`, `app/compiler/models.py:43`
- Why it matters: follow-up detection reuses `SessionQueryContext.last_filtered_schema` without checking that it still matches the currently loaded registry artifact. The cached schema carries old aliases, physical mappings, and safety flags.
- Failure scenario: an admin compiles a new registry that removes or reclassifies a sensitive column. A user with an existing `session_id` sends a follow-up query within the session TTL, and the compiler reuses the old filtered schema, allowing queries against data that should already be retired from the active registry.
- Recommended fix: store the loaded registry artifact hash or version in `SessionQueryContext` and only reuse prior schema when it matches the tenant’s current runtime registry. Invalidate cached session context on registry reload.
- Missing tests: a follow-up query after a registry compile/reload should rebuild schema context instead of reusing the old filtered schema.

## 2. Compiling a `pending_review` version mutates live runtime state
- Severity: High
- Confidence: High
- Code location: `app/api/compiler.py:77`, `app/api/router.py:859`, `app/api/router.py:868`, `app/api/router.py:872`, `app/api/router.py:886`
- Why it matters: `MetadataCompiler.compile_version()` accepts `pending_review`, but `compile_metadata_version()` immediately hot-reloads runtime state and publishes reload signals as if the compiled artifact were live.
- Failure scenario: tenant version `V1` is active and `V2` is still `pending_review`. Compiling `V2` sets the worker’s `loaded_artifact_hashes` to `V2`, reloads the currently active schema (`V1`), and may install a `V2` RAG index. The worker now serves a mismatched schema/hash/index combination until another reload corrects it.
- Recommended fix: either require `version_obj.status == "active"` before any runtime mutation, or split “compile preview artifact” from “deploy active artifact” into separate endpoints. Preview compiles must not publish reloads or touch runtime state.
- Missing tests: compiling a `pending_review` version should either fail cleanly or leave runtime schema/hash/index untouched.

## 3. Failed RAG rebuilds are marked as successful and stop retrying
- Severity: High
- Confidence: High
- Code location: `app/reload.py:154`, `app/reload.py:182`, `app/reload.py:188`, `app/api/router.py:871`, `app/api/router.py:872`, `app/api/router.py:886`
- Why it matters: both reload paths advance `loaded_artifact_hashes` before the new RAG index is built successfully. Hash equality is then used as the “already loaded” check, so later poll cycles will skip the tenant even though the vector store is stale or missing.
- Failure scenario: a compile succeeds but `build_from_artifact()` raises `RagDivergenceError`. The schema moves to the new artifact, the old vector store stays in memory, and future reload polls do nothing because the loaded hash already matches the DB hash.
- Recommended fix: track schema load state and RAG load state separately, or only advance the “loaded” hash after both schema and vector store swap successfully. If RAG fails, leave the tenant in a retryable degraded state.
- Missing tests: force `build_from_artifact()` to fail during both `compile_metadata_version()` and `_perform_reload()` and verify the worker retries instead of declaring the tenant up to date.

## 4. The WORM audit chain can fork under concurrent admin writes
- Severity: High
- Confidence: High
- Code location: `app/api/router.py:690`, `app/api/router.py:746`, `app/api/compiler.py:208`, `app/api/compiler.py:227`
- Why it matters: audit appenders read the latest row hash and compute the next row without any lock or serializing constraint on the chain head. Two concurrent writers can legally append different rows with the same `previous_hash`.
- Failure scenario: one request activates a version while another compiles a version. Both transactions read the same chain tip, compute different `row_hash` values, and commit. The result is a branched audit history even though each row individually looks valid.
- Recommended fix: serialize audit appends with a `SELECT ... FOR UPDATE` chain-head row, a dedicated single-row pointer table, or serializable transactions plus retry. A uniqueness rule on non-genesis `previous_hash` would also catch forks.
- Missing tests: concurrent status transition and compile operations should either serialize or force one side to retry; they should never leave two rows pointing at the same `previous_hash`.

## 5. Session history is shared across users within the same tenant
- Severity: Medium
- Confidence: High
- Code location: `app/api/router.py:169`, `app/api/router.py:191`, `app/api/router.py:217`
- Why it matters: `_resolve_session()` scopes an existing session by `tenant_id` only, even though `ChatSession` stores `user_id`. Any user in the tenant who knows a session UUID can load that history and continue the conversation.
- Failure scenario: a client logs `session_id` values or exposes them in browser storage. Another user with a valid key for the same tenant reuses that `session_id` and receives follow-up answers influenced by someone else’s prompts and assistant responses.
- Recommended fix: include `ChatSession.user_id == user_id` in the lookup and reject mismatches. If cross-user sharing is ever required, make it an explicit feature with a separate authorization model.
- Missing tests: same-tenant, different-user requests should not be able to resolve or append to another user’s chat session.

## 6. The app leaks SQLAlchemy async engines on every lifespan cycle
- Severity: Medium
- Confidence: High
- Code location: `app/main.py:345`, `app/main.py:349`, `app/main.py:352`, `app/main.py:355`, `app/main.py:433`
- Why it matters: four async engines are created inline inside session factories and never disposed on shutdown. Only the execution engine is closed.
- Failure scenario: repeated `TestClient` runs, worker reloads, or local dev restarts accumulate open DB pools and file descriptors until SQLite/Postgres starts refusing new connections.
- Recommended fix: keep explicit engine objects on `app.state` and `await engine.dispose()` for each one during shutdown.
- Missing tests: repeated startup/shutdown cycles should not increase the number of open database connections.

## 7. Cloning a metadata version silently drops sample values and curated RAG values
- Severity: Medium
- Confidence: High
- Code location: `app/api/router.py:1477`, `app/api/router.py:1514`, `app/api/meta_models.py:137`, `app/api/meta_models.py:181`
- Why it matters: the baseline clone loads tables, columns, and edges, but not `MetadataColumn.values`, and the clone loop never copies `sample_values`. A new draft created from a working version loses prompt enrichment and RAG seed data immediately.
- Failure scenario: a steward clones the active version to make a small alias change. The resulting draft compiles with empty curated values and no sample hints, causing query quality to regress for unrelated columns.
- Recommended fix: eager-load `MetadataColumn.values` during baseline fetch and copy active values plus `sample_values` into the new version.
- Missing tests: cloning a baseline version should preserve active `MetadataColumnValue` rows and `sample_values`.

## 8. Commit-time infrastructure failures are reported as duplicate conflicts
- Severity: Medium
- Confidence: High
- Code location: `app/api/router.py:996`, `app/api/router.py:999`, `app/api/router.py:1664`, `app/api/router.py:1667`
- Why it matters: both `create_column_value()` and `create_credential()` catch every exception from `commit()` and rewrite it to HTTP 409. That hides outages, permission problems, and transaction errors behind a false “already exists” response.
- Failure scenario: the database restarts between `session.add()` and `session.commit()`. The API returns a conflict instead of surfacing an operational failure, so clients retry the wrong way and operators lose the real signal.
- Recommended fix: catch `IntegrityError` for duplicate-key conflicts and let other SQLAlchemy exceptions propagate as 500/503 responses with appropriate logging.
- Missing tests: simulate `OperationalError` during commit and assert the endpoint does not return 409.
