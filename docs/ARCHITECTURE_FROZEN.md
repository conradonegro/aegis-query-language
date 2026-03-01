# Aegis Query Language - Frozen Architecture

**Date Frozen**: 2026-03-01
**Status**: APPROVED & LOCKED

This document serves as the single source of truth for the core architecture, bounded contexts, invariants, and out-of-scope items for the Aegis Query Language proxy system. Any deviation from these rules requires explicit architectural review and an update to this document.

---

## 1. System Goal
Aegis acts as a strict semantic translation and security proxy between unstructured LLMs, a Natural Language semantic layer, and physical execution targets (PostgreSQL). It compiles user intents into bounded SQL over a predefined abstract schema.

## 2. Bounded Context boundaries & Invariants

The proxy enforces strict bounded contexts across its modules. Internal implementations MUST NOT bleed across boundaries. Interactions happen exclusively through explicit DTOs and Protocols exposed via public package `__init__.py` files.

### `app.steward` (Schema & Policy Definition)
*   **Responsibility**: Defines the shape of the abstract schema, mapping logical aliases to physical database targets. Configures safety policies (e.g., column-level allowances).
*   **Invariant**: The only cross-context shared object is the immutable snapshot `RegistrySchema`.
*   **Constraint**: No other module may invoke the actual data-source loading mechanism of the Steward.

### `app.compiler` (AST, Prompting, LLM Gateway, Translation)
*   **Responsibility**: The heart of the semantic proxy. Validates inputs, filters the RegistrySchema, prepares prompts, contacts the LLM Gateway, parses the resulting text into an AST, validates it against safety rules, and translates it into an `ExecutableQuery`.
*   **Invariant**: `ExecutableQuery` output must contain NO abstract aliases. All physical targets must be fully resolved.
*   **Invariant**: The `PromptEnvelope` passed to the Gateway MUST NOT contain any physical database targets.
*   **Invariant**: AST modifications must use strong immutable copy-on-write tracking (`ValidatedAST`).
*   **Constraint**: The compiler NEVER connects to a database driver directly.

### `app.execution` (Physical Database Layer)
*   **Responsibility**: Safely executes raw PostgreSQL.
*   **Invariant**: **NO CODE OUTSIDE `app/execution` MAY OPEN A DB CONNECTION.**
*   **Invariant**: Must append mandatory execution context mitigations (`SET LOCAL statement_timeout`) and avoid ORM (SQLAlchemy Expression Builders). Must purely run raw string + parametrized tuples over AsyncPG engines.
*   **Hygiene**: One engine pool per service, explicitly bounded.

### `app.audit` (Telemetry & Observability)
*   **Responsibility**: Consumes `QueryAuditEvent` structs encapsulating full provenance (query latency, tokens, success, physical text, hashes) and logs them out of band.
*   **Invariant**: Audit logging mechanisms MUST NEVER raise exceptions that block the API response to the user.

## 3. Mandatory Security Tenets
1. **No In-Place AST Mutation**: AST rewrites must copy the tree to preserve provenance.
2. **Explicit Allow-Lists**: The LLM parsing AST step (`sqlglot`) explicitly denies DDL/DML, CTEs, Subqueries, Anonymity, and Functions not explicitly provided.
3. **No Direct User Querying**: Users NEVER write SQL. They write `UserIntent` (Natural Language).
4. **Parameterization Engine**: Lexical strings and numbers derived from the LLM are bound strictly to DB-driver-level parameters (e.g. `$1`), mitigating generation phase SQL string injection.
5. **No Schema Bleed**: The physical schema mappings are erased prior to Prompt generation (`FilteredSchema`).

## 4. Specifically Out-of-Scope Items (v1)
*   LLM fine-tuning or hosted RAG architectures.
*   Sub-queries or Complex CTEs inside generated ASTs.
*   Dynamic user-interactive Prompt engineering within the UI (All Prompts are static Jinja templates).
*   Execution engines beyond PostgreSQL (No Snowflake, BigQuery, Mongo).
*   Live schema synchronization (For V1, Steward schemas are statically declared in memory or file).
