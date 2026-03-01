# Aegis Query Language - Architecture Summary

This document outlines the complete set of components, interfaces, and domains implemented to construct the secure semantic middleware proxy.

## 1. Domain Models and Interfaces (`app.compiler` & `app.steward`)
The foundation of the architecture is built on strictly defined Pydantic domains and Python `typing.Protocol` interfaces. This "Code-First Contract" approach ensures that components like the `SafetyEngine`, `Translator`, and `LLMGateway` can be swapped or tested in isolation without breaking the system.

- **`UserIntent`**: The raw natural language input from the client.
- **`ExecutableQuery`**: The final, fully-parameterized SQL string and bindings authorized for database execution.
- **`RegistrySchema` & `AbstractIdentifierDef`**: The explicit mapping of abstract business concepts (e.g., "users") to physical database targets (e.g., "auth.users"), including column-level safety access controls.

## 2. Compilation Pipeline (`app.compiler`)
The core six-step engine that translates ambiguous natural language into secure, deterministic execution.

- **`DeterministicSchemaFilter`**: Uses substring matching to aggressively prune the `RegistrySchema` down to only the tables relevant to the `UserIntent`. Limits LLM hallucination scope and reduces token costs.
- **`PromptBuilder`**: Uses file-based `Jinja2` templates to construct a secure `PromptEnvelope`. It mathematically guarantees that physical database targets never leak into the LLM context window.
- **`MockLLMGateway`**: An asynchronous network simulator representing the connection to OpenAI/Anthropic.
- **`SQLParser`**: Uses `sqlglot` to parse the LLM's raw output string into a structured Abstract Syntax Tree (AST). Rejects malformed or multi-statement outputs immediately.
- **`SafetyEngine`**: The critical security boundary. Performs recursive traversal of the SQL AST, enforcing an explicit `ALLOW_LIST` (e.g., `Select`, `Where`) and blocking dangerous nodes (`Drop`, `Subquery`, `Command`).
- **`DeterministicTranslator`**: Performs a safe copy-on-write traversal of the AST, swapping abstract identifiers for physical targets. Injects `LIMIT 1000` bounds and extracts unsafe literals into parameterized bindings (`:p1`).
- **`CompilerEngine`**: The central facade orchestrating the execution of the 6 steps above.

## 3. Execution Layer (`app.execution`)
Translates the authorized `ExecutableQuery` into physical database results.

- **`ExecutionEngine`**: Manages asynchronous connection pooling (via `sqlalchemy` and `asyncpg` or `sqlite`). Enforces `SET LOCAL statement_timeout` immediately before execution to prevent analytical payloads from DOSing the database.
- **`ExecutionContext`**: Pydantic struct enforcing strict multi-tenant boundaries (`tenant_id`, `user_id`) during query execution.
- **`QueryResult`**: The strictly shaped `(columns, rows, metadata)` tuple returned to the API layer after successful database yields.

## 4. Telemetry and Audit (`app.audit`)
A lock-free, zero-blocking stream tracking all activity for security and analytics.

- **`QueryAuditEvent`**: Comprehensive Pydantic schema cataloging every query attempt, including latency metrics, token consumption, translation hashes, and abstract bounds.
- **`JSONAuditLogger`**: Asynchronously formats the `QueryAuditEvent` into structured JSON payloads and pipes them to the standard `logging` out-stream for ingestion by tools like Datadog or Splunk.

## 5. API Application Layer (`app.api` & `app.main`)
The FastAPI boundaries exposing the internal domain securely to external HTTP clients.

- **`main.py` Config**: Mounts the FastAPI application root. Employs `lifespan` state lifecycle hooks to initialize database engines and inject them into `app.state`.
- **Exception Handlers**: Custom `@app.exception_handler` decorators that trap domain errors (`SafetyViolationError`, `TranslationError`) and format them into stable, static HTTP 400/403 JSON payloads. Prevents internal Python stack traces from leaking to clients.
- **API Models**: Pydantic schemas (`QueryRequest`, `QueryExecuteResponse`) built specifically for the web layer, enforcing required fields independently of internal db models.
- **`POST /api/v1/query/generate`**: Endpoint for testing translation logic. Returns the parameterized SQL without touching the database.
- **`POST /api/v1/query/execute`**: Main endpoint executing the full pipeline. Returns JSON rows and dispatches the asynchronous audit logger gracefully via `BackgroundTasks`.

## 6. Testing Strategy (`tests/`)
100% test coverage across all boundaries.

- **Domain Validators**: Tests ensuring Pydantic models reject mutations entirely, dropping payloads with missing metadata or `tenant_id` blocks.
- **AST Fuzzing**: Parametrized Pytest matrices hurling malicious SQL string variants (`DROP TABLE`, UNION subqueries, etc.) against the `SafetyEngine` to certify blockage.
- **Contract Enforcement**: Mypy structural type checks asserting that concrete protocol implementations do not violate expected argument orders or missing kwargs.
- **Integration Coverage**: Fully mocked `TestClient` API tests validating FastAPI Request Dependency cascades, background tasks, routing, and masked HTTP 500 error boundaries intercept uncaught anomalies.
