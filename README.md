# Aegis Query Language

Aegis is a secure, abstract, semantic query layer designed to mediate between Large Language Models (LLMs) and physical data endpoints (like PostgreSQL).

It enforces strict isolation boundaries, parameterization, and AST-level safety checks so that AI agents can query your databases safely without ever knowing the physical schema or being able to run destructive commands.

## Architecture 

The project strictly follows a code-first, bounded-context architecture:

1. **`app.steward`**: The source of truth for schema definitions and safety policies. This module defines the `RegistrySchema` which acts as the abstract mapping dictionary.
2. **`app.compiler`**: The engine that receives Natural Language Intents, maps them to the abstract schema, prompts the LLM for a logical query, parses the logical query into a deterministic AST (Abstract Syntax Tree), applies safety verifications against the steward rules, and translates it into a physical SQL dialect (e.g., PostgreSQL).
3. **`app.execution`**: The *only* layer permitted to open database connections. It receives parameterized `ExecutableQuery` objects from the compiler, enforces strict timeouts, and executes the queries contextually against the physical DB.
4. **`app.audit`**: A telemetry sink for asynchronous JSON logging of query intents and provenance, completely decoupled from the execution flow.

*(Read our [`docs/ARCHITECTURE_FROZEN.md`](docs/ARCHITECTURE_FROZEN.md) for full invariant mapping and rules).*

## Development Setup

We use `uv` for lightning fast Python virtual environments and dependency management. We use `import-linter` to cryptographically enforce the strict boundaries between our architectural contexts.

1. **Install uv**:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. **Sync Dependencies**:
```bash
uv sync
```

3. **Run Code Enforcement Linters**:
```bash
uv run lint-imports   # Verify semantic module boundaries
uv run ruff check .   # Basic code style
uv run mypy .         # Static Type Check
```

4. **Run Pytest Suite**:
```bash
uv run pytest -v
```
