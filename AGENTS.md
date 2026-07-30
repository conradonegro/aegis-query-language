# Repository Guidelines

## Project Structure & Module Organization
`app/` contains the runtime code. Key packages are `app/api` (FastAPI routes and auth), `app/compiler` (NL-to-SQL pipeline), `app/execution` (database access), `app/steward` (registry loading), `app/rag` (semantic hint store), and `app/audit` (telemetry). Keep new code inside the matching layer instead of creating cross-cutting helpers in arbitrary modules.

`tests/` mirrors the application surface with `test_<feature>.py` files. `scripts/` holds operational utilities such as metadata discovery and key creation. `backend_migrations/` contains Alembic migrations. `static/` serves the Steward UI assets, and `docker/` plus `docker-compose*.yml` define local environments.

## Build, Test, and Development Commands
Use `uv` for local development:

- `uv sync` installs runtime and dev dependencies.
- `uv run uvicorn app.main:app --reload` starts the API against your local config.
- `uv run pytest -v` runs the test suite.
- `uv run ruff check .` enforces linting and import sorting.
- `uv run mypy .` runs strict type checking.
- `uv run lint-imports` verifies architectural boundaries from `.importlinter`.
- `docker compose up --build` starts the full local stack with Postgres.

## Coding Style & Naming Conventions
Target Python 3.12, 4-space indentation, and a maximum line length of 88 characters. Ruff handles formatting-related lint rules; mypy is configured in strict mode, so add explicit types to public functions and new complex locals when needed.

Use `snake_case` for modules, functions, variables, and test files. Use `PascalCase` for classes and Pydantic models. Follow existing package boundaries: for example, `app.compiler` must not import `app.execution`, and `app.rag` must not import `app.api`.

## Testing Guidelines
Tests use `pytest` and `pytest-asyncio`. Add tests beside the closest existing file, for example `tests/test_executor.py` for execution changes. Prefer narrow unit tests first, then extend integration coverage for API, auth, or safety-policy changes. No coverage threshold is declared, but behavior changes should ship with regression tests.

## Commit & Pull Request Guidelines
Recent history uses Conventional Commits with scopes, such as `fix(config): ...` and `feat(benchmark): ...`. Keep subjects imperative and scoped to the affected area. If you use internal tracking IDs like `BUG-1`, keep them concise and meaningful.

Pull requests should explain the user-visible effect, note config or migration changes, and list the validation you ran (`pytest`, `ruff`, `mypy`, `lint-imports`). Include screenshots only for `static/` or Steward UI changes.
