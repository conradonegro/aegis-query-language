# Bug log

**Status:** rolling record. Every bug fixed in Aegis, in order, with commit
refs. Useful for "have we seen this before?" and for spotting recurring
classes of defect.

Migrated from memory on 2026-07-30. All fixes are on branch `main` of
https://github.com/conradonegro/aegis-query-language.git
## BUG-002 — `_session_store` AttributeError in tests
**Commit:** d6f3ef0 (pre-session)
Tests accessed `compiler_engine._session_store` but the attribute is `session_store` (public) with internal dict `_local`. Fixed by changing all 6 occurrences to `compiler_engine.session_store._local`.

## BUG-003 — `PromptEnvelope` constructed with invalid fields in tests
**Commit:** d6f3ef0 (pre-session)
`test_ollama.py` and `test_prompting_gateway.py` passed `schema_context` and `hints` fields that don't exist on `PromptEnvelope`. Fixed by removing the spurious fields.

## BUG-004 — LLM refusal detection broken for all real providers
**Commit:** d6f3ef0 (pre-session)
All 5 real gateways pre-extracted `parsed["sql"]`, discarding the refusal signal. Fixed by having all gateways return the full raw JSON string as `LLMResult.raw_text`. The engine's existing `LLMQueryResponse` validator handles structural validation.
**Files:** `app/compiler/ollama.py`, `app/compiler/openai_gateway.py`, `app/compiler/anthropic_gateway.py`, `app/compiler/google_gateway.py`, `app/compiler/xai_gateway.py`

## BUG-005 — `MockLLMGateway` returned JSON-wrapped SQL; real gateways returned plain SQL
**Commit:** d6f3ef0 (pre-session)
Mock returned `json.dumps({"sql": ...})` while real gateways returned plain SQL. Fixed as a consequence of BUG-004: mock now returns plain SQL consistent with real gateways.
**File:** `app/compiler/gateway.py`

## TEST FIX — `test_where_only_aggregates_moves_to_having_and_removes_where` tested wrong scenario
**Commit:** 8e94fc3
Test used a query that already had HAVING and no WHERE, bypassing the repair function entirely. Fixed by using `WHERE SUM(id) > 0` (only aggregate predicates) which actually triggers the repair: WHERE removed, condition promoted to HAVING, translation_repair emitted.
**File:** `tests/test_translator_repairs.py`

## BUG-006 — `SELECT FOR UPDATE` used against SQLite in tests
**Commit:** 6d04554
`/generate` and `/execute` handlers used `.with_for_update()` on SQLAlchemy queries. SQLite (used in tests) silently drops FOR UPDATE. Fixed by replacing DB-level row lock with a per-session `asyncio.Lock` stored in a module-level `defaultdict`. Lock wraps the seq-read + message write + commit section.
**File:** `app/api/router.py`

## BUG-008 — `datetime.utcnow` deprecated in Python 3.12
**Commit:** 8602f38
Six `default=datetime.utcnow` column defaults in ORM models produced 36 DeprecationWarnings per test run. Fixed by replacing all with `default=lambda: datetime.now(timezone.utc)`. Test output went from 37 warnings to 1 (remaining is aiosqlite internals, not fixable).
**File:** `app/api/meta_models.py`

## BUG-007 + IMP-015 — `ExecutionContext(metadata={})` passes non-existent field
**Commit:** 9ee80ab
`/execute` handler passed `metadata={}` to `ExecutionContext` which has no such field. Pydantic silently discarded it. Fixed by:
1. Adding `model_config = ConfigDict(extra="forbid")` to `ExecutionContext`, `UserIntent`, `PromptEnvelope`, `ExecutableQuery` — immediately surfaced the bug as a ValidationError
2. Removing `metadata={}` from `ExecutionContext(...)` in router.py
**Files:** `app/execution/models.py`, `app/compiler/models.py`, `app/api/router.py`

## BUG-011 — `TranslatorProtocol.translate` signature too narrow
**Commit:** 8569d93
Protocol declared only `(ast, schema)` but concrete implementation accepts 4 additional keyword args: `abstract_query_hash`, `safety_version`, `row_limit`, `relationships`. Fixed by updating the protocol signature to match, also importing `AbstractRelationshipDef` in interfaces.py.
**File:** `app/compiler/interfaces.py`

## MYPY FIX — 4 semantic type errors in vault.py and main.py
**Commit:** 7f96aa5
- `vault.py:103`: `_auth_cache` dict had `None | int` type for `expires_at`, causing unsafe float comparison. Fixed by replacing opaque dict with typed `_auth_token: str` and `_auth_expires_at: float` instance attributes.
- `vault.py:207`: `not all([...])` didn't narrow `str | None` to `str`. Changed to `not (a and b and c)`.
- `main.py:112`: `load_active_schema()` returns `RegistrySchema | None` assigned to `RegistrySchema` variable. Fixed with local `_loaded` variable and explicit None branch.
- `main.py:137`: `MockLLMGateway` assigned to `OllamaLLMGateway`-typed variable. Fixed by annotating as `LLMGatewayProtocol`.
**Files:** `app/vault.py`, `app/main.py`

## MYPY FIX — hvac import-untyped warnings + no-any-return in vault.py
**Commit:** 90f1aaa
hvac has no published type stubs. Added `# type: ignore[import-untyped]` on both hvac import lines. Also added `str()` casts on `_get_cached_secret` return sites where `dict[str, Any]` values were returned as declared `str`.
**Files:** `app/vault.py`, `tests/test_vault.py`

## BUG-009 — Redis URL with credentials logged in plaintext
**Commit:** ac742a5
`logger.info(f"Session store: Redis ({redis_url})")` logged full URL including password. Fixed by parsing with `urlparse`, replacing password with `***`, reconstructing with `urlunparse`.
**File:** `app/main.py`

## BUG-010 + REFACTOR — Gateways inherited from Protocol; massive code duplication
**Commit:** 61bf8b6
All 4 remote gateways (OpenAI, Anthropic, Google, xAI) had ~80 lines of identical boilerplate each (imports, httpx call, error handling, JSON validation, LLMResult construction). Also all inherited from `LLMGatewayProtocol` (wrong — concrete classes satisfy protocols structurally).

Fixed by:
1. Creating `app/compiler/base_gateway.py` with `RemoteLLMGateway(ABC)` (shared generate() with HTTP transport, validation, result construction) and `OpenAICompatibleGateway(RemoteLLMGateway)` (shared message format for OpenAI-compatible APIs).
2. Reducing openai_gateway.py to 16 lines, xai_gateway.py to 16 lines (just _provider_name + _endpoint_url).
3. Removing `(LLMGatewayProtocol)` from OllamaLLMGateway (MockLLMGateway was already correct).
4. Updating all test patch targets from per-module to `app.compiler.base_gateway.get_secrets_manager`.
Net: -401 lines, +283 lines across gateway files.
**Files:** `app/compiler/base_gateway.py` (new), all 4 remote gateway files, `app/compiler/ollama.py`, `tests/test_gateways.py`, `tests/test_llm_factory.py`

## BUG-012 — Session management duplicated in /generate and /execute
Extracted `_resolve_session()` helper in `app/api/router.py` eliminating ~25 lines of duplication.

## IMP-003 — httpx.AsyncClient created per LLM call
Module-level `_http_client` shared across all calls in `app/compiler/ollama.py` and `app/compiler/base_gateway.py`.

## CODE QUALITY — C901 complexity, line-length, mypy strict
- All `# noqa: C901` removed; complex functions properly refactored:
  - `translate()` split into 11 helper methods in `translator.py`
  - `filter_schema()` / `is_follow_up()` split into 5 helpers in `filter.py`
  - `compile()` split into `_init_explain_context`, `_apply_rag_hints`, `_parse_llm_response` in `engine.py`
  - `lifespan()` split into `_build_test_registry_schema`, `_warm_rag_store` in `main.py`
- `line-length` reverted from 120 to 88; 161+ E501 violations fixed across all files
- Mypy overrides for `tests.*` removed; 163 mypy errors fixed properly with type annotations
- mypy strict passes clean across all 84 source files

## BUG-008 (incomplete fix) — DateTime(timezone=False) mismatch with datetime.now(UTC)
**Commit:** 705b090
The original BUG-008 fix changed defaults to `datetime.now(UTC)` (timezone-aware) but left all `DateTime` column types as `DateTime` (PostgreSQL `TIMESTAMP WITHOUT TIME ZONE`). asyncpg raises `can't subtract offset-naive and offset-aware datetimes` when binding an aware datetime to a naive timestamp column. Tests missed this because aiosqlite silently accepts any datetime regardless of timezone awareness.
Fixed by:
1. Changing all 7 `DateTime` columns in `app/api/meta_models.py` to `DateTime(timezone=True)` (TIMESTAMPTZ)
2. Adding migration `a3b8f2c91d04` to ALTER existing columns to TIMESTAMPTZ (safe: PostgreSQL reinterprets stored values as UTC)
**File:** `app/api/meta_models.py`, `backend_migrations/versions/a3b8f2c91d04_convert_timestamp_to_timestamptz.py`
