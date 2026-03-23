# Schema Hints Hardening — Implementation Plan

## Goal

Eliminate `schema_hints` as an external prompt-injection surface while preserving
the ability to inject trusted, server-generated context that improves LLM accuracy.
The system prompt remains closed to external input; trusted internal context is
injected explicitly by a dedicated compiler-adjacent module.

---

## Background and Decision

The agreed architecture:

- **External `schema_hints`** (user/caller-supplied): off by default, opt-in via
  `SCHEMA_HINTS=on` env flag (operator acknowledges the trust tradeoff).
- **Backend hints** (server-generated): always on, built by a new
  `BackendHintBuilder` in `app/compiler/backend_hints.py`, called by the router
  with explicit context before `compile()`.
- **No new infrastructure**: no HMAC signing, no `hints` scope, no structured DSL,
  no DB views. These remain valid future improvements but are out of scope.

The `PromptEnvelope` / `PromptHints` data model is unchanged. The template is
unchanged. All changes are in how `PromptHints.column_hints` is populated.

---

## Invariants — No Breaches

Verified against CLAUDE.md and codebase:

| Invariant | Status |
|---|---|
| `PromptEnvelope` never contains physical DB targets | Unaffected — hints are NL context, not schema targets |
| `ExecutableQuery` never contains abstract aliases | Unaffected — hint changes happen before LLM call |
| `ValidatedAST` immutable copy-on-write | Unaffected — no AST changes |
| LLM output literals bound as `$1` parameters | Unaffected — hints are LLM input, not output |

Documentation fix also required: `prompting.py` docstrings overclaim "immutable"
for the rendered content. The accurate guarantee is that the *template structure*
is static and the rendered envelope is frozen after construction — not that content
is externally uninfluenced. Updated language: *"All dynamic content is injected by
trusted internal pipeline stages only — never from raw external input."*

---

## Step 1 — Create `app/compiler/hints.py` (shared validator)

**Files:** `app/compiler/hints.py` (new)

Neutral module — no parent dependency — so both `backend_hints.py` and
`api/models.py` can import from it without layering issues.

```python
def validate_hints(hints: list[str]) -> list[str]:
    """Raises ValueError on any malformed hint. Returns hints unchanged if valid."""
```

Rules:
- Max 5 hints per request.
- Max 200 characters per hint.
- **Character allowlist** (not blacklist): `^[a-zA-Z0-9 '\.,:!\?_\-\/\(\)=%]+$`

The allowlist approach is intentional. A blacklist (reject newlines, backticks,
XML tags) is inherently incomplete — attackers use `<system>`, HTML comments
(`<!--`), and structural markers (`---`, `===`) to trick LLMs into starting a new
context block. A whitelist that permits only the characters needed for legitimate
business hints (`()` for qualifiers like "FY2026 (Apr–Mar)", `=` for filters like
"status=active", `%` for thresholds like "rate=95%", `/` for dates) cannot be
bypassed by novel structural characters. Future editors must not loosen this to a
blacklist.

---

## Step 2 — Create `app/compiler/backend_hints.py`

**Files:** `app/compiler/backend_hints.py` (new)

A `BackendHintContext` dataclass holds everything the router knows that the engine
cannot:

```python
@dataclass
class BackendHintContext:
    tenant_id: str
    now: datetime          # UTC, supplied by router at request time
    timezone: str = "UTC"  # future: per-tenant config
```

`build_backend_hints(ctx: BackendHintContext) -> list[str]` returns a validated
list of hint strings. Initial implementation produces one hint:

```
"Current date/time (UTC): 2026-03-18T12:34:56Z"
```

Datetime format: `ctx.now.strftime("%Y-%m-%dT%H:%M:%SZ")` — explicit RFC3339 with
`Z` suffix (not `+00:00`) so test assertions can match an exact string shape.
Caller is responsible for passing a UTC-aware datetime; the function does not
convert timezones.

Framed as orientation context, not a rule. Future additions (fiscal year, tenant
soft definitions) extend this function only — no other files change.

All generated hints are passed through `validate_hints()` from `hints.py` before
being returned — safety net against accidental misconfiguration.

---

## Step 3 — Request Model and `SCHEMA_HINTS` Flag

**Files:** `app/api/models.py`, `app/main.py`, `app/api/router.py`

### Why not a dynamic model swap

FastAPI evaluates endpoint type signatures exactly once at Python import time to
build the OpenAPI schema and Pydantic validators. A runtime swap
(`PayloadModel = QueryRequestWithHints if settings... else QueryRequest`) does not
work — the router locks to the type present at startup, and test fixtures that
toggle the env var mid-suite will not be reflected.

### Model hierarchy

```python
class QueryRequest(BaseModel):
    """Public model — no schema_hints."""
    model_config = ConfigDict(frozen=True)
    intent: str
    explain: bool = False
    session_id: str | None = None
    provider_id: str | None = None
    source_database: str | None = None

class QueryRequestWithHints(QueryRequest):
    """Extends QueryRequest with optional external hints. frozen=True inherited."""
    schema_hints: list[str] = Field(default_factory=list)

    @field_validator("schema_hints", mode="before")
    @classmethod
    def _validate_hints(cls, v: list[str]) -> list[str]:
        from app.compiler.hints import validate_hints  # local import: avoids
        return validate_hints(v)                        # circular import and
                                                        # defers startup cost
```

`QueryRequestWithHints` inherits `frozen=True` from `QueryRequest`. Pydantic v2
allows a subclass to add fields without requiring mutability — no `frozen=False`
override needed. The router only *reads* `payload.schema_hints`; it never mutates
the model.

### Deliberate compromise on OpenAPI visibility

Both endpoints always declare `payload: QueryRequestWithHints`. This means:

- External callers **can** submit `schema_hints` even when `SCHEMA_HINTS=off`.
- When the flag is off, submitted hints are silently ignored by the router.
- The `schema_hints` field **always appears in the OpenAPI schema**, regardless of
  the flag.

This is an accepted tradeoff for a dev-mode feature flag. The alternative — two
`APIRouter` instances conditionally included in `main.py` — gives accurate OpenAPI
docs but adds routing complexity. That path remains available if OpenAPI accuracy
becomes a requirement; it is explicitly deferred, not forgotten.

### Flag and startup warning

Add `SCHEMA_HINTS` to app settings (default `False`). In `app/main.py` lifespan:

```python
if settings.schema_hints_enabled:
    logger.warning(
        "[!] SCHEMA_HINTS=on: external caller hints accepted in system prompt. "
        "Ensure callers are trusted internal services only."
    )
```

---

## Step 4 — Clock Injection via FastAPI Dependency

**Files:** `app/api/router.py` (or `app/api/deps.py`)

Hardcoding `datetime.now()` inside the endpoint makes any test that asserts
against `PromptHints` content or compiled context inherently flaky — the timestamp
changes every millisecond.

Fix: injectable clock dependency.

```python
def get_utc_now() -> datetime:
    return datetime.now(timezone.utc)
```

Endpoint signature:

```python
async def generate_query(
    ...,
    now: Annotated[datetime, Depends(get_utc_now)],
):
    ctx = BackendHintContext(tenant_id=cred.tenant_id, now=now)
```

Tests override the dependency in `conftest.py`:

```python
FROZEN_DT = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
app.dependency_overrides[get_utc_now] = lambda: FROZEN_DT
```

No `freezegun` required.

---

## Step 5 — Wire `BackendHintBuilder` into Router

**Files:** `app/api/router.py`

Both `/generate` and `/execute` endpoints:

```python
from app.compiler.backend_hints import BackendHintContext, build_backend_hints

ctx = BackendHintContext(tenant_id=cred.tenant_id, now=now)
hints = PromptHints(column_hints=build_backend_hints(ctx))

if settings.schema_hints_enabled:
    hints.column_hints.extend(payload.schema_hints)
```

**Hint ordering:** backend → external (optional) → RAG (appended inside engine).

Note: `column_hints` is a flat `list[str]` with no provenance metadata. If future
debugging or explainability needs to attribute which hint influenced the query, the
list would need to become a list of tagged objects. Tracked as a future item.

---

## Step 6 — Fix `prompting.py` and `gateway.py` Docstrings

**Files:** `app/compiler/prompting.py`, `app/compiler/gateway.py`

| Location | Current | Replacement |
|---|---|---|
| `PromptBuilder` class docstring | `"secure, immutable PromptEnvelope"` | `"PromptEnvelope from a fixed template. All dynamic content injected by trusted internal pipeline stages only — never from raw external input."` |
| `build_prompt` method docstring | `"Renders the static template into an immutable PromptEnvelope"` | `"Renders the fixed template into a frozen PromptEnvelope. Template structure is static; content is supplied by trusted internal sources."` |
| Inline comment line 39 | `"# 1. Load the immutable static system instruction template"` | `"# 1. Load the fixed system instruction template (structure is static)"` |
| `MockLLMGateway.generate` docstring | `"Takes the immutable PromptEnvelope"` | `"Takes the frozen PromptEnvelope"` |

---

## Step 7 — Tests

**Files:** `tests/test_backend_hints.py` (new), existing router/engine tests updated

Test assertions on hint presence must use `any(h for h in hints if "..." in h)`,
not index-based access — RAG hints may append during the pipeline and shift
positions.

1. `test_backend_hints_contains_datetime` — output contains a string matching
   `"Current date/time (UTC): 2026-01-15T12:00:00Z"` (frozen clock via dependency
   override). Assert with `any()`, not index.
2. `test_backend_hints_pass_validator` — `build_backend_hints()` output never
   raises when passed through `validate_hints()`.
3. `test_hint_validator_rejects_newline` — hint containing `\n` raises
   `ValueError`.
4. `test_hint_validator_rejects_overlong` — hint > 200 chars raises `ValueError`.
5. `test_hint_validator_rejects_too_many` — list of 6 hints raises `ValueError`.
6. `test_hint_validator_rejects_xml_tag` — hint containing `<system>` raises
   `ValueError` (character allowlist catches `<` and `>`).
7. `test_schema_hints_not_forwarded_when_flag_off` — post a request with
   `schema_hints=["injected hint"]` when `SCHEMA_HINTS=off`; assert that no hint
   containing `"injected hint"` appears anywhere in `PromptHints.column_hints`
   (via `explain=True` or mock on `build_prompt`). Assert with `any()` / `all()`,
   not by index.
8. `test_schema_hints_validated_when_flag_on` — hint containing `\n` returns 422
   when `SCHEMA_HINTS=on`.
9. `test_backend_hints_always_present` — `PromptHints.column_hints` contains the
   datetime hint regardless of `SCHEMA_HINTS` value. Assert with `any()`.

---

## Deliverables Checklist

- [ ] `app/compiler/hints.py` — shared `validate_hints()` with character allowlist
- [ ] `app/compiler/backend_hints.py` — `BackendHintContext`, `build_backend_hints()`
- [ ] `app/api/models.py` — `QueryRequest` + `QueryRequestWithHints` subclass (both `frozen=True`)
- [ ] `app/main.py` — `SCHEMA_HINTS` setting, startup warning
- [ ] `app/api/router.py` — `get_utc_now` dependency, backend hints built and prepended, external hints conditional
- [ ] `app/compiler/prompting.py` + `gateway.py` — docstrings corrected
- [ ] `tests/test_backend_hints.py` — all 9 test cases passing

---

## Out of Scope (future)

- Two `APIRouter` instances for accurate OpenAPI docs when `SCHEMA_HINTS=off`
- HMAC-signed hint payloads
- Dedicated `scope=hints` credential type
- Structured hard-constraint DSL enforced in the Translator
- Per-tenant soft definitions stored in registry metadata
- DB views or virtual columns for business definitions
- Hint provenance metadata (`list[TaggedHint]` replacing `list[str]`)
