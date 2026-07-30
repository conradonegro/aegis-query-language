# Schema hints hardening

**Status:** **shipped.** Verified in code on 2026-07-30 —
`app/compiler/hints.py`, `app/compiler/backend_hints.py`,
`tests/test_backend_hints.py`, `QueryRequestWithHints` (`app/api/models.py:47`)
and the `SCHEMA_HINTS` startup warning (`app/main.py:438`) all exist.

> Note: this was recorded as "READY TO IMPLEMENT (lower priority)" in memory
> long after it had shipped. Status lives here now, next to the code.

Original plan: `docs/schema_hints_fix_implementation_plan.md`.

## What it does

External caller-supplied hints reach the system prompt, so they are treated as
untrusted input:

- `validate_hints()` enforces **max 5 items, max 200 chars each**, and an
  **allowlist** regex `^[a-zA-Z0-9 '\.,:!\?_\-\/\(\)=%]+$`.
  *Why an allowlist, not a blocklist:* `<system>`, HTML comments and `---`
  markers all slip past blocklists. Enumerate what's permitted instead.
- External hints are accepted **only** when `SCHEMA_HINTS=on`, which logs a
  startup warning. Backend-generated hints always flow.
- Hint order is **backend → external (optional) → RAG** (the engine appends
  last).

## Decisions and gotchas

- **FastAPI locks type signatures at import time**, so the request model can't
  be swapped dynamically on a flag. Both endpoints always declare
  `QueryRequestWithHints`; the router simply ignores `schema_hints` when the
  flag is off. Consequence: OpenAPI always advertises the field. Accepted
  trade-off for a dev-mode flag.
- **Both request models are `frozen=True`.** The router only ever reads the
  payload. (Pydantic v2 allows a frozen subclass to add fields, so
  `QueryRequestWithHints` extending frozen `QueryRequest` is fine.)
- **`validate_hints` is imported *inside* the field validator** — avoids a
  circular import and defers the cost off the startup path.
- **`get_utc_now` is a FastAPI dependency**, so tests override it via
  `app.dependency_overrides` for deterministic assertions. No `freezegun`.
- **Assert hint presence with `any(...)`, never by index.** RAG appends hints,
  so positions shift.

## Later change worth knowing

The backend hint was originally specified as a full timestamp
(`Current date/time (UTC): 2026-03-18T12:34:56Z`). It is now **day
granularity** — `Current date (UTC): YYYY-MM-DD` (`backend_hints.py:25`).

*Why:* sub-day precision serves no SQL-generation purpose, and a per-second
timestamp makes every system prompt unique, which defeats both provider prompt
caches and the benchmark's prompt-keyed replay cache.

That hint is still the sole reason BIRD replay artifacts expire at UTC
midnight — see item **E6** in [bird-benchmark.md](bird-benchmark.md), where the
decision is to remove it entirely and let the model use `CURRENT_DATE`/`NOW()`.
