# Database selection / `source_database` scoping — design decisions

**Status:** shipped. Kept as design reference — these are the rules to apply
when touching detection, scoping, or the schema filter.

Each rule records the decision *and why*, so a future reader can tell whether
it still applies.

## Detection and scoping

**1. Use MAX, not SUM, for auto-detection scoring.**
`db_scores[db] = max(db_scores.get(db, 0.0), best_for_table)` in
`_detect_source_database`.
*Why:* `token_match_score` is a bounded ratio in [0,1]. Summing across tables
scales with table count regardless of match quality, so 50 weak hits in a large
database beat 1 strong hit in a small one.

**2. Hard-fail on ambiguous auto-detection — never silently fall back to the
full schema.**
Detection returning `(None, [db_a, db_b])` raises `AmbiguousSourceDatabaseError`
→ HTTP 400 with a `candidate_databases` list.
*Why:* Silently using all 75 tables degrades LLM accuracy and returns HTTP 200
with plausible-looking garbage. The caller cannot distinguish a good result
from a degraded one without inspecting metadata.
Only fall back to the full schema on `(None, [])` — zero signal, not ambiguous
signal.

**3. Hard-fail on an unknown explicit `source_database`.**
`_apply_database_scope` returning an empty list raises
`UnknownSourceDatabaseError` → HTTP 400.
*Why:* Paying a commercial LLM to generate SQL against zero schema is wasteful
and always indicates a caller error — a typo or a stale value.

**4. Do NOT restrict relationship expansion across `source_database`
boundaries.** This step was deliberately dropped.
*Why:* BIRD databases are physically disjoint, so the restriction is redundant
there. In real enterprise schemas, cross-database FK edges represent explicitly
modelled join paths, and severing them breaks legitimate queries.

**5. Session continuity handles follow-up queries natively.**
`SessionQueryContext` caches the `FilteredSchema` from the first query;
follow-ups reuse it with no extra parameters and no re-detection. This is a
design strength, not a gap to close.

## API boundary

**6. Normalise `source_database` at the HTTP boundary** with a Pydantic
`field_validator(mode="before")` doing `.strip().lower()` on `QueryRequest`.
*Why:* The population script writes lowercase, but callers may send
`"Financial"` or `" financial "`. Strict equality would mis-fire on innocent
capitalisation. The constraint belongs at the boundary, not in the compiler.

**7. The Query Console UI must handle structured 400 bodies.**
There is no `source_database` input field and none should be added. On
ambiguity, render the candidates as clickable pills; a click sets a
module-scope `_pendingSourceDatabase`, re-invokes the existing submit function,
and injects the value into the payload. Clear it after use so later queries
don't silently inherit the scope.

## Performance

**8. `_tokenize` in `filter.py` is `@staticmethod` + `@functools.lru_cache(maxsize=8192)`.**
*Why `@staticmethod` is required:* `lru_cache` on an instance method keys on
`self`, making the cache per-instance and mostly useless. Removing `self` from
the key gives one process-wide cache, so each unique alias or description is
tokenised once across all instances and calls.

## Benchmark harness

**9. Use a fresh `session_id` per question.**
*Why:* Sharing a session means the cached `FilteredSchema` from question N
mis-scopes question N+1 whenever they target different databases. Generate a
new UUID per question and never reuse one.

## See also

FastAPI exception-handler gotchas that came out of this work are in
`CLAUDE.md` under "Code-level gotchas", since they apply repository-wide.
