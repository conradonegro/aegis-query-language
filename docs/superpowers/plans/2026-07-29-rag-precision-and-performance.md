# RAG Precision and Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the RAG value-hint channel fast and precise — eliminate a fuzzy-match path that burns ~98% of scan time producing zero matches, scope value lookup to the detected source database, and replace the blanket distinct-count enablement rule with one based on intrinsic value shape.

**Architecture:** Six independent commits, ordered so each is measurable on its own. The RAG store keeps its linear scan; the cost problem is solved by not calling `difflib.ratio()` when difflib's own upper bounds prove it cannot clear the threshold. Database scoping is added as an optional filter on the store (default `None` = today's behaviour), then wired in the compiler by hoisting source-database resolution above the RAG step — safe because detection depends only on the intent and schema, never on RAG output.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2.x (async), pydantic v2, pytest, `difflib`, PostgreSQL (`asyncpg`), SQLite + `aiosqlite` in tests.

## Global Constraints

- **TDD is mandatory.** Every task writes a failing test first, watches it fail, then implements.
- **Validation suite must be green before every commit:** `uv run pytest`, `uv run ruff check .`, `uv run mypy .` (strict), `uv run lint-imports`.
- **Never suppress lint/mypy** with `noqa`, `type: ignore`, or config overrides. Fix the code instead.
- **No new Alembic migrations.** If a schema change is needed, edit `alembic/versions/0001_initial_schema.py` directly — the database is dropped and recreated freely.
- **Benchmark integrity rule (firm):** no per-question tuning, no edited column descriptions, no injected FK relationships. Every change must generalise across all 13 BIRD databases. Enablement rules must be derivable from intrinsic column/value properties — never from BIRD gold SQL.
- **import-linter contract `rag_no_upstream`:** `app.rag` must not import `app.compiler`, `app.execution`, or `app.api`. Database scoping crosses this boundary as a plain `str`, never a compiler type.
- **Invariant:** `PromptEnvelope` must never contain physical DB targets. RAG hints use `abstract_column` only.
- One commit per task. Benchmark model is haiku only.

## Measurement checkpoints

Tasks 1, 2 and 3 do **not** change prompt content, so they need no benchmark regeneration. Tasks 4, 5 and 6 each change the hints rendered into the system prompt, which changes every replay key — `benchmarks/responses.jsonl` becomes invalid and requires a **full 500-question regeneration** (~45 min, subject to the subscription usage window). Land 4–6 as a batch and measure once. Baseline to beat: **52.2% (261/500)**, run `20260729-045019-24ebc8f`.

## File Structure

| File | Responsibility | Tasks |
|---|---|---|
| `app/rag/store.py` | scoring + search; the linear scan | 1, 3, 5 |
| `app/rag/models.py` | `CategoricalValue` | 3 |
| `app/rag/builder.py` | artifact → index; eligibility at index time | 3, 6 |
| `app/api/router.py` | `_run_strategy_refresh` value materialisation | 2 |
| `app/compiler/filter.py` | source-database resolution | 4 |
| `app/compiler/engine.py` | pipeline order | 4 |
| `scripts/discover_metadata.py` | `_rag_config` enablement rule | 6 |
| `tests/test_rag.py` | store scoring/search behaviour | 1, 3, 5 |
| `tests/test_rag_builder.py` | index build eligibility | 3, 6 |
| `tests/test_metadata_value_purge.py` (new) | sensitive-value purge | 2 |
| `tests/test_filter.py` | DB resolution | 4 |
| `tests/test_compiler_engine.py` | pipeline order | 4 |
| `tests/test_discover_rag_config.py` | enablement rule | 6 |

---

### Task 1: difflib upper-bound precheck and matcher reuse

The fuzzy fallback calls `difflib.SequenceMatcher(None, val, query_full).ratio()` for every value sharing a word with the query. Measured on the real index: **547,998 calls across 8 BIRD queries produced 0 matches**, and it is ~98% of scan time (bare loop over 1M values is 317 ms; with difflib it is 12–26 s).

This is provable, not incidental. `ratio() = 2M/T` with `M ≤ len(val)`, so clearing 0.85 requires `len(val) ≥ 0.739 × len(query)`. BIRD queries run 177–435 chars; the longest indexed value is 300. `real_quick_ratio()` is exactly `2·min(la,lb)/(la+lb)` and `quick_ratio()` is a character-multiset bound — both are guaranteed `>= ratio()`, so skipping when either falls below the caller's threshold cannot change which values clear it.

The fallback stays live for **short** queries (`"nvidia korp"` still fuzzy-matches `"Nvidia Corp"` at 0.909), so this is an optimisation, not a deletion.

Also: `SequenceMatcher(None, val, query_full)` rebuilds the b2j index of the **long** string on every call. Setting the query once as `seq2` and varying `seq1` reuses it. `autojunk` must stay at its default `True` to keep results identical.

**Files:**
- Modify: `app/rag/store.py:20-64` (`_score_value`), `app/rag/store.py:99-104` (search loop)
- Test: `tests/test_rag.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `_score_value(val: str, query_words: set[str], query_full: str, quoted_phrases: list[str], matcher: difflib.SequenceMatcher, threshold: float) -> float` — Task 5 modifies this same function and must keep the new signature.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_rag.py` (add `import difflib` and `import pytest` at the top if absent):

```python
def test_fuzzy_fallback_still_matches_short_query() -> None:
    """The difflib fallback is live for SHORT queries: 'nvidia korp' has
    neither an exact nor a substring match against 'Nvidia Corp', so only
    the fuzzy path can find it (ratio 0.909). This must survive the
    upper-bound optimisation — the optimisation must not become a deletion."""
    s = InMemoryVectorStore()
    s.index_value(
        CategoricalValue(
            value="Nvidia Corp", abstract_column="c.name", tenant_id="t1"
        )
    )
    res = s.search("nvidia korp", tenant_id="t1")
    assert res.outcome == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH
    assert res.match is not None
    assert res.match.categorical_value.value == "Nvidia Corp"


def test_fuzzy_fallback_not_called_for_long_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A short value can never clear the threshold against a long query:
    ratio() <= 2*len(val)/(len(val)+len(query)). difflib.ratio() must not be
    called at all. Each seeded value shares a whole word with the query (so
    the shared-word gate passes) but is NOT a substring of it (so the 0.9
    substring path does not short-circuit first) — this pins the precheck
    itself, not the gate."""
    s = InMemoryVectorStore()
    for name in ("aaron o'brien", "doran mcgregor", "rating specialist"):
        s.index_value(
            CategoricalValue(
                value=name, abstract_column="player.name", tenant_id="t1"
            )
        )

    calls = 0
    real_ratio = difflib.SequenceMatcher.ratio

    def counting_ratio(self: difflib.SequenceMatcher) -> float:
        nonlocal calls
        calls += 1
        return real_ratio(self)

    monkeypatch.setattr(difflib.SequenceMatcher, "ratio", counting_ratio)

    long_query = (
        "What is the overall rating for the football player aaron doran "
        "and how does that compare with the average rating of all other "
        "players in the same league during the 2015 season?"
    )
    res = s.search(long_query, tenant_id="t1")

    assert calls == 0
    assert res.outcome == RAGOutcome.NO_MATCH
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_rag.py::test_fuzzy_fallback_not_called_for_long_queries -v`
Expected: FAIL — `assert calls == 0` fails with a non-zero count (difflib currently runs for every value sharing a word).

`test_fuzzy_fallback_still_matches_short_query` should already PASS — it pins existing behaviour that must not regress.

- [ ] **Step 3: Implement**

In `app/rag/store.py`, replace the `_score_value` signature and its fuzzy branch:

```python
def _score_value(
    val: str,
    query_words: set[str],
    query_full: str,
    quoted_phrases: list[str],
    matcher: difflib.SequenceMatcher,
    threshold: float,
) -> float:
    """Score a single normalized value against the pre-processed query.

    Returns the best score across three strategies:
      1. Quoted-phrase matching (exact or substring → 0.95-1.0)
      2. Word/substring matching against the full query (0.88-1.0)
      3. Fuzzy difflib ratio as fallback

    *matcher* must already have seq2 set to *query_full* by the caller.
    """
```

Leave strategies 1 and 2 exactly as they are. Replace the strategy-3 block:

```python
    # 3. Fuzzy fallback — gated on sharing at least one whole word with
    # the query, then on difflib's own upper bounds. real_quick_ratio() is
    # 2*min(la,lb)/(la+lb) and quick_ratio() is a character-multiset bound;
    # both are guaranteed >= ratio(), so skipping when either falls below
    # the caller's threshold cannot change which values clear it. A short
    # value can never approach a long query — clearing 0.85 needs
    # len(val) >= 0.739 * len(query) — which made this the single largest
    # cost in the scan while contributing no matches.
    if best < 0.85 and set(val.split()) & query_words:
        matcher.set_seq1(val)
        if (
            matcher.real_quick_ratio() >= threshold
            and matcher.quick_ratio() >= threshold
        ):
            best = max(best, matcher.ratio())
    return best
```

Then in `InMemoryVectorStore.search`, build the matcher once before the loop and pass it in. Replace lines 99-104:

```python
        # One matcher for the whole scan: set_seq2 caches the b2j index of
        # the long query string, which SequenceMatcher(None, val, query)
        # would otherwise rebuild on every value. autojunk keeps its default
        # (True) so ratio() results are identical to the previous form.
        matcher = difflib.SequenceMatcher()
        matcher.set_seq2(query_normalized)

        matches: list[ValueMatch] = []
        for cat_val in tenant_values:
            val_normalized = cat_val.value.lower().strip()
            score = _score_value(
                val_normalized,
                query_words,
                query_normalized,
                quoted_phrases,
                matcher,
                threshold,
            )
```

- [ ] **Step 4: Run the full RAG suite**

Run: `uv run pytest tests/test_rag.py tests/test_rag_builder.py -v`
Expected: PASS, including the pre-existing `test_rag_no_match` which asserts the reason string `"No candidates met the threshold (0.85)."`

- [ ] **Step 5: Run the validation suite and commit**

```bash
uv run pytest && uv run ruff check . && uv run mypy . && uv run lint-imports
git add app/rag/store.py tests/test_rag.py
git commit -m "perf(rag): skip difflib ratio via its own upper bounds

real_quick_ratio/quick_ratio are guaranteed upper bounds on ratio(), so
skipping below-threshold candidates cannot change results. Measured:
547,998 difflib calls across 8 BIRD queries produced 0 matches while
costing ~98% of scan time. Also reuses one SequenceMatcher with seq2
pinned to the query so the b2j index is built once, not per value."
```

---

### Task 2: make `is_sensitive` actually stop value extraction

`_run_strategy_refresh` selects columns filtered on `refresh_on_compile` AND `rag_enabled` — but **not** `is_sensitive`. A column flagged sensitive still has its real values `SELECT`ed from the production database and written into `metadata_column_values` on every compile; only indexing is skipped later. And if a steward sets `rag_enabled=False` instead, the column drops out of that query entirely, so the per-column `DELETE` never runs and previously extracted values linger at rest forever. There is no other purge path.

This is pre-existing and does not affect BIRD (public data, no column is flagged — 0 of 798). It must be fixed before the Task 6 enablement rule ships to any real database, because that rule's purpose is to admit exactly the personal-data-shaped columns.

**Files:**
- Modify: `app/api/router.py:1271-1291` (`_run_strategy_refresh`)
- Create: `tests/test_metadata_value_purge.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `_purge_ineligible_rag_values(steward_session: AsyncSession, version_id: uuid.UUID) -> int` returning the number of deleted value rows.

- [ ] **Step 1: Write the failing test**

Create `tests/test_metadata_value_purge.py`:

```python
"""A column that becomes ineligible for RAG must have its extracted values
purged from the registry.

_run_strategy_refresh only visits eligible columns, so its per-column DELETE
never runs for a column that just became sensitive or had rag_enabled turned
off — leaving previously extracted production values at rest indefinitely.
"""

import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.meta_models import (
    Base,
    MetadataColumn,
    MetadataColumnValue,
    MetadataTable,
    MetadataVersion,
)
from app.api.router import _purge_ineligible_rag_values


def _column(version_id: uuid.UUID, table_id: uuid.UUID, name: str, **kw: object):
    return MetadataColumn(
        column_id=uuid.uuid4(),
        version_id=version_id,
        table_id=table_id,
        real_name=name,
        alias=name,
        data_type="text",
        rag_enabled=True,
        is_sensitive=False,
        refresh_on_compile=True,
        **kw,
    )


@pytest.mark.asyncio
async def test_purge_removes_values_for_sensitive_and_disabled_columns() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(
            Base.metadata.create_all,
            tables=[
                MetadataVersion.__table__,
                MetadataTable.__table__,
                MetadataColumn.__table__,
                MetadataColumnValue.__table__,
            ],
        )
    maker = async_sessionmaker(engine, expire_on_commit=False)

    version_id = uuid.uuid4()
    table_id = uuid.uuid4()
    async with maker() as session:
        session.add(
            MetadataVersion(
                version_id=version_id,
                tenant_id="t1",
                status="draft",
                created_by="test",
            )
        )
        session.add(
            MetadataTable(
                table_id=table_id,
                version_id=version_id,
                real_name="users",
                alias="users",
                tenant_id="t1",
            )
        )
        keep = _column(version_id, table_id, "city")
        sensitive = _column(version_id, table_id, "displayname", is_sensitive=True)
        disabled = _column(version_id, table_id, "location", rag_enabled=False)
        session.add_all([keep, sensitive, disabled])
        for col in (keep, sensitive, disabled):
            for val in ("alpha", "beta"):
                session.add(
                    MetadataColumnValue(
                        column_id=col.column_id,
                        version_id=version_id,
                        value=val,
                    )
                )
        await session.commit()
        keep_id = keep.column_id

    async with maker() as session:
        deleted = await _purge_ineligible_rag_values(session, version_id)
        await session.commit()

    assert deleted == 4

    async with maker() as session:
        rows = (
            await session.execute(select(MetadataColumnValue.column_id))
        ).scalars().all()
    assert set(rows) == {keep_id}
    assert len(rows) == 2

    await engine.dispose()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_metadata_value_purge.py -v`
Expected: FAIL with `ImportError: cannot import name '_purge_ineligible_rag_values' from 'app.api.router'`

- [ ] **Step 3: Implement**

In `app/api/router.py`, add the helper immediately above `_run_strategy_refresh`:

```python
async def _purge_ineligible_rag_values(
    steward_session: AsyncSession, version_id: uuid.UUID
) -> int:
    """Delete materialised values for columns no longer eligible for RAG.

    _run_strategy_refresh only visits eligible columns, so its per-column
    DELETE never fires for a column that has just been marked sensitive or
    had rag_enabled turned off. Without this purge, values extracted from
    the production database stay at rest in the registry forever.
    Returns the number of value rows deleted.
    """
    ineligible = select(MetadataColumn.column_id).where(
        MetadataColumn.version_id == version_id,
        or_(
            MetadataColumn.is_sensitive.is_(True),
            MetadataColumn.rag_enabled.is_(False),
        ),
    )
    result = await steward_session.execute(
        sa_delete(MetadataColumnValue).where(
            MetadataColumnValue.version_id == version_id,
            MetadataColumnValue.column_id.in_(ineligible),
        )
    )
    return int(result.rowcount or 0)
```

Add the imports at the top of `app/api/router.py` (the existing function imports `delete` locally inside its body; hoist it so both functions share it):

```python
from sqlalchemy import delete as sa_delete
from sqlalchemy import or_
```

Remove the now-redundant `from sqlalchemy import delete as sa_delete` line from inside `_run_strategy_refresh`.

Add the sensitivity filter to the `_run_strategy_refresh` select (after `MetadataColumn.rag_enabled.is_(True),`):

```python
            MetadataColumn.is_sensitive.is_(False),
```

And call the purge at the end of `_run_strategy_refresh`, immediately before `return refreshed`:

```python
    await _purge_ineligible_rag_values(steward_session, version_id)
    return refreshed
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_metadata_value_purge.py tests/test_api.py tests/test_version_lifecycle.py -v`
Expected: PASS

- [ ] **Step 5: Run the validation suite and commit**

```bash
uv run pytest && uv run ruff check . && uv run mypy . && uv run lint-imports
git add app/api/router.py tests/test_metadata_value_purge.py
git commit -m "fix(metadata): is_sensitive must stop value extraction, not just indexing

_run_strategy_refresh filtered on refresh_on_compile AND rag_enabled but
not is_sensitive, so a column flagged sensitive still had its production
values SELECTed and written to metadata_column_values on every compile.
Turning rag_enabled off instead dropped the column from the refresh query
entirely, so its per-column DELETE never ran and old values stayed at rest.
Adds the filter plus an explicit purge of ineligible columns' values."
```

---

### Task 3: carry `source_database` on indexed values and let the store scope by it

Measured on run `20260729-045019-24ebc8f`: **87.7% of injected hints (2,190/2,497) name columns that are not in the question's own schema context**, and 372/500 questions receive 5-out-of-5 cross-database values. `CategoricalValue` has no database field and `search()` takes only `tenant_id`, while all 13 BIRD databases share tenant `default`.

This task is RAG-layer only and is **behaviour-preserving**: `source_database` defaults to `None` on both the model and the search parameter, so nothing scopes until Task 4 wires it.

**Files:**
- Modify: `app/rag/models.py` (`CategoricalValue`), `app/rag/builder.py:105-208`, `app/rag/store.py` (`search`)
- Test: `tests/test_rag.py`, `tests/test_rag_builder.py`

**Interfaces:**
- Consumes: `_score_value(...)` signature from Task 1.
- Produces:
  - `CategoricalValue.source_database: str | None = None`
  - `InMemoryVectorStore.search(query: str, tenant_id: str, limit: int = 5, threshold: float = 0.85, source_database: str | None = None) -> RAGResult`
  - `_index_column(store, col_dict, abstract_col, table_tenant, artifact_version, column_values, stats, source_database)` — note the new trailing positional parameter.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_rag.py`:

```python
def test_search_scoped_to_source_database() -> None:
    """Values from another database must not be returned when the caller
    knows which database the query targets."""
    s = InMemoryVectorStore()
    s.index_value(
        CategoricalValue(
            value="Nvidia",
            abstract_column="companies.name",
            tenant_id="t1",
            source_database="finance",
        )
    )
    s.index_value(
        CategoricalValue(
            value="Nvidia",
            abstract_column="cards.artist",
            tenant_id="t1",
            source_database="card_games",
        )
    )

    scoped = s.search("Tell me about Nvidia", tenant_id="t1", source_database="finance")
    assert scoped.outcome == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH
    assert scoped.match is not None
    assert scoped.match.categorical_value.abstract_column == "companies.name"


def test_search_unscoped_when_no_source_database() -> None:
    """source_database=None keeps the pre-scoping behaviour: search
    everything. This is the fallback when detection is not confident."""
    s = InMemoryVectorStore()
    s.index_value(
        CategoricalValue(
            value="Nvidia",
            abstract_column="companies.name",
            tenant_id="t1",
            source_database="finance",
        )
    )
    s.index_value(
        CategoricalValue(
            value="Nvidia",
            abstract_column="cards.artist",
            tenant_id="t1",
            source_database="card_games",
        )
    )
    res = s.search("Tell me about Nvidia", tenant_id="t1")
    assert res.outcome == RAGOutcome.AMBIGUOUS_MATCH
    assert res.candidates is not None
    assert len(res.candidates) == 2
```

Add to `tests/test_rag_builder.py`:

```python
def test_indexed_values_carry_source_database() -> None:
    """The builder must thread each table's source_database onto its values
    so the store can scope lookups without importing compiler types."""
    artifact = {
        "tables": [
            {
                "alias": "players",
                "tenant_id": "t1",
                "source_database": "european_football_2",
                "columns": [
                    {
                        "id": "col-1",
                        "alias": "player_name",
                        "rag_enabled": True,
                        "rag_cardinality_hint": "low",
                    }
                ],
            }
        ]
    }
    store = _build_inner(artifact, "t1", "v1", {"col-1": ["Aaron Doran"]})
    values = store._store["t1"]
    assert len(values) == 1
    assert values[0].source_database == "european_football_2"
```

Add `_build_inner` to the imports at the top of `tests/test_rag_builder.py` if it is not already imported:

```python
from app.rag.builder import _build_inner
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_rag.py::test_search_scoped_to_source_database tests/test_rag_builder.py::test_indexed_values_carry_source_database -v`
Expected: FAIL — `CategoricalValue` rejects the unknown field `source_database` (pydantic `ValidationError`), and `search()` rejects the unexpected keyword argument.

- [ ] **Step 3: Implement**

In `app/rag/models.py`, add the field to `CategoricalValue` after `tenant_id`:

```python
    source_database: str | None = Field(
        default=None,
        description=(
            "Logical source database this value belongs to. Used to scope "
            "lookups; None means the value participates in unscoped search."
        ),
    )
```

In `app/rag/store.py`, add the parameter to `search` and filter the candidate list. Change the signature:

```python
    def search(
        self,
        query: str,
        tenant_id: str,
        limit: int = 5,
        threshold: float = 0.85,
        source_database: str | None = None,
    ) -> RAGResult:
```

and immediately after `tenant_values = self._store.get(tenant_id, [])` and its empty check, narrow the list:

```python
        # Scope to one logical database when the caller has resolved it.
        # None means unscoped — the deliberate fallback for queries where
        # database detection was not confident.
        if source_database is not None:
            tenant_values = [
                v for v in tenant_values if v.source_database == source_database
            ]
            if not tenant_values:
                return RAGResult(
                    outcome=RAGOutcome.NO_MATCH,
                    reason=(
                        f"No indexed values for source database "
                        f"'{source_database}'."
                    ),
                )
```

In `app/rag/builder.py`, add the parameter to `_index_column`:

```python
def _index_column(
    store: InMemoryVectorStore,
    col_dict: dict[str, Any],
    abstract_col: str,
    table_tenant: str,
    artifact_version: str,
    column_values: dict[str, list[str]],
    stats: _IndexStats,
    source_database: str | None,
) -> None:
```

and set it on the constructed value:

```python
        store.index_value(
            CategoricalValue(
                value=raw_val,
                abstract_column=abstract_col,
                tenant_id=table_tenant,
                artifact_version=artifact_version,
                source_database=source_database,
            )
        )
```

In `_build_inner`, read it from the table dict and pass it through:

```python
    for tbl_dict in artifact_blob.get("tables", []):
        table_alias: str = tbl_dict.get("alias", "")
        table_tenant: str = tbl_dict.get("tenant_id", tenant_id)
        table_source_db: str | None = tbl_dict.get("source_database")

        for col_dict in tbl_dict.get("columns", []):
            col_alias: str = col_dict.get("alias", "")
            abstract_col = f"{table_alias}.{col_alias}"
            _index_column(
                store,
                col_dict,
                abstract_col,
                table_tenant,
                artifact_version,
                column_values,
                stats,
                table_source_db,
            )
```

Verify `source_database` is present in the compiled artifact's table dicts. Check `_build_table_dict` in `app/api/compiler.py`; if it does not emit `source_database`, add it there alongside `alias` and `tenant_id`.

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_rag.py tests/test_rag_builder.py tests/test_loader.py -v`
Expected: PASS

- [ ] **Step 5: Run the validation suite and commit**

```bash
uv run pytest && uv run ruff check . && uv run mypy . && uv run lint-imports
git add app/rag/models.py app/rag/store.py app/rag/builder.py app/api/compiler.py tests/test_rag.py tests/test_rag_builder.py
git commit -m "feat(rag): carry source_database on indexed values and scope search

87.7% of injected hints named columns outside the question's own schema
because all 13 BIRD databases share one tenant and search() only filtered
by tenant_id. Adds an optional scope; default None preserves behaviour."
```

---

### Task 4: resolve the source database before the RAG step

RAG currently runs at `engine.py:115`, before `filter_schema` at `engine.py:135` — but the source database is resolved *inside* `filter_schema`. Hoisting resolution is safe: `_detect_source_database(schema, intent_tokens)` reads only the schema and the intent tokens; `included_columns` (the RAG output) is consumed later, during column scoring. There is no circular dependency.

Measured detection quality on run `20260729-045019-24ebc8f`: **481 correct, 0 wrong**, with 19 rows unrecorded purely because those queries errored before compiling. Mis-scoping risk is therefore not a practical concern, and when detection is not confident the fallback is deliberately **unscoped** — withholding hints exactly when the compiler is least certain would be the worse failure mode.

**Files:**
- Modify: `app/compiler/filter.py:341-380`, `app/compiler/engine.py:111-137`, `app/compiler/engine.py:318-347`
- Test: `tests/test_filter.py`, `tests/test_compiler_engine.py`

**Interfaces:**
- Consumes: `search(..., source_database=...)` from Task 3.
- Produces: `DeterministicSchemaFilter.resolve_source_database(intent: UserIntent, schema: RegistrySchema) -> str | None` — raises `UnknownSourceDatabaseError` / `AmbiguousSourceDatabaseError` exactly as `filter_schema` does today.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_filter.py` (reuse whichever schema-building helper the file already defines; the names below assume `_make_schema_with_relationship()` — substitute the file's actual helper if it differs):

```python
def test_resolve_source_database_is_callable_standalone() -> None:
    """RAG needs the database before filter_schema runs, so resolution must
    be reachable on its own and agree with what filter_schema resolves."""
    schema = _make_schema_with_relationship()
    intent = UserIntent(natural_language_query="list every user id")
    filt = DeterministicSchemaFilter()

    resolved = filt.resolve_source_database(intent, schema)
    via_filter = filt.filter_schema(intent, schema).source_database_used

    assert resolved == via_filter


def test_resolve_source_database_honours_explicit_scope() -> None:
    schema = _make_schema_with_relationship()
    intent = UserIntent(
        natural_language_query="list every user id", source_database="unknown_db"
    )
    filt = DeterministicSchemaFilter()

    with pytest.raises(UnknownSourceDatabaseError):
        filt.resolve_source_database(intent, schema)
```

Add to `tests/test_compiler_engine.py`:

```python
def test_rag_search_is_scoped_to_resolved_database(monkeypatch) -> None:
    """The engine must resolve the source database BEFORE the RAG lookup and
    pass it down, so value hints cannot come from an unrelated database."""
    seen: dict[str, object] = {}

    class RecordingStore:
        def search(self, query, tenant_id, limit=5, source_database=None):
            seen["source_database"] = source_database
            return RAGResult(outcome=RAGOutcome.NO_MATCH, reason="none")

    engine = _make_engine()  # existing helper in this file
    engine._vector_stores["default"] = RecordingStore()

    engine.compile(
        UserIntent(
            natural_language_query="list every user id",
            source_database="analytics",
        ),
        tenant_id="default",
    )

    assert seen["source_database"] == "analytics"
```

Adjust `_make_engine()` / the `compile()` call to match the constructor and awaiting convention already used in `tests/test_compiler_engine.py`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_filter.py::test_resolve_source_database_is_callable_standalone tests/test_compiler_engine.py::test_rag_search_is_scoped_to_resolved_database -v`
Expected: FAIL — `AttributeError: 'DeterministicSchemaFilter' object has no attribute 'resolve_source_database'`

- [ ] **Step 3: Implement**

In `app/compiler/filter.py`, add the public method above `filter_schema`:

```python
    def resolve_source_database(
        self, intent: UserIntent, schema: RegistrySchema
    ) -> str | None:
        """Resolve the target source database from the intent alone.

        Exposed separately because the RAG lookup runs before schema
        filtering and must scope its value search to the same database.
        Depends only on the intent and the schema — never on RAG output —
        so calling it early cannot change what filter_schema decides.
        Returns None when no database clears the detection threshold.
        """
        if intent.source_database:
            if not self._apply_database_scope(schema, intent.source_database):
                raise UnknownSourceDatabaseError(intent.source_database)
            return intent.source_database
        intent_tokens = self._tokenize(intent.natural_language_query)
        detected, _scores = self._detect_source_database(schema, intent_tokens)
        return detected
```

Leave `filter_schema` as it is. It re-runs the same pure resolution, which keeps this task free of behavioural change beyond the RAG scoping; the duplicated work is a token scan over the schema, not a database call.

In `app/compiler/engine.py`, resolve before the RAG call. Replace the block at lines 111-117:

```python
            included_cols = RAGIncludedColumns(columns=[])

            # Resolve the source database first so RAG can scope its value
            # search to it. Detection reads only the intent and schema, so
            # hoisting it above RAG introduces no circular dependency.
            # None (not confidently detected) deliberately falls back to an
            # unscoped search rather than withholding hints.
            rag_source_database = self.schema_filter.resolve_source_database(
                intent, schema
            )
            explain_context["session"]["rag_source_database"] = (
                rag_source_database
            )

            # RAG runs on every query — follow-up or not — so value hints are
            # always available to the LLM even when the schema is reused.
            self._apply_rag_hints(
                intent,
                hints,
                included_cols,
                explain_context,
                tenant_id,
                rag_source_database,
            )
```

Update `_apply_rag_hints` to accept and forward it:

```python
    def _apply_rag_hints(
        self,
        intent: UserIntent,
        hints: PromptHints,
        included_cols: RAGIncludedColumns,
        explain_context: dict[str, Any],
        tenant_id: str,
        source_database: str | None = None,
    ) -> None:
```

and its search call:

```python
        rag_result = store.search(
            intent.natural_language_query,
            tenant_id=tenant_id,
            limit=5,
            source_database=source_database,
        )
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_filter.py tests/test_compiler_engine.py tests/test_rag.py tests/test_explainability.py -v`
Expected: PASS. If any test asserts the *ordering* of `UnknownSourceDatabaseError` / `AmbiguousSourceDatabaseError` relative to other validation errors, it may now raise earlier — update the assertion to match the new order rather than reintroducing late resolution.

- [ ] **Step 5: Run the validation suite and commit**

```bash
uv run pytest && uv run ruff check . && uv run mypy . && uv run lint-imports
git add app/compiler/filter.py app/compiler/engine.py tests/test_filter.py tests/test_compiler_engine.py
git commit -m "feat(compiler): resolve source database before the RAG lookup

Scopes RAG value search to the detected database. Detection measured 481
correct / 0 wrong on the 500-question mini-dev set. Falls back to unscoped
search when no database clears the threshold."
```

---

### Task 5: length-aware substring matching and specificity tie-breaks

Two scoring defects, both independent of which columns are enabled:

1. `elif val in query_full: best = 0.9` is a raw substring test, so a 1-character value scores 0.9 against nearly any English question. Across the run, **91% of injected values were 1–2 characters** (median length 1); the most-injected were `'R'`×437, `'G'`×371, `'B'`×266.
2. `matches.sort(key=score)` is stable, so values tying at 0.9 are ordered by **artifact insertion order**. The same question can surface different hints purely from table ordering — unauditable and non-deterministic with respect to meaning.

**Files:**
- Modify: `app/rag/store.py` (`_score_value`, `search` sort)
- Test: `tests/test_rag.py`

**Interfaces:**
- Consumes: `_score_value(..., matcher, threshold)` from Task 1; `search(..., source_database)` from Task 3.
- Produces: module constant `_MIN_SUBSTRING_MATCH_LEN = 3`; helper `_is_word_boundary_match(val: str, query_full: str) -> bool`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_rag.py`:

```python
def test_single_character_values_do_not_match() -> None:
    """Single-letter codes were 91% of all injected values because
    'R' is a substring of almost any English question."""
    s = InMemoryVectorStore()
    for code in ("R", "G", "B", "U", "W"):
        s.index_value(
            CategoricalValue(
                value=code, abstract_column="cards.colorindicator", tenant_id="t1"
            )
        )
    res = s.search(
        "What is the ratio of customers who pay in EUR against customers who pay in CZK?",
        tenant_id="t1",
    )
    assert res.outcome == RAGOutcome.NO_MATCH


def test_substring_match_requires_word_boundary() -> None:
    """'art' must not match inside 'artifact' — a coincidental infix is not
    evidence that the user meant that value."""
    s = InMemoryVectorStore()
    s.index_value(
        CategoricalValue(value="art", abstract_column="cards.type", tenant_id="t1")
    )
    res = s.search("List every artifact card in the set", tenant_id="t1")
    assert res.outcome == RAGOutcome.NO_MATCH


def test_word_boundary_substring_still_matches() -> None:
    """A genuine multi-word phrase match must still score 0.9."""
    s = InMemoryVectorStore()
    s.index_value(
        CategoricalValue(
            value="Post Cards", abstract_column="events.name", tenant_id="t1"
        )
    )
    res = s.search("how much was spent on post cards last year", tenant_id="t1")
    assert res.outcome == RAGOutcome.SINGLE_HIGH_CONFIDENCE_MATCH


def test_ties_broken_by_specificity_not_insertion_order() -> None:
    """Equal scores must rank the more specific (longer) value first,
    regardless of the order values were indexed in."""
    s = InMemoryVectorStore()
    s.index_value(
        CategoricalValue(
            value="red", abstract_column="cards.color", tenant_id="t1"
        )
    )
    s.index_value(
        CategoricalValue(
            value="red deck wins", abstract_column="decks.name", tenant_id="t1"
        )
    )
    res = s.search("how many red deck wins entries are there", tenant_id="t1")
    assert res.outcome == RAGOutcome.AMBIGUOUS_MATCH
    assert res.candidates is not None
    assert res.candidates[0].categorical_value.value == "red deck wins"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_rag.py -k "single_character or word_boundary or specificity" -v`
Expected: `test_single_character_values_do_not_match` and `test_substring_match_requires_word_boundary` FAIL (both currently return matches at 0.9); `test_ties_broken_by_specificity_not_insertion_order` FAIL (insertion order puts `"red"` first).

- [ ] **Step 3: Implement**

In `app/rag/store.py`, add near the existing constants:

```python
# A substring shorter than this carries no evidence that the user meant the
# value — single letters are substrings of almost any English question.
_MIN_SUBSTRING_MATCH_LEN = 3


def _is_word_boundary_match(val: str, query_full: str) -> bool:
    """True when *val* occurs in *query_full* delimited by word boundaries.

    Guards against coincidental infixes ('art' inside 'artifact').
    """
    return (
        re.search(rf"(?<!\w){re.escape(val)}(?!\w)", query_full) is not None
    )
```

Replace the strategy-2 substring branch in `_score_value`:

```python
    elif (
        len(val) >= _MIN_SUBSTRING_MATCH_LEN
        and _is_word_boundary_match(val, query_full)
    ):
        best = max(best, 0.9)
```

In `search`, replace the sort so ties resolve by value length:

```python
        # Rank by score, then by specificity. Sorting on score alone is
        # stable, which meant equal-scoring values were ordered by artifact
        # insertion order — the same question could surface different hints
        # purely from table ordering.
        matches.sort(
            key=lambda x: (x.similarity_score, len(x.categorical_value.value)),
            reverse=True,
        )
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_rag.py tests/test_compiler_engine.py -v`
Expected: PASS

- [ ] **Step 5: Run the validation suite and commit**

```bash
uv run pytest && uv run ruff check . && uv run mypy . && uv run lint-imports
git add app/rag/store.py tests/test_rag.py
git commit -m "fix(rag): require word-boundary substring matches; break ties by specificity

Single-character values were 91% of all injected hints because a raw
substring test scored them 0.9 against nearly any question. Ties at equal
score were resolved by artifact insertion order, making hint selection
non-deterministic with respect to meaning."
```

---

### Task 6: intrinsic RAG enablement rule

`_rag_config` is a purely structural rule — text, not PK, 9–50,000 distinct, avg length ≤ 100 — with no assessment of whether a column could ever be matched from natural language. The result, measured in the registry: RAG is enabled on **UUID columns and numeric IDs stored as text**, which dominate index volume.

| column | values | avg len | word-like |
|---|---|---|---|
| `cards.tcgplayerproductid` | 49,470 | 5.3 | 0% |
| `foreign_data.uuid` | 34,056 | 36.0 | 0% |
| `posts.tags` | 28,528 | 43.3 | 0% |
| `cards.name` | 21,738 | 15.5 | 93.8% |
| `users.displayname` | 34,875 | 9.1 | 74.6% |

Separately, `builder._index_column` **skips every column with `rag_cardinality_hint == "high"`**, which silently discarded all 113 columns in the 201–50,000 band — the round-4 widening never reached the index (`indexed=4483 skipped(cardinality=113)`). Removing that skip is only safe once shape filtering and database scoping have cut the pool.

The rule below uses only intrinsic properties of the column's own values. It must never be derived from BIRD gold SQL. The 0.5 threshold means "a majority of values are natural-language-shaped" — a principled cutoff, not one fitted to a column list.

**Files:**
- Modify: `scripts/discover_metadata.py:36-68` (`_rag_config`), `scripts/discover_metadata.py:166-211` (discovery query), `app/rag/builder.py:131-136` (remove the skip)
- Test: `tests/test_discover_rag_config.py`, `tests/test_rag_builder.py`

**Interfaces:**
- Consumes: `_index_column(..., source_database)` from Task 3.
- Produces: `_rag_config(dtype: str, is_pk: bool, distinct_count: int | None, avg_len: float | None = None, word_like_ratio: float | None = None) -> tuple[bool, str | None, int | None, str | None, bool]` — the new fifth parameter is keyword-friendly and defaults to `None`, which is treated as "unknown, do not enable".

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_discover_rag_config.py`:

```python
def test_rag_disabled_for_uuid_column() -> None:
    """UUID columns are text, non-PK and in the cardinality band, but no
    natural-language question will ever contain a UUID."""
    enabled, *_ = mod._rag_config(
        "text", False, 34_056, avg_len=36.0, word_like_ratio=0.0
    )
    assert not enabled


def test_rag_disabled_for_numeric_id_stored_as_text() -> None:
    enabled, *_ = mod._rag_config(
        "text", False, 49_470, avg_len=5.3, word_like_ratio=0.0
    )
    assert not enabled


def test_rag_enabled_for_entity_name_column_with_wordlike_values() -> None:
    enabled, hint, limit, strategy, refresh = mod._rag_config(
        "text", False, 21_738, avg_len=15.5, word_like_ratio=0.938
    )
    assert enabled and hint == "high" and strategy == "distinct"
    assert limit == 21_738


def test_rag_disabled_when_shape_unknown() -> None:
    """word_like_ratio=None means discovery could not sample the column;
    fail closed rather than indexing an unknown value population."""
    enabled, *_ = mod._rag_config("text", False, 500, avg_len=12.0)
    assert not enabled
```

The existing `test_rag_enabled_for_entity_name_column` in this file calls `_rag_config("text", False, 10_848, avg_len=14.0)` with no ratio and asserts `enabled`. Update it to pass `word_like_ratio=0.9`, since "no shape information" now means "do not enable".

Add to `tests/test_rag_builder.py`:

```python
def test_high_cardinality_columns_are_indexed() -> None:
    """The 201-50k band was silently discarded at index time, so the entity
    name columns it was meant to add never reached the store."""
    artifact = {
        "tables": [
            {
                "alias": "players",
                "tenant_id": "t1",
                "source_database": "european_football_2",
                "columns": [
                    {
                        "id": "col-1",
                        "alias": "player_name",
                        "rag_enabled": True,
                        "rag_cardinality_hint": "high",
                        "rag_limit": 3,
                    }
                ],
            }
        ]
    }
    store = _build_inner(
        artifact, "t1", "v1", {"col-1": ["Aaron Doran", "Shay Given"]}
    )
    assert len(store._store["t1"]) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_discover_rag_config.py tests/test_rag_builder.py::test_high_cardinality_columns_are_indexed -v`
Expected: FAIL — `_rag_config()` raises `TypeError` for the unexpected `word_like_ratio` argument, and the builder test asserts 2 indexed values but gets 0.

- [ ] **Step 3: Implement**

In `scripts/discover_metadata.py`, add the constant near `_MAX_RAG_VALUE_AVG_LEN`:

```python
# A column earns a place in the RAG index only when most of its values look
# like something a person could type in a question. Values that are opaque
# identifiers (UUIDs, numeric IDs), markup blobs, or single-letter codes can
# never be matched from natural language, and they dominate index volume.
_MIN_WORD_LIKE_RATIO = 0.5
```

Replace `_rag_config`:

```python
def _rag_config(
    dtype: str,
    is_pk: bool,
    distinct_count: int | None,
    avg_len: float | None = None,
    word_like_ratio: float | None = None,
) -> tuple[bool, str | None, int | None, str | None, bool]:
    """RAG configuration for a discovered column.

    A column is indexed only when its values could plausibly appear in a
    natural-language question:
      - text, not a primary key, 9-50,000 distinct
      - avg value length <= 100 (longer means documents, not values)
      - a majority of sampled values are word-shaped

    The last rule is what excludes UUID and numeric-id columns, which are
    text, non-PK and inside the cardinality band, yet can never be matched
    from a question — and which otherwise dominate the index.

    word_like_ratio=None means discovery could not sample the column; that
    fails closed rather than indexing an unknown value population.

    Values are populated at compile time via refresh_on_compile=True.
    """
    is_text = dtype.lower() in ("text", "character varying", "varchar")
    if not is_text or is_pk or distinct_count is None:
        return False, None, None, None, False
    if avg_len is not None and avg_len > _MAX_RAG_VALUE_AVG_LEN:
        return False, None, None, None, False
    if word_like_ratio is None or word_like_ratio < _MIN_WORD_LIKE_RATIO:
        return False, None, None, None, False
    if 9 <= distinct_count <= 200:
        hint = "low" if distinct_count <= 50 else "medium"
        return True, hint, min(distinct_count, 100), "most_frequent", True
    if 200 < distinct_count <= 50_000:
        return True, "high", distinct_count, "distinct", True
    return False, None, None, None, False
```

In the discovery loop, compute the ratio alongside `avg_len`. Inside the existing `try:` block, directly after the `avg_len` computation, add:

```python
                word_like_sql = text(
                    "SELECT AVG(CASE WHEN v ~ '^[A-Za-z][A-Za-z .''&-]{2,}$'"
                    " THEN 1.0 ELSE 0.0 END)"
                    f' FROM (SELECT DISTINCT "{col_name}" AS v FROM "{tbl_name}"'
                    f' WHERE "{col_name}" IS NOT NULL LIMIT 1000) s'
                )
                word_like_row = await session.execute(word_like_sql)
                raw_ratio = word_like_row.scalar()
                word_like_ratio = (
                    float(raw_ratio) if raw_ratio is not None else None
                )
```

Declare it with the other locals near `avg_len: float | None = None`:

```python
        word_like_ratio: float | None = None
```

and pass it to the call:

```python
        ) = _rag_config(dtype, is_pk, distinct_count, avg_len, word_like_ratio)
```

In `app/rag/builder.py`, delete the high-cardinality skip block (lines 131-136):

```python
    if col_dict.get("rag_cardinality_hint") == "high":
        stats.skipped_cardinality += 1
        logger.warning(
            "RAG: skipping %s — rag_cardinality_hint=high", abstract_col
        )
        return
```

Remove `skipped_cardinality` from `_IndexStats` and from the `logger.info` format string and arguments in `_build_inner`. Update the module docstring, deleting the line `- rag_cardinality_hint="high" columns are skipped`.

Add `"high"` to `_LIMIT_BY_CARDINALITY` so the fallback limit is explicit rather than silently defaulting to 100 when a column has no per-column `rag_limit`:

```python
_LIMIT_BY_CARDINALITY: dict[str | None, int] = {
    "low": 500,
    "medium": _DEFAULT_RAG_LIMIT,
    "high": 50_000,
    None: _DEFAULT_RAG_LIMIT,
}
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_discover_rag_config.py tests/test_rag_builder.py tests/test_loader.py -v`
Expected: PASS

- [ ] **Step 5: Run the validation suite and commit**

```bash
uv run pytest && uv run ruff check . && uv run mypy . && uv run lint-imports
git add scripts/discover_metadata.py app/rag/builder.py tests/test_discover_rag_config.py tests/test_rag_builder.py
git commit -m "feat(rag): enable indexing on value shape, not distinct count

The band rule enabled RAG on UUID and numeric-id columns, which dominated
index volume and can never be matched from a question, while the builder
silently discarded the entire 201-50k band (indexed=4483, skipped=113) so
entity-name columns never reached the store. Enablement now requires a
majority of word-shaped values; the skip is removed."
```

---

### Task 7: rebuild, measure, record

**Files:**
- Modify: none (operational)

- [ ] **Step 1: Full reset and re-discovery**

Enablement changed, so the artifact must be rebuilt from scratch. Follow `feedback_bird_reset_procedure` in full — `down -v`, rebuild, admin key, query key, approve draft → pending_review → active, compile:

```bash
docker compose -f docker-compose.yml -f docker-compose.bird.yml down -v
docker compose -f docker-compose.yml -f docker-compose.bird.yml up --build -d
```

- [ ] **Step 2: Confirm the index changed as intended**

```bash
docker logs aegis_app 2>&1 | grep "RAG index built"
```

Expected: an `indexed=` count that is **not** 4483, with no `skipped(...cardinality=...)` term. Confirm UUID/id columns are gone and entity-name columns are present:

```bash
docker exec aegis_postgres psql -U postgres -d aegis_data_warehouse -tA -F'|' -c "
with v as (select version_id from aegis_meta.metadata_versions where status='active' order by created_at desc limit 1)
select t.source_database, t.alias||'.'||c.alias, c.rag_cardinality_hint::text
from aegis_meta.metadata_columns c join aegis_meta.metadata_tables t on t.table_id=c.table_id
where c.version_id=(select version_id from v) and c.rag_enabled
  and t.alias||'.'||c.alias in
    ('cards.name','users.displayname','player.player_name',
     'cards.tcgplayerproductid','foreign_data.uuid','posts.tags');"
```

Expected: the first three present, the last three absent.

- [ ] **Step 3: Full regeneration and replay**

Prompt content changed, so every replay key changed and `benchmarks/responses.jsonl` is stale. Archive it and regenerate all 500 — do **not** attempt to re-key:

```bash
mkdir -p benchmarks/archive-preintrinsic && mv benchmarks/responses.jsonl benchmarks/archive-preintrinsic/
scripts/run_offline_benchmark.sh <query-api-key> haiku 500
```

If generation stops early with empty-stderr `CLI exit 1` entries in `benchmarks/failures.jsonl`, that is the subscription usage window — wait and re-run `cli_batch_generate.py`, which resumes by key.

- [ ] **Step 4: Compare and record**

Baseline to beat: **52.2% (261/500)**, run `20260729-045019-24ebc8f`. Record the new run in the benchmark history table in `project_status.md`, along with the observed RAG scan latency and the new indexed-value count. Report the result before starting any further work.

---

## Self-Review

**Spec coverage.** Every agreed item maps to a task: difflib optimisation → Task 1; `is_sensitive` extraction and purge → Task 2; per-database scoping → Tasks 3 (store) and 4 (compiler wiring); unscoped fallback when detection is not confident → Task 4 Step 3; length-aware substring and specificity tie-break → Task 5; intrinsic enablement rule → Task 6; measurement → Task 7. The token/inverted index was explicitly parked and is deliberately absent. The "repair the fuzzy path to compare against query n-grams" option was deferred and is likewise absent — Task 1 preserves its current semantics rather than redesigning it.

**Placeholder scan.** No TBD, no "add error handling", no "similar to Task N". Every code step carries the actual code. Three places name an existing helper the executing engineer must confirm rather than invent — `_make_schema_with_relationship()` and `_make_engine()` in the test files, and `_build_table_dict` in `app/api/compiler.py`; each is flagged inline with what to do if the name differs.

**Type consistency.** `_score_value` gains `matcher` and `threshold` in Task 1 and keeps that signature in Task 5. `search` gains `source_database` in Task 3 and is called with it in Task 4. `_index_column` gains a trailing `source_database` in Task 3 and is not re-signatured in Task 6. `_rag_config` returns the same 5-tuple throughout; only its parameter list grows, with the new parameter defaulting to `None`. `resolve_source_database` is defined in Task 4 and used only there.

**Known risk carried forward.** Task 4 may surface `UnknownSourceDatabaseError` / `AmbiguousSourceDatabaseError` earlier than today; Step 4 of that task tells the engineer to update ordering assertions rather than restore late resolution.
