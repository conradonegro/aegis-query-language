# Safety Engine Design Notes

Documents intentional decisions in `app/compiler/safety.py`,
`app/compiler/translator.py`, and `app/steward/loader.py` that are not
self-evident from the code.

---

## Allow-list philosophy

The safety engine uses a strict allow-list: every AST node type must appear in
`ALLOW_LIST` or it is rejected. This is intentional — unknown LLM output is
rejected by default rather than permitted by default.

---

## CTEs and subqueries (allowed)

`WITH` / `WITH RECURSIVE` (CTEs) and inline subqueries (`SELECT ... FROM
(SELECT ...)`) are permitted. The original v1 block was conservative; the
actual risks are:

- **Recursive CTEs causing resource exhaustion** — mitigated by
  `SET LOCAL statement_timeout` enforced at execution time for every query.
  A runaway recursive CTE is killed by the same mechanism as any slow query.
- **Data exfiltration via nested queries** — every column reference inside a
  CTE or subquery still passes through the translator's column-safety checks.
  No new data access paths are opened by allowing these constructs.

Top-level `UNION` (where `exp.Union` is the root node) remains blocked by the
existing `isinstance(tree, exp.Select)` root-node check.

---

## `aggregation_allowed` default

`SafetyClassification.aggregation_allowed` controls whether a column may
appear inside an aggregation function (e.g., `COUNT(col)`, `SUM(CASE WHEN col
= 'x' THEN 1 END)`).

**Current default (loader.py):** inherits `allowed_in_select`. If a column is
selectable, it is also aggregation-allowed by default.

**Rationale:** `COUNT(col)`, `COUNT(DISTINCT col)`, and `CASE`-inside-`SUM`
patterns are valid for columns of any type. Blocking non-numeric columns in
aggregations prevented legitimate analytical queries (e.g., counting customers
by currency segment). Columns that must genuinely be blocked from aggregation
should set `safety_classification.aggregation_allowed = false` explicitly in
the metadata artifact.

**Previous default (before this change):** `col_type in numeric_types` —
only numeric columns were aggregation-allowed by default.
