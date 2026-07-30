# BIRD Benchmark — Improvement Backlog

**Status:** **ACTIVE** — the only active initiative.
**Created:** 2026-07-29 · **Branch:** `rag-precision-and-performance`
**Purpose:** the single durable list of identified changes, to be worked through
**one item at a time**, each discussed before implementation.
**Next action:** step 0 of the recommended order — re-key the replay artifacts
(zero tokens). See the callout immediately below.

---

## ⚠️ BEFORE ANY WORK: check the replay keys

`_prompt_key` hashes system+user, and the system prompt carries
`- Current date (UTC): YYYY-MM-DD` from `backend_hints.py:25`. **The keys expire at
UTC midnight.** `benchmarks/prompts.jsonl` was dumped **2026-07-29** and stores
`Current date (UTC): 2026-07-29`, so on any later day a replay scores **0.0%** — every
key misses.

**This blocks steps 2–5 of the recommended order**, all of which assume a free replay.
Do NOT regenerate (that costs tokens). **Re-key instead:**

1. Back up `prompts.jsonl` + `responses.jsonl`.
2. Re-dump prompts (note: `DumpPromptGateway` truncates only **once per process**; the
   long-lived container means a second dump **appends**, so expect 1000 lines — old 500
   first, new 500 last).
3. Join old↔new on `(user_prompt, system_prompt with the date line regex-normalised)` —
   this has matched **500/500 with zero ambiguity** before.
4. Rewrite `responses.jsonl` with the new keys.

The permanent fix is **E6** (pin the date hint). Do E6 early and this tax disappears.

---

## Current state

| | |
|---|---|
| Best run | `20260729-154032-7749c1a` |
| **Official EX** | **55.4% (277/500)** — re-measured at 278 on a later replay; ±1 is normal |
| Soft-F1 | 54.6% |
| R-VES | 54.5 |
| tolerant EX | 60.6% *(diagnostic only — NEVER quote as a BIRD result)* |
| Errors | 19 |
| Ran-but-wrong | ~203 |
| Model | Haiku only (user constraint) |
| Dialect | PostgreSQL mini-dev (500 questions) |

Per-DB: student_club 81 · superhero 77 · formula_1 58 · debit_card 53 ·
codebase_community 53 · toxicology 53 · european_football_2 53 · card_games 52 ·
thrombosis 50 · california_schools 33 · financial 28.

### The bar we are measured against

- **mini-dev PostgreSQL leaderboard** (our track, apples-to-apples):
  **TA + GPT-4 (HKU) = 50.80%**, then bare models ≤36.0% (gpt-4-turbo 36.0,
  gpt-4 35.8, gpt-4-32k 35.0, llama3-70b 29.4, gpt-3.5-turbo 27.4, …).
  At 55.4% we would lead this board.
- **Main dev/test leaderboard** (SQLite, 1,534 questions — NOT comparable to ours):
  AskData + GPT-4o 77.64/81.95 · Agentar-Scale-SQL 74.90/81.67 · Sber Text2SQL 75.74/81.33.
  Also TA-SQL, DAIL-SQL, CHESS, XiYan-SQL, CHASE-SQL, ExSL, Distillery.
- ⚠️ **Honest framing:** the mini-dev PG board is sparsely populated because serious
  systems report on the main set. A system at 81% test would not land at 50% on
  mini-dev PG. "Leads mini-dev PostgreSQL" is true but reflects thin competition.
  Our Haiku is also a 2026 model against their 2024 GPT-4.
- BIRD **accepts pipeline/system/code-agent submissions**, not just models — Aegis is
  the right class of entry.
- Submission: email `bird.bench23@gmail.com` per the Submission Guideline (~10-day
  turnaround). Leaderboard updates need a public paper/resource + a PR.
  Self-consistency entries must use <8 candidates.
- Context: arXiv 2601.08778 reports a **52.8% annotation-error rate in BIRD mini-dev**,
  which caps the achievable ceiling for everyone.

---

## Standing rules (absolute)

1. **No per-question tuning.** Every change must generalise across all 13 databases.
2. **No column-description edits.**
3. **No FK injection.** *(See item 0 — current state is keep-and-disclose.)*
4. **Do NOT teach the model to match BIRD's inconsistent gold conventions.**
   We accept losing those questions.
5. **TDD for all app code**; commit per logical change with `pytest`, `ruff`, `mypy`
   and `lint-imports` green.
6. Only **`--eval-mode official`** is BIRD-comparable. It needs an **admin-scope**
   API key (it reads the explain payload for native driver types) and fails loudly
   without one. Never quote tolerant mode.
7. **Haiku only.** No model upgrade (no Sonnet comparison) until the pipeline work is
   done — otherwise a model change confounds every measurement.
8. **Never relax a rule to pass a question.** No `noqa`/mypy overrides, and no
   loosening a safety control (e.g. the multi-statement parser rejection) to convert
   an error into a pass. Fix the code, or accept the loss.

## Known permanent losses (subtract from any ceiling estimate)

- **~7 questions** to BIRD's inconsistent gold conventions — accepted by decision (rule 4).
- **~17 questions** to `numeric` vs gold's `CAST(… AS REAL)` representation — accepted by
  decision (see the float8 section). *Unless* **G1** shows SQLite recovers them for free.
- **q83 + q173** — the model emits two SQL statements; the parser's multi-statement
  rejection is a security control and must not be relaxed (rule 8). **Now addressed from
  the other side by B6** (a prompt rule requiring exactly one statement), which may
  recover q83. q173 is likely still wrong on content regardless.
- BIRD mini-dev's own **52.8% annotation-error rate** (arXiv 2601.08778) caps everyone,
  though it does not mean 52.8% are unanswerable — treat it as context, not a number
  to subtract.

## Sequencing mechanic (drives the whole order)

- **Translator / harness / RAG / safety changes do NOT alter prompts** → scoreable by
  a **free replay** against the existing `responses.jsonl`. Do these first.
- **Prompt or discovery changes bust all 500 replay keys** → require a full
  regeneration (real token cost). **Batch every prompt change into one release.**
- Replay keys also embed the UTC date, so `prompts.jsonl` dumped yesterday replays at
  0.0% today. Re-key rather than regenerate (join old↔new on user_prompt with the date
  line normalised — this has matched 500/500 before).

---

## ⚠️ Hidden coupling — items whose real impact exceeds their description

Read this before picking up **any** item. Several entries look like local cleanups but
touch a safety control, change prompts, or can move the score the wrong way.

| Item | What it looks like | What it actually is |
|---|---|---|
| **A1** | "Fix 6 JOIN validation errors" | ⚠️ **The most dangerous item on the list.** "Hallucinated JOIN blocked" is a **security control** — it stops the LLM joining tables in ways the steward never declared, which is close to the core promise of the product. Loosening it to pass 6 questions erodes that guarantee. Sub-cases differ sharply: q880 (`results.driverid = results.driverid`, a self-join on one table) is probably safe; q197 (a CTE derived from a declared table not inheriting its edges) is arguably a **genuine gap**; but "make JOIN validation more permissive" as a general move is not acceptable. Treat each of the 6 separately, and justify each on its own terms. |
| **A2** | "Repair a mis-qualified column" | The translator would silently rewrite **which table a column comes from** (`examination.rnp` → `laboratory.rnp`). That can cross a data-access boundary. Constrain to: exactly one candidate table **and** that table already present in the FROM/JOIN graph. |
| **A4** | "Allow 2 cross-JOIN questions" | The comma/cross-join block guards against cartesian products. Precedent for the required rigor is commit `b3a5d72`, which allowed comma joins **only between provably single-row CTEs** — not a blanket allow. |
| **C1** | "Add the missing statement_timeout" | Counterintuitive risk: adding a timeout to `_run_gold_sql` can make **gold** queries time out, turning currently-correct questions into errors and **lowering** the score. Gold needs a generous timeout, and the two paths may need different ones. |
| **D2** | "Remove a 2× inefficiency" | Removing it silently reverts the ~7 pp asymmetry fix. See D2's own row. |
| **E1** | "Cheap determinism cleanup, saves tokens" | It changes `sample_values` → changes **prompts** → busts all replay keys **and** changes what the model sees. It is **not accuracy-neutral**; expect the ±35-question churn of C4. It cannot be validated without a paid regeneration, so it must ride in a prompt batch. |
| **E3** | "Trim prompt size" | Removing schema columns can remove the one column the model needed. Not a free saving — must be scored, not assumed. |
| **D1** | "RAG performance work" | Changing the index changes which value hints are injected → changes prompts → regeneration plus accuracy churn. Filed under performance, but it is not purely a performance change. |

Second sweep (2026-07-30) — the remaining items, so the list is exhaustive:

| Item | What it looks like | What it actually is |
|---|---|---|
| **A3** | "Resolve an ambiguous column" | Same family as A2. Resolving `date` across two scoped tables means **picking a table on the model's behalf**; pick wrong and the result is a silent wrong answer rather than an error. Existing logic only resolves when candidates are join-equivalent via declared edges — keep that bar. |
| **A6** | "Fix a statement timeout" | ⚠️ The tempting fix is raising `_STATEMENT_TIMEOUT_MS`. That is a **product DoS control** protecting every tenant, and raising it also makes every future benchmark run slower (D-tier regression). Fix the query plan or accept the loss; do not raise the limit to buy one question. |
| **A5** | "Two execution errors" | q253 (translator alias scoping) is a genuine bug — low risk. q988 is not: the model cast a text lap-time column to numeric, and "fixing" it by making the translator coerce defensively **changes query semantics** for every user. |
| **B3** | "Prompt rule for float division" | ⚠️ A prompt rule is **less precise than the mechanical rewrite that was measured**. V2 scored +21/−0 because it only touched divisions; a model told to "cast to DOUBLE PRECISION" may over-apply it to `AVG`/`ROUND` and reproduce **V1's −10 losses**. Must be scored for gains *and* losses, not assumed to inherit V2's number. |
| **B4 / B6** | "Add a prompt rule" | Rule 12 is already the longest rule in the prompt. Each addition dilutes attention across all 500 questions and can move unrelated ones. Prompt additions are global changes, never local ones. |
| **E2** | "Iterate on a 150-question subset" | Per-DB counts get small fast (financial has 32 questions in the full set), so subset deltas carry large noise. Use it for direction only; never quote a subset number as a result, and always confirm on the full 500. |
| **E6** | "Delete one prompt line" | Removing the date hint drops the only anchor for **relative-date questions** ("last month") for real product users. BIRD barely uses them, so the benchmark won't notice — the product might. Mitigation: `CURRENT_DATE`/`NOW()` are already allow-listed, so the capability exists; confirm the model actually reaches for them before removing. |
| **D4 / F** | "Cache gold results for speed" | Using `--gold-cache` makes gold a cache hit, which **cannot be timed**, so R-VES becomes unreportable. If a submission ever needs R-VES, at least one uncached full run is required. |
| **F2** | "Disclose the injected LIMIT" | If BIRD objects and it has to go, note `LIMIT 500000` is also a **row-cap safety control** (with the executor's fetch cap as defence in depth). Removing it for compliance would need a different protection, and could surface timeouts on large results. |

**The general rule:** if an item touches the safety engine, the translator's validation,
the prompt, or discovery, it is not a local change. Ask "what does this let through that
was previously blocked?" and "does this alter the prompt?" before starting.

**Every item on this list has now been reviewed for hidden coupling.** Items with no
entry in either table (A7, B1, B2, B5, C2–C5, D3, D5, E4, E5, F1/F3/F4/F5, G1–G3) are
either pure measurement, pure documentation, or already carry their caveat inline.

---

## 0 — Baseline integrity: the STEP-2 FKs

`docker/bird_data/02_restore_fks.sql` has two sections:

- **STEP 1 — 16 FKs.** Declared in BIRD's own SQLite DDL and dropped by the official
  SQLite→PostgreSQL transpile. Restoring them repairs a conversion artifact.
  **Not injection.**
- **STEP 2 — 4 FKs.** `transactions_1k`→`gasstations`/`customers`/`products` and
  `cards.setcode`→`sets.code`. **Not declared in the SQLite DDL.**

### Verified against the SQLite DDL (2026-07-29, `PRAGMA foreign_key_list`)

Ground truth, re-checked directly rather than from notes. The probe is known-good
because it does find the STEP-1 edges:

```
card_games — ALL declared FKs:
  foreign_data     -> cards(uuid)
  legalities       -> cards(uuid)
  rulings          -> cards(uuid)
  set_translations -> sets(setCode -> code)
debit_card_specializing — ALL declared FKs:
  yearmonth -> customers(CustomerID)   [x2]
```

So `cards.setcode → sets.code` and `transactions_1k → gasstations/customers/products`
appear **nowhere** in the SQLite DDL. This matches the comment written into
`02_restore_fks.sql` when the change landed (*"NOT declared in any BIRD artifact…
absent from the BIRD SQLite DDL, dev_tables.json, and the official PG dump alike"*).

**Mitigating facts on record** (context, not justification):
- `set_translations.setCode → sets.code` **is** declared, so `sets.code` is an
  established FK target in BIRD's own DDL and `cards.setcode` points at the same key —
  an argument that its absence is a DDL/transpile omission rather than an invention.
- BIRD's `database_description` CSVs document the linkage as model input, and 34
  mini-dev gold queries join through these edges.
- Worth ~2.2 pp (37.4% → 39.6%) and ~19 unblocked JOIN errors.

**DECISION (user, 2026-07-29): keep them, disclose them on submission, remove only if
BIRD objects.** The disclosure must state the STEP-1/STEP-2 distinction accurately —
STEP 1 is in the SQLite DDL, STEP 2 is not in any artifact.

---

## A — Correctness: the 19 errors

Ceiling **+3.8 pp**. Every item here is free to measure (no prompt change).
Each error is a guaranteed-wrong question.

| # | Item | Qs | Notes |
|---|---|---|---|
| **A1** | JOIN-relationship validation gaps | 6 | Largest cluster. q880 self-joins `results.driverid = results.driverid`; q197 joins a CTE (`distinct_mol.molecule_id = atom.molecule_id`) — CTE not accepted as a relationship endpoint; q716 uses alias `comments.userid = u.id`; plus q1113, q879, q884 ("JOIN ON clause contains no column-equality condition that matches a declared relationship"). Look like genuine translator gaps, not model errors. |
| **A2** | Mis-qualified column repair | 3 | q1265/1270/1275. Model writes `examination.rnp` when `rnp` exists on exactly **one** scoped table (`laboratory`). **Verified:** `laboratory.rnp` IS in the filtered context (64 aliases, all 44 laboratory columns, 3 tables) — so this is a model slip that a translator repair could absorb, analogous to the existing ambiguous-naked-column resolution. NOT a schema-filter bug (hypothesis tested and rejected). |
| **A3** | Ambiguous naked column / orphaned prefix | 3 | q1529 + q98 (`date` belongs to two scoped tables), q94 (`acc` prefix ambiguous across `disp`/`account`). |
| **A4** | Implicit/cross JOIN | 2 | q1094, q1115 — "every JOIN must have an explicit ON or USING". |
| **A5** | Execution errors | 2 | q988 `invalid input syntax for numeric: "18:55.797"` (model casts a lap-time text column); q253 `missing FROM-clause entry for table "atom"` — translator alias scoping. |
| **A6** | Statement timeout | 1 | q247. |
| **A7** | Redeploy the landed ParseError fix | 2 | q83 + q173 are `sqlglot.errors.ParseError: Expected exactly 1 SQL statement, got 2` escaping as bare 500s. `app/main.py:557` already handles this — **the running container is stale** (built 13:20 UTC; commit `2ca7013` landed 15:43 UTC). **No EX gain** (the model really did emit two statements) — this only removes the 500s. **Redeploy delta verified: `2ca7013` is the ONLY undeployed commit touching `app/`** (the 15:07–15:11 commits predate the 13:20 UTC container build; rule 12's new text is already present in `prompts.jsonl`, confirming `571f261` shipped). So the redeploy is low-risk and will not move the baseline. **The tempting bad fix:** taking the first statement and discarding the rest. It *would* likely pass q83 (whose first SELECT answers the question) but would turn q173 into a **confidently wrong answer** (its first SELECT answers only half the question), and it defeats a control whose stated purpose is rejecting multi-statement payloads "rather than silently discarding everything after the first semicolon". Forbidden by standing rule 8. |

---

## B — Correctness: the ~203 ran-but-wrong

| # | Item | Est. | Notes |
|---|---|---|---|
| **B1** | Fresh systematic analysis of the 203 | largest unknown | This is where every big win has come from — passing BIRD `evidence` was +12.6 pp. Highest expected value on the list, but unscoped until analysed. |
| **B2** | `financial` regression 46.9% → 28.1% | ~+6 | Never root-caused. 32 questions in that DB. Check whether rule 12 pushed the model into new shapes. |
| **B3** | Integer-division truncation + float4 `REAL/REAL` | ~+4 | **Correctness-only scope.** See the float section below. |
| **B4** | Rule-12 output-shape misses | ~12 | Prompt change → batch. Previously parked. |
| **B6** | **Prompt rule: return exactly ONE query** | ~2 | **NEW (user, 2026-07-30).** q83 and q173 both emit two SQL statements and are rejected by the parser's multi-statement control. Rather than weakening that control (forbidden, rule 8), tell the model up front to produce a single statement. Rule 6 already mandates `{"sql": "..."}` but never says *one statement*. Clean, general, and not gold-convention fitting — a multi-statement answer is wrong by any standard. May convert q83; q173 is likely still wrong on content (its first statement answers only half the question, and it says `FROM order`, an unquoted reserved word). **Prompt change → batch.** |
| **B5** | Scope-aware column resolution refactor | ? | PARKED. q1526/q1014/q1225/q1115 — 3 of 4 self-fixed on regeneration, so re-derive the value before planning. See the B5 appendix below for why the original plan was deferred and the verified sqlglot API facts. |

### The float8 investigation — measured, mostly abandoned by decision

Measured over the full 500 by replay (baseline reproduced exactly at 277–278):

| Variant | Score | Net |
|---|---|---|
| baseline | 278 (55.6%) | — |
| **V1** — cast final result columns to float8 (≡ `float(Decimal)`) | 287 (57.4%) | **+10** (+20 / **−10**) |
| **V2** — compute every division in float8 | **299 (59.8%)** | **+21 (+21 / −0)** |

- V1 **breaks 10 passing questions**: 6 plain `AVG(...)`, 4 `ROUND(CAST(… AS NUMERIC), n)`
  — exactly where gold deliberately stays numeric. The old "~5–6 pp / 26–30 questions"
  estimate in memory was V1-shaped and is **wrong**.
- A sqlglot parse+render **control** scored +0/−1, so V2's gains are the cast, not the
  round-trip.
- **Breakdown of V2's +21** — this is why most of it was abandoned:
  | Cause | Qs |
  |---|---|
  | `COUNT(a)/COUNT(b)` is bigint/bigint → **TRUNCATES** in PG (q371 → 0, then ×100 = 0) | 2 (q371, q629) |
  | Model casts **both** operands to `REAL` → float4 single-precision loss | 2 (q1471, q1359) |
  | Model's exact `numeric` vs gold's `CAST(… AS REAL)` — same value, different last digit | **~17** |
- **DECISION (user):** the ~17 representation cases are gold-convention fitting and are
  **dropped** under standing rule 4. Only the ~4 genuine bugs (B3) remain in scope, to be
  justified as correctness, not as BIRD score.
- **DECISION (user):** implement via **prompt rule**, not a translator rewrite of
  generated SQL. Aegis must not post-process the model's query to match gold.
- Dialect fact worth keeping: PostgreSQL `CAST(x AS REAL)/int` resolves to **double
  precision**, so gold is effectively float8 despite `REAL` being float4.

---

## C — No regressions

| # | Item | Notes |
|---|---|---|
| **C1** | `statement_timeout` missing on **both** harness execution paths | `grep` matches only `app/execution/executor.py`. `_rerun_generated_sql` (`scripts/run_bird_benchmark.py:313`) **and** `_run_gold_sql` (`:264`) run unbounded — an unbounded stall can corrupt a run. The gold path was not in the original bug note. |
| **C2** | Score every change by replay, reporting gains **AND** losses | The V1 experiment surfaced a "+20" that was really +10 once losses were counted. Never quote one side. |
| **C3** | Respect the ±1 replay nondeterminism | Two replays of identical responses gave 278 and 277 (statement timeouts on slow queries). Do not chase single-question deltas. |
| **C4** | Noise floor for *generation* churn is ~35 gained / 35 lost | Between two runs of identical configuration. A ±1 pp move after regeneration is **not** a result. |
| **C5** | Re-measure RAG hot paths at production index size | A word-boundary regex once caused a 289× slowdown (27.2 s vs 94 ms/query) that unit tests could not catch — it only appeared at real index size. |

---

## D — Performance (wall-clock)

| # | Item | Known figure |
|---|---|---|
| **D1** | RAG store is a linear scan + difflib over **184,156** values. `difflib.quick_ratio` ≈4.1 s of 14.6 s profiled. Worst DB codebase_community = 81,632 values ≈1.3 s/query, because `posts.title` (42,869 values, avg 57 chars) passes the length bound. **Token/inverted index is the durable answer** (currently parked). | top hotspot |
| **D2** | Harness **double-executes every query** — the API runs it, then `_rerun_generated_sql` runs it again to get native driver types. ⚠️ **Do NOT simply remove it.** The re-execution is what makes `--eval-mode official` valid: the API's JSON layer stringifies `Decimal`/`date` (`_coerce_row`), and a stringified Decimal can never equal gold's native value under BIRD's raw set equality. Deleting the second execution silently reverts the asymmetry bug that was costing ~7 pp. The only real fix is making the API able to return native-typed rows; otherwise this cost is inherent to official mode. | ~2× DB work |
| **D3** | Unbounded stalls (same root as C1) | tail risk |
| **D4** | Gold-cache vs R-VES tension — `--gold-cache` makes gold a cache hit, which cannot be timed, so R-VES is reported only over questions actually executed. Resolve deliberately. | per-run |
| **D5** | Prompt size p90 ~53 KB inflates generation latency | — |
| | *Already won:* 500-prompt dump 10+ min → **48 s**; difflib precheck 13.0 → 6.4 ms/query; concurrency dump/gen 8, replay 10. | |

---

## E — Token / usage consumption

| # | Item | Effect |
|---|---|---|
| **E1** | **Discovery is nondeterministic.** Both sampling queries in `scripts/discover_metadata.py` (~line 250 exhaustive, ~line 260 `LIMIT 8`) use `ORDER BY COUNT(*) DESC` with **no tie-break**. Verified by re-running the exact SQL for all 798 columns against identical static data with the same `_truncate_samples(80)`: **33/796 columns returned different values than stored**; **306/798 have a COUNT(*) tie at the top-8 boundary** (membership unspecified) and **441/798 have ties within the sample** (order unspecified). All 11 DBs affected (formula_1 52 · card_games 46 · european_football_2 40 · financial 31 · codebase_community 30 · student_club 28 · california_schools 26 · thrombosis 22 · superhero 14 · debit_card 11 · toxicology 6). Fix = stable tie-break (`ORDER BY COUNT(*) DESC, "<col>" ASC`), so a re-discovery only churns DBs that actually changed and `cli_batch_generate` can resume the rest. | biggest structural saving |
| **E2** | Stratified ~150-question subset for iteration; full 500 only at milestones | ~70% per cycle |
| **E3** | Trim prompt size (p90 53 KB). q1265 shipped all 44 `laboratory` columns in 64 aliases. Fewer tokens per call, and possibly better accuracy. | per-call |
| **E4** | Order batch generation by database so the shared system-prompt prefix hits prompt caching | needs verification |
| **E5** | **Process rule:** land all non-prompt changes first; batch prompt changes into one regeneration | avoids whole cycles |
| **E6** | **DECIDED (user, 2026-07-30): REMOVE the date hint entirely.** `backend_hints.py:25` renders `Current date (UTC): {ctx.now:%Y-%m-%d}` into the system prompt — it is `build_backend_hints`'s *only* output — so every replay artifact dies at UTC midnight and costs a re-key cycle (top callout). **Rationale: the model does not need a "now" concept; it can get it from the database** via `CURRENT_DATE` / `NOW()`, which is strictly better because it evaluates at execution time and can never be stale. `CurrentDate`/`CurrentTimestamp` are already in the safety allow-list (added in round-2 fixes), so this is supported today. Removing it kills the daily re-key tax permanently and stabilises the shared prompt prefix, which is exactly what **E4** needs for provider prompt caching. It also *shrinks* the prompt. **Prompt change → batch.** ⚠️ See hidden-coupling note. | kills a recurring tax; unblocks E4 |
| | *Note:* the archived-vs-current prompt diff is **not** a usable control for E1 — it is dominated by intentional code changes (rule 12 wording, schema expansion), which is why 0/500 keys matched. | |

---

## F — Submission compliance (only if we submit)

| # | Item |
|---|---|
| **F1** | Disclose the FKs, with the STEP-1 (in SQLite DDL) vs STEP-2 (in no artifact) distinction stated plainly. See item 0. |
| **F2** | Disclose the injected `LIMIT 500000` (`AEGIS_ROW_LIMIT`) on every generated query. |
| **F3** | Public write-up + reproducible harness (leaderboard requires a public paper/resource + PR). |
| **F4** | Disclose the information condition: Aegis feeds a **filtered, aliased** schema with BIRD's own unedited descriptions plus RAG value hints, rather than full DDL. Different from baseline entries — arguably our product's legitimate design, but it must be stated. |
| **F5** | Confirm BIRD's `evidence` usage is within protocol (it is official model input — 498/500 questions have it). |

---

## G — Dialect strategy

| # | Item | Notes |
|---|---|---|
| **G1** | **Price a SQLite migration before building it.** SQLite has no `numeric` type — everything is `REAL`/`INTEGER` — which is very likely *why* BIRD's SQLite scores run ~10 pp above PostgreSQL for the same system. The ~17 representation questions abandoned in B3 would probably pass **for free**, with no gold-convention fitting and no integrity question. The q988 `invalid input syntax for numeric` error class also disappears. **Cheap measurement:** transpile our existing generated SQL to SQLite with sqlglot and execute against the on-disk `data/minidev/MINIDEV/dev_databases/*/*.sqlite`, then count how many currently-failing questions pass. Zero tokens. If it recovers ~17–25, the backend work has a number; if ~5, drop it. **Method notes (verified 2026-07-29):** (a) gold must come from **`mini_dev_sqlite.json`, NOT the PostgreSQL file** — same 500 `question_id`s (confirmed identical), but genuinely different gold SQL: backtick quoting, and no `NULLIF` (SQLite division by zero yields NULL natively). Using PG gold would invalidate the whole measurement. (b) The transpile is an **approximation and a lower bound** — a model actually prompted for SQLite would generate different SQL, so a poor result is not conclusive while a good result is. (c) Report gains **and** losses per C2. |
| **G2** | **Cost of actually migrating — verified 2026-07-29, and it is a genuinely scoped effort.** An earlier assessment in this session called Aegis "structurally PostgreSQL" and claimed the timeout invariant was a blocker. **Both were wrong**; the README's Database-compatibility table is substantially accurate. Measured coupling: <ul><li>**sqlglot dialect — LOW.** 5 literal sites in 2 files: `parser.py:18` (`read="postgres"`), `translator.py:120/1165/1177/1224` (`dialect="postgres"`).</li><li>**Execution — LOW.** SQLAlchemy async; swap the driver in the connection string (`aiosqlite`).</li><li>**Statement timeout — NOT a blocker.** Already gated on `engine.name == "postgresql"` (`executor.py:65`); other engines skip it and lose timeout protection. Real SQLite protection would need `progress_handler`/`interrupt` — an enhancement, not a prerequisite.</li><li>**Postgres coupling in `app/` is only 5 files:** executor, cleanup_sql, meta_models, parser, translator.</li><li>**Registry does NOT need to move — the key simplification.** `meta_models.py` has 39 `JSONB`/`UUID` references (the expensive item), but that is the *metadata* store. Only the **target warehouse** (`DB_URL_RUNTIME`) needs to be SQLite; `aegis_meta` stays on PostgreSQL. This removes the largest chunk of work entirely.</li><li>**Schema discovery — MEDIUM, bounded.** `discover_metadata.py` has 9 pg-catalog sites (`pg_constraint`/`pg_class`/`pg_attribute`/`information_schema`); SQLite needs `PRAGMA table_info`, `PRAGMA foreign_key_list`, `sqlite_master`.</li><li>**Prompt dialect rules — the only recurring token cost.** Rule 9 and the "specialized in PostgreSQL translation" preamble are Postgres-specific; changing them busts all 500 replay keys and forces a regeneration.</li></ul> |
| **G3** | A dialect switch alone does **not** buy comparison with the leaders. mini-dev SQLite only puts us alongside TA+GPT-4's 58.0%. AskData / Agentar / Sber are on the **main dev set (1,534 questions)** — 3× the questions and 3× the token spend, which collides with the usage constraint. Decide the goal explicitly: *lead mini-dev PG*, *lead mini-dev SQLite*, or *compete on main dev*. |

---

## Recommended order

Ordered by **risk-adjusted** value, not raw question count. The earlier draft put **A1**
second; the hidden-coupling review moved it later, because it is the one item that can
weaken a security control.

0. **Re-key the replay artifacts** (top callout) — nothing below can be scored until this
   is done, and it costs zero tokens.
1. **Item 0** — settled (keep + disclose). No work.
2. **A7 redeploy** — verified to be a single-commit delta (`2ca7013`), so it is low-risk
   and clears the two bare 500s. Do it early so later results aren't attributed to a
   stale image.
3. **G1** — zero-token measurement that may reframe everything downstream: if SQLite
   recovers the ~17 representation questions for free, **B3 becomes moot** and the
   dialect question outranks most of B.
4. **A3, A5, A6** — the narrower error fixes, least entangled with safety controls.
5. **A2** — only under the constraint in the hidden-coupling table (single candidate
   table, already in the JOIN graph).
6. **A1** — **split into its 6 cases and justify each separately.** Do not treat as one
   "make JOIN validation more permissive" change. q880 (self-join) and q197 (CTE edge
   inheritance) are the two most likely to be legitimate gaps.
7. **C1 + D2** — same code region; mind C1's gold-timeout trap.
8. **One batched prompt release: E6 + E1 + B3 + B4 + B6.** All change prompts, so they
   share a single regeneration. E6 first inside the batch means the re-key tax never
   recurs afterwards. Score the batch for gains **and** losses — B3 in particular must
   not be assumed to inherit V2's +21/−0.
9. **B2**, then **B1** — the biggest prize, after the cheap certain wins.
10. **D1** — durable RAG index work (note it also changes prompts).
11. **F** — only when a submission is actually on the table.

---

## Artifacts and how to reproduce

- Results DB: `benchmarks/results.db` (`benchmark_runs`, `benchmark_results` with
  `match`, `match_tolerant`, `soft_f1`). **The only recorded baseline is
  `20260729-154032-7749c1a`** — the V1/V2 and discovery measurements were standalone
  scripts and are NOT in `results.db`; their numbers live in this document only.
- `benchmarks/prompts.jsonl` + `responses.jsonl` — 500/500, matching the running stack.
  A replay re-scores at **zero** token cost **only after re-keying** — see the callout
  at the top of this file. As dumped (2026-07-29) they replay at 0.0% on any later day.
- ⚠️ **Measurement scripts are NOT yet in the repo.** The rigs built on 2026-07-29 live
  in a session scratchpad and will be lost: the full-500 gains/losses replay harness
  (the shape C2 mandates for every future change), the float8 root-cause classifier, and
  the discovery-determinism checker that produced E1's 33/796 + 306/798 figures.
  **They should be committed under `scripts/analysis/` before the session ends**,
  otherwise every one of these measurements has to be rewritten to be re-verified.
- The stale-container trap: always confirm the running image matches HEAD before
  attributing a result to code. `docker inspect aegis_app --format '{{.Created}}'`
  vs `git log -1 --format=%ci <commit>`. A7 exists because this was missed.
- Gold cache `benchmarks/gold_cache.db` survives resets (BIRD data is static).
- Full reset: see the reset-procedure appendix below — `down -v` + rebuild +
  admin key + version approval + compile. Steps 3–4 alone regenerate keys.
- Rebuild app only:
  `docker compose -f docker-compose.yml -f docker-compose.bird.yml up --build -d aegis`
- Benchmark:
  ```bash
  uv run python scripts/run_bird_benchmark.py \
    --questions data/minidev/MINIDEV/mini_dev_postgresql.json \
    --api-key <ADMIN-scope key> \
    --api-url http://localhost:8000 \
    --db-url "postgresql+asyncpg://user_aegis_runtime:runtime_pass@127.0.0.1:5433/aegis_data_warehouse" \
    --provider-id replay --limit 500 --concurrency 10
  ```

---

## Appendix — reset procedure

Any time metadata must be wiped and re-discovered. **Always do a full volume
wipe.** Partial resets (truncate + re-grant + restart) cause a second discovery
run to find 0 tables, because grants get revoked by the first successful run.

```bash
# 1. Tear down containers AND volumes
docker compose -f docker-compose.yml -f docker-compose.bird.yml down -v

# 2. Build and start the full stack
docker compose -f docker-compose.yml -f docker-compose.bird.yml up --build -d

# 3. Wait for init containers (migrate -> bird-loader -> discover) to exit,
#    then create an admin key
docker exec -e PYTHONPATH=/app aegis_app uv run python scripts/create_admin_key.py \
  --tenant-id default --user-id admin --scope admin --description "admin key"

# 4. Create a query key using the admin key from step 3
curl -s -X POST -H "Authorization: Bearer <admin_key>" \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"default","user_id":"demo","scope":"query","description":"demo query key"}' \
  http://localhost:8000/api/v1/auth/credentials | jq '{key: .raw_key, id: .credential_id}'

# 5. Get the draft version id
curl -s -H "Authorization: Bearer <admin_key>" \
  http://localhost:8000/api/v1/metadata/versions | jq '.[0].version_id'

# 6. Approve it (draft -> pending_review -> active)
curl -s -X PATCH -H "Authorization: Bearer <admin_key>" \
  -H "Content-Type: application/json" \
  -d '{"status":"pending_review","reason":"initial load"}' \
  http://localhost:8000/api/v1/metadata/versions/<version_id>/status
curl -s -X PATCH -H "Authorization: Bearer <admin_key>" \
  -H "Content-Type: application/json" \
  -d '{"status":"active","reason":"initial load"}' \
  http://localhost:8000/api/v1/metadata/versions/<version_id>/status

# 7. Compile it (triggers hot-reload; no restart needed)
curl -s -X POST -H "Authorization: Bearer <admin_key>" \
  http://localhost:8000/api/v1/metadata/compile/<version_id>
```

Steps 3–4 alone are enough to regenerate keys in a new session when metadata
has not changed.

---

## Appendix — run history

Every run is in `benchmarks/results.db` (`benchmark_runs`, `benchmark_results`).
Scores before 2026-07-29 used an evaluator with a known defect (see below), so
they are **not** comparable to current numbers.

| Run | Qs | Accuracy | Notes |
|---|---|---|---|
| 20260321-131714 | 10 | 0% | baseline |
| 20260321-220235 | 50 | 40% | after BUG-1–4, INFRA-1, PROMPT-1–3 |
| 20260408-204036 | 50 | 36% | post Phase 0–3, no RAG — variance sample A |
| 20260408-205119 | 50 | 40% | post Phase 0–3, no RAG — variance sample B |
| 20260409-070114 | 50 | 48% | + BUG-5 real fix + RAG auto-enabled |
| 20260409-082218 | 500 | 20.8% | live haiku; 111 HTTP 429s motivated the offline pipeline |
| 20260412-201335 | 500 | 22.2% | replay of haiku batch, 161 errors |
| 20260728-110258 | 500 | 36.2% | post 2026-07-28 fixes, 69 errors |
| 20260728-141731 | 500 | 36.8% | retry-filled responses, 52 errors |
| 20260728-145231 | 500 | 37.4% | round-2 bug fixes, 27 errors |
| 20260728-162211 | 500 | 39.6% | step-2 curated FKs, 11 errors |
| **20260728-195700** | 500 | **52.2%** | **BIRD `evidence` passed in intent + rules 12/13 — the single biggest win (+12.6 pp)** |
| 20260729-045019 | 500 | 52.2% | RAG widening complete — exactly ties baseline, i.e. net-neutral |
| 20260729-120729 | 500 | 53.2% | RAG precision+perf branch; +1.0 pp = within noise |
| 20260729-124128 | 500 | 60.2% | same responses, re-scored after the evaluator fix — a **measurement correction**, not a gain |
| **20260729-154032** | 500 | **55.4%** | **current best on the aligned official evaluator** |

### Two measurement lessons embedded in that table

**The evaluator was wrong for months.** `_normalize_value` stringified
`Decimal` but left `float` alone, so a float could never equal a Decimal. Gold's
`CAST(... AS REAL)` yields float8 while the model's plain `/` yields numeric —
same arithmetic answer, incomparable types. 34 questions agreed to ~16
significant digits and were scored wrong. Re-scoring identical responses moved
266 → 301.

**The deeper cause was asymmetry, not precision.** Gold was fetched natively
while predicted rows came through the API's JSON layer, where `_coerce_row`
stringifies `Decimal`/`date` — and a stringified Decimal can never equal a
float. Fixed by re-executing Aegis's own parameterised SQL through the same
driver as gold, after which Python's numeric tower compares value-equal
int/float/Decimal correctly with **no** normalisation. This is why `official`
mode needs the explain payload, and why the harness executes each query twice
(see D2).

### Historical context on the RAG work

The round-4 RAG widening measured 0.0 pp because it never reached the index:
`discover_metadata` tagged the new band `rag_cardinality_hint="high"` while
`builder._index_column` unconditionally skipped every "high" column. The two
halves contradicted each other, and the index held exactly the wrong columns —
single-letter MTG colour codes in, `player.player_name` and `users.displayname`
out. The follow-up branch fixed targeting (index on value shape, not distinct
count), scoped search to the resolved `source_database`, and made the store's
hot loop fast enough to survive a 184k-value index.

Measured outcomes: cross-database hint contamination 87.7% → 0.0%; 1–2-character
injected values 91% → 2.4%; indexed values 4,483 → 184,156; 500-prompt dump
10+ min → 48 s. Accuracy effect was within noise — the wins were structural.

---

## Appendix — B5: why the scope-aware column refactor was deferred

Phase 4 of `docs/superpowers/plans/2026-04-07-bird-benchmark-phase-2.md`
(scope-aware column resolution for q1526) was stopped at discovery, because
discovery found two correctness problems with the plan as written:

1. **The plan's CTE-output collection only catches `exp.Alias`.** sqlglot
   represents `SELECT users.id, orders.total` (no `AS`) as bare `exp.Column`
   projections, so the plan's logic would silently no-op for any CTE that
   projects bare columns. The right primitive is `inner_select.named_selects`,
   which captures both AS-declared aliases and bare-column names.

2. **The plan's own q1526 regression test already passes on `main`.** Building
   the plan's exact SQL against `_make_schema_with_relationship()` and running
   it through the current `DeterministicTranslator` produces clean output with
   no "Ambiguous naked column" error. So either q1526's real failing SQL
   differs from the plan's test, or the plan misdiagnosed the failure. Either
   way, implementing it as written would ship code that fixes no reproducible
   bug.

**Verified sqlglot 29.0.1 API facts — carry these into any new plan:**

- `Scope.selected_sources` values are `(node, source)` **tuples**; a defensive
  `isinstance(source, tuple)` check is correct.
- `Scope.cte_sources` values are bare `Scope` objects, **not** tuples.
- `Scope.columns` returns only columns belonging to that scope, not nested
  descendants.
- `traverse_scope()` returns **leaf-first**.
- `inner_select.named_selects` enumerates CTE output column names, covering
  both AS-declared aliases and bare-column projections.
- `build_scope` and `traverse_scope` live in `sqlglot.optimizer.scope`.

**A new plan must also resolve this**, which the original did not: the current
translator resolves bare-column outer references through to physical names, and
that works *because* the CTE body has already been rewritten to physical names.
Code that bypasses resolution would leave an outer reference pointing at an
abstract name against a CTE projecting physical names — broken SQL at
execution.
