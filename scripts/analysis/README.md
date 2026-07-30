# Benchmark analysis scripts

Measurement rigs built on 2026-07-29 while investigating the BIRD backlog
(`docs/benchmark/2026-07-29-improvement-backlog.md`). These are **analysis tools, not
app code** — they answer "what would happen if…" *before* anything is implemented.

They talk to a running BIRD stack and are read-only against it.

## Setup

```bash
export AEGIS_ANALYSIS_DIR=benchmarks/analysis      # scratch output dir (default)
mkdir -p "$AEGIS_ANALYSIS_DIR"
# official eval needs an ADMIN-scope key (it reads the explain payload)
echo "<admin key>" > "$AEGIS_ANALYSIS_DIR/admin_key.txt"
```

All of these use `--provider-id replay`, so they cost **zero tokens** — but the replay
keys expire at UTC midnight (see the backlog's top callout). Re-key first or every
question will miss.

## The scripts

| Script | Answers |
|---|---|
| `replay_whatif_sql_rewrite.py` | Full-500: what does rewriting the generated SQL do to official EX? Rewrites every division to `CAST(a AS DOUBLE PRECISION)/b`, and runs a **sqlglot parse+render control** so round-trip artifacts can be told apart from the real effect. Reports gains AND losses. Produced the V2 = +21/−0 figure. |
| `replay_whatif_row_coercion.py` | Full-500: what does coercing result rows do? Simulates `CAST(<numeric> AS float8)` exactly, since that equals `float(Decimal)` in Python — no SQL re-run needed. Produced V1 = +20/−10, i.e. the finding that a blanket float8 output **breaks** 10 passing questions. |
| `classify_near_misses.py` | Root-causes the official-fail / tolerant-pass questions by comparing native driver types on both sides: equal-as-float vs last-digit noise vs text-vs-number vs genuinely different. This is what showed the 26 near-misses were **not** one homogeneous bucket. |
| `check_discovery_determinism.py` | Re-runs `discover_metadata.py`'s exact sampling SQL for every column with stored `sample_values` and diffs against what's stored, plus counts COUNT(*) ties at the top-8 boundary and within the sample. Produced E1's 33/796 changed · 306/798 boundary ties · 441/798 within-sample ties. Needs the postgres superuser (reads `aegis_meta`). |

## The rule these exist to enforce

Backlog item **C2**: score every proposed change over the full 500 and report **gains
and losses**. The V1 experiment looked like "+20" until losses were counted and it was
really +10. Never quote one side.

Always sanity-check that the rig reproduces the recorded baseline
(`20260729-154032-7749c1a` = 277/500) before trusting a delta from it — both replay
scripts did, which is why their numbers are usable.
