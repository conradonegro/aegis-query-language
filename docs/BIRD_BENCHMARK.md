# BIRD Benchmark Tracking

This guide covers running the BIRD execution-match benchmark and tracking results
across code changes. It uses the harness in `scripts/run_bird_benchmark.py` and a
local SQLite store for comparisons.

If you do not have the BIRD dataset loaded, follow `docs/BIRD_SETUP.md` first.

---

## Run A Full Evaluation

```bash
uv run python scripts/run_bird_benchmark.py \
  --questions path/to/dev.json \
  --api-key <key> \
  --api-url http://localhost:8000 \
  --db-url postgresql+asyncpg://postgres:postgrespassword@127.0.0.1:5432/aegis_data_warehouse
```

Notes:
- Default concurrency is 5. Progress logs are printed in completion order
  (out of order is expected under concurrency).
- Results are stored in `benchmarks/results.db` by default.
- Run IDs are UTC timestamps: `YYYYMMDD-HHMMSS-<commit[:7]>` or `YYYYMMDD-HHMMSS-nogit`.

To disable persistence entirely:

```bash
uv run python scripts/run_bird_benchmark.py \
  --questions path/to/dev.json \
  --api-key <key> \
  --store /dev/null
```

---

## Smoke Tests

Quick subsets for iteration:

```bash
# First 50 questions
uv run python scripts/run_bird_benchmark.py \
  --questions path/to/dev.json \
  --api-key <key> \
  --limit 50

# Single database
uv run python scripts/run_bird_benchmark.py \
  --questions path/to/dev.json \
  --api-key <key> \
  --db-filter financial
```

---

## Reporting And Comparisons

List stored runs:

```bash
uv run python scripts/benchmark_report.py list
```

Compare two runs:

```bash
uv run python scripts/benchmark_report.py compare <run_a> <run_b>
```

If run totals differ, the report prints a warning and computes deltas on the
intersection of `question_id` across both runs. This makes comparisons valid
for filtered or limited runs.

List regressions (correct in run A, wrong in run B):

```bash
uv run python scripts/benchmark_report.py regressions <run_a> <run_b>
```
