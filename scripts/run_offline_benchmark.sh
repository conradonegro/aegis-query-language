#!/usr/bin/env bash
# Offline BIRD benchmark pipeline: dump prompts -> generate SQL via the
# claude CLI (subscription auth) -> replay through the full Aegis
# translator/executor -> score.
#
# Prereqs: BIRD stack up (docker-compose.bird.yml), metadata compiled,
# a query-scope API key, and ./benchmarks mounted into the aegis container.
#
# Usage: scripts/run_offline_benchmark.sh <query-api-key> [model] [limit]
set -euo pipefail

API_KEY="${1:?usage: run_offline_benchmark.sh <query-api-key> [model] [limit]}"
MODEL="${2:-haiku}"
LIMIT="${3:-500}"

QUESTIONS=data/minidev/MINIDEV/mini_dev_postgresql.json
DB_URL="postgresql+asyncpg://user_aegis_runtime:runtime_pass@127.0.0.1:5433/aegis_data_warehouse"
API_URL="http://localhost:8000"

echo "=== Pass 1: dump prompts (expected: every question errors) ==="
uv run python scripts/run_bird_benchmark.py \
  --questions "$QUESTIONS" --api-key "$API_KEY" --api-url "$API_URL" \
  --db-url "$DB_URL" --provider-id dump \
  --limit "$LIMIT" --concurrency 4 --store /dev/null

echo "=== Pass 2: generate SQL via claude CLI (model=$MODEL) ==="
uv run python scripts/cli_batch_generate.py --model "$MODEL" --concurrency 4

echo "=== Pass 3: replay through translator/executor and score ==="
uv run python scripts/run_bird_benchmark.py \
  --questions "$QUESTIONS" --api-key "$API_KEY" --api-url "$API_URL" \
  --db-url "$DB_URL" --provider-id replay \
  --limit "$LIMIT" --concurrency 4
