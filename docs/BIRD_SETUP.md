# BIRD Dataset Setup

This guide covers loading the BIRD-SQL benchmark database (75 tables, 3.9M rows)
into the Aegis Docker stack for development and testing.

---

## Attribution and license

The dataset used here is the **BIRD-SQL Mini-Dev** package, created by the BIRD
team and released under
[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).

> Jinyang Li, Binyuan Hui, Ge Qu, Jiaxi Yang, Binhua Li, Bowen Li, Bailin Wang,
> Bowen Qin, Ruiying Geng, Nan Huo, Xuanhe Zhou, Chenhao Ma, Guoliang Li,
> Kevin C.C. Chang, Fei Huang, Reynold Cheng, Yongbin Li.
> **"Can LLM Already Serve as A Database Interface? A BIg Bench for Large-Scale
> Database Grounded Text-to-SQLs."** NeurIPS 2023.
> [https://bird-bench.github.io](https://bird-bench.github.io)

**What this means for you:**

- **Attribution required** — if you share or publish work that includes this
  dataset, credit the BIRD team and link to the license.
- **ShareAlike** — if you modify the data and redistribute it, your version
  must also be licensed under CC BY-SA 4.0.
- **Commercial use is permitted** — the CC license grants this explicitly.
- The BIRD team notes the benchmark is intended for *"research and healthy
  application"* use. Using it as a test dataset for an AI text-to-SQL system
  qualifies.

No changes are made to the dataset by Aegis. The SQL dump is loaded as-is
into a local Docker Postgres instance and never committed to this repository.

---

## When do you need this?

The **core stack** (`docker compose up`) starts with an empty public schema.
You define your own tables via the Steward UI.

The **BIRD overlay** adds the full BIRD-SQL dev dataset as a pre-populated
starting point — useful for demos, reproducing benchmark results, or
end-to-end testing without building your own schema.

---

## Step 1 — Download the SQL dump

Download `01_BIRD_dev.sql` from Google Drive:

```
https://drive.google.com/file/d/13VLWIwpw5E3d5DUkMvzw7hvHE67a4XkG/view?usp=sharing
```

The file is approximately 1GB.

---

## Step 2 — Place the file

Put the downloaded file at the default path:

```bash
mv ~/Downloads/01_BIRD_dev.sql \
   /path/to/aegis/docker/bird_data/01_BIRD_dev.sql
```

The `docker/bird_data/` directory is committed (with a `.gitkeep`) but
`*.sql` files are gitignored — the dump never enters version control.

If you want to keep the file elsewhere, set the `BIRD_SQL_PATH` env var:

```bash
export BIRD_SQL_PATH=/your/custom/path/01_BIRD_dev.sql
```

---

## Step 3 — Start the BIRD stack

```bash
docker compose -f docker-compose.yml -f docker-compose.bird.yml up --build
```

Boot sequence with BIRD:

1. `db` — Postgres starts, runs `00_init_roles.sql` + `99_ready_signal.sql` (fast)
2. `migrate` and `bird-loader` run **in parallel**:
   - `migrate` — Alembic creates `aegis_meta` schema + applies grants
   - `bird-loader` — loads `01_BIRD_dev.sql` into `public` schema (~5–10 min)
   The BIRD load typically completes in under 1 minute.
3. `discover` — waits for both, then reverse-engineers the 75 BIRD tables into
   a draft MetadataVersion and populates semantic column descriptions
4. `aegis` — starts, loads the compiled registry, ready to serve queries

---

## Switching between core-only and BIRD

The `discover` service has an idempotency guard: it skips discovery if any
MetadataVersion already exists. This means you **cannot** switch modes on an
existing volume — you must wipe state first:

```bash
# Wipe all data and start fresh
docker compose down -v

# Then start with whichever mode you want
docker compose up --build                                          # core only
docker compose -f docker-compose.yml -f docker-compose.bird.yml up --build  # BIRD
```

---

## Re-running a failed BIRD load

`bird-loader` is not idempotent — re-running against a partially-loaded DB
will fail on duplicate table names. Always wipe first:

```bash
docker compose -f docker-compose.yml -f docker-compose.bird.yml down -v
docker compose -f docker-compose.yml -f docker-compose.bird.yml up --build
```

---

## Teardown

```bash
# Stop containers, keep volumes (data preserved)
docker compose -f docker-compose.yml -f docker-compose.bird.yml down

# Stop containers and delete all data
docker compose -f docker-compose.yml -f docker-compose.bird.yml down -v
```
