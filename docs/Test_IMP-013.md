# IMP-013 Test Guide — Docker Containerization

## Overview

This guide tests the full `docker compose up` boot sequence on a clean database
while keeping your existing Postgres data safe. It covers every step from
pre-flight to functional verification and teardown.

**What you are testing:**
- `init_roles.sql` runs correctly on a brand-new Postgres
- 1GB BIRD data loads fully before any dependent service starts
- Alembic migrations create `aegis_meta` schema correctly
- `discover_metadata.py` reverse-engineers the BIRD schema into a draft version
- Bootstrap public grants are revoked after discovery completes
- `aegis` starts fully wired to the Docker-internal DB and Redis
- Auth (API key) works end to end

---

## Your Data Is Safe — Understanding What `docker rm` Does

Your existing Postgres data lives in Docker volume:
```
b775d994be48f8deae88717579684bf84f8626b9d7058f5885eff03038b58b32
```
`docker rm` removes the **container shell only** — volumes are never touched unless
you explicitly pass the `-v` flag (which we never do here). After removal, the
volume becomes "orphaned" (dangling) and remains intact until you delete it
manually. To recover it at any time, see the **Recovery** section at the bottom.

---

## Phase 1 — Pre-flight

### Step 1.1 — Note your existing data volume (do this before anything else)

```bash
docker inspect aegis_postgres \
  --format '{{range .Mounts}}{{.Type}} | {{.Name}}{{"\n"}}{{end}}'
```

**Expected output:**
```
bind |
volume | b775d994be48f8deae88717579684bf84f8626b9d7058f5885eff03038b58b32
```

The `volume` line is your Postgres data. Write down the full hash — this is
your recovery key if you ever need to reattach it.

---

### Step 1.2 — Stop the local web server

The `aegis` Docker service binds to host port `8000`. If your local `uvicorn`
is running it will conflict.

Check what is on port 8000:
```bash
lsof -i :8000 | grep LISTEN
```

If you see a uvicorn process, stop it with `Ctrl+C` in its terminal, or:
```bash
kill $(lsof -ti :8000)
```

**Expected after:** `lsof -i :8000` returns nothing.

---

### Step 1.3 — Stop and remove the old containers

```bash
docker stop aegis_postgres aegis_pgadmin
docker rm aegis_postgres aegis_pgadmin
```

> **Important:** Do NOT use `docker rm -v`. The `-v` flag would delete the
> volume. Plain `docker rm` is safe.

**Expected output:**
```
aegis_postgres
aegis_pgadmin
aegis_postgres
aegis_pgadmin
```

Verify both containers are gone:
```bash
docker ps -a | grep aegis
```

**Expected:** No `aegis_postgres` or `aegis_pgadmin` lines.

---

### Step 1.4 — Confirm the data volume still exists

```bash
docker volume ls | grep b775d994
```

**Expected:** The volume hash is still listed. Data is safe.

---

### Step 1.5 — Place the BIRD SQL file

BIRD data is no longer loaded via `initdb`. It is loaded by a dedicated
`bird-loader` container in the BIRD overlay. See `docs/BIRD_SETUP.md` for
the full download and setup instructions. The short version:

```bash
# Download 01_BIRD_dev.sql from:
# https://drive.google.com/file/d/13VLWIwpw5E3d5DUkMvzw7hvHE67a4XkG/view?usp=sharing

mv ~/Downloads/01_BIRD_dev.sql \
   /Users/cnegro/Projects/reviewing/aegis/docker/bird_data/01_BIRD_dev.sql
```

Verify the file is in place:
```bash
ls -lh /Users/cnegro/Projects/reviewing/aegis/docker/bird_data/
```

**Expected:**
```
-rw-r--r--  ...  01_BIRD_dev.sql    (~1.0G)
```

---

### Step 1.6 — Verify `.env` has the HMAC secret

```bash
grep API_KEY_HMAC_SECRET /Users/cnegro/Projects/reviewing/aegis/.env
```

**Expected:** A line like `API_KEY_HMAC_SECRET=<64-char hex string>`.

If it is missing or empty, generate one now:
```bash
echo "API_KEY_HMAC_SECRET=$(python3 -c 'import secrets; print(secrets.token_hex(32))')" \
  >> /Users/cnegro/Projects/reviewing/aegis/.env
```

---

### Step 1.7 — Confirm no port conflicts remain

```bash
lsof -i :8000 -i :5050 -i :5433 | grep LISTEN
```

**Expected:** No output. All three ports are free.

---

## Phase 2 — Build and Launch

Navigate to the project directory:
```bash
cd /Users/cnegro/Projects/reviewing/aegis
```

### Step 2.1 — Build and start all services

This test uses the BIRD overlay:

```bash
docker compose -f docker-compose.yml -f docker-compose.bird.yml up --build
```

`--build` forces a fresh image build on first run (~1–2 minutes for the uv
dependency install). After that, the output switches to streaming service logs.

> **Note:** Leave this terminal open. All service logs stream here. Open a
> second terminal for verification commands in Phase 3.

---

## Phase 3 — Monitor the Boot Sequence

The logs from all services are interleaved. Here is exactly what to look for
and in what order.

### Step 3.1 — Docker image build

```
#1 [internal] load build definition from Dockerfile
...
#5 RUN uv sync --frozen --no-dev --compile-bytecode
...
=> exporting to image
```

**Expected:** Build completes with no errors. Takes ~1–2 minutes on first run;
subsequent runs are instant (cached layers).

---

### Step 3.2 — `db` starts and runs initdb scripts

```
aegis_postgres  | PostgreSQL init process complete; ready for start up.
aegis_postgres  | ...
aegis_postgres  | executing /docker-entrypoint-initdb.d/00_init_roles.sql
aegis_postgres  | executing /docker-entrypoint-initdb.d/01_BIRD_dev.sql
```

The `01_BIRD_dev.sql` line will appear and then nothing visible will happen
briefly while 3.9 million rows load (typically under 1 minute). This is normal.

Eventually:
```
aegis_postgres  | executing /docker-entrypoint-initdb.d/99_ready_signal.sql
aegis_postgres  | LOG:  database system is ready to accept connections
```

The `db` healthcheck (`SELECT 1 FROM public._aegis_docker_ready LIMIT 1`) will
now start passing. You will NOT see this in logs directly — you will know it
passed because `migrate` starts printing output next.

> **If you see `aegis_migrate exited with code 1` before the BIRD load finishes,
> the healthcheck retries are working correctly — migrate will restart
> automatically once the healthcheck passes.**

---

### Step 3.3 — `migrate` runs Alembic

```
aegis_migrate   | INFO  [alembic.runtime.migration] Context impl PostgreSQLImpl.
aegis_migrate   | INFO  [alembic.runtime.migration] Will assume transactional DDL.
aegis_migrate   | INFO  [alembic.runtime.migration] Running upgrade  -> <hash>, <description>
...
aegis_migrate   | INFO  [alembic.runtime.migration] Running upgrade <hash> -> <hash>, <description>
```

Then the container exits:
```
aegis_migrate exited with code 0
```

**Code 0 = success.** If you see code 1, check the migrate logs for a SQL
error. The most likely cause is a credentials mismatch or a migration that
was already partially applied.

---

### Step 3.4 — `discover` runs schema discovery

```
aegis_discover  | [*] Bootstrapping Draft Version: <uuid>
aegis_discover  | [*] Generated 75 Tables and <N> Columns
aegis_discover  | [*] Generated <N> standard Relationship edges.
aegis_discover  | [*] Discovery Draft Version <uuid> completed successfully!
aegis_discover  | [*] Bootstrap grants on public schema revoked.
```

Then:
```
aegis_discover exited with code 0
```

Key things to check:
- Table count should be **75** (the full BIRD dataset)
- The `Bootstrap grants ... revoked` line confirms the `try/else` cleanup ran
- Code 0 = success

If discover shows `[*] Metadata registry already populated. Skipping discovery.`
and exits 0, the idempotency guard fired (a leftover version from a previous
run). This is correct behaviour — it is still a successful exit.

---

### Step 3.5 — `aegis` starts

```
aegis_app       | INFO:     Started server process [1]
aegis_app       | INFO:     Waiting for application startup.
aegis_app       | INFO:     Session store: Redis (redis://redis:6379/0)
aegis_app       | INFO:     Application startup complete.
aegis_app       | INFO:     Uvicorn running on http://0.0.0.0:8000
```

The system is fully up. Proceed to functional verification.

---

## Phase 4 — Functional Verification

Open a second terminal and `cd` to the project directory:
```bash
cd /Users/cnegro/Projects/reviewing/aegis
```

All commands below run on the host from this directory.

### Step 4.1 — Health check

```bash
curl -s http://localhost:8000/health | python3 -m json.tool
```

**Expected:**
```json
{"status": "ok"}
```

---

### Step 4.2 — Create a test admin API key

```bash
docker compose exec aegis sh -c "cd /app && PYTHONPATH=/app .venv/bin/python scripts/create_admin_key.py --tenant-id docker-test --user-id tester@aegis.com --scope admin --description 'IMP-013 test key'"
```

> **Note:** `docker compose exec` defaults to `/` as the working directory, not the
> container's `WORKDIR`. The `sh -c "cd /app && ..."` pattern is required.

**Expected output (stdout):** The raw API key, e.g.:
```
aegis_abc123def456...
```

Save it:
```bash
export RAW_KEY=<paste the key here>
```

---

### Step 4.3 — Verify auth works

**Without a key (should reject):**
```bash
curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/api/v1/metadata/versions
```
**Expected:** `401`

**With the key (should succeed):**
```bash
curl -s -H "Authorization: Bearer $RAW_KEY" \
  http://localhost:8000/api/v1/metadata/versions | python3 -m json.tool
```
**Expected:** A JSON array containing one version with `"status": "draft"` and
`"created_by": "system-auto-discovery"`.

---

### Step 4.4 — Confirm 75 tables were discovered

```bash
VERSION_ID=$(curl -s -H "Authorization: Bearer $RAW_KEY" \
  http://localhost:8000/api/v1/metadata/versions \
  | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['version_id'])")

curl -s -H "Authorization: Bearer $RAW_KEY" \
  http://localhost:8000/api/v1/metadata/versions/$VERSION_ID/schema \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Tables: {len(d[\"tables\"])}')"
```

**Expected:** `Tables: 75`

---

### Step 4.5 — Confirm bootstrap grants were revoked

Connect to the Docker Postgres directly and verify `role_aegis_meta_owner` can
no longer read the `public` schema:

```bash
docker compose exec db psql -U postgres -d aegis_data_warehouse -c "SET ROLE role_aegis_meta_owner; SELECT 1 FROM public.cars LIMIT 1;"
```

**Expected:**
```
ERROR:  permission denied for schema public
```

This confirms the `try/else` revocation in `discover_metadata.py` ran
successfully. The REVOKE removed both `SELECT ON ALL TABLES` and `USAGE ON SCHEMA
public`, so Postgres rejects at the schema level — which is stricter than
table-level denial.

---

### Step 4.6 — Open the Steward UI

Navigate to `http://localhost:8000/steward` in your browser.

**Expected:**
- Auth modal appears (no key stored in this browser session yet)
- Enter the `RAW_KEY` from Step 4.2
- After authentication: sidebar shows one draft version
- Clicking the version shows the 75 BIRD tables with all columns editable

---

### Step 4.7 — Verify Redis is wiring sessions

> **Prerequisite:** Complete step 4.6 first. You must compile the draft version via
> the Steward UI (`POST /api/v1/metadata/compile/{version_id}`) before queries will
> have a schema to work against. Without an active compiled artifact the compiler
> serves an empty schema and all queries fail.

Submit the same query twice to confirm session continuity (follow-up works):

```bash
# First query — omit session_id; server creates the session and returns its UUID
FIRST=$(curl -s -X POST http://localhost:8000/api/v1/query/execute \
  -H "Authorization: Bearer $RAW_KEY" \
  -H "Content-Type: application/json" \
  -d '{"intent": "how many records are there?"}')

echo $FIRST | python3 -m json.tool

# Capture the server-assigned session_id
SESSION_ID=$(echo $FIRST | python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")
echo "Session: $SESSION_ID"

# Follow-up — pass the server-assigned session_id to reuse the session
curl -s -X POST http://localhost:8000/api/v1/query/execute \
  -H "Authorization: Bearer $RAW_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"intent\": \"give me more details\", \"session_id\": \"$SESSION_ID\"}" \
  | python3 -m json.tool
```

**Expected:** Both responses return SQL results and show the **same**
`session_id` UUID. Matching session IDs confirm Redis correctly persisted
and reused the session across requests.

---

## Phase 5 — Teardown

When you are done testing:

```bash
docker compose -f docker-compose.yml -f docker-compose.bird.yml down
```

This stops and removes all containers. Volumes are NOT deleted — both your
old Postgres volume and the new test volume remain.

To also remove the test database volume (clean slate for next test run):
```bash
docker compose -f docker-compose.yml -f docker-compose.bird.yml down -v
```

---

## Recovery — Restoring Your Original Postgres

If at any point you want your original `aegis_postgres` back with all its data:

```bash
# 1. Stop the new stack
docker compose down

# 2. Start the original postgres container reattached to your old volume
docker run -d \
  --name aegis_postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgrespassword \
  -e POSTGRES_DB=aegis_data_warehouse \
  -p 5432:5432 \
  -v b775d994be48f8deae88717579684bf84f8626b9d7058f5885eff03038b58b32:/var/lib/postgresql/data \
  postgres:15-alpine

# 3. Verify it came back
docker exec aegis_postgres psql -U postgres -d aegis_data_warehouse \
  -c "SELECT count(*) FROM aegis_meta.metadata_versions;"
```

**Expected:** Row count matching your original setup. All data intact.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `db` never becomes healthy | BIRD file missing or corrupt | Check `ls -lh data/processed/` and re-copy |
| `migrate exited with code 1` | Schema conflict or wrong credentials | Check migrate logs: `docker compose logs migrate` |
| `discover exited with code 1` | Permission denied on `public` | Check `init_roles.sql` grants applied — see `docker compose logs db` |
| `aegis` port 8000 already in use | Local uvicorn still running | `kill $(lsof -ti :8000)` |
| Auth returns 500 instead of 401 | `API_KEY_HMAC_SECRET` not set | Verify `grep API_KEY_HMAC_SECRET .env` |
| Tables: 0 in Step 4.4 | Discover skipped (idempotency) but DB was empty | `docker compose down -v` then re-run |
