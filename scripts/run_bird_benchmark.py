"""BIRD-SQL benchmark evaluation harness for Aegis Query Language.

Evaluates Execution Match Accuracy (EX) by:
  1. Submitting each BIRD question to /api/v1/query/execute with:
     - A fresh session_id UUID per question (no cross-question context leakage)
     - source_database set to the BIRD db_id
  2. Running the gold SQL directly against the database
  3. Comparing result sets (unordered row match)

Usage:
    uv run python scripts/run_bird_benchmark.py \\
        --questions path/to/dev.json \\
        --api-key <key> \\
        [--api-url http://localhost:8000] \\
        [--db-url postgresql+asyncpg://user:pass@host:5432/db] \\
        [--concurrency 5] \\
        [--limit 50] \\
        [--db-filter financial] \\
        [--output results.json] \\
        [--store benchmarks/results.db]
"""

import argparse
import asyncio
import json
import math
import sqlite3
import subprocess
import sys
import time
import uuid
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import httpx
from sqlalchemy.ext.asyncio import create_async_engine

# ---------------------------------------------------------------------------
# Result comparison — official BIRD EX semantics
# ---------------------------------------------------------------------------


# Significant digits kept when comparing numbers. PostgreSQL float8 carries
# ~15-17 significant digits, so 10 is comfortably inside the noise floor of
# either representation while still separating genuinely different answers.
_NUMERIC_SIG_DIGITS = 10


def _as_number(value: Any) -> float | None:
    """Return *value* as a float when it denotes a finite number, else None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        num = float(value)
    elif isinstance(value, str):
        try:
            num = float(value)
        except ValueError:
            return None
    else:
        return None
    return num if math.isfinite(num) else None


def _normalize_value(value: Any) -> Any:
    """Bridge the JSON serialization boundary between gold and generated rows.

    Gold rows carry native driver types; generated rows arrive through the
    API's JSON layer where pydantic serializes Decimal as str(Decimal) and
    date/datetime as isoformat strings.

    Numbers are canonicalised to a fixed number of significant digits rather
    than compared by representation. Gold's CAST(... AS REAL) yields float8
    while the model's plain `/` yields numeric, so the same arithmetic answer
    arrives as a float on one side and a Decimal-turned-string on the other —
    types that can never compare equal. Even Decimal against Decimal failed
    when the scales differed ('100.0' vs '100.0000000000000000'). This
    compares what the numbers *are*, not how the driver spelled them.
    """
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    num = _as_number(value)
    if num is not None:
        return float(f"{num:.{_NUMERIC_SIG_DIGITS}g}")
    return value


EvalMode = Literal["official", "tolerant"]


def rows_match(
    gold_rows: Sequence[tuple[Any, ...]],
    gen_rows: Sequence[tuple[Any, ...]],
    mode: EvalMode = "official",
) -> bool:
    """Execution accuracy over row tuples. Row order ignored, duplicate rows
    collapsed, column count and order significant.

    "official" is BIRD's calculate_ex verbatim — `set(predicted) ==
    set(ground_truth)` with no normalisation. Both sides must therefore be
    fetched as native driver values: Python's numeric tower already makes
    int/float/Decimal of equal value compare (and hash) equal, so no
    normalisation is needed or wanted. This is the only mode whose number is
    comparable to published BIRD scores.

    "tolerant" additionally canonicalises numbers to a fixed number of
    significant digits, which forgives the last-digit disagreement between
    gold's float8 (CAST(... AS REAL)) and a model's numeric division. Useful
    for tracking whether the arithmetic is right, but it scores strictly
    higher than official and must never be reported as a BIRD result.
    """
    if mode == "official":
        return set(gold_rows) == set(gen_rows)

    def norm(rows: Sequence[tuple[Any, ...]]) -> set[tuple[Any, ...]]:
        return {tuple(_normalize_value(v) for v in row) for row in rows}

    return norm(gold_rows) == norm(gen_rows)


def calculate_row_match(
    predicted_row: Sequence[Any], ground_truth_row: Sequence[Any]
) -> tuple[int, int, int]:
    """Column-level overlap for one row pair — BIRD's calculate_row_match.

    Position within the row is ignored; a predicted value counts as matched
    when it appears anywhere in the gold row.
    """
    matches = 0
    pred_only = 0
    for pred_val in predicted_row:
        if pred_val in ground_truth_row:
            matches += 1
        else:
            pred_only += 1
    truth_only = sum(
        1 for truth_val in ground_truth_row if truth_val not in predicted_row
    )
    return matches, pred_only, truth_only


def calculate_f1_score(
    predicted: Sequence[Sequence[Any]],
    ground_truth: Sequence[Sequence[Any]],
) -> float:
    """BIRD Soft-F1: partial credit for partially-correct result tables.

    Rows are aligned by position, columns compared as sets within each row,
    and the totals treated as one classification problem.
    """
    if not predicted and not ground_truth:
        return 1.0
    if not predicted or not ground_truth:
        return 0.0

    tp = fp = fn = 0
    for i, gt_row in enumerate(ground_truth):
        if i >= len(predicted):
            fn += len(gt_row)
            continue
        matches, pred_only, truth_only = calculate_row_match(
            predicted[i], gt_row
        )
        tp += matches
        fp += pred_only
        fn += truth_only

    precision = tp / (tp + fp) if tp + fp > 0 else 0.0
    recall = tp / (tp + fn) if tp + fn > 0 else 0.0
    if precision + recall == 0:
        return 0.0
    return 2 * precision * recall / (precision + recall)


def ves_reward(time_ratio: float) -> float:
    """BIRD R-VES reward bands over gold_time / predicted_time."""
    if time_ratio >= 2:
        return 1.25
    if time_ratio >= 1:
        return 1.0
    if time_ratio >= 0.5:
        return 0.75
    if time_ratio >= 0.25:
        return 0.5
    return 0.25


def ves_score(reward: float) -> float:
    """Per-question R-VES contribution: sqrt(reward) * 100.

    A wrong query scores zero — R-VES rewards efficiency only among queries
    that are already valid.
    """
    if reward <= 0:
        return 0.0
    return math.sqrt(reward) * 100


# ---------------------------------------------------------------------------
# Intent construction — question + BIRD evidence annotation
# ---------------------------------------------------------------------------


def _build_intent(question: str, evidence: str | None) -> str:
    """Appends BIRD's per-question evidence to the intent.

    Evidence is official model input in the BIRD protocol (external
    knowledge every benchmarked system receives, e.g. "Average Monthly
    consumption = AVG(Consumption) / 12"). Concatenating it onto the
    question mirrors the official prompt construction.
    """
    if not evidence or not evidence.strip():
        return question
    return f"{question}\n\nHint (verified domain knowledge): {evidence.strip()}"


# ---------------------------------------------------------------------------
# Gold SQL execution
# ---------------------------------------------------------------------------

def _gold_cache_key(db_id: str, sql: str) -> str:
    import hashlib

    return hashlib.sha256(f"{db_id}\x00{sql}".encode()).hexdigest()


async def _run_gold_sql(
    engine: Any,
    sql: str,
    db_id: str,
    cache_path: Path | None = None,
) -> list[tuple[Any, ...]]:
    """Execute gold SQL against the physical database.

    Uses raw driver execution: gold SQL is trusted verbatim text with no
    bind parameters, and literals like '%:57' would be misparsed as bind
    parameters by SQLAlchemy text().

    Gold results are deterministic (static benchmark data), so they are
    cached across runs in a local sqlite file when cache_path is given.
    """
    import pickle

    key = _gold_cache_key(db_id, sql)
    if cache_path is not None:
        conn = sqlite3.connect(cache_path)
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS gold_cache"
                " (key TEXT PRIMARY KEY, rows BLOB)"
            )
            hit = conn.execute(
                "SELECT rows FROM gold_cache WHERE key = ?", (key,)
            ).fetchone()
            if hit:
                cached: list[tuple[Any, ...]] = pickle.loads(hit[0])
                return cached
        finally:
            conn.close()

    async with engine.connect() as conn_db:
        result = await conn_db.exec_driver_sql(sql)
        rows = [tuple(r) for r in result.fetchall()]

    if cache_path is not None:
        conn = sqlite3.connect(cache_path)
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS gold_cache"
                " (key TEXT PRIMARY KEY, rows BLOB)"
            )
            conn.execute(
                "INSERT OR REPLACE INTO gold_cache (key, rows) VALUES (?, ?)",
                (key, pickle.dumps(rows)),
            )
            conn.commit()
        finally:
            conn.close()
    return rows


# ---------------------------------------------------------------------------
# API question submission
# ---------------------------------------------------------------------------

async def _rerun_generated_sql(
    engine: Any, body: dict[str, Any]
) -> list[tuple[Any, ...]] | None:
    """Execute Aegis's own parameterised SQL through the benchmark's driver.

    The explain payload carries both the physical SQL and its bound
    parameters, so this runs exactly what Aegis ran — no re-translation — but
    returns native driver values instead of the API's JSON-coerced ones.
    That symmetry is what makes BIRD's raw set equality valid.

    Returns None when the payload lacks the trace, so the caller can fall
    back to the JSON rows.
    """
    from sqlalchemy import text

    explain = body.get("explainability") or {}
    translation = explain.get("translation") or {}
    sql = translation.get("parameterized_sql")
    params = translation.get("parameters")
    if not sql:
        return None
    if isinstance(params, list):
        params = {f"p{i + 1}": v for i, v in enumerate(params)}

    async with engine.connect() as conn:
        result = await conn.execute(text(sql), params or {})
        return [tuple(r) for r in result.fetchall()]


async def _submit_question(
    client: httpx.AsyncClient,
    api_url: str,
    api_key: str,
    question: str,
    db_id: str,
    provider_id: str | None,
) -> dict[str, Any]:
    """Submit one question with a fresh session_id and source_database scoping."""
    payload: dict[str, Any] = {
        "intent": question,
        "source_database": db_id,
        "session_id": str(uuid.uuid4()),  # fresh per question — no context leakage
        "explain": True,
        "schema_hints": [],
    }
    if provider_id:
        payload["provider_id"] = provider_id

    resp = await client.post(
        f"{api_url}/api/v1/query/execute",
        json=payload,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=120.0,
    )
    return {"status_code": resp.status_code, "body": resp.json()}


# ---------------------------------------------------------------------------
# Per-question evaluation
# ---------------------------------------------------------------------------

async def _evaluate_question(
    client: httpx.AsyncClient,
    engine: Any,
    api_url: str,
    api_key: str,
    entry: dict[str, Any],
    provider_id: str | None,
    gold_cache: Path | None = None,
    eval_mode: EvalMode = "official",
) -> dict[str, Any]:
    question_id = entry.get("question_id", "?")
    db_id: str = entry["db_id"]
    question: str = _build_intent(entry["question"], entry.get("evidence"))
    gold_sql: str = entry["SQL"]

    result: dict[str, Any] = {
        "question_id": question_id,
        "db_id": db_id,
        "question": question,
        "gold_sql": gold_sql,
        "generated_sql": None,
        "source_database_used": None,
        "status": "exception",
        "match": False,
        "match_tolerant": False,
        "soft_f1": 0.0,
        "gold_ms": None,
        "pred_ms": None,
        "gold_timed": False,
        "error": None,
        "latency_ms": None,
    }

    t0 = time.perf_counter()

    try:
        api_resp = await _submit_question(
            client, api_url, api_key, question, db_id, provider_id
        )
        result["latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)

        if api_resp["status_code"] != 200:
            detail = str(api_resp["body"].get("detail", ""))
            if "explain=true requires admin scope" in detail:
                raise SystemExit(
                    "\nofficial eval mode needs the explain payload to fetch"
                    " native driver values, and explain=true requires an"
                    " admin-scope API key.\n"
                    "Re-run with an admin key, or pass --eval-mode tolerant"
                    " (whose number is NOT BIRD-comparable).\n"
                )
            result["status"] = "api_error"
            result["error"] = api_resp["body"].get("message", str(api_resp["body"]))
            return result

        body = api_resp["body"]
        generated_sql = body.get("sql", "")
        result["generated_sql"] = generated_sql
        result["source_database_used"] = body.get("source_database_used")

        _t_gold = time.perf_counter()
        gold_rows = await _run_gold_sql(engine, gold_sql, db_id, gold_cache)
        result["gold_ms"] = (time.perf_counter() - _t_gold) * 1000
        # A cache hit is not a real execution, so it cannot time gold. R-VES
        # is reported only over questions actually executed in this run.
        result["gold_timed"] = gold_cache is None

        # Re-execute what Aegis produced through the same driver as gold, so
        # both sides carry native PostgreSQL types. The API's JSON layer
        # stringifies Decimal and date (see _coerce_row), which makes them
        # incomparable to gold's native values under BIRD's raw set equality
        # and silently lost correct answers. Falls back to the JSON rows when
        # the explain payload is unavailable.
        _t_pred = time.perf_counter()
        gen_rows = await _rerun_generated_sql(engine, body)
        result["pred_ms"] = (time.perf_counter() - _t_pred) * 1000
        if gen_rows is None:
            result["gold_timed"] = False
            gen_rows = [tuple(row.values()) for row in body.get("results", [])]

        result["match"] = rows_match(gold_rows, gen_rows, mode=eval_mode)
        result["match_tolerant"] = rows_match(gold_rows, gen_rows, mode="tolerant")
        result["soft_f1"] = calculate_f1_score(gen_rows, gold_rows)
        result["status"] = "success"
        result["gold_row_count"] = len(gold_rows)
        result["gen_row_count"] = len(gen_rows)

    except Exception as exc:
        result["latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        result["status"] = "exception"
        result["error"] = str(exc)

    return result


# ---------------------------------------------------------------------------
# Progress printer
# ---------------------------------------------------------------------------

def _print_progress(i: int, total: int, res: dict[str, Any]) -> None:
    is_error = res["status"] != "success"
    symbol = "✓" if res["match"] else ("E" if is_error else "✗")
    suffix = f"  err={res['error'][:60]}" if res.get("error") else ""
    print(
        f"[{i:>4}/{total}] {symbol}  "
        f"db={res['db_id']:<25} "
        f"q={str(res['question_id']):<6} "
        f"({res['latency_ms']}ms){suffix}"
    )


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------

def _print_summary(results: list[dict[str, Any]], output: str | None) -> None:
    total = len(results)
    matched = sum(1 for r in results if r["match"])
    errored = sum(1 for r in results if r["status"] != "success")
    accuracy = matched / total * 100 if total else 0.0

    print("\n" + "=" * 60)
    print(f"  Total questions : {total}")
    print(f"  Correct (EX)    : {matched}")
    print(f"  Incorrect       : {total - matched - errored}")
    print(f"  Errors          : {errored}")
    print(f"  EX Accuracy     : {accuracy:.1f}%")

    tolerant = sum(1 for r in results if r.get("match_tolerant"))
    soft_f1 = sum(float(r.get("soft_f1") or 0.0) for r in results)
    print(f"  Soft-F1         : {soft_f1 / total * 100 if total else 0.0:.1f}%")
    print(
        f"  [diagnostic] tolerant EX : {tolerant}"
        f" ({tolerant / total * 100 if total else 0.0:.1f}%)"
        f"  — representation-forgiving, NOT a BIRD-comparable number"
    )

    timed = [
        r for r in results
        if r.get("gold_timed") and r.get("pred_ms") and r.get("gold_ms")
    ]
    if timed:
        total_reward = 0.0
        for r in timed:
            if not r["match"]:
                continue
            ratio = float(r["gold_ms"]) / float(r["pred_ms"])
            total_reward += ves_score(ves_reward(ratio))
        print(
            f"  R-VES           : {total_reward / len(timed):.1f}"
            f"  (over {len(timed)}/{total} questions executed this run)"
        )
    else:
        print(
            "  R-VES           : n/a — gold came from cache;"
            " re-run without --gold-cache to time it"
        )
    print("=" * 60)

    db_stats: dict[str, dict[str, int]] = {}
    for r in results:
        db = r["db_id"]
        if db not in db_stats:
            db_stats[db] = {"total": 0, "matched": 0}
        db_stats[db]["total"] += 1
        if r["match"]:
            db_stats[db]["matched"] += 1

    if len(db_stats) > 1:
        print("\nPer-database breakdown:")
        for db, s in sorted(db_stats.items()):
            pct = s["matched"] / s["total"] * 100 if s["total"] else 0.0
            print(f"  {db:<30} {s['matched']}/{s['total']}  ({pct:.1f}%)")

    if output:
        output_path = Path(output)
        with output_path.open("w") as f:
            json.dump(
                {
                    "summary": {
                        "total": total,
                        "matched": matched,
                        "errored": errored,
                        "accuracy_pct": round(accuracy, 2),
                    },
                    "results": results,
                },
                f,
                indent=2,
                default=str,
            )
        print(f"\nDetailed results written to: {output_path}")


# ---------------------------------------------------------------------------
# Run metadata + persistence
# ---------------------------------------------------------------------------

def _get_git_metadata(repo_root: Path) -> tuple[str | None, int | None]:
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return None, None

    try:
        status = subprocess.check_output(
            ["git", "status", "--porcelain"],
            cwd=repo_root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return commit, None

    dirty = 1 if status else 0
    return commit, dirty


def _utc_timestamp() -> tuple[str, str]:
    now = datetime.now(UTC)
    run_id_ts = now.strftime("%Y%m%d-%H%M%S")
    iso_ts = now.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return run_id_ts, iso_ts


def _redacted_args(args: argparse.Namespace) -> str:
    payload = dict(vars(args))
    if "api_key" in payload:
        payload["api_key"] = "<redacted>"
    return json.dumps(payload, sort_keys=True)


def _init_store(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS benchmark_runs(
            run_id TEXT PRIMARY KEY,
            commit_hash TEXT,
            dirty INTEGER,
            timestamp TEXT,
            provider_id TEXT,
            total INTEGER,
            matched INTEGER,
            errored INTEGER,
            accuracy_pct REAL,
            args_json TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS benchmark_results(
            run_id TEXT,
            question_id TEXT,
            db_id TEXT,
            question TEXT,
            gold_sql TEXT,
            generated_sql TEXT,
            source_database_used TEXT,
            status TEXT,
            match INTEGER,
            match_tolerant INTEGER,
            soft_f1 REAL,
            error TEXT,
            latency_ms REAL
        )
        """
    )


def _persist_results(
    store_path: Path,
    run_record: dict[str, Any],
    results: list[dict[str, Any]],
) -> None:
    store_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(store_path)
    try:
        _init_store(conn)
        conn.execute(
            """
            INSERT INTO benchmark_runs(
                run_id,
                commit_hash,
                dirty,
                timestamp,
                provider_id,
                total,
                matched,
                errored,
                accuracy_pct,
                args_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_record["run_id"],
                run_record["commit_hash"],
                run_record["dirty"],
                run_record["timestamp"],
                run_record["provider_id"],
                run_record["total"],
                run_record["matched"],
                run_record["errored"],
                run_record["accuracy_pct"],
                run_record["args_json"],
            ),
        )

        result_rows = [
            (
                run_record["run_id"],
                str(r.get("question_id", "")),
                r.get("db_id", ""),
                r.get("question", ""),
                r.get("gold_sql", ""),
                r.get("generated_sql"),
                r.get("source_database_used"),
                r.get("status", "exception"),
                1 if r.get("match") else 0,
                1 if r.get("match_tolerant") else 0,
                float(r.get("soft_f1") or 0.0),
                r.get("error"),
                r.get("latency_ms"),
            )
            for r in results
        ]
        conn.executemany(
            """
            INSERT INTO benchmark_results(
                run_id,
                question_id,
                db_id,
                question,
                gold_sql,
                generated_sql,
                source_database_used,
                status,
                match,
                match_tolerant,
                soft_f1,
                error,
                latency_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            result_rows,
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main evaluation loop
# ---------------------------------------------------------------------------

async def main(args: argparse.Namespace) -> None:
    questions_path = Path(args.questions)
    if not questions_path.exists():
        print(f"ERROR: questions file not found: {questions_path}", file=sys.stderr)
        sys.exit(1)

    with questions_path.open() as f:
        dataset: list[dict[str, Any]] = json.load(f)

    if args.db_filter:
        dataset = [e for e in dataset if e.get("db_id") == args.db_filter]

    if args.limit:
        dataset = dataset[: args.limit]

    if args.concurrency < 1:
        print("ERROR: --concurrency must be >= 1", file=sys.stderr)
        sys.exit(1)

    print(
        f"Evaluating {len(dataset)} questions"
        + (f" (db_filter={args.db_filter})" if args.db_filter else "")
        + (f" (limit={args.limit})" if args.limit else "")
    )

    repo_root = Path(__file__).resolve().parents[1]
    commit_hash, dirty = _get_git_metadata(repo_root)
    run_id_ts, timestamp = _utc_timestamp()
    if commit_hash:
        run_id = f"{run_id_ts}-{commit_hash[:7]}"
    else:
        run_id = f"{run_id_ts}-nogit"

    engine = create_async_engine(args.db_url, echo=False)

    async with httpx.AsyncClient() as client:
        results: list[dict[str, Any]] = []
        semaphore = asyncio.Semaphore(args.concurrency)

        gold_cache = Path(args.gold_cache) if args.gold_cache else None

        async def _bound_eval(entry: dict[str, Any]) -> dict[str, Any]:
            async with semaphore:
                return await _evaluate_question(
                    client,
                    engine,
                    args.api_url,
                    args.api_key,
                    entry,
                    args.provider_id,
                    gold_cache,
                    args.eval_mode,
                )

        tasks = [asyncio.create_task(_bound_eval(entry)) for entry in dataset]
        for i, task in enumerate(asyncio.as_completed(tasks), 1):
            res = await task
            results.append(res)
            _print_progress(i, len(dataset), res)

    await engine.dispose()

    total = len(results)
    matched = sum(1 for r in results if r["match"])
    errored = sum(1 for r in results if r["status"] != "success")
    accuracy = matched / total * 100 if total else 0.0

    store_path = Path(args.store)
    if store_path.as_posix() != "/dev/null":
        run_record = {
            "run_id": run_id,
            "commit_hash": commit_hash,
            "dirty": dirty,
            "timestamp": timestamp,
            "provider_id": args.provider_id,
            "eval_mode": args.eval_mode,
            "total": total,
            "matched": matched,
            "errored": errored,
            "accuracy_pct": round(accuracy, 2),
            "args_json": _redacted_args(args),
        }
        try:
            _persist_results(store_path, run_record, results)
            print(f"\nStored run {run_id} in {store_path}")
        except Exception as exc:
            print(f"\nWARNING: Failed to persist results: {exc}", file=sys.stderr)

    _print_summary(results, args.output)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="BIRD-SQL execution-match benchmark for Aegis Query Language"
    )
    parser.add_argument(
        "--questions",
        required=True,
        help="Path to BIRD dev.json questions file",
    )
    parser.add_argument(
        "--api-key",
        required=True,
        help="Aegis API key (query scope)",
    )
    parser.add_argument(
        "--api-url",
        default="http://localhost:8000",
        help="Aegis API base URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--db-url",
        default=(
            "postgresql+asyncpg://postgres:postgrespassword"
            "@127.0.0.1:5432/aegis_data_warehouse"
        ),
        help="PostgreSQL connection URL for gold SQL execution",
    )
    parser.add_argument(
        "--provider-id",
        default=None,
        help="LLM provider override (e.g. 'anthropic:claude-sonnet-4-6')",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=2,
        help=(
            "Max in-flight questions (default: 2; keep low for"
            " multi-database runs to avoid 429s)"
        ),
    )
    parser.add_argument(
        "--eval-mode",
        choices=["official", "tolerant"],
        default="official",
        help=(
            "Scoring semantics. 'official' is BIRD's calculate_ex verbatim and"
            " is the only BIRD-comparable number. 'tolerant' additionally"
            " forgives float8-vs-numeric representation differences; useful"
            " for tracking arithmetic correctness, never for reporting."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Evaluate only the first N questions (for quick smoke tests)",
    )
    parser.add_argument(
        "--db-filter",
        default=None,
        help="Restrict evaluation to a single BIRD database (e.g. 'financial')",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional path to write detailed JSON results",
    )
    parser.add_argument(
        "--gold-cache",
        default="benchmarks/gold_cache.db",
        help=(
            "SQLite path caching gold SQL results across runs"
            " (empty string disables)"
        ),
    )
    parser.add_argument(
        "--store",
        default="benchmarks/results.db",
        help="SQLite DB path to store results (use /dev/null to disable)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main(_parse_args()))
