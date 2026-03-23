#!/usr/bin/env python3
"""Benchmark results reporting for BIRD runs.

Reads from the SQLite store produced by scripts/run_bird_benchmark.py.

Usage:
  uv run python scripts/benchmark_report.py [--store PATH] list
  uv run python scripts/benchmark_report.py [--store PATH] compare <run_a> <run_b>
  uv run python scripts/benchmark_report.py [--store PATH] regressions <run_a> <run_b>
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Any

DEFAULT_STORE = "benchmarks/results.db"


def _connect(store_path: Path) -> sqlite3.Connection:
    if not store_path.exists():
        print(f"ERROR: store not found: {store_path}", file=sys.stderr)
        sys.exit(1)
    uri = f"file:{store_path.as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def _print_table(headers: list[str], rows: list[list[str]]) -> None:
    if not rows:
        print("No rows to display.")
        return

    widths = [len(h) for h in headers]
    for row in rows:
        for i, value in enumerate(row):
            widths[i] = max(widths[i], len(value))

    header_line = " ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(header_line)
    print("-" * len(header_line))
    for row in rows:
        print(" ".join(value.ljust(widths[i]) for i, value in enumerate(row)))


def _load_run_meta(conn: sqlite3.Connection, run_id: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT run_id, total, matched, accuracy_pct,
               commit_hash, dirty, timestamp, provider_id
        FROM benchmark_runs
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    if not row:
        print(f"ERROR: run_id not found: {run_id}", file=sys.stderr)
        sys.exit(1)
    return {
        "run_id": row[0],
        "total": row[1],
        "matched": row[2],
        "accuracy_pct": row[3],
        "commit_hash": row[4],
        "dirty": row[5],
        "timestamp": row[6],
        "provider_id": row[7],
    }


def _cmd_list(args: argparse.Namespace) -> None:
    conn = _connect(Path(args.store))
    try:
        rows = conn.execute(
            """
            SELECT run_id, commit_hash, dirty, timestamp,
                   provider_id, total, accuracy_pct
            FROM benchmark_runs
            ORDER BY timestamp DESC
            """
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        print("No benchmark runs found.")
        return

    formatted: list[list[str]] = []
    for run_id, commit_hash, dirty, timestamp, provider_id, total, accuracy_pct in rows:
        commit = (commit_hash[:7] if commit_hash else "nogit")
        if dirty == 1:
            dirty_str = "yes"
        elif dirty == 0:
            dirty_str = "no"
        else:
            dirty_str = "n/a"
        provider = provider_id or "default"
        accuracy = f"{accuracy_pct:.1f}%" if accuracy_pct is not None else "n/a"
        formatted.append(
            [
                str(run_id),
                commit,
                dirty_str,
                str(timestamp),
                provider,
                str(total),
                accuracy,
            ]
        )

    _print_table(
        ["run_id", "commit", "dirty", "timestamp", "provider", "questions", "accuracy"],
        formatted,
    )


def _cmd_compare(args: argparse.Namespace) -> None:
    run_a = args.run_a
    run_b = args.run_b

    conn = _connect(Path(args.store))
    try:
        meta_a = _load_run_meta(conn, run_a)
        meta_b = _load_run_meta(conn, run_b)

        rows = conn.execute(
            """
            SELECT a.question_id, a.db_id, a.match, b.match
            FROM benchmark_results a
            JOIN benchmark_results b
              ON a.question_id = b.question_id AND a.db_id = b.db_id
            WHERE a.run_id = ? AND b.run_id = ?
            """,
            (run_a, run_b),
        ).fetchall()
    finally:
        conn.close()

    if meta_a["total"] != meta_b["total"]:
        print(
            "WARNING: run totals differ "
            f"({run_a}={meta_a['total']}, {run_b}={meta_b['total']}). "
            "Using intersection of (question_id, db_id) for deltas."
        )

    if not rows:
        print("No overlapping questions between runs.")
        return

    total = len(rows)
    matched_a = sum(1 for _qid, _db, match_a, _match_b in rows if match_a == 1)
    matched_b = sum(1 for _qid, _db, _match_a, match_b in rows if match_b == 1)
    acc_a = matched_a / total * 100
    acc_b = matched_b / total * 100
    delta = acc_b - acc_a

    def _fmt_commit(meta: dict[str, Any]) -> str:
        commit = (meta["commit_hash"] or "nogit")[:7]
        dirty = " (dirty)" if meta["dirty"] == 1 else ""
        return f"{commit}{dirty}"

    def _fmt_run(meta: dict[str, Any]) -> str:
        provider = meta["provider_id"] or "default"
        return (
            f"{meta['run_id']}  commit={_fmt_commit(meta)}"
            f"  ts={meta['timestamp']}  provider={provider}"
            f"  questions={meta['total']}"
        )

    print(f"Run A: {_fmt_run(meta_a)}")
    print(f"Run B: {_fmt_run(meta_b)}")
    print(f"Intersection questions: {total}")
    print(f"Overall accuracy: {acc_a:.1f}% -> {acc_b:.1f}% ({delta:+.1f} pp)")

    per_db: dict[str, dict[str, int]] = {}
    for _qid, db_id, match_a, match_b in rows:
        stats = per_db.setdefault(db_id, {"total": 0, "matched_a": 0, "matched_b": 0})
        stats["total"] += 1
        if match_a == 1:
            stats["matched_a"] += 1
        if match_b == 1:
            stats["matched_b"] += 1

    print("\nPer-database accuracy:")
    table_rows: list[list[str]] = []
    for db_id in sorted(per_db):
        stats = per_db[db_id]
        total_db = stats["total"]
        acc_db_a = stats["matched_a"] / total_db * 100 if total_db else 0.0
        acc_db_b = stats["matched_b"] / total_db * 100 if total_db else 0.0
        delta_db = acc_db_b - acc_db_a
        table_rows.append(
            [
                db_id,
                f"{acc_db_a:.1f}%",
                f"{acc_db_b:.1f}%",
                f"{delta_db:+.1f} pp",
            ]
        )

    _print_table(["db_id", "run_a", "run_b", "delta"], table_rows)

    regressions = sum(
        1 for _qid, _db, match_a, match_b in rows if match_a == 1 and match_b == 0
    )
    improvements = sum(
        1 for _qid, _db, match_a, match_b in rows if match_a == 0 and match_b == 1
    )

    print("\nDelta counts:")
    print(f"Regressions: {regressions}")
    print(f"Improvements: {improvements}")


def _cmd_regressions(args: argparse.Namespace) -> None:
    run_a = args.run_a
    run_b = args.run_b

    conn = _connect(Path(args.store))
    try:
        _load_run_meta(conn, run_a)
        _load_run_meta(conn, run_b)

        rows = conn.execute(
            """
            SELECT a.question_id, a.db_id, a.question, b.status, b.error
            FROM benchmark_results a
            JOIN benchmark_results b
              ON a.question_id = b.question_id AND a.db_id = b.db_id
            WHERE a.run_id = ?
              AND b.run_id = ?
              AND a.match = 1
              AND b.match = 0
            ORDER BY a.db_id, a.question_id
            """,
            (run_a, run_b),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        print("No regressions found.")
        return

    formatted: list[list[str]] = []
    for question_id, db_id, question, status, error in rows:
        error_text = error or ""
        if error_text and len(error_text) > 200:
            error_text = error_text[:197] + "..."
        formatted.append([
            str(question_id),
            str(db_id),
            str(question),
            str(status),
            error_text,
        ])

    _print_table(["question_id", "db_id", "question", "status", "error"], formatted)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="BIRD benchmark report tool (reads SQLite store)"
    )
    parser.add_argument(
        "--store",
        default=DEFAULT_STORE,
        help=f"SQLite DB path (default: {DEFAULT_STORE})",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="List stored benchmark runs")

    compare = sub.add_parser("compare", help="Compare two runs")
    compare.add_argument("run_a", help="Baseline run_id")
    compare.add_argument("run_b", help="Newer run_id")

    regressions = sub.add_parser("regressions", help="List regressions")
    regressions.add_argument("run_a", help="Baseline run_id")
    regressions.add_argument("run_b", help="Newer run_id")

    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.command == "list":
        _cmd_list(args)
    elif args.command == "compare":
        _cmd_compare(args)
    elif args.command == "regressions":
        _cmd_regressions(args)
    else:
        print(f"ERROR: Unknown command: {args.command}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
